#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from glob import glob
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

try:
    import psycopg
    from psycopg.rows import dict_row

    DB_DRIVER = "psycopg3"
except ModuleNotFoundError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]
    DB_DRIVER = ""

try:
    import psycopg2
    from psycopg2.extras import Json, RealDictCursor

    if not DB_DRIVER:
        DB_DRIVER = "psycopg2"
except ModuleNotFoundError:  # pragma: no cover
    psycopg2 = None  # type: ignore[assignment]
    Json = None  # type: ignore[assignment]
    RealDictCursor = None  # type: ignore[assignment]

try:
    from rich.console import Console
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
except ModuleNotFoundError:  # pragma: no cover
    print("Missing dependency: rich. Install with `pip install -r scripts/cc_usage/requirements.txt`.")
    raise SystemExit(1)


console = Console()
STOP = False
LOCK_HANDLES: dict[str, Any] = {}
MODEL_RE = re.compile(r"model\s*[:=]\s*([A-Za-z0-9._:-]+)")
INPUT_RE = re.compile(r"(?:input_tokens|prompt_tokens)\s*[:=]\s*(\d+)")
OUTPUT_RE = re.compile(r"(?:output_tokens|completion_tokens)\s*[:=]\s*(\d+)")
SESSION_RE = re.compile(r"session(?:_id)?\s*[:=]\s*([A-Za-z0-9._:-]+)")
REQUEST_RE = re.compile(r"request(?:_id)?\s*[:=]\s*([A-Za-z0-9._:-]+)")
LATENCY_RE = re.compile(r"(?:latency_ms|duration_ms)\s*[:=]\s*(\d+)")
PERCENT_RE = re.compile(r"(?<!\d)(100|[1-9]?\d)\s*%")
ANSI_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


@dataclass(frozen=True)
class AppConfig:
    database_url: str
    local_tz: str


@dataclass(frozen=True)
class UsageEvent:
    ts: datetime
    session_id: str | None
    project: str
    model: str
    input_tokens: int
    output_tokens: int
    request_id: str | None
    latency_ms: int | None
    raw_payload: dict[str, Any]


@dataclass(frozen=True)
class CollectorResult:
    source_key: str
    offset: int
    parsed: int
    inserted: int
    skipped: int


def _handle_signal(_signum: int, _frame: Any) -> None:
    global STOP
    STOP = True


def load_config() -> AppConfig:
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required.")
    local_tz = os.getenv("LOCAL_TZ", "Asia/Seoul").strip() or "Asia/Seoul"
    return AppConfig(database_url=database_url, local_tz=local_tz)


def connect(cfg: AppConfig) -> Any:
    if DB_DRIVER == "psycopg3":
        return psycopg.connect(cfg.database_url, row_factory=dict_row)
    if DB_DRIVER == "psycopg2":
        return psycopg2.connect(cfg.database_url)
    raise RuntimeError(
        "Missing dependency: install psycopg or psycopg2. "
        "Try `pip install -r scripts/cc_usage/requirements.txt`."
    )


def get_cursor(conn: Any) -> Any:
    if DB_DRIVER == "psycopg2":
        return conn.cursor(cursor_factory=RealDictCursor)
    return conn.cursor()


def jsonb_param(payload: dict[str, Any]) -> Any:
    if DB_DRIVER == "psycopg2":
        return Json(payload)
    return payload


def parse_money(value: Any) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def fmt_money(value: Any) -> str:
    return f"${parse_money(value):,.6f}"


def fmt_int(value: Any) -> str:
    return f"{int(value or 0):,}"


def fmt_ts(value: Any, tz_name: str) -> str:
    if value is None:
        return "-"
    if not isinstance(value, datetime):
        return str(value)
    return value.astimezone(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")


def compact_int(value: int) -> str:
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}m"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k"
    return str(value)


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def atomic_write_text(path: Path, text: str) -> None:
    ensure_parent_dir(path)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(text, encoding="utf-8")
    tmp_path.replace(path)


def default_lock_file(name: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-") or "default"
    return Path(os.path.expanduser(f"~/.cache/cc-usage/locks/{safe}.lock")).resolve()


def acquire_single_instance_lock(name: str, holder: str) -> Path:
    lock_path = default_lock_file(name)
    ensure_parent_dir(lock_path)
    fh = open(lock_path, "a+", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        fh.seek(0)
        existing = fh.read().strip()
        fh.close()
        detail = f" ({existing})" if existing else ""
        raise RuntimeError(f"another '{name}' process is already running{detail}") from exc

    fh.seek(0)
    fh.truncate()
    fh.write(f"pid={os.getpid()} holder={holder} started_at={datetime.now(timezone.utc).isoformat()}\n")
    fh.flush()
    LOCK_HANDLES[name] = fh
    return lock_path


def default_status_file() -> Path:
    return Path(os.path.expanduser("~/.cache/cc-usage/status.json")).resolve()


def default_manual_remaining_file() -> Path:
    return Path(os.path.expanduser("~/.cache/cc-usage/claude-remaining.json")).resolve()


def read_manual_sync_payload(path: Path | None = None) -> dict[str, Any]:
    target = path or default_manual_remaining_file()
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
        return {}
    except Exception:
        return {}


def read_manual_remaining_percent(path: Path | None = None) -> float | None:
    payload = read_manual_sync_payload(path)
    value = payload.get("remaining_percent")
    if value is None:
        return None
    try:
        result = float(value)
    except Exception:
        return None
    if result < 0 or result > 100:
        return None
    return result


def read_manual_reset_text(path: Path | None = None) -> str | None:
    payload = read_manual_sync_payload(path)
    value = payload.get("reset_text")
    if not isinstance(value, str):
        return None
    text = value.strip()
    return text or None


def write_manual_remaining_percent(percent: float, path: Path | None = None, reset_text: str | None = None) -> Path:
    target = path or default_manual_remaining_file()
    payload = {
        "remaining_percent": float(percent),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if reset_text:
        payload["reset_text"] = reset_text.strip()
    atomic_write_text(
        target,
        json.dumps(payload, ensure_ascii=False, indent=2),
    )
    return target


def nested_get(data: dict[str, Any], *paths: str) -> Any:
    for path in paths:
        cur: Any = data
        ok = True
        for part in path.split("."):
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
            else:
                ok = False
                break
        if ok:
            return cur
    return None


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        if value.isdigit():
            return int(value)
    return None


def parse_event_ts(value: Any) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if isinstance(value, (int, float)):
        iv = float(value)
        if iv > 10_000_000_000:
            iv = iv / 1000.0
        return datetime.fromtimestamp(iv, tz=timezone.utc)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            iv = safe_int(text)
            if iv is not None:
                return parse_event_ts(iv)
    return datetime.now(timezone.utc)


def parse_usage_event_from_json(data: dict[str, Any], project: str, default_model: str | None) -> UsageEvent | None:
    model = nested_get(data, "model", "message.model", "response.model", "meta.model", "usage.model")
    if not model:
        model = default_model
    if not model:
        return None

    usage_obj = nested_get(data, "usage", "message.usage")
    last_usage_obj = nested_get(data, "payload.info.last_token_usage")
    total_usage_obj = nested_get(data, "payload.info.total_token_usage")
    input_tokens = safe_int(
        nested_get(
            data,
            "input_tokens",
            "prompt_tokens",
            "inputTokenCount",
            "token_usage.input_tokens",
            "message.usage.input_tokens",
            "message.usage.prompt_tokens",
        )
    )
    output_tokens = safe_int(
        nested_get(
            data,
            "output_tokens",
            "completion_tokens",
            "outputTokenCount",
            "token_usage.output_tokens",
            "message.usage.output_tokens",
            "message.usage.completion_tokens",
        )
    )
    if isinstance(usage_obj, dict):
        if input_tokens is None:
            input_tokens = safe_int(
                nested_get(
                    usage_obj,
                    "input_tokens",
                    "prompt_tokens",
                    "inputTokenCount",
                )
            )
        if output_tokens is None:
            output_tokens = safe_int(
                nested_get(
                    usage_obj,
                    "output_tokens",
                    "completion_tokens",
                    "outputTokenCount",
                )
            )
    # Codex session jsonl token_count format
    if isinstance(last_usage_obj, dict):
        if input_tokens is None:
            input_tokens = safe_int(last_usage_obj.get("input_tokens"))
        if output_tokens is None:
            output_tokens = safe_int(last_usage_obj.get("output_tokens"))
    if isinstance(total_usage_obj, dict):
        if input_tokens is None:
            input_tokens = safe_int(total_usage_obj.get("input_tokens"))
        if output_tokens is None:
            output_tokens = safe_int(total_usage_obj.get("output_tokens"))
    input_tokens = input_tokens or 0
    output_tokens = output_tokens or 0
    if input_tokens <= 0 and output_tokens <= 0:
        return None

    session_id = nested_get(
        data,
        "session_id",
        "sessionId",
        "session.id",
        "metadata.session_id",
        "conversation_id",
    )
    request_id = nested_get(
        data,
        "request_id",
        "requestId",
        "id",
        "message.id",
        "response_id",
        "request.id",
    )
    latency_ms = safe_int(nested_get(data, "latency_ms", "duration_ms", "meta.latency_ms"))
    ts = parse_event_ts(nested_get(data, "ts", "timestamp", "created_at", "time", "message.timestamp"))

    return UsageEvent(
        ts=ts,
        session_id=str(session_id) if session_id else None,
        project=project,
        model=str(model),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        request_id=str(request_id) if request_id else None,
        latency_ms=latency_ms,
        raw_payload=data,
    )


def parse_usage_event_from_text(line: str, project: str, default_model: str | None) -> UsageEvent | None:
    in_match = INPUT_RE.search(line)
    out_match = OUTPUT_RE.search(line)
    if not in_match and not out_match:
        return None
    input_tokens = int(in_match.group(1)) if in_match else 0
    output_tokens = int(out_match.group(1)) if out_match else 0
    if input_tokens <= 0 and output_tokens <= 0:
        return None

    model_match = MODEL_RE.search(line)
    model = model_match.group(1) if model_match else default_model
    if not model:
        return None
    session_match = SESSION_RE.search(line)
    request_match = REQUEST_RE.search(line)
    latency_match = LATENCY_RE.search(line)

    return UsageEvent(
        ts=datetime.now(timezone.utc),
        session_id=session_match.group(1) if session_match else None,
        project=project,
        model=model,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        request_id=request_match.group(1) if request_match else None,
        latency_ms=int(latency_match.group(1)) if latency_match else None,
        raw_payload={"raw_line": line},
    )


def parse_usage_event(line: str, project: str, default_model: str | None) -> UsageEvent | None:
    line = line.strip()
    if not line:
        return None

    if line.startswith("{") and line.endswith("}"):
        try:
            payload = json.loads(line)
            if isinstance(payload, dict):
                return parse_usage_event_from_json(payload, project=project, default_model=default_model)
        except json.JSONDecodeError:
            pass
    return parse_usage_event_from_text(line, project=project, default_model=default_model)


def ensure_event_identity(event: UsageEvent, source_key: str, raw_line: str, session_hint: str | None) -> UsageEvent:
    session_id = event.session_id or session_hint
    request_id = event.request_id
    if not request_id:
        digest = hashlib.sha1(f"{source_key}|{raw_line}".encode("utf-8")).hexdigest()[:24]
        request_id = f"auto:{digest}"
    return UsageEvent(
        ts=event.ts,
        session_id=session_id,
        project=event.project,
        model=event.model,
        input_tokens=event.input_tokens,
        output_tokens=event.output_tokens,
        request_id=request_id,
        latency_ms=event.latency_ms,
        raw_payload=event.raw_payload,
    )


def insert_usage_event(cur: Any, event: UsageEvent, with_rollup: bool = True) -> bool:
    cur.execute(
        """
        INSERT INTO usage_events (
            ts, session_id, project, model, input_tokens, output_tokens, request_id, latency_ms, raw_payload
        ) VALUES (
            %(ts)s, %(session_id)s, %(project)s, %(model)s, %(input_tokens)s, %(output_tokens)s, %(request_id)s, %(latency_ms)s, %(raw_payload)s
        )
        ON CONFLICT DO NOTHING
        """,
        {
            "ts": event.ts,
            "session_id": event.session_id,
            "project": event.project,
            "model": event.model,
            "input_tokens": event.input_tokens,
            "output_tokens": event.output_tokens,
            "request_id": event.request_id,
            "latency_ms": event.latency_ms,
            "raw_payload": jsonb_param(event.raw_payload),
        },
    )
    inserted = cur.rowcount > 0
    if not inserted or not with_rollup:
        return inserted

    cur.execute(
        """
        INSERT INTO usage_rollup_minute (
            bucket_minute, project, model, requests, input_tokens, output_tokens, cost
        )
        VALUES (
            date_trunc('minute', %(ts)s),
            %(project)s,
            %(model)s,
            1,
            %(input_tokens)s,
            %(output_tokens)s,
            (
                COALESCE(
                    (
                        SELECT pr.input_per_mtok
                        FROM pricing pr
                        WHERE pr.model = %(model)s
                          AND pr.effective_from <= %(ts)s
                        ORDER BY pr.effective_from DESC
                        LIMIT 1
                    ),
                    0
                ) * %(input_tokens)s / 1000000.0
              +
                COALESCE(
                    (
                        SELECT pr.output_per_mtok
                        FROM pricing pr
                        WHERE pr.model = %(model)s
                          AND pr.effective_from <= %(ts)s
                        ORDER BY pr.effective_from DESC
                        LIMIT 1
                    ),
                    0
                ) * %(output_tokens)s / 1000000.0
            )
        )
        ON CONFLICT (bucket_minute, project, model)
        DO UPDATE SET
            requests = usage_rollup_minute.requests + EXCLUDED.requests,
            input_tokens = usage_rollup_minute.input_tokens + EXCLUDED.input_tokens,
            output_tokens = usage_rollup_minute.output_tokens + EXCLUDED.output_tokens,
            cost = usage_rollup_minute.cost + EXCLUDED.cost
        """,
        {
            "ts": event.ts,
            "project": event.project,
            "model": event.model,
            "input_tokens": event.input_tokens,
            "output_tokens": event.output_tokens,
        },
    )
    return True


def get_saved_offset(cur: Any, source_file: str) -> int | None:
    cur.execute(
        "SELECT file_offset FROM collector_offsets WHERE source_file = %(source_file)s",
        {"source_file": source_file},
    )
    row = cur.fetchone()
    if not row:
        return None
    return int(row["file_offset"])


def upsert_offset(cur: Any, source_file: str, offset: int) -> None:
    cur.execute(
        """
        INSERT INTO collector_offsets(source_file, file_offset, updated_at)
        VALUES (%(source_file)s, %(offset)s, NOW())
        ON CONFLICT (source_file)
        DO UPDATE SET file_offset = EXCLUDED.file_offset, updated_at = NOW()
        """,
        {"source_file": source_file, "offset": max(0, offset)},
    )


def command_init_db(cfg: AppConfig, schema_path: str | None) -> int:
    path = Path(schema_path) if schema_path else Path(__file__).with_name("schema.sql")
    if not path.exists():
        console.print(f"[red]schema file not found:[/red] {path}")
        return 1
    sql = path.read_text(encoding="utf-8")
    with connect(cfg) as conn:
        with get_cursor(conn) as cur:
            cur.execute(sql)
        conn.commit()
    console.print(f"[green]DB schema applied:[/green] {path}")
    return 0


def command_doctor(cfg: AppConfig) -> int:
    table_names = ("usage_events", "pricing", "usage_rollup_minute", "collector_offsets")
    with connect(cfg) as conn:
        with get_cursor(conn) as cur:
            cur.execute("SELECT current_database() AS db, current_user AS usr, version() AS ver;")
            db_info = cur.fetchone()
            cur.execute(
                """
                SELECT
                    to_regclass('public.usage_events') AS usage_events,
                    to_regclass('public.pricing') AS pricing,
                    to_regclass('public.usage_rollup_minute') AS usage_rollup_minute,
                    to_regclass('public.collector_offsets') AS collector_offsets
                """
            )
            reg = cur.fetchone()
            cur.execute("SELECT COUNT(*) AS cnt FROM pricing;")
            pricing_cnt = cur.fetchone()["cnt"]

    console.print("[bold]cc-usage doctor[/bold]")
    console.print(f"Database: {db_info['db']}")
    console.print(f"User: {db_info['usr']}")
    console.print(f"Version: {db_info['ver'].split(',')[0]}")
    console.print(f"DB driver: {DB_DRIVER}")
    for name in table_names:
        status = "OK" if reg[name] else "MISSING"
        color = "green" if reg[name] else "red"
        console.print(f"{name}: [{color}]{status}[/{color}]")
    console.print(f"pricing rows: {pricing_cnt}")
    return 0 if all(reg[name] for name in table_names) else 1


def where_project_session(project: str | None, session_id: str | None) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if project:
        clauses.append("e.project = %(project)s")
        params["project"] = project
    if session_id:
        clauses.append("e.session_id = %(session_id)s")
        params["session_id"] = session_id
    where_sql = " AND ".join(clauses)
    return where_sql, params


def query_today_summary(
    conn: Any, tz_name: str, project: str | None = None, session_id: str | None = None
) -> dict[str, Any]:
    extra_where, params = where_project_session(project, session_id)
    sql = f"""
        SELECT
            COUNT(*) AS requests,
            COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
            COALESCE(SUM(e.output_tokens), 0) AS output_tokens,
            COALESCE(
                SUM(
                    (COALESCE(p.input_per_mtok, 0) * e.input_tokens / 1000000.0)
                  + (COALESCE(p.output_per_mtok, 0) * e.output_tokens / 1000000.0)
                ),
                0
            ) AS cost
        FROM usage_events e
        LEFT JOIN LATERAL (
            SELECT pr.input_per_mtok, pr.output_per_mtok
            FROM pricing pr
            WHERE pr.model = e.model
              AND pr.effective_from <= e.ts
            ORDER BY pr.effective_from DESC
            LIMIT 1
        ) p ON TRUE
        WHERE e.ts >= date_trunc('day', NOW() AT TIME ZONE %(tz)s) AT TIME ZONE %(tz)s
          AND e.ts < (date_trunc('day', NOW() AT TIME ZONE %(tz)s) + INTERVAL '1 day') AT TIME ZONE %(tz)s
          {f"AND {extra_where}" if extra_where else ""}
    """
    params["tz"] = tz_name
    with get_cursor(conn) as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def query_recent_events(
    conn: Any,
    tz_name: str,
    limit: int = 20,
    project: str | None = None,
    session_id: str | None = None,
) -> list[dict[str, Any]]:
    extra_where, params = where_project_session(project, session_id)
    params["limit"] = limit
    sql = f"""
        SELECT
            e.ts,
            e.session_id,
            e.project,
            e.model,
            e.input_tokens,
            e.output_tokens,
            e.latency_ms,
            (
                COALESCE(p.input_per_mtok, 0) * e.input_tokens / 1000000.0
              + COALESCE(p.output_per_mtok, 0) * e.output_tokens / 1000000.0
            ) AS cost
        FROM usage_events e
        LEFT JOIN LATERAL (
            SELECT pr.input_per_mtok, pr.output_per_mtok
            FROM pricing pr
            WHERE pr.model = e.model
              AND pr.effective_from <= e.ts
            ORDER BY pr.effective_from DESC
            LIMIT 1
        ) p ON TRUE
        WHERE 1=1
          {f"AND {extra_where}" if extra_where else ""}
        ORDER BY e.ts DESC
        LIMIT %(limit)s
    """
    with get_cursor(conn) as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    for row in rows:
        row["ts"] = fmt_ts(row["ts"], tz_name)
    return rows


def query_recent_rate(
    conn: Any, minutes: int = 5, project: str | None = None, session_id: str | None = None
) -> dict[str, Any]:
    extra_where, params = where_project_session(project, session_id)
    params["minutes"] = minutes
    sql = f"""
        SELECT
            COUNT(*) AS requests,
            COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
            COALESCE(SUM(e.output_tokens), 0) AS output_tokens,
            COALESCE(
                SUM(
                    (COALESCE(p.input_per_mtok, 0) * e.input_tokens / 1000000.0)
                  + (COALESCE(p.output_per_mtok, 0) * e.output_tokens / 1000000.0)
                ),
                0
            ) AS cost
        FROM usage_events e
        LEFT JOIN LATERAL (
            SELECT pr.input_per_mtok, pr.output_per_mtok
            FROM pricing pr
            WHERE pr.model = e.model
              AND pr.effective_from <= e.ts
            ORDER BY pr.effective_from DESC
            LIMIT 1
        ) p ON TRUE
        WHERE e.ts >= NOW() - make_interval(mins => %(minutes)s)
          {f"AND {extra_where}" if extra_where else ""}
    """
    with get_cursor(conn) as cur:
        cur.execute(sql, params)
        return cur.fetchone()


def query_latest_rate_limits(conn: Any, project: str | None = None, session_id: str | None = None) -> dict[str, float | None]:
    extra_where, params = where_project_session(project, session_id)
    sql = f"""
        SELECT
            raw_payload #>> '{{payload,rate_limits,limit_id}}' AS limit_id,
            raw_payload #>> '{{payload,rate_limits,limit_name}}' AS limit_name,
            NULLIF(raw_payload #>> '{{payload,rate_limits,primary,window_minutes}}', '')::double precision AS primary_window_minutes,
            NULLIF(raw_payload #>> '{{payload,rate_limits,primary,used_percent}}', '')::double precision AS primary_used_percent,
            NULLIF(raw_payload #>> '{{payload,rate_limits,secondary,used_percent}}', '')::double precision AS secondary_used_percent
        FROM usage_events e
        WHERE 1=1
          {f"AND {extra_where}" if extra_where else ""}
          AND (
            (raw_payload #>> '{{payload,rate_limits,primary,used_percent}}') IS NOT NULL
            OR (raw_payload #>> '{{payload,rate_limits,secondary,used_percent}}') IS NOT NULL
          )
        ORDER BY
            CASE
                WHEN (raw_payload #>> '{{payload,rate_limits,primary,window_minutes}}') = '300'
                     AND (raw_payload #>> '{{payload,rate_limits,limit_name}}') IS NULL THEN 0
                WHEN (raw_payload #>> '{{payload,rate_limits,primary,window_minutes}}') = '300' THEN 1
                ELSE 2
            END,
            e.ts DESC
        LIMIT 1
    """
    with get_cursor(conn) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if not row:
        return {
            "primary_used_percent": None,
            "primary_remaining_percent": None,
            "secondary_used_percent": None,
            "secondary_remaining_percent": None,
        }
    primary_used = row["primary_used_percent"]
    secondary_used = row["secondary_used_percent"]
    primary_remain = None if primary_used is None else max(0.0, 100.0 - float(primary_used))
    secondary_remain = None if secondary_used is None else max(0.0, 100.0 - float(secondary_used))
    return {
        "limit_id": row.get("limit_id"),
        "limit_name": row.get("limit_name"),
        "primary_window_minutes": None if row.get("primary_window_minutes") is None else float(row.get("primary_window_minutes")),
        "primary_used_percent": None if primary_used is None else float(primary_used),
        "primary_remaining_percent": primary_remain,
        "secondary_used_percent": None if secondary_used is None else float(secondary_used),
        "secondary_remaining_percent": secondary_remain,
    }


def query_top_projects_today(conn: Any, tz_name: str, top_n: int = 5) -> list[dict[str, Any]]:
    sql = """
        SELECT
            e.project,
            COUNT(*) AS requests,
            COALESCE(SUM(e.input_tokens), 0) AS input_tokens,
            COALESCE(SUM(e.output_tokens), 0) AS output_tokens,
            COALESCE(
                SUM(
                    (COALESCE(p.input_per_mtok, 0) * e.input_tokens / 1000000.0)
                  + (COALESCE(p.output_per_mtok, 0) * e.output_tokens / 1000000.0)
                ),
                0
            ) AS cost
        FROM usage_events e
        LEFT JOIN LATERAL (
            SELECT pr.input_per_mtok, pr.output_per_mtok
            FROM pricing pr
            WHERE pr.model = e.model
              AND pr.effective_from <= e.ts
            ORDER BY pr.effective_from DESC
            LIMIT 1
        ) p ON TRUE
        WHERE e.ts >= date_trunc('day', NOW() AT TIME ZONE %(tz)s) AT TIME ZONE %(tz)s
          AND e.ts < (date_trunc('day', NOW() AT TIME ZONE %(tz)s) + INTERVAL '1 day') AT TIME ZONE %(tz)s
        GROUP BY e.project
        ORDER BY cost DESC, requests DESC
        LIMIT %(top_n)s
    """
    with get_cursor(conn) as cur:
        cur.execute(sql, {"tz": tz_name, "top_n": top_n})
        return cur.fetchall()


def get_auto_session(conn: Any, project: str | None = None) -> str | None:
    params: dict[str, Any] = {}
    project_filter = ""
    if project:
        project_filter = "AND project = %(project)s"
        params["project"] = project
    sql = f"""
        SELECT session_id
        FROM usage_events
        WHERE session_id IS NOT NULL
          {project_filter}
        GROUP BY session_id
        ORDER BY MAX(ts) DESC
        LIMIT 1
    """
    with get_cursor(conn) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row["session_id"] if row else None


def render_kpi_table(today: dict[str, Any], recent: dict[str, Any], recent_minutes: int = 5) -> Table:
    table = Table(title="Usage KPI", expand=True)
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    req_per_min = int((recent["requests"] or 0) / max(1, recent_minutes))
    in_tok_per_min = int((recent["input_tokens"] or 0) / max(1, recent_minutes))
    out_tok_per_min = int((recent["output_tokens"] or 0) / max(1, recent_minutes))
    cost_per_hour = parse_money(recent["cost"]) * Decimal(60 / max(1, recent_minutes))

    table.add_row(f"Recent req/min ({recent_minutes}m)", fmt_int(req_per_min))
    table.add_row("Recent in_tok/min", fmt_int(in_tok_per_min))
    table.add_row("Recent out_tok/min", fmt_int(out_tok_per_min))
    table.add_row("Recent cost/hour", fmt_money(cost_per_hour))
    table.add_section()
    table.add_row("Today requests", fmt_int(today["requests"]))
    table.add_row("Today input_tokens", fmt_int(today["input_tokens"]))
    table.add_row("Today output_tokens", fmt_int(today["output_tokens"]))
    table.add_row("Today total cost", fmt_money(today["cost"]))
    return table


def render_projects_table(rows: Iterable[dict[str, Any]]) -> Table:
    table = Table(title="Top Projects (Today)", expand=True)
    table.add_column("Project")
    table.add_column("Req", justify="right")
    table.add_column("Input", justify="right")
    table.add_column("Output", justify="right")
    table.add_column("Cost", justify="right")
    added = False
    for row in rows:
        added = True
        table.add_row(
            row["project"] or "-",
            fmt_int(row["requests"]),
            fmt_int(row["input_tokens"]),
            fmt_int(row["output_tokens"]),
            fmt_money(row["cost"]),
        )
    if not added:
        table.add_row("-", "0", "0", "0", "$0.000000")
    return table


def render_events_table(rows: Iterable[dict[str, Any]]) -> Table:
    table = Table(title="Recent Events", expand=True)
    table.add_column("Time")
    table.add_column("Session")
    table.add_column("Project")
    table.add_column("Model")
    table.add_column("In", justify="right")
    table.add_column("Out", justify="right")
    table.add_column("Latency(ms)", justify="right")
    table.add_column("Cost", justify="right")
    added = False
    for row in rows:
        added = True
        table.add_row(
            row["ts"],
            row["session_id"] or "-",
            row["project"] or "-",
            row["model"],
            fmt_int(row["input_tokens"]),
            fmt_int(row["output_tokens"]),
            fmt_int(row["latency_ms"]),
            fmt_money(row["cost"]),
        )
    if not added:
        table.add_row("-", "-", "-", "-", "0", "0", "-", "$0.000000")
    return table


def build_live_layout(
    cfg: AppConfig,
    conn: Any,
    project: str | None,
    session_id: str | None,
    recent_minutes: int = 5,
) -> Layout:
    today = query_today_summary(conn, cfg.local_tz, project=project, session_id=session_id)
    recent = query_recent_rate(conn, recent_minutes, project=project, session_id=session_id)
    top_projects = query_top_projects_today(conn, cfg.local_tz, top_n=5)
    events = query_recent_events(conn, cfg.local_tz, limit=20, project=project, session_id=session_id)

    title = f"cc-usage live | tz={cfg.local_tz}"
    if project:
        title += f" | project={project}"
    if session_id:
        title += f" | session={session_id}"

    layout = Layout(name="root")
    layout.split_column(
        Layout(Panel(render_kpi_table(today, recent, recent_minutes), title=title), size=14),
        Layout(Panel(render_projects_table(top_projects))),
        Layout(Panel(render_events_table(events)), size=18),
    )
    return layout


def process_source_once(
    conn: Any,
    source_file: Path,
    source_key: str,
    project: str,
    default_model: str | None,
    with_rollup: bool,
    offset: int | None,
    from_start: bool,
    session_hint: str | None,
) -> tuple[int, int, int, int]:
    with get_cursor(conn) as cur:
        if offset is None:
            saved = get_saved_offset(cur, source_key)
            if saved is not None:
                offset = saved
            else:
                offset = 0 if from_start else source_file.stat().st_size
                upsert_offset(cur, source_key, offset)
                conn.commit()

        file_size = source_file.stat().st_size
        if file_size < offset:
            offset = 0

        parsed = 0
        inserted = 0
        skipped = 0
        with source_file.open("rb") as f:
            f.seek(offset)
            while True:
                raw = f.readline()
                if not raw:
                    break
                offset = f.tell()
                line = raw.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                event = parse_usage_event(line, project=project, default_model=default_model)
                if event is None:
                    skipped += 1
                    continue
                event = ensure_event_identity(event, source_key=source_key, raw_line=line, session_hint=session_hint)
                parsed += 1
                if insert_usage_event(cur, event, with_rollup=with_rollup):
                    inserted += 1

        upsert_offset(cur, source_key, offset)
        conn.commit()
        return offset, parsed, inserted, skipped


def resolve_source_file(source_file: str | None) -> tuple[Path | None, bool]:
    source_file = source_file or os.getenv("CC_USAGE_SOURCE_FILE", "").strip()
    if source_file:
        return Path(source_file).expanduser().resolve(), False
    return detect_latest_session_file(), True


def build_status_snapshot(
    conn: Any,
    cfg: AppConfig,
    project: str | None,
    session_id: str | None,
    source_key: str | None,
    collector_state: str,
    last_ingest_at: datetime | None,
    recent_minutes: int = 5,
) -> dict[str, Any]:
    today = query_today_summary(conn, cfg.local_tz, project=project, session_id=session_id)
    recent = query_recent_rate(conn, minutes=recent_minutes, project=project, session_id=session_id)
    rate_limits = query_latest_rate_limits(conn, project=project, session_id=session_id)
    manual_remaining = read_manual_remaining_percent()
    manual_reset_text = read_manual_reset_text()
    now_utc = datetime.now(timezone.utc)
    last_age = None
    if last_ingest_at is not None:
        last_age = max(0, int((now_utc - last_ingest_at).total_seconds()))
    return {
        "generated_at": now_utc.isoformat(),
        "project": project,
        "session_id": session_id,
        "source_file": source_key,
        "collector_state": collector_state,
        "last_ingest_age_sec": last_age,
        "recent_minutes": recent_minutes,
        "rate_limits": rate_limits,
        "manual_remaining_percent": manual_remaining,
        "manual_reset_text": manual_reset_text,
        "recent": {
            "requests": int(recent["requests"] or 0),
            "input_tokens": int(recent["input_tokens"] or 0),
            "output_tokens": int(recent["output_tokens"] or 0),
            "cost": float(parse_money(recent["cost"])),
        },
        "today": {
            "requests": int(today["requests"] or 0),
            "input_tokens": int(today["input_tokens"] or 0),
            "output_tokens": int(today["output_tokens"] or 0),
            "cost": float(parse_money(today["cost"])),
        },
    }


def format_status_line(status: dict[str, Any], fmt: str) -> str:
    recent = status.get("recent", {})
    today = status.get("today", {})
    minutes = int(status.get("recent_minutes", 5) or 5)
    req_per_min = int((recent.get("requests", 0) or 0) / max(1, minutes))
    in_per_min = int((recent.get("input_tokens", 0) or 0) / max(1, minutes))
    out_per_min = int((recent.get("output_tokens", 0) or 0) / max(1, minutes))
    today_total_tok = int(today.get("input_tokens", 0) or 0) + int(today.get("output_tokens", 0) or 0)
    today_cost = Decimal(str(today.get("cost", 0.0) or 0.0))
    state = status.get("collector_state", "UNKNOWN")
    rate_limits = status.get("rate_limits", {})
    manual_remaining = status.get("manual_remaining_percent")
    manual_reset_text = (status.get("manual_reset_text") or "").strip()
    primary_remaining = rate_limits.get("primary_remaining_percent")
    secondary_remaining = rate_limits.get("secondary_remaining_percent")
    remaining = manual_remaining
    if remaining is None:
        remaining = primary_remaining if primary_remaining is not None else secondary_remaining
    used_text = "N/A" if remaining is None else f"{max(0.0, 100.0 - float(remaining)):.0f}%"
    reset_part = "" if not manual_reset_text else f" 초기화:{manual_reset_text}"

    if fmt == "tmux":
        return (
            f"클로드5h사용:{used_text}{reset_part} R:{req_per_min}/m "
            f"I/O:{compact_int(in_per_min)}/{compact_int(out_per_min)} T:{compact_int(today_total_tok)}"
        )
    if fmt == "zsh":
        return f"클로드5h사용:{used_text}{reset_part} R:{req_per_min}/m T:{compact_int(today_total_tok)}"

    base = (
        f"ClaudeCode R:{req_per_min}/m I/O:{compact_int(in_per_min)}/{compact_int(out_per_min)} "
        f"T:{compact_int(today_total_tok)} 클로드5시간사용:{used_text}{reset_part} ${today_cost:.4f} SRC:{state}"
    )
    if fmt == "json":
        return json.dumps(status, ensure_ascii=False)
    return base


def run_collector_iteration(
    conn: Any,
    source_path: Path,
    source_key: str,
    project_name: str,
    default_model_name: str,
    with_rollup: bool,
    offset: int | None,
    from_start: bool,
    session_hint: str | None,
) -> tuple[int, CollectorResult]:
    offset, parsed, inserted, skipped = process_source_once(
        conn=conn,
        source_file=source_path,
        source_key=source_key,
        project=project_name,
        default_model=default_model_name,
        with_rollup=with_rollup,
        offset=offset,
        from_start=from_start,
        session_hint=session_hint,
    )
    return offset, CollectorResult(
        source_key=source_key,
        offset=offset,
        parsed=parsed,
        inserted=inserted,
        skipped=skipped,
    )


def detect_latest_claude_session_file() -> Path | None:
    candidates = glob(os.path.expanduser("~/.claude/projects/**/*.jsonl"), recursive=True)
    if not candidates:
        return None
    latest = max(candidates, key=lambda p: os.path.getmtime(p))
    return Path(latest).resolve()


def detect_latest_codex_session_file() -> Path | None:
    candidates = glob(os.path.expanduser("~/.codex/sessions/*/*/*/*.jsonl"))
    if not candidates:
        return None
    latest = max(candidates, key=lambda p: os.path.getmtime(p))
    return Path(latest).resolve()


def detect_latest_session_file() -> Path | None:
    # Default to Claude Code sessions only.
    claude_latest = detect_latest_claude_session_file()
    if claude_latest is not None:
        return claude_latest
    if os.getenv("CC_USAGE_ALLOW_CODEX_FALLBACK", "0") == "1":
        return detect_latest_codex_session_file()
    return None


def command_collect(
    cfg: AppConfig,
    source_file: str | None,
    project: str | None,
    default_model: str | None,
    interval: float,
    from_start: bool,
    once: bool,
    no_rollup: bool,
) -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    source_path, auto_source = resolve_source_file(source_file)
    if source_path is None:
        raise RuntimeError("No source file found. Use --source-file or create a Claude/Codex session log.")
    source_key = str(source_path)
    project_name = project or os.getenv("CC_USAGE_PROJECT", "default")
    default_model_name = default_model or os.getenv("CC_USAGE_DEFAULT_MODEL", "claude-opus-4-6")
    session_hint = source_path.stem.rsplit("-", 1)[-1] if source_path.suffix == ".jsonl" else None

    console.print(f"[bold]collector[/bold] source={source_key}")
    console.print(
        f"project={project_name} | default_model={default_model_name} | rollup={'off' if no_rollup else 'on'}"
    )

    with connect(cfg) as conn:
        offset: int | None = None
        while not STOP:
            if auto_source:
                latest = detect_latest_session_file()
                if latest and str(latest) != source_key:
                    source_path = latest
                    source_key = str(source_path)
                    session_hint = source_path.stem.rsplit("-", 1)[-1] if source_path.suffix == ".jsonl" else None
                    offset = None
                    console.print(f"[cyan]source switched:[/cyan] {source_key}")

            if not source_path.exists():
                console.print(f"[yellow]source not found:[/yellow] {source_key}")
                if once:
                    return 1
                time.sleep(max(interval, 0.5))
                continue

            offset, result = run_collector_iteration(
                conn=conn,
                source_path=source_path,
                source_key=source_key,
                project_name=project_name,
                default_model_name=default_model_name,
                with_rollup=not no_rollup,
                offset=offset,
                from_start=from_start,
                session_hint=session_hint,
            )
            if result.parsed > 0 or result.inserted > 0:
                console.print(
                    f"{datetime.now().strftime('%H:%M:%S')} parsed={result.parsed} inserted={result.inserted} "
                    f"skipped={result.skipped} offset={result.offset}"
                )
            if once:
                break
            time.sleep(max(interval, 0.5))
    return 0


def command_daemon(
    cfg: AppConfig,
    source_file: str | None,
    project: str | None,
    default_model: str | None,
    interval: float,
    from_start: bool,
    no_rollup: bool,
    status_file: str | None,
    status_interval: float,
    usage_sync_interval: float,
    usage_tmux_session: str,
    usage_tmux_cwd: str | None,
    no_usage_probe: bool,
    once: bool,
) -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    acquire_single_instance_lock("cc-usage-daemon", f"daemon project={project or 'default'}")
    if not no_usage_probe:
        acquire_single_instance_lock("cc-usage-remaining-sync", f"daemon usage_session={usage_tmux_session}")

    source_path, auto_source = resolve_source_file(source_file)
    if source_path is None:
        raise RuntimeError("No source file found. Use --source-file or create a Claude/Codex session log.")
    source_key = str(source_path)
    project_name = project or os.getenv("CC_USAGE_PROJECT", "default")
    default_model_name = default_model or os.getenv("CC_USAGE_DEFAULT_MODEL", "claude-opus-4-6")
    session_hint = source_path.stem.rsplit("-", 1)[-1] if source_path.suffix == ".jsonl" else None
    status_path = Path(status_file).expanduser().resolve() if status_file else default_status_file()
    usage_cwd = usage_tmux_cwd or str(Path.home())
    session_id_filter = None

    console.print(f"[bold]daemon[/bold] source={source_key}")
    console.print(
        f"project={project_name} | status_file={status_path} | default_model={default_model_name} | rollup={'off' if no_rollup else 'on'}"
    )
    if no_usage_probe:
        console.print("usage_probe=off")
    else:
        console.print(
            f"usage_probe=on | usage_session={usage_tmux_session} | usage_interval={max(usage_sync_interval, 3.0):.1f}s"
        )

    with connect(cfg) as conn:
        offset: int | None = None
        last_ingest_at: datetime | None = None
        last_status_write = 0.0
        last_usage_sync = 0.0

        while not STOP:
            collector_state = "OK"

            if auto_source:
                latest = detect_latest_session_file()
                if latest and str(latest) != source_key:
                    source_path = latest
                    source_key = str(source_path)
                    session_hint = source_path.stem.rsplit("-", 1)[-1] if source_path.suffix == ".jsonl" else None
                    offset = None
                    console.print(f"[cyan]source switched:[/cyan] {source_key}")

            if not source_path.exists():
                collector_state = "MISSING"
            else:
                offset, result = run_collector_iteration(
                    conn=conn,
                    source_path=source_path,
                    source_key=source_key,
                    project_name=project_name,
                    default_model_name=default_model_name,
                    with_rollup=not no_rollup,
                    offset=offset,
                    from_start=from_start,
                    session_hint=session_hint,
                )
                if result.parsed > 0 or result.inserted > 0:
                    last_ingest_at = datetime.now(timezone.utc)

            now_ts = time.time()
            if not no_usage_probe and (now_ts - last_usage_sync >= max(usage_sync_interval, 3.0)):
                try:
                    ensure_usage_tmux_session(usage_tmux_session, usage_cwd)
                    remaining_percent, reset_text = tmux_request_usage_info(usage_tmux_session)
                    if remaining_percent is not None:
                        write_manual_remaining_percent(remaining_percent, reset_text=reset_text)
                except Exception:
                    # Keep daemon alive even when tmux/claude probe fails transiently.
                    pass
                last_usage_sync = now_ts

            if now_ts - last_status_write >= max(status_interval, 0.5):
                status_snapshot = build_status_snapshot(
                    conn=conn,
                    cfg=cfg,
                    project=project_name,
                    session_id=session_id_filter,
                    source_key=source_key,
                    collector_state=collector_state,
                    last_ingest_at=last_ingest_at,
                )
                atomic_write_text(status_path, json.dumps(status_snapshot, ensure_ascii=False, indent=2))
                last_status_write = now_ts

            if once:
                break
            time.sleep(max(interval, 0.5))
    return 0


def command_status(
    cfg: AppConfig,
    fmt: str,
    project: str | None,
    session_id: str | None,
    status_file: str | None,
    from_db: bool,
) -> int:
    status_path = Path(status_file).expanduser().resolve() if status_file else default_status_file()
    if not from_db and status_path.exists():
        payload = json.loads(status_path.read_text(encoding="utf-8"))
        manual_remaining = read_manual_remaining_percent()
        manual_reset_text = read_manual_reset_text()
        if manual_remaining is not None:
            payload["manual_remaining_percent"] = manual_remaining
        if manual_reset_text:
            payload["manual_reset_text"] = manual_reset_text
        console.print(format_status_line(payload, fmt))
        return 0

    with connect(cfg) as conn:
        payload = build_status_snapshot(
            conn=conn,
            cfg=cfg,
            project=project,
            session_id=session_id,
            source_key=None,
            collector_state="DB",
            last_ingest_at=None,
        )
    console.print(format_status_line(payload, fmt))
    return 0


def command_set_remaining(percent: float) -> int:
    if percent < 0 or percent > 100:
        raise RuntimeError("--percent must be between 0 and 100")
    path = write_manual_remaining_percent(percent)
    console.print(f"manual remaining updated: {percent:.0f}% ({path})")
    return 0


def command_set_remaining_from_clipboard() -> int:
    try:
        text = subprocess.check_output(["pbpaste"], text=True, stderr=subprocess.DEVNULL)
    except Exception as exc:
        raise RuntimeError(f"failed to read clipboard: {exc}") from exc
    m = re.search(r"(\d{1,3})\s*%", text)
    if not m:
        raise RuntimeError("no percent value found in clipboard text")
    percent = float(m.group(1))
    return command_set_remaining(percent)


def extract_usage_info_from_text(text: str) -> tuple[float | None, str | None]:
    clean_text = ANSI_RE.sub("", text).replace("\r", "")
    lines = clean_text.splitlines()
    for i, line in enumerate(lines):
        if "current session" in line.lower():
            used_percent: float | None = None
            reset_text: str | None = None
            for j in range(i + 1, min(i + 14, len(lines))):
                lower_line = lines[j].lower()
                if "current week" in lower_line:
                    break
                m = re.search(r"(?<!\d)(100|[1-9]?\d)\s*%\s*used", lines[j], flags=re.IGNORECASE)
                if m and used_percent is None:
                    used = float(m.group(1))
                    used_percent = used
                r = re.search(r"^\s*Resets\s+(.+?)\s*$", lines[j], flags=re.IGNORECASE)
                if r:
                    reset_text = r.group(1).strip()
            if used_percent is not None:
                return max(0.0, 100.0 - used_percent), reset_text
    return None, None


def tmux_has_session(name: str) -> bool:
    proc = subprocess.run(["tmux", "has-session", "-t", name], capture_output=True, text=True)
    return proc.returncode == 0


def ensure_usage_tmux_session(name: str, cwd: str) -> None:
    if tmux_has_session(name):
        return
    subprocess.run(["tmux", "new-session", "-d", "-s", name, "-c", cwd, "claude"], check=False)
    time.sleep(1.0)


def tmux_request_usage_info(name: str, settle_sec: float = 1.4) -> tuple[float | None, str | None]:
    if not tmux_has_session(name):
        return None, None
    settle = max(settle_sec, 0.6)

    def capture_text() -> str:
        proc = subprocess.run(["tmux", "capture-pane", "-pt", name, "-S", "-260"], capture_output=True, text=True)
        return proc.stdout if proc.returncode == 0 else ""

    def maybe_accept_trust_prompt(text: str) -> str:
        lower = text.lower()
        if "quick safety check" in lower and "yes, i trust this folder" in lower:
            subprocess.run(["tmux", "send-keys", "-t", name, "Enter"], check=False)
            time.sleep(settle)
            return capture_text()
        return text

    for _ in range(3):
        # Normalize state before opening /usage dialog.
        subprocess.run(["tmux", "send-keys", "-t", name, "Escape"], check=False)
        time.sleep(0.1)
        _ = maybe_accept_trust_prompt(capture_text())
        subprocess.run(["tmux", "send-keys", "-t", name, "/usage", "Enter"], check=False)
        menu_confirmed = False
        deadline = time.time() + max(5.0, settle * 4.0)

        while time.time() < deadline:
            time.sleep(0.35)
            text = maybe_accept_trust_prompt(capture_text())

            # If slash command menu opened instead of executing, confirm selection once.
            if "Show plan usage limits" in text and "Current session" not in text and not menu_confirmed:
                subprocess.run(["tmux", "send-keys", "-t", name, "Enter"], check=False)
                menu_confirmed = True
                continue

            remaining, reset_text = extract_usage_info_from_text(text)
            if remaining is not None:
                subprocess.run(["tmux", "send-keys", "-t", name, "Escape"], check=False)
                return remaining, reset_text

    # Final best-effort parse of whatever is visible.
    text = capture_text()
    remaining, reset_text = extract_usage_info_from_text(text)
    subprocess.run(["tmux", "send-keys", "-t", name, "Escape"], check=False)
    return remaining, reset_text


def estimate_remaining_percent() -> float | None:
    # Conservative fallback: keep latest manual value when direct /usage parse is unavailable.
    return read_manual_remaining_percent()


def command_sync_remaining(
    interval: float,
    tmux_session: str,
    tmux_cwd: str | None,
    no_tmux_bootstrap: bool,
) -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    acquire_single_instance_lock("cc-usage-remaining-sync", f"sync-remaining session={tmux_session}")
    cwd = tmux_cwd or str(Path.home())
    if not no_tmux_bootstrap:
        ensure_usage_tmux_session(tmux_session, cwd)

    console.print(f"[bold]sync-remaining[/bold] session={tmux_session} interval={interval:.1f}s")
    while not STOP:
        percent, reset_text = tmux_request_usage_info(tmux_session)
        source = "tmux:/usage"
        if percent is None:
            percent = estimate_remaining_percent()
            reset_text = read_manual_reset_text()
            source = "estimate"
        if percent is not None:
            write_manual_remaining_percent(percent, reset_text=reset_text)
            reset_suffix = f", resets {reset_text}" if reset_text else ""
            console.print(f"[cyan]remaining[/cyan] {percent:.0f}% ({source}{reset_suffix})")
        else:
            console.print("[yellow]remaining unavailable[/yellow] (/usage parse failed)")
        time.sleep(max(interval, 3.0))
    return 0


def command_today(cfg: AppConfig, project: str | None, session_id: str | None) -> int:
    with connect(cfg) as conn:
        summary = query_today_summary(conn, cfg.local_tz, project=project, session_id=session_id)
        top_projects = query_top_projects_today(conn, cfg.local_tz, top_n=10)
    console.print("[bold]Today Summary[/bold]")
    if project:
        console.print(f"Project filter: {project}")
    if session_id:
        console.print(f"Session filter: {session_id}")
    table = Table(expand=True)
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Requests", fmt_int(summary["requests"]))
    table.add_row("Input tokens", fmt_int(summary["input_tokens"]))
    table.add_row("Output tokens", fmt_int(summary["output_tokens"]))
    table.add_row("Cost", fmt_money(summary["cost"]))
    console.print(table)
    console.print(render_projects_table(top_projects))
    return 0


def command_session(cfg: AppConfig, session_id: str) -> int:
    with connect(cfg) as conn:
        summary = query_today_summary(conn, cfg.local_tz, session_id=session_id)
        events = query_recent_events(conn, cfg.local_tz, limit=30, session_id=session_id)
    console.print(f"[bold]Session[/bold] {session_id}")
    table = Table(expand=True)
    table.add_column("Metric")
    table.add_column("Value", justify="right")
    table.add_row("Today requests", fmt_int(summary["requests"]))
    table.add_row("Today input tokens", fmt_int(summary["input_tokens"]))
    table.add_row("Today output tokens", fmt_int(summary["output_tokens"]))
    table.add_row("Today cost", fmt_money(summary["cost"]))
    console.print(table)
    console.print(render_events_table(events))
    return 0


def command_live(cfg: AppConfig, project: str | None, session_id: str | None, interval: float) -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    with connect(cfg) as conn:
        local_session = session_id or get_auto_session(conn, project=project)
        with Live(
            build_live_layout(cfg, conn, project=project, session_id=local_session),
            refresh_per_second=4,
            console=console,
            transient=False,
        ) as live:
            while not STOP:
                live.update(build_live_layout(cfg, conn, project=project, session_id=local_session))
                time.sleep(max(interval, 0.5))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Claude Code usage terminal monitor (PostgreSQL)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_db = subparsers.add_parser("init-db", help="apply schema.sql")
    init_db.add_argument("--schema", help="custom schema sql path")

    subparsers.add_parser("doctor", help="verify DB connectivity and tables")

    today = subparsers.add_parser("today", help="show today's usage summary")
    today.add_argument("--project", help="project filter")
    today.add_argument("--session", help="session filter")

    live = subparsers.add_parser("live", help="show live terminal dashboard")
    live.add_argument("--project", help="project filter")
    live.add_argument("--session", help="session filter")
    live.add_argument("--interval", type=float, default=2.0, help="polling interval in seconds")

    session = subparsers.add_parser("session", help="show session summary")
    session.add_argument("session_id")

    collect = subparsers.add_parser("collect", help="tail usage log and ingest events continuously")
    collect.add_argument("--source-file", help="log file path (or CC_USAGE_SOURCE_FILE)")
    collect.add_argument("--project", help="project name override")
    collect.add_argument("--default-model", help="fallback model when log line has no model")
    collect.add_argument("--interval", type=float, default=1.0, help="polling interval in seconds")
    collect.add_argument("--from-start", action="store_true", help="read from file start on first run")
    collect.add_argument("--once", action="store_true", help="process current unread chunk once and exit")
    collect.add_argument("--no-rollup", action="store_true", help="disable minute rollup upsert")

    daemon = subparsers.add_parser("daemon", help="run integrated collector + status cache updater")
    daemon.add_argument("--source-file", help="log file path (or CC_USAGE_SOURCE_FILE)")
    daemon.add_argument("--project", help="project name override")
    daemon.add_argument("--default-model", help="fallback model when log line has no model")
    daemon.add_argument("--interval", type=float, default=1.0, help="collector polling interval in seconds")
    daemon.add_argument("--from-start", action="store_true", help="read from file start on first run")
    daemon.add_argument("--no-rollup", action="store_true", help="disable minute rollup upsert")
    daemon.add_argument("--status-file", help="status cache output path (default: ~/.cache/cc-usage/status.json)")
    daemon.add_argument("--status-interval", type=float, default=1.0, help="status file refresh interval in seconds")
    daemon.add_argument(
        "--usage-sync-interval", type=float, default=20.0, help="Claude /usage polling interval in seconds"
    )
    daemon.add_argument("--usage-tmux-session", default="cc-usage-probe", help="tmux session name for /usage polling")
    daemon.add_argument("--usage-tmux-cwd", help="cwd for auto-created usage probe tmux session")
    daemon.add_argument("--no-usage-probe", action="store_true", help="disable tmux /usage probe in daemon")
    daemon.add_argument("--once", action="store_true", help="run one collection/status cycle and exit")

    status = subparsers.add_parser("status", help="print one-line status for terminal status bars")
    status.add_argument("--format", choices=["plain", "tmux", "zsh", "json"], default="plain")
    status.add_argument("--project", help="project filter (DB fallback mode)")
    status.add_argument("--session", help="session filter (DB fallback mode)")
    status.add_argument("--status-file", help="status cache path (default: ~/.cache/cc-usage/status.json)")
    status.add_argument("--from-db", action="store_true", help="ignore cache file and query DB directly")

    set_remaining = subparsers.add_parser(
        "set-remaining", help="set Claude 5-hour remaining percent manually (from /usage)"
    )
    set_group = set_remaining.add_mutually_exclusive_group(required=True)
    set_group.add_argument("--percent", type=float, help="remaining percent, e.g. 71")
    set_group.add_argument("--from-clipboard", action="store_true", help="read first %% value from clipboard text")

    sync_remaining = subparsers.add_parser(
        "sync-remaining", help="hybrid sync: periodic /usage parse in tmux + fallback to estimate"
    )
    sync_remaining.add_argument("--interval", type=float, default=30.0, help="sync interval seconds")
    sync_remaining.add_argument("--tmux-session", default="cc-usage-sync", help="tmux session name for /usage polling")
    sync_remaining.add_argument("--tmux-cwd", help="cwd for auto-created tmux session")
    sync_remaining.add_argument("--no-tmux-bootstrap", action="store_true", help="do not auto-create tmux session")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        cfg = load_config()
        if args.command == "init-db":
            return command_init_db(cfg, args.schema)
        if args.command == "doctor":
            return command_doctor(cfg)
        if args.command == "today":
            return command_today(cfg, args.project, args.session)
        if args.command == "live":
            return command_live(cfg, args.project, args.session, args.interval)
        if args.command == "session":
            return command_session(cfg, args.session_id)
        if args.command == "collect":
            return command_collect(
                cfg,
                source_file=args.source_file,
                project=args.project,
                default_model=args.default_model,
                interval=args.interval,
                from_start=args.from_start,
                once=args.once,
                no_rollup=args.no_rollup,
            )
        if args.command == "daemon":
            return command_daemon(
                cfg,
                source_file=args.source_file,
                project=args.project,
                default_model=args.default_model,
                interval=args.interval,
                from_start=args.from_start,
                no_rollup=args.no_rollup,
                status_file=args.status_file,
                status_interval=args.status_interval,
                usage_sync_interval=args.usage_sync_interval,
                usage_tmux_session=args.usage_tmux_session,
                usage_tmux_cwd=args.usage_tmux_cwd,
                no_usage_probe=args.no_usage_probe,
                once=args.once,
            )
        if args.command == "status":
            return command_status(
                cfg,
                fmt=args.format,
                project=args.project,
                session_id=args.session,
                status_file=args.status_file,
                from_db=args.from_db,
            )
        if args.command == "set-remaining":
            if args.from_clipboard:
                return command_set_remaining_from_clipboard()
            return command_set_remaining(args.percent)
        if args.command == "sync-remaining":
            return command_sync_remaining(
                interval=args.interval,
                tmux_session=args.tmux_session,
                tmux_cwd=args.tmux_cwd,
                no_tmux_bootstrap=args.no_tmux_bootstrap,
            )
        console.print(f"[red]unknown command:[/red] {args.command}")
        return 1
    except KeyboardInterrupt:
        return 130
    except Exception as exc:  # noqa: BLE001
        console.print(f"[red]error:[/red] {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
