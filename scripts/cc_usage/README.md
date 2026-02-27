# cc-usage (PostgreSQL)

Claude Code usage terminal monitor for local macOS.

## 1) Install

```bash
cd /Users/ijinseong/Documents/golf/dy_golfcart_monitoring
python3 -m venv .venv-cc-usage
source .venv-cc-usage/bin/activate
pip install -r scripts/cc_usage/requirements.txt
```

## 2) Configure

Set `DATABASE_URL`:

```bash
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/cc_usage'
```

Optional:

- `LOCAL_TZ` (default: `Asia/Seoul`)

## 3) Initialize DB

```bash
python3 scripts/cc_usage/main.py init-db
```

## 4) Commands

```bash
# health check
python3 scripts/cc_usage/main.py doctor

# today's usage summary
python3 scripts/cc_usage/main.py today
python3 scripts/cc_usage/main.py today --project my-project

# live terminal dashboard
python3 scripts/cc_usage/main.py live
python3 scripts/cc_usage/main.py live --interval 2 --project my-project
python3 scripts/cc_usage/main.py live --session session-123

# specific session report
python3 scripts/cc_usage/main.py session session-123

# collector (real-time auto ingest + minute rollup)
python3 scripts/cc_usage/main.py collect --source-file ~/claude-code-usage.log
python3 scripts/cc_usage/main.py collect --source-file ~/claude-code-usage.log --from-start
python3 scripts/cc_usage/main.py collect --source-file ~/claude-code-usage.log --once
python3 scripts/cc_usage/main.py collect --source-file ~/.codex/sessions/2026/01/22/rollout-...jsonl --once
# if --source-file is omitted, latest ~/.codex/sessions/*/*/*/*.jsonl is auto-detected
python3 scripts/cc_usage/main.py collect

# integrated daemon (collector + status cache writer)
python3 scripts/cc_usage/main.py daemon --project codex-local
python3 scripts/cc_usage/main.py daemon --project codex-local --once

# one-line status (for tmux/zsh status bar)
python3 scripts/cc_usage/main.py status --format plain
python3 scripts/cc_usage/main.py status --format tmux
python3 scripts/cc_usage/main.py status --format zsh
```

Collector env options:

- `CC_USAGE_SOURCE_FILE` (if `--source-file` omitted)
- `CC_USAGE_PROJECT` (default project name)
- `CC_USAGE_DEFAULT_MODEL` (fallback model, default: `codex`)

## 5) Status Bar Integration

Run daemon in one terminal:

```bash
export DATABASE_URL='postgresql://localhost:5432/cc_usage'
python3 /Users/ijinseong/Documents/golf/dy_golfcart_monitoring/scripts/cc_usage/main.py daemon --project codex-local
```

tmux (`~/.tmux.conf`):

```tmux
set -g status-interval 2
set -g status-right '#(/usr/bin/env DATABASE_URL=postgresql://localhost:5432/cc_usage python3 /Users/ijinseong/Documents/golf/dy_golfcart_monitoring/scripts/cc_usage/main.py status --format tmux) | %H:%M'
```

zsh (`~/.zshrc`):

```bash
cc_usage_status() {
  DATABASE_URL='postgresql://localhost:5432/cc_usage' \
  python3 /Users/ijinseong/Documents/golf/dy_golfcart_monitoring/scripts/cc_usage/main.py status --format zsh 2>/dev/null
}
RPROMPT='$(cc_usage_status)'
```

## 6) Minimal seed data example

```sql
INSERT INTO pricing(model, effective_from, input_per_mtok, output_per_mtok)
VALUES ('claude-sonnet-4-5', NOW(), 3.00, 15.00);

INSERT INTO usage_events(ts, session_id, project, model, input_tokens, output_tokens, request_id, latency_ms)
VALUES
  (NOW(), 'sess-1', 'local-monitor', 'claude-sonnet-4-5', 1200, 450, 'req-1', 920),
  (NOW(), 'sess-1', 'local-monitor', 'claude-sonnet-4-5', 950, 380, 'req-2', 810);
```

## 7) Supported collector log line formats

JSON line example:

```json
{"timestamp":"2026-02-27T16:40:00Z","session_id":"sess-1","request_id":"req-1","model":"claude-sonnet-4-5","usage":{"input_tokens":1200,"output_tokens":450},"latency_ms":920}
```

Text line example:

```txt
model=claude-sonnet-4-5 input_tokens=1200 output_tokens=450 session_id=sess-1 request_id=req-1 latency_ms=920
```
