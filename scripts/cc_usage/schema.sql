CREATE TABLE IF NOT EXISTS usage_events (
    id BIGSERIAL PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    session_id TEXT,
    project TEXT NOT NULL DEFAULT 'default',
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    request_id TEXT,
    latency_ms INTEGER CHECK (latency_ms IS NULL OR latency_ms >= 0),
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_events_request_model
ON usage_events (request_id, model)
WHERE request_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_usage_events_ts ON usage_events (ts DESC);
CREATE INDEX IF NOT EXISTS idx_usage_events_session_ts ON usage_events (session_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_usage_events_project_ts ON usage_events (project, ts DESC);

CREATE TABLE IF NOT EXISTS pricing (
    model TEXT NOT NULL,
    effective_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    input_per_mtok NUMERIC(12, 6) NOT NULL CHECK (input_per_mtok >= 0),
    output_per_mtok NUMERIC(12, 6) NOT NULL CHECK (output_per_mtok >= 0),
    PRIMARY KEY (model, effective_from)
);

CREATE TABLE IF NOT EXISTS usage_rollup_minute (
    bucket_minute TIMESTAMPTZ NOT NULL,
    project TEXT NOT NULL,
    model TEXT NOT NULL,
    requests INTEGER NOT NULL CHECK (requests >= 0),
    input_tokens BIGINT NOT NULL CHECK (input_tokens >= 0),
    output_tokens BIGINT NOT NULL CHECK (output_tokens >= 0),
    cost NUMERIC(14, 6) NOT NULL CHECK (cost >= 0),
    PRIMARY KEY (bucket_minute, project, model)
);

CREATE INDEX IF NOT EXISTS idx_usage_rollup_minute_bucket_desc
ON usage_rollup_minute (bucket_minute DESC);

CREATE TABLE IF NOT EXISTS collector_offsets (
    source_file TEXT PRIMARY KEY,
    file_offset BIGINT NOT NULL DEFAULT 0 CHECK (file_offset >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
