BEGIN;

CREATE SCHEMA IF NOT EXISTS app_meta;
CREATE TABLE IF NOT EXISTS app_meta.catalog_imports (
    repository TEXT NOT NULL,
    tag TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    dump_sha256 TEXT NOT NULL CHECK (dump_sha256 ~ '^[a-f0-9]{64}$'),
    snapshot_at TIMESTAMPTZ NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    configurations BIGINT NOT NULL CHECK (configurations >= 0),
    inserted BIGINT NOT NULL CHECK (inserted >= 0),
    PRIMARY KEY (repository, tag)
);

CREATE TABLE IF NOT EXISTS app_meta.proxy_test_state (
    proxy_id BIGINT PRIMARY KEY REFERENCES public.proxies(proxy_id),
    next_test_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 UTC',
    checked_at TIMESTAMPTZ,
    quality_checked_at TIMESTAMPTZ,
    last_response_at TIMESTAMPTZ,
    latency_ms DOUBLE PRECISION CHECK (latency_ms >= 0 AND latency_ms < 'Infinity'::float8),
    failure_streak INTEGER NOT NULL DEFAULT 0 CHECK (failure_streak >= 0),
    last_error TEXT
);
CREATE INDEX IF NOT EXISTS proxy_test_due_idx
    ON app_meta.proxy_test_state(next_test_at, proxy_id);
CREATE INDEX IF NOT EXISTS proxy_quality_due_idx
    ON app_meta.proxy_test_state(quality_checked_at ASC NULLS FIRST,proxy_id)
    WHERE last_response_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS proxy_test_responded_idx
    ON app_meta.proxy_test_state(last_response_at DESC) WHERE last_response_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS app_meta.proxy_observation_imports (
    journal_sha256 BYTEA PRIMARY KEY CHECK (octet_length(journal_sha256) = 32),
    run_id TEXT NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    observations BIGINT NOT NULL CHECK (observations >= 0)
);

COMMIT;
