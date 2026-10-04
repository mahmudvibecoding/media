BEGIN;

CREATE TABLE IF NOT EXISTS app_meta.proxy_round_results (
    round_id UUID NOT NULL,
    proxy_id BIGINT NOT NULL REFERENCES public.proxies(proxy_id),
    passes SMALLINT NOT NULL CHECK (passes BETWEEN 1 AND 7),
    response_count SMALLINT NOT NULL CHECK (response_count BETWEEN 0 AND bit_count(passes::integer::bit(3))),
    response_time_ms DOUBLE PRECISION NOT NULL CHECK (response_time_ms >= 0 AND response_time_ms < 'Infinity'::float8),
    checked_at TIMESTAMPTZ NOT NULL,
    last_response_at TIMESTAMPTZ,
    working_protocol TEXT,
    last_http_status SMALLINT CHECK (last_http_status BETWEEN 100 AND 599),
    PRIMARY KEY (round_id,proxy_id),
    CHECK ((response_count > 0) = (last_response_at IS NOT NULL)
       AND (response_count > 0) = (working_protocol IS NOT NULL)
       AND (response_count > 0) = (last_http_status IS NOT NULL)),
    CHECK (response_count > 0 OR response_time_ms = 0)
);

CREATE TABLE IF NOT EXISTS app_meta.proxy_pool_results (
    proxy_id BIGINT PRIMARY KEY REFERENCES public.proxies(proxy_id),
    round_id UUID NOT NULL,
    response_count SMALLINT NOT NULL CHECK (response_count BETWEEN 0 AND 3),
    average_response_ms DOUBLE PRECISION CHECK (average_response_ms >= 0 AND average_response_ms < 'Infinity'::float8),
    completed_at TIMESTAMPTZ NOT NULL,
    last_response_at TIMESTAMPTZ,
    working_protocol TEXT,
    last_http_status SMALLINT CHECK (last_http_status BETWEEN 100 AND 599),
    CHECK ((response_count > 0) = (average_response_ms IS NOT NULL)
       AND (response_count > 0) = (last_response_at IS NOT NULL)
       AND (response_count > 0) = (working_protocol IS NOT NULL)
       AND (response_count > 0) = (last_http_status IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS proxy_pool_response_rank_idx
    ON app_meta.proxy_pool_results(response_count DESC,average_response_ms,proxy_id)
    WHERE response_count > 0;

COMMENT ON TABLE app_meta.proxy_round_results IS
    'Three manual test passes per run. Pass bits make saved batch replay idempotent. Any verified YouTube HTTP response counts.';
COMMENT ON TABLE app_meta.proxy_pool_results IS
    'Latest completed three-check result per proxy. Rank by response count descending then average elapsed time of responding checks ascending. Zero removes a proxy from the pool.';

COMMIT;
