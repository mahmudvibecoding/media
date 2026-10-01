BEGIN;

CREATE TABLE public.proxy_test_runs (
    test_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_key TEXT NOT NULL UNIQUE,
    metadata JSONB NOT NULL,
    summary JSONB NOT NULL,
    source_host TEXT NOT NULL,
    journal_sha256 BYTEA,
    status TEXT NOT NULL CHECK (status IN ('importing', 'complete')),
    imported_count BIGINT NOT NULL DEFAULT 0,
    imported_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.proxy_test_results (
    test_run_id BIGINT NOT NULL REFERENCES public.proxy_test_runs(test_run_id),
    proxy_id BIGINT NOT NULL REFERENCES public.proxies(proxy_id),
    checked_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    attempted BOOLEAN NOT NULL,
    responds BOOLEAN NOT NULL,
    declared_protocol TEXT NOT NULL,
    detected_protocol TEXT,
    http_status SMALLINT,
    total_ms DOUBLE PRECISION NOT NULL,
    attempts JSONB NOT NULL,
    PRIMARY KEY (test_run_id, proxy_id)
);

CREATE TABLE public.proxy_test_health (
    proxy_id BIGINT PRIMARY KEY REFERENCES public.proxies(proxy_id),
    test_run_id BIGINT NOT NULL REFERENCES public.proxy_test_runs(test_run_id),
    checked_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL,
    attempted BOOLEAN NOT NULL,
    responds BOOLEAN NOT NULL,
    detected_protocol TEXT,
    http_status SMALLINT,
    total_ms DOUBLE PRECISION NOT NULL,
    first_response_at TIMESTAMPTZ,
    last_response_at TIMESTAMPTZ,
    checks INTEGER NOT NULL,
    network_checks INTEGER NOT NULL,
    successful_checks INTEGER NOT NULL
);
CREATE INDEX proxy_test_health_responds_idx ON public.proxy_test_health(proxy_id) WHERE responds;

CREATE VIEW public.proxy_health AS
SELECT p.proxy_id, p.address, p.port, p.protocol AS declared_protocol,
       h.detected_protocol, h.checked_at, h.status, h.attempted,
       h.responds AS youtube_responds, h.http_status, h.total_ms,
       h.first_response_at, h.last_response_at, h.checks, h.network_checks, h.successful_checks,
       CASE WHEN h.responds AND h.successful_checks >= 2 THEN 'repeatedly_responding'
            WHEN h.responds THEN 'responding'
            WHEN h.last_response_at IS NOT NULL THEN 'intermittently_responding'
            ELSE h.status END AS availability
FROM public.proxies p JOIN public.proxy_test_health h USING (proxy_id);

COMMENT ON TABLE public.proxy_test_results IS
    'One saved result per configuration and test round. Any HTTP response through verified YouTube TLS counts as responds, including errors or bot challenges. Full bodies are read within the total deadline; body errors do not negate a verified response.';
COMMENT ON COLUMN public.proxies.tested_at IS
    'Latest timestamp of an individual attempted connectivity test. List downloads and configurations rejected before an attempt do not update this field.';
COMMENT ON TABLE public.proxies IS
    'Published proxy connection candidates. Identity includes address, port, declared protocol and settings. Consult proxy_health for tested reachability.';

GRANT SELECT ON public.proxy_test_runs, public.proxy_test_results,
    public.proxy_test_health, public.proxy_health TO media_viewer;

COMMIT;
