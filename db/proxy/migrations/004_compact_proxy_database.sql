-- Consolidate the existing catalog without retaining historical rows.
-- Run once, after deploying the updated collector/importer. All changes are atomic.
BEGIN;
SET LOCAL lock_timeout = '15s';
SET LOCAL work_mem = '128MB';
SET LOCAL jit = off;
SELECT pg_advisory_xact_lock(hashtextextended('proxy:proxy_collection',0));
SELECT pg_advisory_xact_lock(6389247650123);
LOCK TABLE public.proxies, public.proxy_test_health, public.proxy_test_results,
    public.proxy_test_runs, public.proxy_lists, public.proxy_list_entries,
    public.proxy_list_downloads, public.proxy_collection_runs,
    public.proxy_list_checks, public.proxy_sources IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.proxy_test_runs WHERE status <> 'complete' OR journal_sha256 IS NULL) THEN
        RAISE EXCEPTION 'Finish test imports before cleanup';
    END IF;
    IF EXISTS (SELECT 1 FROM public.proxy_collection_runs WHERE status = 'running') THEN
        RAISE EXCEPTION 'Stop the collector before cleanup';
    END IF;
END $$;
CREATE TABLE public.proxy_stats (
    proxy_id BIGINT PRIMARY KEY REFERENCES public.proxies(proxy_id),
    checked_at TIMESTAMPTZ NOT NULL,
    last_attempt_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    attempted BOOLEAN NOT NULL,
    responds BOOLEAN NOT NULL,
    detected_protocol TEXT,
    working_protocol TEXT,
    http_status SMALLINT CHECK (http_status BETWEEN 100 AND 599),
    total_ms DOUBLE PRECISION NOT NULL CHECK (total_ms >= 0),
    first_response_at TIMESTAMPTZ,
    last_response_at TIMESTAMPTZ,
    checks BIGINT NOT NULL CHECK (checks > 0),
    network_checks BIGINT NOT NULL CHECK (network_checks BETWEEN 0 AND checks),
    requests_sent BIGINT NOT NULL CHECK (requests_sent BETWEEN 0 AND network_checks),
    successful_checks BIGINT NOT NULL CHECK (successful_checks BETWEEN 0 AND requests_sent),
    last_import_key BYTEA NOT NULL CHECK (octet_length(last_import_key) = 32)
);
CREATE INDEX proxy_stats_responds_idx ON public.proxy_stats(proxy_id) WHERE responds;

CREATE TABLE public.proxy_lists_compact (
    url TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    protocol_hints TEXT[] NOT NULL DEFAULT '{}',
    format_hint TEXT,
    enabled BOOLEAN NOT NULL DEFAULT false,
    run_id UUID,
    status TEXT NOT NULL DEFAULT 'not_checked',
    fetched_at TIMESTAMPTZ,
    fetch_state JSONB NOT NULL DEFAULT '{}'::jsonb
);


-- The request counter and last working protocol are recovered before history is dropped.
CREATE TEMP TABLE cleanup_request_counts ON COMMIT DROP AS
SELECT proxy_id, sum(jsonb_array_length(jsonb_path_query_array(attempts,
       '$[*] ? (@.request_sent == true)')))::bigint AS requests_sent
FROM public.proxy_test_results GROUP BY proxy_id;
CREATE UNIQUE INDEX ON cleanup_request_counts(proxy_id);
ANALYZE cleanup_request_counts;
CREATE TEMP TABLE cleanup_working_protocols ON COMMIT DROP AS
SELECT DISTINCT ON (proxy_id) proxy_id, detected_protocol
FROM public.proxy_test_results WHERE responds
ORDER BY proxy_id, checked_at DESC, test_run_id DESC;
CREATE UNIQUE INDEX ON cleanup_working_protocols(proxy_id);
ANALYZE cleanup_working_protocols;

INSERT INTO public.proxy_stats
SELECT h.proxy_id,h.checked_at,p.tested_at,h.status,h.attempted,h.responds,
       h.detected_protocol,w.detected_protocol,h.http_status,h.total_ms,
       h.first_response_at,h.last_response_at,h.checks,h.network_checks,
       coalesce(r.requests_sent,0),h.successful_checks,t.journal_sha256
FROM public.proxy_test_health h JOIN public.proxies p USING(proxy_id)
JOIN public.proxy_test_runs t USING(test_run_id)
LEFT JOIN cleanup_request_counts r USING(proxy_id)
LEFT JOIN cleanup_working_protocols w USING(proxy_id);

INSERT INTO public.proxy_lists_compact
SELECT l.url,l.kind,l.protocol_hints,l.format_hint,
       (c.data_observed_without_account AND c.is_html=false
        AND l.kind IN ('feed_candidate','api_candidate')) IS TRUE,
       d.run_id,coalesce(d.status,c.response_status,'not_checked'),d.finished_at,
       coalesce(to_jsonb(d) - ARRAY['run_id','list_url','protocol_hints','status','finished_at'],'{}'::jsonb)
FROM public.proxy_lists l JOIN public.proxy_list_catalog c USING(url)
LEFT JOIN LATERAL (
    SELECT d.* FROM public.proxy_list_downloads d
    JOIN public.proxy_collection_runs r USING(run_id)
    WHERE d.list_url=l.url
    ORDER BY r.started_at DESC,d.finished_at DESC NULLS LAST LIMIT 1
) d ON true;

-- These assertions run before any history is deleted.
DO $$ BEGIN
    IF (SELECT count(*) FROM public.proxy_stats) <> (SELECT count(*) FROM public.proxy_test_health)
       OR EXISTS (
        SELECT 1 FROM public.proxy_test_health h JOIN public.proxy_stats s USING(proxy_id)
        JOIN public.proxies p USING(proxy_id)
        WHERE (h.checked_at,h.status,h.attempted,h.responds,h.detected_protocol,h.http_status,
               h.total_ms,h.first_response_at,h.last_response_at,h.checks,h.network_checks,
               h.successful_checks,p.tested_at) IS DISTINCT FROM
              (s.checked_at,s.status,s.attempted,s.responds,s.detected_protocol,s.http_status,
               s.total_ms,s.first_response_at,s.last_response_at,s.checks,s.network_checks,
               s.successful_checks,s.last_attempt_at)) THEN
        RAISE EXCEPTION 'Proxy statistics changed during consolidation';
    END IF;
    IF (SELECT count(*) FROM public.proxy_lists_compact) <> (SELECT count(*) FROM public.proxy_lists)
       OR EXISTS (SELECT 1 FROM public.proxy_lists l JOIN public.proxy_lists_compact n USING(url)
            WHERE (l.kind,l.protocol_hints,l.format_hint) IS DISTINCT FROM
                  (n.kind,n.protocol_hints,n.format_hint)) THEN
        RAISE EXCEPTION 'Source list identity changed';
    END IF;
    IF (SELECT sum(checks) FROM public.proxy_stats) IS DISTINCT FROM
       (SELECT sum(imported_count) FROM public.proxy_test_runs) THEN
        RAISE EXCEPTION 'Test totals differ from completed imports';
    END IF;
    IF EXISTS (SELECT 1 FROM public.proxy_stats WHERE last_response_at IS NOT NULL AND working_protocol IS NULL) THEN
        RAISE EXCEPTION 'Last working protocol was not recovered';
    END IF;
END $$;

DROP VIEW public.proxy_catalog, public.proxy_health, public.proxy_list_catalog;
DROP TABLE public.proxy_test_results, public.proxy_test_health;
DROP TABLE public.proxy_test_runs;
DROP TABLE public.proxy_list_entries, public.proxy_list_downloads, public.proxy_list_checks;
DROP TABLE public.proxy_collection_runs;
DROP TABLE public.proxy_lists;
DROP TABLE public.proxy_sources;
ALTER TABLE public.proxy_lists_compact RENAME TO proxy_lists;
ALTER TABLE public.proxy_lists RENAME CONSTRAINT proxy_lists_compact_pkey TO proxy_lists_pkey;
ALTER TABLE public.proxies DROP COLUMN tested_at;
DROP INDEX public.proxies_address_port_idx, public.proxies_protocol_idx;

CREATE VIEW public.proxy_catalog AS
SELECT p.proxy_id, p.address, p.port, p.protocol, p.first_seen_at, p.last_seen_at,
       s.last_attempt_at AS tested_at
FROM public.proxies p LEFT JOIN public.proxy_stats s USING (proxy_id);

CREATE VIEW public.proxy_health AS
SELECT p.proxy_id, p.address, p.port, p.protocol AS declared_protocol,
       s.detected_protocol, s.working_protocol, s.checked_at, s.last_attempt_at,
       s.status, s.attempted, s.responds AS youtube_responds, s.http_status, s.total_ms,
       s.first_response_at, s.last_response_at, s.checks, s.network_checks,
       s.requests_sent, s.successful_checks,
       CASE WHEN s.responds AND s.successful_checks >= 2 THEN 'repeatedly_responding'
            WHEN s.responds THEN 'responding'
            WHEN s.last_response_at IS NOT NULL THEN 'intermittently_responding'
            ELSE s.status END AS availability
FROM public.proxies p JOIN public.proxy_stats s USING (proxy_id);

CREATE VIEW public.proxy_list_catalog AS
SELECT url, kind, protocol_hints, format_hint, enabled, run_id, status, fetched_at,
       (fetch_state->>'http_status')::smallint AS http_status,
       (fetch_state->>'unique_entries')::bigint AS unique_entries,
       fetch_state->>'error_type' AS error_type
FROM public.proxy_lists;

COMMENT ON TABLE public.proxies IS
    'Published connection configurations. Identity includes address, port, declared protocol and settings. See proxy_health for observed YouTube reachability.';
COMMENT ON COLUMN public.proxies.connection_settings IS
    'Connection options can contain credentials. Binary nulls use a JSON string containing escaped JSON text; other settings are JSON objects.';
COMMENT ON TABLE public.proxy_stats IS
    'One cumulative summary and latest result per configuration. No individual request history is retained. Import finished journals in chronological order.';
COMMENT ON COLUMN public.proxy_stats.requests_sent IS
    'YouTube metadata requests actually sent, excluding proxy negotiation and connection failures before sending.';
COMMENT ON COLUMN public.proxy_stats.successful_checks IS
    'Verified YouTube HTTP responses, including error statuses. This does not measure usable metadata; historical bodies were not validated.';
COMMENT ON COLUMN public.proxy_stats.working_protocol IS
    'Protocol from the most recent verified YouTube response; retained when a later check fails.';
COMMENT ON COLUMN public.proxy_stats.last_import_key IS
    'SHA256 of the latest imported journal. Exact retries are skipped; older or changed overlapping journals are rejected before counters change.';
COMMENT ON COLUMN public.proxy_stats.last_attempt_at IS
    'Latest actual network attempt; configuration rejections do not advance it.';
COMMENT ON TABLE public.proxy_lists IS
    'Source URLs and only their current collection state. Starting a new collection replaces the previous run selection and state for enabled URLs.';
COMMENT ON COLUMN public.proxy_lists.fetch_state IS
    'Latest download/parser state, including payload checksum and cache path needed for reparsing and pagination. Replaced on each completed download.';

GRANT SELECT ON public.proxies, public.proxy_stats, public.proxy_lists,
    public.proxy_catalog, public.proxy_health, public.proxy_list_catalog TO media_viewer;
ANALYZE public.proxy_stats;
ANALYZE public.proxy_lists;
COMMIT;
-- Run VACUUM (FULL, ANALYZE) public.proxies separately to reclaim dropped-column space.
