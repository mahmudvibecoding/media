-- Reduce the three compact proxy tables to 7, 11 and 8 columns.
-- Rebuild instead of leaving dropped attributes in the physical row layout.
BEGIN;
SET LOCAL lock_timeout = '15s';
SET LOCAL work_mem = '128MB';
SET LOCAL maintenance_work_mem = '256MB';
SET LOCAL jit = off;
SELECT pg_advisory_xact_lock(hashtextextended('proxy:proxy_collection',0));
SELECT pg_advisory_xact_lock(6389247650123);
LOCK TABLE public.proxies, public.proxy_stats, public.proxy_lists IN ACCESS EXCLUSIVE MODE;

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.proxy_stats
        WHERE status NOT IN ('responds','not_responding','invalid_configuration','incompatible_protocol')
           OR responds IS DISTINCT FROM (status='responds')
           OR attempted IS DISTINCT FROM (status IN ('responds','not_responding'))
           OR detected_protocol IS DISTINCT FROM CASE WHEN responds THEN working_protocol END) THEN
        RAISE EXCEPTION 'Latest status does not determine the flags and detected protocol';
    END IF;
END $$;
CREATE TEMP TABLE cleanup_identity_state ON COMMIT DROP AS
SELECT last_value,is_called FROM public.proxies_proxy_id_seq;

CREATE TABLE public.proxies_reduced (
    proxy_id BIGINT GENERATED ALWAYS AS IDENTITY,
    connection_key BYTEA NOT NULL CHECK (octet_length(connection_key) = 32),
    address TEXT NOT NULL CHECK (length(address) BETWEEN 1 AND 253),
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    protocol TEXT NOT NULL CHECK (protocol <> ''),
    connection_settings JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_seen_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE public.proxy_stats_reduced (
    proxy_id BIGINT,
    checked_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('responds','not_responding','invalid_configuration','incompatible_protocol')),
    working_protocol TEXT,
    http_status SMALLINT CHECK (http_status BETWEEN 100 AND 599),
    total_ms DOUBLE PRECISION NOT NULL CHECK (total_ms >= 0),
    last_response_at TIMESTAMPTZ,
    network_checks BIGINT NOT NULL CHECK (network_checks >= 0),
    requests_sent BIGINT NOT NULL CHECK (requests_sent BETWEEN 0 AND network_checks),
    youtube_responses BIGINT NOT NULL CHECK (youtube_responses BETWEEN 0 AND requests_sent),
    last_import_key BYTEA NOT NULL CHECK (octet_length(last_import_key) = 32),
    CHECK ((status = 'responds') = (http_status IS NOT NULL)),
    CHECK ((youtube_responses > 0) = (last_response_at IS NOT NULL)),
    CHECK ((youtube_responses > 0) = (working_protocol IS NOT NULL)),
    CHECK (last_response_at IS NULL OR last_response_at <= checked_at),
    CHECK (status <> 'responds' OR (last_response_at IS NOT NULL AND last_response_at = checked_at))
);

CREATE TABLE public.proxy_lists_reduced (
    url TEXT,
    kind TEXT NOT NULL,
    protocol_hints TEXT[] NOT NULL DEFAULT '{}',
    enabled BOOLEAN NOT NULL DEFAULT false,
    run_id UUID,
    status TEXT NOT NULL DEFAULT 'not_checked',
    fetched_at TIMESTAMPTZ,
    fetch_state JSONB NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO public.proxies_reduced
    (proxy_id,connection_key,address,port,protocol,connection_settings,last_seen_at)
    OVERRIDING SYSTEM VALUE
SELECT proxy_id,connection_key,address,port,protocol,connection_settings,last_seen_at FROM public.proxies;
INSERT INTO public.proxy_stats_reduced
SELECT proxy_id,checked_at,status,working_protocol,http_status,total_ms,last_response_at,
       network_checks,requests_sent,successful_checks,last_import_key
FROM public.proxy_stats;
INSERT INTO public.proxy_lists_reduced
SELECT url,kind,protocol_hints,enabled,run_id,status,fetched_at,fetch_state FROM public.proxy_lists;

ALTER TABLE public.proxies_reduced ADD CONSTRAINT proxies_reduced_pkey PRIMARY KEY(proxy_id);
ALTER TABLE public.proxies_reduced ADD CONSTRAINT proxies_reduced_connection_key_key UNIQUE(connection_key);
ALTER TABLE public.proxy_stats_reduced ADD CONSTRAINT proxy_stats_reduced_pkey PRIMARY KEY(proxy_id);
ALTER TABLE public.proxy_stats_reduced ADD CONSTRAINT proxy_stats_reduced_proxy_id_fkey
    FOREIGN KEY(proxy_id) REFERENCES public.proxies_reduced(proxy_id);
CREATE INDEX proxy_stats_reduced_responds_idx ON public.proxy_stats_reduced(proxy_id) WHERE status='responds';
ALTER TABLE public.proxy_lists_reduced ADD CONSTRAINT proxy_lists_reduced_pkey PRIMARY KEY(url);
ANALYZE public.proxies_reduced;
ANALYZE public.proxy_stats_reduced;
ANALYZE public.proxy_lists_reduced;

-- Compare every retained value before deleting the old tables.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.proxies p FULL JOIN public.proxies_reduced n USING(proxy_id)
        WHERE (p.proxy_id,p.connection_key,p.address,p.port,p.protocol,p.connection_settings,p.last_seen_at)
          IS DISTINCT FROM
              (n.proxy_id,n.connection_key,n.address,n.port,n.protocol,n.connection_settings,n.last_seen_at)) THEN
        RAISE EXCEPTION 'Proxy identity or last observation changed';
    END IF;
    IF EXISTS (SELECT 1 FROM public.proxy_stats s FULL JOIN public.proxy_stats_reduced n USING(proxy_id)
        WHERE (s.proxy_id,s.checked_at,s.status,s.working_protocol,s.http_status,s.total_ms,s.last_response_at,
               s.network_checks,s.requests_sent,s.successful_checks,s.last_import_key)
          IS DISTINCT FROM
              (n.proxy_id,n.checked_at,n.status,n.working_protocol,n.http_status,n.total_ms,n.last_response_at,
               n.network_checks,n.requests_sent,n.youtube_responses,n.last_import_key)) THEN
        RAISE EXCEPTION 'Retained statistics changed';
    END IF;
    IF EXISTS (SELECT 1 FROM public.proxy_lists l FULL JOIN public.proxy_lists_reduced n USING(url)
        WHERE (l.url,l.kind,l.protocol_hints,l.enabled,l.run_id,l.status,l.fetched_at,l.fetch_state)
          IS DISTINCT FROM
              (n.url,n.kind,n.protocol_hints,n.enabled,n.run_id,n.status,n.fetched_at,n.fetch_state)) THEN
        RAISE EXCEPTION 'Source list or collection state changed';
    END IF;
END $$;

DROP VIEW public.proxy_catalog, public.proxy_health, public.proxy_list_catalog;
DROP TABLE public.proxy_stats;
DROP TABLE public.proxies;
DROP TABLE public.proxy_lists;
ALTER TABLE public.proxies_reduced RENAME TO proxies;
ALTER TABLE public.proxies RENAME CONSTRAINT proxies_reduced_pkey TO proxies_pkey;
ALTER TABLE public.proxies RENAME CONSTRAINT proxies_reduced_connection_key_key TO proxies_connection_key_key;
ALTER SEQUENCE public.proxies_reduced_proxy_id_seq RENAME TO proxies_proxy_id_seq;
SELECT setval(pg_get_serial_sequence('public.proxies','proxy_id'),last_value,is_called) FROM cleanup_identity_state;
ALTER TABLE public.proxy_stats_reduced RENAME TO proxy_stats;
ALTER TABLE public.proxy_stats RENAME CONSTRAINT proxy_stats_reduced_pkey TO proxy_stats_pkey;
ALTER TABLE public.proxy_stats RENAME CONSTRAINT proxy_stats_reduced_proxy_id_fkey TO proxy_stats_proxy_id_fkey;
ALTER INDEX public.proxy_stats_reduced_responds_idx RENAME TO proxy_stats_responds_idx;
ALTER TABLE public.proxy_lists_reduced RENAME TO proxy_lists;
ALTER TABLE public.proxy_lists RENAME CONSTRAINT proxy_lists_reduced_pkey TO proxy_lists_pkey;

CREATE VIEW public.proxy_catalog AS
SELECT p.proxy_id, p.address, p.port, p.protocol, p.last_seen_at, s.checked_at
FROM public.proxies p LEFT JOIN public.proxy_stats s USING (proxy_id);

CREATE VIEW public.proxy_health AS
SELECT p.proxy_id, p.address, p.port, p.protocol AS declared_protocol,
       CASE WHEN s.status = 'responds' THEN s.working_protocol END AS detected_protocol,
       s.working_protocol, s.checked_at, s.status,
       s.status IN ('responds','not_responding') AS attempted,
       s.status = 'responds' AS youtube_responds,
       s.http_status, s.total_ms, s.last_response_at, s.network_checks,
       s.requests_sent, s.youtube_responses,
       CASE WHEN s.status = 'responds' AND s.youtube_responses >= 2 THEN 'repeatedly_responding'
            WHEN s.status = 'responds' THEN 'responding'
            WHEN s.last_response_at IS NOT NULL THEN 'intermittently_responding'
            ELSE s.status END AS availability
FROM public.proxies p JOIN public.proxy_stats s USING (proxy_id);

CREATE VIEW public.proxy_list_catalog AS
SELECT url, kind, protocol_hints, enabled, run_id, status, fetched_at,
       (fetch_state->>'http_status')::smallint AS http_status,
       (fetch_state->>'unique_entries')::bigint AS unique_entries,
       fetch_state->>'error_type' AS error_type
FROM public.proxy_lists;

COMMENT ON TABLE public.proxies IS
    'Published connection configurations. Identity includes address, port, declared protocol and settings. See proxy_health for observed YouTube reachability.';
COMMENT ON COLUMN public.proxies.connection_settings IS
    'Connection options can contain credentials. Binary nulls use a JSON string containing escaped JSON text; other settings are JSON objects.';
COMMENT ON TABLE public.proxy_stats IS
    'One cumulative summary and latest result per configuration. Import immutable finished journals in chronological order.';
COMMENT ON COLUMN public.proxy_stats.status IS
    'responds and not_responding imply a network attempt; invalid_configuration and incompatible_protocol imply no attempt in the latest check. Flags in proxy_health are derived.';
COMMENT ON COLUMN public.proxy_stats.checked_at IS
    'Latest configuration check, including checks rejected before a network attempt.';
COMMENT ON COLUMN public.proxy_stats.network_checks IS
    'Cumulative checks that attempted a connection. One check may try multiple proxy protocols before sending at most one YouTube metadata request.';
COMMENT ON COLUMN public.proxy_stats.requests_sent IS
    'YouTube metadata requests actually sent, excluding proxy negotiation and connection failures before sending.';
COMMENT ON COLUMN public.proxy_stats.youtube_responses IS
    'Verified YouTube HTTP responses, including error statuses. This does not measure usable metadata; historical bodies were not validated.';
COMMENT ON COLUMN public.proxy_stats.working_protocol IS
    'Protocol from the most recent verified YouTube response; retained when a later check fails.';
COMMENT ON COLUMN public.proxy_stats.last_import_key IS
    'SHA256 of the latest imported journal. Exact retries are skipped; older or changed overlapping journals are rejected before counters change.';
COMMENT ON TABLE public.proxy_lists IS
    'Source URLs and only their current collection state. Starting a new collection replaces the previous run selection and state for enabled URLs. Format hints are derived from URLs by the collector.';
COMMENT ON COLUMN public.proxy_lists.fetch_state IS
    'Latest download/parser state, including payload checksum and cache path needed for reparsing and pagination. Replaced on each completed download.';

GRANT SELECT ON public.proxies, public.proxy_stats, public.proxy_lists,
    public.proxy_catalog, public.proxy_health, public.proxy_list_catalog TO media_viewer;
COMMIT;
