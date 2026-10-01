-- Keep one row per proxy: shared connection fields and fixed website column groups.
BEGIN;
SELECT pg_advisory_xact_lock(6389247650123);
SET LOCAL lock_timeout = '15s';
LOCK TABLE public.proxy_stats IN ACCESS EXCLUSIVE MODE;

-- Preserve the order of rejected checks separately from actual attempts.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.proxy_stats
               WHERE status NOT IN ('responds','not_responding') AND connection_attempts > 0) THEN
        RAISE EXCEPTION 'Recover prior attempt times from journals before migrating these rejected checks';
    END IF;
END $$;

DROP VIEW public.proxy_health;
DROP VIEW public.proxy_catalog;

-- Replace checks whose old names and semantics describe a single website.
DO $$
DECLARE item RECORD;
BEGIN
    FOR item IN SELECT conname FROM pg_constraint
                WHERE conrelid='public.proxy_stats'::regclass AND contype='c'
    LOOP
        EXECUTE format('ALTER TABLE public.proxy_stats DROP CONSTRAINT %I',item.conname);
    END LOOP;
END $$;

ALTER TABLE public.proxy_stats RENAME COLUMN checked_at TO youtube_last_checked_at;
ALTER TABLE public.proxy_stats RENAME COLUMN working_protocol TO youtube_working_protocol;
ALTER TABLE public.proxy_stats RENAME COLUMN last_http_status TO youtube_last_http_status;
ALTER TABLE public.proxy_stats RENAME COLUMN last_attempt_duration_ms TO youtube_last_check_duration_ms;
ALTER TABLE public.proxy_stats RENAME COLUMN last_response_at TO youtube_last_response_at;
ALTER TABLE public.proxy_stats RENAME COLUMN last_import_key TO youtube_last_import_key;
ALTER TABLE public.proxy_stats RENAME COLUMN weighted_connection_attempts TO youtube_weighted_attempts;
ALTER TABLE public.proxy_stats RENAME COLUMN weighted_youtube_successful_data_received TO youtube_weighted_successful_data_received;
ALTER TABLE public.proxy_stats RENAME COLUMN last_scored_attempt_at TO youtube_last_scored_attempt_at;

ALTER TABLE public.proxy_stats
    ALTER COLUMN youtube_last_checked_at DROP NOT NULL,
    ALTER COLUMN youtube_last_check_duration_ms DROP NOT NULL,
    ALTER COLUMN youtube_last_import_key DROP NOT NULL,
    ALTER COLUMN connection_attempts SET DEFAULT 0,
    ALTER COLUMN youtube_requests_sent SET DEFAULT 0,
    ALTER COLUMN youtube_responses_received SET DEFAULT 0,
    ADD COLUMN last_connection_attempt_at TIMESTAMPTZ,
    ADD COLUMN youtube_last_attempt_at TIMESTAMPTZ,
    ADD COLUMN youtube_last_error TEXT;

UPDATE public.proxy_stats SET
    last_connection_attempt_at=CASE WHEN status IN ('responds','not_responding') THEN youtube_last_checked_at END,
    youtube_last_attempt_at=CASE WHEN status IN ('responds','not_responding') THEN youtube_last_checked_at END,
    youtube_last_error=coalesce(last_connection_error,
        CASE WHEN youtube_last_http_status >= 300 THEN 'http:http_' || youtube_last_http_status::text END),
    last_connection_error=CASE WHEN status IN ('responds','not_responding') THEN last_connection_error END;

ALTER TABLE public.proxy_stats DROP COLUMN status;
ALTER TABLE public.proxy_stats
    ADD CONSTRAINT proxy_stats_attempt_count_check CHECK (connection_attempts >= 0),
    ADD CONSTRAINT proxy_stats_connection_count_check CHECK
        (successful_connections BETWEEN 0 AND connection_attempts),
    ADD CONSTRAINT proxy_stats_attempt_at_check CHECK
        ((connection_attempts > 0) = (last_connection_attempt_at IS NOT NULL)),
    ADD CONSTRAINT proxy_stats_connected_at_check CHECK
        (last_connected_at IS NULL OR (successful_connections > 0 AND last_connected_at <= last_connection_attempt_at)),
    ADD CONSTRAINT proxy_stats_connection_error_check CHECK
        (last_connection_error IS NULL OR last_connection_error ~ '^[a-z][a-z0-9_]*:[a-z][a-z0-9_]*$'),
    ADD CONSTRAINT proxy_stats_youtube_requests_check CHECK
        (youtube_requests_sent BETWEEN 0 AND connection_attempts),
    ADD CONSTRAINT proxy_stats_youtube_responses_check CHECK
        (youtube_responses_received BETWEEN 0 AND youtube_requests_sent),
    ADD CONSTRAINT proxy_stats_youtube_data_check CHECK
        (youtube_successful_data_received BETWEEN 0 AND youtube_responses_received),
    ADD CONSTRAINT proxy_stats_youtube_http_check CHECK
        (youtube_last_http_status IS NULL OR (youtube_last_http_status BETWEEN 100 AND 599
         AND youtube_last_attempt_at IS NOT NULL)),
    ADD CONSTRAINT proxy_stats_youtube_duration_check CHECK (youtube_last_check_duration_ms >= 0),
    ADD CONSTRAINT proxy_stats_youtube_error_check CHECK
        (youtube_last_error IS NULL OR youtube_last_error ~ '^[a-z][a-z0-9_]*:[a-z][a-z0-9_]*$'),
    ADD CONSTRAINT proxy_stats_youtube_attempt_at_check CHECK
        (youtube_last_attempt_at IS NULL OR (youtube_last_checked_at IS NOT NULL
         AND youtube_last_attempt_at <= youtube_last_checked_at
         AND last_connection_attempt_at IS NOT NULL AND youtube_last_attempt_at <= last_connection_attempt_at)),
    ADD CONSTRAINT proxy_stats_youtube_response_at_check CHECK
        ((youtube_responses_received > 0) = (youtube_last_response_at IS NOT NULL)
         AND (youtube_responses_received > 0) = (youtube_working_protocol IS NOT NULL)
         AND (youtube_last_response_at IS NULL OR (youtube_last_attempt_at IS NOT NULL
              AND youtube_last_response_at <= youtube_last_attempt_at))
         AND (youtube_last_http_status IS NULL OR (youtube_last_response_at IS NOT NULL
              AND youtube_last_response_at = youtube_last_attempt_at))),
    ADD CONSTRAINT proxy_stats_youtube_weight_check CHECK
        (youtube_weighted_attempts >= 0 AND youtube_weighted_attempts < 'Infinity'::double precision
         AND youtube_weighted_successful_data_received BETWEEN 0 AND youtube_weighted_attempts),
    ADD CONSTRAINT proxy_stats_youtube_scored_at_check CHECK
        ((youtube_last_scored_attempt_at IS NULL AND youtube_weighted_attempts = 0
          AND youtube_successful_data_received = 0)
         OR (youtube_last_scored_attempt_at IS NOT NULL AND youtube_last_attempt_at IS NOT NULL
             AND youtube_last_scored_attempt_at <= youtube_last_attempt_at AND youtube_weighted_attempts >= 1)),
    ADD CONSTRAINT proxy_stats_youtube_import_check CHECK
        ((youtube_last_checked_at IS NULL) = (youtube_last_import_key IS NULL)
         AND (youtube_last_import_key IS NULL OR octet_length(youtube_last_import_key) = 32));

CREATE INDEX proxy_stats_youtube_responds_idx ON public.proxy_stats(proxy_id)
    WHERE youtube_last_http_status IS NOT NULL;

CREATE VIEW public.proxy_catalog AS
SELECT p.proxy_id, p.address, p.port, p.protocol, p.last_seen_at, s.last_connection_attempt_at
FROM public.proxies p LEFT JOIN public.proxy_stats s USING (proxy_id);

CREATE VIEW public.proxy_health AS
SELECT p.proxy_id, p.address, p.port, p.protocol AS declared_protocol,
       s.connection_attempts, s.successful_connections, s.last_connection_attempt_at,
       s.last_connected_at, s.last_connection_error,
       s.youtube_last_checked_at, s.youtube_last_attempt_at,
       CASE WHEN s.youtube_last_attempt_at IS NOT NULL
            THEN s.youtube_last_http_status IS NOT NULL END AS youtube_responded,
       s.youtube_last_http_status, s.youtube_last_check_duration_ms, s.youtube_last_response_at,
       s.youtube_working_protocol, s.youtube_last_error,
       s.youtube_requests_sent, s.youtube_responses_received, s.youtube_successful_data_received,
       coalesce(recent.attempts,0) AS youtube_weighted_attempts,
       coalesce(recent.successes,0) AS youtube_weighted_successful_data_received,
       s.youtube_last_scored_attempt_at,
       CASE WHEN s.youtube_last_scored_attempt_at IS NOT NULL
            THEN 100.0 * (recent.successes + 1.0) / (recent.attempts + 2.0) END AS youtube_score,
       CASE WHEN s.youtube_last_scored_attempt_at IS NOT NULL THEN greatest(0,
            extract(epoch FROM (statement_timestamp()-s.youtube_last_scored_attempt_at))) END AS youtube_score_age_seconds
FROM public.proxies p JOIN public.proxy_stats s USING (proxy_id)
CROSS JOIN LATERAL (
    SELECT public.proxy_decayed_weight(s.youtube_weighted_attempts,
               s.youtube_last_scored_attempt_at,statement_timestamp()) AS attempts,
           public.proxy_decayed_weight(s.youtube_weighted_successful_data_received,
               s.youtube_last_scored_attempt_at,statement_timestamp()) AS successes
) AS recent;

GRANT SELECT ON public.proxy_catalog,public.proxy_health TO media_viewer;

COMMENT ON TABLE public.proxies IS
    'Published proxies. Identity includes address, port, declared protocol and settings. See proxy_health for observed results.';
COMMENT ON COLUMN public.proxies.connection_settings IS
    'Connection options can contain credentials. Binary nulls use a JSON string containing escaped JSON text; other settings are JSON objects.';
COMMENT ON TABLE public.proxy_stats IS
    'One summary per proxy, with shared connection fields and a fixed column group per website. Statistics never delay collection. Import ordering and scores are independent for each website.';
COMMENT ON COLUMN public.proxy_stats.connection_attempts IS
    'Recorded attempts to use a configuration, including connection failures. A legacy check may try several protocol handshakes before at most one YouTube request.';
COMMENT ON COLUMN public.proxy_stats.successful_connections IS
    'Recorded attempts with a confirmed proxy connection, counted once per check or collection request, including connection reuse. Missing telemetry is not a confirmed failure.';
COMMENT ON COLUMN public.proxy_stats.last_connected_at IS
    'Observation time of the latest confirmed proxy connection. Older missing observations cannot supply an exact time.';
COMMENT ON COLUMN public.proxy_stats.last_connection_error IS
    'Network failure from the latest actual attempt across websites, as stage:code. NULL when none was recorded. Checks rejected before a network attempt do not change it.';
COMMENT ON COLUMN public.proxy_stats.last_connection_attempt_at IS
    'Completion time of the latest actual connection attempt across websites, including failures and requests reusing a connection. NULL before any attempt.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_checked_at IS
    'Latest YouTube check, including rejection before a network attempt. Used with youtube_last_import_key to reject stale or duplicate imports.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_attempt_at IS
    'Completion time of the latest actual attempt to use this proxy for YouTube. Unchanged by checks rejected before an attempt.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_http_status IS
    'HTTP status from the latest actual YouTube attempt, or NULL when no response arrived. A failed connection clears it.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_check_duration_ms IS
    'Duration of the latest YouTube check, including a check rejected before a network attempt.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_error IS
    'Safe stage:code error from the latest YouTube check: setup, network, HTTP or collector-reported data failure. NULL when successful or unobserved.';
COMMENT ON COLUMN public.proxy_stats.youtube_requests_sent IS
    'YouTube metadata requests actually sent, excluding proxy negotiation and connection failures before sending.';
COMMENT ON COLUMN public.proxy_stats.youtube_responses_received IS
    'Verified YouTube HTTP responses, including error statuses. This does not measure usable metadata; historical bodies were not validated.';
COMMENT ON COLUMN public.proxy_stats.youtube_working_protocol IS
    'Protocol from the most recent verified YouTube response; retained when a later check fails.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_import_key IS
    'Latest YouTube batch or journal SHA256, committed atomically with shared and YouTube counters. Exact retries are skipped; overlapping older YouTube batches are not counted again.';
COMMENT ON COLUMN public.proxy_stats.youtube_successful_data_received IS
    'Recorded collector-reported usable-data responses since scoring was enabled. Historical connectivity results have unknown data quality.';
COMMENT ON COLUMN public.proxy_stats.youtube_weighted_attempts IS
    'Eligible YouTube attempts weighted with a one-hour half-life, anchored at youtube_last_scored_attempt_at. proxy_health decays this value to query time.';
COMMENT ON COLUMN public.proxy_stats.youtube_weighted_successful_data_received IS
    'Collector-reported data successes with the same one-hour weighting as attempts. No content validation is performed by the scorer.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_scored_attempt_at IS
    'Time of the latest observation included in scoring, not the time a delayed statistics batch was written. NULL means data quality is unscored.';
COMMENT ON COLUMN public.proxy_health.youtube_responded IS
    'TRUE for a verified YouTube HTTP response in the latest actual attempt, including error statuses; FALSE for no response; NULL before an attempt. Usable data is tracked separately.';
COMMENT ON COLUMN public.proxy_health.youtube_score IS
    '100*(decayed successes+1)/(decayed attempts+2). NULL before any scored attempt; aging evidence tends toward 50. Read with evidence weight and age. Missing statistics are unknown.';

COMMIT;
