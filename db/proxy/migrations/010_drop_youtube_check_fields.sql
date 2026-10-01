-- Remove check timestamps and durations; actual attempt times protect counters.
BEGIN;
SET LOCAL lock_timeout = '15s';
SELECT pg_advisory_xact_lock(6389247650123);
LOCK TABLE public.proxy_stats IN ACCESS EXCLUSIVE MODE;

DROP VIEW public.proxy_health;
ALTER TABLE public.proxy_stats
    DROP CONSTRAINT proxy_stats_youtube_attempt_at_check,
    DROP CONSTRAINT proxy_stats_youtube_import_check,
    DROP CONSTRAINT proxy_stats_youtube_duration_check,
    DROP COLUMN youtube_last_checked_at,
    DROP COLUMN youtube_last_check_duration_ms,
    ADD CONSTRAINT proxy_stats_youtube_attempt_at_check CHECK
        (youtube_last_attempt_at IS NULL OR (last_connection_attempt_at IS NOT NULL
         AND youtube_last_attempt_at <= last_connection_attempt_at)),
    ADD CONSTRAINT proxy_stats_youtube_import_check CHECK
        ((youtube_last_attempt_at IS NULL OR youtube_last_import_key IS NOT NULL)
         AND (youtube_last_import_key IS NULL OR octet_length(youtube_last_import_key) = 32));

CREATE VIEW public.proxy_health AS
SELECT p.proxy_id, p.address, p.port,
       s.connection_attempts, s.successful_connections, s.last_connection_attempt_at,
       s.last_connected_at, s.last_connection_error, s.working_protocol,
       s.youtube_last_attempt_at,
       CASE WHEN s.youtube_last_attempt_at IS NOT NULL
            THEN s.youtube_last_http_status IS NOT NULL END AS youtube_responded,
       s.youtube_last_http_status, s.youtube_last_response_at,
       s.youtube_last_error,
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

GRANT SELECT ON public.proxy_health TO media_viewer;

COMMENT ON COLUMN public.proxy_stats.youtube_last_attempt_at IS
    'Observation time of the latest actual attempt to use this proxy for YouTube. Used with youtube_last_import_key to reject stale or duplicate attempt imports. Unchanged by checks rejected before an attempt.';
COMMENT ON COLUMN public.proxy_stats.youtube_last_error IS
    'Safe stage:code error from the most recently imported YouTube outcome: setup, network, HTTP or collector-reported data failure. Import rejected configurations in chronological order; their check times are not stored. NULL when successful or unobserved.';
COMMENT ON COLUMN public.proxy_health.youtube_responded IS
    'TRUE for a verified YouTube HTTP response in the latest actual attempt, including error statuses; FALSE for no response; NULL before an attempt. Usable data is tracked separately.';
COMMENT ON COLUMN public.proxy_health.youtube_score IS
    '100*(decayed successes+1)/(decayed attempts+2). NULL before any scored attempt; aging evidence tends toward 50. Read with evidence weight and age. Missing statistics are unknown.';

COMMIT;
