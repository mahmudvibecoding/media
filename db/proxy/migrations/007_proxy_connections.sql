BEGIN;
SELECT pg_advisory_xact_lock(6389247650123);
ALTER TABLE public.proxy_stats
    ADD COLUMN successful_connections BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN last_connected_at TIMESTAMPTZ,
    ADD COLUMN last_connection_error TEXT,
    ADD CONSTRAINT proxy_stats_connection_count_check CHECK
        (successful_connections BETWEEN 0 AND connection_attempts),
    ADD CONSTRAINT proxy_stats_connected_at_check CHECK
        (last_connected_at IS NULL OR (successful_connections > 0 AND last_connected_at <= checked_at)),
    ADD CONSTRAINT proxy_stats_connection_error_check CHECK
        (last_connection_error IS NULL OR last_connection_error ~ '^[a-z][a-z0-9_]*:[a-z][a-z0-9_]*$');

CREATE OR REPLACE VIEW public.proxy_health AS
SELECT p.proxy_id, p.address, p.port, p.protocol AS declared_protocol,
       CASE WHEN s.status = 'responds' THEN s.working_protocol END AS detected_protocol,
       s.working_protocol, s.checked_at, s.status,
       s.status IN ('responds','not_responding') AS attempted,
       s.status = 'responds' AS youtube_responds,
       s.last_http_status, s.last_attempt_duration_ms, s.last_response_at, s.connection_attempts,
       s.youtube_requests_sent, s.youtube_responses_received,
       CASE WHEN s.status = 'responds' AND s.youtube_responses_received >= 2 THEN 'repeatedly_responding'
            WHEN s.status = 'responds' THEN 'responding'
            WHEN s.last_response_at IS NOT NULL THEN 'intermittently_responding'
            ELSE s.status END AS availability,
       s.youtube_successful_data_received,
       coalesce(recent.attempts,0) AS weighted_connection_attempts,
       coalesce(recent.successes,0) AS weighted_youtube_successful_data_received,
       s.last_scored_attempt_at,
       CASE WHEN s.last_scored_attempt_at IS NOT NULL
            THEN 100.0 * (recent.successes + 1.0) / (recent.attempts + 2.0) END AS score,
       CASE WHEN s.last_scored_attempt_at IS NOT NULL THEN greatest(0,
            extract(epoch FROM (statement_timestamp()-s.last_scored_attempt_at))) END AS score_age_seconds,
       s.successful_connections, s.last_connected_at, s.last_connection_error
FROM public.proxies p JOIN public.proxy_stats s USING (proxy_id)
CROSS JOIN LATERAL (
    SELECT public.proxy_decayed_weight(s.weighted_connection_attempts,
               s.last_scored_attempt_at,statement_timestamp()) AS attempts,
           public.proxy_decayed_weight(s.weighted_youtube_successful_data_received,
               s.last_scored_attempt_at,statement_timestamp()) AS successes
) AS recent;


COMMENT ON COLUMN public.proxy_stats.successful_connections IS
    'Recorded attempts with a confirmed proxy connection, counted once per check or collection request, including connection reuse. Missing telemetry is not a confirmed failure.';
COMMENT ON COLUMN public.proxy_stats.last_connected_at IS
    'Observation time of the latest confirmed proxy connection. Older missing observations cannot supply an exact time.';
COMMENT ON COLUMN public.proxy_stats.last_connection_error IS
    'Network or connection-setup failure from the latest check as stage:code. NULL for successful connections without a later network error, or when the latest error was not observed. HTTP error statuses and missing metadata are separate outcomes.';
COMMIT;
