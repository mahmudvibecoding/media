-- Add best-effort YouTube data scoring without retaining request history.
BEGIN;
SET LOCAL lock_timeout = '15s';
SELECT pg_advisory_xact_lock(6389247650123);
LOCK TABLE public.proxy_stats IN ACCESS EXCLUSIVE MODE;
DROP VIEW public.proxy_health;
ALTER TABLE public.proxy_stats RENAME COLUMN network_checks TO connection_attempts;
ALTER TABLE public.proxy_stats RENAME COLUMN requests_sent TO youtube_requests_sent;
ALTER TABLE public.proxy_stats RENAME COLUMN youtube_responses TO youtube_responses_received;
ALTER TABLE public.proxy_stats RENAME COLUMN total_ms TO last_attempt_duration_ms;
ALTER TABLE public.proxy_stats RENAME COLUMN http_status TO last_http_status;
ALTER TABLE public.proxy_stats
    ADD COLUMN youtube_successful_data_received BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN weighted_connection_attempts DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN weighted_youtube_successful_data_received DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN last_scored_attempt_at TIMESTAMPTZ,
    ADD CONSTRAINT proxy_stats_data_count_check CHECK
        (youtube_successful_data_received BETWEEN 0 AND youtube_responses_received),
    ADD CONSTRAINT proxy_stats_weight_check CHECK
        (weighted_connection_attempts >= 0 AND weighted_connection_attempts < 'Infinity'::double precision
         AND weighted_youtube_successful_data_received BETWEEN 0 AND weighted_connection_attempts),
    ADD CONSTRAINT proxy_stats_scored_at_check CHECK
        ((last_scored_attempt_at IS NULL AND weighted_connection_attempts = 0
          AND youtube_successful_data_received = 0)
         OR (last_scored_attempt_at IS NOT NULL AND last_scored_attempt_at <= checked_at
             AND weighted_connection_attempts >= 1));

-- Both scoring updates and reads use this one-hour half-life. The logarithmic
-- form avoids floating-point underflow when evidence is extremely old.
CREATE FUNCTION public.proxy_decayed_weight(weight DOUBLE PRECISION,
    measured_at TIMESTAMPTZ, at_time TIMESTAMPTZ)
RETURNS DOUBLE PRECISION LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE AS $$
    SELECT CASE WHEN weight = 0 THEN 0
                WHEN at_time <= measured_at THEN weight
                WHEN log_weight < -700 THEN 0
                ELSE exp(log_weight) END
    FROM (SELECT ln(NULLIF(weight,0))
          - ln(2.0::double precision)
            * greatest(0, extract(epoch FROM (at_time-measured_at))::double precision) / 3600.0
          AS log_weight) AS evidence
$$;

CREATE VIEW public.proxy_health AS
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
            extract(epoch FROM (statement_timestamp()-s.last_scored_attempt_at))) END AS score_age_seconds
FROM public.proxies p JOIN public.proxy_stats s USING (proxy_id)
CROSS JOIN LATERAL (
    SELECT public.proxy_decayed_weight(s.weighted_connection_attempts,
               s.last_scored_attempt_at,statement_timestamp()) AS attempts,
           public.proxy_decayed_weight(s.weighted_youtube_successful_data_received,
               s.last_scored_attempt_at,statement_timestamp()) AS successes
) AS recent;

COMMENT ON TABLE public.proxy_stats IS
    'One best-effort cumulative summary per configuration. Statistics never delay collection. Apply immutable batches and legacy journals in chronological order.';
COMMENT ON COLUMN public.proxy_stats.status IS
    'responds and not_responding imply a network attempt; invalid_configuration and incompatible_protocol imply no attempt in the latest check. Flags in proxy_health are derived.';
COMMENT ON COLUMN public.proxy_stats.checked_at IS
    'Latest configuration check, including checks rejected before a network attempt.';
COMMENT ON COLUMN public.proxy_stats.connection_attempts IS
    'Recorded attempts to use a configuration, including connection failures. A legacy check may try several protocol handshakes before at most one YouTube request.';
COMMENT ON COLUMN public.proxy_stats.youtube_requests_sent IS
    'YouTube metadata requests actually sent, excluding proxy negotiation and connection failures before sending.';
COMMENT ON COLUMN public.proxy_stats.youtube_responses_received IS
    'Verified YouTube HTTP responses, including error statuses. This does not measure usable metadata; historical bodies were not validated.';
COMMENT ON COLUMN public.proxy_stats.working_protocol IS
    'Protocol from the most recent verified YouTube response; retained when a later check fails.';
COMMENT ON COLUMN public.proxy_stats.last_import_key IS
    'Latest immutable batch identifier or journal SHA256, committed with counters. Exact retries are skipped; overlapping older batches cannot be counted again.';
COMMENT ON COLUMN public.proxy_stats.youtube_successful_data_received IS
    'Recorded collector-reported usable-data responses since scoring was enabled. Historical connectivity results have unknown data quality.';
COMMENT ON COLUMN public.proxy_stats.weighted_connection_attempts IS
    'Eligible recorded attempts weighted with a one-hour half-life, anchored at last_scored_attempt_at. proxy_health decays this value to query time.';
COMMENT ON COLUMN public.proxy_stats.weighted_youtube_successful_data_received IS
    'Collector-reported data successes with the same one-hour weighting as attempts. No content validation is performed by the scorer.';
COMMENT ON COLUMN public.proxy_stats.last_scored_attempt_at IS
    'Time of the latest observation included in scoring, not the time a delayed statistics batch was written. NULL means data quality is unscored.';
COMMENT ON COLUMN public.proxy_health.score IS
    '100*(decayed successes+1)/(decayed attempts+2). NULL before any scored attempt; aging evidence tends toward 50. Read with evidence weight and age. Missing statistics are unknown.';

DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='media_viewer') THEN
        GRANT SELECT ON public.proxy_health TO media_viewer;
    END IF;
END $$;
COMMIT;
