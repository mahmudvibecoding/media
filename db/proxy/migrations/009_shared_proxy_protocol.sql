-- Share the confirmed proxy protocol and keep transport configuration together.
BEGIN;
SET LOCAL lock_timeout = '15s';
SELECT pg_advisory_xact_lock(6389247650123);
SELECT pg_advisory_xact_lock(hashtextextended('proxy:proxy_collection',0));
LOCK TABLE public.proxies,public.proxy_stats IN ACCESS EXCLUSIVE MODE;

DROP VIEW public.proxy_catalog;
DROP VIEW public.proxy_health;

-- Nest the original settings verbatim. A string here is escaped JSON used for
-- binary nulls; it must not be cast to JSONB. IDs and identity hashes stay intact.
UPDATE public.proxies SET connection_settings =
    jsonb_build_object('transport',protocol,'options',connection_settings);
ALTER TABLE public.proxies
    DROP COLUMN protocol,
    ALTER COLUMN connection_settings SET DEFAULT '{"transport":"unknown","options":{}}'::jsonb,
    ADD CONSTRAINT proxies_connection_settings_check CHECK
        (jsonb_typeof(connection_settings) = 'object'
         AND connection_settings ?& ARRAY['transport','options']
         AND jsonb_typeof(connection_settings->'transport') = 'string'
         AND length(connection_settings->>'transport') > 0
         AND jsonb_typeof(connection_settings->'options') IN ('object','string'));

ALTER TABLE public.proxy_stats RENAME COLUMN youtube_working_protocol TO working_protocol;
ALTER TABLE public.proxy_stats
    DROP CONSTRAINT proxy_stats_connection_error_check,
    DROP CONSTRAINT proxy_stats_youtube_response_at_check;

-- Existing website errors already contain these labels. Fill a missing current
-- website error before clearing target/ambiguous failures from the shared field.
UPDATE public.proxy_stats SET
    youtube_last_error=coalesce(youtube_last_error,
        CASE WHEN youtube_last_checked_at=last_connection_attempt_at THEN last_connection_error END),
    last_connection_error=NULL
WHERE last_connection_error IS NOT NULL AND last_connection_error !~
    '^((resolve|connect|proxy_tls):[a-z][a-z0-9_]*|proxy_handshake:(proxy_authentication_required|proxy_authentication_failed|proxy_http_407|not_socks5|invalid_socks5_address|socks4_reply_92|socks4_reply_93|socks5_reply_7|socks5_reply_8))$';

ALTER TABLE public.proxy_stats
    ADD CONSTRAINT proxy_stats_connection_error_check CHECK
        (last_connection_error IS NULL OR last_connection_error ~
         '^((resolve|connect|proxy_tls):[a-z][a-z0-9_]*|proxy_handshake:(proxy_authentication_required|proxy_authentication_failed|proxy_http_407|not_socks5|invalid_socks5_address|socks4_reply_92|socks4_reply_93|socks5_reply_7|socks5_reply_8))$'),
    ADD CONSTRAINT proxy_stats_working_protocol_check CHECK
        (working_protocol IS NULL OR length(working_protocol) > 0),
    ADD CONSTRAINT proxy_stats_youtube_response_at_check CHECK
        ((youtube_responses_received > 0) = (youtube_last_response_at IS NOT NULL)
         AND (youtube_responses_received = 0 OR working_protocol IS NOT NULL)
         AND (youtube_last_response_at IS NULL OR (youtube_last_attempt_at IS NOT NULL
              AND youtube_last_response_at <= youtube_last_attempt_at))
         AND (youtube_last_http_status IS NULL OR (youtube_last_response_at IS NOT NULL
              AND youtube_last_response_at = youtube_last_attempt_at)));

CREATE VIEW public.proxy_catalog AS
SELECT p.proxy_id, p.address, p.port, s.working_protocol, p.last_seen_at, s.last_connection_attempt_at
FROM public.proxies p LEFT JOIN public.proxy_stats s USING (proxy_id);

CREATE VIEW public.proxy_health AS
SELECT p.proxy_id, p.address, p.port,
       s.connection_attempts, s.successful_connections, s.last_connection_attempt_at,
       s.last_connected_at, s.last_connection_error, s.working_protocol,
       s.youtube_last_checked_at, s.youtube_last_attempt_at,
       CASE WHEN s.youtube_last_attempt_at IS NOT NULL
            THEN s.youtube_last_http_status IS NOT NULL END AS youtube_responded,
       s.youtube_last_http_status, s.youtube_last_check_duration_ms, s.youtube_last_response_at,
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

GRANT SELECT ON public.proxy_catalog,public.proxy_health TO media_viewer;

COMMENT ON TABLE public.proxies IS
    'Published proxy configurations. Identity includes address, port and transport configuration. See proxy_health for observed results.';
COMMENT ON COLUMN public.proxies.connection_settings IS
    'Transport configuration: transport is the input type and options are its original settings, possibly containing credentials. Options with binary nulls remain escaped JSON text. The shared working_protocol records confirmed results.';
COMMENT ON TABLE public.proxy_stats IS
    'One summary per proxy, with shared connection fields and a fixed column group per website. Statistics never delay collection. Import ordering and scores are independent for each website.';
COMMENT ON COLUMN public.proxy_stats.connection_attempts IS
    'Recorded attempts to use a configuration, including connection failures. A legacy check may try several protocol handshakes before at most one YouTube request.';
COMMENT ON COLUMN public.proxy_stats.successful_connections IS
    'Recorded attempts with a confirmed proxy connection, counted once per check or collection request, including connection reuse. Missing telemetry is not a confirmed failure.';
COMMENT ON COLUMN public.proxy_stats.last_connected_at IS
    'Observation time of the latest confirmed proxy connection. Older missing observations cannot supply an exact time.';
COMMENT ON COLUMN public.proxy_stats.last_connection_error IS
    'Proxy endpoint, TLS, authentication or explicit protocol error from the latest actual attempt, as stage:code. Target tunnel, website TLS and response errors stay in the website group. NULL when no proxy-specific error was observed.';
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
COMMENT ON COLUMN public.proxy_stats.working_protocol IS
    'Shared proxy transport confirmed by a verified website response. Any installed website writer can supply it. Failed attempts retain the known protocol; NULL before confirmation.';
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
COMMENT ON TABLE public.proxy_lists IS
    'Source URLs and only their current collection state. Starting a new collection replaces the previous run selection and state for enabled URLs. Format hints are derived from URLs by the collector.';
COMMENT ON COLUMN public.proxy_lists.fetch_state IS
    'Latest download/parser state, including payload checksum and cache path needed for reparsing and pagination. Replaced on each completed download.';

COMMIT;
