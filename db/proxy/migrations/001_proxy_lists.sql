BEGIN;

CREATE TABLE public.proxy_sources (
    source_key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    url TEXT NOT NULL,
    kind TEXT NOT NULL,
    repository_pushed_at TIMESTAMPTZ,
    archived BOOLEAN,
    is_fork BOOLEAN,
    repository_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE public.proxy_lists (
    url TEXT PRIMARY KEY,
    source_key TEXT NOT NULL REFERENCES public.proxy_sources (source_key),
    kind TEXT NOT NULL,
    protocol_hints TEXT[] NOT NULL DEFAULT '{}',
    format_hint TEXT,
    aliases TEXT[] NOT NULL DEFAULT '{}',
    provenance JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX proxy_lists_source_idx ON public.proxy_lists (source_key);

CREATE TABLE public.proxy_list_checks (
    list_url TEXT NOT NULL REFERENCES public.proxy_lists (url),
    checked_at TIMESTAMPTZ NOT NULL,
    response_status TEXT NOT NULL,
    http_status SMALLINT CHECK (http_status BETWEEN 100 AND 599),
    final_url TEXT,
    is_html BOOLEAN,
    sample_proxy_count INTEGER CHECK (sample_proxy_count >= 0),
    sample_bytes INTEGER CHECK (sample_bytes >= 0),
    sample_truncated BOOLEAN,
    sample_sha256 TEXT,
    credentials_used BOOLEAN NOT NULL,
    listed_proxy_used BOOLEAN NOT NULL,
    elapsed_seconds DOUBLE PRECISION,
    error_type TEXT,
    error_message TEXT,
    details JSONB NOT NULL,
    PRIMARY KEY (list_url, checked_at)
);

CREATE VIEW public.proxy_list_catalog AS
SELECT
    l.url,
    s.name AS source,
    l.source_key,
    s.url AS source_url,
    s.kind AS source_kind,
    l.kind,
    l.protocol_hints,
    l.format_hint,
    c.checked_at,
    COALESCE(c.response_status, 'not_checked') AS response_status,
    c.http_status,
    c.is_html,
    c.sample_proxy_count,
    c.sample_bytes,
    c.sample_truncated,
    c.final_url,
    COALESCE(
        c.response_status IN (
            'proxy_entries_observed',
            'subscription_entries_observed',
            'telegram_proxy_entries_observed'
        ) AND NOT c.credentials_used,
        false
    ) AS data_observed_without_account,
    s.repository_pushed_at,
    s.archived,
    s.is_fork,
    l.created_at
FROM public.proxy_lists l
JOIN public.proxy_sources s USING (source_key)
LEFT JOIN LATERAL (
    SELECT c.* FROM public.proxy_list_checks c
    WHERE c.list_url = l.url
    ORDER BY c.checked_at DESC
    LIMIT 1
) c ON true;

COMMENT ON TABLE public.proxy_sources IS
    'Discovered repository/domain groups; groups can share operators and contain unverified leads.';
COMMENT ON TABLE public.proxy_lists IS
    'Public proxy-list URL candidates, including APIs, pages and repository discovery links. See kind and latest check before use.';
COMMENT ON TABLE public.proxy_list_checks IS
    'Unauthenticated source URL checks, not tests of individual proxies. Counts refer to a response sample, not the whole list.';
COMMENT ON COLUMN public.proxy_lists.protocol_hints IS
    'Inferred from URL text; not verified protocol support.';
COMMENT ON COLUMN public.proxy_sources.repository_pushed_at IS
    'Repository-wide push time; not the update time of a particular proxy file.';

GRANT SELECT ON public.proxy_sources, public.proxy_lists,
    public.proxy_list_checks, public.proxy_list_catalog TO media_viewer;

COMMIT;
