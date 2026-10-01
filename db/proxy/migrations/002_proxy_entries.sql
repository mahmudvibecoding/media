BEGIN;

CREATE TABLE public.proxies (
    proxy_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    connection_key BYTEA NOT NULL UNIQUE CHECK (octet_length(connection_key) = 32),
    address TEXT NOT NULL CHECK (length(address) BETWEEN 1 AND 253),
    port INTEGER NOT NULL CHECK (port BETWEEN 1 AND 65535),
    protocol TEXT NOT NULL CHECK (protocol <> ''),
    connection_settings JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    tested_at TIMESTAMPTZ,
    CHECK (last_seen_at >= first_seen_at)
);
CREATE INDEX proxies_address_port_idx ON public.proxies (address, port);
CREATE INDEX proxies_protocol_idx ON public.proxies (protocol);

CREATE TABLE public.proxy_list_entries (
    list_url TEXT NOT NULL REFERENCES public.proxy_lists (url),
    proxy_id BIGINT NOT NULL REFERENCES public.proxies (proxy_id),
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (list_url, proxy_id),
    CHECK (last_seen_at >= first_seen_at)
);
CREATE INDEX proxy_list_entries_proxy_idx ON public.proxy_list_entries (proxy_id);

CREATE TABLE public.proxy_collection_runs (
    run_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL CHECK (status IN ('running', 'unfinished', 'completed', 'failed')),
    selected_count INTEGER NOT NULL CHECK (selected_count >= 0),
    settings JSONB NOT NULL,
    summary JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE public.proxy_list_downloads (
    run_id UUID NOT NULL REFERENCES public.proxy_collection_runs (run_id),
    list_url TEXT NOT NULL REFERENCES public.proxy_lists (url),
    protocol_hints TEXT[] NOT NULL DEFAULT '{}',
    status TEXT NOT NULL DEFAULT 'pending',
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    http_status SMALLINT,
    attempts INTEGER NOT NULL DEFAULT 0,
    final_url TEXT,
    content_type TEXT,
    decoded_bytes BIGINT,
    received_body_bytes BIGINT,
    content_sha256 TEXT,
    payload_path TEXT,
    entries_found BIGINT,
    unique_entries BIGINT,
    duplicates_in_list BIGINT,
    invalid_entries BIGINT,
    parser_details JSONB,
    error_type TEXT,
    elapsed_seconds DOUBLE PRECISION,
    PRIMARY KEY (run_id, list_url)
);
CREATE INDEX proxy_list_downloads_url_idx ON public.proxy_list_downloads (list_url, finished_at DESC);

CREATE VIEW public.proxy_catalog AS
SELECT p.proxy_id, p.address, p.port, p.protocol,
       p.first_seen_at, p.last_seen_at, p.tested_at,
       (SELECT count(*) FROM public.proxy_list_entries e WHERE e.proxy_id=p.proxy_id) AS list_count
FROM public.proxies p;

COMMENT ON TABLE public.proxies IS
    'Untested published proxy connection candidates. Identity hashes normalized address, port, claimed protocol and connection settings. Unknown protocols are retained as unknown.';
COMMENT ON COLUMN public.proxies.connection_settings IS
    'Required published connection options, which can contain authentication credentials. Usually a JSON object; options with binary nulls are stored as a JSON string containing escaped JSON text. Display labels and performance metadata are excluded from identity.';
COMMENT ON COLUMN public.proxies.tested_at IS
    'NULL until an individual proxy connectivity test is implemented and performed. Downloading a list never updates this field.';
COMMENT ON TABLE public.proxy_list_entries IS
    'Historical list membership; last_seen_at records successful observations. It does not imply a proxy remains in a source or is working.';
COMMENT ON TABLE public.proxy_list_downloads IS
    'Full list downloads and parsing outcomes. Successful rows and their proxy observations commit together; interrupted pending rows can resume.';

GRANT SELECT ON public.proxies, public.proxy_list_entries,
    public.proxy_collection_runs, public.proxy_list_downloads, public.proxy_catalog TO media_viewer;

COMMIT;
