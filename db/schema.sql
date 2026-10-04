CREATE TABLE public.channels (
    channel_id TEXT PRIMARY KEY,
    subscriber_count BIGINT CHECK (subscriber_count >= 0),
    title TEXT,
    handle TEXT,
    description TEXT,
    video_count BIGINT CHECK (video_count >= 0),
    view_count BIGINT CHECK (view_count >= 0),
    joined_date DATE,
    country TEXT,
    avatar_url TEXT,
    keywords TEXT[],
    external_links JSONB CHECK (jsonb_typeof(external_links) = 'array'),
    metadata_updated_at TIMESTAMPTZ,
    metadata_error TEXT
);

CREATE INDEX channels_metadata_pending_idx ON public.channels(channel_id)
    WHERE metadata_updated_at IS NULL;

CREATE TABLE public.videos (
    video_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES public.channels (channel_id),
    type TEXT NOT NULL CHECK (type IN ('video', 'short')),
    published_at TIMESTAMPTZ,
    title TEXT,
    description TEXT,
    duration_seconds INTEGER CHECK (duration_seconds >= 0),
    thumbnail_url TEXT,
    metadata_updated_at TIMESTAMPTZ,
    metadata_error TEXT,
    view_count BIGINT CHECK (view_count >= 0),
    like_count BIGINT CHECK (like_count >= 0),
    stats_updated_at TIMESTAMPTZ,
    stats_error TEXT,
    comments_updated_at TIMESTAMPTZ,
    comments_error TEXT
);

CREATE INDEX videos_channel_type_idx ON public.videos (channel_id, type);
CREATE INDEX videos_metadata_pending_idx ON public.videos (video_id)
    WHERE metadata_updated_at IS NULL;
CREATE INDEX videos_comments_pending_idx ON public.videos (video_id)
    WHERE comments_updated_at IS NULL;

CREATE TABLE public.comments (
    video_id TEXT NOT NULL REFERENCES public.videos (video_id),
    comment_id TEXT NOT NULL,
    text TEXT NOT NULL,
    author_channel_id TEXT,
    author_name TEXT,
    is_pinned BOOLEAN,
    PRIMARY KEY (video_id, comment_id)
);

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;

CREATE FUNCTION public.media_search_normalize(value text) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN lower(translate(coalesce(value, ''), '‘’ʻʼ`', repeat(chr(39), 5)));

CREATE FUNCTION public.media_search_keywords(value text[]) RETURNS text
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN coalesce(array_to_string(value, ' '), '');

CREATE FUNCTION public.media_search_vector(value text) RETURNS tsvector
LANGUAGE sql IMMUTABLE PARALLEL SAFE
RETURN to_tsvector('simple'::regconfig, public.media_search_normalize(value));

-- Large indexes are built concurrently by prepare_dashboard.py after migration.
