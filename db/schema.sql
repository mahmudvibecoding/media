CREATE TABLE public.channels (
    channel_id TEXT PRIMARY KEY,
    subscriber_count BIGINT CHECK (subscriber_count >= 0)
);

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
    metadata_error TEXT
);

CREATE INDEX videos_channel_type_idx ON public.videos (channel_id, type);
CREATE INDEX videos_metadata_pending_idx ON public.videos (video_id)
    WHERE metadata_updated_at IS NULL;

CREATE TABLE public.channel_scan_state (
    channel_id TEXT NOT NULL REFERENCES public.channels (channel_id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK (type IN ('video', 'short')),
    PRIMARY KEY (channel_id, type)
);
