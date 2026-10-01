CREATE TABLE public.videos (
    video_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL REFERENCES public.channels (channel_id),
    type TEXT NOT NULL CHECK (type IN ('video', 'short')),
    published_at TIMESTAMPTZ
);

CREATE INDEX videos_channel_type_idx ON public.videos (channel_id, type);
