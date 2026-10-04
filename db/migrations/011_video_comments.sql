ALTER TABLE public.videos
    ADD COLUMN comments_updated_at TIMESTAMPTZ,
    ADD COLUMN comments_error TEXT;

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
