ALTER TABLE public.videos ADD COLUMN metadata_updated_at TIMESTAMPTZ;

CREATE INDEX videos_metadata_pending_idx ON public.videos (video_id)
    WHERE metadata_updated_at IS NULL;
