BEGIN;
ALTER TABLE public.channels
    ADD COLUMN title TEXT,
    ADD COLUMN handle TEXT,
    ADD COLUMN description TEXT,
    ADD COLUMN video_count BIGINT CHECK (video_count >= 0),
    ADD COLUMN view_count BIGINT CHECK (view_count >= 0),
    ADD COLUMN joined_date DATE,
    ADD COLUMN country TEXT,
    ADD COLUMN avatar_url TEXT,
    ADD COLUMN keywords TEXT[],
    ADD COLUMN external_links JSONB CHECK (jsonb_typeof(external_links) = 'array'),
    ADD COLUMN metadata_updated_at TIMESTAMPTZ,
    ADD COLUMN metadata_error TEXT;
CREATE INDEX channels_metadata_pending_idx ON public.channels(channel_id)
    WHERE metadata_updated_at IS NULL;
COMMIT;
