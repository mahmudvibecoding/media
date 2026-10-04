ALTER TABLE public.videos
    ADD COLUMN view_count BIGINT CHECK (view_count >= 0),
    ADD COLUMN like_count BIGINT CHECK (like_count >= 0),
    ADD COLUMN stats_updated_at TIMESTAMPTZ,
    ADD COLUMN stats_error TEXT;
