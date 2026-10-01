ALTER TABLE public.videos
    ADD COLUMN title TEXT,
    ADD COLUMN description TEXT,
    ADD COLUMN duration_seconds INTEGER CHECK (duration_seconds >= 0),
    ADD COLUMN player_status TEXT;
