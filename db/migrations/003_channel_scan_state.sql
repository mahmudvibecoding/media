CREATE TABLE public.channel_scan_state (
    channel_id TEXT NOT NULL REFERENCES public.channels (channel_id) ON DELETE CASCADE,
    type TEXT NOT NULL CHECK (type IN ('video', 'short')),
    PRIMARY KEY (channel_id, type)
);

-- Existing video rows came from completed first-page scans.
INSERT INTO public.channel_scan_state (channel_id, type)
SELECT DISTINCT channel_id, type FROM public.videos;
