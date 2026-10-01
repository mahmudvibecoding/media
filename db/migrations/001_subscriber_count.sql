ALTER TABLE public.channels
ADD COLUMN IF NOT EXISTS subscriber_count BIGINT CHECK (subscriber_count >= 0);
