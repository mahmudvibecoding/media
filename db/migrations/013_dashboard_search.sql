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
