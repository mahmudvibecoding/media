"""Shared search expressions keep indexed documents and queries identical."""
import re


DOCUMENTS = {
    'channels': "coalesce(title,'') || ' ' || coalesce(handle,'') || ' ' || coalesce(description,'') || ' ' || public.media_search_keywords(keywords)",
    'videos': "coalesce(title,'') || ' ' || coalesce(description,'')",
    'comments': "text || ' ' || coalesce(author_name,'')",
}
NAMES = "public.media_search_normalize(coalesce(title,'') || ' ' || coalesce(handle,''))"
INDEXES = {
    'channels': {
        'media_dash_channels_search': f"USING gin (public.media_search_vector({DOCUMENTS['channels']}))",
        'media_dash_channels_names': f'USING gin ({NAMES} public.gin_trgm_ops)',
        'media_dash_channels_subscribers': "(coalesce(subscriber_count,-1) DESC, channel_id)",
        'media_dash_channels_title': '(public.media_search_normalize(title), channel_id)',
    },
    'videos': {
        'media_dash_videos_search': f"USING gin (public.media_search_vector({DOCUMENTS['videos']}))",
        'media_dash_videos_recent': "(coalesce(published_at,'1900-01-01 00:00:00+00'::timestamptz) DESC, video_id)",
        'media_dash_videos_views': '(coalesce(view_count,-1) DESC, video_id)',
        'media_dash_videos_title': '(public.media_search_normalize(title), video_id)',
        'media_dash_videos_channel_recent': "(channel_id, coalesce(published_at,'1900-01-01 00:00:00+00'::timestamptz) DESC, video_id)",
    },
    'comments': {
        'media_dash_comments_search': f"USING gin (public.media_search_vector({DOCUMENTS['comments']}))",
        'media_dash_comments_author': '(public.media_search_normalize(author_name), video_id, comment_id)',
        'media_dash_comments_pinned': '(video_id, comment_id) WHERE is_pinned IS TRUE',
    },
}
APOSTROPHES = str.maketrans({value: "'" for value in '‘’ʻʼ`'})


def normalize(value):
    return value.translate(APOSTROPHES)


def terms(query):
    result = []
    for match in re.finditer(r'"([^"]+)"|(\S+)', normalize(query)):
        phrase, word = match.groups()
        if phrase:
            result.append(phrase)
        elif word.upper() != 'OR' and not word.startswith('-'):
            result.extend(re.findall(r"[\w]+(?:'[\w]+)*", word))
    return sorted(set(result), key=len, reverse=True)[:20]


def partial_name_query(query):
    return len(query) >= 3 and '"' not in query and not re.search(r'\bOR\b|(?:^|\s)-', query, re.I)
