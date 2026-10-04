"""Fetch exact public video counts in one small, identity-checked response."""
import asyncio
from datetime import datetime, timezone
import json
import re
import ssl
import time
import unicodedata

import httpx

from collect_subscribers import CLIENT_VERSION
from collection_policy import video_error_reason
from collect_video_metadata import (RequestTrace, ResponseShapeError, local_worker_error,
                                    request_error_label)
from proxy_statistics import AttemptOutcome, proxy_connection_error

ENDPOINT = 'https://www.youtube.com/youtubei/v1/next'
_ROOT = 'contents/twoColumnWatchNextResults/results/results/contents/videoPrimaryInfoRenderer'
_BUTTON = ('segmentedLikeDislikeButtonViewModel/likeButtonViewModel/likeButtonViewModel/'
           'toggleButtonViewModel/toggleButtonViewModel/defaultButtonViewModel/'
           'buttonViewModel(title,accessibilityText,iconName)')
_LEGACY = 'toggleButtonRenderer(defaultText,defaultIcon,accessibility,accessibilityData,defaultTooltip)'
FIELD_MASK = (f'currentVideoEndpoint/watchEndpoint/videoId,{_ROOT}(viewCount,'
              f'videoActions/menuRenderer/topLevelButtons({_BUTTON},'
              f'segmentedLikeDislikeButtonRenderer/likeButton/{_LEGACY},{_LEGACY}))')
MAX_BODY_BYTES = 65_536
MAX_COUNT = 2**63 - 1


def path(value, *keys):
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def text(value):
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return None
    if isinstance(value.get('simpleText'), str):
        return value['simpleText']
    runs = value.get('runs')
    if isinstance(runs, list) and all(isinstance(r, dict) and isinstance(r.get('text'), str) for r in runs):
        return ''.join(r['text'] for r in runs)
    return None


def exact_count(value, kind):
    """Never promote a rounded label, live audience, or absent label to a count."""
    if not isinstance(value, str):
        return None
    value = ''.join(c for c in value if unicodedata.category(c) != 'Cf')
    value = value.replace('\u00a0', ' ').replace('\u202f', ' ').strip().lower()
    if kind == 'view':
        match = re.fullmatch(r'(no|[0-9][0-9, ]*) views?', value)
    elif kind == 'like':
        match = re.fullmatch(r'like this video along with ([0-9][0-9, ]*) other (?:people|person)', value)
        if not match:
            match = re.fullmatch(r'(no|[0-9][0-9, ]*) likes?', value)
    elif kind == 'number':
        match = re.fullmatch(r'([0-9][0-9, ]*)', value)
    else:
        raise ValueError('Unknown count kind')
    if not match:
        return None
    digits = match[1]
    if digits == 'no':
        return 0
    if not (re.fullmatch(r'[0-9]+', digits) or
            re.fullmatch(r'[0-9]{1,3}(?:,[0-9]{3})+', digits) or
            re.fullmatch(r'[0-9]{1,3}(?: [0-9]{3})+', digits)):
        return None
    count = int(digits.replace(',', '').replace(' ', ''))
    if count > MAX_COUNT:
        raise ResponseShapeError('Count is outside the database range')
    return count


def parse_stats(payload, video_id):
    if not isinstance(payload, dict):
        raise ResponseShapeError('Response is not an object')
    if path(payload, 'currentVideoEndpoint', 'watchEndpoint', 'videoId') != video_id:
        raise ResponseShapeError('Response video ID does not match the requested ID')
    contents = path(payload, 'contents', 'twoColumnWatchNextResults', 'results', 'results', 'contents')
    if not isinstance(contents, list):
        raise ResponseShapeError('Missing watch response contents')
    primary = [c['videoPrimaryInfoRenderer'] for c in contents
               if isinstance(c, dict) and 'videoPrimaryInfoRenderer' in c]
    if len(primary) > 1 or primary and not isinstance(primary[0], dict):
        raise ResponseShapeError('Invalid primary video information')
    if not primary:
        return {'stats': None, 'stats_complete': False, 'stats_missing': ['VIDEO_UNAVAILABLE'],
                'evidence': {'response_video_id': video_id}}
    info = primary[0]
    view_text = text(path(info, 'viewCount', 'videoViewCountRenderer', 'viewCount'))
    views = exact_count(view_text, 'view')
    buttons = path(info, 'videoActions', 'menuRenderer', 'topLevelButtons') or []
    if not isinstance(buttons, list):
        raise ResponseShapeError('Invalid video buttons')
    like_count, like_text, like_source, like_label = None, None, None, None
    for button in buttons:
        model = path(button, 'segmentedLikeDislikeButtonViewModel', 'likeButtonViewModel',
                     'likeButtonViewModel', 'toggleButtonViewModel', 'toggleButtonViewModel',
                     'defaultButtonViewModel', 'buttonViewModel')
        if isinstance(model, dict) and model.get('iconName') == 'LIKE':
            candidates = [(model.get('accessibilityText'), 'like', 'accessibility'),
                          (model.get('title'), 'number', 'title')]
        else:
            model = (path(button, 'segmentedLikeDislikeButtonRenderer', 'likeButton', 'toggleButtonRenderer')
                     or path(button, 'toggleButtonRenderer'))
            if not isinstance(model, dict) or path(model, 'defaultIcon', 'iconType') != 'LIKE':
                continue
            candidates = [(path(model, 'accessibility', 'label'), 'like', 'accessibility'),
                          (path(model, 'accessibilityData', 'accessibilityData', 'label'), 'like', 'accessibility'),
                          (path(model, 'defaultText', 'accessibility', 'accessibilityData', 'label'), 'like', 'accessibility'),
                          (text(model.get('defaultText')), 'number', 'title')]
        like_label = next((c[0] for c in candidates if isinstance(c[0], str)), like_label)
        for candidate, kind, source in candidates:
            parsed = exact_count(candidate, kind)
            if parsed is not None:
                if like_count is not None and parsed != like_count:
                    raise ResponseShapeError('Conflicting exact like counts')
                like_count, like_text, like_source = parsed, candidate, source
                break
    missing = []
    if views is None:
        missing.append('VIEWS_NOT_EXPOSED')
    if like_count is None:
        missing.append('LIKES_NOT_EXPOSED')
    return {'stats': {'view_count': views, 'like_count': like_count}, 'stats_complete': not missing,
            'stats_missing': missing, 'evidence': {'response_video_id': video_id,
            'view_text': view_text, 'like_text': like_text, 'like_source': like_source, 'like_label': like_label}}


def has_stats(result):
    return result.get('status') == 'ok' and any(
        type(value) is int and 0 <= value <= MAX_COUNT
        for value in ((result.get('stats') or {}).get(name) for name in ('view_count', 'like_count')))


def stats_error_reason(result):
    if has_stats(result):
        return ','.join(result.get('stats_missing') or []) or None
    status = result.get('http_status')
    if type(status) is int and not 200 <= status < 300:
        return f'HTTP_{status}'
    if result.get('status') == 'unexpected_response':
        return 'INVALID_RESPONSE: ' + str(result.get('error', ''))[:200]
    if result.get('stats_missing'):
        return ','.join(result['stats_missing'])
    error = result.get('error')
    if error:
        return str(error)[:200]
    return 'NO_STATISTICS'


def parse_player_views(payload, video_id):
    if not isinstance(payload, dict):
        raise ResponseShapeError('Response is not an object')
    details = payload.get('videoDetails') or {}
    if not isinstance(details, dict) or details.get('videoId') != video_id:
        raise ResponseShapeError('Player response video ID does not match the requested ID')
    raw = details.get('viewCount')
    views = exact_count(raw, 'number')
    return {'stats': {'view_count': views, 'like_count': None}, 'stats_complete': False,
            'stats_missing': ['LIKES_NOT_EXPOSED'] if views is not None else ['VIEWS_NOT_EXPOSED', 'LIKES_NOT_EXPOSED'],
            'player_status': path(payload, 'playabilityStatus', 'status'),
            'evidence': {'response_video_id': video_id, 'view_source': 'player', 'view_count_raw': raw}}


async def _fetch_counts(client, video_id, client_version, retries, *, endpoint, fields, parser,
                        on_attempt=None, total_timeout=None):
    if retries != 0:
        raise ValueError('Statistics retries belong to the durable queue')
    body = json.dumps({'context': {'client': {'clientName': 'WEB', 'clientVersion': client_version,
                        'hl': 'en', 'gl': 'US'}}, 'videoId': video_id}, separators=(',', ':')).encode()
    result = {'video_id': video_id, 'status': 'error', 'stats': None, 'attempts': 1,
              'request_body_bytes': len(body), 'response_body_bytes': 0, 'decoded_body_bytes': 0}
    bridge = getattr(client, 'catalog_bridge', None)
    trace = RequestTrace(bridge=bridge is not None)
    excluded, connection_error = False, None
    started = time.monotonic()
    try:
        async with asyncio.timeout(total_timeout), client.stream('POST', endpoint,
                params={'prettyPrint': 'false', 'fields': fields}, content=body,
                extensions={'trace': trace}) as response:
            result.update(http_status=response.status_code)
            if not 200 <= response.status_code < 300:
                result.update(status='blocked', error=f'HTTP_{response.status_code}')
            else:
                chunks, size = [], 0
                try:
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_BODY_BYTES:
                            raise ResponseShapeError('Statistics response exceeded expected size')
                        chunks.append(chunk)
                finally:
                    result.update(response_body_bytes=response.num_bytes_downloaded, decoded_body_bytes=size)
                result.update(parser(json.loads(b''.join(chunks)), video_id), status='ok')
    except (ResponseShapeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        result.update(status='unexpected_response', error=str(exc))
    except (httpx.HTTPError, TimeoutError, ssl.SSLError) as exc:
        excluded = local_worker_error(exc) or trace.local_failure or (bridge is not None and bridge.local_error(exc))
        connection_error = request_error_label(exc, trace)
        result['error'] = type(exc).__name__
    except BaseException:
        excluded = True
        raise
    finally:
        result['seconds'] = round(time.monotonic() - started, 4)
        if on_attempt is not None and not excluded:
            received = result.get('http_status') is not None
            video_error = video_error_reason(result) if not connection_error else None
            error = (connection_error or (None if has_stats(result) else
                     'video:' + video_error.lower() if video_error else
                     f"http:http_{result['http_status']}" if received and result['http_status'] >= 300 else
                     'data:unexpected_response' if result['status'] == 'unexpected_response' else 'data:no_usable_data'))
            on_attempt(AttemptOutcome(checked_at=datetime.now(timezone.utc),
                request_sent=trace.request_sent or received, http_status=result.get('http_status'),
                data_received=None if video_error else has_stats(result),
                connected=True if trace.request_sent or received else trace.connected,
                connection_error=proxy_connection_error(connection_error), website_error=error))
    return result


async def fetch_stats(client, video_id, client_version=CLIENT_VERSION, retries=0, *,
                      on_attempt=None, total_timeout=None):
    return await _fetch_counts(client, video_id, client_version, retries, endpoint=ENDPOINT,
                              fields=FIELD_MASK, parser=parse_stats, on_attempt=on_attempt,
                              total_timeout=total_timeout)


async def fetch_player_views(client, video_id, *, on_attempt=None, total_timeout=None):
    return await _fetch_counts(client, video_id, CLIENT_VERSION, 0,
        endpoint='https://www.youtube.com/youtubei/v1/player',
        fields='videoDetails(videoId,viewCount),playabilityStatus(status,reason)',
        parser=parse_player_views, on_attempt=on_attempt, total_timeout=total_timeout)
