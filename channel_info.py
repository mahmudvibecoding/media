"""Read a channel overview and its returned About endpoint through InnerTube."""
import asyncio
from datetime import datetime, timezone
from decimal import Decimal
import json
import re
import shlex
from urllib.parse import parse_qs, unquote, urlsplit

import httpx

from collect_subscribers import CLIENT_VERSION

ENDPOINT = 'https://www.youtube.com/youtubei/v1/browse'
OVERVIEW_FIELDS = ('metadata,header,microformat,alerts,'
    'contents/twoColumnBrowseResultsRenderer/tabs/tabRenderer(endpoint,title,tabIdentifier)')
ABOUT_FIELDS = 'onResponseReceivedEndpoints,onResponseReceivedActions,contents,alerts'
FIELDS = ('title', 'handle', 'description', 'subscriber_count', 'video_count', 'view_count',
          'joined_date', 'country', 'avatar_url', 'keywords', 'external_links')
MAX_BYTES = 512 * 1024


class ResponseShapeError(ValueError):
    pass


class ChannelUnavailable(ResponseShapeError):
    pass


def nodes(value, name):
    if isinstance(value, dict):
        if name in value:
            yield value[name]
        for child in value.values():
            yield from nodes(child, name)
    elif isinstance(value, list):
        for child in value:
            yield from nodes(child, name)


def text(value):
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for name in ('content', 'simpleText'):
            if isinstance(value.get(name), str):
                return value[name]
        if isinstance(value.get('runs'), list):
            return ''.join(run.get('text', '') for run in value['runs'])
    return None


def count(value, unit):
    value = text(value)
    if value is None:
        return None
    value = ' '.join(value.replace('\u200e', '').replace('\u200f', '').split())
    if value.lower() == 'no ' + unit:
        return 0
    match = re.fullmatch(r'((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*([KMB]?)\s+' + unit + r'?', value, re.I)
    if not match:
        raise ResponseShapeError('Unrecognized ' + unit + ' count')
    number = Decimal(match[1].replace(',', '')) * {'':1, 'K':1000, 'M':1000000, 'B':1000000000}[match[2].upper()]
    if number != number.to_integral_value() or not 0 <= number <= 2**63-1:
        raise ResponseShapeError('Count is outside the supported range')
    return int(number)


def joined_date(value):
    value = text(value)
    if value is None:
        return None
    for pattern in ('Joined %b %d, %Y', 'Joined %B %d, %Y'):
        try:
            return datetime.strptime(value, pattern).date().isoformat()
        except ValueError:
            pass
    raise ResponseShapeError('Unrecognized joined date')


def public_url(value):
    if not isinstance(value, str):
        return None
    parsed = urlsplit(value)
    if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username or parsed.password:
        return None
    return value


def channel_handle(value):
    if not isinstance(value, str):
        return None
    path = unquote(urlsplit(value).path).strip('/')
    return path if path.startswith('@') and '/' not in path else None


def parse_overview(payload, channel_id):
    if not isinstance(payload, dict):
        raise ResponseShapeError('Channel response is not an object')
    metadata = payload.get('metadata', {}).get('channelMetadataRenderer')
    if not isinstance(metadata, dict):
        alerts = [text(value.get('text')) for value in nodes(payload.get('alerts', []), 'alertRenderer')
                  if value.get('type') == 'ERROR']
        if alerts:
            raise ChannelUnavailable((alerts[0] or 'Channel unavailable')[:240])
        raise ResponseShapeError('Channel metadata is missing')
    if metadata.get('externalId') != channel_id:
        raise ResponseShapeError('Channel ID does not match the request')
    if not isinstance(metadata.get('title'), str):
        raise ResponseShapeError('Channel title is missing')
    microformat = payload.get('microformat', {}).get('microformatDataRenderer', {})
    keywords = microformat.get('tags')
    if keywords is None and isinstance(metadata.get('keywords'), str):
        try:
            keywords = shlex.split(metadata['keywords'])
        except ValueError:
            raise ResponseShapeError('Malformed channel keywords')
    if keywords is not None and (not isinstance(keywords, list) or any(not isinstance(k, str) for k in keywords)):
        raise ResponseShapeError('Malformed channel keywords')
    thumbnails = metadata.get('avatar', {}).get('thumbnails', [])
    avatars = [item for item in thumbnails if public_url(item.get('url'))]
    avatar = max(avatars, key=lambda item:item.get('width', 0))['url'] if avatars else None
    description = metadata.get('description')
    if description is not None and not isinstance(description, str):
        raise ResponseShapeError('Malformed channel description')
    return dict(title=metadata['title'], description=description,
        handle=channel_handle(metadata.get('vanityChannelUrl')),
        avatar_url=avatar, keywords=list(dict.fromkeys(keywords)) if keywords is not None else None)


def about_request(payload, channel_id):
    header = payload.get('header', {})
    for endpoint in nodes(header, 'continuationEndpoint'):
        api = endpoint.get('commandMetadata', {}).get('webCommandMetadata', {}).get('apiUrl')
        command = endpoint.get('continuationCommand', {})
        if api == '/youtubei/v1/browse' and isinstance(command.get('token'), str) and command['token']:
            return {'continuation': command['token']}
    for endpoint in nodes(header, 'moreEndpoint'):
        browse = endpoint.get('browseEndpoint', {})
        if browse.get('browseId') == channel_id and isinstance(browse.get('params'), str):
            return {'browseId':channel_id, 'params':browse['params']}
    for tab in nodes(payload.get('contents', {}), 'tabRenderer'):
        endpoint = tab.get('endpoint', {})
        url = endpoint.get('commandMetadata', {}).get('webCommandMetadata', {}).get('url', '')
        browse = endpoint.get('browseEndpoint', {})
        if (tab.get('title') == 'About' or url.endswith('/about')) and browse.get('browseId') == channel_id:
            return {key:browse[key] for key in ('browseId', 'params') if key in browse}
    raise ResponseShapeError('About endpoint is missing')


def external_links(about):
    result, seen = [], set()
    for item in about.get('links', about.get('primaryLinks', [])):
        item = item.get('channelExternalLinkViewModel', item)
        destinations = list(nodes(item, 'urlEndpoint'))
        url = next((endpoint.get('url') for endpoint in destinations if isinstance(endpoint.get('url'), str)), None)
        if url and re.match(r'(?:(?:www|m|music)\.)?(?:youtube\.com|youtu\.be)(?:[/?#]|$)', url, re.I):
            url = 'https://' + url
        if url and url.startswith('/redirect?'):
            url = 'https://www.youtube.com' + url
        if url and urlsplit(url).hostname in ('www.youtube.com', 'youtube.com'):
            parsed = urlsplit(url)
            if parsed.path == '/redirect':
                destination = parse_qs(parsed.query).get('q', [None])[0]
                # YouTube accepts bare domains and other non-absolute targets.
                # Keep its original redirect when there is no public URL to unwrap.
                url = public_url(destination) or url
        url = public_url(url)
        if url is None:
            # A present link with an unknown shape must not silently disappear.
            raise ResponseShapeError('Unrecognized external link')
        if url not in seen:
            result.append({'title': text(item.get('title')), 'url': url})
            seen.add(url)
    return result


def parse_about(payload, channel_id, overview):
    current = list(nodes(payload, 'aboutChannelViewModel'))
    legacy = list(nodes(payload, 'channelAboutFullMetadataRenderer'))
    matches = [item for item in current + legacy if item.get('channelId') == channel_id]
    if len(matches) != 1:
        raise ResponseShapeError('About response is missing the requested channel')
    about = matches[0]
    result = dict(overview)
    description = text(about.get('description'))
    if description is not None:
        result['description'] = description
    country = text(about.get('country'))
    result.update(subscriber_count=count(about.get('subscriberCountText'), 'subscribers'),
        video_count=count(about.get('videoCountText'), 'videos'),
        view_count=count(about.get('viewCountText'), 'views'),
        country=country, joined_date=joined_date(about.get('joinedDateText')),
        external_links=external_links(about))
    result['handle'] = channel_handle(about.get('canonicalChannelUrl')) or result['handle']
    def invalid_text(value):
        if isinstance(value, str):
            return '\x00' in value
        if isinstance(value, dict):
            return any(invalid_text(item) for item in value.values())
        if isinstance(value, list):
            return any(invalid_text(item) for item in value)
        return False
    if invalid_text(result):
        raise ResponseShapeError('Channel text contains a null character')
    return result


async def fetch_info(client, channel_id, *, client_version=CLIENT_VERSION, timeout=15, stats=None):
    context = {'client': {'clientName':'WEB', 'clientVersion':client_version, 'hl':'en'}}
    result = {'channel_id':channel_id, 'status':'error', 'metadata':None, 'http_requests':0}

    async def request(body, fields):
        result['http_requests'] += 1
        if stats is not None:
            stats['http_requests'] += 1
        async with asyncio.timeout(timeout), client.stream('POST', ENDPOINT,
                params={'prettyPrint':'false', 'fields':fields}, json={'context':context, **body}) as response:
            result['http_status'] = response.status_code
            if response.status_code != 200:
                result['error'] = 'HTTP ' + str(response.status_code)
                result['retry_after'] = response.headers.get('retry-after')
                return None
            data = bytearray()
            async for block in response.aiter_bytes():
                data.extend(block)
                if len(data) > MAX_BYTES:
                    raise ResponseShapeError('Channel response exceeds its size limit')
            return json.loads(data)

    try:
        first = await request({'browseId':channel_id}, OVERVIEW_FIELDS)
        if first is None:
            return result
        overview = parse_overview(first, channel_id)
        endpoint = about_request(first, channel_id)
        second = await request(endpoint, ABOUT_FIELDS)
        if second is None:
            return result
        result.update(status='ok', metadata=parse_about(second, channel_id, overview))
    except (httpx.HTTPError, TimeoutError) as exc:
        bridge = getattr(client, 'catalog_bridge', None)
        if bridge and bridge.local_error(exc):
            raise RuntimeError('The local proxy bridge failed') from exc
        result['error'] = type(exc).__name__
    except (ResponseShapeError, ValueError, TypeError, KeyError) as exc:
        result['error'] = type(exc).__name__ + ': ' + str(exc)[:240]
    finally:
        result['observed_at'] = datetime.now(timezone.utc).isoformat()
    return result
