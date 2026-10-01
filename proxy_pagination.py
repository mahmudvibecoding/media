"""Recognize public list API pagination while retaining the original filters."""
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit


def with_params(url, **changes):
    p = urlsplit(url)
    query = dict(parse_qsl(p.query, keep_blank_values=True))
    for key, value in changes.items():
        if value is None:
            query.pop(key, None)
        else:
            query[key] = str(value)
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(sorted(query.items())), ''))


def as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def next_page(url, document):
    """Return a same-origin next URL, or None. Never interpret cursors as code."""
    if not isinstance(document, dict):
        return None
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    host = parsed.hostname
    data = document.get('data')

    if host == 'freeproxydb.com' and parsed.path == '/api/proxy/search' and isinstance(data, dict):
        rows = data.get('data', [])
        if not isinstance(rows, list) or not rows:
            return None
        total = as_int(data.get('total_count'))
        size = as_int(query.get('page_size'), 10)
        page = as_int(query.get('page_index'), 1)
        # The public API documents a maximum of 100 records per request.
        if size < 100 and total > size:
            return with_params(url, page_size=100, page_index=1)
        return with_params(url, page_index=page + 1) if page * size < total else None

    metadata = document.get('pagination', document.get('meta', document))
    if not isinstance(metadata, dict):
        return None
    # Use page sizes documented by the source or exposed in its public UI.
    preferred = {
        'proxylist.geonode.com': ('limit', 500, 'page'),
        'rola-ip.co': ('pageSize', 500, 'page'),
        'proxylister.com': ('limit', 500, 'page'),
        'api.socks5proxies.com': ('limit', 100, 'offset'),
    }.get(host)
    if preferred and not query.get('cursor'):
        size_key, size, page_key = preferred
        actual = as_int(metadata.get('limit', metadata.get('pageSize')), len(data) if isinstance(data, list) else 50)
        total = as_int(metadata.get('total', document.get('total', document.get('count'))))
        requested = as_int(query.get(size_key), actual)
        if requested < size and (total > actual or bool(document.get('next'))):
            return with_params(url, **{size_key:size, page_key:0 if page_key == 'offset' else 1})

    nxt = document.get('next') or metadata.get('next')
    if isinstance(nxt, str):
        if nxt.startswith(('https://', 'http://', '/', '?')):
            target = urljoin(url, nxt)
            other = urlsplit(target)
            return target if (other.scheme, other.netloc) == (parsed.scheme, parsed.netloc) else None
        if 'cursor' in query or host == 'proxylister.com':
            return with_params(url, cursor=nxt, page=None)

    if host == 'proxylister.com':
        if 'cursor' in query:
            return None
        if isinstance(document.get('results'), list):
            limit = as_int(document.get('limit'), 50)
            if limit and len(document['results']) >= limit:
                return with_params(url, page=as_int(document.get('page'), 1) + 1)
        return None

    total = as_int(metadata.get('total', metadata.get('totalCount', document.get('count'))))
    limit = as_int(metadata.get('limit', metadata.get('pageSize')))
    if 'offset' in metadata and limit > 0:
        offset = as_int(metadata['offset'])
        return with_params(url, offset=offset + limit, limit=limit) if offset + limit < total else None
    if 'page' in metadata and limit > 0:
        page = as_int(metadata['page'], 1)
        pages = as_int(metadata.get('totalPages'))
        more = page < pages if pages else page * limit < total
        return with_params(url, page=page + 1) if more else None
    return None
