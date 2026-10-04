"""Download public proxy lists and upsert connection configurations.

No listed proxy is contacted. Connections and each list's latest download state
commit together. Resume the current collection with --run-id UUID.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import gzip
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import socket
import time
from urllib.parse import urljoin, urlsplit
import uuid

from datetime import datetime, timezone
import httpx
import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from proxy_formats import PARSER_VERSION, Proxy, canonical_json, pack_connection_settings, parse_proxies
from proxy_pagination import next_page
from proxy_service_common import write_json
from runtime_config import ROOT, STATE_DIR, connect_database

STORAGE = STATE_DIR / 'proxy-collection'
DOWNLOAD_COLUMNS = (
    'status', 'started_at', 'finished_at', 'http_status', 'attempts', 'final_url',
    'content_type', 'decoded_bytes', 'received_body_bytes', 'content_sha256', 'payload_path',
    'entries_found', 'unique_entries', 'duplicates_in_list', 'invalid_entries',
    'parser_details', 'error_type', 'elapsed_seconds',
    'new_configurations', 'existing_configurations',
)


def now():
    return datetime.now(timezone.utc)


def output(value):
    print(json.dumps(value, default=str, ensure_ascii=False), flush=True)


def stored_settings(settings, protocol):
    return Jsonb(pack_connection_settings(protocol, settings))


def connection():
    return connect_database("proxy", autocommit=True, row_factory=dict_row)


def format_hint_for_url(url):
    return Path(urlsplit(url).path).suffix.lower().removeprefix('.') or None


def parsed_cache_path(version, checksum, hints):
    key = hashlib.sha256(canonical_json([version, checksum, hints]).encode()).hexdigest()
    return STORAGE / 'parsed' / (key + '.jsonl.gz')


def same_parsed_entries(left, right):
    if left == right:
        return True
    with gzip.open(left, 'rb') as a, gzip.open(right, 'rb') as b:
        a.readline()
        b.readline()
        while True:
            chunk = a.read(1024 * 1024)
            if chunk != b.read(1024 * 1024):
                return False
            if not chunk:
                return True


class Store:
    def __init__(self, exclusive_owner=None):
        self.control = connection()
        if exclusive_owner is None and not self.control.execute(
            "SELECT pg_try_advisory_lock(hashtextextended('media:proxy_service',0)) AS locked"
        ).fetchone()['locked']:
            self.control.close()
            raise RuntimeError('Another proxy refresh or test is running')
        locked = self.control.execute(
            "SELECT pg_try_advisory_lock(hashtextextended('proxy:proxy_collection',0)) AS locked"
        ).fetchone()['locked']
        if not locked:
            self.control.close()
            raise RuntimeError('Another proxy collector is already running')
        self.writer = connection()
        self.writer.execute("SET lock_timeout = '30s'")
        self.writer.execute('''CREATE TEMP TABLE proxy_stage (
            connection_key BYTEA PRIMARY KEY, address TEXT, port INTEGER,
            connection_settings JSONB
        ) ON COMMIT DELETE ROWS''')
        self.run_id = None

    def prepare(self, args):
        if args.run_id:
            self.run_id = uuid.UUID(args.run_id)
            exists = self.control.execute('SELECT 1 FROM proxy_lists WHERE run_id=%s LIMIT 1',
                                          (self.run_id,)).fetchone()
            if not exists:
                raise ValueError('Unknown or superseded collection run')
            if getattr(args,'auto_resume',False):
                # Include sources added by a newer catalog while finishing the
                # earlier source pass; completed URLs in that pass stay saved.
                self.control.execute("""UPDATE proxy_lists SET run_id=%s,status='pending',fetched_at=NULL,fetch_state='{}'
                    WHERE enabled AND kind IN ('feed_candidate','api_candidate') AND run_id IS DISTINCT FROM %s""",
                    (self.run_id,self.run_id))
                self.control.execute("""UPDATE proxy_lists SET run_id=NULL,status='not_checked'
                    WHERE run_id=%s AND status='pending' AND
                    (NOT enabled OR kind NOT IN ('feed_candidate','api_candidate'))""",(self.run_id,))
            if args.retry_failures:
                self.control.execute("""UPDATE proxy_lists SET status='pending'
                    WHERE run_id=%s AND status NOT IN ('collected','pending')""", (self.run_id,))
        else:
            if self.control.execute("SELECT 1 FROM proxy_lists WHERE run_id IS NOT NULL AND status='pending' LIMIT 1").fetchone():
                raise ValueError('Resume the unfinished collection before starting another')
            self.run_id = uuid.uuid4()
            self.control.execute("""UPDATE proxy_lists SET run_id=%s,status='pending',fetched_at=NULL,fetch_state='{}'
                WHERE enabled AND kind IN ('feed_candidate','api_candidate')""", (self.run_id,))
        query = "SELECT * FROM proxy_lists WHERE run_id=%s AND status='pending' ORDER BY url"
        params = (self.run_id,)
        if args.reparse or args.complete_pagination:
            query = """SELECT * FROM proxy_lists WHERE run_id=%s
                AND fetch_state->>'payload_path' IS NOT NULL
                AND status IN ('collected','collected_partial','no_entries')
                AND (%s=false OR kind='api_candidate') ORDER BY url"""
            params = (self.run_id, args.complete_pagination)
        rows = []
        for saved in self.control.execute(query, params).fetchall():
            row = dict(saved['fetch_state'], list_url=saved['url'],
                       protocol_hints=saved['protocol_hints'],format_hint=format_hint_for_url(saved['url']),
                       status=saved['status'],finished_at=saved['fetched_at'])
            if isinstance(row.get('started_at'), str):
                row['started_at'] = datetime.fromisoformat(row['started_at'])
            rows.append(row)
        # Start with several formats to expose parsing failures early; then interleave hosts.
        by_host = collections.defaultdict(collections.deque)
        priority = []
        seen_format = set()
        for row in rows:
            fmt = (row['format_hint'], tuple(row['protocol_hints']))
            if len(priority) < 24 and fmt not in seen_format:
                priority.append(row)
                seen_format.add(fmt)
            else:
                by_host[urlsplit(row['list_url']).hostname].append(row)
        ordered = priority
        while by_host:
            for host in list(by_host):
                ordered.append(by_host[host].popleft())
                if not by_host[host]:
                    del by_host[host]
        if args.take:
            ordered = ordered[:args.take]
        return ordered

    def parsed_cache(self, result):
        hints = result['protocol_hints']
        cache = parsed_cache_path(PARSER_VERSION, result['content_sha256'], hints)
        cache.parent.mkdir(parents=True, exist_ok=True)
        if not cache.exists():
            with gzip.open(result['payload_path'], 'rb') as f:
                parsed = parse_proxies(f.read(), hints, result.get('content_type', ''))
            temp = cache.with_suffix('.tmp')
            try:
                with gzip.open(temp, 'wt', encoding='utf-8', compresslevel=3) as f:
                    f.write(canonical_json(parsed.summary()) + '\n')
                    for proxy in parsed.proxies.values():
                        f.write(canonical_json(proxy.as_dict()) + '\n')
                temp.replace(cache)
            finally:
                temp.unlink(missing_ok=True)
        with gzip.open(cache, 'rt', encoding='utf-8') as f:
            metrics = json.loads(next(f))
        return cache, metrics

    def save(self, result):
        cache = None
        partial = result['status'] == 'downloaded_partial'
        if result['status'] in {'downloaded', 'downloaded_partial'}:
            cache, metrics = self.parsed_cache(result)
            for key in ('entries_found', 'unique_entries', 'duplicates_in_list', 'invalid_entries'):
                result[key] = metrics[key]
            result['parser_details'] = dict(metrics)
            if result.get('download_details'):
                result['parser_details']['download_details'] = result['download_details']
            result['status'] = ('collected_partial' if partial else 'collected') if metrics['unique_entries'] else 'no_entries'
            if metrics['warnings'].get('html_response'):
                result['status'] = 'unexpected_html'
        observed = result['finished_at']
        with self.writer.transaction():
            self.writer.execute('TRUNCATE proxy_stage')
            unchanged_entries = False
            result['new_configurations'] = 0
            if result.get('reparse') and cache:
                previous = self.writer.execute('SELECT fetch_state,fetched_at FROM proxy_lists WHERE run_id=%s AND url=%s',
                                               (self.run_id,result['list_url'])).fetchone()
                if previous and previous['fetched_at'] == observed:
                    state = previous['fetch_state']
                    if state.get('content_sha256') == result['content_sha256']:
                        version = (state.get('parser_details') or {}).get('parser_version')
                        old_cache = parsed_cache_path(version, result['content_sha256'], result['protocol_hints'])
                        unchanged_entries = old_cache.exists() and same_parsed_entries(old_cache, cache)
            if cache and result['unique_entries'] and not unchanged_entries:
                with self.writer.cursor().copy('''COPY proxy_stage
                    (connection_key,address,port,connection_settings) FROM STDIN''') as cp:
                    with gzip.open(cache, 'rt', encoding='utf-8') as f:
                        next(f)
                        for line in f:
                            row = json.loads(line)
                            proxy = Proxy(row['address'], row['port'], row['protocol'], row['settings'])
                            cp.write_row((proxy.key, proxy.address, proxy.port,
                                          stored_settings(proxy.settings, proxy.protocol)))
                self.writer.execute('ANALYZE proxy_stage')
                result['new_configurations'] = self.writer.execute('''WITH added AS (INSERT INTO proxies
                    (connection_key,address,port,connection_settings,last_seen_at)
                    SELECT s.connection_key,s.address,s.port,s.connection_settings,%s FROM proxy_stage s
                    WHERE NOT EXISTS(SELECT 1 FROM proxies p WHERE p.connection_key=s.connection_key)
                    ON CONFLICT (connection_key) DO NOTHING RETURNING proxy_id)
                    SELECT count(*) AS n FROM added''', (observed,)).fetchone()['n']
                self.writer.execute('''UPDATE proxies p SET last_seen_at=%s FROM proxy_stage s
                    WHERE p.connection_key=s.connection_key AND p.last_seen_at<%s''', (observed,observed))
            result['existing_configurations'] = (result.get('unique_entries') or 0)-result['new_configurations']
            # Membership history is intentionally absent. Reparsing can add corrected
            # configurations but cannot prove an old one is safe to delete.
            state = {key: result.get(key) for key in DOWNLOAD_COLUMNS
                     if key not in ('status','finished_at') and result.get(key) is not None}
            state = json.loads(json.dumps(state, default=lambda value: value.isoformat()))
            updated = self.writer.execute("""UPDATE proxy_lists SET status=%s,fetched_at=%s,fetch_state=%s
                WHERE run_id=%s AND url=%s""",
                (result['status'],observed,Jsonb(state),self.run_id,result['list_url']))
            if updated.rowcount != 1:
                raise RuntimeError('Current list record missing; proxy observations rolled back')
        return {'status': result['status'], 'unique_entries': result.get('unique_entries', 0) or 0,
                'new_configurations': result['new_configurations']}

    def progress(self):
        rows = self.control.execute('SELECT status,count(*) AS n FROM proxy_lists WHERE run_id=%s GROUP BY status ORDER BY status',
                                    (self.run_id,)).fetchall()
        return {r['status']: r['n'] for r in rows}

    def finish(self, failed=False, inventory=True):
        statuses = self.progress()
        summary = dict(self.control.execute("""SELECT count(*) AS selected_urls,
            count(*) FILTER (WHERE status IN ('collected','collected_partial')) AS urls_with_imported_entries,
            count(*) FILTER (WHERE fetch_state->>'payload_path' IS NOT NULL) AS downloaded_urls,
            sum((fetch_state->>'entries_found')::bigint)::bigint AS entries_found,
            sum((fetch_state->>'unique_entries')::bigint)::bigint AS unique_entries_summed_across_lists,
            sum((fetch_state->>'decoded_bytes')::bigint)::bigint AS decoded_response_bytes,
            sum((fetch_state->>'attempts')::bigint)::bigint AS download_attempts,
            coalesce(sum((fetch_state->>'new_configurations')::bigint),0)::bigint AS new_configurations,
            count(*) FILTER (WHERE status NOT IN ('collected','no_entries','pending')) AS source_errors,
            count(*) FILTER (WHERE status='pending') AS pending_sources
            FROM proxy_lists WHERE run_id=%s""", (self.run_id,)).fetchone())
        summary.update(statuses=statuses,run_id=str(self.run_id),
                       status='failed' if failed else ('unfinished' if statuses.get('pending') else 'completed'))
        if inventory:
            summary['database_totals'] = self.control.execute("""SELECT count(*) AS unique_connections,
            count(DISTINCT (address,port)) AS unique_addresses_and_ports,
            (SELECT count(*) FROM proxy_stats WHERE connection_attempts > 0) AS individually_tested
            FROM proxies""").fetchone()
            summary['protocol_counts'] = {r['protocol']: r['n'] for r in self.control.execute(
                "SELECT connection_settings->>'transport' AS protocol,count(*) AS n "
                "FROM proxies GROUP BY connection_settings->>'transport' ORDER BY count(*) DESC").fetchall()}
        path = STORAGE / 'runs' / str(self.run_id) / 'summary.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        summary['summary_path'] = str(path)
        summary['failed_sources_path'] = str(path.with_name('failed-sources.jsonl'))
        with path.with_name('failed-sources.jsonl').open('w') as output_file:
            for row in self.control.execute('''SELECT url,status,fetch_state->>'error_type' AS error_type
                FROM proxy_lists WHERE run_id=%s AND status NOT IN ('collected','no_entries') ORDER BY url''',
                (self.run_id,)):
                output_file.write(json.dumps(row)+'\n')
            output_file.flush()
            os.fsync(output_file.fileno())
        write_json(path,summary)
        return summary

    def close(self):
        self.writer.close()
        self.control.close()


class Downloader:
    def __init__(self, args):
        self.args = args
        self.host_limits = {}
        self.global_limit = asyncio.Semaphore(args.concurrency)
        self.rate_limited = set()
        self.dns = {}
        self.client = httpx.AsyncClient(
            http2=True, follow_redirects=False, trust_env=False,
            timeout=httpx.Timeout(30, connect=10),
            limits=httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency),
            headers={'User-Agent': 'ProxyListCollector/1.0 (public list download)',
                     'Accept-Encoding': 'gzip, deflate', 'Accept': 'text/plain,application/json,*/*;q=0.8'})

    async def public_url(self, url):
        try:
            p = urlsplit(url)
            if p.scheme not in ('https', 'http') or not p.hostname or p.username is not None or p.password is not None:
                return False
            if p.port is not None and not 1 <= p.port <= 65535:
                return False
            host = p.hostname
            if host in self.dns:
                return self.dns[host]
            addresses = await asyncio.wait_for(asyncio.get_running_loop().getaddrinfo(host, None, type=socket.SOCK_STREAM), 8)
            allowed = bool(addresses) and all(ipaddress.ip_address(a[4][0]).is_global for a in addresses)
            self.dns[host] = allowed
            return allowed
        except (ValueError, OSError, TimeoutError):
            return False

    async def fetch_attempt(self, row, result, temp):
        current = row['list_url']
        redirects = []
        for _ in range(7):
            host = urlsplit(current).hostname
            if host in self.rate_limited:
                result['status'] = 'host_rate_limited'
                return
            limit = self.host_limits.setdefault(host, asyncio.Semaphore(
                self.args.github_concurrency if host == 'raw.githubusercontent.com' else self.args.per_host))
            async with limit, self.global_limit:
                if host in self.rate_limited:
                    result['status'] = 'host_rate_limited'
                    return
                if not await self.public_url(current):
                    result['status'] = 'source_unreachable_or_not_public'
                    return
                # The total request timeout begins after waiting for a concurrency slot.
                async with asyncio.timeout(180), self.client.stream('GET', current) as response:
                    try:
                        result.update(http_status=response.status_code, final_url=str(response.url),
                                      content_type=response.headers.get('content-type', ''))
                        if response.status_code in (301, 302, 303, 307, 308) and response.headers.get('location'):
                            current = urljoin(current, response.headers['location'])
                            redirects.append(response.status_code)
                            continue
                        if response.status_code == 429:
                            self.rate_limited.add(host)
                            self.rate_limited.add(urlsplit(row['list_url']).hostname)
                            result['status'] = 'http_429'
                            return
                        if response.status_code != 200 or response.headers.get('content-range'):
                            result['status'] = 'http_' + str(response.status_code)
                            return
                        digest = hashlib.sha256()
                        decoded = 0
                        with gzip.open(temp, 'wb', compresslevel=3) as f:
                            async for block in response.aiter_bytes():
                                decoded += len(block)
                                if decoded > self.args.max_bytes:
                                    result['status'] = 'size_limit_exceeded'
                                    result['decoded_bytes'] = decoded
                                    return
                                digest.update(block)
                                f.write(block)
                        result['decoded_bytes'] = decoded
                        checksum = digest.hexdigest()
                        target = STORAGE / 'payloads' / (checksum + '.gz')
                        if target.exists():
                            temp.unlink(missing_ok=True)
                        else:
                            temp.replace(target)
                        result.update(status='downloaded', content_sha256=checksum, payload_path=str(target))
                        return
                    finally:
                        result['received_body_bytes'] += response.num_bytes_downloaded
        result['status'] = 'too_many_redirects'

    async def fetch_one(self, row):
        result = {'list_url': row['list_url'], 'protocol_hints': row['protocol_hints'],
                  'attempts': 0, 'received_body_bytes': 0}
        filename = hashlib.sha256(row['list_url'].encode()).hexdigest() + '.part.gz'
        temp = STORAGE / 'tmp' / filename
        start = time.monotonic()
        result['started_at'] = now()
        try:
            for attempt in range(self.args.retries + 1):
                result['attempts'] = attempt + 1
                result.pop('error_type', None)
                try:
                    await self.fetch_attempt(row, result, temp)
                except (httpx.HTTPError, TimeoutError, OSError) as exc:
                    result.update(status='download_error', error_type=type(exc).__name__)
                finally:
                    temp.unlink(missing_ok=True)
                if result['status'] not in {'download_error', 'http_500', 'http_502', 'http_503', 'http_504'}:
                    break
                if attempt < self.args.retries:
                    await asyncio.sleep(1 + attempt)
            result['finished_at'] = now()
            result['elapsed_seconds'] = round(time.monotonic() - start, 3)
            return result
        finally:
            temp.unlink(missing_ok=True)

    async def complete_pages(self, result, row):
        if not result.get('payload_path'):
            return result
        try:
            with gzip.open(result['payload_path'], 'rb') as f:
                if not f.read(2048).lstrip().startswith((b'{',b'[')):
                    return result
                f.seek(0)
                document = json.load(f)
        except (ValueError, UnicodeError):
            return result
        initial_url = result.get('final_url') or row['list_url']
        following = next_page(initial_url, document)
        if not following:
            return result
        pages = [(initial_url, result['payload_path'])]
        total_decoded = result.get('decoded_bytes') or 0
        visited = {initial_url}
        digests = {result['content_sha256']}
        error = None
        started = time.monotonic()
        while following:
            if following in visited:
                error = 'pagination_cycle'
                break
            if len(pages) >= 2000:
                error = 'pagination_page_limit'
                break
            visited.add(following)
            page = await self.fetch_one({'list_url': following, 'protocol_hints': row['protocol_hints']})
            result['attempts'] = (result.get('attempts') or 0) + page['attempts']
            result['received_body_bytes'] = (result.get('received_body_bytes') or 0) + page['received_body_bytes']
            if page['status'] != 'downloaded':
                error = page['status']
                break
            if page['content_sha256'] in digests:
                error = 'pagination_repeated_response'
                break
            digests.add(page['content_sha256'])
            try:
                with gzip.open(page['payload_path'], 'rb') as f:
                    document = json.load(f)
            except (ValueError, UnicodeError):
                error = 'pagination_non_json_response'
                break
            if not isinstance(document, dict):
                error = 'pagination_unexpected_shape'
                break
            if urlsplit(following).hostname == 'freeproxydb.com' and document.get('status') != 1:
                error = 'pagination_api_error'
                break
            if total_decoded + (page.get('decoded_bytes') or 0) > self.args.max_bytes:
                error = 'pagination_size_limit'
                break
            total_decoded += page.get('decoded_bytes') or 0
            pages.append((following, page['payload_path']))
            following = next_page(following, document)
            # Avoid bursts during walks of a public API.
            if following:
                await asyncio.sleep(0.1)
        # Combine complete page responses into one parseable JSON document.
        temp = STORAGE / 'tmp' / (uuid.uuid4().hex + '.pages.gz')
        digest = hashlib.sha256()
        size = 0
        try:
            with gzip.open(temp, 'wb', compresslevel=3) as out:
                def write(block):
                    nonlocal size
                    out.write(block)
                    digest.update(block)
                    size += len(block)
                write(b'[')
                for i, (_, path) in enumerate(pages):
                    if i:
                        write(b',')
                    with gzip.open(path, 'rb') as source:
                        while block := source.read(1024 * 1024):
                            write(block)
                write(b']')
            checksum = digest.hexdigest()
            target = STORAGE / 'payloads' / (checksum + '.gz')
            if target.exists():
                temp.unlink()
            else:
                temp.replace(target)
        finally:
            temp.unlink(missing_ok=True)
        result.update(status='downloaded_partial' if error else 'downloaded',
                      payload_path=str(target), content_sha256=checksum, decoded_bytes=size,
                      finished_at=now(), elapsed_seconds=(result.get('elapsed_seconds') or 0) + time.monotonic() - started,
                      download_details={'pages': len(pages), 'page_urls': [u for u, _ in pages],
                                        'pagination_complete': not error, 'pagination_error': error})
        return result

    async def fetch(self, row):
        result = await self.fetch_one(row)
        if result['status'] == 'downloaded':
            result = await self.complete_pages(result, row)
        return result


async def main(args, *, exclusive_owner=None, stop=None, on_progress=None):
    os.umask(0o077)
    for sub in ('payloads', 'parsed', 'tmp', 'runs'):
        (STORAGE / sub).mkdir(parents=True, exist_ok=True)
    store = Store(exclusive_owner=exclusive_owner)
    downloader = Downloader(args)
    began = time.monotonic()
    queue = asyncio.Queue(maxsize=max(2, args.concurrency))
    finished = 0
    failed = False
    try:
        if getattr(args, 'auto_resume', False) and not args.run_id:
            pending = store.control.execute('''SELECT DISTINCT run_id FROM proxy_lists
                WHERE run_id IS NOT NULL AND status='pending' ''').fetchall()
            if len(pending) > 1:
                raise ValueError('Several unfinished source runs require explicit recovery')
            if pending:
                args.run_id = str(pending[0]['run_id'])
        work = store.prepare(args)
        output({'run_id': str(store.run_id), 'urls_to_process': len(work),
                'concurrency': args.concurrency, 'github_concurrency': args.github_concurrency,
                'per_other_host': args.per_host, 'resume_command': f'.venv/bin/python collect_proxies.py --run-id {store.run_id}'})
        async def download(row):
            if args.reparse or args.complete_pagination:
                previous_details = (row.get('parser_details') or {}).get('download_details')
                result = dict(row, status='downloaded_partial' if row['status']=='collected_partial' else 'downloaded',
                              reparse=True, download_details=previous_details)
                if args.complete_pagination:
                    result = await downloader.complete_pages(result, row)
            else:
                result = await downloader.fetch(row)
            await queue.put(result)

        async def producer():
            async with asyncio.TaskGroup() as group:
                for row in work:
                    group.create_task(download(row))
            await queue.put(None)

        async def consumer():
            nonlocal finished
            while True:
                result = await queue.get()
                if result is None:
                    return
                saving = asyncio.create_task(asyncio.to_thread(store.save, result))
                try:
                    await asyncio.shield(saving)
                except asyncio.CancelledError:
                    await saving
                    raise
                finished += 1

        async def progress():
            while True:
                await asyncio.sleep(10)
                values = {'saved_this_invocation': finished, 'selected_this_invocation': len(work),
                          'elapsed_seconds': round(time.monotonic() - began, 1), 'statuses': store.progress()}
                output(values)
                if on_progress:
                    on_progress(values)

        async def collect_tasks():
            async with asyncio.TaskGroup() as group:
                group.create_task(producer())
                group.create_task(consumer())

        async def wait_for_stop():
            while stop is None or not stop.is_set():
                await asyncio.sleep(0.2)

        progress_task = asyncio.create_task(progress())
        collection_task = asyncio.create_task(collect_tasks())
        stop_task = asyncio.create_task(wait_for_stop())
        try:
            done,_ = await asyncio.wait((collection_task,stop_task),return_when=asyncio.FIRST_COMPLETED)
            if stop_task in done:
                collection_task.cancel()
                await asyncio.gather(collection_task,return_exceptions=True)
                raise InterruptedError('Source refresh stopped; completed source commits were preserved')
            await collection_task
        finally:
            for task in (progress_task,collection_task,stop_task):
                task.cancel()
            await asyncio.gather(progress_task,collection_task,stop_task,return_exceptions=True)
    except BaseException:
        failed = True
        raise
    finally:
        try:
            await downloader.client.aclose()
            if store.run_id is not None:
                summary = store.finish(failed=failed,inventory=getattr(args,'inventory',True))
                summary['invocation_seconds'] = round(time.monotonic() - began, 2)
                write_json(summary['summary_path'],summary)
                output(summary)
        finally:
            store.close()
    return summary


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-id', help='Resume a saved run and process its pending list URLs')
    parser.add_argument('--take', type=int, help='Process only this many pending URLs, leaving the rest for resume')
    parser.add_argument('--retry-failures', action='store_true', help='Requeue failed URLs in the specified run')
    parser.add_argument('--reparse', action='store_true', help='Reparse current saved payloads and upsert configurations without downloading')
    parser.add_argument('--complete-pagination', action='store_true', help='Follow remaining API pages from saved responses')
    parser.add_argument('--concurrency', type=int, default=24)
    parser.add_argument('--per-host', type=int, default=2)
    parser.add_argument('--github-concurrency', type=int, default=8)
    parser.add_argument('--retries', type=int, default=2)
    parser.add_argument('--max-bytes', type=int, default=256 * 1024 * 1024,
                        help='Maximum decoded bytes per list; oversized responses are recorded as incomplete')
    return parser


if __name__ == '__main__':
    parser = argument_parser()
    parsed_args = parser.parse_args()
    if min(parsed_args.concurrency, parsed_args.per_host, parsed_args.github_concurrency, parsed_args.max_bytes) < 1:
        parser.error('Concurrency and maximum size must be positive')
    if parsed_args.retries < 0 or (parsed_args.take is not None and parsed_args.take < 1):
        parser.error('Invalid retry count or take count')
    if parsed_args.retry_failures and not parsed_args.run_id:
        parser.error('--retry-failures requires --run-id')
    if (parsed_args.reparse or parsed_args.complete_pagination) and not parsed_args.run_id:
        parser.error('Maintenance modes require --run-id')
    if sum([parsed_args.retry_failures, parsed_args.reparse, parsed_args.complete_pagination]) > 1:
        parser.error('Choose only one maintenance mode')
    try:
        asyncio.run(main(parsed_args))
    except Exception as exc:
        # PostgreSQL COPY exceptions can embed settings containing credentials.
        # Report exception types without printing input data in a traceback.
        def exception_types(error):
            if isinstance(error, BaseExceptionGroup):
                return [name for inner in error.exceptions for name in exception_types(inner)]
            return [type(error).__name__]
        output({'collector_failed': exception_types(exc), 'progress_saved': True})
        raise SystemExit(1)
