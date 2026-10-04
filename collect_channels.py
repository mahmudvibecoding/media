"""Collect every pending channel profile, with direct requests and ranked proxy retries."""
import argparse
import asyncio
from collections import Counter
from contextlib import AsyncExitStack
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import threading
import time
import uuid

import httpx

from channel_info import CLIENT_VERSION, FIELDS, fetch_info
from channel_storage import ChannelWriter, save_input, select_channels
from discovery_pool import ConcurrencyTuner, DiscoveryPool
from discovery_storage import load_ranked_proxies
from proxy_catalog import CatalogClients
from proxy_service_common import file_lock, read_json, write_json
from runtime_config import OUTPUT_DIR, STATE_DIR, connect_database


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def emit(event, **values):
    print(json.dumps(dict(event=event, **values), separators=(',', ':')), flush=True)


class Fetcher:
    def __init__(self, options, direct, clients, ranks, stats, stop):
        self.options, self.direct, self.clients = options, direct, clients
        self.stats, self.stop = stats, stop
        self.pool = DiscoveryPool(ranks) if clients is not None else None
        self.direct_active = 0
        self.direct_limit = options.direct_concurrency
        self.direct_until = 0

    async def fetch(self, channel_id, previous=None):
        previous_index = previous.get('proxy_index') if previous else None
        total = previous.get('attempts', 0) if previous else 0
        for attempt in range(self.options.attempts):
            if self.stop.is_set():
                raise asyncio.CancelledError()
            use_direct = (self.direct is not None and self.direct_active < self.direct_limit and
                          time.monotonic() >= self.direct_until and (attempt == 0 and previous is None or self.pool is None))
            if self.pool is None and not use_direct:
                await asyncio.sleep(max(0.01, min(1, self.direct_until-time.monotonic())))
                while self.direct_active >= self.direct_limit or time.monotonic() < self.direct_until:
                    if self.stop.is_set():
                        raise asyncio.CancelledError()
                    await asyncio.sleep(0.05)
                use_direct = True
            index = None
            if use_direct:
                self.direct_active += 1
                client, route = self.direct, 'direct'
            else:
                index = await self.pool.acquire(previous_index)
                client, route = self.clients[index], 'proxy'
            started = time.monotonic()
            result = None
            try:
                result = await fetch_info(client, channel_id, client_version=self.options.client_version,
                                          timeout=self.options.timeout, stats=self.stats)
            finally:
                if use_direct:
                    self.direct_active -= 1
                else:
                    self.pool.release(index, result['status'] == 'ok' if result else None, time.monotonic()-started)
            self.stats[route+'_attempts'] += 1
            self.stats[route+'_successes' if result['status']=='ok' else route+'_errors'] += 1
            total += 1
            result.update(attempts=total, route=route, proxy_index=index,
                          proxy_id=self.clients.proxies[index].proxy_id if index is not None else None)
            if use_direct and result.get('http_status') in (403, 429):
                delay = result.get('retry_after') or '60'
                delay = max(1, int(delay)) if delay.isdigit() else 60
                if time.monotonic() >= self.direct_until:
                    self.direct_limit = max(1, self.direct_limit//2)
                    emit('direct_backoff', http_status=result['http_status'], seconds=delay, concurrency=self.direct_limit)
                self.direct_until = max(self.direct_until, time.monotonic()+delay)
            if result['status'] == 'ok':
                return result
            previous_index = index
        return result


async def run(options, conn, ids, folder, manifest, stop):
    started = time.monotonic()
    stats = Counter()
    summary = dict(run_id=manifest['run_id'], started_at=manifest['started_at'], input_count=len(ids),
        saved=0, failed=0, processed=0, field_coverage={name:0 for name in FIELDS}, errors={},
        output_directory=str(folder), writer_batches=0, writer_seconds=0.0, concurrency_changes=[])
    proxies, ranks = await asyncio.to_thread(load_ranked_proxies, options.ranked) if ids and options.transport!='direct' else ([], [])
    maximum = options.max_concurrency or max(options.direct_concurrency, len(proxies))
    target = min(options.concurrency or 128, maximum)
    tuner = ConcurrencyTuner(target, maximum, options.tune_seconds)
    summary.update(initial_concurrency=target, peak_concurrency=target, ranked_proxies=len(proxies))
    queue = asyncio.Queue(maxsize=2048)
    recorded, failed, fatal = set(), [], []
    writer = ChannelWriter(conn)
    last_progress = time.monotonic()

    async with AsyncExitStack() as stack:
        direct = None
        if options.transport != 'proxy':
            direct = await stack.enter_async_context(httpx.AsyncClient(http2=True, trust_env=False,
                follow_redirects=False, timeout=httpx.Timeout(options.timeout, connect=options.connect_timeout),
                limits=httpx.Limits(max_connections=options.direct_concurrency, max_keepalive_connections=options.direct_concurrency),
                headers={'User-Agent':'Mozilla/5.0', 'Origin':'https://www.youtube.com', 'Accept-Encoding':'gzip'}))
        clients = await stack.enter_async_context(CatalogClients(proxies, len(proxies),
            connect_timeout=options.connect_timeout, request_timeout=options.timeout,
            per_proxy_connections=1, keepalive_expiry=120)) if proxies else None
        fetcher = Fetcher(options, direct, clients, ranks, stats, stop)

        async def process(items, recovery=False):
            nonlocal target, last_progress
            iterator, pending, exhausted = iter(items), set(), False
            assigned = 0

            async def one(item):
                cid, previous = (item['channel_id'], item) if recovery else (item, None)
                result = await fetcher.fetch(cid, previous)
                if result['status'] != 'ok' and not recovery:
                    failed.append(result)
                else:
                    result['recovery'] = recovery
                    await queue.put(result)

            try:
                while pending or not exhausted:
                    while len(pending) < target and not exhausted and not stop.is_set():
                        item = next(iterator, None)
                        if item is None:
                            exhausted = True
                        else:
                            assigned += 1
                            pending.add(asyncio.create_task(one(item)))
                    if stop.is_set() or not pending:
                        break
                    done, pending = await asyncio.wait(pending, timeout=0.5, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                    now = time.monotonic()
                    if not options.concurrency:
                        changed = tuner.observe(now, summary['saved'], len(items)-assigned, queue.qsize()>1536)
                        if changed:
                            target = changed['concurrency']
                            summary['peak_concurrency'] = max(summary['peak_concurrency'], target)
                            summary['concurrency_changes'].append(changed)
                            emit('concurrency', **changed)
                    if now-last_progress >= 10:
                        progress = dict(summary, **stats, seconds=round(now-started, 2), concurrency=target,
                            active_channels=len(pending), deferred_retries=len(failed), writer_pending=queue.qsize(),
                            saved_per_second=round(summary['saved']/max(0.001, now-started), 2))
                        write_json(folder/'progress.json', progress)
                        emit('progress', **progress)
                        last_progress = now
            finally:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        async def produce():
            await process(ids)
            if failed and not stop.is_set():
                emit('recovery', channels=len(failed))
                await process(failed, recovery=True)
            await queue.put(None)

        async def consume():
            batch = []
            last_flush = time.monotonic()
            with (folder/'results.jsonl').open('w') as output:
                async def flush():
                    nonlocal batch, last_flush
                    if not batch:
                        return
                    if recorded.intersection(row['channel_id'] for row in batch):
                        raise RuntimeError('Channel was finalized twice')
                    began = time.monotonic()
                    saving = asyncio.create_task(asyncio.to_thread(writer.write, batch))
                    try:
                        await asyncio.shield(saving)
                    except asyncio.CancelledError:
                        await saving
                        raise
                    summary['writer_batches'] += 1
                    summary['writer_seconds'] += time.monotonic()-began
                    for result in batch:
                        recorded.add(result['channel_id'])
                        summary['processed'] += 1
                        good = result['status'] == 'ok'
                        summary['saved' if good else 'failed'] += 1
                        if good:
                            for name in FIELDS:
                                summary['field_coverage'][name] += result['metadata'][name] is not None
                        else:
                            error = result.get('error', 'Unknown error')
                            summary['errors'][error] = summary['errors'].get(error, 0)+1
                        output.write(json.dumps({k:v for k,v in result.items() if k!='proxy_index'}, separators=(',', ':'))+'\n')
                    output.flush()
                    os.fsync(output.fileno())
                    batch = []
                    last_flush = time.monotonic()
                while True:
                    try:
                        item = await asyncio.wait_for(queue.get(), options.flush_seconds)
                    except TimeoutError:
                        await flush()
                        continue
                    if item is None:
                        await flush()
                        return
                    batch.append(item)
                    if len(batch) >= options.batch_size or time.monotonic()-last_flush >= options.flush_seconds:
                        await flush()

        emit('started', **summary)
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(produce())
                group.create_task(consume())
        except BaseException as exc:
            stop.set()
            def errors(error):
                if isinstance(error, BaseExceptionGroup):
                    return [name for inner in error.exceptions for name in errors(inner)]
                return [type(error).__name__+': '+str(error)[:500]]
            fatal.extend(errors(exc))
    rows = conn.execute('''SELECT channel_id,metadata_error FROM public.channels
        WHERE channel_id=ANY(%s) AND (metadata_updated_at IS NULL OR metadata_updated_at<%s)
        ORDER BY channel_id''', (ids, manifest['started_at'])).fetchall()
    with (folder/'unresolved.jsonl').open('w') as output:
        for cid, error in rows:
            output.write(json.dumps(dict(channel_id=cid, error=error))+'\n')
        output.flush()
        os.fsync(output.fileno())
    elapsed = time.monotonic()-started
    summary.update(**stats, finished_at=utcnow(), seconds=round(elapsed, 3), interrupted=stop.is_set(),
        fatal_errors=fatal, unprocessed=len(ids)-len(recorded), database_unresolved=len(rows),
        database_completed=len(ids)-len(rows), final_concurrency=target,
        saved_per_second=round(summary['saved']/max(0.001, elapsed), 2))
    write_json(folder/'summary.json', summary)
    write_json(STATE_DIR/'channels-last.json', summary)
    write_json(STATE_DIR/'channels-active.json', dict(active=bool(stop.is_set() or fatal or summary['unprocessed']), folder=str(folder)))
    emit('finished', **summary)
    return 2 if rows or fatal or stop.is_set() else 0


def collect(options):
    active = read_json(STATE_DIR/'channels-active.json', {})
    resume = options.resume or (Path(active['folder']) if active.get('active') and not options.refresh else None)
    if resume and options.limit:
        raise ValueError('A resumed run uses its frozen selection')
    with connect_database('media', autocommit=True, application_name='channel-profile-writer') as conn:
        # Subscriber-only writes must not overlap the profile snapshot.
        for name in ('media.channel-info', 'media.subscriber-count'):
            if not conn.execute('SELECT pg_try_advisory_lock(hashtext(%s))', (name,)).fetchone()[0]:
                raise RuntimeError('Another channel collector is running')
        ids, cutoff = select_channels(conn, resume=resume, refresh=options.refresh, limit=options.limit)
        run_id, started = str(uuid.uuid4()), utcnow()
        folder = options.output or OUTPUT_DIR/('channels-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+run_id[:8])
        folder.mkdir(parents=True, exist_ok=False)
        manifest = dict(version=1, run_id=run_id, started_at=cutoff or started,
            invocation_started_at=started, resumed_from=str(resume) if resume else None, refresh=options.refresh)
        save_input(folder, ids, manifest)
        write_json(STATE_DIR/'channels-active.json', dict(active=True, folder=str(folder)))
        stop = threading.Event()
        handlers = {sig:signal.signal(sig, lambda *_:stop.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            return asyncio.run(run(options, conn, ids, folder, manifest, stop))
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    select = result.add_mutually_exclusive_group()
    select.add_argument('--resume', type=Path)
    select.add_argument('--refresh', action='store_true')
    result.add_argument('--limit', type=int, default=0)
    result.add_argument('--output', type=Path)
    result.add_argument('--transport', choices=('auto','direct','proxy'), default='auto')
    result.add_argument('--ranked', type=Path, default=STATE_DIR/'proxy-service/ranked-proxies.jsonl')
    result.add_argument('--concurrency', type=int, default=0, help='Zero tunes saved profiles per second')
    result.add_argument('--max-concurrency', type=int, default=0)
    result.add_argument('--direct-concurrency', type=int, default=512)
    result.add_argument('--attempts', type=int, default=3, help='Attempts per pass, with one recovery pass')
    result.add_argument('--timeout', type=float, default=15)
    result.add_argument('--connect-timeout', type=float, default=3)
    result.add_argument('--batch-size', type=int, default=500)
    result.add_argument('--flush-seconds', type=float, default=1)
    result.add_argument('--tune-seconds', type=float, default=15)
    result.add_argument('--client-version', default=os.environ.get('YOUTUBE_CLIENT_VERSION', CLIENT_VERSION))
    return result


def main():
    options = parser().parse_args()
    if (min(options.limit, options.concurrency, options.max_concurrency)<0 or
            min(options.direct_concurrency, options.attempts, options.timeout, options.connect_timeout,
                options.batch_size, options.flush_seconds, options.tune_seconds)<=0):
        raise ValueError('Invalid channel collection limits')
    os.umask(0o077)
    with file_lock(STATE_DIR/'channels.lock'):
        return collect(options)


if __name__ == '__main__':
    raise SystemExit(main())
