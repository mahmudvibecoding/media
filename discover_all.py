"""Discover new Videos and Shorts for every saved channel using ranked proxies."""
import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import multiprocessing
import os
from pathlib import Path
import queue
import signal
import sys
import time
import traceback
import uuid

from discover_videos import CLIENT_VERSION, fetch_page, scan_tab, select_tabs
from discovery_pool import ConcurrencyTuner, DiscoveryPool
from discovery_storage import BatchWriter, load_inventory, load_ranked_proxies
from proxy_catalog import CatalogClients
from proxy_service_common import file_lock, write_json
from runtime_config import OUTPUT_DIR, STATE_DIR, connect_database


USABLE = {'ok', 'empty', 'tab_absent'}
COUNTERS = ('http_attempts', 'usable_pages', 'failed_pages', 'neutral_pages', 'request_seconds',
            'recovery_tabs')


def emit(event, **values):
    print(json.dumps(dict(event=event, **values), separators=(',', ':')), flush=True)


async def fetch_using_pool(clients, pool, stats, channel, kind, continuation, options, stop):
    previous = None
    totals = Counter()
    started = time.monotonic()
    for attempt in range(options.attempts):
        if stop.is_set():
            raise asyncio.CancelledError()
        index = await pool.acquire(previous)
        before = time.monotonic()
        try:
            async with asyncio.timeout(options.timeout):
                page = await fetch_page(clients[index], channel, kind, options.client_version,
                                        retries=0, continuation=continuation)
        except TimeoutError:
            page = dict(channel_id=channel, type=kind, status='error', error='RequestDeadline',
                        videos=[], continuation=None, complete_tab=False, latest_verified=False,
                        attempts=1, request_body_bytes=0, response_body_bytes=0, decoded_body_bytes=0)
        except BaseException:
            pool.release(index, None, time.monotonic()-before)
            raise
        seconds = time.monotonic()-before
        if page.get('local_error'):
            pool.release(index, None, seconds)
            raise RuntimeError('The local proxy bridge failed; proxy scores were not penalized')
        usable = True if page['status'] in USABLE else None if page['status'] == 'unavailable' else False
        pool.release(index, usable, seconds)
        stats['http_attempts'] += 1
        stats['request_seconds'] += seconds
        stats['usable_pages' if usable is True else 'neutral_pages' if usable is None else 'failed_pages'] += 1
        for key in ('attempts', 'request_body_bytes', 'response_body_bytes', 'decoded_body_bytes'):
            totals[key] += page[key]
        page['proxy_id'] = clients.proxies[index].proxy_id
        if usable is not False:
            break
        previous = index
    page.update(totals, seconds=round(time.monotonic()-started, 4))
    return page


async def run_worker(number, count, jobs, known, proxies, ranks, next_job, target, messages, stop, options,
                     progress_slot=None):
    stats = Counter({key: 0 for key in COUNTERS})
    pool = DiscoveryPool(ranks)
    failed = []

    def progress():
        if progress_slot is not None:
            with progress_slot.get_lock():
                progress_slot.get_obj()[:] = [stats[key] for key in COUNTERS] + [len(pool.active)]
            return
        message = dict(event='progress', worker=number, counts=dict(stats), active=len(pool.active))
        try:
            messages.put_nowait(message)
        except queue.Full:
            pass

    async def report():
        while True:
            await asyncio.sleep(1)
            progress()

    async with CatalogClients(proxies, len(proxies), connect_timeout=options.connect_timeout,
                              request_timeout=options.timeout, per_proxy_connections=1,
                              keepalive_expiry=120) as clients:
        async def fetch(channel, kind, continuation):
            return await fetch_using_pool(clients, pool, stats, channel, kind, continuation, options, stop)

        async def scan(job, recovery=False):
            channel, kind = job
            before = known.get(job, set())
            result = await scan_tab(None, None, channel, kind, options.client_version,
                                    max_pages=0, stop=stop, known_ids=before, fetch=fetch, persist=False)
            returned = len(result['videos'])
            result['videos'] = [v for v in result['videos'] if v['video_id'] not in before]
            result['videos_already_present'] = returned-len(result['videos'])
            result['recovery'] = recovery
            if not result['scan_complete'] and not recovery and not stop.is_set():
                failed.append(job)
            else:
                await asyncio.to_thread(messages.put, dict(event='result', worker=number, result=result))

        async def process(recovery=False):
            pending = set()
            recovery_jobs = iter(failed) if recovery else None
            exhausted = False
            try:
                while pending or not exhausted:
                    limit = min(len(proxies), max(1, (target.value+count-1-number)//count))
                    while len(pending) < limit and not exhausted and not stop.is_set():
                        if recovery:
                            job = next(recovery_jobs, None)
                        else:
                            with next_job.get_lock():
                                position = next_job.value
                                next_job.value += int(position < len(jobs))
                            job = jobs[position] if position < len(jobs) else None
                        if job is None:
                            exhausted = True
                            break
                        if recovery:
                            stats['recovery_tabs'] += 1
                        pending.add(asyncio.create_task(scan(job, recovery)))
                    if stop.is_set():
                        break
                    if not pending:
                        break
                    done, pending = await asyncio.wait(pending, timeout=0.5,
                                                       return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
            finally:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        reporter = asyncio.create_task(report())
        try:
            await process()
            if failed and not stop.is_set():
                await process(recovery=True)
        finally:
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)
            progress()


def worker_entry(number, count, jobs, known, proxies, ranks, next_job, target, messages, stop, options,
                 progress_slot=None):
    if sys.platform.startswith('linux'):
        os.setsid()
    cpus = getattr(os, 'process_cpu_count', os.cpu_count)() or 2
    os.environ.setdefault('GOMAXPROCS', str(max(1, cpus//count-1)))
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    try:
        asyncio.run(run_worker(number, count, jobs, known, proxies, ranks, next_job,
                               target, messages, stop, options, progress_slot))
    except BaseException as exc:
        stop.set()
        messages.put(dict(event='fatal', worker=number, error=type(exc).__name__+': '+str(exc)))
        traceback.print_exc()
        raise
    finally:
        messages.put(dict(event='worker_done', worker=number))


def collect(options):
    started = time.monotonic()
    run_id = str(uuid.uuid4())
    folder = options.output or OUTPUT_DIR/('discovery-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+run_id[:8])
    folder.mkdir(parents=True, exist_ok=False)
    emit('loading', run_id=run_id, output_directory=str(folder))
    channels, known = load_inventory(options.limit)
    proxies, ranks = load_ranked_proxies(options.ranked)
    jobs = select_tabs(channels)
    cpus = getattr(os, 'process_cpu_count', os.cpu_count)() or 2
    workers = min(options.workers or max(1, min(8, cpus//4)), len(proxies), max(1, len(jobs)))
    maximum = min(options.max_concurrency or len(proxies), len(proxies))
    initial = min(options.concurrency or min(1024, max(64, cpus*32)), maximum)
    workers = min(workers, initial)
    # All database connections are closed before forking. Linux workers share
    # the immutable existing-ID map through copy-on-write memory.
    context = multiprocessing.get_context('fork' if sys.platform.startswith('linux') else 'spawn')
    next_job, target = context.Value('i', 0), context.Value('i', initial)
    messages, stop = context.Queue(maxsize=2048), context.Event()
    progress_slots = [context.Array('d', len(COUNTERS)+1) for _ in range(workers)]
    processes = []
    previous_handlers = {}
    for sig in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[sig] = signal.signal(sig, lambda *_: stop.set())
    summary = dict(run_id=run_id, started_at=datetime.now(timezone.utc).isoformat(),
                   channels_selected=len(channels), tabs_planned=len(jobs),
                   existing_video_ids=sum(map(len, known.values())), proxies=len(proxies),
                   workers=workers, automatic_concurrency=not bool(options.concurrency),
                   initial_concurrency=initial, peak_concurrency=initial,
                   tabs_completed=0, tabs_failed=0, videos_inserted=0, writer_batches=0,
                   writer_seconds=0.0, output_directory=str(folder), concurrency_changes=[])
    tuner = ConcurrencyTuner(initial, maximum, options.tune_seconds)
    observed, done, recorded = Counter(), set(), set()
    channel_success = Counter()
    pending, pending_rows = [], 0
    fatal, sample_counts = [], Counter()
    last_flush = last_report = time.monotonic()
    last_totals, cached_totals = 0.0, {}

    def totals(force=False):
        nonlocal last_totals, cached_totals
        now = time.monotonic()
        if force or now-last_totals >= 1:
            snapshots = []
            for slot in progress_slots:
                with slot.get_lock():
                    snapshots.append(list(slot.get_obj()))
            cached_totals = {name: sum(row[index] for row in snapshots)
                             for index, name in enumerate(COUNTERS)}
            cached_totals = {name: value if name == 'request_seconds' else int(value)
                             for name, value in cached_totals.items()}
            cached_totals['active_requests'] = int(sum(row[-1] for row in snapshots))
            last_totals = now
        return cached_totals

    try:
        for number in range(workers):
            process = context.Process(target=worker_entry,
                args=(number, workers, jobs, known, proxies[number::workers], ranks[number::workers],
                      next_job, target, messages, stop, options, progress_slots[number]),
                name=f'discovery-{number}')
            process.start()
            processes.append(process)
        emit('started', **summary)
        with connect_database('media', autocommit=True, application_name='discovery-batch-writer') as conn, \
                (folder/'results.jsonl').open('w') as results, \
                (folder/'new-video-ids.jsonl').open('w') as inserted_file, \
                (folder/'unresolved.jsonl').open('w') as unresolved:
            writer = BatchWriter(conn)

            def record(scan):
                identity = (scan['channel_id'], scan['type'])
                if identity in recorded:
                    raise RuntimeError('A channel tab was completed twice')
                recorded.add(identity)
                observed[scan['status']] += 1
                if scan['scan_complete']:
                    summary['tabs_completed'] += 1
                    channel_success[scan['channel_id']] += 1
                    summary['videos_inserted'] += scan.get('videos_inserted', 0)
                else:
                    summary['tabs_failed'] += 1
                    unresolved.write(json.dumps({key: scan.get(key) for key in
                        ('channel_id', 'type', 'status', 'error', 'pages_fetched', 'recovery')})+'\n')
                row = {key: value for key, value in scan.items()
                       if key not in {'videos', 'inserted_video_ids', 'pages', 'continuation', 'response_sample'}}
                if scan.get('response_sample') and sample_counts[scan['status']] < 3:
                    row['response_sample'] = scan['response_sample']
                    sample_counts[scan['status']] += 1
                results.write(json.dumps(row, separators=(',', ':'))+'\n')

            def flush():
                nonlocal pending, pending_rows, last_flush
                if not pending:
                    last_flush = time.monotonic()
                    return
                before = time.monotonic()
                inserted = writer.write(pending)
                summary['writer_seconds'] += time.monotonic()-before
                summary['writer_batches'] += 1
                for video_id, channel_id, kind in inserted:
                    inserted_file.write(json.dumps(dict(video_id=video_id, channel_id=channel_id,
                                                         type=kind), separators=(',', ':'))+'\n')
                for scan in pending:
                    record(scan)
                pending, pending_rows = [], 0
                last_flush = time.monotonic()

            while len(done) < workers or any(p.is_alive() for p in processes):
                try:
                    message = messages.get(timeout=0.2)
                except queue.Empty:
                    message = None
                    for index, process in enumerate(processes):
                        if process.exitcode not in (None, 0) and index not in done:
                            fatal.append(f'Worker {index} exited with status {process.exitcode}')
                            done.add(index)
                            stop.set()
                if message:
                    event = message['event']
                    if event == 'result':
                        result = message['result']
                        if result['scan_complete']:
                            pending.append(result)
                            pending_rows += len(result['videos'])
                        else:
                            record(result)
                    elif event == 'worker_done':
                        done.add(message['worker'])
                    elif event == 'fatal':
                        fatal.append(message['error'])
                        stop.set()
                now = time.monotonic()
                if pending_rows >= options.batch_size or len(pending) >= 512 or now-last_flush >= options.flush_seconds:
                    flush()
                total = totals()
                if not options.concurrency and not stop.is_set():
                    try:
                        backlog = messages.qsize() > 1536
                    except (NotImplementedError, AttributeError):
                        backlog = False
                    changed = tuner.observe(now, total['usable_pages'], len(jobs)-next_job.value, backlog)
                    if changed:
                        target.value = max(workers, changed['concurrency'])
                        tuner.current = target.value
                        changed['concurrency'] = target.value
                        summary['peak_concurrency'] = max(summary['peak_concurrency'], target.value)
                        summary['concurrency_changes'].append(changed)
                        emit('concurrency', **changed)
                if now-last_report >= 10:
                    progress = dict(summary, **total, tabs_recorded=len(recorded),
                                    channels_completed=sum(n == 2 for n in channel_success.values()),
                                    concurrency=target.value, seconds=round(now-started, 2),
                                    useful_pages_per_second=round(total['usable_pages']/max(0.001, now-started), 2),
                                    outcomes=dict(observed), pending_writer_ids=pending_rows)
                    write_json(folder/'progress.json', progress)
                    emit('progress', **progress)
                    last_report = now
            flush()
            for channel, kind in jobs:
                if (channel, kind) not in recorded:
                    unresolved.write(json.dumps(dict(channel_id=channel, type=kind,
                                                      status='unprocessed'))+'\n')
            for output in (results, inserted_file, unresolved):
                output.flush()
                os.fsync(output.fileno())
    except BaseException as exc:
        fatal.append(type(exc).__name__+': '+str(exc))
        stop.set()
        traceback.print_exc()
    finally:
        if stop.is_set():
            deadline = time.monotonic()+options.timeout+5
            while any(p.is_alive() for p in processes) and time.monotonic() < deadline:
                try:
                    messages.get(timeout=0.2)
                except queue.Empty:
                    pass
        for process in processes:
            process.join(timeout=2)
            if process.is_alive():
                if sys.platform.startswith('linux') and os.getpgid(process.pid) == process.pid:
                    os.killpg(process.pid, signal.SIGTERM)
                else:
                    process.terminate()
                process.join(timeout=3)
            if process.is_alive():
                if sys.platform.startswith('linux') and os.getpgid(process.pid) == process.pid:
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.join()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
        elapsed = time.monotonic()-started
        summary.update(**totals(force=True), seconds=round(elapsed, 3), outcomes=dict(observed),
                       channels_completed=sum(n == 2 for n in channel_success.values()),
                       tabs_unprocessed=len(jobs)-len(recorded), fatal_errors=fatal,
                       final_concurrency=target.value, best_measured_concurrency=tuner.best_limit,
                       finished_at=datetime.now(timezone.utc).isoformat())
        summary['useful_pages_per_second'] = round(summary['usable_pages']/max(0.001, elapsed), 3)
        summary['new_ids_per_second'] = round(summary['videos_inserted']/max(0.001, elapsed), 3)
        write_json(folder/'summary.json', summary)
        write_json(STATE_DIR/'discovery-last.json', summary)
        emit('finished', **summary)
    return 2 if fatal or summary['tabs_failed'] or summary['tabs_unprocessed'] else 0


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--ranked', type=Path, default=STATE_DIR/'proxy-service/ranked-proxies.jsonl')
    result.add_argument('--limit', type=int, default=0, help='Channel limit; zero scans every saved channel')
    result.add_argument('--workers', type=int, default=0, help='Zero chooses processes from available CPUs')
    result.add_argument('--concurrency', type=int, default=0, help='Zero automatically tunes concurrency')
    result.add_argument('--max-concurrency', type=int, default=0, help='Zero permits one request per ranked proxy')
    result.add_argument('--attempts', type=int, default=3, help='Attempts per page through different available proxies')
    result.add_argument('--timeout', type=float, default=15)
    result.add_argument('--connect-timeout', type=float, default=3)
    result.add_argument('--batch-size', type=int, default=5000)
    result.add_argument('--flush-seconds', type=float, default=1)
    result.add_argument('--tune-seconds', type=float, default=30)
    result.add_argument('--client-version', default=os.environ.get('YOUTUBE_CLIENT_VERSION', CLIENT_VERSION))
    result.add_argument('--output', type=Path)
    return result


def main():
    options = parser().parse_args()
    if (min(options.limit, options.workers, options.concurrency, options.max_concurrency) < 0 or
            min(options.attempts, options.timeout, options.connect_timeout, options.batch_size,
                options.flush_seconds, options.tune_seconds) <= 0):
        raise ValueError('Limits must be nonnegative and request/batch settings positive')
    os.umask(0o077)
    with file_lock(STATE_DIR/'discovery.lock'):
        return collect(options)


if __name__ == '__main__':
    raise SystemExit(main())
