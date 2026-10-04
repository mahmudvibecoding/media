"""Collect pending video metadata using ranked proxies and one batch writer."""
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

from collect_video_metadata import CLIENT_VERSION, fetch_metadata, has_metadata, metadata_error_reason
from collection_policy import VIDEO_CONFIRMATION_MAX_AGE, classify_outcome
from discovery_pool import ConcurrencyTuner, DiscoveryPool
from discovery_storage import load_ranked_proxies
from metadata_storage import FIELDS, MetadataWriter, resume_source, save_input, select_input
from proxy_catalog import CatalogClients
from proxy_service_common import file_lock, read_json, write_json
from runtime_config import OUTPUT_DIR, STATE_DIR, connect_database


COUNTERS = ('http_attempts', 'data_responses', 'request_errors', 'video_error_responses',
            'request_seconds', 'recovery_ids')


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def emit(event, **values):
    print(json.dumps(dict(event=event, **values), separators=(',', ':')), flush=True)


async def fetch_using_pool(clients, pool, stats, job, options, stop, prior=None):
    video, kind = job
    evidence = list(prior.get('availability_evidence', [])) if prior else []
    previous = prior.get('last_proxy_index') if prior else None
    attempts = prior.get('attempts', 0) if prior else 0
    seconds = prior.get('request_seconds', 0.0) if prior else 0.0
    for _ in range(options.attempts):
        if stop.is_set():
            raise asyncio.CancelledError()
        index = await pool.acquire(previous)
        observations = []
        started = time.monotonic()
        try:
            result = await fetch_metadata(clients[index], video, options.client_version,
                retries=0, on_attempt=observations.append, total_timeout=options.timeout)
        except BaseException:
            pool.release(index, None, time.monotonic()-started)
            raise
        elapsed = time.monotonic()-started
        observation = vars(observations[-1]) if observations else None
        outcome = classify_outcome(result, observation, has_metadata(result))
        pool.release(index, outcome.proxy_success, elapsed)
        if outcome.category == 'local_error':
            raise RuntimeError('Local metadata worker failure; proxy scores were not penalized')
        stats['http_attempts'] += 1
        stats['request_seconds'] += elapsed
        stats['data_responses' if outcome.proxy_success is True else
              'request_errors' if outcome.proxy_success is False else 'video_error_responses'] += 1
        attempts += 1
        seconds += elapsed
        now = time.time()
        evidence = [e for e in evidence if 0 <= now-e['at'] <= VIDEO_CONFIRMATION_MAX_AGE]
        proxy_id = clients.proxies[index].proxy_id
        if outcome.video_error:
            evidence.append(dict(reason=outcome.video_error, proxy_id=proxy_id, at=now))
        confirmed = bool(outcome.video_error and len({e['proxy_id'] for e in evidence
            if e['reason'] == outcome.video_error}) >= 2)
        result.update(video_id=video, type=kind, attempts=attempts, request_seconds=seconds,
            observed_at=utcnow(), proxy_id=proxy_id, last_proxy_index=index,
            availability_evidence=evidence, availability_confirmed=confirmed,
            video_error=outcome.video_error, outcome=outcome.category,
            final=has_metadata(result) or confirmed)
        if result['final']:
            break
        previous = index
    return result


async def run_worker(number, count, jobs, proxies, ranks, next_job, target, messages, stop,
                     options, progress_slot):
    stats = Counter({key: 0 for key in COUNTERS})
    pool = DiscoveryPool(ranks)
    failed = []

    def progress():
        with progress_slot.get_lock():
            progress_slot.get_obj()[:] = [stats[key] for key in COUNTERS]+[len(pool.active)]

    async def report():
        while True:
            await asyncio.sleep(1)
            progress()

    async with CatalogClients(proxies, len(proxies), connect_timeout=options.connect_timeout,
            request_timeout=options.timeout, per_proxy_connections=1, keepalive_expiry=120) as clients:
        async def fetch(job, previous=None):
            result = await fetch_using_pool(clients, pool, stats, job, options, stop, previous)
            result['recovery'] = previous is not None
            if stop.is_set() and not result['final']:
                return
            if not result['final'] and previous is None and not stop.is_set():
                failed.append((job, result))
            else:
                result['final'] = True
                await asyncio.to_thread(messages.put, dict(event='result', result=result))

        async def process(recovery=False):
            pending = set()
            deferred = iter(failed) if recovery else None
            exhausted = False
            try:
                while pending or not exhausted:
                    limit = min(len(proxies), max(1, (target.value+count-1-number)//count))
                    while len(pending) < limit and not exhausted and not stop.is_set():
                        previous = None
                        if recovery:
                            pair = next(deferred, None)
                            job, previous = pair if pair is not None else (None, None)
                        else:
                            with next_job.get_lock():
                                position = next_job.value
                                next_job.value += int(position < len(jobs))
                            job = jobs[position] if position < len(jobs) else None
                        if job is None:
                            exhausted = True
                            break
                        if recovery:
                            stats['recovery_ids'] += 1
                        pending.add(asyncio.create_task(fetch(job, previous)))
                    if stop.is_set() or not pending:
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


def worker_entry(number, count, jobs, proxies, ranks, next_job, target, messages, stop,
                 options, progress_slot):
    if sys.platform.startswith('linux'):
        os.setsid()
    cpus = getattr(os, 'process_cpu_count', os.cpu_count)() or 2
    os.environ.setdefault('GOMAXPROCS', str(max(1, cpus//count-1)))
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    try:
        asyncio.run(run_worker(number, count, jobs, proxies, ranks, next_job, target,
                               messages, stop, options, progress_slot))
    except BaseException as exc:
        stop.set()
        messages.put(dict(event='fatal', worker=number, error=type(exc).__name__+': '+str(exc)))
        traceback.print_exc()
        raise
    finally:
        messages.put(dict(event='worker_done', worker=number))


def stop_process(process, sig):
    try:
        if sys.platform.startswith('linux') and os.getpgid(process.pid) == process.pid:
            os.killpg(process.pid, sig)
        elif sig == signal.SIGTERM:
            process.terminate()
        else:
            process.kill()
    except ProcessLookupError:
        pass


def collect(options):
    started = time.monotonic()
    run_id = str(uuid.uuid4())
    folder = options.output or OUTPUT_DIR/('metadata-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+run_id[:8])
    active_path = STATE_DIR/'metadata-active.json'
    active = read_json(active_path, {})
    resume = options.resume or (Path(active['folder']) if active.get('active') and options.input is None else None)
    if resume and options.limit:
        raise ValueError('Resume uses the saved input; --limit is only for a new run')
    source = resume_source(resume) if resume else options.input
    with connect_database('media', autocommit=True, application_name='metadata-batch-writer') as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(hashtext('media.video-metadata'))").fetchone()[0]:
            raise RuntimeError('Another video metadata collector is running for this database')
        jobs, selection = select_input(conn, source, options.limit)
        folder.mkdir(parents=True, exist_ok=False)
        manifest = save_input(folder, jobs, dict(run_id=run_id, created_at=utcnow(),
            source=str(source) if source else 'fresh_pending', resumed_from=str(resume) if resume else None,
            selection=selection))
        write_json(active_path, dict(active=True, folder=str(folder), run_id=run_id))
        emit('loading', run_id=run_id, output_directory=str(folder), input_count=len(jobs))
        return collect_locked(options, conn, jobs, folder, manifest, active_path, started)


def collect_locked(options, conn, jobs, folder, manifest, active_path, started):
    proxies, ranks = load_ranked_proxies(options.ranked) if jobs else ([], [])
    cpus = getattr(os, 'process_cpu_count', os.cpu_count)() or 2
    workers = min(options.workers or max(1, min(8, cpus//4)), len(proxies), len(jobs))
    maximum = min(options.max_concurrency or len(proxies), len(proxies))
    initial = min(options.concurrency or min(1024, max(64, cpus*32)), maximum)
    workers = min(workers, initial)
    # Spawn keeps the parent's database lock/connection out of child processes.
    context = multiprocessing.get_context('spawn')
    next_job, target = context.Value('i', 0), context.Value('i', initial)
    messages, stop = context.Queue(maxsize=2048), context.Event()
    slots = [context.Array('d', len(COUNTERS)+1) for _ in range(workers)]
    processes, handlers = [], {}
    for sig in (signal.SIGTERM, signal.SIGINT):
        handlers[sig] = signal.signal(sig, lambda *_: stop.set())
    summary = dict(run_id=manifest['run_id'], started_at=manifest['created_at'],
        input_count=len(jobs), selection=manifest['selection'], input_sha256=manifest['input_sha256'],
        output_directory=str(folder), proxies=len(proxies), workers=workers,
        automatic_concurrency=not bool(options.concurrency), initial_concurrency=initial,
        peak_concurrency=initial, processed=0, saved=0, failed=0, already_saved=0,
        metadata_complete=0, availability_confirmed=0, writer_batches=0, writer_seconds=0.0,
        concurrency_changes=[], field_coverage={name: 0 for name in FIELDS})
    tuner = ConcurrencyTuner(initial, maximum, options.tune_seconds)
    done, recorded, pending, fatal = set(), set(), [], []
    outcomes, errors, saved_types, precisions = Counter(), Counter(), Counter(), Counter()
    last_flush = last_report = time.monotonic()
    last_totals, cached_totals = 0.0, {}

    def totals(force=False):
        nonlocal last_totals, cached_totals
        now = time.monotonic()
        if force or now-last_totals >= 1:
            values = []
            for slot in slots:
                with slot.get_lock():
                    values.append(list(slot.get_obj()))
            cached_totals = {key: sum(row[i] for row in values) for i, key in enumerate(COUNTERS)}
            cached_totals = {key: value if key == 'request_seconds' else int(value)
                             for key, value in cached_totals.items()}
            cached_totals['active_requests'] = int(sum(row[-1] for row in values))
            last_totals = now
        return cached_totals

    try:
        for number in range(workers):
            process = context.Process(target=worker_entry,
                args=(number, workers, jobs, proxies[number::workers], ranks[number::workers],
                      next_job, target, messages, stop, options, slots[number]),
                name=f'metadata-{number}')
            process.start()
            processes.append(process)
        emit('started', **summary)
        with (folder/'results.jsonl').open('w') as results, \
             (folder/'saved-video-ids.jsonl').open('w') as saved_file:
            writer = MetadataWriter(conn)

            def flush():
                nonlocal pending, last_flush
                if not pending:
                    last_flush = time.monotonic()
                    return
                if any(row['video_id'] in recorded for row in pending):
                    raise RuntimeError('A metadata ID was completed twice')
                before = time.monotonic()
                writer.write(pending)
                summary['writer_seconds'] += time.monotonic()-before
                summary['writer_batches'] += 1
                for row in pending:
                    video = row['video_id']
                    recorded.add(video)
                    summary['processed'] += 1
                    outcomes[row['outcome']] += 1
                    if row['saved']:
                        summary['saved'] += 1
                        summary['metadata_complete'] += int(all(row['fields_saved'].values()))
                        saved_types[row['type']] += 1
                        precisions[row.get('publication_precision', 'missing')] += 1
                        for name, filled in row['fields_saved'].items():
                            summary['field_coverage'][name] += filled
                        saved_file.write(json.dumps(dict(video_id=video, type=row['type']))+'\n')
                    elif row['already_saved']:
                        summary['already_saved'] += 1
                    else:
                        summary['failed'] += 1
                        summary['availability_confirmed'] += row['availability_confirmed']
                        errors[metadata_error_reason(row)] += 1
                    record = {key: value for key, value in row.items()
                              if key not in ('metadata', 'availability_evidence', 'last_proxy_index')}
                    results.write(json.dumps(record, separators=(',', ':'))+'\n')
                for output in (results, saved_file):
                    output.flush()
                    os.fsync(output.fileno())
                pending = []
                last_flush = time.monotonic()

            while len(done) < workers or any(p.is_alive() for p in processes):
                try:
                    message = messages.get(timeout=0.2)
                except queue.Empty:
                    message = None
                    for number, process in enumerate(processes):
                        if process.exitcode not in (None, 0) and number not in done:
                            fatal.append(f'Worker {number} exited with status {process.exitcode}')
                            done.add(number)
                            stop.set()
                if message:
                    if message['event'] == 'result':
                        pending.append(message['result'])
                    elif message['event'] == 'worker_done':
                        done.add(message['worker'])
                    elif message['event'] == 'fatal':
                        fatal.append(message['error'])
                        stop.set()
                now = time.monotonic()
                if len(pending) >= options.batch_size or now-last_flush >= options.flush_seconds:
                    flush()
                if not options.concurrency and not stop.is_set():
                    try:
                        backlog = messages.qsize() > 1536
                    except (NotImplementedError, AttributeError):
                        backlog = False
                    changed = tuner.observe(now, summary['saved'], len(jobs)-next_job.value, backlog)
                    if changed:
                        target.value = max(workers, changed['concurrency'])
                        tuner.current = target.value
                        changed['concurrency'] = target.value
                        changed['saved_records_per_second'] = changed.pop('useful_pages_per_second')
                        summary['peak_concurrency'] = max(summary['peak_concurrency'], target.value)
                        summary['concurrency_changes'].append(changed)
                        emit('concurrency', **changed)
                if now-last_report >= 10:
                    progress = dict(summary, **totals(), seconds=round(now-started, 2),
                        concurrency=target.value, pending_writer_results=len(pending),
                        saved_records_per_second=round(summary['saved']/max(0.001, now-started), 3),
                        errors=dict(errors))
                    write_json(folder/'progress.json', progress)
                    emit('progress', **progress)
                    last_report = now
            flush()
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
                stop_process(process, signal.SIGTERM)
                process.join(timeout=3)
            if process.is_alive():
                stop_process(process, signal.SIGKILL)
                process.join()
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        elapsed = time.monotonic()-started
        summary.update(**totals(force=True), seconds=round(elapsed, 3), outcomes=dict(outcomes),
            errors=dict(errors), saved_types=dict(saved_types), publication_precisions=dict(precisions),
            unprocessed=len(jobs)-len(recorded), fatal_errors=fatal, interrupted=stop.is_set(),
            final_concurrency=target.value, best_measured_concurrency=tuner.best_limit,
            finished_at=utcnow(), saved_records_per_second=round(summary['saved']/max(0.001, elapsed), 3))
        # The database is authoritative if interruption occurs between commit and reporting.
        try:
            rows = conn.execute('''SELECT video_id,type,metadata_error FROM public.videos
                WHERE video_id=ANY(%s) AND metadata_updated_at IS NULL ORDER BY video_id''',
                ([video for video, _ in jobs],)).fetchall()
            with (folder/'unresolved.jsonl').open('w') as output:
                for video, kind, error in rows:
                    output.write(json.dumps(dict(video_id=video, type=kind, error=error))+'\n')
                output.flush()
                os.fsync(output.fileno())
            summary['database_unresolved'] = len(rows)
            summary['database_completed'] = len(jobs)-len(rows)
        except Exception as exc:
            fatal.append('Final database reconciliation failed: '+type(exc).__name__)
            summary.update(database_unresolved=None, database_completed=None)
        write_json(folder/'summary.json', summary)
        write_json(STATE_DIR/'metadata-last.json', summary)
        write_json(active_path, dict(active=bool(fatal or summary['unprocessed'] or stop.is_set()),
                                    folder=str(folder), run_id=summary['run_id']))
        emit('finished', **summary)
    return 2 if fatal or summary['database_unresolved'] else 0


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    selection = result.add_mutually_exclusive_group()
    selection.add_argument('--input', type=Path, help='Verified JSONL video IDs and types')
    selection.add_argument('--resume', type=Path, help='Retry unfinished IDs from a saved run folder')
    result.add_argument('--limit', type=int, default=0, help='Limit a new run; zero selects all pending IDs')
    result.add_argument('--output', type=Path)
    result.add_argument('--ranked', type=Path, default=STATE_DIR/'proxy-service/ranked-proxies.jsonl')
    result.add_argument('--workers', type=int, default=0)
    result.add_argument('--concurrency', type=int, default=0, help='Zero tunes committed records per second')
    result.add_argument('--max-concurrency', type=int, default=0)
    result.add_argument('--attempts', type=int, default=3, help='Attempts per pass; unsuccessful IDs get one recovery pass')
    result.add_argument('--timeout', type=float, default=15)
    result.add_argument('--connect-timeout', type=float, default=3)
    result.add_argument('--batch-size', type=int, default=5000)
    result.add_argument('--flush-seconds', type=float, default=1)
    result.add_argument('--tune-seconds', type=float, default=30)
    result.add_argument('--client-version', default=os.environ.get('YOUTUBE_CLIENT_VERSION', CLIENT_VERSION))
    return result


def main():
    options = parser().parse_args()
    if (min(options.limit, options.workers, options.concurrency, options.max_concurrency) < 0 or
            min(options.attempts, options.timeout, options.connect_timeout, options.batch_size,
                options.flush_seconds, options.tune_seconds) <= 0):
        raise ValueError('Limits must be nonnegative and request/batch settings positive')
    os.umask(0o077)
    with file_lock(STATE_DIR/'metadata.lock'):
        return collect(options)


if __name__ == '__main__':
    raise SystemExit(main())
