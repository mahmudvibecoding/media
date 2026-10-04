"""Resumable metadata collection with a durable queue and immutable result files."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, deque
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
from functools import lru_cache
import gzip
import hashlib
import heapq
import json
import os
from pathlib import Path
import random
import signal
import socket
import socketserver
import sqlite3
import struct
import subprocess
import sys
import threading
import time
import uuid

from collect_video_metadata import fetch_metadata, has_metadata, metadata_error_reason
from collection_policy import (CollectionOutcome, VIDEO_CONFIRMATION_MAX_AGE, ProxyPerformance,
                               classify_outcome)
from proxy_catalog import CatalogClients, CatalogProxy, load_catalog


ROOT = Path(__file__).resolve().parent


@lru_cache(maxsize=32)
def collector_functions(folder):
    """Select the collector recorded in the immutable run manifest."""
    manifest = json.loads((Path(folder) / 'manifest.json').read_text())
    kind = manifest.get('collector', 'metadata')
    if kind == 'statistics':
        from collect_video_stats import fetch_stats, fetch_player_views, has_stats, stats_error_reason
        endpoint = manifest.get('statistics_endpoint', 'next')
        if endpoint not in ('next', 'player'):
            raise ValueError('Unknown statistics endpoint')
        return fetch_player_views if endpoint == 'player' else fetch_stats, has_stats, stats_error_reason
    if kind != 'metadata':
        raise ValueError('Unknown collector type')
    return fetch_metadata, has_metadata, metadata_error_reason


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as output:
        json.dump(value, output, separators=(',', ':'), default=str)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)
    descriptor = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def digest(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


@contextmanager
def connect_queue(path):
    conn = sqlite3.connect(path, timeout=60, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout=60000')
    conn.execute('PRAGMA synchronous=FULL')
    try:
        yield conn
    finally:
        if conn.in_transaction:
            conn.rollback()
        conn.close()


def ensure_policy_tables(conn):
    """Add scheduling state to old queues without rewriting jobs or journals."""
    conn.execute('''CREATE TABLE IF NOT EXISTS proxy_performance(
        proxy_id INTEGER PRIMARY KEY,quality REAL NOT NULL,latency_seconds REAL,
        samples INTEGER NOT NULL,failure_streak INTEGER NOT NULL,
        updated_at REAL NOT NULL,cooldown_until REAL NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS video_error_evidence(
        video_id TEXT PRIMARY KEY,reason TEXT NOT NULL,proxy_id INTEGER NOT NULL,
        observed_at REAL NOT NULL)''')


def confirm_video_error(conn, video_id, proxy_id, reason, observed_at):
    previous = conn.execute('SELECT * FROM video_error_evidence WHERE video_id=?', (video_id,)).fetchone()
    if (previous and previous['reason'] == reason and
            0 <= observed_at - previous['observed_at'] <= VIDEO_CONFIRMATION_MAX_AGE):
        return previous['proxy_id'] != proxy_id
    conn.execute('''INSERT INTO video_error_evidence VALUES (?,?,?,?)
        ON CONFLICT(video_id) DO UPDATE SET reason=excluded.reason,
            proxy_id=excluded.proxy_id,observed_at=excluded.observed_at''',
        (video_id, reason, proxy_id, observed_at))
    return False


def export_snapshot(folder):
    from discover_videos import open_database
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=False)
    manifest = {'version': 1, 'run_id': str(uuid.uuid4()), 'created_at': utcnow(), 'files': {}}
    videos = folder / 'videos.jsonl.gz'
    with open_database() as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        with conn.cursor(name='metadata_snapshot') as cursor, videos.open('wb') as raw:
            cursor.itersize = 10000
            cursor.execute('''SELECT video_id,type,(metadata_error IS NOT NULL)::integer
                FROM public.videos WHERE metadata_updated_at IS NULL ORDER BY video_id''')
            counts = Counter()
            with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=1) as output:
                for video_id, kind, previous_error in cursor:
                    output.write((json.dumps([video_id, kind, previous_error], separators=(',', ':')) + '\n').encode())
                    counts['videos'] += 1
                    counts['previous_errors'] += previous_error
                    counts[kind] += 1
            raw.flush()
            os.fsync(raw.fileno())
    proxies = load_catalog()
    random.Random(manifest['run_id']).shuffle(proxies)
    catalog = folder / 'proxies.jsonl.gz'
    with catalog.open('wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=1) as output:
            for proxy in proxies:
                output.write((json.dumps(proxy.bridge_record(), separators=(',', ':')) + '\n').encode())
        raw.flush()
        os.fsync(raw.fileno())
    manifest.update(counts, proxies=len(proxies), protocols=dict(Counter(p.working_protocol for p in proxies)))
    for path in (videos, catalog):
        path.chmod(0o600)
        manifest['files'][path.name] = {'sha256': digest(path), 'bytes': path.stat().st_size}
    atomic_json(folder / 'manifest.json', manifest)
    print(json.dumps(manifest), flush=True)


def load_proxies(folder):
    with gzip.open(Path(folder) / 'proxies.jsonl.gz', 'rt') as source:
        for line in source:
            row = json.loads(line)
            yield CatalogProxy(row['id'], bytes.fromhex(row['key']), row['address'], row['port'],
                               row['protocol'], row['working_protocol'], row['settings'])


def initialize(folder, workers, concurrency, max_attempts):
    folder = Path(folder)
    manifest = json.loads((folder / 'manifest.json').read_text())
    for name, details in manifest['files'].items():
        if digest(folder / name) != details['sha256']:
            raise ValueError('Snapshot checksum mismatch: ' + name)
    database = folder / 'queue.sqlite3'
    if database.exists():
        raise ValueError('Queue already exists; use run to resume it')
    with connect_queue(database) as conn:
        conn.executescript('''
            PRAGMA journal_mode=WAL;
            CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE jobs(
                video_id TEXT PRIMARY KEY,kind TEXT NOT NULL,priority INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'ready',attempts INTEGER NOT NULL DEFAULT 0,
                local_failures INTEGER NOT NULL DEFAULT 0,owner INTEGER,last_proxy_id INTEGER,last_error TEXT);
            CREATE INDEX jobs_ready ON jobs(status,priority,video_id);
            CREATE INDEX jobs_owner ON jobs(owner) WHERE owner IS NOT NULL;
            CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,worker INTEGER NOT NULL,
                proxy_id INTEGER NOT NULL,video_id TEXT NOT NULL,payload TEXT NOT NULL);
            CREATE TABLE proxy_usage(proxy_id INTEGER PRIMARY KEY,attempts INTEGER NOT NULL DEFAULT 0,
                successes INTEGER NOT NULL DEFAULT 0,local_failures INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE counters(name TEXT PRIMARY KEY,value INTEGER NOT NULL);
        ''')
        ensure_policy_tables(conn)
        conn.execute('BEGIN IMMEDIATE')
        config = {'run_id': manifest['run_id'], 'workers': workers, 'max_attempts': max_attempts,
                  'exported_seq': 0, 'round': 1, 'state': 'ready', 'started_at': None}
        conn.executemany('INSERT INTO settings VALUES (?,?)', [(k, json.dumps(v)) for k, v in config.items()])
        with gzip.open(folder / 'videos.jsonl.gz', 'rt') as source:
            conn.executemany('INSERT INTO jobs(video_id,kind,priority) VALUES (?,?,?)',
                             (json.loads(line) for line in source))
        count = conn.execute('SELECT count(*) FROM jobs').fetchone()[0]
        if count != manifest['videos']:
            conn.rollback()
            raise ValueError('Snapshot video count mismatch')
        conn.executemany('INSERT INTO counters VALUES (?,0)', [(name,) for name in (
            'events', 'attempts', 'saved', 'failed', 'local_errors', 'requests_sent', 'responses_received')])
        conn.commit()
    (folder / 'outbox').mkdir()
    (folder / 'workers').mkdir()
    atomic_json(folder / 'control.json', {'concurrency_per_worker': concurrency, 'stop': False})
    return {'initialized': count, 'workers': workers, 'concurrency': workers * concurrency}


def setting(conn, key):
    return json.loads(conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()[0])


def claim(folder, worker, count):
    with connect_queue(Path(folder) / 'queue.sqlite3') as conn:
        conn.execute('BEGIN IMMEDIATE')
        rows = conn.execute('''SELECT video_id,kind,attempts,last_proxy_id FROM jobs
            WHERE status='ready' ORDER BY priority,video_id LIMIT ?''', (count,)).fetchall()
        conn.executemany("UPDATE jobs SET status='leased',owner=? WHERE video_id=? AND status='ready'",
                         [(worker, row['video_id']) for row in rows])
        conn.commit()
        return [dict(row) for row in rows]


def recover_leases(folder, worker):
    with connect_queue(Path(folder) / 'queue.sqlite3') as conn:
        return conn.execute("UPDATE jobs SET status='ready',owner=NULL WHERE status='leased' AND owner=?",
                            (worker,)).rowcount


def finish(folder, worker, events):
    if not events:
        return
    _, has_data, error_reason = collector_functions(str(Path(folder).resolve()))
    with connect_queue(Path(folder) / 'queue.sqlite3') as conn:
        conn.execute('BEGIN IMMEDIATE')
        ensure_policy_tables(conn)
        max_attempts = setting(conn, 'max_attempts')
        counts, performance = Counter(), {}
        for event in events:
            job = conn.execute('SELECT * FROM jobs WHERE video_id=?', (event['video_id'],)).fetchone()
            if job is None or job['status'] != 'leased' or job['owner'] != worker:
                raise ValueError('Result does not own its video lease')
            observed = event['observation'] is not None
            successful = has_data(event['result'])
            if successful and not observed:
                raise ValueError('A metadata success requires an attempt observation')
            proxy_id = event['proxy']['id']
            at = datetime.fromisoformat(event['at']).timestamp()
            outcome = classify_outcome(event['result'], event['observation'], successful)
            confirmed = bool(outcome.video_error and confirm_video_error(
                conn, event['video_id'], proxy_id, outcome.video_error, at))
            if proxy_id not in performance:
                row = conn.execute('SELECT * FROM proxy_performance WHERE proxy_id=?', (proxy_id,)).fetchone()
                usage = conn.execute('SELECT * FROM proxy_usage WHERE proxy_id=?', (proxy_id,)).fetchone()
                performance[proxy_id] = ProxyPerformance.restore(row, dict(usage or {}), at)
            performance[proxy_id].observe(outcome, event['result'].get('seconds'), at)
            attempts = job['attempts'] + int(observed)
            local_failures = job['local_failures'] + int(not observed)
            final = successful or confirmed or attempts >= max_attempts or local_failures >= 5
            status = 'saved' if successful else 'failed' if final else 'retry' if observed else 'ready'
            error = None if successful else error_reason(event['result'])
            event.update(attempt=attempts, final=final, successful=successful,
                         local_failure=not observed, kind=job['kind'], worker=worker,
                         outcome_category=outcome.category, video_error_confirmed=confirmed)
            payload = json.dumps(event, separators=(',', ':'), default=str)
            conn.execute('INSERT INTO events(worker,proxy_id,video_id,payload) VALUES (?,?,?,?)',
                         (worker, event['proxy']['id'], event['video_id'], payload))
            conn.execute('''UPDATE jobs SET status=?,attempts=?,local_failures=?,owner=NULL,
                last_proxy_id=?,last_error=? WHERE video_id=?''',
                (status, attempts, local_failures, event['proxy']['id'], error, event['video_id']))
            conn.execute('''INSERT INTO proxy_usage(proxy_id,attempts,successes,local_failures) VALUES (?,?,?,?)
                ON CONFLICT(proxy_id) DO UPDATE SET attempts=attempts+excluded.attempts,
                    successes=successes+excluded.successes,local_failures=local_failures+excluded.local_failures''',
                (event['proxy']['id'], int(observed), int(successful), int(not observed)))
            counts.update(events=1, attempts=int(observed), saved=int(successful),
                          failed=int(status == 'failed'), local_errors=int(not observed))
            if observed:
                counts.update(requests_sent=int(event['observation']['request_sent']),
                              responses_received=int(event['observation']['http_status'] is not None))
        conn.executemany('''INSERT INTO proxy_performance VALUES (?,?,?,?,?,?,?)
            ON CONFLICT(proxy_id) DO UPDATE SET quality=excluded.quality,
                latency_seconds=excluded.latency_seconds,samples=excluded.samples,
                failure_streak=excluded.failure_streak,updated_at=excluded.updated_at,
                cooldown_until=excluded.cooldown_until''',
            [(proxy_id, *asdict(profile).values()) for proxy_id, profile in performance.items()])
        conn.executemany('UPDATE counters SET value=value+? WHERE name=?', [(v, k) for k, v in counts.items()])
        conn.commit()


def receive_message(stream):
    header = stream.read(4)
    if len(header) != 4:
        raise ConnectionError('Queue message header is incomplete')
    size = struct.unpack('!I', header)[0]
    if not 0 < size <= 64 * 1024 * 1024:
        raise ValueError('Queue message is too large')
    data = stream.read(size)
    if len(data) != size:
        raise ConnectionError('Queue message body is incomplete')
    return json.loads(data)


def send_message(stream, value):
    data = json.dumps(value, ensure_ascii=False, separators=(',', ':'), default=str).encode()
    if len(data) > 64 * 1024 * 1024:
        raise ValueError('Queue message is too large')
    stream.write(struct.pack('!I', len(data)) + data)
    stream.flush()


def queue_request(folder, action, worker, **values):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(120)
        connection.connect(str(Path(folder) / 'queue.sock'))
        with connection.makefile('rwb') as stream:
            send_message(stream, {'action': action, 'worker': worker, **values})
            response = receive_message(stream)
    if not response['ok']:
        raise RuntimeError('Queue request failed: ' + response['error'])
    return response['result']


def run_broker(folder):
    """Serialize writes outside fetching processes and acknowledge only commits."""
    folder = Path(folder)
    lock = (folder / 'broker.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    address = folder / 'queue.sock'
    address.unlink(missing_ok=True)
    mutation = threading.Lock()

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(120)
            try:
                request = receive_message(self.rfile)
                with mutation:
                    if request['action'] == 'claim':
                        result = claim(folder, request['worker'], request['count'])
                    elif request['action'] == 'finish':
                        finish(folder, request['worker'], request['events'])
                        result = len(request['events'])
                    else:
                        raise ValueError('Unknown queue action')
                send_message(self.wfile, {'ok': True, 'result': result})
            except Exception as exc:
                print(json.dumps({'queue_error': type(exc).__name__, 'at': utcnow()}), flush=True)
                try:
                    send_message(self.wfile, {'ok': False, 'error': type(exc).__name__})
                except OSError:
                    pass

    class Server(socketserver.ThreadingUnixStreamServer):
        daemon_threads = True
        request_queue_size = 128

    try:
        with Server(str(address), Handler) as server:
            address.chmod(0o600)
            print(json.dumps({'queue_ready': True}), flush=True)
            server.serve_forever()
    finally:
        address.unlink(missing_ok=True)
        lock.close()


class ProxyPool:
    """One request per proxy, ranked by recent success and successful-response time."""
    def __init__(self, proxies, usage, performance=None):
        self.proxies = proxies
        performance = performance or {}
        wall_now = time.time()
        self.performance = [ProxyPerformance.restore(performance.get(p.proxy_id),
                            usage.get(p.proxy_id, {}), wall_now) for p in proxies]
        self.untested = deque(i for i, p in enumerate(proxies)
                              if p.proxy_id not in usage and p.proxy_id not in performance)
        self.proven = {i for i, p in enumerate(proxies) if usage.get(p.proxy_id, {}).get('successes', 0)}
        self.active, self.available, self.available_proven = set(), {}, set()
        self.ranked, self.oldest, self.cooling = [], [], []
        self.versions = [0] * len(proxies)
        self.order = 0
        untested = set(self.untested)
        for index, profile in enumerate(self.performance):
            if index not in untested:
                self._schedule(index, max(0, profile.cooldown_until - wall_now))
        self.recovery = False
        self.selections = 0
        self.changed = asyncio.Event()

    def _schedule(self, index, delay):
        self.versions[index] += 1
        heapq.heappush(self.cooling, (time.monotonic() + delay, index, self.versions[index]))

    def _promote(self, now):
        while self.cooling and self.cooling[0][0] <= now:
            _, index, version = heapq.heappop(self.cooling)
            self.order += 1
            self.available[index] = (version, self.order)
            heapq.heappush(self.ranked, (-self.performance[index].score, self.order, index, version))
            heapq.heappush(self.oldest, (self.order, index, version))
            if index in self.proven:
                self.available_proven.add(index)

    def _activate(self, index):
        self.available.pop(index, None)
        self.available_proven.discard(index)
        self.active.add(index)
        self.selections += 1
        # Selecting through one heap leaves a stale entry in the other. Bound
        # those entries so a long collection does not grow scheduler memory.
        if max(len(self.ranked), len(self.oldest)) > max(64, 4 * len(self.proxies)):
            self.ranked = [(-self.performance[i].score, order, i, version)
                           for i, (version, order) in self.available.items()]
            self.oldest = [(order, i, version) for i, (version, order) in self.available.items()]
            heapq.heapify(self.ranked)
            heapq.heapify(self.oldest)
        return index

    def _take(self, queue, previous):
        held, chosen = [], None
        while queue:
            entry = heapq.heappop(queue)
            index, version = entry[-2:]
            current = self.available.get(index)
            if current is None or current[0] != version:
                continue
            if self.proxies[index].proxy_id == previous:
                held.append(entry)
                continue
            chosen = index
            break
        for entry in held:
            heapq.heappush(queue, entry)
        return self._activate(chosen) if chosen is not None else None

    async def acquire(self, previous=None):
        while True:
            now = time.monotonic()
            self._promote(now)
            if self.untested and (not self.recovery or not self.available_proven):
                index = self.untested.popleft()
                if self.proxies[index].proxy_id == previous:
                    if self.untested:
                        self.untested.append(index)
                        index = self.untested.popleft()
                    elif self.available:
                        self.untested.append(index)
                        index = None
                if index is not None:
                    return self._activate(index)
            if self.available:
                # One selection in twenty checks the longest-waiting eligible
                # configuration, including slow or formerly unsuccessful ones.
                queue = self.oldest if self.selections % 20 == 19 else self.ranked
                index = self._take(queue, previous)
                if index is None:
                    index = self._take(queue, None)
                if index is not None:
                    return index
            wait = max(0.01, min(1.0, self.cooling[0][0] - now)) if self.cooling else 1.0
            self.changed.clear()
            try:
                await asyncio.wait_for(self.changed.wait(), wait)
            except TimeoutError:
                pass

    def release(self, index, successful=False, local_failure=False, *, outcome=None, seconds=None):
        if index not in self.active:
            raise ValueError('Proxy has no active request')
        if outcome is None:
            outcome = CollectionOutcome('local_error' if local_failure else 'data' if successful else 'connection_error',
                                        None if local_failure else successful)
        if outcome.category == 'data':
            self.proven.add(index)
        now = time.time()
        self.performance[index].observe(outcome, seconds, now)
        self.active.remove(index)
        self._schedule(index, max(0, self.performance[index].cooldown_until - now))
        self.changed.set()


async def run_worker(folder, worker, connect_timeout=5, total_timeout=20):
    folder = Path(folder)
    fetch, has_data, _ = collector_functions(str(folder.resolve()))
    lock = (folder / 'workers' / f'{worker}.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    recover_leases(folder, worker)
    with connect_queue(folder / 'queue.sqlite3') as conn:
        ensure_policy_tables(conn)
        workers = setting(conn, 'workers')
        usage = {row['proxy_id']: dict(row) for row in conn.execute('SELECT * FROM proxy_usage')}
        performance = {row['proxy_id']: dict(row) for row in conn.execute('SELECT * FROM proxy_performance')}
    proxies = [proxy for i, proxy in enumerate(load_proxies(folder)) if i % workers == worker]
    if not proxies:
        raise ValueError('Worker has no proxy configurations')
    pool = ProxyPool(proxies, usage, performance)
    pending = deque()
    writes = asyncio.Queue(maxsize=1024)
    fetching = asyncio.Lock()
    active = 0
    stopped = False
    control = {'concurrency_per_worker': 1, 'stop': False}
    counts = Counter()
    started = time.monotonic()
    next_claim_at = 0.0

    async def writer():
        while True:
            first = await writes.get()
            if first is None:
                writes.task_done()
                return
            batch = [first]
            await asyncio.sleep(0.02)
            while len(batch) < 512:
                try:
                    item = writes.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if item is None:
                    raise RuntimeError('Writer closed before pending results were flushed')
                batch.append(item)
            await asyncio.to_thread(queue_request, folder, 'finish', worker, events=batch)
            for _ in batch:
                writes.task_done()

    async def next_job():
        nonlocal next_claim_at
        if pending:
            return pending.popleft()
        if time.monotonic() < next_claim_at:
            return None
        async with fetching:
            if not pending and time.monotonic() < next_claim_at:
                return None
            if not pending:
                rows = await asyncio.to_thread(queue_request, folder, 'claim', worker,
                    count=max(32, min(512, control['concurrency_per_worker'])))
                pending.extend(rows)
                if not rows:
                    next_claim_at = time.monotonic() + 0.5
            return pending.popleft() if pending else None

    async def task(number, clients):
        nonlocal active
        while not stopped:
            if control.get('stop') or number >= control['concurrency_per_worker']:
                await asyncio.sleep(0.25)
                continue
            job = await next_job()
            if job is None:
                await asyncio.sleep(0.25)
                continue
            if job['attempts'] > 0:
                pool.recovery = True
            index = await pool.acquire(job['last_proxy_id'])
            proxy = proxies[index]
            active += 1
            observations = []
            try:
                result = await fetch(clients[index], job['video_id'], retries=0,
                    on_attempt=observations.append, total_timeout=total_timeout)
                observation = asdict(observations[0]) if observations else None
                event = {'video_id': job['video_id'], 'at': utcnow(), 'result': result,
                         'proxy': {'id': proxy.proxy_id, 'key': proxy.connection_key.hex(),
                                   'protocol': proxy.working_protocol}, 'observation': observation}
                successful = has_data(result)
                await writes.put(event)
                counts.update(completed=1, saved=int(successful), local_errors=int(observation is None))
                pool.release(index, outcome=classify_outcome(result, observation, successful),
                             seconds=result.get('seconds'))
            finally:
                active -= 1

    async def refresh():
        nonlocal control, stopped
        while not stopped:
            control = json.loads((folder / 'control.json').read_text())
            if not 1 <= control['concurrency_per_worker'] <= 1024:
                raise ValueError('Concurrency must be between 1 and 1024 per worker')
            with connect_queue(folder / 'queue.sqlite3') as conn:
                state = setting(conn, 'state')
                pool.recovery = setting(conn, 'round') > 1
            atomic_json(folder / 'workers' / f'{worker}.json', {
                'worker': worker, 'pid': os.getpid(), 'updated_at': utcnow(), 'active': active,
                'seconds': round(time.monotonic() - started, 1), 'counts': dict(counts),
                'concurrency': control['concurrency_per_worker'], 'proxies': len(proxies),
                'untested_proxies': len(pool.untested), 'pending_writes': writes.qsize()})
            if state == 'complete' or control.get('stop'):
                stopped = True
                return
            await asyncio.sleep(2)

    async with CatalogClients(proxies, 1024, connect_timeout=connect_timeout,
                              request_timeout=total_timeout, per_proxy_connections=1, keepalive_expiry=120) as clients:
        writer_task = asyncio.create_task(writer())
        tasks = [asyncio.create_task(task(i, clients)) for i in range(1024)]
        refresh_task = asyncio.create_task(refresh())
        try:
            while not refresh_task.done():
                for running in (writer_task, *tasks):
                    if running.done():
                        running.result()
                await asyncio.sleep(0.25)
            refresh_task.result()
            await asyncio.gather(*tasks)
            await writes.join()
            await writes.put(None)
            await writer_task
        finally:
            for running in (refresh_task, writer_task, *tasks):
                if not running.done():
                    running.cancel()
            await asyncio.gather(refresh_task, writer_task, *tasks, return_exceptions=True)
            recover_leases(folder, worker)
            lock.close()


def export_events(folder, size=2000):
    folder = Path(folder)
    with connect_queue(folder / 'queue.sqlite3') as conn:
        after = setting(conn, 'exported_seq')
        published = list((folder / 'outbox').glob(f'events-{after + 1:012d}-*.jsonl.gz.json'))
        if published:
            if len(published) != 1:
                raise ValueError('Conflicting result batch boundaries')
            info = json.loads(published[0].read_text())
            if (info['run_id'] != setting(conn, 'run_id') or info['first_seq'] != after + 1
                    or digest(folder / 'outbox' / info['name']) != info['sha256']):
                raise ValueError('Published result batch changed')
            conn.execute("UPDATE settings SET value=? WHERE key='exported_seq'", (json.dumps(info['last_seq']),))
            return info['rows']
        rows = conn.execute('SELECT seq,payload FROM events WHERE seq>? ORDER BY seq LIMIT ?', (after, size)).fetchall()
        if not rows:
            return 0
        first, last = rows[0]['seq'], rows[-1]['seq']
        name = f'events-{first:012d}-{last:012d}.jsonl.gz'
        path = folder / 'outbox' / name
        temporary = path.with_suffix('.tmp')
        with temporary.open('wb') as raw:
            with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=1) as output:
                for row in rows:
                    output.write(('{' + f'"seq":{row["seq"]},"event":' + row['payload'] + '}\n').encode())
            raw.flush()
            os.fsync(raw.fileno())
        temporary.replace(path)
        atomic_json(path.with_suffix(path.suffix + '.json'), {
            'run_id': setting(conn, 'run_id'), 'name': name, 'first_seq': first,
            'last_seq': last, 'rows': len(rows), 'sha256': digest(path)})
        conn.execute('UPDATE settings SET value=? WHERE key=\'exported_seq\'', (json.dumps(last),))
        return len(rows)


def queue_status(folder):
    with connect_queue(Path(folder) / 'queue.sqlite3') as conn:
        conn.execute('BEGIN')
        states = dict(conn.execute('SELECT status,count(*) FROM jobs GROUP BY status'))
        counters = dict(conn.execute('SELECT name,value FROM counters'))
        proxies = conn.execute('''SELECT count(*),sum(attempts>0),sum(successes>0) FROM proxy_usage''').fetchone()
        return {'updated_at': utcnow(), 'state': setting(conn, 'state'), 'round': setting(conn, 'round'),
                'started_at': setting(conn, 'started_at'), 'jobs': states, 'counters': counters,
                'proxies_seen': proxies[0], 'proxies_attempted': proxies[1] or 0,
                'proxies_with_data': proxies[2] or 0, 'exported_seq': setting(conn, 'exported_seq')}


def run_controller(folder):
    folder = Path(folder).resolve()
    lock = (folder / 'controller.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    with connect_queue(folder / 'queue.sqlite3') as conn:
        ensure_policy_tables(conn)
        workers = setting(conn, 'workers')
        if setting(conn, 'started_at') is None:
            conn.execute("UPDATE settings SET value=? WHERE key='started_at'", (json.dumps(utcnow()),))
        if setting(conn, 'state') != 'complete':
            conn.execute("UPDATE settings SET value=? WHERE key='state'", (json.dumps('running'),))
    control = json.loads((folder / 'control.json').read_text())
    control['stop'] = False
    atomic_json(folder / 'control.json', control)
    children, logs, restarts = {}, {}, Counter()
    stop_requested = False

    def stop(*_):
        nonlocal stop_requested
        stop_requested = True
        control = json.loads((folder / 'control.json').read_text())
        control['stop'] = True
        atomic_json(folder / 'control.json', control)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    previous = None
    broker_log = (folder / 'queue.log').open('ab', buffering=0)
    broker = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), 'broker', '--run', str(folder)],
                              stdout=broker_log, stderr=subprocess.STDOUT)
    try:
        for _ in range(100):
            if broker.poll() is not None:
                raise RuntimeError('Queue broker did not start; inspect queue.log')
            try:
                queue_request(folder, 'claim', -1, count=0)
                break
            except (ConnectionError, OSError):
                time.sleep(0.1)
        else:
            raise RuntimeError('Queue broker did not become ready')
        while True:
            if broker.poll() is not None:
                raise RuntimeError('Queue broker stopped; inspect queue.log')
            if json.loads((folder / 'control.json').read_text()).get('stop'):
                stop_requested = True
            state = queue_status(folder)
            if not stop_requested and state['state'] != 'complete':
                for number in range(workers):
                    child = children.get(number)
                    if child is None or child.poll() is not None:
                        if child is not None:
                            restarts[number] += 1
                            logs[number].close()
                            if restarts[number] > 5:
                                raise RuntimeError(f'Worker {number} repeatedly failed; inspect its log')
                        logs[number] = (folder / 'workers' / f'{number}.log').open('ab', buffering=0)
                        children[number] = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                            'worker', '--run', str(folder), '--worker', str(number)],
                            stdout=logs[number], stderr=subprocess.STDOUT)
            for _ in range(20):
                if export_events(folder) < 2000:
                    break
            with connect_queue(folder / 'queue.sqlite3') as conn:
                conn.execute('BEGIN IMMEDIATE')
                busy = conn.execute("SELECT count(*) FROM jobs WHERE status IN ('ready','leased')").fetchone()[0]
                if busy == 0:
                    retries = conn.execute("UPDATE jobs SET status='ready' WHERE status='retry'").rowcount
                    if retries:
                        round_number = setting(conn, 'round') + 1
                        conn.execute("UPDATE settings SET value=? WHERE key='round'", (json.dumps(round_number),))
                    else:
                        conn.execute("UPDATE settings SET value=? WHERE key='state'", (json.dumps('complete'),))
                conn.commit()
            state = queue_status(folder)
            now = time.monotonic()
            if previous:
                seconds = now - previous[0]
                state['recent_saved_per_second'] = round((state['counters']['saved'] - previous[1]) / seconds, 2)
                state['recent_attempts_per_second'] = round((state['counters']['attempts'] - previous[2]) / seconds, 2)
            previous = (now, state['counters']['saved'], state['counters']['attempts'])
            atomic_json(folder / 'status.json', state)
            print(json.dumps(state), flush=True)
            if state['state'] == 'complete' or stop_requested:
                break
            time.sleep(5)
    finally:
        stop()
        deadline = time.monotonic() + 30
        for child in children.values():
            try:
                child.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.terminate()
        for child in children.values():
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        while export_events(folder):
            pass
        atomic_json(folder / 'status.json', queue_status(folder))
        for log in logs.values():
            log.close()
        broker.terminate()
        try:
            broker.wait(timeout=5)
        except subprocess.TimeoutExpired:
            broker.kill()
            broker.wait()
        broker_log.close()
        lock.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    export = sub.add_parser('export')
    export.add_argument('--output', type=Path, required=True)
    init = sub.add_parser('init')
    init.add_argument('--run', type=Path, required=True)
    init.add_argument('--workers', type=int, default=8)
    init.add_argument('--concurrency', type=int, default=128)
    init.add_argument('--max-attempts', type=int, default=3)
    for name in ('run', 'worker', 'status', 'broker'):
        command = sub.add_parser(name)
        command.add_argument('--run', type=Path, required=True)
        if name == 'worker':
            command.add_argument('--worker', type=int, required=True)
    args = parser.parse_args()
    if args.command == 'export':
        export_snapshot(args.output)
    elif args.command == 'init':
        if args.workers < 1 or not 1 <= args.concurrency <= 1024 or args.max_attempts < 1:
            parser.error('Workers, concurrency and attempts must be positive')
        print(json.dumps(initialize(args.run, args.workers, args.concurrency, args.max_attempts)))
    elif args.command == 'worker':
        asyncio.run(run_worker(args.run, args.worker))
    elif args.command == 'broker':
        run_broker(args.run)
    elif args.command == 'run':
        run_controller(args.run)
    else:
        print(json.dumps(queue_status(args.run)))


if __name__ == '__main__':
    main()
