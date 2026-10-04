"""Verify and import exact view and like counts from immutable result batches."""
from __future__ import annotations

import argparse
from datetime import datetime
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

import psycopg

from collect_video_stats import has_stats, stats_error_reason, exact_count, MAX_COUNT
from metadata_bulk_import import read_chunk as read_common_chunk
from discover_videos import open_database
from metadata_bulk import atomic_json, digest, utcnow
from proxy_statistics import Aggregate, AttemptOutcome, ProxyTarget, StatisticsBatch, write_batch
from collection_policy import data_observation_matches


ROOT = Path(__file__).resolve().parent
VIDEO_ID = re.compile(r'[A-Za-z0-9_-]{11}\Z')


def read_chunk(path, run_id):
    return read_common_chunk(path, run_id, success_check=has_stats)


def statistics_rows(rows):
    selected = {}
    for row in rows:
        event = row['event']
        if not event['final']:
            continue
        previous = selected.get(event['video_id'])
        if previous is None or (event['at'] >= previous['at'] and
                                (event['successful'] or not previous['successful'])):
            selected[event['video_id']] = event
    output = []
    for video_id, event in selected.items():
        result = event['result']
        stats = result.get('stats') or {}
        values = [stats.get(name) for name in ('view_count', 'like_count')]
        if any(value is not None and (type(value) is not int or not 0 <= value <= MAX_COUNT) for value in values):
            raise ValueError('Invalid statistics count')
        if event['successful']:
            evidence = result.get('evidence') or {}
            if evidence.get('response_video_id') != video_id:
                raise ValueError('Statistics evidence has a mismatched video ID')
            if values[0] is not None and exact_count(evidence.get('view_text'), 'view') != values[0]:
                raise ValueError('View count does not match exact source evidence')
            kind = 'number' if evidence.get('like_source') == 'title' else 'like'
            if values[1] is not None and exact_count(evidence.get('like_text'), kind) != values[1]:
                raise ValueError('Like count does not match exact source evidence')
        output.append((video_id, event['successful'], *values, datetime.fromisoformat(event['at']),
                       stats_error_reason(result)))
    return output


def statistics_batch(rows, key):
    aggregates = {}
    for row in rows:
        event = row['event']
        observation = event['observation']
        if observation is None:
            continue
        observation = {**observation, 'checked_at': datetime.fromisoformat(observation['checked_at'])}
        outcome = AttemptOutcome(**observation)
        proxy = event['proxy']
        target = ProxyTarget.from_catalog(proxy['id'], bytes.fromhex(proxy['key']), proxy['protocol'])
        if not data_observation_matches(event['result'], event['successful'], outcome.data_received, outcome.http_status):
            raise ValueError('Proxy and video statistics success disagree')
        aggregates.setdefault(target, Aggregate()).add(outcome, target.protocol)
    return StatisticsBatch(aggregates, key=key)


def apply_statistics(conn, values):
    conn.execute('''CREATE TEMP TABLE bulk_statistics_stage(
        video_id TEXT PRIMARY KEY,successful BOOLEAN,view_count BIGINT,like_count BIGINT,
        observed_at TIMESTAMPTZ,error TEXT) ON COMMIT DROP''')
    with conn.cursor().copy('COPY bulk_statistics_stage FROM STDIN') as copy:
        for row in values:
            copy.write_row(row)
    matched = conn.execute('SELECT count(*) FROM bulk_statistics_stage s JOIN videos v USING(video_id)').fetchone()[0]
    if matched != len(values):
        raise ValueError('A selected video is missing from the database')
    saved = conn.execute('''UPDATE videos v SET
        view_count=coalesce(s.view_count,v.view_count),like_count=coalesce(s.like_count,v.like_count),
        stats_updated_at=s.observed_at,stats_error=s.error
        FROM bulk_statistics_stage s WHERE v.video_id=s.video_id AND s.successful
          AND (v.stats_updated_at IS NULL OR v.stats_updated_at<s.observed_at)''').rowcount
    errors = conn.execute('''UPDATE videos v SET stats_error=s.error FROM bulk_statistics_stage s
        WHERE v.video_id=s.video_id AND NOT s.successful AND v.stats_updated_at IS NULL
          AND v.stats_error IS DISTINCT FROM s.error''').rowcount
    return {'statistics_saved': saved, 'errors_recorded': errors, 'final_outcomes': len(values)}


def apply_chunk(media, proxy, path, run_id):
    info, rows = read_chunk(path, run_id)
    key = hashlib.sha256((run_id + ':' + info['sha256']).encode()).digest()
    batch = statistics_batch(rows, key)
    values = statistics_rows(rows)
    # Hold the proxy transaction until video counts commit. Replaying the same
    # checksummed batch preserves counters and skips already applied timestamps.
    with proxy.transaction():
        stats = write_batch(proxy, batch)
        if stats['unmatched_attempts'] or stats['stale_attempts']:
            raise ValueError('Refusing mismatched or overlapping proxy statistics')
        with media.transaction():
            saved = apply_statistics(media, values)
    return {'file': path.name, 'first_seq': info['first_seq'], 'last_seq': info['last_seq'],
            'events': len(rows), **saved, 'statistics': stats}


def sync(args):
    folder = args.local.resolve()
    folder.mkdir(parents=True, exist_ok=True)
    (folder / 'outbox').mkdir(exist_ok=True)
    lock = (folder / 'sync.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    state_path = folder / 'import-status.json'
    state = json.loads(state_path.read_text()) if state_path.exists() else {
        'run_id': args.run_id, 'last_seq': 0, 'statistics_saved': 0, 'errors_recorded': 0,
        'events_imported': 0, 'batches': 0, 'started_at': utcnow()}
    if state['run_id'] != args.run_id:
        raise ValueError('Import state belongs to another run')
    media = proxy = None
    while True:
        try:
            if media is None or media.closed:
                media = open_database(autocommit=True)
                if not media.execute("SELECT pg_try_advisory_lock(hashtext('media.video-statistics'))").fetchone()[0]:
                    media.close()
                    raise RuntimeError('Another statistics collector is running')
            if proxy is None or proxy.closed:
                proxy = psycopg.connect(dbname='proxy', user='mahmud', host=str(ROOT / '.local/postgres/socket'),
                    port=5432, autocommit=True, application_name='bulk-statistics-import')
            subprocess.run(['rsync', '-a', '--ignore-existing', '--include=*.jsonl.gz',
                '--include=*.jsonl.gz.json', '--exclude=*', '-e', 'ssh -o BatchMode=yes -o ConnectTimeout=10',
                f'{args.host}:{args.remote}/outbox/', str(folder / 'outbox') + '/'], check=True, timeout=120,
                stdout=subprocess.DEVNULL)
            for receipt in sorted((folder / 'outbox').glob('events-*.jsonl.gz.json')):
                info = json.loads(receipt.read_text())
                if info['last_seq'] <= state['last_seq']:
                    continue
                if info['first_seq'] != state['last_seq'] + 1:
                    raise ValueError('Result files have a gap; refusing to skip events')
                path = folder / 'outbox' / info['name']
                if not path.is_file():
                    break
                result = apply_chunk(media, proxy, path, args.run_id)
                state.update(last_seq=result['last_seq'], updated_at=utcnow(), last_batch=result, error=None)
                for name in ('statistics_saved', 'errors_recorded'):
                    state[name] += result[name]
                state['events_imported'] += result['events']
                state['batches'] += 1
                atomic_json(state_path, state)
                print(json.dumps(result), flush=True)
            command = 'cat ' + shlex.quote(str(Path(args.remote) / 'status.json'))
            response = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', args.host, command],
                check=True, timeout=20, text=True, capture_output=True)
            remote = json.loads(response.stdout)
            state.update(remote=remote, updated_at=utcnow(), error=None)
            state['complete'] = (remote['state'] == 'complete'
                and remote['exported_seq'] == remote['counters']['events'] == state['last_seq'])
            atomic_json(state_path, state)
            if state['complete']:
                print(json.dumps({'complete': True, **state}), flush=True)
                media.close()
                proxy.close()
                return
        except (OSError, subprocess.SubprocessError, psycopg.Error, RuntimeError) as exc:
            state.update(error=type(exc).__name__, updated_at=utcnow())
            atomic_json(state_path, state)
            print(json.dumps({'retrying_import': type(exc).__name__}), flush=True)
            for conn in (media, proxy):
                if conn is not None:
                    conn.close()
            media = proxy = None
        if args.once:
            return
        time.sleep(args.interval)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', required=True)
    parser.add_argument('--remote', required=True)
    parser.add_argument('--local', type=Path, required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--once', action='store_true')
    sync(parser.parse_args())


if __name__ == '__main__':
    main()
