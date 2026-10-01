"""Verify and import immutable metadata batches, retrying both databases safely."""
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

from collect_video_metadata import has_metadata, metadata_error_reason
from discover_videos import open_database
from metadata_bulk import atomic_json, digest, utcnow
from proxy_statistics import Aggregate, AttemptOutcome, ProxyTarget, StatisticsBatch, write_batch


ROOT = Path(__file__).resolve().parent
VIDEO_ID = re.compile(r'[A-Za-z0-9_-]{11}\Z')


def read_chunk(path, run_id):
    path = Path(path)
    info = json.loads(path.with_suffix(path.suffix + '.json').read_text())
    if info['run_id'] != run_id or info['name'] != path.name or digest(path) != info['sha256']:
        raise ValueError('Result batch identity or checksum mismatch')
    rows = []
    with gzip.open(path, 'rt') as source:
        for line in source:
            row = json.loads(line)
            event = row['event']
            if not VIDEO_ID.fullmatch(event['video_id']) or event['result']['video_id'] != event['video_id']:
                raise ValueError('Result video identity mismatch')
            if type(event['final']) is not bool or type(event['successful']) is not bool:
                raise ValueError('Invalid outcome flags')
            if has_metadata(event['result']) != event['successful'] or event['successful'] and not event['final']:
                raise ValueError('Result success classification mismatch')
            at = datetime.fromisoformat(event['at'])
            if at.tzinfo is None:
                raise ValueError('Result timestamp needs a timezone')
            rows.append(row)
    if len(rows) != info['rows'] or [row['seq'] for row in rows] != list(range(info['first_seq'], info['last_seq'] + 1)):
        raise ValueError('Result sequence is incomplete or duplicated')
    return info, rows


def text_value(value):
    if value is not None and not isinstance(value, str):
        raise ValueError('Metadata text has an invalid type')
    # PostgreSQL text cannot hold NUL; retain a visible replacement character.
    return value.replace('\x00', '\ufffd') if value is not None else None


def metadata_rows(rows):
    selected = {}
    for row in rows:
        event = row['event']
        if not event['final']:
            continue
        previous = selected.get(event['video_id'])
        if previous is None or not previous['successful'] or event['successful']:
            selected[event['video_id']] = event
    output = []
    for video_id, event in selected.items():
        metadata = event['result'].get('metadata') or {}
        duration = metadata.get('duration_seconds')
        if duration is not None and (type(duration) is not int or not 0 <= duration <= 2**31 - 1):
            raise ValueError('Invalid metadata duration')
        published = metadata.get('published_at')
        published = datetime.fromisoformat(published) if published else None
        if published is not None and published.tzinfo is None:
            raise ValueError('Publication time needs a timezone')
        output.append((video_id, event['successful'], text_value(metadata.get('title')),
            text_value(metadata.get('description')), duration, published, text_value(metadata.get('thumbnail_url')),
            datetime.fromisoformat(event['at']), None if event['successful'] else metadata_error_reason(event['result'])))
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
        if outcome.data_received is not event['successful']:
            raise ValueError('Statistics and metadata success disagree')
        aggregates.setdefault(target, Aggregate()).add(outcome, target.protocol)
    return StatisticsBatch(aggregates, key=key)


def apply_metadata(conn, values):
    conn.execute('''CREATE TEMP TABLE bulk_metadata_stage(
        video_id TEXT PRIMARY KEY,successful BOOLEAN,title TEXT,description TEXT,duration_seconds INTEGER,
        published_at TIMESTAMPTZ,thumbnail_url TEXT,observed_at TIMESTAMPTZ,error TEXT) ON COMMIT DROP''')
    with conn.cursor().copy('COPY bulk_metadata_stage FROM STDIN') as copy:
        for row in values:
            copy.write_row(row)
    matched = conn.execute('SELECT count(*) FROM bulk_metadata_stage s JOIN videos v USING(video_id)').fetchone()[0]
    if matched != len(values):
        raise ValueError('A selected video is missing from the database')
    saved = conn.execute('''UPDATE videos v SET
        title=coalesce(s.title,v.title),description=coalesce(s.description,v.description),
        duration_seconds=coalesce(s.duration_seconds,v.duration_seconds),
        published_at=coalesce(s.published_at,v.published_at),thumbnail_url=coalesce(s.thumbnail_url,v.thumbnail_url),
        metadata_updated_at=s.observed_at,metadata_error=NULL
        FROM bulk_metadata_stage s WHERE v.video_id=s.video_id AND s.successful AND v.metadata_updated_at IS NULL''').rowcount
    errors = conn.execute('''UPDATE videos v SET metadata_error=s.error FROM bulk_metadata_stage s
        WHERE v.video_id=s.video_id AND NOT s.successful AND v.metadata_updated_at IS NULL
          AND v.metadata_error IS DISTINCT FROM s.error''').rowcount
    return {'metadata_saved': saved, 'errors_recorded': errors, 'final_outcomes': len(values)}


def apply_chunk(media, proxy, path, run_id):
    info, rows = read_chunk(path, run_id)
    key = hashlib.sha256((run_id + ':' + info['sha256']).encode()).digest()
    batch = statistics_batch(rows, key)
    values = metadata_rows(rows)
    # Hold the statistics transaction until metadata commits. After an uncertain
    # acknowledgement, the same digest retries statistics and successful videos
    # are skipped by their existing metadata timestamp.
    with proxy.transaction():
        stats = write_batch(proxy, batch)
        if stats['unmatched_attempts'] or stats['stale_attempts']:
            raise ValueError('Refusing mismatched or overlapping proxy statistics')
        with media.transaction():
            saved = apply_metadata(media, values)
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
        'run_id': args.run_id, 'last_seq': 0, 'metadata_saved': 0, 'errors_recorded': 0,
        'events_imported': 0, 'batches': 0, 'started_at': utcnow()}
    if state['run_id'] != args.run_id:
        raise ValueError('Import state belongs to another run')
    media = proxy = None
    while True:
        try:
            if media is None or media.closed:
                media = open_database(autocommit=True)
                if not media.execute("SELECT pg_try_advisory_lock(hashtext('media.video-metadata'))").fetchone()[0]:
                    media.close()
                    raise RuntimeError('Another metadata collector is running')
            if proxy is None or proxy.closed:
                proxy = psycopg.connect(dbname='proxy', user='mahmud', host=str(ROOT / '.local/postgres/socket'),
                    port=5432, autocommit=True, application_name='bulk-metadata-import')
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
                for name in ('metadata_saved', 'errors_recorded'):
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
