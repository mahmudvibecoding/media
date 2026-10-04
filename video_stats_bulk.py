"""Snapshot all videos and the fresh responder pool for resumable statistics collection."""
import argparse
from collections import Counter
import gzip
import json
import os
from pathlib import Path
import uuid

from discover_videos import open_database
from metadata_bulk import atomic_json, digest, initialize, utcnow
from proxy_catalog import load_catalog


def export_snapshot(folder, ranked, limit=0, *, missing_only=False):
    folder, ranked = Path(folder), Path(ranked)
    folder.mkdir(parents=True, exist_ok=False)
    ranking = [json.loads(line) for line in ranked.read_text().splitlines()]
    identifiers = [row['proxy_id'] for row in ranking]
    if not identifiers or len(set(identifiers)) != len(identifiers):
        raise ValueError('Responder pool contains duplicate IDs or is empty')
    by_id = {p.proxy_id: p for p in load_catalog(identifiers)}
    manifest = {'version': 1, 'collector': 'statistics', 'run_id': str(uuid.uuid4()),
                'created_at': utcnow(), 'files': {}, 'ranked_pool_sha256': digest(ranked),
                'selection': 'missing_counts' if missing_only else 'all_videos'}
    counts = Counter()
    videos = folder / 'videos.jsonl.gz'
    where = 'WHERE view_count IS NULL OR like_count IS NULL' if missing_only else ''
    order = 'md5(video_id),video_id' if limit else 'video_id'
    query = f'SELECT video_id,type,(metadata_updated_at IS NULL)::integer FROM videos {where} ORDER BY {order}'
    if limit:
        query += ' LIMIT %s'
    with open_database() as conn, videos.open('wb') as raw:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
        with conn.cursor(name='statistics_snapshot') as cursor:
            cursor.itersize = 10000
            cursor.execute(query, (limit,) if limit else ())
            with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=1) as output:
                for video_id, kind, priority in cursor:
                    output.write((json.dumps([video_id, kind, priority], separators=(',', ':')) + '\n').encode())
                    counts.update(videos=1, **{kind: 1})
        raw.flush()
        os.fsync(raw.fileno())
    catalog = folder / 'proxies.jsonl.gz'
    with catalog.open('wb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0, compresslevel=1) as output:
            for identifier in identifiers:
                output.write((json.dumps(by_id[identifier].bridge_record(), separators=(',', ':')) + '\n').encode())
        raw.flush()
        os.fsync(raw.fileno())
    manifest.update(counts, proxies=len(by_id), protocols=dict(Counter(p.working_protocol for p in by_id.values())))
    for file in (videos, catalog):
        file.chmod(0o600)
        manifest['files'][file.name] = {'bytes': file.stat().st_size, 'sha256': digest(file)}
    atomic_json(folder / 'manifest.json', manifest)
    return manifest


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    export = sub.add_parser('export')
    export.add_argument('--output', type=Path, required=True)
    export.add_argument('--ranked', type=Path, required=True)
    export.add_argument('--limit', type=int, default=0)
    export.add_argument('--missing-only', action='store_true')
    init = sub.add_parser('init')
    init.add_argument('--run', type=Path, required=True)
    init.add_argument('--workers', type=int, default=16)
    init.add_argument('--concurrency', type=int, default=256)
    init.add_argument('--max-attempts', type=int, default=3)
    args = parser.parse_args()
    if args.command == 'export':
        if args.limit < 0:
            parser.error('Limit cannot be negative')
        result = export_snapshot(args.output, args.ranked, args.limit, missing_only=args.missing_only)
    else:
        if args.workers < 1 or not 1 <= args.concurrency <= 1024 or args.max_attempts < 1:
            parser.error('Invalid worker, concurrency or attempt setting')
        result = initialize(args.run, args.workers, args.concurrency, args.max_attempts)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
