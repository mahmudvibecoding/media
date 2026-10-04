"""Recover connection observations from retained journals without replaying old counters.

Only temporary tables hold journal rows. The three new statistics fields are
initialized once; repeating the same backfill cannot add its counts again.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import time

import psycopg

from import_results import import_journal
from runtime_config import connect_database


ROOT = Path(__file__).resolve().parent.parent
IMPORT_LOCK = 6389247650123


def discover_journals(paths):
    journals = {}
    for path in map(Path, paths):
        candidates = list(path.rglob('results.jsonl')) + list(path.rglob('results.jsonl.gz')) if path.is_dir() else [path]
        for candidate in candidates:
            base = str(candidate.resolve()).removesuffix('.gz')
            if base not in journals or candidate.suffix == '.gz':
                journals[base] = candidate
    if not journals:
        raise ValueError('No saved journals were found')
    return [journals[key] for key in sorted(journals)]


def prepare_history(conn):
    conn.execute('''CREATE TEMP TABLE connection_backfill (
        proxy_id BIGINT PRIMARY KEY, connection_key BYTEA NOT NULL, declared_protocol TEXT NOT NULL,
        checked_at TIMESTAMPTZ, last_connection_error TEXT,
        connection_attempts BIGINT NOT NULL, requests_sent BIGINT NOT NULL,
        successful_connections BIGINT NOT NULL, last_connected_at TIMESTAMPTZ
    )''')


def merge_history(conn):
    mismatch = conn.execute('''SELECT count(*) FROM import_proxy_results s JOIN connection_backfill h USING(proxy_id)
        WHERE s.connection_key <> h.connection_key OR s.declared_protocol <> h.declared_protocol''').fetchone()[0]
    if mismatch:
        raise ValueError('A proxy identity changed between saved journals')
    conn.execute('''INSERT INTO connection_backfill AS h
        SELECT proxy_id,connection_key,declared_protocol,
               CASE WHEN attempted THEN checked_at END,connection_error,
               attempted::integer,requests_sent,connected::integer,
               CASE WHEN connected THEN checked_at END
        FROM import_proxy_results
        ON CONFLICT (proxy_id) DO UPDATE SET
            last_connection_error=CASE WHEN excluded.checked_at IS NOT NULL
                AND (h.checked_at IS NULL OR excluded.checked_at >= h.checked_at)
                THEN excluded.last_connection_error ELSE h.last_connection_error END,
            checked_at=greatest(h.checked_at,excluded.checked_at),
            connection_attempts=h.connection_attempts+excluded.connection_attempts,
            requests_sent=h.requests_sent+excluded.requests_sent,
            successful_connections=h.successful_connections+excluded.successful_connections,
            last_connected_at=greatest(h.last_connected_at,excluded.last_connected_at)''')


def stage_history(conn, journals, batch_size=10000):
    prepare_history(conn)
    seen, reports = set(), []
    for path in journals:
        result = import_journal(conn, path, batch_size, stage_only=True)
        key = result['journal_key']
        if key in seen:
            continue
        seen.add(key)
        merge_history(conn)
        reports.append({'file':str(Path(path).resolve()),'run':result['run'],
                        'records':result['results'],'sha256':key.hex()})
        print(json.dumps({'event':'journal_staged',**reports[-1]}),flush=True)
    conn.execute('ANALYZE connection_backfill')
    return reports


def validate_history(conn):
    row = conn.execute('''SELECT
        count(*) FILTER(WHERE p.proxy_id IS NULL OR p.connection_key<>b.connection_key
            OR p.connection_settings->>'transport' IS DISTINCT FROM b.declared_protocol),
        count(*) FILTER(WHERE s.proxy_id IS NULL
            OR (b.checked_at IS NOT NULL AND (s.youtube_last_attempt_at IS NULL OR s.youtube_last_attempt_at<b.checked_at))
            OR s.connection_attempts<b.connection_attempts OR s.youtube_requests_sent<b.requests_sent),
        count(*) FILTER(WHERE b.successful_connections<0 OR b.successful_connections>b.connection_attempts
            OR (b.last_connected_at IS NOT NULL AND b.last_connected_at>b.checked_at))
        FROM connection_backfill b LEFT JOIN proxies p USING(proxy_id)
        LEFT JOIN proxy_stats s USING(proxy_id)''').fetchone()
    if any(row):
        raise ValueError(f'Backfill validation failed: identity, unimported history, invalid observations = {tuple(row)}')


def history_summary(conn):
    row = conn.execute('''SELECT count(*),count(*) FILTER(WHERE successful_connections>0),
        sum(successful_connections),sum(connection_attempts),sum(requests_sent)
        FROM connection_backfill''').fetchone()
    return dict(zip(('proxies','connected_proxies','successful_connections','connection_attempts','requests_sent'),
                    (int(value or 0) for value in row)))


def apply_backfill(conn):
    """Initialize recorded successes, or recognize a safely repeated backfill.

    Post-journal requests prove additional connections. Other old collection
    attempts lacked connection telemetry, so their outcomes remain unknown.
    A new writer must not start populating connection counts before the initial
    backfill; initialization detects that case instead of losing its counts.
    """
    with conn.transaction():
        if not conn.execute('SELECT pg_try_advisory_xact_lock(%s)',(IMPORT_LOCK,)).fetchone()[0]:
            raise RuntimeError('Another statistics import is running; retry the backfill later')
        validate_history(conn)
        conn.execute('DROP TABLE IF EXISTS pg_temp.connection_backfill_expected')
        conn.execute('''CREATE TEMP TABLE connection_backfill_expected ON COMMIT DROP AS
            SELECT b.proxy_id,
                b.successful_connections + (s.youtube_requests_sent-b.requests_sent) AS successful_connections,
                greatest(b.last_connected_at,s.youtube_last_response_at) AS last_connected_at,
                CASE WHEN s.last_connection_attempt_at=b.checked_at THEN b.last_connection_error
                     ELSE s.last_connection_error END AS last_connection_error
            FROM connection_backfill b JOIN proxy_stats s USING(proxy_id)''')
        conn.execute('CREATE UNIQUE INDEX ON connection_backfill_expected(proxy_id)')
        conn.execute('ANALYZE connection_backfill_expected')
        existing, insufficient = conn.execute('''SELECT
            count(*) FILTER(WHERE s.successful_connections>0),
            count(*) FILTER(WHERE s.successful_connections<e.successful_connections)
            FROM connection_backfill_expected e JOIN proxy_stats s USING(proxy_id)''').fetchone()
        if existing and insufficient:
            raise ValueError('Connection counts were written before initialization, or this is a different history snapshot; refusing to overwrite them')
        updated = conn.execute('''UPDATE proxy_stats s SET
                successful_connections=greatest(s.successful_connections,e.successful_connections),
                last_connected_at=greatest(s.last_connected_at,e.last_connected_at),
                last_connection_error=e.last_connection_error
            FROM connection_backfill_expected e WHERE s.proxy_id=e.proxy_id
            AND (s.successful_connections,s.last_connected_at,s.last_connection_error) IS DISTINCT FROM
                (greatest(s.successful_connections,e.successful_connections),
                 greatest(s.last_connected_at,e.last_connected_at),e.last_connection_error)''').rowcount
        return {'updated_proxies':updated,'mode':'already_initialized' if existing else 'initialized'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('paths',nargs='+',type=Path,help='Saved journal files or directories')
    parser.add_argument('--apply',action='store_true',help='Write only the three connection fields')
    parser.add_argument('--expected-connected',type=int,help='Verify this many proxies connected in the saved journals')
    parser.add_argument('--output',type=Path,help='Save aggregate verification evidence')
    args=parser.parse_args()
    started=time.monotonic()
    report={'started_at':datetime.now(timezone.utc).isoformat(),'applied':False}
    with connect_database("proxy", autocommit=True, application_name='proxy-connection-backfill') as conn:
        report['journals']=stage_history(conn,discover_journals(args.paths))
        validate_history(conn)
        report['history']=history_summary(conn)
        if args.expected_connected is not None and report['history']['connected_proxies']!=args.expected_connected:
            raise ValueError('The saved connection count does not match the expected count')
        if args.apply:
            report['write']=apply_backfill(conn)
            report['applied']=True
        report['stored']=dict(zip(('connected_proxies','successful_connections','proxies_with_latest_connection_error'),
            map(int,conn.execute('''SELECT count(*) FILTER(WHERE successful_connections>0),
                coalesce(sum(successful_connections),0),count(*) FILTER(WHERE last_connection_error IS NOT NULL)
                FROM proxy_stats''').fetchone())))
    report.update(finished_at=datetime.now(timezone.utc).isoformat(),seconds=round(time.monotonic()-started,3))
    if args.output:
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:value for key,value in report.items() if key!='journals'}),flush=True)


if __name__=='__main__':
    main()
