"""Import immutable, finished journals into bounded per-proxy statistics.

Import rounds in chronological order. An exact retry of a proxy's latest journal
is skipped. Older or changed overlapping attempt results fail without changing
totals. Checks rejected before an attempt have no stored timestamp; their errors
follow import order.
"""

import argparse
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from proxy_statistics import proxy_connection_error

COLUMNS = ('proxy_id,connection_key,checked_at,status,attempted,responds,'
           'declared_protocol,detected_protocol,http_status,total_ms,requests_sent,'
           'connected,connection_error,website_error')


def connection_observation(result):
    """One connection outcome per check, even if it tried multiple handshakes."""
    attempts = result['attempts']
    connected = False
    for attempt in attempts:
        value = attempt.get('connected')
        if value is not None and type(value) is not bool:
            raise ValueError('Invalid connection observation in journal')
        sent = bool(attempt.get('request_sent'))
        if value is False and sent:
            raise ValueError('A sent request requires a connection')
        connected |= value is True or sent or attempt.get('status') == 'responds'
    if connected and not result['attempted']:
        raise ValueError('A connection requires an attempted check')
    last = attempts[-1] if attempts else {}
    stage, code = last.get('stage'), last.get('error_code')
    if result['responds']:
        stage, code = None, None
    error = f'{stage}:{code}' if stage and code else None
    if error is not None and re.fullmatch(r'[a-z][a-z0-9_]*:[a-z][a-z0-9_]*', error) is None:
        raise ValueError('Invalid connection error label in journal')
    return connected, proxy_connection_error(error)


def website_error(result, connection_error):
    last = result['attempts'][-1] if result['attempts'] else {}
    error = connection_error
    if error is None and last.get('body_error'):
        error = 'youtube_body:' + last['body_error']
    if error is None and not result['responds'] and last.get('stage') and last.get('error_code'):
        error = last['stage'] + ':' + last['error_code']
    if error is None and result['responds'] and last.get('http_status', 0) >= 300:
        error = 'http:http_' + str(last['http_status'])
    if error is not None and re.fullmatch(r'[a-z][a-z0-9_]*:[a-z][a-z0-9_]*', error) is None:
        raise ValueError('Invalid website error label in journal')
    return error


def prepare_stage(conn):
    conn.execute('''CREATE TEMP TABLE IF NOT EXISTS import_proxy_results (
        proxy_id BIGINT PRIMARY KEY, connection_key BYTEA NOT NULL,
        checked_at TIMESTAMPTZ NOT NULL, status TEXT NOT NULL,
        attempted BOOLEAN NOT NULL, responds BOOLEAN NOT NULL, declared_protocol TEXT NOT NULL,
        detected_protocol TEXT, http_status SMALLINT, total_ms DOUBLE PRECISION NOT NULL,
        requests_sent BIGINT NOT NULL,
        connected BOOLEAN NOT NULL, connection_error TEXT, website_error TEXT,
        CHECK (status IN ('responds','not_responding','invalid_configuration','incompatible_protocol')),
        CHECK (attempted = (status IN ('responds','not_responding'))),
        CHECK (responds = (status = 'responds')),
        CHECK (responds = (detected_protocol IS NOT NULL)),
        CHECK (responds = (http_status IS NOT NULL))
    )''')
    conn.execute('TRUNCATE import_proxy_results')


def stage_rows(conn, rows):
    if rows:
        with conn.cursor().copy(f'COPY import_proxy_results ({COLUMNS}) FROM STDIN') as copy:
            for row in rows:
                copy.write_row(row)


def apply_staged(conn, journal_key):
    """Validate the whole journal, then update all counters in one transaction."""
    with conn.transaction():
        if not conn.execute('SELECT pg_try_advisory_xact_lock(6389247650123)').fetchone()[0]:
            raise RuntimeError('Another importer or database cleanup is running')
        conn.execute('ANALYZE import_proxy_results')
        mismatches = conn.execute('''SELECT count(*) FROM import_proxy_results s
            LEFT JOIN proxies p USING(proxy_id)
            WHERE p.proxy_id IS NULL OR p.connection_key <> s.connection_key
               OR p.connection_settings->>'transport' IS DISTINCT FROM s.declared_protocol''').fetchone()[0]
        if mismatches:
            raise ValueError(f'Refusing import: {mismatches} configurations do not match this catalog')
        stale = conn.execute('''SELECT count(*) FROM import_proxy_results s
            JOIN proxy_stats h USING(proxy_id)
            WHERE h.youtube_last_import_key IS DISTINCT FROM %s
                AND s.checked_at <= h.youtube_last_attempt_at''', (journal_key,)).fetchone()[0]
        if stale:
            raise ValueError(f'Refusing {stale} older or changed results; import immutable journals in chronological order')
        inserted = conn.execute('''
            INSERT INTO proxy_stats AS h
                (proxy_id,connection_attempts,successful_connections,last_connection_attempt_at,
                 last_connected_at,last_connection_error,
                 youtube_last_attempt_at,working_protocol,
                 youtube_last_http_status,youtube_last_response_at,
                 youtube_requests_sent,youtube_responses_received,youtube_last_import_key,youtube_last_error)
            SELECT s.proxy_id,s.attempted::integer,s.connected::integer,
                   CASE WHEN s.attempted THEN s.checked_at END,
                   CASE WHEN s.connected THEN s.checked_at END,
                   CASE WHEN s.attempted THEN s.connection_error END,
                   CASE WHEN s.attempted THEN s.checked_at END,s.detected_protocol,
                   s.http_status,CASE WHEN s.responds THEN s.checked_at END,
                   s.requests_sent,s.responds::integer,%s,s.website_error
            FROM import_proxy_results s LEFT JOIN proxy_stats previous USING(proxy_id)
            WHERE previous.youtube_last_import_key IS DISTINCT FROM %s
            ON CONFLICT (proxy_id) DO UPDATE SET
                connection_attempts=h.connection_attempts+excluded.connection_attempts,
                successful_connections=h.successful_connections+excluded.successful_connections,
                last_connection_attempt_at=greatest(h.last_connection_attempt_at,excluded.last_connection_attempt_at),
                last_connected_at=greatest(h.last_connected_at,excluded.last_connected_at),
                last_connection_error=CASE WHEN excluded.last_connection_attempt_at IS NOT NULL
                    AND (h.last_connection_attempt_at IS NULL OR excluded.last_connection_attempt_at>=h.last_connection_attempt_at)
                    THEN excluded.last_connection_error ELSE h.last_connection_error END,
                youtube_last_attempt_at=coalesce(excluded.youtube_last_attempt_at,h.youtube_last_attempt_at),
                working_protocol=coalesce(excluded.working_protocol,h.working_protocol),
                youtube_last_http_status=CASE WHEN excluded.youtube_last_attempt_at IS NOT NULL
                    THEN excluded.youtube_last_http_status ELSE h.youtube_last_http_status END,
                youtube_last_response_at=coalesce(excluded.youtube_last_response_at,h.youtube_last_response_at),
                youtube_last_error=excluded.youtube_last_error,
                youtube_requests_sent=h.youtube_requests_sent+excluded.youtube_requests_sent,
                youtube_responses_received=h.youtube_responses_received+excluded.youtube_responses_received,
                youtube_last_import_key=excluded.youtube_last_import_key
        ''', (journal_key,journal_key)).rowcount
    return inserted


def import_journal(conn, journal, batch_size, source_host=None, *, stage_only=False):
    journal = Path(journal)
    base = Path(str(journal).removesuffix('.gz'))
    metadata = json.loads(Path(str(base) + '.meta.json').read_text())
    summary = json.loads(Path(str(base) + '.summary.json').read_text())
    sealed_path = Path(str(base) + '.sealed.json')
    sealed = json.loads(sealed_path.read_text()) if sealed_path.exists() else None
    if not metadata.get('run_id'):
        raise ValueError('Journal metadata must identify its round')
    if sealed:
        with journal.open('rb') as source:
            actual = hashlib.file_digest(source, 'sha256').hexdigest()
        expected = sealed['compressed_sha256'] if journal.suffix == '.gz' else sealed['uncompressed_sha256']
        if actual != expected:
            raise ValueError('Transferred journal checksum mismatch')
    if summary['state'] not in ('complete', 'interrupted', 'local_overload'):
        raise ValueError('Only a stopped journal with a final summary can be imported')
    prepare_stage(conn)
    opener = gzip.open if journal.suffix == '.gz' else open
    digest = hashlib.sha256()
    rows = []
    count = attempted_count = response_count = 0
    started, last_progress = time.monotonic(), time.monotonic()
    with opener(journal, 'rb') as source:
        for line in source:
            digest.update(line)
            if not line.endswith(b'\n'):
                raise ValueError('Journal has an incomplete final line')
            result = json.loads(line)
            if result['status'] == 'local_error':
                continue
            status = result['status']
            if (status not in ('responds','not_responding','invalid_configuration','incompatible_protocol')
                or result['attempted'] is not (status in ('responds','not_responding'))
                or result['responds'] is not (status == 'responds')
                or bool(result.get('detected_protocol')) != result['responds']):
                raise ValueError('Status must consistently determine attempted, responds and detected protocol')
            attempts = result['attempts']
            replies = [a for a in attempts if a['status'] == 'responds']
            requests = sum(bool(a.get('request_sent')) for a in attempts)
            if (bool(replies) != result['responds']
                or any(not a.get('tls_verified') for a in replies)
                or bool(replies) != (result['status'] == 'responds')
                or (replies and (not result.get('detected_protocol') or requests != 1))
                or (requests and not result['attempted'])):
                raise ValueError('Response classification, request count or TLS verification is inconsistent')
            if len(attempts) > 4 or len(replies) > 1 or requests > 1:
                raise ValueError('Unexpected repeated metadata requests in a candidate result')
            checked = datetime.fromisoformat(result['tested_at'].replace('Z', '+00:00'))
            if checked.tzinfo is None:
                raise ValueError('Test timestamp must include a time zone')
            connected, connection_error = connection_observation(result)
            rows.append((result['id'],bytes.fromhex(result['key']),checked,result['status'],result['attempted'],
                         result['responds'],result['declared_protocol'],result.get('detected_protocol'),
                         replies[0]['http_status'] if replies else None,result['total_ms'],requests,
                         connected,connection_error,website_error(result,connection_error)))
            count += 1
            attempted_count += result['attempted']
            response_count += result['responds']
            if len(rows) >= batch_size:
                stage_rows(conn, rows)
                rows.clear()
            if time.monotonic() - last_progress >= 10:
                print(json.dumps({'run':metadata['run_id'],'validated':count,
                                  'seconds':round(time.monotonic()-started,1)}), flush=True)
                last_progress = time.monotonic()
    stage_rows(conn, rows)
    expected = summary['counters']
    if (count,attempted_count,response_count) != (expected['completed'],expected['attempted'],expected['youtube_responses']):
        raise ValueError('Journal counts differ from the final summary')
    if sealed and digest.hexdigest() != sealed['uncompressed_sha256']:
        raise ValueError('Uncompressed journal checksum mismatch')
    if stage_only:
        return {'run':metadata['run_id'], 'results':count, 'journal_key':digest.digest()}
    inserted = apply_staged(conn, digest.digest())
    report = {'run':metadata['run_id'],'complete':True,'results':count,'new_results':inserted,
              'already_imported':inserted == 0,'seconds':round(time.monotonic()-started,1)}
    print(json.dumps(report), flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('journals', nargs='+', type=Path)
    parser.add_argument('--batch-size', type=int, default=10000)
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error('batch size must be positive')
    os.umask(0o077)
    with psycopg.connect(dbname='proxy',user='mahmud',host=str(ROOT / '.local/postgres/socket'),
                         port=5432,connect_timeout=5,autocommit=True) as conn:
        conn.execute("SET temp_buffers = '128MB'")
        conn.execute("SET work_mem = '128MB'")
        conn.execute('SET jit = off')
        if not conn.execute('SELECT pg_try_advisory_lock(6389247650123)').fetchone()[0]:
            raise SystemExit('Another results importer or cleanup is running')
        for journal in args.journals:
            import_journal(conn, journal, args.batch_size)


if __name__ == '__main__':
    main()
