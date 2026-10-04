"""A deterministic HTTP-fixture collection cycle for a disposable Docker database."""
import argparse
import asyncio
from contextlib import closing
from dataclasses import asdict
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'tests')]

import httpx

from collect_video_comments import CommentBuffer, collect_comments, load_saved_history, save_scan
from collect_video_metadata import fetch_metadata
from collect_video_stats import fetch_stats
from discover_videos import scan_tab
from manage import add_channels
from metadata_bulk import atomic_json, claim, connect_queue, digest, export_events, finish, initialize, queue_status
from proxy_statistics import AttemptOutcome
from proxy_formats import Proxy, pack_connection_settings
from proxy_service_common import SERVICE_DIR, read_json, write_json, utcnow
from psycopg.types.json import Jsonb
from runtime_config import OUTPUT_DIR, STATE_DIR, connect_database
from test_collect_video_comments import page as comment_page
from test_collect_video_metadata import payload as metadata_payload
from test_discover_videos import card, page as discovery_page
from test_video_stats import VIDEO, payload as statistics_payload

CHANNEL = 'UC' + 'd' * 22
PROXY_KEY = hashlib.sha256(b'docker-smoke-fixture').digest()
REPORT = OUTPUT_DIR / 'docker-smoke.json'


def prepare_manual_proxies():
    # A saved catalog selection exercises the real shell command without GitHub
    # downloads or public-network traffic in this disposable verification project.
    with connect_database('proxy') as conn:
        assert conn.execute('SELECT count(*) FROM public.proxies').fetchone()[0] == 0
        for port in (1, 2):
            proxy = Proxy('127.0.0.1', port, 'http', {})
            conn.execute('''INSERT INTO public.proxies(connection_key,address,port,connection_settings,last_seen_at)
                VALUES (%s,%s,%s,%s,clock_timestamp())''',
                (proxy.key,proxy.address,proxy.port,Jsonb(pack_connection_settings(proxy.protocol,proxy.settings))))
        maximum = conn.execute('SELECT max(proxy_id) FROM public.proxies').fetchone()[0]
    write_json(SERVICE_DIR/'current-refresh.json', {'run_id':str(uuid.uuid4()), 'started_at':utcnow().isoformat(),
        'catalog':{'fixture':True}, 'max_id':maximum, 'configurations':2, 'pass':1,
        'after_id':0, 'observations':0, 'last_batch':None})


def verify_manual_proxies():
    assert not (SERVICE_DIR/'current-refresh.json').exists()
    report = read_json(SERVICE_DIR/'last-refresh.json')
    assert report['passes_completed'] == 3 and report['observations'] == 6
    assert report['pool']['limit'] == 0
    assert report['pool']['scoring'] == 'youtube_responses_last_three'
    with connect_database('proxy') as conn:
        assert conn.execute('SELECT count(*) FROM public.proxy_stats').fetchone()[0] == 0
        assert conn.execute('SELECT count(*) FROM app_meta.proxy_pool_results').fetchone()[0] == 0
    assert report['checkpoints'] is False and report['new_checks'] == 6
    assert not list(SERVICE_DIR.glob('proxy-results-*'))
    print(json.dumps({'manual_proxy_command':'passed','configurations':2,'passes':3,'observations':6}))


def snapshot():
    with connect_database('media') as conn:
        video = conn.execute('''SELECT video_id,title,duration_seconds,view_count,like_count,
            metadata_updated_at,stats_updated_at,comments_updated_at,comments_error
            FROM public.videos WHERE video_id=%s''', (VIDEO,)).fetchone()
        comments = conn.execute('''SELECT comment_id,text,is_pinned FROM public.comments
            WHERE video_id=%s ORDER BY comment_id''', (VIDEO,)).fetchall()
    with connect_database('proxy') as conn:
        stats = conn.execute('''SELECT connection_attempts,youtube_successful_data_received
            FROM public.proxy_stats s JOIN public.proxies p USING(proxy_id)
            WHERE p.connection_key=%s''', (PROXY_KEY,)).fetchone()
    return json.loads(json.dumps({'video': video, 'comments': comments, 'proxy_statistics': stats}, default=str))


def import_result(collector, result, proxy_id):
    folder = STATE_DIR / ('smoke-' + collector)
    folder.mkdir()
    files = {}
    for name, rows in (('videos.jsonl.gz', [[VIDEO, 'video', 0]]), ('proxies.jsonl.gz', [])):
        with gzip.open(folder / name, 'wt') as stream:
            for row in rows:
                stream.write(json.dumps(row) + '\n')
        files[name] = {'sha256': digest(folder / name)}
    atomic_json(folder / 'manifest.json', {'collector': collector, 'run_id': str(uuid.uuid4()),
                'files': files, 'videos': 1})
    initialize(folder, 1, 1, 1)
    claim(folder, 0, 1)
    at = datetime.now(timezone.utc)
    finish(folder, 0, [{'video_id': VIDEO, 'at': at.isoformat(), 'result': result,
        'proxy': {'id': proxy_id, 'key': PROXY_KEY.hex(), 'protocol': 'http'},
        'observation': asdict(AttemptOutcome(at, True, 200, True))}])
    export_events(folder)
    with connect_queue(folder / 'queue.sqlite3') as conn:
        conn.execute("UPDATE settings SET value=? WHERE key='state'", (json.dumps('complete'),))
    atomic_json(folder / 'status.json', queue_status(folder))
    importer = 'metadata_bulk_import.py' if collector == 'metadata' else 'video_stats_bulk_import.py'
    command = [sys.executable, str(ROOT / importer), '--run', str(folder), '--once']
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
    first = snapshot()
    # Lose the local acknowledgement and replay the immutable database batch.
    (folder / 'import-status.json').unlink()
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
    assert snapshot() == first, 'Batch replay changed stored rows or counters'
    assert json.loads((folder / 'import-status.json').read_text())['complete']


async def collect():
    assert not REPORT.exists(), 'Use a fresh disposable Docker project for this check'
    add_channels([CHANNEL])
    with connect_database('proxy') as conn:
        proxy_id = conn.execute('''INSERT INTO public.proxies(connection_key,address,port,last_seen_at)
            VALUES (%s,'192.0.2.1',8080,now()) RETURNING proxy_id''', (PROXY_KEY,)).fetchone()[0]
    with connect_database('media', autocommit=True) as conn:
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=discovery_page(items=[card(VIDEO)])))) as client:
            first = await scan_tab(client, conn, CHANNEL, 'video', retries=0)
            again = await scan_tab(client, conn, CHANNEL, 'video', retries=0)
            assert first['scan_complete'] and first['videos_inserted'] == 1
            assert again['scan_complete'] and again['videos_inserted'] == 0
        for collector, fetch, payload in (('metadata', fetch_metadata, metadata_payload(VIDEO)),
                                          ('statistics', fetch_stats, statistics_payload())):
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json=payload))) as client:
                result = await fetch(client, VIDEO, retries=0)
            import_result(collector, result, proxy_id)
        pages = iter([comment_page(['a'], token='second'), comment_page(['b'], with_header=False)])
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=next(pages)))) as client:
            with closing(CommentBuffer()) as buffer:
                result = await collect_comments(client, VIDEO, buffer, retries=0)
                assert result['complete'] and result['pages'] == 2
                assert save_scan(conn, result, buffer) == 2
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=comment_page(['new', 'a'], token='unused')))) as client:
            with closing(CommentBuffer()) as buffer:
                load_saved_history(conn, VIDEO, buffer)
                result = await collect_comments(client, VIDEO, buffer, retries=0)
                assert result['complete'] and result['stop_reason'] == 'saved_history'
                assert save_scan(conn, result, buffer) == 1
    saved = snapshot()
    assert saved['video'][1:5] == ['Test title', 577, 348, 3]
    assert len(saved['comments']) == 3 and saved['proxy_statistics'] == [2, 2]
    (STATE_DIR / 'docker-smoke-sentinel').write_text('persistent state\n')
    atomic_json(REPORT, saved)
    print(json.dumps({'fixture_cycle': 'passed', 'channels': 1, 'videos': 1, 'comments': 3,
                      'metadata_and_statistics_replay': 'passed'}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('collect', 'verify', 'prepare-proxies', 'verify-proxies'))
    command = parser.parse_args().command
    if command == 'prepare-proxies':
        prepare_manual_proxies()
    elif command == 'verify-proxies':
        verify_manual_proxies()
    elif command == 'collect':
        asyncio.run(collect())
    else:
        assert (STATE_DIR / 'docker-smoke-sentinel').read_text() == 'persistent state\n'
        assert snapshot() == json.loads(REPORT.read_text()), 'Data changed after the container restart'
        print(json.dumps({'restart': 'passed', 'database': 'preserved', 'state': 'preserved', 'outputs': 'preserved'}))


if __name__ == '__main__':
    main()
