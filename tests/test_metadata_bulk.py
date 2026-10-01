"""Durable leases, verified batches, and recovery across database commits."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid

import httpx
import psycopg

from collect_video_metadata import fetch_metadata, metadata_error_reason
from metadata_bulk import (ProxyPool, atomic_json, claim, connect_queue, digest,
    export_events, finish, initialize, queue_status, recover_leases)
from metadata_bulk_import import apply_chunk, read_chunk, statistics_batch
from proxy_catalog import CatalogProxy
from proxy_statistics import AttemptOutcome


ROOT = Path(__file__).resolve().parents[1]
AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
VIDEO = 'RtXBV0X1v1Q'


def event(video=VIDEO, success=True, observed=True, proxy_id=1):
    result = {'video_id': video, 'status': 'ok' if success else 'error',
              'metadata': {'title': 'New title'} if success else None}
    if not success:
        result['error'] = 'ConnectError'
    return {'video_id': video, 'at': AT.isoformat(), 'result': result,
        'proxy': {'id': proxy_id, 'key': (b'x' * 32).hex(), 'protocol': 'http'},
        'observation': asdict(AttemptOutcome(AT, True, 200, True)) if success else
            asdict(AttemptOutcome(AT, False, None, False)) if observed else None}


class QueueFixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.folder = Path(self.temp.name)
        self.run_id = str(uuid.uuid4())

    def tearDown(self):
        self.temp.cleanup()

    def initialize(self, videos=None, attempts=3):
        videos = videos or [[VIDEO, 'video', 0]]
        files = {}
        for name, rows in (('videos.jsonl.gz', videos), ('proxies.jsonl.gz', [])):
            path = self.folder / name
            with gzip.open(path, 'wt') as output:
                for row in rows:
                    output.write(json.dumps(row) + '\n')
            files[name] = {'sha256': digest(path)}
        atomic_json(self.folder / 'manifest.json', {'run_id': self.run_id, 'files': files,
                                                   'videos': len(videos)})
        initialize(self.folder, 2, 4, attempts)


class QueueTests(QueueFixture, unittest.TestCase):
    def test_concurrent_claims_are_distinct_and_restart_only_recovers_owned_leases(self):
        self.initialize([[f'{i:011d}', 'video', 0] for i in range(100)])
        with ThreadPoolExecutor(max_workers=2) as pool:
            a, b = list(pool.map(lambda worker: claim(self.folder, worker, 50), (0, 1)))
        self.assertEqual(len(a), 50)
        self.assertFalse({r['video_id'] for r in a} & {r['video_id'] for r in b})
        self.assertEqual(recover_leases(self.folder, 0), 50)
        self.assertEqual({r['video_id'] for r in claim(self.folder, 0, 100)}, {r['video_id'] for r in a})

    def test_prior_errors_are_processed_after_fresh_videos(self):
        self.initialize([['00000000000', 'video', 1], [VIDEO, 'video', 0]])
        self.assertEqual(claim(self.folder, 0, 1)[0]['video_id'], VIDEO)

    def test_retries_exhaust_exactly_three_observed_attempts(self):
        self.initialize()
        for attempt in range(1, 4):
            self.assertEqual(claim(self.folder, 0, 1)[0]['attempts'], attempt - 1)
            finish(self.folder, 0, [event(success=False)])
            with connect_queue(self.folder / 'queue.sqlite3') as conn:
                row = conn.execute('SELECT status,attempts FROM jobs').fetchone()
                self.assertEqual(tuple(row), ('failed' if attempt == 3 else 'retry', attempt))
                conn.execute("UPDATE jobs SET status='ready' WHERE status='retry'")
        self.assertEqual(queue_status(self.folder)['counters']['failed'], 1)

    def test_local_failures_never_count_as_proxy_attempts(self):
        self.initialize()
        for _ in range(5):
            claim(self.folder, 0, 1)
            finish(self.folder, 0, [event(success=False, observed=False)])
        state = queue_status(self.folder)
        self.assertEqual(state['counters']['attempts'], 0)
        self.assertEqual(state['jobs'], {'failed': 1})
        export_events(self.folder)
        _, rows = read_chunk(next((self.folder / 'outbox').glob('*.gz')), self.run_id)
        self.assertFalse(statistics_batch(rows, b'k' * 32).aggregates)

    def test_wrong_owner_and_repeated_completion_cannot_write_results(self):
        self.initialize()
        claim(self.folder, 0, 1)
        with self.assertRaises(ValueError):
            finish(self.folder, 1, [event()])
        finish(self.folder, 0, [event()])
        with self.assertRaises(ValueError):
            finish(self.folder, 0, [event()])
        self.assertEqual(queue_status(self.folder)['counters']['saved'], 1)

    def test_outbox_is_verified_and_reexport_after_cursor_loss_is_identical(self):
        self.initialize()
        claim(self.folder, 0, 1)
        finish(self.folder, 0, [event()])
        self.assertEqual(export_events(self.folder), 1)
        path = next((self.folder / 'outbox').glob('*.gz'))
        info, rows = read_chunk(path, self.run_id)
        with connect_queue(self.folder / 'queue.sqlite3') as conn:
            conn.execute("UPDATE settings SET value='0' WHERE key='exported_seq'")
        export_events(self.folder)
        self.assertEqual(read_chunk(path, self.run_id), (info, rows))
        with path.open('ab') as output:
            output.write(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'checksum'):
            read_chunk(path, self.run_id)

    def test_valid_checksum_cannot_hide_a_video_id_mismatch(self):
        self.initialize()
        claim(self.folder, 0, 1)
        finish(self.folder, 0, [event()])
        export_events(self.folder)
        path = next((self.folder / 'outbox').glob('*.gz'))
        info, rows = read_chunk(path, self.run_id)
        rows[0]['event']['result']['video_id'] = '00000000000'
        with gzip.open(path, 'wt') as output:
            output.write(json.dumps(rows[0]) + '\n')
        info['sha256'] = digest(path)
        atomic_json(path.with_suffix(path.suffix + '.json'), info)
        with self.assertRaisesRegex(ValueError, 'identity mismatch'):
            read_chunk(path, self.run_id)

    def test_export_restart_preserves_published_boundaries_when_more_results_arrive(self):
        self.initialize([['00000000000', 'video', 0], [VIDEO, 'video', 0]])
        first = claim(self.folder, 0, 1)[0]['video_id']
        finish(self.folder, 0, [event(first)])
        export_events(self.folder)
        second = claim(self.folder, 0, 1)[0]['video_id']
        finish(self.folder, 0, [event(second)])
        with connect_queue(self.folder / 'queue.sqlite3') as conn:
            conn.execute("UPDATE settings SET value='0' WHERE key='exported_seq'")
        self.assertEqual(export_events(self.folder), 1)
        self.assertEqual(export_events(self.folder), 1)
        chunks = [read_chunk(path, self.run_id)[0] for path in sorted((self.folder / 'outbox').glob('*.gz'))]
        self.assertEqual([(i['first_seq'], i['last_seq']) for i in chunks], [(1, 1), (2, 2)])


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_wall_deadline_interrupts_a_stalled_body_and_records_one_attempt(self):
        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                await asyncio.sleep(10)
                yield b'{}'
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda _: httpx.Response(200, stream=Body()))) as client:
            observed = []
            result = await asyncio.wait_for(fetch_metadata(client, VIDEO, retries=0,
                total_timeout=0.02, on_attempt=observed.append), 1)
        self.assertTrue(metadata_error_reason(result).startswith('TIMEOUT'))
        self.assertEqual(len(observed), 1)
        self.assertFalse(observed[0].data_received)
        self.assertEqual(observed[0].http_status, 200)

    async def test_recovery_uses_a_proven_proxy(self):
        proxies = [CatalogProxy(n, bytes([n]) * 32, '8.8.8.8', 8080, 'http', 'http', {})
                   for n in (1, 2, 3)]
        pool = ProxyPool(proxies, {1: {'successes': 0}, 2: {'successes': 1}, 3: {'successes': 0}})
        pool.recovery = True
        self.assertEqual(await asyncio.wait_for(pool.acquire(), 1), 1)


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE') == '1', 'Opt in to isolated database tests')
class ImportDatabaseTests(QueueFixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.schema = 'test_bulk_' + uuid.uuid4().hex
        options = dict(dbname='proxy', user='mahmud', host=str(ROOT / '.local/postgres/socket'))
        self.proxy = psycopg.connect(**options)
        self.proxy.execute('CREATE SCHEMA ' + self.schema)
        self.proxy.execute('SET LOCAL search_path TO ' + self.schema + ',public')
        ddl = '\n'.join(line for line in (ROOT / 'db/proxy/schema.sql').read_text().splitlines()
                        if line.strip() not in ('BEGIN;', 'COMMIT;'))
        self.proxy.execute(ddl.replace('public.', self.schema + '.'))
        self.proxy_id = self.proxy.execute('''INSERT INTO proxies(connection_key,address,port,last_seen_at)
            VALUES (%s,'8.8.8.8',8080,%s) RETURNING proxy_id''', (b'x' * 32, AT)).fetchone()[0]
        self.media = psycopg.connect(dbname='media', user='mahmud',
                                    host=str(ROOT / '.local/postgres/socket'), autocommit=True)
        self.media.execute('CREATE SCHEMA ' + self.schema)
        self.media.execute('SET search_path TO ' + self.schema + ',public')
        self.media.execute('''CREATE TABLE videos(video_id TEXT PRIMARY KEY,title TEXT,description TEXT,
            duration_seconds INTEGER,published_at TIMESTAMPTZ,thumbnail_url TEXT,
            metadata_updated_at TIMESTAMPTZ,metadata_error TEXT)''')
        self.media.execute("INSERT INTO videos(video_id,title,description,metadata_error) VALUES (%s,'Old','Keep','Old error')", (VIDEO,))

    def tearDown(self):
        self.proxy.rollback()
        self.proxy.close()
        self.media.execute('DROP SCHEMA ' + self.schema + ' CASCADE')
        self.media.close()
        super().tearDown()

    def make_chunk(self):
        self.initialize()
        claim(self.folder, 0, 1)
        finish(self.folder, 0, [event(proxy_id=self.proxy_id)])
        export_events(self.folder)
        return next((self.folder / 'outbox').glob('*.gz'))

    def test_repeated_import_preserves_old_fields_and_counts_once(self):
        path = self.make_chunk()
        first = apply_chunk(self.media, self.proxy, path, self.run_id)
        second = apply_chunk(self.media, self.proxy, path, self.run_id)
        self.assertEqual(first['metadata_saved'], 1)
        self.assertEqual(second['metadata_saved'], 0)
        self.assertEqual(second['statistics']['replayed_attempts'], 1)
        self.assertEqual(self.media.execute('SELECT title,description,metadata_updated_at,metadata_error FROM videos').fetchone(),
                         ('New title', 'Keep', AT, None))
        self.assertEqual(self.proxy.execute('SELECT connection_attempts,youtube_successful_data_received FROM proxy_stats').fetchone(), (1, 1))

    def test_metadata_failure_rolls_back_the_statistics_update(self):
        path = self.make_chunk()
        self.media.execute('DELETE FROM videos')
        with self.assertRaisesRegex(ValueError, 'missing'):
            apply_chunk(self.media, self.proxy, path, self.run_id)
        self.assertEqual(self.proxy.execute('SELECT count(*) FROM proxy_stats').fetchone()[0], 0)

    def test_proxy_identity_mismatch_prevents_metadata_write(self):
        path = self.make_chunk()
        self.proxy.execute('UPDATE proxies SET connection_key=%s', (b'z' * 32,))
        with self.assertRaisesRegex(ValueError, 'mismatched'):
            apply_chunk(self.media, self.proxy, path, self.run_id)
        self.assertIsNone(self.media.execute('SELECT metadata_updated_at FROM videos').fetchone()[0])


if __name__ == '__main__':
    unittest.main()
