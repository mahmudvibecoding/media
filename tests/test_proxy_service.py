"""Catalog identity, download recovery, ranked responses, and durable test imports."""
import asyncio
from contextlib import redirect_stdout
from dataclasses import replace
from datetime import timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import Mock, patch
import uuid

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

import proxy_service as service
import catalog_sync as sync
from database_helpers import connect_test_database
from proxy_catalog import CatalogProxy
from proxy_formats import Proxy, pack_connection_settings
from proxy_pool import Observation, apply_observations, export_due, export_pool, import_test_journal, ranked_rows
from proxy_service import Settings, clean_completed, collect_quality, read_quality
from proxy_service_common import digest, run_owned, utcnow
from runtime_config import ROOT


class DownloadsTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)

    def test_interrupted_download_resumes_exact_bytes_and_reuses_verified_file(self):
        payload = b'a' * (1024 * 1024) + b'b' * (1024 * 1024)
        expected = {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
        requests = []
        class Interrupted(httpx.SyncByteStream):
            def __iter__(self):
                yield payload[:1024 * 1024]
                raise httpx.ReadError("injected disconnect")
        def handle(request):
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(200, stream=Interrupted())
            self.assertEqual(request.headers['range'], 'bytes=1048576-')
            return httpx.Response(206, headers={'content-range': 'bytes 1048576-2097151/2097152'},
                                  stream=httpx.ByteStream(payload[1048576:]))
        target = self.folder / 'part'
        with httpx.Client(transport=httpx.MockTransport(handle)) as client:
            with self.assertRaises(httpx.ReadError):
                sync.download_asset(client, 'https://example.test/part', target, expected)
            self.assertFalse(target.exists())
            self.assertEqual(target.with_name('part.partial').stat().st_size, 1048576)
            sync.download_asset(client, 'https://example.test/part', target, expected)
            sync.download_asset(client, 'https://example.test/part', target, expected)
        self.assertEqual(target.read_bytes(), payload)
        self.assertEqual(len(requests), 2)

    def test_server_ignoring_range_restarts_and_bad_checksums_are_not_published(self):
        target = self.folder / 'part'
        target.with_name('part.partial').write_bytes(b'old')
        payload = b'complete database bytes'
        with httpx.Client(transport=httpx.MockTransport(lambda request:
                httpx.Response(200, stream=httpx.ByteStream(payload)))) as client:
            sync.download_asset(client, 'https://example.test/part', target,
                                {'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()})
            self.assertEqual(target.read_bytes(), payload)
            with self.assertRaisesRegex(ValueError, 'checksum'):
                sync.download_asset(client, 'https://example.test/bad', self.folder / 'bad',
                                    {'bytes': len(payload), 'sha256': '0' * 64})
        self.assertFalse((self.folder / 'bad').exists())
        self.assertFalse((self.folder / 'bad.partial').exists())

    def test_range_mismatch_and_manifest_traversal_are_rejected(self):
        target = self.folder / 'part'
        target.with_name('part.partial').write_bytes(b'abc')
        with httpx.Client(transport=httpx.MockTransport(lambda request:
                httpx.Response(206, headers={'content-range': 'bytes 0-5/6'}, stream=httpx.ByteStream(b'abcdef')))) as client:
            with self.assertRaisesRegex(ValueError, 'range'):
                sync.download_asset(client, 'https://example.test/part', target,
                                    {'bytes': 6, 'sha256': hashlib.sha256(b'abcdef').hexdigest()})
        manifest = {'format_version': 1, 'repository': sync.DEFAULT_REPOSITORY,
                    'dump_sha256': '0'*64, 'dump_bytes': 1, 'created_at': utcnow().isoformat(),
                    'postgres_version': '18.6', 'tables': {
                        **{name: {'rows': 0, 'content_fingerprint': '0'} for name in sync.TABLES},
                        'identity_sequence': {'last_value': 1, 'is_called': False}},
                    'assets': [{'name': '../escape', 'role': 'database', 'bytes': 1, 'sha256': '0'*64}]}
        with self.assertRaisesRegex(ValueError, 'asset name'):
            sync.validate_manifest(manifest, sync.DEFAULT_REPOSITORY)
        manifest['assets'][0]['name'] = 'catalog.dump.part-0001'
        with self.assertRaisesRegex(ValueError, 'out of order'):
            sync.validate_manifest(manifest, sync.DEFAULT_REPOSITORY)

    def test_owned_process_stops_with_its_checkpoint_preserved(self):
        stop = threading.Event()
        path = self.folder / 'started'
        def stop_started_child():
            deadline = time.monotonic() + 4
            while not path.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            stop.set()
        timer = threading.Thread(target=stop_started_child)
        timer.start()
        self.addCleanup(timer.join)
        started = time.monotonic()
        with self.assertRaises(InterruptedError):
            run_owned([sys.executable, '-c', 'import pathlib,sys,time;pathlib.Path(sys.argv[1]).touch();time.sleep(60)',
                       str(path)], stop=stop, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.assertTrue(path.exists())
        self.assertLess(time.monotonic()-started, 5)

    def test_quality_journal_recovers_only_an_incomplete_final_line(self):
        path = self.folder / 'quality.jsonl'
        complete = b'{"proxy_id":1}\n'
        path.write_bytes(complete + b'{"proxy_id":')
        self.assertEqual(read_quality(path), [{'proxy_id': 1}])
        self.assertEqual(path.read_bytes(), complete)
        path.write_bytes(complete + complete)
        with self.assertRaisesRegex(ValueError, 'Duplicate'):
            read_quality(path)

    def test_retention_keeps_active_and_unimported_work(self):
        old = (utcnow() - timedelta(days=3)).isoformat()
        for name in ('done', 'active', 'unfinished'):
            folder = self.folder / 'runs' / name
            folder.mkdir(parents=True)
            if name != 'unfinished':
                (folder / 'completed.json').write_text(json.dumps({'completed_at': old}))
        (self.folder / 'current-test.json').write_text(json.dumps({'run_id': 'active'}))
        clean_completed(self.folder, 86400)
        self.assertFalse((self.folder / 'runs/done').exists())
        self.assertTrue((self.folder / 'runs/active').exists())
        self.assertTrue((self.folder / 'runs/unfinished').exists())

    def test_lost_database_ownership_stops_all_service_workers(self):
        stop = threading.Event()
        owner = Mock()
        owner.execute.side_effect = psycopg.OperationalError('database restarted')
        with patch.object(service, 'pool_worker'), patch.object(service, 'sync_worker'):
            with self.assertRaises(psycopg.OperationalError):
                service.serve(self.folder,Settings(quality_sample=0),stop,service.Status(self.folder,stop),owner)
        self.assertTrue(stop.is_set())


class QualityTests(unittest.IsolatedAsyncioTestCase):
    async def test_challenge_response_is_recorded_as_response_with_lower_data_quality(self):
        identifier = 'jNQXAC9IVRw'
        proxies = [CatalogProxy(i, bytes([i])*32, 'example.test', 8080, 'http', 'http', {}) for i in (1, 2)]
        clients = []
        class Pool:
            def __init__(self, *args, **kwargs):
                pass
            async def __aenter__(self):
                payloads = [{'playabilityStatus': {'status': 'LOGIN_REQUIRED', 'reason': 'Sign in to confirm you are not a bot'}},
                            {'playabilityStatus': {'status': 'OK'}, 'videoDetails': {'videoId': identifier, 'title': 'Example'}}]
                for payload in payloads:
                    clients.append(httpx.AsyncClient(transport=httpx.MockTransport(
                        lambda request, payload=payload: httpx.Response(200, json=payload))))
                return clients
            async def __aexit__(self, *args):
                await asyncio.gather(*(client.aclose() for client in clients))
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'quality.jsonl'
            await collect_quality(proxies, path, Settings(video_id=identifier), threading.Event(), pool_factory=Pool)
            records = sorted(read_quality(path), key=lambda r: r['proxy_id'])
            self.assertEqual([r['outcome']['http_status'] for r in records], [200, 200])
            self.assertEqual([r['outcome']['data_received'] for r in records], [False, True])
            # Restart skips completed probes, including the challenge response.
            await collect_quality(proxies, path, Settings(video_id=identifier), threading.Event(), pool_factory=Pool)
            self.assertEqual(len(read_quality(path)), 2)


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE'), 'Enable isolated PostgreSQL tests')
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.admin = connect_test_database('proxy', autocommit=True)
        self.options = self.admin.info.get_parameters()
        self.addCleanup(self.admin.close)
        prefix = 'proxy_service_' + uuid.uuid4().hex
        self.names = []
        self.connections = []
        self.addCleanup(self.cleanup_databases)
        for suffix in ('live', 'source', 'stage'):
            name = prefix + '_' + suffix
            self.admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))
            self.names.append(name)
            conn = psycopg.connect(**{**self.options, 'dbname': name}, autocommit=True)
            self.connections.append(conn)
            if suffix != 'stage':
                conn.execute((ROOT / 'db/proxy/schema.sql').read_text())
            conn.execute("SET TIME ZONE 'UTC'")
        self.live, self.source, self.stage = self.connections
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)
        self.at = utcnow() - timedelta(seconds=30)

    def cleanup_databases(self):
        for conn in self.connections:
            conn.close()
        for name in self.names:
            self.admin.execute(sql.SQL('DROP DATABASE {} WITH (FORCE)').format(sql.Identifier(name)))

    def seed(self, conn, count=3):
        for identifier in range(1, count+1):
            proxy = Proxy(f'203.0.113.{identifier}', 8080, 'http', {'password': 'fixture\\nvalue', 'label': 'unicode ✓'})
            conn.execute('''INSERT INTO public.proxies
                (proxy_id,connection_key,address,port,connection_settings,last_seen_at)
                OVERRIDING SYSTEM VALUE VALUES (%s,%s,%s,%s,%s,%s)''',
                (identifier, proxy.key, proxy.address, proxy.port,
                 Jsonb(pack_connection_settings(proxy.protocol, proxy.settings)), self.at))
        conn.execute("SELECT setval(pg_get_serial_sequence('public.proxies','proxy_id'),%s,true)", (count,))

    def observation(self, identifier, *, conn=None, status=200, data=None, seconds=0, latency=20):
        conn = conn or self.live
        key = bytes(conn.execute('SELECT connection_key FROM public.proxies WHERE proxy_id=%s', (identifier,)).fetchone()[0])
        return Observation(identifier, key, self.at+timedelta(seconds=seconds), 'http', 'http' if status is not None else None,
            True, True, True, status, data, None, None if data else 'data:no_usable_data', latency)

    def manifest(self, tag='catalog-test-1', seconds=0):
        manifest = {'format_version': 1, 'repository': sync.DEFAULT_REPOSITORY,
            'created_at': (self.at+timedelta(seconds=seconds)).isoformat(), 'postgres_version': '18.6',
            'dump_sha256': hashlib.sha256(tag.encode()).hexdigest(), 'tables': sync.table_metrics(self.source)}
        pointer = {'tag': tag, 'manifest_sha256': hashlib.sha256((tag+'manifest').encode()).hexdigest()}
        return manifest, pointer

    def test_merge_replay_update_preserves_ids_local_statistics_and_source_baseline(self):
        self.seed(self.source, 2)
        apply_observations(self.source, [self.observation(1, conn=self.source, data=True)], b'a'*32, 'source')
        manifest, pointer = self.manifest()
        first = sync.merge_snapshot(self.live, self.source, manifest, pointer)
        self.assertEqual(first['inserted'], 2)
        self.assertEqual(self.live.execute('SELECT proxy_id,connection_key,connection_settings FROM public.proxies ORDER BY proxy_id').fetchall(),
                         self.source.execute('SELECT proxy_id,connection_key,connection_settings FROM public.proxies ORDER BY proxy_id').fetchall())
        self.assertEqual(self.live.execute('SELECT count(*) FROM app_meta.proxy_test_state WHERE last_response_at IS NOT NULL').fetchone()[0], 0)
        apply_observations(self.live, [self.observation(1, data=True, seconds=3)], b'b'*32, 'local')
        saved = self.live.execute('SELECT * FROM public.proxy_stats').fetchall()
        self.assertTrue(sync.merge_snapshot(self.live, self.source, manifest, pointer)['already_imported'])
        apply_observations(self.source, [self.observation(1, conn=self.source, data=False, seconds=5)], b'c'*32, 'upstream-new')
        self.source.execute("UPDATE public.proxies SET last_seen_at=last_seen_at+interval '1 hour'")
        next_manifest, next_pointer = self.manifest('catalog-test-2', seconds=6)
        self.assertEqual(sync.merge_snapshot(self.live, self.source, next_manifest, next_pointer)['inserted'], 0)
        self.assertEqual(self.live.execute('SELECT * FROM public.proxy_stats').fetchall(), saved)
        self.assertEqual(self.live.execute('SELECT count(*) FROM app_meta.catalog_imports').fetchone()[0], 2)

    def test_published_columns_in_migration_order_are_matched_by_name(self):
        self.seed(self.source, 1)
        apply_observations(self.source, [self.observation(1, conn=self.source, data=True)], b'a'*32, 'source')
        names = [name for name, _ in reversed(sync.columns(self.source, 'proxy_stats'))]
        self.source.execute(sql.SQL('CREATE TABLE reordered AS SELECT {} FROM public.proxy_stats').format(
            sql.SQL(',').join(map(sql.Identifier, names))))
        self.source.execute('DROP VIEW public.proxy_health, public.proxy_catalog; DROP TABLE public.proxy_stats; ALTER TABLE reordered RENAME TO proxy_stats')
        manifest, pointer = self.manifest()
        sync.merge_snapshot(self.live, self.source, manifest, pointer)
        self.assertEqual(self.live.execute('SELECT proxy_id,youtube_successful_data_received,connection_attempts FROM public.proxy_stats').fetchone(), (1,1,1))

    def test_identity_conflict_and_late_import_failure_leave_live_generation_unchanged(self):
        self.seed(self.source, 3)
        self.seed(self.live, 1)
        self.live.execute("UPDATE public.proxies SET connection_key=%s", (b'x'*32,))
        manifest, pointer = self.manifest()
        with self.assertRaisesRegex(ValueError, 'conflict'):
            sync.merge_snapshot(self.live, self.source, manifest, pointer)
        self.assertEqual(self.live.execute('SELECT count(*) FROM public.proxies').fetchone()[0], 1)
        self.assertEqual(self.live.execute('SELECT count(*) FROM app_meta.catalog_imports').fetchone()[0], 0)
        self.live.execute('UPDATE public.proxies SET connection_key=%s',
                          (self.source.execute('SELECT connection_key FROM public.proxies WHERE proxy_id=1').fetchone()[0],))
        original = sync.copy_between
        def failed(source, destination, query, target, names):
            if target == 'incoming_proxy_stats':
                raise OSError('injected failure after inserting new IDs')
            return original(source, destination, query, target, names)
        with patch.object(sync, 'copy_between', side_effect=failed), self.assertRaises(OSError):
            sync.merge_snapshot(self.live, self.source, manifest, pointer)
        self.assertEqual(self.live.execute('SELECT count(*) FROM public.proxies').fetchone()[0], 1)
        self.assertEqual(self.live.execute('SELECT count(*) FROM app_meta.catalog_imports').fetchone()[0], 0)
        self.assertEqual(sync.merge_snapshot(self.live, self.source, manifest, pointer)['inserted'], 2)

    def test_actual_custom_dump_restore_checks_every_row_and_sequence(self):
        self.seed(self.source, 3)
        apply_observations(self.source, [self.observation(1, conn=self.source, status=403)], b'a'*32, 'source')
        binary = Path(os.environ.get('MEDIA_TEST_PG_BIN', '/usr/local/bin'))
        dump_tool = str(binary / 'pg_dump') if (binary / 'pg_dump').exists() else shutil.which('pg_dump')
        restore_tool = str(binary / 'pg_restore') if (binary / 'pg_restore').exists() else shutil.which('pg_restore')
        self.assertTrue(dump_tool and restore_tool, 'PostgreSQL client tools are required for restore verification')
        path = self.folder / 'catalog.dump'
        subprocess.run([dump_tool, '--format=custom', '--compress=zstd:1', '--no-owner', '--no-acl',
                        '--dbname', make_conninfo(**{**self.options, 'dbname': self.names[1]}), '--file', str(path)], check=True)
        manifest, pointer = self.manifest()
        manifest.update(dump_sha256=digest(path), dump_bytes=path.stat().st_size)
        stage = sync.restore_snapshot(self.live, self.folder, manifest,
            options={**self.options, 'dbname': self.names[2]}, executable=restore_tool)
        try:
            self.assertEqual(sync.table_metrics(stage), manifest['tables'])
            sync.merge_snapshot(self.live, stage, manifest, pointer)
        finally:
            stage.close()
        self.assertEqual(sync.table_metrics(self.live), manifest['tables'])
        with self.assertRaisesRegex(ValueError, 'Staging database'):
            sync.restore_snapshot(self.live, self.folder, manifest,
                options={**self.options, 'dbname': self.names[0]}, executable=restore_tool)

    def test_any_http_response_qualifies_and_repeated_data_success_ranks_higher(self):
        self.seed(self.live, 5)
        for round_number in range(3):
            values = [self.observation(1, data=True, seconds=round_number, latency=200),
                      self.observation(2, data=False, seconds=round_number, latency=1),
                      self.observation(3, status=403, data=False, seconds=round_number),
                      self.observation(4, status=429, data=None, seconds=round_number),
                      self.observation(5, status=500, data=None, seconds=round_number)]
            apply_observations(self.live, values, bytes([round_number])*32, f'round-{round_number}', quality=True)
        ranked = list(ranked_rows(self.live))
        self.assertEqual(ranked[0][0], 1)
        self.assertEqual({row[0] for row in ranked}, {1,2,3,4,5})
        self.assertGreater(ranked[0][3], next(row[3] for row in ranked if row[0] == 2))
        report = export_pool(self.live, self.folder)
        self.assertEqual(report['exported'], 5)
        self.assertNotIn('password', (self.folder / 'ranked-proxies.jsonl').read_text())

    def test_late_results_add_counts_once_without_overwriting_newer_outcomes(self):
        self.seed(self.live, 1)
        newer = self.observation(1, data=True, seconds=10)
        older = self.observation(1, status=403, data=False, seconds=2)
        apply_observations(self.live, [newer], b'n'*32, 'newer', quality=True)
        apply_observations(self.live, [older], b'o'*32, 'older', quality=True)
        before = self.live.execute('SELECT * FROM public.proxy_stats').fetchone()
        self.assertTrue(apply_observations(self.live, [older], b'o'*32, 'older', quality=True)['already_imported'])
        self.assertEqual(before, self.live.execute('SELECT * FROM public.proxy_stats').fetchone())
        self.assertEqual(self.live.execute('''SELECT connection_attempts,youtube_responses_received,
            youtube_successful_data_received,youtube_last_http_status,youtube_last_attempt_at FROM public.proxy_stats''').fetchone(),
            (2,2,1,200,newer.checked_at))
        self.assertEqual(self.live.execute('SELECT checked_at,quality_checked_at FROM app_meta.proxy_test_state').fetchone(),
                         (newer.checked_at,newer.checked_at))

    def test_invalid_identity_rolls_back_receipt_and_can_retry_after_correction(self):
        self.seed(self.live, 1)
        item = self.observation(1, data=True)
        with self.assertRaisesRegex(ValueError, 'identity'):
            apply_observations(self.live, [replace(item, connection_key=b'x'*32)], b'a'*32, 'invalid')
        self.assertEqual(self.live.execute('SELECT count(*) FROM app_meta.proxy_observation_imports').fetchone()[0], 0)
        apply_observations(self.live, [item], b'a'*32, 'valid')
        self.assertEqual(self.live.execute('SELECT connection_attempts FROM public.proxy_stats').fetchone()[0], 1)

    def test_test_journal_keeps_error_and_incomplete_body_responses_eligible(self):
        self.seed(self.live, 2)
        path = self.folder / 'results.jsonl'
        lines = []
        for identifier, status in ((1,403),(2,429)):
            key = bytes(self.live.execute('SELECT connection_key FROM public.proxies WHERE proxy_id=%s', (identifier,)).fetchone()[0])
            lines.append({'id': identifier, 'key': key.hex(), 'declared_protocol': 'http', 'detected_protocol': 'http',
                'tested_at': self.at.isoformat(), 'status': 'responds', 'attempted': True, 'responds': True, 'total_ms': 20,
                'attempts': [{'protocol': 'http', 'status': 'responds', 'http_status': status, 'tls_verified': True,
                    'connected': True, 'request_sent': True, 'body_complete': False, 'body_error': 'timeout', 'retry_after': '40000'}]})
        path.write_text(''.join(json.dumps(item)+'\n' for item in lines))
        Path(str(path)+'.meta.json').write_text(json.dumps({'run_id':'fixture'}))
        Path(str(path)+'.summary.json').write_text(json.dumps({'state':'complete', 'counters':
            {'completed':2,'attempted':2,'youtube_responses':2}}))
        with redirect_stdout(io.StringIO()):
            import_test_journal(self.live, path)
            import_test_journal(self.live, path)
        self.assertEqual({r[0] for r in ranked_rows(self.live)}, {1,2})
        self.assertEqual(self.live.execute('SELECT sum(connection_attempts) FROM public.proxy_stats').fetchone()[0], 2)
        self.assertEqual(self.live.execute('SELECT next_test_at FROM app_meta.proxy_test_state WHERE proxy_id=2').fetchone()[0],
                         self.at+timedelta(seconds=40000))

    def test_due_export_is_bounded_and_never_retests_fresh_results(self):
        self.seed(self.live, 3)
        self.live.execute('INSERT INTO app_meta.proxy_test_state(proxy_id) SELECT proxy_id FROM public.proxies')
        apply_observations(self.live, [self.observation(1)], b'a'*32, 'tested')
        manifest = export_due(self.live, self.folder/'run', limit=1)
        self.assertEqual(manifest['records'], 1)
        self.assertEqual(json.loads((self.folder/'run/input.jsonl').read_text())['id'], 2)
        self.assertEqual(digest(self.folder/'run/input.jsonl'), manifest['shards'][0]['sha256'])

    def test_batch_restarts_saved_import_without_retesting_or_double_counting(self):
        self.seed(self.live, 1)
        self.live.execute('INSERT INTO app_meta.proxy_test_state(proxy_id) VALUES (1)')
        def connect(*args, **kwargs):
            return psycopg.connect(**{**self.options, 'dbname': self.names[0]}, autocommit=True)
        def tester(command, **kwargs):
            path = Path(command[command.index('--output')+1])
            manifest = Path(command[command.index('--input')+1])
            run_id = command[command.index('--run-id')+1]
            key = bytes(self.live.execute('SELECT connection_key FROM public.proxies WHERE proxy_id=1').fetchone()[0])
            row = {'id': 1, 'key': key.hex(), 'declared_protocol': 'http', 'detected_protocol': 'http',
                'tested_at': utcnow().isoformat(), 'status': 'responds', 'attempted': True, 'responds': True, 'total_ms': 20,
                'attempts': [{'protocol': 'http', 'status': 'responds', 'http_status': 403, 'tls_verified': True,
                    'connected': True, 'request_sent': True, 'body_complete': True}]}
            path.write_text(json.dumps(row)+'\n')
            Path(str(path)+'.meta.json').write_text(json.dumps({'run_id':run_id, 'input_sha256':digest(manifest)}))
            Path(str(path)+'.summary.json').write_text(json.dumps({'state':'complete', 'counters':
                {'completed':1,'attempted':1,'youtube_responses':1}}))
            return 0
        stop = threading.Event()
        status = service.Status(self.folder, stop)
        settings = Settings(batch_size=1, test_concurrency=1)
        with patch.object(service, 'connect_database', side_effect=connect), patch.object(service, 'run_owned', side_effect=tester) as runner:
            with patch.object(service, 'import_test_journal', side_effect=RuntimeError('injected database outage')):
                with self.assertRaises(RuntimeError):
                    service.test_batch(self.folder, settings, stop, status)
            saved = json.loads((self.folder / 'current-test.json').read_text())
            self.assertEqual(saved['phase'], 'importing')
            service.test_batch(self.folder, settings, stop, status)
            self.assertEqual(runner.call_count, 1)
            # A crash after transaction commit but before checkpoint deletion is safe too.
            (self.folder / 'current-test.json').write_text(json.dumps(saved))
            self.assertTrue(service.test_batch(self.folder, settings, stop, status)['already_imported'])
            self.assertEqual(runner.call_count, 1)
        self.assertEqual(self.live.execute('SELECT connection_attempts FROM public.proxy_stats').fetchone()[0], 1)

    def test_real_go_tester_consumes_exported_manifest_and_imports_once(self):
        if not service.BRIDGE_BINARY.is_file():
            self.skipTest('Build the Go tester before running this integration check')
        proxy = Proxy('127.0.0.1', 1, 'http', {})
        self.live.execute('INSERT INTO public.proxies(connection_key,address,port,connection_settings,last_seen_at) VALUES (%s,%s,%s,%s,clock_timestamp())',
            (proxy.key,proxy.address,proxy.port,Jsonb(pack_connection_settings(proxy.protocol,proxy.settings))))
        self.live.execute('INSERT INTO app_meta.proxy_test_state(proxy_id) SELECT proxy_id FROM public.proxies')
        def connect(*args, **kwargs):
            return psycopg.connect(**{**self.options, 'dbname': self.names[0]}, autocommit=True)
        stop = threading.Event()
        with patch.object(service, 'connect_database', side_effect=connect):
            report = service.test_batch(self.folder, Settings(batch_size=1,test_concurrency=1,connect_timeout=1,request_timeout=1),
                                        stop,service.Status(self.folder,stop))
        self.assertEqual(report['observations'], 1)
        self.assertFalse((self.folder/'current-test.json').exists())
        self.assertEqual(self.live.execute('SELECT connection_attempts,youtube_responses_received FROM public.proxy_stats').fetchone(), (1,0))

    def test_first_data_score_after_reachability_only_has_zero_prior_weight(self):
        self.seed(self.live, 1)
        apply_observations(self.live, [self.observation(1, data=None)], b'u'*32, 'unscored')
        apply_observations(self.live, [self.observation(1, data=True, seconds=1)], b's'*32, 'scored', quality=True)
        self.assertEqual(self.live.execute('SELECT connection_attempts,youtube_successful_data_received,youtube_weighted_attempts,youtube_weighted_successful_data_received FROM public.proxy_stats').fetchone(), (2,1,1.0,1.0))
        self.assertEqual(self.live.execute('SELECT quality_checked_at FROM app_meta.proxy_test_state').fetchone()[0], self.at+timedelta(seconds=1))
