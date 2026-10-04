"""Catalog integrity and fresh in-memory proxy scoring."""
from datetime import timedelta
import hashlib
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
from unittest.mock import AsyncMock, patch
import uuid

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb

import proxy_service as service
import catalog_sync as sync
import collect_proxies as sources
from database_helpers import connect_test_database
from proxy_formats import Proxy, pack_connection_settings
from proxy_service import Settings
from proxy_service_common import digest, run_owned, utcnow
from runtime_config import BRIDGE_BINARY, ROOT


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


    def seed_statistics(self, conn):
        conn.execute("""INSERT INTO public.proxy_stats
            (proxy_id,connection_attempts,successful_connections,last_connected_at,last_connection_attempt_at,
             working_protocol,youtube_requests_sent,youtube_responses_received,youtube_last_response_at,
             youtube_last_attempt_at,youtube_last_http_status,youtube_last_import_key,
             youtube_successful_data_received,youtube_weighted_attempts,
             youtube_weighted_successful_data_received,youtube_last_scored_attempt_at)
            VALUES (1,1,1,%s,%s,'http',1,1,%s,%s,200,%s,1,1,1,%s)""",
            (self.at,self.at,self.at,self.at,b'a'*32,self.at))

    def manifest(self, tag='catalog-test-1', seconds=0):
        manifest = {'format_version': 1, 'repository': sync.DEFAULT_REPOSITORY,
            'created_at': (self.at+timedelta(seconds=seconds)).isoformat(), 'postgres_version': '18.6',
            'dump_sha256': hashlib.sha256(tag.encode()).hexdigest(), 'tables': sync.table_metrics(self.source)}
        pointer = {'tag': tag, 'manifest_sha256': hashlib.sha256((tag+'manifest').encode()).hexdigest()}
        return manifest, pointer

    def test_merge_replay_update_preserves_ids_local_statistics_and_source_baseline(self):
        self.seed(self.source, 2)
        self.seed_statistics(self.source)
        manifest, pointer = self.manifest()
        first = sync.merge_snapshot(self.live, self.source, manifest, pointer)
        self.assertEqual(first['inserted'], 2)
        self.assertEqual(self.live.execute('SELECT proxy_id,connection_key,connection_settings FROM public.proxies ORDER BY proxy_id').fetchall(),
                         self.source.execute('SELECT proxy_id,connection_key,connection_settings FROM public.proxies ORDER BY proxy_id').fetchall())
        self.live.execute('UPDATE public.proxy_stats SET connection_attempts=5')
        saved = self.live.execute('SELECT * FROM public.proxy_stats').fetchall()
        self.assertTrue(sync.merge_snapshot(self.live, self.source, manifest, pointer)['already_imported'])
        self.source.execute('UPDATE public.proxy_stats SET connection_attempts=8')
        self.source.execute("UPDATE public.proxies SET last_seen_at=last_seen_at+interval '1 hour'")
        next_manifest, next_pointer = self.manifest('catalog-test-2', seconds=6)
        self.assertEqual(sync.merge_snapshot(self.live, self.source, next_manifest, next_pointer)['inserted'], 0)
        self.assertEqual(self.live.execute('SELECT * FROM public.proxy_stats').fetchall(), saved)
        self.assertEqual(self.live.execute('SELECT count(*) FROM app_meta.catalog_imports').fetchone()[0], 2)

    def test_published_columns_in_migration_order_are_matched_by_name(self):
        self.seed(self.source, 1)
        self.seed_statistics(self.source)
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
        self.live.execute("UPDATE public.proxies SET address='different.example'")
        manifest, pointer = self.manifest()
        with self.assertRaisesRegex(ValueError, 'conflict'):
            sync.merge_snapshot(self.live, self.source, manifest, pointer)
        self.assertEqual(self.live.execute('SELECT count(*) FROM public.proxies').fetchone()[0], 1)
        self.assertEqual(self.live.execute('SELECT count(*) FROM app_meta.catalog_imports').fetchone()[0], 0)
        self.live.execute('UPDATE public.proxies SET address=%s',
                          (self.source.execute('SELECT address FROM public.proxies WHERE proxy_id=1').fetchone()[0],))
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

    def test_local_discovery_and_github_ids_can_diverge_without_changing_saved_ids(self):
        self.seed(self.source,1)
        manifest,pointer=self.manifest()
        sync.merge_snapshot(self.live,self.source,manifest,pointer)
        local=Proxy('1.1.1.1',8080,'http',{})
        local_id=self.live.execute('''INSERT INTO public.proxies(connection_key,address,port,connection_settings,last_seen_at)
            VALUES (%s,%s,%s,%s,%s) RETURNING proxy_id''',
            (local.key,local.address,local.port,Jsonb(pack_connection_settings(local.protocol,local.settings)),self.at)).fetchone()[0]
        self.assertEqual(local_id,2)
        # GitHub independently assigns 2 to a different configuration and 3 to
        # the configuration this database already discovered as local ID 2.
        self.source.execute('''INSERT INTO public.proxies(connection_key,address,port,connection_settings,last_seen_at)
            SELECT %s,%s,8080,%s,%s''',(b'n'*32,'8.8.8.8',Jsonb({'transport':'http','options':{}}),self.at))
        self.source.execute('''INSERT INTO public.proxies(connection_key,address,port,connection_settings,last_seen_at)
            VALUES (%s,%s,%s,%s,%s)''',
            (local.key,local.address,local.port,Jsonb(pack_connection_settings(local.protocol,local.settings)),self.at))
        self.seed_statistics(self.source)
        self.source.execute('UPDATE public.proxy_stats SET proxy_id=2')
        self.seed_statistics(self.live)
        self.live.execute('UPDATE public.proxy_stats SET proxy_id=2')
        old_stats=self.live.execute('SELECT * FROM public.proxy_stats WHERE proxy_id=2').fetchone()
        later,pointer=self.manifest('catalog-diverged',seconds=5)
        result=sync.merge_snapshot(self.live,self.source,later,pointer)
        self.assertEqual(result['inserted'],1)
        self.assertEqual(self.live.execute('SELECT proxy_id FROM public.proxies WHERE connection_key=%s',(local.key,)).fetchone()[0],2)
        new_id=self.live.execute('SELECT proxy_id FROM public.proxies WHERE connection_key=%s',(b'n'*32,)).fetchone()[0]
        self.assertNotEqual(new_id,2)
        self.assertEqual(self.live.execute('SELECT * FROM public.proxy_stats WHERE proxy_id=2').fetchone(),old_stats)
        self.assertEqual(self.live.execute('SELECT connection_attempts FROM public.proxy_stats WHERE proxy_id=%s',(new_id,)).fetchone()[0],1)
        self.assertTrue(sync.merge_snapshot(self.live,self.source,later,pointer)['already_imported'])

    def test_catalog_merge_preserves_local_source_progress(self):
        self.seed(self.source,1)
        url='https://feeds.example.test/proxies.txt'
        self.source.execute("INSERT INTO public.proxy_lists(url,kind,enabled) VALUES (%s,'feed_candidate',true)",(url,))
        manifest,pointer=self.manifest()
        sync.merge_snapshot(self.live,self.source,manifest,pointer)
        run=uuid.uuid4()
        self.live.execute("UPDATE public.proxy_lists SET run_id=%s,status='pending',fetch_state=%s",(run,Jsonb({'local':'state'})))
        self.source.execute("UPDATE public.proxy_lists SET run_id=%s,status='collected',fetch_state=%s",(uuid.uuid4(),Jsonb({'foreign':'path'})))
        new_url='https://feeds.example.test/new.txt'
        self.source.execute("""INSERT INTO public.proxy_lists(url,kind,enabled,run_id,status,fetch_state)
            VALUES (%s,'feed_candidate',true,%s,'pending',%s)""",(new_url,uuid.uuid4(),Jsonb({'foreign':'unfinished'})))
        manifest,pointer=self.manifest('catalog-next',seconds=5)
        sync.merge_snapshot(self.live,self.source,manifest,pointer)
        self.assertEqual(self.live.execute('SELECT run_id,status,fetch_state FROM public.proxy_lists WHERE url=%s',(url,)).fetchone(),(run,'pending',{'local':'state'}))
        self.assertEqual(self.live.execute('SELECT run_id,status,fetch_state FROM public.proxy_lists WHERE url=%s',(new_url,)).fetchone(),(None,'not_checked',{}))

    def source_connection(self):
        from psycopg.rows import dict_row
        return psycopg.connect(**{**self.options,'dbname':self.names[0]},autocommit=True,row_factory=dict_row)

    def test_full_refresh_imports_downloads_deduplicates_and_tests_despite_source_failure(self):
        self.seed(self.source,1)
        self.source.execute("""INSERT INTO public.proxy_lists(url,kind,protocol_hints,enabled) VALUES
            ('https://feeds.example.test/good','feed_candidate',ARRAY['http'],true),
            ('https://feeds.example.test/broken','feed_candidate',ARRAY['http'],true),
            ('https://feeds.example.test/disabled','feed_candidate',ARRAY['http'],false)""")
        manifest,pointer=self.manifest()
        order=[]
        def import_catalog(*args,**kwargs):
            order.append('import')
            return sync.merge_snapshot(self.live,self.source,manifest,pointer)
        requested=[]
        def response(request):
            requested.append(request.url.path)
            order.append('download')
            return httpx.Response(404) if request.url.path=='/broken' else httpx.Response(200,content=b'8.8.8.8:3128\n8.8.8.8:3128\n')
        real_client=httpx.AsyncClient
        def client(**options):
            return real_client(transport=httpx.MockTransport(response),**options)
        def test_catalog(*args):
            order.append('test')
            self.assertEqual(self.live.execute('SELECT count(*) FROM public.proxies').fetchone()[0],2)
            return {'configurations':2,'new_checks':6}
        stop=threading.Event()
        with patch.object(service,'synchronize',side_effect=import_catalog), \
             patch.object(sources,'connection',side_effect=self.source_connection), \
             patch.object(sources,'STORAGE',self.folder/'sources'), \
             patch.object(sources.Downloader,'public_url',new=AsyncMock(return_value=True)), \
             patch.object(httpx,'AsyncClient',side_effect=client), \
             patch.object(service,'test_catalog',side_effect=test_catalog):
            result=service.refresh(Settings(),self.folder,stop,service.Status(self.folder,stop),self.live)
            self.assertEqual(result['sources']['new_configurations'],1)
            self.assertEqual(result['sources']['source_errors'],1)
            self.assertEqual((order[0],order[-1]),('import','test'))
            self.assertEqual(set(requested),{'/good','/broken'})
            again=service.refresh(Settings(),self.folder,stop,service.Status(self.folder,stop),self.live)
        self.assertEqual(again['sources']['new_configurations'],0)
        self.assertEqual(json.loads((self.folder/'last-refresh.json').read_text())['new_checks'],6)
        failed=Path(again['sources']['failed_sources_path']).read_text().splitlines()
        self.assertEqual(json.loads(failed[0])['status'],'http_404')

    def test_source_refresh_stops_and_resumes_with_newly_added_urls(self):
        import asyncio
        self.live.execute("""INSERT INTO public.proxy_lists(url,kind,protocol_hints,enabled) VALUES
            ('https://feeds.example.test/first','feed_candidate',ARRAY['http'],true),
            ('https://feeds.example.test/second','feed_candidate',ARRAY['http'],true)""")
        stop=threading.Event()
        async def interrupted(request):
            if request.url.path=='/second':
                while self.live.execute("SELECT status FROM public.proxy_lists WHERE url LIKE '%/first'").fetchone()[0]!='collected':
                    await asyncio.sleep(.01)
                stop.set()
                await asyncio.sleep(60)
            return httpx.Response(200,content=b'8.8.8.8:3128\n')
        real_client=httpx.AsyncClient
        options=sources.argument_parser().parse_args(['--concurrency','1'])
        options.auto_resume,options.inventory=True,False
        with patch.object(sources,'connection',side_effect=self.source_connection), \
             patch.object(sources,'STORAGE',self.folder/'sources'), \
             patch.object(sources.Downloader,'public_url',new=AsyncMock(return_value=True)):
            with patch.object(httpx,'AsyncClient',side_effect=lambda **kw:real_client(transport=httpx.MockTransport(interrupted),**kw)):
                with self.assertRaises(InterruptedError):
                    asyncio.run(sources.main(options,exclusive_owner=self.live,stop=stop))
            self.assertEqual(self.live.execute("SELECT count(*) FROM public.proxy_lists WHERE status='pending'").fetchone()[0],1)
            self.assertEqual(self.live.execute('SELECT count(*) FROM public.proxies').fetchone()[0],1)
            self.live.execute("""INSERT INTO public.proxy_lists(url,kind,protocol_hints,enabled)
                VALUES ('https://feeds.example.test/new','feed_candidate',ARRAY['http'],true)""")
            requested=[]
            def resumed(request):
                requested.append(request.url.path)
                return httpx.Response(200,content=b'1.1.1.1:3128\n')
            stop.clear()
            with patch.object(httpx,'AsyncClient',side_effect=lambda **kw:real_client(transport=httpx.MockTransport(resumed),**kw)):
                result=asyncio.run(sources.main(options,exclusive_owner=self.live,stop=stop))
        self.assertEqual(set(requested),{'/second','/new'})
        self.assertEqual((result['selected_urls'],result['new_configurations'],result['pending_sources']),(3,2,0))

    def test_actual_custom_dump_restore_checks_every_row_and_sequence(self):
        self.seed(self.source, 3)
        self.seed_statistics(self.source)
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

    def test_fresh_three_checks_use_no_database_results_or_checkpoints(self):
        if not BRIDGE_BINARY.is_file():
            self.skipTest('Build the Go tester before running this integration check')
        import proxy_file_test
        for port in (1,2,3):
            proxy = Proxy('127.0.0.1', port, 'http', {})
            self.live.execute("""INSERT INTO public.proxies(connection_key,address,port,connection_settings,last_seen_at)
                VALUES (%s,%s,%s,%s,clock_timestamp())""",
                (proxy.key,proxy.address,proxy.port,Jsonb(pack_connection_settings(proxy.protocol,proxy.settings))))
        self.seed_statistics(self.live)
        before = self.live.execute('SELECT * FROM public.proxy_stats').fetchall()
        (self.folder/'ranked-proxies.jsonl').write_text('old ranking')
        def connect(*args, **kwargs):
            return psycopg.connect(**{**self.options, 'dbname':self.names[0]}, autocommit=True)
        stop = threading.Event()
        settings = Settings(test_concurrency=4,test_workers=2,connect_timeout=1,request_timeout=1)
        with patch.object(proxy_file_test, 'connect_database', side_effect=connect):
            result = proxy_file_test.test_catalog(self.folder,settings,stop,service.Status(self.folder,stop),self.live)
            next_result = proxy_file_test.test_catalog(self.folder,settings,stop,service.Status(self.folder,stop),self.live)
        self.assertEqual((result['configurations'],result['observations'],result['new_checks']), (3,9,9))
        self.assertEqual(next_result['new_checks'],9)
        self.assertNotEqual(result['run_id'],next_result['run_id'])
        self.assertFalse(result['checkpoints'])
        self.assertEqual(result['scores'],{'3':0,'2':0,'1':0,'0':3})
        self.assertEqual((self.folder/'ranked-proxies.jsonl').read_text(),'')
        self.assertEqual(self.live.execute('SELECT * FROM public.proxy_stats').fetchall(), before)
        self.assertFalse((self.folder/'current-refresh.json').exists())
        self.assertFalse(list(self.folder.glob('proxy-results-*')))

    def test_failed_test_does_not_replace_the_previous_ranking(self):
        import proxy_file_test
        self.seed(self.live,1)
        path = self.folder/'ranked-proxies.jsonl'
        path.write_text('previous complete ranking')
        stop = threading.Event()
        with patch.object(proxy_file_test, 'BRIDGE_BINARY', Path('/missing/proxy-tester')):
            with self.assertRaises(FileNotFoundError):
                proxy_file_test.test_catalog(self.folder,Settings(test_concurrency=2),stop,
                                            service.Status(self.folder,stop),self.live)
        self.assertEqual(path.read_text(),'previous complete ranking')
        self.assertFalse(list(self.folder.glob('proxy-results-*')))

    def test_migration_removes_only_the_old_testing_tables(self):
        self.seed(self.live,1)
        self.seed_statistics(self.live)
        before = sync.table_metrics(self.live)
        self.live.execute((ROOT/'db/proxy/migrations/011_proxy_service.sql').read_text())
        self.live.execute((ROOT/'db/proxy/migrations/012_proxy_response_scores.sql').read_text())
        self.live.execute((ROOT/'db/proxy/migrations/013_drop_proxy_test_state.sql').read_text())
        self.assertEqual(sync.table_metrics(self.live),before)
        for name in ('proxy_test_state','proxy_round_results','proxy_pool_results','proxy_observation_imports'):
            self.assertIsNone(self.live.execute('SELECT to_regclass(%s)',('app_meta.'+name,)).fetchone()[0])
        self.assertIsNotNone(self.live.execute("SELECT to_regclass('app_meta.catalog_imports')").fetchone()[0])
