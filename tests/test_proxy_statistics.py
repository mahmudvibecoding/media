"""Proxy scoring, retry accounting and collection independence from statistics."""
import asyncio
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
import errno
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

import httpcore
import httpx
import psycopg
from psycopg.types.json import Jsonb

from collect_video_metadata import collect, fetch_metadata, fetch_metadata_with_proxies, RequestTrace, save_metadata
from proxy_formats import Proxy
from proxy_statistics import Aggregate, AttemptOutcome, ProxyStatistics, ProxyTarget, StatisticsBatch, decayed_weight, write_batch


ROOT = Path(__file__).resolve().parents[1]
T = datetime(2026, 1, 1, tzinfo=timezone.utc)
URL = 'http://8.8.8.8:8080'
TARGET = ProxyTarget.from_url(URL)
VIDEO = 'RtXBV0X1v1Q'
DATA = {'videoDetails': {'videoId': VIDEO, 'title': 'Available metadata'},
        'playabilityStatus': {'status': 'OK'}}


def outcome(at=T, success=True, sent=True, status=200):
    return AttemptOutcome(at, sent, status, success, 20.0)


def batch(*observations, target=TARGET):
    aggregate = Aggregate()
    for item in observations:
        aggregate.add(item, target.protocol)
    return StatisticsBatch({target: aggregate})


class WeightTests(unittest.TestCase):
    def test_connection_success_survives_a_later_failure_and_error_clears_on_success(self):
        connected = AttemptOutcome(T,False,None,False,20,True,'proxy_handshake:proxy_http_407')
        failed = AttemptOutcome(T+timedelta(seconds=1),False,None,False,20,False,'connect:connection_refused')
        aggregate = batch(failed,connected).aggregates[TARGET]
        self.assertEqual(aggregate.successful_connections,1)
        self.assertEqual(aggregate.last_connected_at,T)
        self.assertEqual(aggregate.last_connection_error,'connect:connection_refused')
        aggregate.add(outcome(T+timedelta(seconds=2)),TARGET.protocol)
        self.assertEqual(aggregate.successful_connections,2)
        self.assertIsNone(aggregate.last_connection_error)
        self.assertEqual(aggregate.last_connected_at,T+timedelta(seconds=2))

    def test_unknown_connection_is_not_inferred_as_a_failure_or_success(self):
        aggregate = batch(outcome(sent=False,status=None,success=False)).aggregates[TARGET]
        self.assertEqual(aggregate.successful_connections,0)
        self.assertIsNone(aggregate.last_connection_error)
        self.assertIsNone(aggregate.last_connected_at)
        with self.assertRaises(ValueError):
            batch(AttemptOutcome(T,True,200,True,20,False))
        with self.assertRaises(ValueError):
            batch(AttemptOutcome(T,False,None,False,20,False,'secret raw exception'))

    def test_recent_deterioration_uses_fifteen_attempt_weight_and_six_success_weight(self):
        aggregate = Aggregate()
        for success in [True]*8 + [False]*2:
            aggregate.add(outcome(success=success), 'http')
        for success in [True]*2 + [False]*8:
            aggregate.add(outcome(T+timedelta(hours=1), success), 'http')
        self.assertEqual(aggregate.connection_attempts, 20)
        self.assertEqual(aggregate.youtube_successful_data_received, 10)
        self.assertAlmostEqual(aggregate.weighted_connection_attempts, 15)
        self.assertAlmostEqual(aggregate.weighted_youtube_successful_data_received, 6)
        self.assertAlmostEqual(100*(6+1)/(15+2), 41.17647058823529)

    def test_out_of_order_completions_and_merged_batches_have_the_same_weights(self):
        observations = [outcome(T, True), outcome(T+timedelta(hours=2), False),
                        outcome(T+timedelta(hours=1), True)]
        together = batch(*observations).aggregates[TARGET]
        merged = Aggregate()
        for item in reversed(observations):
            merged.merge(batch(item).aggregates[TARGET])
        self.assertEqual(together, merged)
        self.assertAlmostEqual(together.weighted_connection_attempts, 1.75)
        self.assertAlmostEqual(together.weighted_youtube_successful_data_received, .75)

    def test_unknown_data_quality_changes_reachability_only(self):
        result = batch(outcome(success=None)).aggregates[TARGET]
        self.assertEqual((result.connection_attempts, result.youtube_responses_received), (1,1))
        self.assertEqual(result.youtube_successful_data_received, 0)
        self.assertIsNone(result.last_scored_attempt_at)
        self.assertEqual(result.weighted_connection_attempts, 0)

    def test_very_old_or_zero_evidence_is_safe_and_clock_reversal_does_not_inflate_it(self):
        self.assertEqual(decayed_weight(1,T,T+timedelta(days=1000)), 0)
        self.assertEqual(decayed_weight(0,T,T), 0)
        self.assertEqual(decayed_weight(1,T,T-timedelta(hours=1)), 1)


class CollectorStatisticsTests(unittest.IsolatedAsyncioTestCase):
    async def test_every_failed_retry_and_data_success_is_reported(self):
        replies = iter([httpx.ConnectError('failed'), httpx.Response(403),
                        httpx.Response(200,json={'playabilityStatus': {'status':'LOGIN_REQUIRED'}}),
                        httpx.Response(200,json=DATA)])
        def handle(request):
            reply = next(replies)
            if isinstance(reply, Exception):
                raise reply
            return reply
        observed = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            result = await fetch_metadata(client, VIDEO, retries=3, on_attempt=observed.append)
        self.assertEqual(result['attempts'], 4)
        self.assertEqual([o.data_received for o in observed], [False,False,False,True])
        self.assertEqual([o.request_sent for o in observed], [False,True,True,True])
        aggregate = batch(*observed).aggregates[TARGET]
        self.assertEqual((aggregate.connection_attempts,aggregate.youtube_requests_sent,
                          aggregate.youtube_responses_received,aggregate.youtube_successful_data_received), (4,3,3,1))

    async def test_sent_request_followed_by_timeout_is_still_counted_as_sent(self):
        async def handle(request):
            trace = request.extensions['trace']
            sent = httpcore.Request('POST','https://www.youtube.com/youtubei/v1/player')
            await trace('http2.send_request_body.started', {'request':sent})
            await trace('http2.send_request_body.complete', {'return_value':None})
            raise httpx.ReadTimeout('no response')
        observed = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            await fetch_metadata(client, VIDEO, retries=0, on_attempt=observed.append)
        self.assertEqual(len(observed),1)
        self.assertTrue(observed[0].request_sent)
        self.assertIsNone(observed[0].http_status)
        self.assertFalse(observed[0].data_received)

    async def test_proxy_connect_and_failed_body_write_are_not_sent_target_requests(self):
        trace = RequestTrace()
        connect = httpcore.Request('CONNECT','http://8.8.8.8:8080')
        await trace('http11.send_request_body.started', {'request':connect})
        await trace('http11.send_request_body.complete', {'return_value':None})
        self.assertFalse(trace.request_sent)
        target = httpcore.Request('POST','https://www.youtube.com/youtubei/v1/player')
        await trace('http11.send_request_body.started', {'request':target})
        await trace('http11.send_request_body.failed', {'exception':OSError('failed')})
        self.assertFalse(trace.request_sent)
        await trace('http11.send_request_body.started', {'request':target})
        await trace('http11.send_request_body.complete', {'return_value':None})
        self.assertTrue(trace.request_sent)

    async def test_proxy_handshake_error_has_no_youtube_response(self):
        def handle(request):
            raise httpx.ProxyError('407 Proxy Authentication Required')
        observed = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            await fetch_metadata(client, VIDEO, retries=0, on_attempt=observed.append)
        self.assertFalse(observed[0].request_sent)
        self.assertIsNone(observed[0].http_status)
        self.assertFalse(observed[0].data_received)

    async def test_statistics_callback_failure_does_not_change_collection_or_retries(self):
        replies = iter([httpx.Response(403), httpx.Response(200,json=DATA)])
        observer = Mock(side_effect=RuntimeError('statistics unavailable'))
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: next(replies))) as client:
            result = await fetch_metadata(client, VIDEO, retries=1, on_attempt=observer)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(observer.call_count,2)

    async def test_local_resource_errors_and_cancellations_do_not_reduce_proxy_score(self):
        exhausted = httpx.ConnectError('resource exhaustion')
        exhausted.__cause__ = OSError(errno.EMFILE, 'open files')
        for error in (exhausted, httpx.PoolTimeout('client pool'), asyncio.CancelledError()):
            with self.subTest(error=type(error).__name__):
                observed = []
                def handle(request):
                    raise error
                async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
                    if isinstance(error, asyncio.CancelledError):
                        with self.assertRaises(asyncio.CancelledError):
                            await fetch_metadata(client, VIDEO, retries=0, on_attempt=observed.append)
                    else:
                        await fetch_metadata(client, VIDEO, retries=0, on_attempt=observed.append)
                self.assertEqual(observed,[])

    async def test_data_success_is_reported_before_a_failed_metadata_save(self):
        observed = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200,json=DATA))) as client:
            result = await fetch_metadata(client, VIDEO, retries=0, on_attempt=observed.append)
        conn = Mock()
        conn.execute.side_effect = psycopg.OperationalError('metadata storage unavailable')
        with self.assertRaises(psycopg.OperationalError):
            save_metadata(conn,result)
        self.assertTrue(observed[0].data_received)

    async def test_each_proxy_receives_only_its_assigned_attempt_outcome(self):
        first, second = [], []
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(403))) as a, \
                httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200,json=DATA))) as b:
            failed = await fetch_metadata_with_proxies([a,b],VIDEO,on_attempts=[first.append,second.append])
            self.assertEqual((len(first),len(second)),(1,0))
            result = await fetch_metadata_with_proxies([a,b],VIDEO,on_attempts=[first.append,second.append],proxy_index=1)
        self.assertEqual((failed['attempts'],result['attempts']),(1,1))
        self.assertEqual([o.data_received for o in first+second],[False,True])

    async def test_collection_finishes_while_statistics_writer_is_blocked(self):
        entered, release = threading.Event(), threading.Event()
        def blocked_writer(value):
            entered.set()
            release.wait(5)
            return {'accepted_attempts':sum(a.connection_attempts for a in value.aggregates.values())}
        statistics = ProxyStatistics([TARGET],writer=blocked_writer,interval=.001).start()
        statistics.record(TARGET,outcome())
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait,1))
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200,json=DATA))) as client:
                result = await asyncio.wait_for(fetch_metadata(client,VIDEO,on_attempt=statistics.observer(0)),1)
            self.assertEqual(result['status'],'ok')
            statistics.close()
            self.assertTrue(statistics._thread.is_alive())
        finally:
            release.set()
            statistics.close()
            await asyncio.to_thread(statistics._thread.join,2)

    async def test_statistics_setup_and_shutdown_failures_do_not_fail_a_run(self):
        for stage in ('setup','shutdown'):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as directory:
                connection = Mock()
                connection.execute.return_value.fetchone.return_value = (True,)
                args = SimpleNamespace(video_id=[VIDEO],limit=1,concurrency=1,retries=0,
                                       client_version='test',output=Path(directory)/'run')
                client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200,json=DATA)))
                statistics = Mock()
                statistics.start.return_value = statistics
                statistics.observer.return_value = Mock(side_effect=RuntimeError('report unavailable'))
                statistics.close.side_effect = RuntimeError('shutdown unavailable')
                with patch.dict(os.environ,{'MEDIA_PROXY_URL':URL},clear=True), \
                        patch('collect_video_metadata.open_database',return_value=nullcontext(connection)), \
                        patch('collect_video_metadata.select_videos',return_value=[(VIDEO,'video')]), \
                        patch('collect_video_metadata.save_metadata',return_value=1), \
                        patch('collect_video_metadata.httpx.AsyncClient',return_value=client), \
                        patch('collect_video_metadata.ProxyStatistics.from_urls',
                              side_effect=RuntimeError('setup unavailable') if stage=='setup' else None,
                              return_value=statistics), patch('builtins.print'):
                    self.assertEqual(await collect(args),0)
                report=json.loads((args.output/'summary.json').read_text())
                self.assertEqual(report['saved'],1)
                self.assertIsNone(report['stopped_reason'])


class BackgroundTests(unittest.TestCase):
    def test_memory_lock_contention_drops_statistics_without_waiting(self):
        statistics = ProxyStatistics([TARGET])
        with statistics._lock:
            self.assertFalse(statistics.record(TARGET,outcome()))
        self.assertEqual(statistics.dropped_attempts,1)

    def test_results_are_aggregated_by_proxy_without_a_request_queue(self):
        statistics = ProxyStatistics([TARGET])
        for _ in range(10000):
            self.assertTrue(statistics.record(TARGET,outcome()))
        self.assertEqual(len(statistics._pending),1)
        self.assertEqual(statistics._pending[TARGET].connection_attempts,10000)

    def test_failed_writes_retry_the_same_batch_while_new_observations_accumulate(self):
        failed, recover, all_written = threading.Event(), threading.Event(), threading.Event()
        seen, accepted = [], []
        def writer(value):
            seen.append(value.key)
            if not recover.is_set():
                failed.set()
                raise psycopg.OperationalError('statistics database offline')
            amount=sum(a.connection_attempts for a in value.aggregates.values())
            accepted.append(amount)
            if sum(accepted)==4:
                all_written.set()
            return {'accepted_attempts':amount}
        statistics = ProxyStatistics([TARGET],writer=writer,interval=.005)
        statistics.record(TARGET,outcome())
        statistics.start()
        try:
            self.assertTrue(failed.wait(1))
            for second in range(1,4):
                self.assertTrue(statistics.record(TARGET,outcome(T+timedelta(seconds=second))))
            recover.set()
            self.assertTrue(all_written.wait(2))
            self.assertEqual(len(set(seen)),2)
            self.assertEqual(seen[0],seen[-2])
            self.assertEqual(accepted,[1,3])
        finally:
            recover.set()
            statistics.close()
            statistics._thread.join(2)


@unittest.skipUnless(shutil.which('openssl'), 'Local TLS fixture requires openssl')
class WireTracingTests(unittest.IsolatedAsyncioTestCase):
    """Real HTTPX/HTTPCore traffic through a loopback CONNECT proxy and TLS origin."""
    async def asyncSetUp(self):
        self.directory=tempfile.TemporaryDirectory()
        certificate=Path(self.directory.name)/'certificate.pem'
        key=Path(self.directory.name)/'key.pem'
        await asyncio.to_thread(subprocess.run, [shutil.which('openssl'),'req','-x509','-newkey','ec',
            '-pkeyopt','ec_paramgen_curve:P-256','-nodes','-keyout',str(key),'-out',str(certificate),
            '-days','1','-subj','/CN=www.youtube.com','-addext','subjectAltName=DNS:www.youtube.com'],
            check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certificate,key)
        self.trusted=ssl.create_default_context(cafile=str(certificate))
        self.handlers=set()
        self.posts=self.connects=0
        self.reject_connect=False
        self.reject_response=None
        self.origin=await asyncio.start_server(self.serve_origin,'127.0.0.1',0,ssl=context)
        self.origin_port=self.origin.sockets[0].getsockname()[1]
        self.proxy=await asyncio.start_server(self.serve_proxy,'127.0.0.1',0)
        self.proxy_url=f'http://127.0.0.1:{self.proxy.sockets[0].getsockname()[1]}'

    async def asyncTearDown(self):
        self.proxy.close()
        self.origin.close()
        await self.proxy.wait_closed()
        await self.origin.wait_closed()
        tasks=list(self.handlers)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
        self.directory.cleanup()

    async def serve_origin(self,reader,writer):
        task=asyncio.current_task()
        self.handlers.add(task)
        try:
            while True:
                headers=await reader.readuntil(b'\r\n\r\n')
                self.assertTrue(headers.startswith(b'POST /youtubei/v1/player?'))
                size=next(int(line.split(b':',1)[1]) for line in headers.split(b'\r\n')
                          if line.lower().startswith(b'content-length:'))
                await reader.readexactly(size)
                self.posts+=1
                code=503 if self.posts==1 else 200
                body=b'' if code==503 else json.dumps(DATA).encode()
                writer.write(f'HTTP/1.1 {code} Response\r\nContent-Length: {len(body)}\r\nContent-Type: application/json\r\n\r\n'.encode()+body)
                await writer.drain()
        except (asyncio.IncompleteReadError,ConnectionError):
            pass
        finally:
            writer.close()
            self.handlers.discard(task)

    async def serve_proxy(self,reader,writer):
        task=asyncio.current_task()
        self.handlers.add(task)
        upstream=None
        pumps=[]
        try:
            headers=await reader.readuntil(b'\r\n\r\n')
            self.assertTrue(headers.startswith(b'CONNECT www.youtube.com:443 '))
            self.connects+=1
            if self.reject_connect or self.reject_response:
                writer.write(self.reject_response or b'HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\n\r\n')
                await writer.drain()
                return
            response,upstream=await asyncio.open_connection('127.0.0.1',self.origin_port)
            writer.write(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            await writer.drain()
            async def forward(source,destination):
                try:
                    while data:=await source.read(65536):
                        destination.write(data)
                        await destination.drain()
                finally:
                    destination.close()
            pumps=[asyncio.create_task(forward(reader,upstream)),asyncio.create_task(forward(response,writer))]
            await asyncio.gather(*pumps)
        except (asyncio.IncompleteReadError,ConnectionError):
            pass
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps,return_exceptions=True)
            writer.close()
            if upstream is not None:
                upstream.close()
            self.handlers.discard(task)

    async def test_real_connect_tls_and_reused_connection_count_only_target_requests(self):
        observed=[]
        async with httpx.AsyncClient(proxy=self.proxy_url,verify=self.trusted,trust_env=False) as client:
            result=await fetch_metadata(client,VIDEO,retries=1,on_attempt=observed.append)
        self.assertEqual(result['status'],'ok')
        self.assertEqual((self.connects,self.posts),(1,2))
        self.assertEqual([item.request_sent for item in observed],[True,True])
        self.assertEqual([item.http_status for item in observed],[503,200])
        self.assertEqual([item.data_received for item in observed],[False,True])
        self.assertEqual([item.connected for item in observed],[True,True])
        self.assertEqual([item.connection_error for item in observed],[None,None])
        self.assertEqual(batch(*observed).aggregates[TARGET].successful_connections,2)

    async def test_real_rejected_connect_is_not_a_youtube_request_or_response(self):
        self.reject_connect=True
        observed=[]
        async with httpx.AsyncClient(proxy=self.proxy_url,verify=self.trusted,trust_env=False) as client:
            await fetch_metadata(client,VIDEO,retries=0,on_attempt=observed.append)
        self.assertEqual((self.connects,self.posts),(1,0))
        self.assertEqual(len(observed),1)
        self.assertFalse(observed[0].request_sent)
        self.assertIsNone(observed[0].http_status)
        self.assertTrue(observed[0].connected)
        self.assertEqual(observed[0].connection_error,'proxy_handshake:proxy_http_407')

    async def test_refused_proxy_is_not_a_successful_connection(self):
        self.proxy.close()
        await self.proxy.wait_closed()
        observed=[]
        async with httpx.AsyncClient(proxy=self.proxy_url,trust_env=False) as client:
            await fetch_metadata(client,VIDEO,retries=0,on_attempt=observed.append)
        self.assertFalse(observed[0].connected)
        self.assertEqual(observed[0].connection_error,'connect:connection_refused')

    async def test_failed_target_tls_still_records_the_proxy_connection(self):
        observed=[]
        async with httpx.AsyncClient(proxy=self.proxy_url,trust_env=False) as client:
            await fetch_metadata(client,VIDEO,retries=0,on_attempt=observed.append)
        self.assertTrue(observed[0].connected)
        self.assertFalse(observed[0].request_sent)
        self.assertEqual(observed[0].connection_error,'youtube_https:youtube_certificate_verification_failed')

    async def test_bridge_loopback_does_not_count_as_the_upstream_connection(self):
        for connected,label in ((False,'connect:connection_refused'),(True,'proxy_handshake:proxy_http_407')):
            with self.subTest(connected=connected):
                self.reject_response=(f'HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n'
                    f'X-Proxy-Connected: {str(connected).lower()}\r\nX-Proxy-Error: {label}\r\n\r\n').encode()
                observed=[]
                async with httpx.AsyncClient(proxy=self.proxy_url,trust_env=False) as client:
                    client.catalog_bridge=SimpleNamespace(local_error=lambda _:False)
                    await fetch_metadata(client,VIDEO,retries=0,on_attempt=observed.append)
                self.assertEqual(observed[0].connected,connected)
                self.assertEqual(observed[0].connection_error,label)
                self.assertFalse(observed[0].request_sent)

    async def test_unavailable_local_bridge_does_not_create_a_proxy_observation(self):
        self.proxy.close()
        await self.proxy.wait_closed()
        observed=[]
        async with httpx.AsyncClient(proxy=self.proxy_url,trust_env=False) as client:
            client.catalog_bridge=SimpleNamespace(local_error=lambda _:False)
            await fetch_metadata(client,VIDEO,retries=0,on_attempt=observed.append)
        self.assertEqual(observed,[])


def ddl(conn,schema,filename):
    text=(ROOT/'db/proxy'/filename).read_text()
    text='\n'.join(line for line in text.splitlines() if line.strip() not in ('BEGIN;','COMMIT;'))
    conn.execute(text.replace('public.',schema+'.'))


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE')=='1','Opt in to rollback-only database tests')
class StatisticsDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn=psycopg.connect(dbname='proxy',user='mahmud',host=str(ROOT/'.local/postgres/socket'),connect_timeout=5)
        self.schema='test_statistics_'+uuid.uuid4().hex
        self.conn.execute('CREATE SCHEMA '+self.schema)
        self.conn.execute('SET LOCAL search_path TO '+self.schema+',public')

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def fresh(self):
        ddl(self.conn,self.schema,'schema.sql')

    def proxy(self,protocol='http',settings=None,address='8.8.8.8'):
        p=Proxy(address,8080,protocol,settings or {})
        return self.conn.execute('''INSERT INTO proxies
            (connection_key,address,port,protocol,connection_settings,last_seen_at)
            VALUES (%s,%s,%s,%s,%s,%s) RETURNING proxy_id''',
            (p.key,p.address,p.port,p.protocol,Jsonb(p.settings),T)).fetchone()[0]

    def test_migration_preserves_all_old_values_and_leaves_history_unscored(self):
        for name in ('001_proxy_lists.sql','002_proxy_entries.sql','003_proxy_tests.sql',
                     '004_compact_proxy_database.sql','005_reduce_proxy_columns.sql'):
            ddl(self.conn,self.schema,'migrations/'+name)
        proxy_id=self.proxy()
        self.conn.execute('''INSERT INTO proxy_stats VALUES
            (%s,%s,'responds','http',200,20,%s,10,8,7,%s)''',(proxy_id,T,T,b'x'*32))
        before=self.conn.execute('SELECT * FROM proxy_stats').fetchone()
        ddl(self.conn,self.schema,'migrations/006_proxy_scoring.sql')
        after=self.conn.execute('SELECT * FROM proxy_stats').fetchone()
        self.assertEqual(after[:11],before)
        self.assertEqual(after[11:],(0,0,0,None))
        self.assertIsNone(self.conn.execute('SELECT score FROM proxy_health').fetchone()[0])
        counts=dict(self.conn.execute('''SELECT table_name,count(*) FROM information_schema.columns
            WHERE table_schema=%s AND table_name IN ('proxies','proxy_stats','proxy_lists') GROUP BY table_name''',(self.schema,)))
        self.assertEqual(counts,{'proxies':7,'proxy_stats':15,'proxy_lists':8})
        ddl(self.conn,self.schema,'migrations/007_proxy_connections.sql')
        latest=self.conn.execute('SELECT * FROM proxy_stats').fetchone()
        self.assertEqual(latest[:15],after)
        self.assertEqual(latest[15:],(0,None,None))
        self.assertEqual(self.conn.execute('SELECT successful_connections,last_connected_at,last_connection_error FROM proxy_health').fetchone(),(0,None,None))

    def test_connection_counts_and_latest_error_persist_without_replaying(self):
        self.fresh()
        self.proxy()
        value=batch(AttemptOutcome(T,False,None,False,20,True,'proxy_handshake:proxy_http_407'),
                    AttemptOutcome(T+timedelta(seconds=1),False,None,False,20,False,'connect:timeout'))
        write_batch(self.conn,value)
        write_batch(self.conn,value)
        self.assertEqual(self.conn.execute('SELECT successful_connections,last_connected_at,last_connection_error FROM proxy_stats').fetchone(),(1,T,'connect:timeout'))
        write_batch(self.conn,batch(outcome(T+timedelta(seconds=2),False,status=403)))
        self.assertEqual(self.conn.execute('SELECT successful_connections,last_connected_at,last_connection_error FROM proxy_stats').fetchone(),(2,T+timedelta(seconds=2),None))

    def test_batch_replay_stale_overlap_and_data_counters(self):
        self.fresh()
        self.proxy()
        initial=batch(outcome(success=False,sent=False,status=None),outcome(T+timedelta(seconds=1),False,status=403),
                      outcome(T+timedelta(seconds=2),False),outcome(T+timedelta(seconds=3)))
        self.assertEqual(write_batch(self.conn,initial)['updated_proxies'],1)
        counters=self.conn.execute('''SELECT connection_attempts,youtube_requests_sent,youtube_responses_received,
            youtube_successful_data_received FROM proxy_stats''').fetchone()
        self.assertEqual(counters,(4,3,3,1))
        before=self.conn.execute('SELECT * FROM proxy_stats').fetchone()
        self.assertEqual(write_batch(self.conn,initial)['replayed_attempts'],4)
        self.assertEqual(self.conn.execute('SELECT * FROM proxy_stats').fetchone(),before)
        newer=batch(outcome(T+timedelta(hours=1)))
        write_batch(self.conn,newer)
        self.assertEqual(write_batch(self.conn,initial)['stale_attempts'],4)
        self.assertEqual(self.conn.execute('SELECT connection_attempts,youtube_successful_data_received FROM proxy_stats').fetchone(),(5,2))

    def test_unknown_legacy_data_and_later_unmeasured_response_keep_score_evidence(self):
        self.fresh()
        self.proxy()
        write_batch(self.conn,batch(outcome(success=None)))
        self.assertEqual(self.conn.execute('SELECT score,youtube_successful_data_received FROM proxy_health').fetchone(),(None,0))
        write_batch(self.conn,batch(outcome(T+timedelta(hours=1))))
        before=self.conn.execute('SELECT last_scored_attempt_at,weighted_connection_attempts FROM proxy_stats').fetchone()
        write_batch(self.conn,batch(outcome(T+timedelta(hours=2),None)))
        self.assertEqual(self.conn.execute('SELECT last_scored_attempt_at,weighted_connection_attempts FROM proxy_stats').fetchone(),before)
        self.assertEqual(self.conn.execute('SELECT youtube_successful_data_received FROM proxy_stats').fetchone()[0],1)

    def test_python_and_sql_decay_agree_and_view_ages_evidence_without_writes(self):
        self.fresh()
        self.proxy()
        for weight,hours in ((0,0),(1,1),(10,2),(1e-200,1000),(1,100000),(3,-1)):
            at=T+timedelta(hours=hours)
            actual=self.conn.execute('SELECT proxy_decayed_weight(%s,%s,%s)',(weight,T,at)).fetchone()[0]
            self.assertAlmostEqual(actual,decayed_weight(weight,T,at))
        now=datetime.now(timezone.utc)
        write_batch(self.conn,batch(outcome(now-timedelta(hours=1))))
        stored=self.conn.execute('SELECT weighted_connection_attempts FROM proxy_stats').fetchone()[0]
        attempts,successes,score,age=self.conn.execute('''SELECT weighted_connection_attempts,
            weighted_youtube_successful_data_received,score,score_age_seconds FROM proxy_health''').fetchone()
        self.assertEqual(stored,1)
        self.assertAlmostEqual(attempts,.5,places=3)
        self.assertAlmostEqual(successes,.5,places=3)
        self.assertAlmostEqual(score,60,places=2)
        self.assertGreaterEqual(age,3600)
        self.assertEqual(self.conn.execute('SELECT weighted_connection_attempts FROM proxy_stats').fetchone()[0],1)

    def test_credentials_and_ambiguous_aliases_cannot_mix_configurations(self):
        self.fresh()
        unknown=self.proxy('unknown')
        self.proxy('https')
        self.assertEqual(write_batch(self.conn,batch(outcome()))['unmatched_attempts'],1)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM proxy_stats').fetchone()[0],0)
        selected=ProxyTarget.from_url(URL,unknown)
        write_batch(self.conn,batch(outcome(),target=selected))
        wrong_credentials=ProxyTarget.from_url('http://user:password@8.8.8.8:8080',unknown)
        self.assertEqual(write_batch(self.conn,batch(outcome(),target=wrong_credentials))['unmatched_attempts'],1)
        self.assertEqual(self.conn.execute('SELECT proxy_id,connection_attempts FROM proxy_stats').fetchone(),(unknown,1))

    def test_exact_identity_wins_and_a_unique_unknown_configuration_is_supported(self):
        self.fresh()
        unknown=self.proxy('unknown')
        write_batch(self.conn,batch(outcome()))
        self.assertEqual(self.conn.execute('SELECT proxy_id FROM proxy_stats').fetchone()[0],unknown)
        exact=self.proxy('http')
        write_batch(self.conn,batch(outcome(T+timedelta(seconds=1))))
        self.assertEqual(self.conn.execute('SELECT connection_attempts FROM proxy_stats WHERE proxy_id=%s',(exact,)).fetchone()[0],1)

    def test_catalog_bridge_keeps_advanced_configuration_identity_and_protocol(self):
        from proxy_catalog import load_catalog
        self.fresh()
        original = self.proxy('vless', {'uuid':'622693d9-5812-4542-8344-a32bbb5bfbcd'})
        other = self.proxy('vless', {'uuid':'877adba0-13e5-497e-9f88-1c3f4714b748'})
        selected = load_catalog([original, other], connection=self.conn)
        target = next(proxy.statistics_target for proxy in selected if proxy.proxy_id == original)
        write_batch(self.conn, batch(outcome(), target=target))
        self.assertEqual(self.conn.execute('''SELECT proxy_id,working_protocol,connection_attempts,
            youtube_requests_sent,youtube_successful_data_received FROM proxy_stats''').fetchall(),
            [(original,'vless',1,1,1)])
        self.assertEqual([proxy.proxy_id for proxy in load_catalog(connection=self.conn)], [original])
        with self.assertRaises(ValueError):
            load_catalog([other], ['http'], connection=self.conn)
        with self.assertRaises(ValueError):
            load_catalog([99999], connection=self.conn)

    def test_invalid_batch_is_atomic_and_lost_acknowledgement_does_not_double_count(self):
        self.fresh()
        self.proxy()
        value=batch(outcome())
        value.aggregates[TARGET].youtube_successful_data_received=2
        with self.assertRaises(psycopg.errors.CheckViolation):
            write_batch(self.conn,value)
        self.assertEqual(self.conn.execute('SELECT count(*) FROM proxy_stats').fetchone()[0],0)
        value=batch(outcome())
        # Simulate a successful commit followed by a lost acknowledgement.
        write_batch(self.conn,value)
        retry=write_batch(self.conn,value)
        self.assertEqual(retry['replayed_attempts'],1)
        self.assertEqual(self.conn.execute('SELECT connection_attempts,youtube_successful_data_received FROM proxy_stats').fetchone(),(1,1))


if __name__=='__main__':
    unittest.main()
