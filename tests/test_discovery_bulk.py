import asyncio
from collections import Counter
import json
import queue
import sys
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import httpx
import psycopg
from psycopg.types.json import Jsonb

from database_helpers import connect_test_database
from discover_all import COUNTERS, collect, fetch_using_pool, parser, run_worker
from discover_videos import scan_tab
from discovery_pool import ConcurrencyTuner, DiscoveryPool, Performance
from discovery_storage import BatchWriter, load_ranked_proxies


def ranking(count):
    return [dict(proxy_id=n+1, score=3, average_response_ms=1000+n) for n in range(count)]


def listing(kind='video', ids=(), token=None):
    items = []
    for video in ids:
        content = {'lockupViewModel': {'contentId': video}} if kind == 'video' else {
            'shortsLockupViewModel': {'onTap': {'innertubeCommand': {'reelWatchEndpoint': {'videoId': video}}}}}
        items.append({'richItemRenderer': {'content': content}})
    if token:
        items.append({'continuationItemRenderer': {'continuationEndpoint': {
            'continuationCommand': {'token': token}}}})
    return {'contents': {'twoColumnBrowseResultsRenderer': {'tabs': [{'tabRenderer': {
        'title': 'Videos' if kind == 'video' else 'Shorts', 'selected': True,
        'content': {'richGridRenderer': {'contents': items, 'header': {'chipBarViewModel': {
            'chips': [{'chipViewModel': {'text': 'Latest', 'selected': True}}]}}}}}}]}}}


def fixture_process(number,count,jobs,known,proxies,ranks,next_job,target,messages,stop,options):
    completed = 0
    while True:
        with next_job.get_lock():
            index = next_job.value
            next_job.value += int(index < len(jobs))
        if index >= len(jobs):
            break
        channel,kind = jobs[index]
        messages.put(dict(event='result',worker=number,result=dict(channel_id=channel,type=kind,
            status='ok',scan_complete=True,videos=[{'video_id':uuid.uuid4().hex[:11]}],pages_fetched=1,
            videos_inserted=0,videos_already_present=0)))
        completed += 1
    messages.put(dict(event='progress',worker=number,counts=dict(http_attempts=completed,usable_pages=completed)))
    messages.put(dict(event='worker_done',worker=number))


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_initial_order_exclusive_use_and_other_proxy_retry(self):
        pool = DiscoveryPool(ranking(3))
        first, second = await pool.acquire(), await pool.acquire()
        self.assertEqual((first, second), (0, 1))
        self.assertEqual(len(pool.active), 2)
        pool.release(first, True, 0.1)
        third = await pool.acquire(previous=first)
        self.assertEqual(third, 2)
        pool.release(second, False, 10)
        pool.release(third, True, 1)
        self.assertEqual(await pool.acquire(), first)

    async def test_other_proxies_are_sampled_despite_one_productive_proxy(self):
        pool = DiscoveryPool(ranking(3))
        selected = []
        for _ in range(20):
            index = await pool.acquire()
            selected.append(index)
            pool.release(index, True, 0.1)
        self.assertEqual(selected[:19], [0]*19)
        self.assertEqual(selected[19], 1)

    def test_failed_attempt_time_reduces_productivity_and_neutral_errors_do_not(self):
        quick = Performance()
        slow = Performance()
        for usable in (True, True, False, True):
            quick.observe(usable, 1)
            slow.observe(usable, 1 if usable else 20)
        self.assertEqual(quick.quality, slow.quality)
        self.assertGreater(quick.score, slow.score)
        original = vars(quick).copy()
        quick.observe(None, 60)
        self.assertEqual(vars(quick), original)

    def test_tuner_requires_sustained_regression_and_ignores_tail(self):
        tuner = ConcurrencyTuner(256, 4096, interval=10)
        self.assertIsNone(tuner.observe(1, 0, 100000))
        self.assertEqual(tuner.observe(11, 1000, 100000)['concurrency'], 512)
        self.assertEqual(tuner.observe(21, 2200, 100000)['concurrency'], 1024)
        self.assertIsNone(tuner.observe(31, 2300, 100000))
        self.assertEqual(tuner.observe(41, 2400, 100000)['concurrency'], 512)
        self.assertIsNone(tuner.observe(100, 2410, 1))


class FakeClients:
    def __init__(self, handlers):
        self.proxies = [SimpleNamespace(proxy_id=n+1) for n in range(len(handlers))]
        self.clients = [httpx.AsyncClient(transport=httpx.MockTransport(handler)) for handler in handlers]

    def __getitem__(self, index):
        return self.clients[index]

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await asyncio.gather(*(client.aclose() for client in self.clients))


class FetchAndScanTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_proxy_retries_elsewhere_and_empty_listing_is_useful(self):
        options = parser().parse_args([])
        stats, pool = Counter(), DiscoveryPool(ranking(2))
        seen = []
        def blocked(request):
            seen.append(1)
            return httpx.Response(429)
        def working(request):
            seen.append(2)
            return httpx.Response(200, json=listing())
        async with FakeClients([blocked, working]) as clients:
            result = await fetch_using_pool(clients, pool, stats, 'channel', 'video', None,
                                           options, threading.Event())
        self.assertEqual(seen, [1, 2])
        self.assertEqual((result['status'], result['attempts']), ('empty', 2))
        self.assertEqual((stats['usable_pages'], stats['failed_pages']), (1, 1))
        self.assertGreater(pool.performance[1].quality, pool.performance[0].quality)

    async def test_scan_reads_existing_ids_from_memory_and_buffers_complete_tab(self):
        async def fetch(channel, kind, continuation):
            return dict(status='ok', videos=[{'video_id':'newvideo001','channel_id':channel,'type':kind},
                                             {'video_id':'knownvideo1','channel_id':channel,'type':kind}],
                        continuation='older', complete_tab=False, latest_verified=True, attempts=1,
                        request_body_bytes=10,response_body_bytes=20,decoded_body_bytes=20,http_status=200)
        result = await scan_tab(None, None, 'channel', 'video', known_ids={'knownvideo1'},
                                fetch=fetch, persist=False)
        self.assertTrue(result['scan_complete'])
        self.assertEqual(result['stop_reason'], 'known_range')
        self.assertEqual(result['videos_inserted'], 0)
        self.assertEqual(result['pages_fetched'], 1)

    async def test_failed_scan_remains_uncommittable(self):
        async def fetch(channel, kind, continuation):
            return dict(status='blocked', videos=[], continuation=None, complete_tab=False,
                        latest_verified=False,attempts=3,request_body_bytes=0,response_body_bytes=0,
                        decoded_body_bytes=0,http_status=403,error='HTTP 403')
        result = await scan_tab(None, None, 'channel', 'short', known_ids=set(), fetch=fetch, persist=False)
        self.assertFalse(result['scan_complete'])
        self.assertEqual(result['videos_inserted'], 0)

    async def test_worker_recovers_failed_tab_and_reports_each_tab_once(self):
        options = parser().parse_args(['--attempts','1'])
        next_job = SimpleNamespace(value=0, get_lock=lambda: threading.Lock())
        messages = queue.Queue()
        calls = Counter()
        def handle(request):
            body = json.loads(request.content)
            kind = 'short' if body['params'].startswith('EgZzaG9y') else 'video'
            calls[kind] += 1
            if kind == 'video' and calls[kind] == 1:
                return httpx.Response(403)
            return httpx.Response(200, json=listing(kind))
        clients = FakeClients([handle, handle])
        with patch('discover_all.CatalogClients', return_value=clients):
            await run_worker(0,1,[('channel','video'),('channel','short')],{}, clients.proxies,ranking(2),
                             next_job,SimpleNamespace(value=2),messages,threading.Event(),options)
        events = []
        while not messages.empty():
            events.append(messages.get_nowait())
        results = [e['result'] for e in events if e['event']=='result']
        self.assertEqual(len(results),2)
        self.assertTrue(all(r['scan_complete'] for r in results))
        self.assertEqual(sum(r['recovery'] for r in results),1)
        self.assertEqual(calls['video'],2)


class WriterDatabaseTests(unittest.TestCase):
    def setUp(self):
        try:
            self.conn = connect_test_database('media', autocommit=True)
        except psycopg.OperationalError as exc:
            self.skipTest(str(exc))
        self.channel = 'UC'+uuid.uuid4().hex[:22]
        self.conn.execute('INSERT INTO public.channels(channel_id) VALUES (%s)', (self.channel,))
        self.writer = BatchWriter(self.conn)

    def tearDown(self):
        if hasattr(self, 'conn'):
            self.conn.execute('DELETE FROM public.videos WHERE channel_id=%s', (self.channel,))
            self.conn.execute('DELETE FROM public.channels WHERE channel_id=%s', (self.channel,))
            self.conn.close()

    def scan(self, ids, **values):
        return dict(channel_id=self.channel,type='video',scan_complete=True,
                    videos=[{'video_id':v} for v in ids],**values)

    def test_duplicates_are_skipped_and_existing_metadata_survives(self):
        first, second = uuid.uuid4().hex[:11], uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos(video_id,channel_id,type,title) VALUES (%s,%s,'video','saved title')",
                          (first,self.channel))
        scan = self.scan([first,second,second])
        inserted = self.writer.write([scan])
        self.assertEqual(inserted,[(second,self.channel,'video')])
        self.assertEqual(scan['videos_inserted'],1)
        self.assertEqual(self.conn.execute('SELECT title FROM public.videos WHERE video_id=%s',(first,)).fetchone()[0],
                         'saved title')
        self.assertEqual(self.writer.write([self.scan([first,second])]),[])

    def test_whole_batch_rolls_back_and_can_be_retried(self):
        first, second = uuid.uuid4().hex[:11], uuid.uuid4().hex[:11]
        good,bad = self.scan([first]),self.scan([second])
        bad['channel_id'] = 'UC'+uuid.uuid4().hex[:22]
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            self.writer.write([good,bad])
        self.assertEqual(self.conn.execute('SELECT count(*) FROM public.videos WHERE video_id=%s',(first,)).fetchone()[0],0)
        self.assertEqual(len(self.writer.write([good])),1)

    def test_incomplete_scan_is_rejected_before_any_write(self):
        scan = self.scan([uuid.uuid4().hex[:11]])
        scan['scan_complete'] = False
        with self.assertRaises(ValueError):
            self.writer.write([scan])
        self.assertEqual(self.conn.execute('SELECT count(*) FROM public.videos WHERE channel_id=%s',(self.channel,)).fetchone()[0],0)

    @unittest.skipUnless(sys.platform.startswith('linux'), 'Exercise the Linux forked collection controller')
    def test_process_controller_drains_results_and_publishes_only_committed_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)/'run'
            options = parser().parse_args(['--output',str(folder),'--workers','2','--concurrency','2'])
            with patch('discover_all.load_inventory',return_value=([self.channel],{})), \
                 patch('discover_all.load_ranked_proxies',return_value=([1,2],ranking(2))), \
                 patch('discover_all.worker_entry',fixture_process), \
                 patch('discover_all.STATE_DIR',Path(temporary)/'state'), \
                 patch('discover_all.connect_database',side_effect=lambda _,**kw:connect_test_database('media',**kw)):
                self.assertEqual(collect(options),0)
            summary = json.loads((folder/'summary.json').read_text())
            self.assertEqual((summary['tabs_completed'],summary['channels_completed'],summary['videos_inserted']),(2,1,2))
            exported = {json.loads(s)['video_id'] for s in (folder/'new-video-ids.jsonl').read_text().splitlines()}
            saved = {r[0] for r in self.conn.execute('SELECT video_id FROM public.videos WHERE channel_id=%s',(self.channel,))}
            self.assertEqual(exported,saved)
            self.assertEqual((folder/'unresolved.jsonl').read_text(),'')


class RankedCatalogDatabaseTests(unittest.TestCase):
    def test_fresh_file_works_without_historical_proxy_statistics(self):
        try:
            conn = connect_test_database('proxy', autocommit=True)
        except psycopg.OperationalError as exc:
            self.skipTest(str(exc))
        key = uuid.uuid4().bytes + uuid.uuid4().bytes
        with conn:
            proxy_id = conn.execute('''INSERT INTO public.proxies(connection_key,address,port,connection_settings,last_seen_at)
                VALUES (%s,'127.0.0.1',12345,%s,now()) RETURNING proxy_id''',
                (key,Jsonb({'transport':'unknown','options':{}}))).fetchone()[0]
            try:
                with tempfile.TemporaryDirectory() as temporary:
                    path = Path(temporary)/'ranked.jsonl'
                    row = dict(proxy_id=proxy_id,connection_key=key.hex(),protocol='http',checks=3,score=3,average_response_ms=10)
                    path.write_text(json.dumps(row)+'\n')
                    with patch('discovery_storage.connect_database', side_effect=lambda _:connect_test_database('proxy')):
                        proxies,_ = load_ranked_proxies(path)
                        self.assertEqual((proxies[0].proxy_id,proxies[0].working_protocol),(proxy_id,'http'))
                        row['connection_key'] = '00'*32
                        path.write_text(json.dumps(row)+'\n')
                        with self.assertRaises(ValueError):
                            load_ranked_proxies(path)
            finally:
                conn.execute('DELETE FROM public.proxies WHERE proxy_id=%s',(proxy_id,))
