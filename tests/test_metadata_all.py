import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import multiprocessing
from pathlib import Path
import queue
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

import httpx
import psycopg

from database_helpers import connect_test_database
from collect_metadata_all import COUNTERS, collect, fetch_using_pool, parser, run_worker
from discovery_pool import DiscoveryPool
from metadata_storage import FIELDS, MetadataWriter, read_input, resume_source, save_input, select_input


VIDEO = 'RtXBV0X1v1Q'


def payload(video=VIDEO):
    return dict(videoDetails=dict(videoId=video, title='A title', shortDescription='', lengthSeconds='0',
        thumbnail={'thumbnails':[{'url':'https://i.ytimg.com/example.jpg'}]}),
        microformat={'playerMicroformatRenderer':{'publishDate':'2026-10-04T00:00:00Z'}},
        playabilityStatus={'status':'OK'})


def ranks(count):
    return [dict(proxy_id=i+1, score=3, average_response_ms=100+i) for i in range(count)]


def final_result(video, kind='video', successful=True):
    return dict(video_id=video, type=kind, status='ok' if successful else 'error',
        metadata=dict(title='A title', description='', duration_seconds=0,
            published_at='2026-10-04T00:00:00+00:00',thumbnail_url='https://i.ytimg.com/example.jpg') if successful else None,
        observed_at=datetime.now(timezone.utc).isoformat(), final=True, attempts=1,
        error=None if successful else 'ConnectError', availability_confirmed=False,
        outcome='data' if successful else 'connection_error', publication_precision='timestamp')


class Clients:
    def __init__(self, handlers):
        self.proxies = [SimpleNamespace(proxy_id=i+1) for i in range(len(handlers))]
        self.clients = [httpx.AsyncClient(transport=httpx.MockTransport(handler)) for handler in handlers]

    def __getitem__(self,index):
        return self.clients[index]

    async def __aenter__(self):
        return self

    async def __aexit__(self,*args):
        await asyncio.gather(*(client.aclose() for client in self.clients))


def fixture_worker(number,count,jobs,proxies,ranking,next_job,target,messages,stop,options,slot):
    completed = 0
    while not stop.is_set():
        with next_job.get_lock():
            index = next_job.value
            next_job.value += int(index < len(jobs))
        if index >= len(jobs):
            break
        video,kind = jobs[index]
        messages.put(dict(event='result',result=final_result(video,kind)))
        completed += 1
    with slot.get_lock():
        slot.get_obj()[:] = [completed if key in ('http_attempts','data_responses') else 0 for key in COUNTERS]+[0]
    messages.put(dict(event='worker_done',worker=number))


class FetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_and_wrong_id_responses_retry_elsewhere(self):
        handlers = [lambda _:httpx.Response(403),lambda _:httpx.Response(200,json=payload('differentid')),
                    lambda _:httpx.Response(200,json=payload())]
        pool,stats = DiscoveryPool(ranks(3)),Counter()
        async with Clients(handlers) as clients:
            result = await fetch_using_pool(clients,pool,stats,(VIDEO,'short'),parser().parse_args([]),threading.Event())
        self.assertTrue(result['final'])
        self.assertEqual(result['attempts'],3)
        self.assertEqual(result['proxy_id'],3)
        self.assertEqual(result['metadata']['description'],'')
        self.assertEqual((stats['request_errors'],stats['data_responses']),(2,1))

    async def test_unavailable_requires_two_proxies_and_is_neutral_for_scores(self):
        def unavailable(_):
            return httpx.Response(200,json={'playabilityStatus':{'status':'ERROR','reason':'Video unavailable'}})
        pool,stats = DiscoveryPool(ranks(2)),Counter()
        async with Clients([unavailable,unavailable]) as clients:
            result = await fetch_using_pool(clients,pool,stats,(VIDEO,'video'),parser().parse_args([]),threading.Event())
        self.assertTrue(result['availability_confirmed'])
        self.assertEqual(result['attempts'],2)
        self.assertEqual(stats['video_error_responses'],2)
        self.assertEqual([p.samples for p in pool.performance],[0,0])

    async def test_one_unavailable_response_can_recover_on_another_proxy(self):
        def unavailable(_):
            return httpx.Response(200,json={'playabilityStatus':{'status':'ERROR','reason':'Video unavailable'}})
        pool,stats = DiscoveryPool(ranks(2)),Counter()
        async with Clients([unavailable,lambda _:httpx.Response(200,json=payload())]) as clients:
            result = await fetch_using_pool(clients,pool,stats,(VIDEO,'video'),parser().parse_args([]),threading.Event())
        self.assertTrue(result['final'])
        self.assertFalse(result['availability_confirmed'])
        self.assertEqual(result['outcome'],'data')

    async def test_local_failure_releases_proxy_without_penalty(self):
        async def local_error(*args,**kwargs):
            return dict(status='error',metadata=None,error='LocalBridgeFailure')
        pool,stats = DiscoveryPool(ranks(1)),Counter()
        async with Clients([lambda _:httpx.Response(200)]) as clients:
            with patch('collect_metadata_all.fetch_metadata',side_effect=local_error):
                with self.assertRaisesRegex(RuntimeError,'Local metadata worker'):
                    await fetch_using_pool(clients,pool,stats,(VIDEO,'video'),parser().parse_args([]),threading.Event())
        self.assertEqual(pool.active,set())
        self.assertEqual(pool.performance[0].samples,0)

    async def run_one_worker(self, handler, stop=None, options=None, messages=None):
        options = options or parser().parse_args([])
        messages = messages or queue.Queue()
        stop = stop or threading.Event()
        slot = multiprocessing.get_context('spawn').Array('d',len(COUNTERS)+1)
        next_job = SimpleNamespace(value=0,get_lock=lambda:threading.Lock())
        clients = Clients([handler]*6)
        with patch('collect_metadata_all.CatalogClients',return_value=clients):
            await run_worker(0,1,[(VIDEO,'video')],clients.proxies,ranks(6),next_job,
                SimpleNamespace(value=1),messages,stop,options,slot)
        return messages,slot

    async def test_deferred_recovery_saves_one_final_result(self):
        calls = 0
        def respond(_):
            nonlocal calls
            calls += 1
            return httpx.Response(403) if calls <= 3 else httpx.Response(200,json=payload())
        messages,slot = await self.run_one_worker(respond)
        result = messages.get_nowait()['result']
        self.assertTrue(result['recovery'])
        self.assertEqual((result['attempts'],calls),(4,4))
        self.assertEqual(result['outcome'],'data')
        self.assertTrue(messages.empty())
        self.assertEqual(slot[COUNTERS.index('recovery_ids')],1)

    async def test_retry_budget_is_six_total_and_only_final_failure_is_emitted(self):
        messages,slot = await self.run_one_worker(lambda _:httpx.Response(429))
        result = messages.get_nowait()['result']
        self.assertEqual(result['attempts'],6)
        self.assertTrue(result['final'])
        self.assertFalse(result['availability_confirmed'])
        self.assertTrue(messages.empty())
        self.assertEqual(slot[COUNTERS.index('http_attempts')],6)

    async def test_stop_leaves_an_unfinished_failure_for_resume(self):
        stop = threading.Event()
        def respond(_):
            stop.set()
            return httpx.Response(403)
        messages,_ = await self.run_one_worker(respond,stop,parser().parse_args(['--attempts','1']))
        self.assertTrue(messages.empty())


class InputFileTests(unittest.TestCase):
    def test_saved_input_detects_tampering_and_duplicate_ids(self):
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            save_input(folder,[(VIDEO,'video')],{'run_id':'test'})
            self.assertEqual(read_input(resume_source(folder)),[(VIDEO,'video')])
            with (folder/'input.jsonl').open('a') as output:
                output.write(json.dumps(dict(video_id=VIDEO,type='video'))+'\n')
            with self.assertRaises(ValueError):
                read_input(folder/'input.jsonl')
            with self.assertRaises(ValueError):
                resume_source(folder)


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        try:
            self.conn = connect_test_database('media',autocommit=True)
        except psycopg.OperationalError as exc:
            self.skipTest(str(exc))
        self.channel = 'UC'+uuid.uuid4().hex[:22]
        self.ids = [uuid.uuid4().hex[:11] for _ in range(3)]
        self.conn.execute('INSERT INTO public.channels(channel_id) VALUES (%s)',(self.channel,))
        self.conn.execute("INSERT INTO public.videos(video_id,channel_id,type) SELECT unnest(%s::text[]),%s,'video'",
                          (self.ids,self.channel))
        self.writer = MetadataWriter(self.conn)

    def tearDown(self):
        if hasattr(self,'conn'):
            self.conn.execute('DELETE FROM public.videos WHERE channel_id=%s',(self.channel,))
            self.conn.execute('DELETE FROM public.channels WHERE channel_id=%s',(self.channel,))
            self.conn.close()

    def test_save_preserves_empty_values_and_other_collection_fields_and_is_idempotent(self):
        self.conn.execute("UPDATE public.videos SET metadata_error='old error',view_count=123,comments_error='kept' WHERE video_id=%s",
                          (self.ids[0],))
        row = final_result(self.ids[0])
        self.writer.write([row])
        self.assertTrue(row['saved'])
        self.assertTrue(all(row['fields_saved'].values()))
        first = self.conn.execute('''SELECT description,duration_seconds,metadata_updated_at,metadata_error,
            view_count,comments_error FROM public.videos WHERE video_id=%s''',(self.ids[0],)).fetchone()
        self.assertEqual((first[0],first[1],first[3],first[4],first[5]),('',0,None,123,'kept'))
        self.writer.write([row])
        self.assertTrue(row['already_saved'])
        self.assertFalse(row['saved'])
        self.assertEqual(self.conn.execute('SELECT metadata_updated_at FROM public.videos WHERE video_id=%s',
                                          (self.ids[0],)).fetchone()[0],first[2])

    def test_missing_fields_preserve_prior_values_and_failures_keep_timestamp_empty(self):
        self.conn.execute("UPDATE public.videos SET description='saved text',duration_seconds=42 WHERE video_id=%s",(self.ids[0],))
        good,bad = final_result(self.ids[0]),final_result(self.ids[1],successful=False)
        good['metadata'] = {'title':'new title'}
        self.writer.write([good,bad])
        self.assertEqual(self.conn.execute('SELECT description,duration_seconds FROM public.videos WHERE video_id=%s',
                                          (self.ids[0],)).fetchone(),('saved text',42))
        at,error = self.conn.execute('SELECT metadata_updated_at,metadata_error FROM public.videos WHERE video_id=%s',
                                     (self.ids[1],)).fetchone()
        self.assertIsNone(at)
        self.assertIn('ConnectError',error)
        self.assertTrue(bad['error_saved'])

    def test_batch_rollback_and_retry(self):
        good,bad = final_result(self.ids[0]),final_result(uuid.uuid4().hex[:11])
        with self.assertRaises(ValueError):
            self.writer.write([good,bad])
        self.assertIsNone(self.conn.execute('SELECT metadata_updated_at FROM public.videos WHERE video_id=%s',
                                          (self.ids[0],)).fetchone()[0])
        self.writer.write([good])
        self.assertTrue(good['saved'])

    def test_duplicate_or_unfinished_outcomes_are_rejected(self):
        row = final_result(self.ids[0])
        with self.assertRaises(ValueError):
            self.writer.write([row,row])
        row['final'] = False
        with self.assertRaises(ValueError):
            self.writer.write([row])

    def test_explicit_input_retries_errors_but_skips_saved_rows(self):
        self.writer.write([final_result(self.ids[0]),final_result(self.ids[1],successful=False)])
        fresh,_ = select_input(self.conn)
        self.assertIn((self.ids[2],'video'),fresh)
        self.assertNotIn((self.ids[1],'video'),fresh)
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary)
            save_input(folder,[(v,'video') for v in self.ids],{'run_id':'test'})
            selected,counts = select_input(self.conn,folder/'input.jsonl')
            self.assertEqual(set(selected),{(v,'video') for v in self.ids[1:]})
            self.assertEqual(counts['already_saved'],1)

    def controller(self,options):
        with patch('collect_metadata_all.STATE_DIR',options.output.parent/'state'), \
             patch('collect_metadata_all.load_ranked_proxies',return_value=([1,2],ranks(2))), \
             patch('collect_metadata_all.worker_entry',fixture_worker), \
             patch('collect_metadata_all.connect_database',side_effect=lambda _,**kw:connect_test_database('media',**kw)):
            return collect(options)

    def test_spawned_controller_saves_only_committed_ids_and_resume_skips_them(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root/'source'
            source.mkdir()
            save_input(source,[(v,'video') for v in self.ids],{'run_id':'source'})
            options = parser().parse_args(['--input',str(source/'input.jsonl'),'--output',str(root/'first'),
                                          '--workers','2','--concurrency','2'])
            self.assertEqual(self.controller(options),0)
            summary = json.loads((options.output/'summary.json').read_text())
            self.assertEqual((summary['saved'],summary['database_completed'],summary['http_attempts']),(3,3,3))
            self.assertEqual({json.loads(line)['video_id'] for line in (options.output/'saved-video-ids.jsonl').read_text().splitlines()},set(self.ids))
            again = parser().parse_args(['--resume',str(options.output),'--output',str(root/'again')])
            self.assertEqual(self.controller(again),0)
            self.assertEqual(json.loads((again.output/'summary.json').read_text())['input_count'],0)

    def test_interrupted_writer_resumes_remaining_ids_automatically(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root/'source'
            source.mkdir()
            save_input(source,[(v,'video') for v in self.ids],{'run_id':'source'})
            options = parser().parse_args(['--input',str(source/'input.jsonl'),'--output',str(root/'first'),
                '--workers','1','--concurrency','1','--batch-size','1'])
            original = MetadataWriter.write
            calls = 0
            def fail_second(writer,rows):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise RuntimeError('simulated writer interruption')
                return original(writer,rows)
            with patch.object(MetadataWriter,'write',fail_second):
                self.assertEqual(self.controller(options),2)
            first = json.loads((options.output/'summary.json').read_text())
            self.assertEqual((first['saved'],first['database_unresolved']),(1,2))
            self.assertTrue(json.loads((root/'state/metadata-active.json').read_text())['active'])
            resumed = parser().parse_args(['--output',str(root/'second'),'--workers','1','--concurrency','1'])
            self.assertEqual(self.controller(resumed),0)
            self.assertEqual(json.loads((resumed.output/'summary.json').read_text())['saved'],2)
            self.assertFalse(json.loads((root/'state/metadata-active.json').read_text())['active'])

    def test_existing_metadata_lock_prevents_a_second_collector(self):
        self.conn.execute("SELECT pg_advisory_lock(hashtext('media.video-metadata'))")
        with tempfile.TemporaryDirectory() as temporary:
            options = parser().parse_args(['--output',str(Path(temporary)/'run')])
            with self.assertRaisesRegex(RuntimeError,'Another video metadata collector'):
                self.controller(options)
            self.assertFalse(options.output.exists())
