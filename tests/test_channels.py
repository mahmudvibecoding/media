import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import uuid

import httpx

from channel_info import FIELDS, ResponseShapeError, about_request, count, fetch_info, parse_about, parse_overview
from channel_storage import ChannelWriter, save_input, select_channels
from collect_channels import Fetcher, collect, parser
from database_helpers import connect_test_database

CHANNEL = 'UC' + 'a'*22


def overview(cid=CHANNEL):
    return {'metadata':{'channelMetadataRenderer':{'externalId':cid,'title':'Channel title',
        'description':'','vanityChannelUrl':'https://www.youtube.com/@example',
        'keywords':'news "local news"','avatar':{'thumbnails':[{'url':'https://example.com/avatar.png','width':900}]}}},
        'header':{'description':{'continuationEndpoint':{
            'commandMetadata':{'webCommandMetadata':{'apiUrl':'/youtubei/v1/browse'}},
            'continuationCommand':{'token':'returned-about-token'}}}}}


def about(cid=CHANNEL):
    return {'onResponseReceivedEndpoints':[{'aboutChannelViewModel':{
        'channelId':cid, 'description':'Full description', 'subscriberCountText':'12.3K subscribers',
        'videoCountText':'1 video','viewCountText':'1,234,567 views',
        'joinedDateText':{'content':'Joined Feb 29, 2024'},'country':'Uzbekistan',
        'canonicalChannelUrl':'http://www.youtube.com/@example',
        'links':[{'channelExternalLinkViewModel':{'title':{'content':'Telegram'},'link':{
            'commandRuns':[{'onTap':{'innertubeCommand':{'urlEndpoint':{
                'url':'/redirect?q=https%3A%2F%2Ft.me%2Fexample&event=channel'}}}}]}}}]}}]}


def final(cid=CHANNEL, good=True):
    return dict(channel_id=cid,status='ok' if good else 'error',
        metadata=parse_about(about(cid),cid,parse_overview(overview(cid),cid)) if good else None,
        observed_at=datetime.now(timezone.utc).isoformat(),error=None if good else 'HTTP 503',http_requests=2)


class ParseTests(unittest.TestCase):
    def test_modern_profile_with_quoted_keywords_date_and_public_links(self):
        result = final()['metadata']
        self.assertEqual(set(result),set(FIELDS))
        self.assertEqual(result['keywords'],['news','local news'])
        self.assertEqual((result['subscriber_count'],result['video_count'],result['view_count']),(12300,1,1234567))
        self.assertEqual(result['joined_date'],'2024-02-29')
        self.assertEqual(result['external_links'],[{'title':'Telegram','url':'https://t.me/example'}])

    def test_absent_counts_are_null_and_explicit_zero_is_zero(self):
        payload = about()
        item = payload['onResponseReceivedEndpoints'][0]['aboutChannelViewModel']
        item.pop('subscriberCountText')
        item['videoCountText'] = 'No videos'
        item['viewCountText'] = '0 views'
        item.pop('country')
        item.pop('links')
        result = parse_about(payload,CHANNEL,parse_overview(overview(),CHANNEL))
        self.assertIsNone(result['subscriber_count'])
        self.assertIsNone(result['country'])
        self.assertEqual((result['video_count'],result['view_count'],result['external_links']),(0,0,[]))
        with self.assertRaises(ResponseShapeError):
            count('not a number subscribers','subscribers')

    def test_bare_domain_destinations_preserve_the_youtube_redirect(self):
        for url in ('https://www.youtube.com/redirect?q=www.cooperation.uz',
                    '/redirect?q=www.cooperation.uz', '/redirect?q=mailto%3Ahello%40example.com'):
            with self.subTest(url=url):
                payload=about()
                item=payload['onResponseReceivedEndpoints'][0]['aboutChannelViewModel']
                item['links']=[{'channelExternalLinkViewModel':{
                    'title':{'content':'Contact'},'urlEndpoint':{'url':url}}}]
                result=parse_about(payload,CHANNEL,parse_overview(overview(),CHANNEL))
                expected='https://www.youtube.com'+url if url.startswith('/') else url
                self.assertEqual(result['external_links'],[{'title':'Contact','url':expected}])

    def test_wrong_ids_missing_about_and_malformed_links_are_rejected(self):
        with self.assertRaises(ResponseShapeError):
            parse_overview(overview('other'),CHANNEL)
        with self.assertRaises(ResponseShapeError):
            parse_about(about('other'),CHANNEL,parse_overview(overview(),CHANNEL))
        with self.assertRaises(ResponseShapeError):
            about_request({'header':{}},CHANNEL)
        payload = about()
        payload['onResponseReceivedEndpoints'][0]['aboutChannelViewModel']['links'] = [{}]
        with self.assertRaises(ResponseShapeError):
            parse_about(payload,CHANNEL,parse_overview(overview(),CHANNEL))
        payload=about()
        payload['onResponseReceivedEndpoints'][0]['aboutChannelViewModel']['description']='bad\x00text'
        with self.assertRaises(ResponseShapeError):
            parse_about(payload,CHANNEL,parse_overview(overview(),CHANNEL))

    def test_legacy_about_endpoint_and_renderer(self):
        first=overview()
        first['header']={}
        first['contents']={'tabRenderer':{'title':'About','endpoint':{
            'browseEndpoint':{'browseId':CHANNEL,'params':'returned-params'}}}}
        self.assertEqual(about_request(first,CHANNEL),{'browseId':CHANNEL,'params':'returned-params'})
        first['contents']={}
        first['header']={'tagline':{'moreEndpoint':{'browseEndpoint':{'browseId':CHANNEL,'params':'tagline-params'}}}}
        self.assertEqual(about_request(first,CHANNEL),{'browseId':CHANNEL,'params':'tagline-params'})
        payload={'contents':{'channelAboutFullMetadataRenderer':{
            'channelId':CHANNEL,'description':{'simpleText':'Legacy'},'country':{'simpleText':'Uzbekistan'},
            'joinedDateText':{'runs':[{'text':'Joined January 3, 2020'}]}}}}
        self.assertEqual(parse_about(payload,CHANNEL,parse_overview(first,CHANNEL))['joined_date'],'2020-01-03')


class FetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_requests_follow_the_returned_about_token(self):
        seen=[]
        def respond(request):
            seen.append(json.loads(request.content))
            return httpx.Response(200,json=overview() if 'browseId' in seen[-1] else about())
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result=await fetch_info(client,CHANNEL)
        self.assertEqual(result['status'],'ok')
        self.assertEqual(result['http_requests'],2)
        self.assertEqual(seen[1]['continuation'],'returned-about-token')
        self.assertEqual(seen[0]['context']['client']['hl'],'en')

    async def test_failed_second_request_does_not_return_partial_profile(self):
        def respond(request):
            return httpx.Response(200,json=overview()) if 'browseId' in json.loads(request.content) else httpx.Response(503)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result=await fetch_info(client,CHANNEL)
        self.assertEqual(result['error'],'HTTP 503')
        self.assertIsNone(result['metadata'])

    async def test_oversized_response_is_rejected(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _:httpx.Response(200,content=b'x'*600000))) as client:
            result=await fetch_info(client,CHANNEL)
        self.assertIn('size limit',result['error'])

    async def test_proxy_retry_recovers_after_a_failed_direct_request(self):
        good=httpx.AsyncClient(transport=httpx.MockTransport(lambda request:httpx.Response(200,
            json=overview() if 'browseId' in json.loads(request.content) else about())))
        bad=httpx.AsyncClient(transport=httpx.MockTransport(lambda _:httpx.Response(403)))
        class Clients:
            proxies=[type('Proxy',(),{'proxy_id':9})()]
            def __getitem__(self,index): return good
        stats=Counter()
        fetcher=Fetcher(parser().parse_args([]),bad,Clients(),[{'average_response_ms':100}],stats,threading.Event())
        try:
            result=await fetcher.fetch(CHANNEL)
            self.assertEqual((result['status'],result['attempts'],result['proxy_id']),('ok',2,9))
            self.assertEqual(stats['http_requests'],3)
            self.assertEqual(fetcher.direct_limit,256)
            self.assertEqual(fetcher.pool.active,set())
        finally:
            await good.aclose()
            await bad.aclose()


@unittest.skipUnless(os.environ.get('MEDIA_TEST_DATABASE_URL'),'Set MEDIA_TEST_DATABASE_URL for database tests')
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn=connect_test_database('media',autocommit=True)
        self.ids=['UC'+uuid.uuid4().hex[:22] for _ in range(3)]
        self.conn.execute('INSERT INTO public.channels(channel_id,subscriber_count) SELECT unnest(%s::text[]),99',(self.ids,))
        self.writer=ChannelWriter(self.conn)

    def tearDown(self):
        self.conn.execute('DELETE FROM public.channels WHERE channel_id=ANY(%s)',(self.ids,))
        self.conn.close()

    def test_complete_profile_roundtrip_and_failure_preserves_prior_values(self):
        result=final(self.ids[0])
        self.writer.write([result,final(self.ids[1],False)])
        columns=','.join(FIELDS)+',metadata_updated_at'
        before=self.conn.execute('SELECT '+columns+' FROM channels WHERE channel_id=%s',(self.ids[0],)).fetchone()
        self.assertEqual(before[3:6],(12300,1,1234567))
        self.assertEqual(before[10],[{'title':'Telegram','url':'https://t.me/example'}])
        self.writer.write([final(self.ids[0],False)])
        self.assertEqual(self.conn.execute('SELECT '+columns+' FROM channels WHERE channel_id=%s',(self.ids[0],)).fetchone(),before)
        self.assertEqual(self.conn.execute('SELECT subscriber_count,metadata_updated_at,metadata_error FROM channels WHERE channel_id=%s',
            (self.ids[1],)).fetchone(),(99,None,'HTTP 503'))

    def test_missing_channel_rolls_back_the_whole_batch(self):
        with self.assertRaises(ValueError):
            self.writer.write([final(self.ids[0]),final('UC'+'z'*22)])
        self.assertEqual(self.conn.execute('SELECT subscriber_count,metadata_updated_at FROM channels WHERE channel_id=%s',
            (self.ids[0],)).fetchone(),(99,None))
        self.writer.write([final(self.ids[0])])

    def source(self,root):
        folder=root/'source'
        folder.mkdir()
        save_input(folder,self.ids,{'run_id':'fixture','started_at':datetime.now(timezone.utc).isoformat()})
        return folder

    def controller(self,options):
        async def fetch(client,cid,**kwargs):
            await asyncio.sleep(0)
            return final(cid)
        with patch('collect_channels.STATE_DIR',options.output.parent/'state'), \
             patch('collect_channels.fetch_info',side_effect=fetch), \
             patch('collect_channels.connect_database',side_effect=lambda _,**kwargs:connect_test_database('media',**kwargs)):
            return collect(options)

    def test_interrupted_batches_resume_without_repeating_completed_channels(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            source=self.source(root)
            options=parser().parse_args(['--resume',str(source),'--output',str(root/'first'),
                '--transport','direct','--concurrency','1','--batch-size','1'])
            original=ChannelWriter.write
            calls=0
            def fail_second(writer,rows):
                nonlocal calls
                calls+=1
                if calls==2:
                    raise RuntimeError('Simulated interruption')
                return original(writer,rows)
            with patch.object(ChannelWriter,'write',fail_second):
                self.assertEqual(self.controller(options),2)
            first=json.loads((options.output/'summary.json').read_text())
            self.assertEqual((first['saved'],first['database_unresolved']),(1,2))
            resumed=parser().parse_args(['--output',str(root/'second'),'--transport','direct','--concurrency','2'])
            self.assertEqual(self.controller(resumed),0)
            second=json.loads((resumed.output/'summary.json').read_text())
            self.assertEqual((second['input_count'],second['saved'],second['database_unresolved']),(2,2,0))

    def test_modified_input_is_rejected_and_subscriber_lock_is_shared(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            source=self.source(root)
            with (source/'input.jsonl').open('a') as f: f.write('{}\n')
            with self.assertRaises(ValueError): select_channels(self.conn,resume=source)
            self.conn.execute("SELECT pg_advisory_lock(hashtext('media.subscriber-count'))")
            options=parser().parse_args(['--output',str(root/'run'),'--transport','direct'])
            with self.assertRaisesRegex(RuntimeError,'Another channel collector'):
                self.controller(options)


if __name__=='__main__':
    unittest.main()
