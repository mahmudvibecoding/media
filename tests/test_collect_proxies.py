import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch
import uuid

import httpx
import psycopg
from psycopg.types.json import Jsonb

from collect_proxies import Downloader, Store, connection, now, stored_settings, format_hint_for_url


def args(**overrides):
    values = dict(concurrency=4, per_host=2, github_concurrency=2, retries=0, max_bytes=2*1024*1024)
    values.update(overrides)
    return argparse.Namespace(**values)


class FormatHintTests(unittest.TestCase):
    def test_url_suffix_ignores_query_and_fragment(self):
        cases={
            'https://example.com/list.JSON?format=yaml#result.xml':'json',
            'https://example.com/paths.with.dots/api':None,
            'https://example.com/.hidden':None,
            'https://example.com/list.tar.gz':'gz',
            'https://example.com/list.txt/':'txt',
        }
        for url,expected in cases.items():
            with self.subTest(url=url):
                self.assertEqual(format_hint_for_url(url),expected)


class DownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)
        for folder in ('tmp', 'payloads', 'parsed'):
            (self.path / folder).mkdir()
        self.patch = patch('collect_proxies.STORAGE', self.path)
        self.patch.start()
        self.downloader = Downloader(args())
        await self.downloader.client.aclose()
        self.downloader.public_url = AsyncMock(return_value=True)
        self.row = {'list_url':'https://lists.example.com/proxies.txt','protocol_hints':[]}

    async def asyncTearDown(self):
        await self.downloader.client.aclose()
        self.patch.stop()
        self.temp.cleanup()

    async def test_full_compressed_download_exceeds_old_sample_limit(self):
        body = b'8.8.8.8:8080\n' * 30000
        seen = []
        def response(request):
            seen.append(str(request.url))
            return httpx.Response(200, content=gzip.compress(body), headers={'Content-Encoding':'gzip'})
        self.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        result = await self.downloader.fetch(self.row)
        self.assertEqual(result['status'], 'downloaded')
        self.assertGreater(result['decoded_bytes'], 262144)
        with gzip.open(result['payload_path'], 'rb') as f:
            self.assertEqual(f.read(), body)
        self.assertEqual(seen, [self.row['list_url']])

    async def test_incomplete_response_is_never_imported(self):
        self.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(206, content=b'8.8.8.8:80')))
        result = await self.downloader.fetch(self.row)
        self.assertEqual(result['status'], 'http_206')
        self.assertNotIn('payload_path', result)

    async def test_size_limit_is_reported_and_partial_payload_deleted(self):
        self.downloader.args.max_bytes = 10
        self.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b'8.8.8.8:8080\n'*5)))
        result = await self.downloader.fetch(self.row)
        self.assertEqual(result['status'], 'size_limit_exceeded')
        self.assertFalse(list((self.path/'tmp').iterdir()))
        self.assertFalse(list((self.path/'payloads').iterdir()))

    async def test_redirect_to_private_address_is_not_followed(self):
        self.downloader.public_url = AsyncMock(side_effect=lambda url: '127.0.0.1' not in url)
        seen = []
        def response(request):
            seen.append(str(request.url))
            return httpx.Response(302, headers={'Location':'http://127.0.0.1/private'})
        self.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        result = await self.downloader.fetch(self.row)
        self.assertEqual(result['status'], 'source_unreachable_or_not_public')
        self.assertEqual(len(seen), 1)

    async def test_host_rate_limit_stops_further_requests(self):
        seen = []
        def response(request):
            seen.append(str(request.url))
            return httpx.Response(429)
        self.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        first = await self.downloader.fetch(self.row)
        second = await self.downloader.fetch(dict(self.row, list_url='https://lists.example.com/other.txt'))
        self.assertEqual(first['status'], 'http_429')
        self.assertEqual(second['status'], 'host_rate_limited')
        self.assertEqual(len(seen), 1)

    async def test_api_pagination_collects_every_page_under_original_url(self):
        self.row['list_url'] = 'https://lists.example.com/api?page=1'
        seen = []
        def response(request):
            page = int(request.url.params['page'])
            seen.append(page)
            return httpx.Response(200,json={'page':page,'limit':1,'total':2,
                'data':[{'ip':'8.8.8.8' if page == 1 else '1.1.1.1','port':8080}]})
        self.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        result = await self.downloader.fetch(self.row)
        self.assertEqual(result['list_url'], self.row['list_url'])
        self.assertEqual(result['status'], 'downloaded')
        self.assertEqual(result['download_details']['pages'], 2)
        self.assertTrue(result['download_details']['pagination_complete'])
        self.assertEqual(seen, [1,2])
        with gzip.open(result['payload_path'],'rt') as f:
            self.assertEqual(len(json.load(f)), 2)

    async def test_failed_later_page_retains_data_and_marks_incomplete(self):
        self.row['list_url'] = 'https://lists.example.com/api?page=1'
        def response(request):
            if request.url.params['page'] == '2':
                return httpx.Response(503)
            return httpx.Response(200,json={'page':1,'limit':1,'total':2,
                'data':[{'ip':'8.8.8.8','port':8080}]})
        self.downloader.client = httpx.AsyncClient(transport=httpx.MockTransport(response))
        result = await self.downloader.fetch(self.row)
        self.assertEqual(result['status'], 'downloaded_partial')
        self.assertEqual(result['download_details']['pagination_error'], 'http_503')
        with gzip.open(result['payload_path'],'rt') as f:
            self.assertEqual(len(json.load(f)), 1)


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE') == '1', 'Opt in to isolated database integration tests')
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        from psycopg.rows import dict_row
        from collect_proxies import DB
        self.conn = psycopg.connect(**DB, row_factory=dict_row)
        schema = 'test_proxy_' + uuid.uuid4().hex
        self.conn.execute('CREATE SCHEMA ' + schema)
        self.conn.execute('SET LOCAL search_path TO ' + schema + ',public')
        ddl = (Path(__file__).resolve().parents[1] / 'db/proxy/schema.sql').read_text()
        self.conn.execute(ddl.replace('public.',schema+'.'))
        self.conn.execute('CREATE TEMP TABLE proxy_stage (connection_key BYTEA PRIMARY KEY, address TEXT, port INTEGER, protocol TEXT, connection_settings JSONB) ON COMMIT DELETE ROWS')
        self.store = Store.__new__(Store)
        self.store.control = self.store.writer = self.conn
        self.store.run_id = uuid.uuid4()
        self.temp = tempfile.TemporaryDirectory()
        self.storage_patch = patch('collect_proxies.STORAGE',Path(self.temp.name))
        self.storage_patch.start()

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()
        self.storage_patch.stop()
        self.temp.cleanup()

    def payload(self, body):
        checksum = hashlib.sha256(body).hexdigest()
        payload = Path(self.temp.name) / (checksum+'.gz')
        with gzip.open(payload,'wb') as f:
            f.write(body)
        return dict(status='downloaded',list_url='https://test.invalid/a',protocol_hints=[],
                    content_sha256=checksum,payload_path=str(payload),finished_at=now(),started_at=now(),attempts=1)

    def add_list(self, url, selected=True):
        self.conn.execute("INSERT INTO proxy_lists (url,kind,enabled,run_id,status) VALUES (%s,'feed_candidate',true,%s,'pending')",
                          (url,self.store.run_id if selected else None))

    def test_binary_settings_round_trip(self):
        original = {'prefix\x00key':'\x16\x03\x00','nested':{'ordinary':'value'}}
        stored = self.conn.execute('SELECT %s::jsonb AS settings',(stored_settings(original),)).fetchone()['settings']
        self.assertIsInstance(stored,str)
        self.assertEqual(json.loads(stored),original)

    def test_deduplication_reparse_and_atomic_failure(self):
        self.add_list('https://test.invalid/a')
        self.add_list('https://test.invalid/b')
        self.add_list('https://test.invalid/c',selected=False)
        result = self.payload(b'8.8.4.4:61234\n8.8.4.4:61234\n')
        self.store.save(dict(result))
        self.store.save(dict(result,reparse=True))
        self.store.save(dict(result,list_url='https://test.invalid/b'))
        self.assertEqual(self.conn.execute('SELECT count(*) AS n FROM proxies').fetchone()['n'],1)
        state = self.conn.execute("SELECT fetch_state FROM proxy_lists WHERE url=%s",(result['list_url'],)).fetchone()['fetch_state']
        self.assertEqual((state['entries_found'],state['unique_entries']),(2,1))
        # A new configuration must roll back with a missing current list record.
        other = self.payload(b'1.1.1.1:61234\n')
        with self.assertRaisesRegex(RuntimeError,'rolled back'):
            self.store.save(dict(other,list_url='https://test.invalid/c'))
        self.assertEqual(self.conn.execute('SELECT count(*) AS n FROM proxies').fetchone()['n'],1)
        # Corrected parsing adds identities while retaining previously collected ones.
        self.store.save(dict(other,reparse=True))
        self.assertEqual(self.conn.execute('SELECT count(*) AS n FROM proxies').fetchone()['n'],2)
        self.store.save(dict(self.payload(b''),reparse=True))
        self.assertEqual(self.conn.execute('SELECT count(*) AS n FROM proxies').fetchone()['n'],2)

    def test_resume_retry_and_current_payload_round_trip(self):
        self.add_list('https://test.invalid/a')
        self.add_list('https://test.invalid/b')
        options = args(run_id=str(self.store.run_id),retry_failures=False,reparse=False,complete_pagination=False,take=None)
        self.assertEqual(len(self.store.prepare(options)),2)
        result = self.payload(b'8.8.4.4:61234\n')
        self.store.save(dict(result))
        self.assertEqual(len(self.store.prepare(options)),1)
        options.reparse = True
        rows = self.store.prepare(options)
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['finished_at'],result['finished_at'])
        self.assertEqual(rows[0]['started_at'],result['started_at'])
        self.assertEqual(rows[0]['content_sha256'],result['content_sha256'])
        options.reparse = False
        self.conn.execute("UPDATE proxy_lists SET status='download_error' WHERE url='https://test.invalid/b'")
        options.retry_failures = True
        self.assertEqual(len(self.store.prepare(options)),1)
        options.run_id = None
        options.retry_failures = False
        with self.assertRaisesRegex(ValueError,'unfinished'):
            self.store.prepare(options)


if __name__ == '__main__':
    unittest.main()
