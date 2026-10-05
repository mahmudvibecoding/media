from contextlib import contextmanager
from datetime import datetime, timezone
import html
import os
from pathlib import Path
import re
import unittest
from urllib.parse import parse_qs, urlsplit
import uuid

from fastapi.testclient import TestClient
import psycopg
from psycopg.rows import dict_row

from dashboard.app import Settings, create_app, highlighted, next_path, safe_url
from dashboard.data import Repository, Selection, SORTS
from dashboard.security import Cursors, password_hash, verify_password


SECRET = 'dashboard-tests-' * 4
ROOT = Path(__file__).resolve().parents[1]


def selection_url(url):
    value = urlsplit(url)
    return Selection.parse(value.path.strip('/'), {key:items[0] for key,items in parse_qs(value.query).items()})


class DashboardSecurityTests(unittest.TestCase):
    def test_password_hash_and_invalid_formats(self):
        hashed = password_hash('a long test password')
        self.assertTrue(verify_password('a long test password',hashed))
        self.assertFalse(verify_password('incorrect',hashed))
        self.assertFalse(verify_password('anything','pbkdf2_sha256$9999999999$x$x'))
        self.assertFalse(verify_password('anything','broken'))

    def test_cursor_cannot_change_query_or_be_tampered_with(self):
        cursors = Cursors(SECRET)
        original = dict(scope='one',keys=[42,'abc'],direction='next',page=2)
        token = cursors.encode(original)
        self.assertEqual(cursors.decode(token,'one'), original)
        for value, scope in ((token,'two'),(token[:-1]+'x','one'),('broken','one'),(cursors.encode([]),'one')):
            with self.subTest(value=value), self.assertRaises(ValueError):
                cursors.decode(value,scope)

    def test_highlight_preserves_text_and_escapes_markup(self):
        text = '<img src=x onerror=alert(1)> O‘zbekiston & HELLO'
        rendered = str(highlighted(text,"o'zbekiston hello"))
        self.assertNotIn('<img',rendered)
        self.assertIn('&lt;img',rendered)
        self.assertIn('<mark>O‘zbekiston</mark>',rendered)
        self.assertIn('<mark>HELLO</mark>',rendered)
        self.assertEqual(html.unescape(re.sub('</?mark>','',rendered)),text)

    def test_external_links_and_redirects(self):
        for value in ('javascript:alert(1)','data:text/html,x','//evil.example','https://user:password@example.com'):
            self.assertEqual(safe_url(value),'')
        self.assertEqual(safe_url('https://example.com/a'),'https://example.com/a')
        self.assertEqual(next_path('//evil.example'),'/channels')
        self.assertEqual(next_path('/videos?q=hello'),'/videos?q=hello')

    def test_parameters_reject_invalid_identifiers_and_sort_injection(self):
        for kind, parameters in (('channels',{'sort':'name;DROP TABLE channels'}),('videos',{'channel':'x'}),
                                  ('comments',{'video':'bad'}),('comments',{'q':'x'*201}),
                                  ('comments',{'q':'""'}),('videos',{'type':'live'})):
            with self.subTest(kind=kind,parameters=parameters), self.assertRaises(ValueError):
                Selection.parse(kind,parameters)


class SchemaPool:
    """Keep each test in one disposable transaction and schema."""
    def __init__(self, conn, schema):
        self.conn, self.schema = conn, schema

    def open(self, **kwargs):
        pass

    def close(self):
        pass

    @contextmanager
    def connection(self):
        yield self

    def transaction(self):
        return self.conn.transaction()

    def execute(self, query, parameters=None):
        return self.conn.execute(query.replace('public.',self.schema+'.'),parameters)


@unittest.skipUnless(os.environ.get('MEDIA_TEST_DATABASE_URL'),'Set MEDIA_TEST_DATABASE_URL for database tests')
class DashboardIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = Settings(username='tester',password_hash=password_hash('test-password'),secret=SECRET)

    def setUp(self):
        self.conn = psycopg.connect(os.environ['MEDIA_TEST_DATABASE_URL'],row_factory=dict_row)
        self.addCleanup(self.conn.close)
        transaction = self.conn.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__,None,None,None)
        schema = 'dashboard_test_'+uuid.uuid4().hex
        self.conn.execute('CREATE SCHEMA '+schema)
        self.pool = SchemaPool(self.conn,schema)
        self.pool.execute((ROOT/'db/schema.sql').read_text())
        self.pool.execute('''INSERT INTO public.channels(channel_id,title,handle,description,subscriber_count,keywords)
            SELECT 'UC'||lpad(n::text,22,'0'),'Library '||lpad(n::text,3,'0'),'@channel'||n,
            CASE WHEN n=0 THEN 'blue quiet river O‘zbekiston' WHEN n=1 THEN 'blue fast river' ELSE '' END,
            CASE WHEN n>108 THEN NULL ELSE n/3 END,ARRAY['collection'] FROM generate_series(0,114) n''')
        self.pool.execute('''INSERT INTO public.videos(video_id,channel_id,type,title,description,published_at,view_count)
            SELECT 'V'||lpad(n::text,10,'0'),'UC'||lpad((n%2)::text,22,'0'),
            CASE WHEN n%3=0 THEN 'short' ELSE 'video' END,'Video '||lpad(n::text,3,'0'),
            CASE WHEN n=0 THEN 'blue quiet river O‘zbekiston' WHEN n=1 THEN 'blue fast river' ELSE '' END,
            CASE WHEN n>108 THEN NULL ELSE '2026-01-01'::timestamptz+(n/3)*interval '1 day' END,
            CASE WHEN n>108 THEN NULL ELSE n/3 END FROM generate_series(0,114) n''')
        self.pool.execute('''INSERT INTO public.comments(video_id,comment_id,text,author_name,is_pinned)
            SELECT 'V'||lpad(n::text,10,'0'),'shared-comment',
            CASE WHEN n=0 THEN 'blue quiet river O‘zbekiston <script>alert(1)</script>' WHEN n=1 THEN 'blue fast river' ELSE 'Comment '||n END,
            'Author '||(n%3),n%7=0 FROM generate_series(0,114) n''')
        self.repo = Repository('',SECRET,pool=self.pool)

    def identity(self, kind, row):
        if kind == 'channels':
            return row['channel_id']
        if kind == 'videos':
            return row['video_id']
        return row['video_id'],row['comment_id']

    def test_every_sort_pages_forward_and_backward_without_duplicates(self):
        for kind, sorts in SORTS.items():
            for sort in sorts:
                with self.subTest(kind=kind,sort=sort):
                    first = self.repo.search(Selection.parse(kind,{'sort':sort}))
                    second = self.repo.search(selection_url(first['next']))
                    third = self.repo.search(selection_url(second['next']))
                    rows = [*first['rows'],*second['rows'],*third['rows']]
                    self.assertEqual(len(rows),115)
                    self.assertEqual(len({self.identity(kind,row) for row in rows}),115)
                    self.assertIsNone(third['next'])
                    back_second = self.repo.search(selection_url(third['previous']))
                    back_first = self.repo.search(selection_url(back_second['previous']))
                    self.assertEqual(back_second['rows'],second['rows'])
                    self.assertEqual(back_first['rows'],first['rows'])
                    self.assertIsNone(back_first['previous'])

    def test_keywords_phrases_apostrophes_and_partial_channel_names(self):
        for kind in SORTS:
            for query, expected in (('BLUE river',2),('"blue quiet river"',1),('blue -quiet',1),("OʻZBEKISTON",1)):
                with self.subTest(kind=kind,query=query):
                    self.assertEqual(len(self.repo.search(Selection.parse(kind,{'q':query}))['rows']),expected)
        rows = self.repo.search(Selection.parse('channels',{'q':'brary 000'}))['rows']
        self.assertEqual([row['channel_id'] for row in rows],['UC'+'0'*22])
        self.assertEqual(len(self.repo.search(Selection.parse('comments',{'q':'"; DROP TABLE comments; --'}))['rows']),0)

    def test_filters_scope_and_details(self):
        channel = 'UC'+'0'*22
        video = 'V'+'0'*10
        result = self.repo.search(Selection.parse('videos',{'channel':channel,'type':'short'}))
        self.assertTrue(result['rows'])
        self.assertTrue(all(row['channel_id']==channel and row['type']=='short' for row in result['rows']))
        result = self.repo.search(Selection.parse('comments',{'channel':channel,'pinned':'1'}))
        self.assertTrue(all(row['channel_id']==channel and row['is_pinned'] for row in result['rows']))
        self.assertEqual(len(self.repo.search(Selection.parse('comments',{'video':video}))['rows']),1)
        detail = self.repo.detail(Selection.parse('channels',{'detail':channel}))
        self.assertEqual(detail['saved_videos'],58)
        detail = self.repo.detail(Selection.parse('videos',{'detail':video}))
        self.assertEqual(detail['saved_comments'],1)
        with self.assertRaises(LookupError):
            self.repo.context(Selection.parse('comments',{'video':video,'channel':'UC'+'0'*21+'1'}))
        self.assertEqual(self.repo.totals()['comments'],115)

    def test_phrase_search_scans_past_nonmatching_batches_and_preserves_pagination(self):
        self.pool.execute('''INSERT INTO public.comments(video_id,comment_id,text,author_name)
            SELECT 'V0000000000','batch-'||lpad(n::text,4,'0'),
            CASE WHEN n>=1000 THEN 'alpha blue quiet river' ELSE 'alpha blue fast quiet river' END,
            'Batch author' FROM generate_series(0,1149) n''')
        for sort in ('video','author'):
            query = 'alpha "blue quiet river"'
            first = self.repo.search(Selection.parse('comments',{'q':query,'sort':sort}))
            second = self.repo.search(selection_url(first['next']))
            third = self.repo.search(selection_url(second['next']))
            self.assertEqual([row['comment_id'] for row in first['rows']+second['rows']+third['rows']],
                             [f'batch-{n:04}' for n in range(1000,1150)])
            self.assertIsNone(third['next'])
            back = self.repo.search(selection_url(second['previous']))
            self.assertEqual(back['rows'],first['rows'])
        # Preserve PostgreSQL's punctuation, OR, and negative-phrase semantics.
        for query in ('"blue quiet" OR "missing phrase"','alpha -"blue quiet river"','"blue river quiet"'):
            expected = self.pool.execute('''SELECT video_id,comment_id FROM public.comments WHERE
                public.media_search_vector(text || ' ' || coalesce(author_name,'')) @@
                websearch_to_tsquery('simple',public.media_search_normalize(%s)) ORDER BY video_id,comment_id LIMIT 50''',(query,)).fetchall()
            actual = self.repo.search(Selection.parse('comments',{'q':query}))['rows']
            self.assertEqual([(row['video_id'],row['comment_id']) for row in actual],
                             [(row['video_id'],row['comment_id']) for row in expected])

    def login(self, client):
        response = client.get('/login')
        csrf = re.search(r'name="csrf" value="([^"]+)"',response.text).group(1)
        response = client.post('/login',data=dict(username='tester',password='test-password',csrf=csrf,next='/channels'))
        self.assertEqual(response.status_code,200)
        return response

    def test_http_auth_csrf_htmx_details_and_html_escaping(self):
        with TestClient(create_app(self.settings,self.repo)) as client:
            response = client.get('/comments',follow_redirects=False)
            self.assertEqual(response.status_code,303)
            response = client.get('/comments',headers={'HX-Request':'true'})
            self.assertEqual(response.status_code,401)
            self.assertIn('/login',response.headers['HX-Redirect'])
            self.assertEqual(client.post('/login',data=dict(username='tester',password='test-password')).status_code,403)
            response = self.login(client)
            self.assertTrue(any('httponly' in hop.headers.get('set-cookie','').lower() for hop in response.history))
            self.assertIn("script-src 'self'",response.headers['content-security-policy'])
            response = client.get('/comments?q=blue',headers={'HX-Request':'true'})
            self.assertEqual(response.status_code,200)
            self.assertNotIn('<!doctype',response.text)
            self.assertIn('<mark>blue</mark>',response.text)
            self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt;',response.text)
            self.assertNotIn('<script>alert(1)</script>',response.text)
            response = client.get('/channels?q=collection')
            self.assertIn('Keywords: <mark>collection</mark>',response.text)
            response = client.get('/comments?detail=shared-comment&detail_video=V0000000000',headers={'HX-Request':'true','HX-Target':'detail-layer'})
            self.assertIn('id="record-drawer"',response.text)
            self.assertNotIn('id="workspace"',response.text)
            response = client.get('/comments?detail=missing&detail_video=V0000000000',headers={'HX-Request':'true','HX-Target':'detail-layer'})
            self.assertEqual(response.status_code,404)
            self.assertNotIn('id="workspace"',response.text)
            response = client.get('/videos?channel=broken',headers={'HX-Request':'true'})
            self.assertEqual(response.status_code,400)
            self.assertIn('Invalid channel filter.',response.text)
            self.assertEqual(client.post('/logout',data=dict(csrf='bad')).status_code,403)
            response = client.get('/channels')
            csrf = re.search(r'name="csrf" value="([^"]+)"',response.text).group(1)
            self.assertEqual(client.post('/logout',data=dict(csrf=csrf),follow_redirects=False).status_code,303)
            self.assertEqual(client.get('/videos',follow_redirects=False).status_code,303)

    def test_public_access_search_pagination_and_details_without_session(self):
        settings = Settings(secret=SECRET,auth_required=False)
        with TestClient(create_app(settings,self.repo)) as client:
            for kind in SORTS:
                response = client.get('/'+kind,follow_redirects=False)
                self.assertEqual(response.status_code,200)
                self.assertNotIn('Sign out',response.text)
                self.assertNotIn('class="account"',response.text)
                self.assertNotIn('set-cookie',response.headers)
                next_page = self.repo.search(Selection(kind=kind,sort=next(iter(SORTS[kind]))))['next']
                self.assertEqual(client.get(next_page).status_code,200)
            response = client.get('/comments?q=%22blue+quiet+river%22',headers={'HX-Request':'true'})
            self.assertEqual(response.status_code,200)
            self.assertIn('<mark>blue quiet river</mark>',response.text)
            self.assertIn('&lt;script&gt;alert(1)&lt;/script&gt;',response.text)
            self.assertNotIn('HX-Redirect',response.headers)
            response = client.get('/comments?detail=shared-comment&detail_video=V0000000000',
                                  headers={'HX-Request':'true','HX-Target':'detail-layer'})
            self.assertEqual(response.status_code,200)
            self.assertIn('id="record-drawer"',response.text)
            for method, path, destination in (('GET','/login?next=/videos','/videos'),
                                               ('POST','/login','/channels'),('POST','/logout','/channels')):
                response = client.request(method,path,follow_redirects=False)
                self.assertEqual(response.status_code,303)
                self.assertEqual(response.headers['location'],destination)
            self.assertFalse(client.cookies)


if __name__ == '__main__':
    unittest.main()
