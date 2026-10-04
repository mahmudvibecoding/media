"""Rollback-only tests for destructive schema consolidation and journal retries."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import psycopg
from psycopg.types.json import Jsonb
from database_helpers import connect_test_database

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('proxy_importer',ROOT/'proxy-tester/import_results.py')
importer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(importer)
spec = importlib.util.spec_from_file_location('proxy_connection_backfill',ROOT/'proxy-tester/backfill_connections.py')
backfill = importlib.util.module_from_spec(spec)
with patch.dict('sys.modules',{'import_results':importer}):
    spec.loader.exec_module(backfill)


class ConnectionJournalTests(unittest.TestCase):
    def test_multiple_protocol_dials_count_as_one_connected_check(self):
        result={'attempted':True,'responds':False,'attempts':[
            {'connected':True,'stage':'proxy_handshake','error_code':'eof'},
            {'connected':True,'stage':'proxy_handshake','error_code':'eof'},
            {'connected':False,'stage':'connect','error_code':'timeout'}]}
        self.assertEqual(importer.connection_observation(result),(True,'connect:timeout'))

    def test_http_response_and_body_format_errors_are_not_connection_errors(self):
        for body_error in (None,'gzip_decode_error','body_too_large','timeout'):
            result={'attempted':True,'responds':True,'attempts':[
                {'status':'responds','connected':True,'request_sent':True,
                 'http_status':403,'body_error':body_error}]}
            self.assertEqual(importer.connection_observation(result),(True,None))
            self.assertEqual(importer.website_error(result,None),
                             'youtube_body:'+body_error if body_error else 'http:http_403')

    def test_unknown_observation_and_invalid_connection_flags(self):
        result={'attempted':True,'responds':False,'attempts':[{}]}
        self.assertEqual(importer.connection_observation(result),(False,None))
        for observation in ({'connected':'true'}, {'connected':False,'request_sent':True},
                            {'stage':'connect','error_code':'raw text with credentials'}):
            result['attempts']=[observation]
            with self.assertRaises(ValueError):
                importer.connection_observation(result)


def ddl(conn, schema, text):
    text = '\n'.join(line for line in text.splitlines() if line.strip() not in ('BEGIN;','COMMIT;'))
    conn.execute(text.replace('public.',schema+'.'))


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE') == '1','Opt in to rollback-only integration tests')
class CompactionTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect_test_database("proxy")
        self.schema = 'test_compact_' + uuid.uuid4().hex
        self.conn.execute('CREATE SCHEMA ' + self.schema)
        self.conn.execute('SET LOCAL search_path TO ' + self.schema + ',public')

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def fresh(self):
        ddl(self.conn,self.schema,(ROOT/'db/proxy/schema.sql').read_text())

    def proxy(self, legacy=False):
        if legacy:
            return self.conn.execute("""INSERT INTO proxies
                (connection_key,address,port,protocol,first_seen_at,last_seen_at)
                VALUES (%s,'8.8.8.8',8080,'unknown','2026-01-01Z','2026-01-02Z') RETURNING proxy_id""",
                (b'x'*32,)).fetchone()[0]
        return self.conn.execute("""INSERT INTO proxies
            (connection_key,address,port,last_seen_at)
            VALUES (%s,'8.8.8.8',8080,'2026-01-02Z') RETURNING proxy_id""",
            (b'x'*32,)).fetchone()[0]

    def journal(self, folder, proxy_id, day=3, responds=True, bad_summary=False, bad_key=False):
        path = Path(folder)/f'result-{day}.jsonl'
        result = dict(id=proxy_id,key=(b'y'*32 if bad_key else b'x'*32).hex(),
                      tested_at=f'2026-01-{day:02d}T00:00:00Z',status='responds' if responds else 'not_responding',
                      attempted=True,responds=responds,declared_protocol='unknown',
                      detected_protocol='socks5' if responds else None,total_ms=20,
                      attempts=[dict(status='responds' if responds else 'not_responding',request_sent=True,
                                     tls_verified=True,http_status=200)])
        path.write_text(json.dumps(result)+'\n')
        Path(str(path)+'.meta.json').write_text(json.dumps(dict(run_id=f'test-{day}')))
        summary=dict(state='complete',counters=dict(completed=2 if bad_summary else 1,attempted=1,youtube_responses=int(responds)))
        Path(str(path)+'.summary.json').write_text(json.dumps(summary))
        return path

    def test_import_retry_stale_rejection_and_counters(self):
        self.fresh()
        proxy_id = self.proxy()
        with tempfile.TemporaryDirectory() as folder:
            first = self.journal(folder,proxy_id)
            self.assertEqual(importer.import_journal(self.conn,first,1)['new_results'],1)
            self.assertEqual(importer.import_journal(self.conn,first,1)['new_results'],0)
            second = self.journal(folder,proxy_id,day=4,responds=False)
            self.assertEqual(importer.import_journal(self.conn,second,1)['new_results'],1)
            row = self.conn.execute('SELECT connection_attempts,youtube_requests_sent,youtube_responses_received,youtube_responded,working_protocol FROM proxy_health').fetchone()
            self.assertEqual(row,(2,2,1,False,'socks5'))
            with self.assertRaisesRegex(ValueError,'older or changed'):
                importer.import_journal(self.conn,first,1)
            self.assertEqual(self.conn.execute('SELECT connection_attempts FROM proxy_stats').fetchone()[0],2)

    def test_connection_backfill_preserves_old_fields_and_replays_without_adding_counts(self):
        self.fresh()
        proxy_id=self.proxy()
        with tempfile.TemporaryDirectory() as folder, patch('builtins.print'):
            first=self.journal(folder,proxy_id)
            second=self.journal(folder,proxy_id,day=4,responds=False)
            result=json.loads(second.read_text())
            result['attempts']=[{'status':'not_responding','connected':True,'stage':'proxy_handshake','error_code':'eof'},
                                {'status':'not_responding','connected':False,'stage':'connect','error_code':'timeout'}]
            second.write_text(json.dumps(result)+'\n')
            for path in (first,second):
                importer.import_journal(self.conn,path,1)
            self.conn.execute('UPDATE proxy_stats SET successful_connections=0,last_connected_at=NULL,last_connection_error=NULL')
            columns=[row[0] for row in self.conn.execute('''SELECT column_name FROM information_schema.columns
                WHERE table_schema=%s AND table_name='proxy_stats' AND column_name NOT IN
                ('successful_connections','last_connected_at','last_connection_error') ORDER BY ordinal_position''',(self.schema,))]
            preserved='SELECT '+','.join(columns)+' FROM proxy_stats'
            before=self.conn.execute(preserved).fetchone()
            reports=backfill.stage_history(self.conn,[first,second,first],1)
            self.assertEqual(len(reports),2)
            self.assertEqual(backfill.history_summary(self.conn)['successful_connections'],2)
            self.assertEqual(backfill.apply_backfill(self.conn)['updated_proxies'],1)
            self.assertEqual(self.conn.execute(preserved).fetchone(),before)
            self.assertEqual(self.conn.execute('SELECT successful_connections,last_connected_at=last_connection_attempt_at,last_connection_error FROM proxy_stats').fetchone(),(2,True,'connect:timeout'))
            self.assertEqual(backfill.apply_backfill(self.conn)['updated_proxies'],0)
            # A newer scored data response proves one further connection.
            self.conn.execute("""UPDATE proxy_stats SET youtube_last_response_at='2026-01-05Z',
                youtube_last_attempt_at='2026-01-05Z',last_connection_attempt_at='2026-01-05Z',
                last_connected_at='2026-01-05Z',last_connection_error=NULL,
                connection_attempts=connection_attempts+1,successful_connections=successful_connections+1,
                youtube_requests_sent=youtube_requests_sent+1,youtube_responses_received=youtube_responses_received+1,
                youtube_successful_data_received=1,youtube_weighted_attempts=1,
                youtube_weighted_successful_data_received=1,youtube_last_scored_attempt_at='2026-01-05Z',
                youtube_last_http_status=200""")
            before=self.conn.execute('SELECT * FROM proxy_stats').fetchone()
            self.assertEqual(backfill.apply_backfill(self.conn)['updated_proxies'],0)
            self.assertEqual(self.conn.execute('SELECT * FROM proxy_stats').fetchone(),before)
            # Initial migration also includes sent requests recorded after the journals.
            self.conn.execute('UPDATE proxy_stats SET successful_connections=0,last_connected_at=NULL')
            self.assertEqual(backfill.apply_backfill(self.conn)['updated_proxies'],1)
            self.assertEqual(self.conn.execute('SELECT * FROM proxy_stats').fetchone(),before)

    def test_connection_backfill_rejects_unimported_or_partly_initialized_history(self):
        self.fresh()
        proxy_id=self.proxy()
        with tempfile.TemporaryDirectory() as folder, patch('builtins.print'):
            first=self.journal(folder,proxy_id)
            second=self.journal(folder,proxy_id,day=4)
            importer.import_journal(self.conn,first,1)
            backfill.stage_history(self.conn,[first,second],1)
            with self.assertRaisesRegex(ValueError,'unimported history'):
                backfill.apply_backfill(self.conn)
            importer.import_journal(self.conn,second,1)
            self.conn.execute('UPDATE proxy_stats SET successful_connections=1')
            before=self.conn.execute('SELECT * FROM proxy_stats').fetchone()
            with self.assertRaisesRegex(ValueError,'before initialization'):
                backfill.apply_backfill(self.conn)
            self.assertEqual(self.conn.execute('SELECT * FROM proxy_stats').fetchone(),before)

    def test_invalid_complete_input_cannot_partially_update(self):
        self.fresh()
        proxy_id = self.proxy()
        with tempfile.TemporaryDirectory() as folder:
            for options in ({'bad_summary':True},{'bad_key':True}):
                with self.assertRaises(ValueError):
                    importer.import_journal(self.conn,self.journal(folder,proxy_id,**options),1)
                self.assertEqual(self.conn.execute('SELECT count(*) FROM proxy_stats').fetchone()[0],0)
            path = self.journal(folder,proxy_id)
            # A late duplicate violates the staging PK before persistent counters change.
            path.write_text(path.read_text()*2)
            with self.assertRaises(psycopg.errors.UniqueViolation), self.conn.transaction():
                importer.import_journal(self.conn,path,1)
            self.assertEqual(self.conn.execute('SELECT count(*) FROM proxy_stats').fetchone()[0],0)

    def test_migration_preserves_failed_latest_and_last_success(self):
        for name in ('001_proxy_lists.sql','002_proxy_entries.sql','003_proxy_tests.sql'):
            ddl(self.conn,self.schema,(ROOT/'db/proxy/migrations'/name).read_text())
        proxy_id = self.proxy(legacy=True)
        self.conn.execute("UPDATE proxies SET tested_at='2026-01-04Z'")
        self.conn.execute("INSERT INTO proxy_sources (source_key,name,url,kind) VALUES ('fixture','fixture','https://test.invalid','test')")
        self.conn.execute("INSERT INTO proxy_lists (url,source_key,kind,protocol_hints) VALUES ('https://test.invalid/list','fixture','feed_candidate',ARRAY['socks5'])")
        self.conn.execute("""INSERT INTO proxy_list_checks
            (list_url,checked_at,response_status,is_html,credentials_used,listed_proxy_used,details)
            VALUES ('https://test.invalid/list',now(),'proxy_entries_observed',false,false,false,'{}')""")
        run = uuid.uuid4()
        self.conn.execute("INSERT INTO proxy_collection_runs (run_id,status,selected_count,settings) VALUES (%s,'completed',1,'{}')",(run,))
        self.conn.execute("""INSERT INTO proxy_list_downloads (run_id,list_url,status,finished_at,payload_path,content_sha256)
            VALUES (%s,'https://test.invalid/list','collected','2026-01-02Z','/fixture.gz','abc')""",(run,))
        ids=[]
        for day, responds in ((3,True),(4,False)):
            rid=self.conn.execute("""INSERT INTO proxy_test_runs (run_key,metadata,summary,source_host,journal_sha256,status,imported_count)
                VALUES (%s,'{}','{}','fixture',%s,'complete',1) RETURNING test_run_id""",(str(day),bytes([day])*32)).fetchone()[0]
            ids.append(rid)
            self.conn.execute("""INSERT INTO proxy_test_results VALUES
                (%s,%s,%s,%s,true,%s,'unknown',%s,%s,20,%s)""",
                (rid,proxy_id,f'2026-01-{day:02d}Z','responds' if responds else 'not_responding',responds,
                 'socks5' if responds else None,200 if responds else None,Jsonb([{'request_sent':responds}])) )
        self.conn.execute("""INSERT INTO proxy_test_health VALUES
            (%s,%s,'2026-01-04Z','not_responding',true,false,NULL,NULL,20,'2026-01-03Z','2026-01-03Z',2,2,1)""",(proxy_id,ids[-1]))
        ddl(self.conn,self.schema,(ROOT/'db/proxy/migrations/004_compact_proxy_database.sql').read_text())
        self.assertEqual(self.conn.execute('SELECT checks,network_checks,requests_sent,successful_checks,working_protocol,responds FROM proxy_stats').fetchone(),(2,2,1,1,'socks5',False))
        self.assertEqual(self.conn.execute('SELECT last_import_key FROM proxy_stats').fetchone()[0],bytes([4])*32)
        self.assertEqual(self.conn.execute('SELECT enabled,run_id,status,fetch_state->>\'payload_path\' FROM proxy_lists').fetchone(),(True,run,'collected','/fixture.gz'))
        names = {r[0] for r in self.conn.execute("SELECT tablename FROM pg_tables WHERE schemaname=%s",(self.schema,))}
        self.assertEqual(names,{'proxies','proxy_stats','proxy_lists'})
        self.assertIsNotNone(self.conn.execute('SELECT tested_at FROM proxy_catalog').fetchone()[0])
        self.conn.execute("SELECT setval(pg_get_serial_sequence(%s,'proxy_id'),2000,true)",(self.schema+'.proxies',))
        # An untested configuration must survive even without a statistics row.
        self.conn.execute("""INSERT INTO proxies (connection_key,address,port,protocol,first_seen_at,last_seen_at)
            VALUES (%s,'1.1.1.1',8080,'unknown','2026-01-01Z','2026-01-02Z')""",(b'y'*32,))
        ddl(self.conn,self.schema,(ROOT/'db/proxy/migrations/005_reduce_proxy_columns.sql').read_text())
        self.assertEqual(self.conn.execute('SELECT network_checks,requests_sent,youtube_responses,working_protocol,status FROM proxy_stats').fetchone(),
                         (2,1,1,'socks5','not_responding'))
        self.assertEqual(self.conn.execute('SELECT count(*) FROM proxies').fetchone()[0],2)
        self.assertEqual(self.conn.execute('SELECT last_value,is_called FROM proxies_proxy_id_seq').fetchone(),(2001,True))
        self.assertEqual(self.conn.execute('SELECT attempted,youtube_responds,detected_protocol,working_protocol FROM proxy_health').fetchone(),
                         (True,False,None,'socks5'))
        column_counts=dict(self.conn.execute("""SELECT table_name,count(*) FROM information_schema.columns
            WHERE table_schema=%s AND table_name IN ('proxies','proxy_stats','proxy_lists') GROUP BY table_name""",(self.schema,)))
        self.assertEqual(column_counts,{'proxies':7,'proxy_stats':11,'proxy_lists':8})
        self.assertEqual(self.conn.execute("SELECT fetch_state->>'payload_path' FROM proxy_lists").fetchone()[0],'/fixture.gz')


    def test_source_import_keeps_current_collection_and_enable_setting(self):
        from import_proxy_lists import import_lists
        self.fresh()
        row = dict(url='https://test.invalid/list',kind='feed_candidate',protocol_hints=['http'],
                   data_observed_without_account=True,is_html=False)
        self.assertEqual(import_lists(self.conn,[row]),1)
        self.conn.execute("UPDATE proxy_lists SET enabled=false,status='collected',fetch_state=%s",
                          (Jsonb({'payload_path':'/fixture.gz'}),))
        self.assertEqual(import_lists(self.conn,[row]),0)
        row['protocol_hints'] = ['socks5']
        self.assertEqual(import_lists(self.conn,[row]),1)
        self.assertEqual(self.conn.execute("SELECT enabled,status,fetch_state->>'payload_path',protocol_hints FROM proxy_lists").fetchone(),
                         (False,'collected','/fixture.gz',['socks5']))

    def test_status_must_determine_removed_fields_before_import(self):
        self.fresh()
        proxy_id=self.proxy()
        with tempfile.TemporaryDirectory() as folder:
            changes=({'attempted':False},{'responds':False},{'status':'internal_error'},
                     {'status':'not_responding','responds':False,'detected_protocol':'socks5'})
            for change in changes:
                path=self.journal(folder,proxy_id)
                row=json.loads(path.read_text())
                row.update(change)
                path.write_text(json.dumps(row)+'\n')
                with self.assertRaisesRegex(ValueError,'Status must consistently'):
                    importer.import_journal(self.conn,path,1)
                self.assertEqual(self.conn.execute('SELECT count(*) FROM proxy_stats').fetchone()[0],0)

    def test_configuration_rejection_preserves_previous_success(self):
        self.fresh()
        proxy_id=self.proxy()
        with tempfile.TemporaryDirectory() as folder:
            first=self.journal(folder,proxy_id)
            importer.import_journal(self.conn,first,1)
            path=self.journal(folder,proxy_id,day=4,responds=False)
            result=json.loads(path.read_text())
            result.update(status='invalid_configuration',attempted=False,attempts=[
                dict(status='invalid_configuration',request_sent=False,tls_verified=False)])
            path.write_text(json.dumps(result)+'\n')
            sidecar=Path(str(path)+'.summary.json')
            summary=json.loads(sidecar.read_text())
            summary['counters']['attempted']=0
            sidecar.write_text(json.dumps(summary))
            self.assertEqual(importer.import_journal(self.conn,path,1)['new_results'],1)
            self.assertEqual(self.conn.execute('SELECT connection_attempts,youtube_requests_sent,youtube_responses_received,working_protocol FROM proxy_stats').fetchone(),
                             (1,1,1,'socks5'))
            self.assertEqual(self.conn.execute("SELECT youtube_last_attempt_at='2026-01-03Z',youtube_responded,working_protocol FROM proxy_health").fetchone(),
                             (True,True,'socks5'))
            self.assertEqual(importer.import_journal(self.conn,path,1)['new_results'],0)
            # Rejected configurations cannot make historical attempt times look stale.
            before=self.conn.execute('SELECT * FROM proxy_stats').fetchone()
            backfill.stage_history(self.conn,[first,path],1)
            self.assertEqual(backfill.apply_backfill(self.conn)['updated_proxies'],0)
            self.assertEqual(self.conn.execute('SELECT * FROM proxy_stats').fetchone(),before)

    def test_column_migration_rejects_inconsistent_previous_status(self):
        for name in ('001_proxy_lists.sql','002_proxy_entries.sql','003_proxy_tests.sql','004_compact_proxy_database.sql'):
            ddl(self.conn,self.schema,(ROOT/'db/proxy/migrations'/name).read_text())
        proxy_id=self.proxy(legacy=True)
        self.conn.execute("""INSERT INTO proxy_stats
            (proxy_id,checked_at,status,attempted,responds,total_ms,checks,network_checks,requests_sent,successful_checks,last_import_key)
            VALUES (%s,'2026-01-03Z','responds',false,false,0,1,0,0,0,%s)""",(proxy_id,b'z'*32))
        with self.assertRaisesRegex(psycopg.errors.RaiseException,'does not determine'),self.conn.transaction():
            ddl(self.conn,self.schema,(ROOT/'db/proxy/migrations/005_reduce_proxy_columns.sql').read_text())
        self.assertEqual(self.conn.execute('SELECT count(*) FROM proxy_stats').fetchone()[0],1)
        self.assertEqual(self.conn.execute('SELECT status,attempted FROM proxy_stats').fetchone(),('responds',False))


if __name__ == '__main__':
    unittest.main()
