"""Migration preservation and replay safety without separate check fields."""
from datetime import datetime, timedelta, timezone
import importlib.util
import os
from pathlib import Path
import unittest
import uuid

import psycopg
from psycopg.rows import dict_row

from database_helpers import connect_test_database
from proxy_catalog import load_catalog
from proxy_statistics import Aggregate, AttemptOutcome, ProxyTarget, StatisticsBatch, write_batch


ROOT = Path(__file__).resolve().parents[1]
T = datetime(2026, 1, 1, tzinfo=timezone.utc)
REMOVED = {'youtube_last_checked_at', 'youtube_last_check_duration_ms'}
spec = importlib.util.spec_from_file_location('check_fields_importer', ROOT / 'proxy-tester/import_results.py')
importer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(importer)


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE') == '1', 'Opt in to rollback-only database tests')
class CheckFieldRemovalDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect_test_database("proxy")
        self.schema = 'test_check_fields_' + uuid.uuid4().hex
        self.conn.execute('CREATE SCHEMA ' + self.schema)
        self.conn.execute('SET LOCAL search_path TO ' + self.schema + ',public')

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def ddl(self, path, schema=None):
        text = '\n'.join(line for line in path.read_text().splitlines()
                         if line.strip() not in ('BEGIN;', 'COMMIT;'))
        self.conn.execute(text.replace('public.', (schema or self.schema) + '.'))

    def fresh(self):
        self.ddl(ROOT / 'db/proxy/schema.sql')

    def proxy(self, number=1):
        return self.conn.execute('''INSERT INTO proxies
            (connection_key,address,port,last_seen_at) VALUES (%s,%s,8080,%s) RETURNING proxy_id''',
            (bytes([number]) * 32, f'8.8.8.{number}', T)).fetchone()[0]

    def rows(self, table):
        with self.conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute('SELECT * FROM ' + table + ' ORDER BY 1')
            return cursor.fetchall()

    def journal_result(self, identifier, number, at, key, *, attempted=True, responds=True):
        importer.prepare_stage(self.conn)
        importer.stage_rows(self.conn, [(identifier, bytes([number]) * 32, at,
            'responds' if responds else ('not_responding' if attempted else 'invalid_configuration'),
            attempted, responds, 'unknown', 'socks5' if responds else None,
            200 if responds else None, 20, int(responds), responds,
            'connect:timeout' if attempted and not responds else None,
            None if responds else ('connect:timeout' if attempted else 'configuration:invalid'))])
        return importer.apply_staged(self.conn, key)

    def batch(self, identifier, number, at):
        aggregate = Aggregate()
        aggregate.add(AttemptOutcome(at, True, 200, True), 'socks5')
        return StatisticsBatch({ProxyTarget.from_catalog(identifier, bytes([number]) * 32, 'socks5'): aggregate})

    def test_migration_drops_only_two_fields_and_matches_fresh_schema(self):
        for path in sorted((ROOT / 'db/proxy/migrations').glob('00[1-9]_*.sql')):
            self.ddl(path)
        ids = [self.proxy(n) for n in range(1, 5)]
        for identifier in (ids[0], ids[2]):
            self.conn.execute('''INSERT INTO proxy_stats
                (proxy_id,connection_attempts,successful_connections,last_connection_attempt_at,last_connected_at,
                 working_protocol,youtube_last_checked_at,youtube_last_attempt_at,youtube_last_http_status,
                 youtube_last_check_duration_ms,youtube_last_response_at,youtube_requests_sent,youtube_responses_received,
                 youtube_successful_data_received,youtube_weighted_attempts,youtube_weighted_successful_data_received,
                 youtube_last_scored_attempt_at,youtube_last_import_key)
                VALUES (%s,3,2,%s,%s,'socks5',%s,%s,200,45.6,%s,2,2,1,2,1,%s,%s)''',
                (identifier, T, T, T, T, T, T, bytes([identifier]) * 32))
        self.conn.execute('''UPDATE proxy_stats SET youtube_last_checked_at=%s,
            youtube_last_error='configuration:invalid' WHERE proxy_id=%s''', (T + timedelta(days=1), ids[2]))
        self.conn.execute('''INSERT INTO proxy_stats
            (proxy_id,youtube_last_checked_at,youtube_last_check_duration_ms,youtube_last_error,youtube_last_import_key)
            VALUES (%s,%s,0.1,'configuration:invalid',%s)''', (ids[1], T, b'r' * 32))
        self.conn.execute('INSERT INTO proxy_stats(proxy_id) VALUES (%s)', (ids[3],))
        self.conn.execute("INSERT INTO proxy_lists(url,kind) VALUES ('https://test.invalid/list','feed_candidate')")
        before = {table: self.rows(table) for table in ('proxies', 'proxy_stats', 'proxy_lists')}
        sequence = self.conn.execute('SELECT last_value,is_called FROM proxies_proxy_id_seq').fetchone()
        self.ddl(ROOT / 'db/proxy/migrations/010_drop_youtube_check_fields.sql')
        for row in before['proxy_stats']:
            for name in REMOVED:
                del row[name]
        for table, expected in before.items():
            self.assertEqual(self.rows(table), expected)
        self.assertEqual(self.conn.execute('SELECT last_value,is_called FROM proxies_proxy_id_seq').fetchone(), sequence)
        self.assertEqual(self.conn.execute('SELECT youtube_responded FROM proxy_health ORDER BY proxy_id').fetchall(),
                         [(True,), (None,), (True,), (None,)])
        twin = self.schema + '_fresh'
        self.conn.execute('CREATE SCHEMA ' + twin)
        self.ddl(ROOT / 'db/proxy/schema.sql', twin)
        query = '''SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns
            WHERE table_schema=%s ORDER BY table_name,column_name'''
        self.assertEqual(self.conn.execute(query, (self.schema,)).fetchall(),
                         self.conn.execute(query, (twin,)).fetchall())
        columns = dict(self.conn.execute('''SELECT table_name,count(*) FROM information_schema.columns
            WHERE table_schema=%s AND table_name IN ('proxies','proxy_stats','proxy_lists') GROUP BY table_name''',
            (self.schema,)))
        self.assertEqual(columns, {'proxies': 6, 'proxy_stats': 18, 'proxy_lists': 8})

    def test_attempt_replays_stay_blocked_after_a_configuration_rejection(self):
        self.fresh()
        identifier = self.proxy()
        self.assertEqual(self.journal_result(identifier, 1, T, b'a' * 32), 1)
        self.assertEqual(self.journal_result(identifier, 1, T + timedelta(days=1), b'r' * 32,
                                             attempted=False, responds=False), 1)
        before = self.rows('proxy_stats')
        with self.assertRaisesRegex(ValueError, 'older or changed'):
            self.journal_result(identifier, 1, T, b'a' * 32)
        self.assertEqual(write_batch(self.conn, self.batch(identifier, 1, T))['stale_attempts'], 1)
        self.assertEqual(self.rows('proxy_stats'), before)
        self.assertEqual(before[0]['youtube_last_attempt_at'], T)
        self.assertEqual(before[0]['connection_attempts'], 1)
        newer = self.batch(identifier, 1, T + timedelta(days=2))
        self.assertEqual(write_batch(self.conn, newer)['updated_proxies'], 1)
        self.assertEqual(write_batch(self.conn, newer)['replayed_attempts'], 1)
        self.assertEqual(self.rows('proxy_stats')[0]['connection_attempts'], 2)

    def test_rejection_before_any_attempt_allows_first_attempt_and_protects_its_retry(self):
        self.fresh()
        identifier = self.proxy()
        self.assertEqual(self.journal_result(identifier, 1, T, b'r' * 32, attempted=False, responds=False), 1)
        self.assertEqual(self.journal_result(identifier, 1, T, b'r' * 32, attempted=False, responds=False), 0)
        row = self.rows('proxy_stats')[0]
        self.assertIsNone(row['youtube_last_attempt_at'])
        self.assertEqual(row['connection_attempts'], 0)
        value = self.batch(identifier, 1, T + timedelta(days=1))
        self.assertEqual(write_batch(self.conn, value)['updated_proxies'], 1)
        self.assertEqual(write_batch(self.conn, value)['replayed_attempts'], 1)
        self.assertEqual(self.rows('proxy_stats')[0]['connection_attempts'], 1)
        with self.assertRaisesRegex(ValueError, 'older or changed'):
            self.journal_result(identifier, 1, T, b'r' * 32, attempted=False, responds=False)

    def test_catalog_order_uses_response_status_then_recency_then_identity(self):
        self.fresh()
        ids = [self.proxy(n) for n in range(1, 5)]
        for number, identifier in enumerate(ids, 1):
            self.journal_result(identifier, number, T + timedelta(hours=number // 2), bytes([number]) * 32)
        self.journal_result(ids[3], 4, T + timedelta(days=1), b'f' * 32, responds=False)
        selected = load_catalog(connection=self.conn)
        self.assertEqual([proxy.proxy_id for proxy in selected], [ids[1], ids[2], ids[0], ids[3]])


if __name__ == '__main__':
    unittest.main()
