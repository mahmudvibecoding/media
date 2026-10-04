"""Shared transport metadata, website errors and migration preservation."""
from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import unittest
import uuid

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from database_helpers import connect_test_database
from proxy_formats import Proxy, canonical_json, pack_connection_settings, unpack_connection_settings
from proxy_statistics import Aggregate, AttemptOutcome, ProxyTarget, StatisticsBatch, proxy_connection_error, write_batch


ROOT = Path(__file__).resolve().parents[1]
T = datetime(2026, 1, 1, tzinfo=timezone.utc)


class ConfigurationTests(unittest.TestCase):
    def test_transport_and_options_round_trip_without_changing_identity(self):
        for protocol, options in (
            ('unknown', {}),
            ('http', {'username': 'fixture', 'password': 'test'}),
            ('vless', {'uuid': '622693d9-5812-4542-8344-a32bbb5bfbcd', 'type': 'ws'}),
            ('vmess', {'transport': 'original option', 'options': {'nested': 1}}),
            ('shadowsocks', {'binary\x00key': '\x16\x03\x00', 'nested': {'x': [1, True, None]}}),
        ):
            with self.subTest(protocol=protocol):
                proxy = Proxy('8.8.8.8', 443, protocol, options)
                configuration = pack_connection_settings(protocol, options)
                actual_protocol, actual_options = unpack_connection_settings(configuration)
                self.assertEqual((actual_protocol, actual_options), (protocol, options))
                self.assertEqual(Proxy(proxy.address, proxy.port, actual_protocol, actual_options).key, proxy.key)

    def test_malformed_configuration_is_rejected_without_exposing_options(self):
        for value in (None, {}, {'transport': '', 'options': {}},
                      {'transport': 'vless', 'options': 'private-invalid-json'},
                      {'transport': 'vless', 'options': []}):
            with self.subTest(value_type=type(value).__name__), self.assertRaises(ValueError) as error:
                unpack_connection_settings(value)
            self.assertNotIn('private-invalid-json', str(error.exception))


class ErrorSeparationTests(unittest.TestCase):
    def test_target_errors_do_not_set_shared_connection_error(self):
        for label in ('youtube_https:timeout', 'youtube_body:timeout', 'proxy_tunnel:dns_error',
                      'proxy_handshake:proxy_http_403', 'proxy_handshake:socks5_reply_5',
                      'proxy_handshake:timeout'):
            with self.subTest(label=label):
                aggregate = Aggregate()
                aggregate.add(AttemptOutcome(T, False, None, False, True, label), 'http')
                self.assertIsNone(aggregate.last_connection_error)
                self.assertEqual(aggregate.last_website_error, label)
                self.assertEqual(aggregate.successful_connections, 1)

    def test_proxy_endpoint_tls_and_authentication_errors_remain_shared(self):
        for label in ('connect:connection_refused', 'resolve:dns_not_found', 'proxy_tls:timeout',
                      'proxy_handshake:proxy_http_407', 'proxy_handshake:proxy_authentication_failed',
                      'proxy_handshake:not_socks5', 'proxy_handshake:socks4_reply_92'):
            with self.subTest(label=label):
                self.assertEqual(proxy_connection_error(label), label)


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE') == '1', 'Opt in to rollback-only database tests')
class SharedProtocolDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect_test_database("proxy")
        self.schema = 'test_shared_protocol_' + uuid.uuid4().hex
        self.conn.execute('CREATE SCHEMA ' + self.schema)
        self.conn.execute('SET LOCAL search_path TO ' + self.schema + ',public')

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def ddl(self, text):
        text = '\n'.join(line for line in text.splitlines() if line.strip() not in ('BEGIN;', 'COMMIT;'))
        self.conn.execute(text.replace('public.', self.schema + '.'))

    def rows(self, table):
        with self.conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute('SELECT * FROM ' + table + ' ORDER BY proxy_id')
            return cursor.fetchall()

    def test_migration_preserves_ids_keys_options_counters_and_website_errors(self):
        for path in sorted((ROOT / 'db/proxy/migrations').glob('00[1-8]_*.sql')):
            self.ddl(path.read_text())
        cases = [('unknown', {}), ('vless', {'uuid': 'fixture', 'binary\x00key': '\x00'}),
                 ('https', {'username': 'fixture', 'password': 'test'})]
        identifiers = []
        for protocol, options in cases:
            proxy = Proxy('8.8.8.8', 8080, protocol, options)
            encoded = canonical_json(options)
            identifier = self.conn.execute('''INSERT INTO proxies
                (connection_key,address,port,protocol,connection_settings,last_seen_at)
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING proxy_id''',
                (proxy.key, proxy.address, proxy.port, protocol,
                 Jsonb(encoded if '\\u0000' in encoded else options), T)).fetchone()[0]
            identifiers.append(identifier)
        self.conn.execute('''INSERT INTO proxy_stats
            (proxy_id,connection_attempts,successful_connections,last_connection_attempt_at,last_connected_at,
             youtube_last_checked_at,youtube_last_attempt_at,youtube_working_protocol,youtube_last_http_status,
             youtube_last_response_at,youtube_requests_sent,youtube_responses_received,
             youtube_successful_data_received,youtube_weighted_attempts,youtube_weighted_successful_data_received,
             youtube_last_scored_attempt_at,youtube_last_import_key)
            VALUES (%s,5,4,%s,%s,%s,%s,'socks5',200,%s,3,2,1,2.5,0.75,%s,%s)''',
            (identifiers[0], T, T, T, T, T, T, b'x' * 32))
        for identifier, label in zip(identifiers[1:],
                                     ('youtube_https:timeout', 'proxy_handshake:proxy_http_407')):
            self.conn.execute('''INSERT INTO proxy_stats
                (proxy_id,connection_attempts,successful_connections,last_connection_attempt_at,last_connected_at,
                 last_connection_error,youtube_last_checked_at,youtube_last_attempt_at,youtube_last_import_key)
                VALUES (%s,1,1,%s,%s,%s,%s,%s,%s)''', (identifier, T, T, label, T, T, b'y' * 32))
        before_proxies, before_stats = self.rows('proxies'), self.rows('proxy_stats')
        sequence = self.conn.execute('SELECT last_value,is_called FROM proxies_proxy_id_seq').fetchone()
        self.ddl((ROOT / 'db/proxy/migrations/009_shared_proxy_protocol.sql').read_text())
        for before, after in zip(before_proxies, self.rows('proxies')):
            protocol = before.pop('protocol')
            options = before.pop('connection_settings')
            self.assertEqual(after.pop('connection_settings'), {'transport': protocol, 'options': options})
            self.assertEqual(after, before)
        for before, after in zip(before_stats, self.rows('proxy_stats')):
            before['working_protocol'] = before.pop('youtube_working_protocol')
            if before['last_connection_error'] == 'youtube_https:timeout':
                before['youtube_last_error'] = before['last_connection_error']
                before['last_connection_error'] = None
            self.assertEqual(after, before)
        self.assertEqual(self.conn.execute('SELECT last_value,is_called FROM proxies_proxy_id_seq').fetchone(), sequence)
        columns = dict(self.conn.execute('''SELECT table_name,count(*) FROM information_schema.columns
            WHERE table_schema=%s AND table_name IN ('proxies','proxy_stats','proxy_lists') GROUP BY table_name''',
            (self.schema,)))
        self.assertEqual(columns, {'proxies': 6, 'proxy_stats': 20, 'proxy_lists': 8})
        names = {row[0] for row in self.conn.execute('''SELECT column_name FROM information_schema.columns
            WHERE table_schema=%s AND table_name IN ('proxy_health','proxy_catalog')''', (self.schema,))}
        self.assertFalse(names & {'protocol', 'declared_protocol', 'youtube_working_protocol'})
        self.assertIn('working_protocol', names)

    def test_shared_protocol_survives_website_failures_and_authentication_is_separate(self):
        self.ddl((ROOT / 'db/proxy/schema.sql').read_text())
        proxy = Proxy('8.8.8.8', 8080, 'http', {})
        identifier = self.conn.execute('''INSERT INTO proxies
            (connection_key,address,port,connection_settings,last_seen_at) VALUES (%s,%s,%s,%s,%s) RETURNING proxy_id''',
            (proxy.key, proxy.address, proxy.port, Jsonb(pack_connection_settings(proxy.protocol, proxy.settings)), T)).fetchone()[0]
        target = ProxyTarget.from_catalog(identifier, proxy.key, 'http')
        observations = [
            AttemptOutcome(T, True, 200, True, True),
            AttemptOutcome(T + timedelta(seconds=1), False, None, False, True,
                           website_error='youtube_https:timeout'),
            AttemptOutcome(T + timedelta(seconds=2), False, None, False, True,
                           'proxy_handshake:proxy_http_407'),
            AttemptOutcome(T + timedelta(seconds=3), True, 403, False, True,
                           website_error='http:http_403'),
        ]
        for index, observation in enumerate(observations):
            aggregate = Aggregate()
            aggregate.add(observation, target.protocol)
            value = StatisticsBatch({target: aggregate})
            write_batch(self.conn, value)
            write_batch(self.conn, value)
            row = self.conn.execute('''SELECT working_protocol,last_connection_error,youtube_last_error,
                connection_attempts,successful_connections FROM proxy_stats''').fetchone()
            self.assertEqual(row[0], 'http')
            self.assertEqual(row[1], observation.connection_error)
            self.assertEqual(row[2], observation.website_error or observation.connection_error)
            self.assertEqual(row[3:], (index + 1, index + 1))
        self.assertEqual(self.conn.execute('SELECT count(*) FROM proxy_stats').fetchone()[0], 1)
        with self.assertRaises(psycopg.errors.CheckViolation), self.conn.transaction():
            self.conn.execute("UPDATE proxy_stats SET last_connection_error='youtube_https:timeout'")


if __name__ == '__main__':
    unittest.main()
