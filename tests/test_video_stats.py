import asyncio
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import gzip
import json
import os
from pathlib import Path
import ssl
import tempfile
import unittest
from unittest.mock import MagicMock, patch
import uuid

import httpx
import psycopg
from database_helpers import connect_test_database

from collect_video_stats import (MAX_COUNT, ResponseShapeError, exact_count, fetch_stats,
                                 fetch_player_views, has_stats, parse_player_views, parse_stats, stats_error_reason)
from metadata_bulk import (atomic_json, claim, connect_queue, digest, export_events,
                           collector_functions, finish, initialize, queue_status)
from proxy_statistics import AttemptOutcome
from proxy_formats import pack_connection_settings
from video_stats_bulk import export_snapshot as export_statistics_snapshot
from video_stats_bulk_import import (apply_chunk, apply_statistics, read_chunk, statistics_batch,
                                    statistics_rows)

ROOT = Path(__file__).resolve().parents[1]
VIDEO = '1hzvCKusdpc'
AT = datetime(2026, 10, 3, tzinfo=timezone.utc)


def payload(video=VIDEO):
    return json.loads((ROOT / 'tests/fixtures/video_stats' / f'{video}.json').read_text())


def primary(data):
    return data['contents']['twoColumnWatchNextResults']['results']['results']['contents'][0]['videoPrimaryInfoRenderer']


def model(data):
    return primary(data)['videoActions']['menuRenderer']['topLevelButtons'][0][
        'segmentedLikeDislikeButtonViewModel']['likeButtonViewModel']['likeButtonViewModel'][
        'toggleButtonViewModel']['toggleButtonViewModel']['defaultButtonViewModel']['buttonViewModel']


def event(proxy_id=1):
    result = dict(parse_stats(payload(), VIDEO), video_id=VIDEO, status='ok', http_status=200)
    return {'video_id': VIDEO, 'at': AT.isoformat(), 'result': result,
            'proxy': {'id': proxy_id, 'key': (b'x' * 32).hex(), 'protocol': 'http'},
            'observation': asdict(AttemptOutcome(AT, True, 200, True))}


class SnapshotTests(unittest.TestCase):
    def test_snapshot_uses_verified_protocol_when_catalog_transport_is_generic(self):
        catalog, media = MagicMock(), MagicMock()
        catalog.execute.return_value.fetchall.return_value = [
            (1, b'x'*32, '127.0.0.1', 1080, pack_connection_settings('socks', {}))]
        cursor = media.cursor.return_value.__enter__.return_value
        cursor.__iter__.return_value = iter([(VIDEO, 'video', 0)])
        with tempfile.TemporaryDirectory() as directory:
            ranked = Path(directory)/'ranked.jsonl'
            ranked.write_text(json.dumps(dict(proxy_id=1, connection_key=(b'x'*32).hex(),
                protocol='socks5', score=3, checks=3, average_response_ms=100))+'\n')
            folder = Path(directory)/'run'
            with patch('discovery_storage.connect_database', return_value=nullcontext(catalog)), \
                 patch('video_stats_bulk.open_database', return_value=nullcontext(media)):
                manifest = export_statistics_snapshot(folder, ranked, missing_only=True)
            with gzip.open(folder/'proxies.jsonl.gz','rt') as source:
                proxy = json.loads(source.readline())
            self.assertEqual((proxy['protocol'],proxy['working_protocol']),('socks','socks5'))
            self.assertEqual(manifest['protocols'],{'socks5':1})
            self.assertEqual(manifest['videos'],1)
            self.assertIn('view_count IS NULL OR like_count IS NULL',cursor.execute.call_args.args[0])
            self.assertEqual(digest(folder/'proxies.jsonl.gz'),manifest['files']['proxies.jsonl.gz']['sha256'])
            cursor.__iter__.return_value = iter([(VIDEO, 'video', 0)])
            with patch('discovery_storage.connect_database', return_value=nullcontext(catalog)), \
                 patch('video_stats_bulk.open_database', return_value=nullcontext(media)):
                recovery = export_statistics_snapshot(Path(directory)/'recovery', ranked, player_views=True)
            self.assertEqual(recovery['statistics_endpoint'],'player')
            self.assertEqual(recovery['selection'],'missing_views')
            query = cursor.execute.call_args.args[0]
            self.assertIn('WHERE view_count IS NULL ORDER BY',query)
            self.assertNotIn('like_count IS NULL',query)

    def test_invalid_ranked_identity_does_not_leave_a_partial_snapshot(self):
        catalog = MagicMock()
        catalog.execute.return_value.fetchall.return_value = [
            (1, b'y'*32, '127.0.0.1', 1080, pack_connection_settings('socks', {}))]
        with tempfile.TemporaryDirectory() as directory:
            ranked = Path(directory)/'ranked.jsonl'
            ranked.write_text(json.dumps(dict(proxy_id=1, connection_key=(b'x'*32).hex(),
                protocol='socks5', score=3, checks=3, average_response_ms=100))+'\n')
            folder = Path(directory)/'run'
            with patch('discovery_storage.connect_database', return_value=nullcontext(catalog)):
                with self.assertRaisesRegex(ValueError,'catalog identity'):
                    export_statistics_snapshot(folder, ranked, missing_only=True)
            self.assertFalse(folder.exists())


class ParserTests(unittest.TestCase):
    def test_player_views_require_exact_matching_id_and_preserve_unavailable(self):
        data = {'videoDetails': {'videoId': VIDEO, 'viewCount': '0'},
                'playabilityStatus': {'status': 'LIVE_STREAM_OFFLINE'}}
        result = parse_player_views(data, VIDEO)
        self.assertEqual(result['stats'], {'view_count': 0, 'like_count': None})
        self.assertEqual(result['evidence']['view_count_raw'], '0')
        del data['videoDetails']['viewCount']
        self.assertIsNone(parse_player_views(data, VIDEO)['stats']['view_count'])
        with self.assertRaises(ResponseShapeError):
            parse_player_views(data, '00000000000')

    def test_captured_small_and_large_exact_counts(self):
        self.assertEqual(parse_stats(payload(), VIDEO)['stats'], {'view_count': 348, 'like_count': 3})
        self.assertEqual(parse_stats(payload('jNQXAC9IVRw'), 'jNQXAC9IVRw')['stats'],
                         {'view_count': 439482129, 'like_count': 19982779})

    def test_original_view_count_placeholder_is_ignored(self):
        data = payload()
        primary(data)['viewCount']['videoViewCountRenderer']['originalViewCount'] = '9999'
        self.assertEqual(parse_stats(data, VIDEO)['stats']['view_count'], 348)

    def test_missing_and_wrong_video_ids_are_rejected(self):
        for wrong in (None, '00000000000'):
            data = payload()
            data['currentVideoEndpoint']['watchEndpoint']['videoId'] = wrong
            with self.assertRaises(ResponseShapeError):
                parse_stats(data, VIDEO)

    def test_rounded_live_negative_and_malformed_counts_are_not_integers(self):
        for value, kind in [('1.2K views', 'view'), ('5,000 watching now', 'view'), ('1.2M', 'number'),
                            ('-1 views', 'view'), ('1,23 views', 'view'), ('Like', 'number'),
                            ('like this video along with 1.2K other people', 'like')]:
            with self.subTest(value=value):
                self.assertIsNone(exact_count(value, kind))
        self.assertEqual(exact_count('No views', 'view'), 0)
        self.assertEqual(exact_count('0', 'number'), 0)
        self.assertEqual(exact_count('\u200e1\u202f234 views', 'view'), 1234)
        with self.assertRaises(ResponseShapeError):
            exact_count(str(MAX_COUNT + 1), 'number')

    def test_hidden_likes_remain_null_and_explicitly_partial(self):
        data = payload()
        model(data).update(title='Like', accessibilityText='Like this video')
        result = dict(parse_stats(data, VIDEO), status='ok')
        self.assertTrue(has_stats(result))
        self.assertIsNone(result['stats']['like_count'])
        self.assertEqual(stats_error_reason(result), 'LIKES_NOT_EXPOSED')

    def test_zero_likes_are_successful(self):
        data = payload()
        model(data).update(title='Like', accessibilityText='like this video along with 0 other people')
        self.assertEqual(parse_stats(data, VIDEO)['stats']['like_count'], 0)

    def test_unavailable_video_does_not_become_zero(self):
        result = dict(parse_stats(payload('00000000000'), '00000000000'), status='ok')
        self.assertFalse(has_stats(result))
        self.assertIsNone(result['stats'])

    def test_dislike_button_and_toggled_like_count_are_ignored(self):
        data = payload()
        model(data).update(iconName='DISLIKE', title='900')
        self.assertIsNone(parse_stats(data, VIDEO)['stats']['like_count'])
        data = payload()
        toggle = primary(data)['videoActions']['menuRenderer']['topLevelButtons'][0][
            'segmentedLikeDislikeButtonViewModel']['likeButtonViewModel']['likeButtonViewModel'][
            'toggleButtonViewModel']['toggleButtonViewModel']
        toggle['toggledButtonViewModel'] = {'buttonViewModel': {'iconName': 'LIKE', 'title': '4'}}
        self.assertEqual(parse_stats(data, VIDEO)['stats']['like_count'], 3)

    def test_legacy_button_exact_accessibility(self):
        data = payload()
        primary(data)['videoActions']['menuRenderer']['topLevelButtons'] = [{
            'segmentedLikeDislikeButtonRenderer': {'likeButton': {'toggleButtonRenderer': {
                'defaultIcon': {'iconType': 'LIKE'}, 'defaultText': {'simpleText': '1.2K'},
                'accessibilityData': {'accessibilityData': {'label': '1,234 likes'}}}}}}]
        self.assertEqual(parse_stats(data, VIDEO)['stats']['like_count'], 1234)

    def test_import_rechecks_counts_against_saved_evidence(self):
        e = event()
        e.update(final=True, successful=True)
        e['result']['stats']['like_count'] = 4
        with self.assertRaisesRegex(ValueError, 'exact source evidence'):
            statistics_rows([{'event': e}])


class RequestTests(unittest.IsolatedAsyncioTestCase):
    async def test_unavailable_video_is_recorded_without_penalizing_proxy_data_score(self):
        observed = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=payload('00000000000')))) as client:
            result = await fetch_stats(client, '00000000000', on_attempt=observed.append)
        self.assertFalse(has_stats(result))
        self.assertIsNone(observed[0].data_received)
        self.assertEqual(observed[0].website_error, 'video:video_unavailable')
        self.assertIsNone(observed[0].connection_error)

    async def test_player_fallback_returns_exact_zero_with_one_observation(self):
        def response(request):
            self.assertEqual(request.url.path, '/youtubei/v1/player')
            return httpx.Response(200, json={'videoDetails': {'videoId': VIDEO, 'viewCount': '0'}})
        observed = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            result = await fetch_player_views(client, VIDEO, on_attempt=observed.append)
        self.assertTrue(has_stats(result))
        self.assertEqual(len(observed), 1)
        self.assertTrue(observed[0].data_received)

    async def test_success_is_one_observed_request_with_small_field_selection(self):
        def response(request):
            self.assertEqual(request.url.path, '/youtubei/v1/next')
            self.assertIn('defaultButtonViewModel', request.url.params['fields'])
            self.assertEqual(json.loads(request.content)['videoId'], VIDEO)
            return httpx.Response(200, json=payload())
        observed = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
            result = await fetch_stats(client, VIDEO, on_attempt=observed.append)
        self.assertTrue(has_stats(result))
        self.assertEqual(len(observed), 1)
        self.assertTrue(observed[0].request_sent and observed[0].data_received)

    async def test_http_error_and_tls_failure_are_per_attempt_failures(self):
        def tls(_):
            raise ssl.SSLError('DECRYPTION_FAILED_OR_BAD_RECORD_MAC')
        for callback in (lambda _: httpx.Response(429), tls):
            observed = []
            async with httpx.AsyncClient(transport=httpx.MockTransport(callback)) as client:
                result = await fetch_stats(client, VIDEO, on_attempt=observed.append)
            self.assertFalse(has_stats(result))
            self.assertEqual(len(observed), 1)

    async def test_invalid_identity_never_reports_usable_data(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=payload('00000000000')))) as client:
            observed = []
            result = await fetch_stats(client, VIDEO, on_attempt=observed.append)
        self.assertEqual(result['status'], 'unexpected_response')
        self.assertFalse(observed[0].data_received)

    async def test_stalled_body_respects_total_timeout(self):
        class Body(httpx.AsyncByteStream):
            async def __aiter__(self):
                await asyncio.sleep(10)
                yield b'{}'
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda _: httpx.Response(200, stream=Body()))) as client:
            observed = []
            result = await fetch_stats(client, VIDEO, total_timeout=0.02, on_attempt=observed.append)
        self.assertEqual(result['error'], 'TimeoutError')
        self.assertEqual(len(observed), 1)


class QueueTests(unittest.TestCase):
    @contextmanager
    def queue(self, endpoint='next'):
        with tempfile.TemporaryDirectory() as name:
            folder = Path(name)
            files = {}
            for filename, rows in [('videos.jsonl.gz', [[VIDEO, 'video', 0]]), ('proxies.jsonl.gz', [])]:
                with gzip.open(folder / filename, 'wt') as output:
                    for row in rows:
                        output.write(json.dumps(row) + '\n')
                files[filename] = {'sha256': digest(folder / filename)}
            run_id = str(uuid.uuid4())
            atomic_json(folder / 'manifest.json', {'run_id': run_id, 'videos': 1,
                        'collector': 'statistics', 'statistics_endpoint':endpoint, 'files': files})
            initialize(folder, 1, 1, 5)
            yield folder, run_id

    def unavailable(self, proxy_id=1):
        e = event(proxy_id)
        e['result'].update(stats=None, stats_missing=['VIDEO_UNAVAILABLE'],
                           evidence={'response_video_id': VIDEO}, seconds=0.2)
        e['observation'].update(data_received=None, website_error='video:video_unavailable')
        return e

    def requeue(self, folder):
        with connect_queue(folder / 'queue.sqlite3') as conn:
            conn.execute("UPDATE jobs SET status='ready' WHERE status='retry'")

    def test_statistics_run_uses_statistics_success_and_validated_immutable_output(self):
        with self.queue() as (folder, run_id):
            claim(folder, 0, 1)
            finish(folder, 0, [event()])
            self.assertEqual(queue_status(folder)['jobs'], {'saved': 1})
            export_events(folder)
            _, rows = read_chunk(next((folder / 'outbox').glob('*.gz')), run_id)
            self.assertEqual(statistics_rows(rows)[0][2:4], (348, 3))

    def test_player_recovery_dispatches_and_journals_exact_zero_views(self):
        with self.queue('player') as (folder, run_id):
            fetch, check, _ = collector_functions(str(folder.resolve()))
            self.assertIs(fetch, fetch_player_views)
            async def request():
                def respond(req):
                    self.assertEqual(req.url.path,'/youtubei/v1/player')
                    return httpx.Response(200,json={'videoDetails':{'videoId':VIDEO,'viewCount':'0'}})
                async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                    return await fetch(client,VIDEO,retries=0)
            result = asyncio.run(request())
            self.assertTrue(check(result))
            e = event()
            e['result'] = result
            claim(folder,0,1)
            finish(folder,0,[e])
            export_events(folder)
            _, rows = read_chunk(next((folder/'outbox').glob('*.gz')),run_id)
            self.assertEqual(statistics_rows(rows)[0][2:4],(0,None))
            altered = deepcopy(rows)
            altered[0]['event']['result']['evidence']['view_count_raw'] = '1'
            with self.assertRaisesRegex(ValueError,'exact source evidence'):
                statistics_rows(altered)
            altered = deepcopy(rows)
            altered[0]['event']['result']['stats']['like_count'] = 0
            with self.assertRaisesRegex(ValueError,'cannot supply a like count'):
                statistics_rows(altered)

    def test_unknown_statistics_endpoint_is_rejected(self):
        with self.queue('unknown') as (folder,_):
            with self.assertRaisesRegex(ValueError,'Unknown statistics endpoint'):
                collector_functions(str(folder.resolve()))

    def test_two_distinct_proxies_confirm_video_error_and_end_retries(self):
        with self.queue() as (folder, run_id):
            for proxy_id in (1, 2):
                claim(folder, 0, 1)
                finish(folder, 0, [self.unavailable(proxy_id)])
                state = queue_status(folder)
                self.assertEqual(state['jobs'], {'retry' if proxy_id == 1 else 'failed': 1})
                self.requeue(folder)
            self.assertEqual(state['counters']['attempts'], 2)
            export_events(folder)
            _, rows = read_chunk(next((folder / 'outbox').glob('*.gz')), run_id)
            self.assertFalse(rows[0]['event']['video_error_confirmed'])
            self.assertTrue(rows[1]['event']['video_error_confirmed'])
            self.assertEqual(statistics_rows(rows)[0][-1], 'VIDEO_UNAVAILABLE')
            aggregates = statistics_batch(rows, b'x' * 32).aggregates.values()
            self.assertEqual(sum(a.weighted_attempts for a in aggregates), 0)
            self.assertEqual(sum(a.responses_received for a in aggregates), 2)

    def test_repeated_error_from_same_proxy_is_not_independent_confirmation(self):
        with self.queue() as (folder, _):
            for _ in range(2):
                claim(folder, 0, 1)
                finish(folder, 0, [self.unavailable()])
                self.assertEqual(queue_status(folder)['jobs'], {'retry': 1})
                self.requeue(folder)

    def test_confirmation_expires_after_a_long_pause(self):
        with self.queue() as (folder, _):
            claim(folder, 0, 1)
            finish(folder, 0, [self.unavailable()])
            self.requeue(folder)
            second = self.unavailable(2)
            second['at'] = (AT + timedelta(hours=2)).isoformat()
            second['observation']['checked_at'] = AT + timedelta(hours=2)
            claim(folder, 0, 1)
            finish(folder, 0, [second])
            self.assertEqual(queue_status(folder)['jobs'], {'retry': 1})

    def test_confirmation_and_performance_roll_back_with_failed_batch(self):
        with self.queue() as (folder, _):
            claim(folder, 0, 1)
            with self.assertRaisesRegex(ValueError, 'own its video lease'):
                finish(folder, 0, [self.unavailable(), self.unavailable(2)])
            with connect_queue(folder / 'queue.sqlite3') as conn:
                for table in ('proxy_usage', 'proxy_performance', 'video_error_evidence', 'events'):
                    self.assertEqual(conn.execute('SELECT count(*) FROM ' + table).fetchone()[0], 0)
            self.assertEqual(queue_status(folder)['jobs'], {'leased': 1})

    def test_changing_video_error_or_wrong_identity_does_not_confirm_unavailability(self):
        for change in ('reason', 'identity', 'shape'):
            with self.subTest(change=change), self.queue() as (folder, _):
                claim(folder, 0, 1)
                finish(folder, 0, [self.unavailable()])
                self.requeue(folder)
                second = self.unavailable(2)
                if change == 'reason':
                    second['result'].update(stats={'view_count': None, 'like_count': None},
                                            stats_missing=['VIEWS_NOT_EXPOSED', 'LIKES_NOT_EXPOSED'])
                elif change == 'identity':
                    second['result']['evidence']['response_video_id'] = '00000000000'
                else:
                    second['result']['status'] = 'unexpected_response'
                claim(folder, 0, 1)
                finish(folder, 0, [second])
                self.assertEqual(queue_status(folder)['jobs'], {'retry': 1})

    def test_importer_rejects_neutral_score_without_valid_video_evidence(self):
        for result_change in ({'status': 'blocked', 'http_status': 429}, {'evidence': {}}):
            e = self.unavailable()
            e.update(successful=False, final=True)
            e['observation']['checked_at'] = AT.isoformat()
            e['result'].update(result_change)
            with self.assertRaisesRegex(ValueError, 'success disagree'):
                statistics_batch([{'event': e}], b'x' * 32)

    def test_importer_rejects_neutral_score_for_failed_http_response(self):
        e = self.unavailable()
        e.update(successful=False, final=True)
        e['observation'].update(checked_at=AT.isoformat(), http_status=503)
        with self.assertRaisesRegex(ValueError, 'success disagree'):
            statistics_batch([{'event': e}], b'x' * 32)

    def test_old_queue_is_upgraded_and_performance_commits_with_result(self):
        with self.queue() as (folder, _):
            with connect_queue(folder / 'queue.sqlite3') as conn:
                conn.execute('DROP TABLE proxy_performance')
                conn.execute('DROP TABLE video_error_evidence')
            claim(folder, 0, 1)
            e = event()
            e['result']['seconds'] = 0.35
            finish(folder, 0, [e])
            with connect_queue(folder / 'queue.sqlite3') as conn:
                profile = conn.execute('SELECT * FROM proxy_performance').fetchone()
                self.assertEqual(profile['samples'], 1)
                self.assertEqual(profile['latency_seconds'], 0.35)
                self.assertGreater(profile['quality'], 0.5)
            self.assertEqual(queue_status(folder)['jobs'], {'saved': 1})


@unittest.skipUnless(os.environ.get('PROXY_TEST_DATABASE') == '1', 'Opt in to isolated database tests')
class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = connect_test_database("media", autocommit=True)
        self.conn.execute('CREATE TEMP TABLE videos(video_id TEXT PRIMARY KEY,title TEXT,description TEXT,'
                          'view_count BIGINT,like_count BIGINT,stats_updated_at TIMESTAMPTZ,stats_error TEXT)')
        self.conn.execute("INSERT INTO videos(video_id,title,description) VALUES (%s,'Keep','Keep description')", (VIDEO,))

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def apply(self, success=True, views=348, likes=3, at=AT, error=None):
        with self.conn.transaction():
            return apply_statistics(self.conn, [(VIDEO, success, views, likes, at, error)])

    def test_apply_replay_and_stale_protection_preserve_metadata(self):
        self.assertEqual(self.apply()['statistics_saved'], 1)
        self.assertEqual(self.apply()['statistics_saved'], 0)
        self.assertEqual(self.apply(views=1, at=AT-timedelta(days=1))['statistics_saved'], 0)
        self.assertEqual(self.conn.execute('SELECT title,description,view_count,like_count FROM videos').fetchone(),
                         ('Keep', 'Keep description', 348, 3))

    def test_partial_and_failed_responses_preserve_successful_counts(self):
        self.apply(views=0, likes=0)
        self.apply(success=False, views=None, likes=None, error='HTTP_429', at=AT+timedelta(seconds=1))
        self.assertEqual(self.conn.execute('SELECT view_count,like_count,stats_error FROM videos').fetchone(), (0, 0, None))
        self.apply(views=1, likes=None, at=AT+timedelta(seconds=2), error='LIKES_NOT_EXPOSED')
        self.assertEqual(self.conn.execute('SELECT view_count,like_count,stats_error FROM videos').fetchone(),
                         (1, 0, 'LIKES_NOT_EXPOSED'))

    def test_missing_row_is_rejected(self):
        self.conn.execute('DELETE FROM videos')
        with self.assertRaisesRegex(ValueError, 'missing'):
            self.apply()

    def test_player_recovery_preserves_known_likes_and_replays_once(self):
        self.conn.execute('UPDATE videos SET like_count=7')
        e = event()
        e.update(final=True,successful=True)
        e['result'] = dict(parse_player_views({'videoDetails':{'videoId':VIDEO,'viewCount':'0'}},VIDEO),
                           status='ok',video_id=VIDEO,http_status=200)
        values = statistics_rows([{'event':e}])
        with self.conn.transaction():
            self.assertEqual(apply_statistics(self.conn,values)['statistics_saved'],1)
        with self.conn.transaction():
            self.assertEqual(apply_statistics(self.conn,values)['statistics_saved'],0)
        self.assertEqual(self.conn.execute('SELECT title,view_count,like_count FROM videos').fetchone(),
                         ('Keep',0,7))


if __name__ == '__main__':
    unittest.main()
