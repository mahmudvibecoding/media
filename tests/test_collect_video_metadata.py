import asyncio
from collections import Counter
from contextlib import AsyncExitStack, nullcontext
from datetime import datetime, timezone
import gzip
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import uuid

import httpx
import psycopg

from collect_video_metadata import (
    CLIENT_VERSION, ENDPOINT, FIELD_MASK, MAX_BODY_BYTES, MAX_METADATA_ERROR_CHARS,
    ResponseShapeError, collect, fetch_metadata, fetch_metadata_with_proxies, has_metadata, metadata_error_reason,
    parse_metadata, parse_proxy_urls, publication_time, save_metadata, save_metadata_error, select_videos,
)


VIDEO_ID = "RtXBV0X1v1Q"


def payload(video_id=VIDEO_ID, status="OK"):
    return {
        "videoDetails": {"videoId": video_id, "title": "Test title", "shortDescription": "",
                         "lengthSeconds": "577", "thumbnail": {"thumbnails": [
                             {"url": f"https://i.ytimg.com/vi/{video_id}/default.jpg"},
                             {"url": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"},
                         ]}},
        "microformat": {"playerMicroformatRenderer": {"publishDate": "2026-09-27T09:48:04-07:00"}},
        "playabilityStatus": {"status": status, "reason": "Video unavailable"},
    }


class MetadataParsingTests(unittest.TestCase):
    def test_full_metadata_includes_empty_description_and_exact_utc_time(self):
        result = parse_metadata(payload(), VIDEO_ID)
        self.assertTrue(result["metadata_complete"])
        self.assertEqual(result["metadata"], {
            "title": "Test title", "description": "", "duration_seconds": 577,
            "published_at": "2026-09-27T16:48:04+00:00",
            "thumbnail_url": f"https://i.ytimg.com/vi/{VIDEO_ID}/hqdefault.jpg",
        })
        self.assertEqual(result["player_status"], "OK")

    def test_unplayable_response_still_returns_its_metadata(self):
        result = parse_metadata(payload(status="UNPLAYABLE"), VIDEO_ID)
        self.assertTrue(result["metadata_complete"])
        self.assertEqual(result["metadata"]["title"], "Test title")
        self.assertEqual(result["player_status"], "UNPLAYABLE")

    def test_restricted_response_can_contain_only_player_status(self):
        result = parse_metadata({"playabilityStatus": {"status": "LOGIN_REQUIRED"}}, VIDEO_ID)
        self.assertFalse(result["metadata_complete"])
        self.assertIsNone(result["metadata"]["title"])
        self.assertIsNone(result["metadata"]["published_at"])

    def test_day_only_and_timezone_free_dates_never_become_exact_timestamps(self):
        for value, precision in ((None, "missing"), ("2026-09-27", "date"),
                                 ("2026-09-27T09:48:04", "insufficient"),
                                 ("2026-09-27T09:48Z", "insufficient")):
            with self.subTest(value=value):
                self.assertEqual(publication_time(value), (None, precision))

    def test_invalid_calendar_dates_are_rejected(self):
        for value in ("2026-02-30", "2026-02-30T00:00:00Z", 123):
            with self.subTest(value=value), self.assertRaises(ResponseShapeError):
                publication_time(value)

    def test_zero_duration_is_preserved_and_bad_durations_are_rejected(self):
        value = payload()
        value["videoDetails"]["lengthSeconds"] = "0"
        self.assertEqual(parse_metadata(value, VIDEO_ID)["metadata"]["duration_seconds"], 0)
        for duration in (True, "-1", "1.5", "unknown", 2**31):
            value["videoDetails"]["lengthSeconds"] = duration
            with self.subTest(duration=duration), self.assertRaises(ResponseShapeError):
                parse_metadata(value, VIDEO_ID)

    def test_wrong_id_and_unrecognized_payloads_are_rejected(self):
        for value in (None, {}, {"playabilityStatus": {"status": "OK"}},
                      payload("differentid"), {"playabilityStatus": "bad"},
                      {"playabilityStatus": {"status": "UNPLAYABLE"},
                       "microformat": {"playerMicroformatRenderer": {"publishDate": "2026-09-27T00:00:00Z"}}}):
            with self.subTest(value=value), self.assertRaises(ResponseShapeError):
                parse_metadata(value, VIDEO_ID)

    def test_missing_description_is_distinct_from_empty_description(self):
        value = payload()
        del value["videoDetails"]["shortDescription"]
        result = parse_metadata(value, VIDEO_ID)
        self.assertIsNone(result["metadata"]["description"])
        self.assertFalse(result["metadata_complete"])

    def test_last_thumbnail_url_is_selected_without_dimensions(self):
        value = payload()
        value["videoDetails"]["thumbnail"]["thumbnails"].extend([
            {"url": "https://i.ytimg.com/last.jpg?token=example"}, {},
        ])
        self.assertEqual(parse_metadata(value, VIDEO_ID)["metadata"]["thumbnail_url"],
                         "https://i.ytimg.com/last.jpg?token=example")

    def test_missing_or_empty_thumbnails_leave_url_unset(self):
        for thumbnails in (None, [], [{}]):
            with self.subTest(thumbnails=thumbnails):
                value = payload()
                if thumbnails is None:
                    del value["videoDetails"]["thumbnail"]
                else:
                    value["videoDetails"]["thumbnail"]["thumbnails"] = thumbnails
                result = parse_metadata(value, VIDEO_ID)
                self.assertIsNone(result["metadata"]["thumbnail_url"])
                self.assertEqual(result["metadata"]["title"], "Test title")
                self.assertFalse(result["metadata_complete"])

    def test_invalid_thumbnail_shapes_are_rejected(self):
        for thumbnails in ({}, ["not-an-object"], [{"url": 123}], [{"url": " "}]):
            value = payload()
            value["videoDetails"]["thumbnail"]["thumbnails"] = thumbnails
            with self.subTest(thumbnails=thumbnails), self.assertRaises(ResponseShapeError):
                parse_metadata(value, VIDEO_ID)


class MetadataHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_one_filtered_request_reads_gzip_metadata_even_when_unplayable(self):
        requests = []
        def respond(request):
            requests.append(request)
            self.assertEqual(str(request.url).split("?")[0], ENDPOINT)
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.params["fields"], FIELD_MASK)
            self.assertEqual(request.url.params["prettyPrint"], "false")
            self.assertEqual(json.loads(request.content)["videoId"], VIDEO_ID)
            self.assertNotIn("streamingData", request.url.params["fields"])
            self.assertIn("thumbnail/thumbnails/url", request.url.params["fields"])
            self.assertNotIn("width", request.url.params["fields"])
            self.assertNotIn("height", request.url.params["fields"])
            return httpx.Response(200, content=gzip.compress(json.dumps(payload(status="UNPLAYABLE")).encode()),
                                  headers={"Content-Encoding": "gzip"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await fetch_metadata(client, VIDEO_ID)
        self.assertEqual(len(requests), 1)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["player_status"], "UNPLAYABLE")

    async def test_http_blocks_use_all_ten_retries_even_with_large_error_body(self):
        for code in (400, 401, 403, 429):
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(code, content=b"x" * (MAX_BODY_BYTES + 1))
            )) as client:
                result = await fetch_metadata(client, VIDEO_ID)
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["attempts"], 11)
            self.assertIsNone(result["metadata"])

    async def test_transient_server_error_retries_immediately(self):
        responses = iter([httpx.Response(500), httpx.Response(200, json=payload())])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: next(responses))) as client:
            with patch("collect_video_metadata.asyncio.sleep", new_callable=AsyncMock) as sleep:
                result = await fetch_metadata(client, VIDEO_ID)
        sleep.assert_not_awaited()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["attempts"], 2)

    async def test_http_200_bot_challenge_is_an_access_failure(self):
        data = {"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot"}}
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=data))) as client:
            result = await fetch_metadata(client, VIDEO_ID, retries=0)
        self.assertEqual(result["status"], "access_challenge")
        self.assertEqual(result["attempts"], 1)
        self.assertEqual(result["player_status"], "LOGIN_REQUIRED")
        self.assertFalse(result["metadata_complete"])
        data["playabilityStatus"]["reason"] = "Sign in to confirm your age"
        self.assertFalse(parse_metadata(data, VIDEO_ID)["access_challenge"])

    async def test_transport_error_retries_immediately(self):
        responses = iter([httpx.ConnectError("Connection failed"), httpx.Response(200, json=payload())])
        def respond(request):
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch("collect_video_metadata.asyncio.sleep", new_callable=AsyncMock) as sleep:
                result = await fetch_metadata(client, VIDEO_ID)
        sleep.assert_not_awaited()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["attempts"], 2)
        self.assertNotIn("error", result)

    async def test_bot_challenge_retries_immediately_and_stops_after_success(self):
        data = {"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot"}}
        responses = iter([httpx.Response(200, json=data), httpx.Response(200, json=payload())])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: next(responses))) as client:
            with patch("collect_video_metadata.asyncio.sleep", new_callable=AsyncMock) as sleep:
                result = await fetch_metadata(client, VIDEO_ID, retries=3)
        sleep.assert_not_awaited()
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["metadata_complete"])
        self.assertEqual(result["attempts"], 2)
        self.assertNotIn("error", result)

    async def test_persistent_bot_challenge_exhausts_the_retry_limit_without_waiting(self):
        data = {"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot"}}
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=data))) as client:
            with patch("collect_video_metadata.asyncio.sleep", new_callable=AsyncMock) as sleep:
                result = await fetch_metadata(client, VIDEO_ID)
        sleep.assert_not_awaited()
        self.assertEqual(result["status"], "access_challenge")
        self.assertEqual(result["attempts"], 11)
        self.assertFalse(result["metadata_complete"])

    async def test_error_after_a_challenge_does_not_keep_stale_response_metadata(self):
        data = {"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot"}}
        for failure, expected, retries in ((lambda: httpx.Response(429), "blocked", 3),
                                           (lambda: httpx.ConnectError("Connection failed"), "error", 1)):
            with self.subTest(expected=expected):
                responses = iter([httpx.Response(200, json=data)] + [failure() for _ in range(retries)])
                def respond(request):
                    response = next(responses)
                    if isinstance(response, Exception):
                        raise response
                    return response
                async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                    result = await fetch_metadata(client, VIDEO_ID, retries=retries)
                self.assertEqual(result["status"], expected)
                self.assertEqual(result["attempts"], retries + 1)
                self.assertIsNone(result["metadata"])
                self.assertNotIn("access_challenge", result)

    async def test_bad_json_wrong_video_and_oversized_success_cannot_release_metadata(self):
        responses = [lambda: httpx.Response(200, text="not-json"),
                     lambda: httpx.Response(200, json=payload("differentid")),
                     lambda: httpx.Response(200, content=b"x" * (MAX_BODY_BYTES + 1))]
        for response in responses:
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response())) as client:
                result = await fetch_metadata(client, VIDEO_ID)
            self.assertEqual(result["status"], "unexpected_response")
            self.assertEqual(result["attempts"], 11)
            self.assertIsNone(result["metadata"])

    async def test_all_failure_types_can_recover_and_stop_retries_on_success(self):
        failures = [
            *[(f"HTTP {code}", lambda code=code: httpx.Response(code)) for code in (400, 401, 403, 404, 429, 500, 503)],
            ("sign-in", lambda: httpx.Response(200, json={"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Please sign in"}})),
            ("unavailable", lambda: httpx.Response(200, json={"playabilityStatus": {"status": "ERROR", "reason": "Video unavailable"}})),
            ("empty metadata", lambda: httpx.Response(200, json={"videoDetails": {"videoId": VIDEO_ID}, "playabilityStatus": {"status": "OK"}})),
            ("invalid JSON", lambda: httpx.Response(200, text="not-json")),
            ("wrong video", lambda: httpx.Response(200, json=payload("differentid"))),
            ("oversized", lambda: httpx.Response(200, content=b"x" * (MAX_BODY_BYTES + 1))),
            ("timeout", lambda: httpx.ReadTimeout("Timed out")),
            ("network", lambda: httpx.ConnectError("Connection failed")),
        ]
        for name, failure in failures:
            with self.subTest(failure=name):
                responses = iter([failure(), httpx.Response(200, json=payload())])
                def respond(request):
                    response = next(responses)
                    if isinstance(response, Exception):
                        raise response
                    return response
                async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
                    with patch("collect_video_metadata.asyncio.sleep", new_callable=AsyncMock) as sleep:
                        result = await fetch_metadata(client, VIDEO_ID)
                sleep.assert_not_awaited()
                self.assertEqual(result["attempts"], 2)
                self.assertEqual(result["status"], "ok")
                self.assertTrue(result["metadata_complete"])
                self.assertNotIn("error", result)
                self.assertNotIn("retry_after", result)

    async def test_different_failures_share_one_retry_budget_and_keep_final_reason(self):
        responses = iter([
            httpx.Response(400),
            httpx.Response(200, json={"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Please sign in"}}),
            httpx.Response(429),
            httpx.Response(200, text="not-json"),
            httpx.ReadTimeout("Timed out"),
            httpx.Response(500),
            httpx.Response(200, json={"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot"}}),
            httpx.Response(200, content=b"x" * (MAX_BODY_BYTES + 1)),
            httpx.Response(200, json={"videoDetails": {"videoId": VIDEO_ID}, "playabilityStatus": {"status": "OK"}}),
            httpx.Response(200, json=payload("differentid")),
            httpx.Response(200, json={"playabilityStatus": {"status": "ERROR", "reason": "Video unavailable"}}),
        ])
        requests = []
        def respond(request):
            requests.append(request)
            response = next(responses)
            if isinstance(response, Exception):
                raise response
            return response
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await fetch_metadata(client, VIDEO_ID)
        self.assertEqual(len(requests), 11)
        self.assertEqual(result["attempts"], 11)
        self.assertEqual(metadata_error_reason(result), "ERROR: Video unavailable")
        self.assertTrue(all(value is None for value in result["metadata"].values()))
        self.assertNotIn("error", result)

    async def test_partial_metadata_with_empty_description_stops_retries(self):
        data = {"videoDetails": {"videoId": VIDEO_ID, "shortDescription": ""},
                "playabilityStatus": {"status": "UNPLAYABLE"}}
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json=data))) as client:
            result = await fetch_metadata(client, VIDEO_ID)
        self.assertEqual(result["attempts"], 1)
        self.assertEqual(result["metadata"]["description"], "")
        self.assertFalse(result["metadata_complete"])


class ProxyListTests(unittest.TestCase):
    def test_proxy_urls_preserve_order(self):
        urls = ['http://user:secret@proxy1.invalid:8001', 'http://user:secret@proxy2.invalid:8002']
        self.assertEqual(parse_proxy_urls(json.dumps(urls)), urls)

    def test_invalid_proxy_lists_fail_without_exposing_credentials(self):
        invalid = ['not-json', '[]', '{}', '[null]', '[123]', '["socks5://host:80"]',
                   '["http:///missing-host"]', '["http://user:private-password@host:bad-port"]',
                   '["http://first:private-password@host:80","http://second:private-password@host"]']
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError) as error:
                parse_proxy_urls(value)
            self.assertNotIn('private-password', str(error.exception))


class MetadataProxyHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_assigned_proxy_gets_one_attempt_without_falling_through_to_success(self):
        calls, body_sizes = [], []
        blocked = {'playabilityStatus': {'status': 'LOGIN_REQUIRED', 'reason': 'Please sign in'}}
        def transport(number):
            def respond(request):
                calls.append(number)
                body_sizes.append(len(request.content))
                if number == 2:
                    raise httpx.ConnectError('Connection failed')
                return httpx.Response(200, json=payload() if number == 3 else blocked)
            return httpx.MockTransport(respond)
        async with AsyncExitStack() as stack:
            clients = [await stack.enter_async_context(httpx.AsyncClient(transport=transport(number)))
                       for number in range(1, 5)]
            result = await fetch_metadata_with_proxies(clients, VIDEO_ID)
        self.assertEqual(calls, [1])
        self.assertEqual(result['attempts'], 1)
        self.assertEqual(result['proxy_number'], 1)
        self.assertEqual(result['request_body_bytes'], sum(body_sizes))
        self.assertEqual(result['status'], 'ok')
        self.assertFalse(has_metadata(result))
        self.assertEqual(metadata_error_reason(result), 'LOGIN_REQUIRED: Please sign in')

    async def test_assigned_proxy_timeout_is_preserved_without_trying_other_proxies(self):
        calls = []
        def transport(number):
            def respond(request):
                calls.append(number)
                if number == 10:
                    raise httpx.ReadTimeout('Timed out')
                return httpx.Response(200, json={'playabilityStatus': {
                    'status': 'LOGIN_REQUIRED', 'reason': 'Sign in to confirm you’re not a bot'}})
            return httpx.MockTransport(respond)
        async with AsyncExitStack() as stack:
            clients = [await stack.enter_async_context(httpx.AsyncClient(transport=transport(number)))
                       for number in range(1, 11)]
            result = await fetch_metadata_with_proxies(clients, VIDEO_ID, proxy_index=9)
        self.assertEqual(calls, [10])
        self.assertEqual(result['attempts'], 1)
        self.assertEqual(result['proxy_number'], 10)
        self.assertEqual(metadata_error_reason(result), 'TIMEOUT')
        self.assertIsNone(result['metadata'])
        self.assertNotIn('player_status', result)


class MetadataErrorTests(unittest.TestCase):
    def test_final_reason_distinguishes_signin_http_transport_and_invalid_responses(self):
        cases = [
            ({"status": "ok", "http_status": 200, "player_status": "LOGIN_REQUIRED", "player_reason": "Please sign in"},
             "LOGIN_REQUIRED: Please sign in"),
            ({"status": "blocked", "http_status": 429}, "HTTP_429: Too Many Requests"),
            ({"status": "error", "http_status": 503}, "HTTP_503: Service Unavailable"),
            ({"status": "error", "error": "ReadTimeout"}, "TIMEOUT"),
            ({"status": "error", "error": "ConnectError"}, "REQUEST_ERROR: ConnectError"),
            ({"status": "unexpected_response", "http_status": 200, "error": "Response video ID does not match the requested ID"},
             "INVALID_RESPONSE: Response video ID does not match the requested ID"),
            ({"status": "ok", "player_status": "OK"}, "NO_METADATA: No metadata returned"),
        ]
        for result, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(metadata_error_reason(result), expected)

    def test_upstream_reason_is_short_and_safe_for_a_postgres_text_value(self):
        result = {"status": "ok", "player_status": "LOGIN_REQUIRED", "player_reason": "  Please\n sign\x00 in\t "}
        self.assertEqual(metadata_error_reason(result), 'LOGIN_REQUIRED: Please sign in')
        result['player_reason'] = 'x' * (MAX_METADATA_ERROR_CHARS * 2)
        self.assertEqual(len(metadata_error_reason(result)), MAX_METADATA_ERROR_CHARS)


class MetadataSaveTests(unittest.TestCase):
    def test_failed_requests_never_write(self):
        for status in ("error", "blocked", "unexpected_response"):
            conn = Mock()
            self.assertEqual(save_metadata(conn, {"status": status, "metadata": None}), 0)
            conn.execute.assert_not_called()

    def test_status_only_responses_never_write(self):
        conn = Mock()
        result = {"status": "access_challenge", "video_id": VIDEO_ID,
                  **parse_metadata({"playabilityStatus": {"status": "LOGIN_REQUIRED"}}, VIDEO_ID)}
        self.assertEqual(save_metadata(conn, result), 0)
        conn.execute.assert_not_called()

    def test_bot_challenge_never_marks_metadata_as_saved_even_if_fields_are_present(self):
        conn = Mock()
        result = {"status": "access_challenge", "video_id": VIDEO_ID,
                  **parse_metadata(payload(), VIDEO_ID)}
        self.assertEqual(save_metadata(conn, result), 0)
        conn.execute.assert_not_called()


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class MetadataDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"])
        self.addCleanup(self.conn.close)
        transaction = self.conn.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__, None, None, None)
        self.channel = "UC" + uuid.uuid4().hex[:22]
        self.video = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.channels (channel_id) VALUES (%s)", (self.channel,))
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'short')",
                          (self.video, self.channel))

    def result(self, data=None):
        return {"status": "ok", "video_id": self.video,
                **parse_metadata(data if data is not None else payload(self.video), self.video)}

    def test_store_metadata_with_exact_date_without_changing_identity(self):
        before = self.conn.execute('SELECT clock_timestamp()').fetchone()[0]
        self.assertEqual(save_metadata(self.conn, self.result(payload(self.video, "UNPLAYABLE"))), 1)
        row = self.conn.execute("SELECT channel_id,type,title,description,duration_seconds,published_at,thumbnail_url FROM public.videos WHERE video_id=%s",
                                (self.video,)).fetchone()
        self.assertEqual(row, (self.channel, "short", "Test title", "", 577,
                               datetime(2026, 9, 27, 16, 48, 4, tzinfo=timezone.utc),
                               f"https://i.ytimg.com/vi/{self.video}/hqdefault.jpg"))
        updated = self.conn.execute('SELECT metadata_updated_at FROM public.videos WHERE video_id=%s',
                                    (self.video,)).fetchone()[0]
        self.assertGreaterEqual(updated, before)
        self.assertLessEqual(updated, self.conn.execute('SELECT clock_timestamp()').fetchone()[0])

    def test_partial_refresh_preserves_existing_metadata(self):
        save_metadata(self.conn, self.result())
        partial = {"playabilityStatus": {"status": "LOGIN_REQUIRED"},
                   "videoDetails": {"videoId": self.video, "title": "Updated title"},
                   "microformat": {"playerMicroformatRenderer": {"publishDate": "2026-09-27"}}}
        save_metadata(self.conn, self.result(partial))
        row = self.conn.execute("SELECT title,description,duration_seconds,published_at,thumbnail_url FROM public.videos WHERE video_id=%s",
                                (self.video,)).fetchone()
        self.assertEqual(row, ("Updated title", "", 577,
                               datetime(2026, 9, 27, 16, 48, 4, tzinfo=timezone.utc),
                               f"https://i.ytimg.com/vi/{self.video}/hqdefault.jpg"))

    def test_selection_uses_timestamp_and_deduplicates_requested_ids(self):
        args = SimpleNamespace(video_id=[self.video, self.video], limit=100)
        self.conn.execute("UPDATE public.videos SET title='Previously collected' WHERE video_id=%s", (self.video,))
        self.assertEqual(select_videos(self.conn, args), [(self.video, "short")])
        save_metadata(self.conn, self.result())
        self.assertEqual(select_videos(self.conn, args), [])
        other = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')",
                          (other, self.channel))
        args.video_id = [self.video, other, self.video]
        self.assertEqual(select_videos(self.conn, args), [(other, "video")])
        args.video_id = ["not-existing"]
        with self.assertRaises(ValueError):
            select_videos(self.conn, args)

    def test_skip_errors_excludes_failed_ids_and_keeps_default_retry_selection(self):
        other = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')",
                          (other, self.channel))
        save_metadata_error(self.conn, {"video_id": self.video, "status": "error", "error": "ReadTimeout"})
        args = SimpleNamespace(video_id=[self.video, other, self.video], limit=100, skip_errors=True)
        self.assertEqual(select_videos(self.conn, args), [(other, "video")])
        args.skip_errors = False
        self.assertEqual(select_videos(self.conn, args), [(self.video, "short"), (other, "video")])
        save_metadata(self.conn, self.result())
        args.skip_errors = True
        self.assertEqual(select_videos(self.conn, args), [(other, "video")])

    def test_metadata_write_rolls_back_with_transaction(self):
        save_metadata_error(self.conn, {"video_id": self.video, "status": "error", "error": "ReadTimeout"})
        with self.conn.transaction(force_rollback=True):
            save_metadata(self.conn, self.result())
            self.assertIsNone(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                                (self.video,)).fetchone()[0])
        row = self.conn.execute("SELECT title,published_at,thumbnail_url,metadata_updated_at,metadata_error FROM public.videos WHERE video_id=%s",
                                (self.video,)).fetchone()
        self.assertEqual(row, (None, None, None, None, 'TIMEOUT'))

    def test_failure_replaces_only_error_and_success_clears_it(self):
        self.assertIsNone(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                            (self.video,)).fetchone()[0])
        save_metadata(self.conn, self.result())
        read = 'SELECT title,description,duration_seconds,published_at,thumbnail_url,metadata_updated_at FROM public.videos WHERE video_id=%s'
        before = self.conn.execute(read, (self.video,)).fetchone()
        error = {"video_id": self.video, "status": "ok", "http_status": 200,
                 "player_status": "LOGIN_REQUIRED", "player_reason": "Please sign in"}
        self.assertEqual(save_metadata_error(self.conn, error), 1)
        self.assertEqual(self.conn.execute(read, (self.video,)).fetchone(), before)
        self.assertEqual(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                           (self.video,)).fetchone()[0], 'LOGIN_REQUIRED: Please sign in')
        save_metadata_error(self.conn, {"video_id": self.video, "status": "blocked", "http_status": 429})
        self.assertEqual(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                           (self.video,)).fetchone()[0], 'HTTP_429: Too Many Requests')
        self.assertEqual(self.conn.execute(read, (self.video,)).fetchone(), before)
        save_metadata(self.conn, self.result())
        self.assertIsNone(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                            (self.video,)).fetchone()[0])

    def test_final_error_is_saved_without_file_logs_and_resume_clears_it(self):
        other = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')",
                          (other, self.channel))
        data = {"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot"}}
        requests = []
        failing = True
        def respond(request):
            video_id = json.loads(request.content)["videoId"]
            requests.append(video_id)
            if failing and video_id == self.video:
                self.assertIsNone(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                                    (self.video,)).fetchone()[0])
            return httpx.Response(200, json=data if failing and video_id == self.video else payload(video_id))
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(video_id=[self.video, other], limit=100,
                                   concurrency=1, retries=10, client_version=CLIENT_VERSION,
                                   output=Path(folder)/"run")
            with patch("collect_video_metadata.open_database", return_value=nullcontext(self.conn)), \
                    patch("collect_video_metadata.httpx.AsyncClient", return_value=client), \
                    patch("builtins.print") as printed:
                self.assertEqual(asyncio.run(collect(args)), 2)
            self.assertEqual(requests, [self.video] * 11 + [other])
            summary = json.loads((args.output/"summary.json").read_text())
            self.assertEqual(summary["unprocessed"], 0)
            self.assertEqual(summary["saved"], 1)
            self.assertEqual(summary["unsaved"], 1)
            self.assertEqual(summary["retry_counts"], {"0": 1, "10": 1})
            self.assertIsNone(summary["stopped_reason"])
            self.assertEqual([path.name for path in args.output.iterdir()], ["summary.json"])
            logged = (args.output / 'summary.json').read_text() + str(printed.call_args_list)
            self.assertNotIn(self.video, logged)
            self.assertNotIn('not a bot', logged)
            self.assertEqual(select_videos(self.conn, args), [(self.video, "short")])
            self.assertIsNone(self.conn.execute('SELECT metadata_updated_at FROM public.videos WHERE video_id=%s',
                                                (self.video,)).fetchone()[0])
            self.assertEqual(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                               (self.video,)).fetchone()[0], 'LOGIN_REQUIRED: Sign in to confirm you’re not a bot')

            failing = False
            client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
            args.output = Path(folder) / 'resume'
            with patch("collect_video_metadata.open_database", return_value=nullcontext(self.conn)), \
                    patch("collect_video_metadata.httpx.AsyncClient", return_value=client), patch("builtins.print"):
                self.assertEqual(asyncio.run(collect(args)), 0)
            self.assertEqual(requests, [self.video] * 11 + [other, self.video])
            self.assertEqual(select_videos(self.conn, args), [])
            self.assertIsNone(self.conn.execute('SELECT metadata_error FROM public.videos WHERE video_id=%s',
                                                (self.video,)).fetchone()[0])

    def test_every_failure_is_recorded_only_after_all_eleven_attempts(self):
        failures = [
            ('LOGIN_REQUIRED: Please sign in', lambda: httpx.Response(200, json={
                'playabilityStatus': {'status': 'LOGIN_REQUIRED', 'reason': 'Please sign in'}})),
            ('ERROR: Video unavailable', lambda: httpx.Response(200, json={
                'playabilityStatus': {'status': 'ERROR', 'reason': 'Video unavailable'}})),
            ('HTTP_429: Too Many Requests', lambda: httpx.Response(429)),
            ('HTTP_404: Not Found', lambda: httpx.Response(404)),
            ('HTTP_503: Service Unavailable', lambda: httpx.Response(503)),
            ('INVALID_RESPONSE: Response is not an object', lambda: httpx.Response(200, json=[])),
            ('NO_METADATA: No metadata returned', lambda: httpx.Response(200, json={
                'videoDetails': {'videoId': self.video}, 'playabilityStatus': {'status': 'OK'}})),
            ('TIMEOUT', lambda: httpx.ReadTimeout('Timed out')),
        ]
        for expected, failure in failures:
            with self.subTest(reason=expected):
                self.conn.execute('UPDATE public.videos SET metadata_error=NULL WHERE video_id=%s', (self.video,))
                requests = []
                def respond(request):
                    requests.append(request)
                    row = self.conn.execute('SELECT title,metadata_updated_at,metadata_error FROM public.videos WHERE video_id=%s',
                                            (self.video,)).fetchone()
                    self.assertEqual(row, (None, None, None))
                    response = failure()
                    if isinstance(response, Exception):
                        raise response
                    return response
                client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
                with tempfile.TemporaryDirectory() as folder:
                    args = SimpleNamespace(video_id=[self.video], limit=100, concurrency=1, retries=10,
                                           client_version=CLIENT_VERSION, output=Path(folder)/'run')
                    with patch('collect_video_metadata.open_database', return_value=nullcontext(self.conn)), \
                            patch('collect_video_metadata.httpx.AsyncClient', return_value=client), \
                            patch('collect_video_metadata.save_metadata_error', wraps=save_metadata_error) as write_error, \
                            patch('builtins.print'):
                        self.assertEqual(asyncio.run(collect(args)), 2)
                    self.assertEqual(len(requests), 11)
                    write_error.assert_called_once()
                    self.assertEqual(write_error.call_args.args[1]['attempts'], 11)
                    summary = json.loads((args.output/'summary.json').read_text())
                    self.assertEqual(summary['saved'], 0)
                    self.assertEqual(summary['retry_counts'], {'10': 1})
                row = self.conn.execute('SELECT title,metadata_updated_at,metadata_error FROM public.videos WHERE video_id=%s',
                                        (self.video,)).fetchone()
                self.assertEqual(row, (None, None, expected))
                self.assertEqual(select_videos(self.conn, args), [(self.video, 'short')])

    def test_proxy_list_records_failure_then_rotates_for_the_next_video(self):
        urls = [f'http://user:secret@proxy{number}.invalid:8000' for number in range(1, 4)]
        second = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')",
                          (second, self.channel))
        self.conn.execute('UPDATE public.videos SET metadata_error=%s WHERE video_id=%s',
                          ('Previous failure', self.video))
        calls = []
        def transport(number):
            def respond(request):
                video_id = json.loads(request.content)['videoId']
                calls.append((number, video_id))
                if number == 2:
                    return httpx.Response(200, json=payload(video_id))
                return httpx.Response(200, json={'playabilityStatus': {
                    'status': 'ERROR', 'reason': 'Video unavailable'}})
            return httpx.MockTransport(respond)
        clients = [httpx.AsyncClient(transport=transport(number)) for number in range(1, 4)]
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(video_id=[self.video, second], limit=100, concurrency=128, retries=10,
                                   client_version=CLIENT_VERSION, output=Path(folder)/'run')
            with patch.dict(os.environ, {'MEDIA_PROXY_URLS': json.dumps(urls), 'MEDIA_PROXY_URL': 'http://unused.invalid:9000'}), \
                    patch('collect_video_metadata.open_database', return_value=nullcontext(self.conn)), \
                    patch('collect_video_metadata.httpx.AsyncClient', side_effect=clients) as factory, \
                    patch('collect_video_metadata.save_metadata_error', wraps=save_metadata_error) as write_error, \
                    patch('builtins.print'):
                self.assertEqual(asyncio.run(collect(args)), 2)
            self.assertEqual([call.kwargs['proxy'] for call in factory.call_args_list], urls)
            self.assertEqual(calls, [(1, self.video), (2, second)])
            write_error.assert_called_once()
            self.assertEqual(write_error.call_args.args[1]['video_id'], self.video)
            summary = json.loads((args.output/'summary.json').read_text())
            self.assertEqual(summary['retry_policy'], 'one_attempt_per_video')
            self.assertEqual(summary['proxy_selection'], 'round_robin')
            self.assertEqual(summary['proxy_count'], 3)
            self.assertEqual(summary['retry_counts'], {'0': 2})
            self.assertEqual((summary['completed'], summary['saved'], summary['http_attempts']), (2, 1, 2))
            failed = self.conn.execute('SELECT metadata_error,metadata_updated_at FROM public.videos WHERE video_id=%s',
                                      (self.video,)).fetchone()
            saved = self.conn.execute('SELECT metadata_error,metadata_updated_at FROM public.videos WHERE video_id=%s',
                                     (second,)).fetchone()
            self.assertEqual(failed, ('ERROR: Video unavailable', None))
            self.assertIsNone(saved[0])
            self.assertIsNotNone(saved[1])

    def test_player_status_column_does_not_exist(self):
        columns = {row[0] for row in self.conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='videos'"
        )}
        self.assertEqual(columns, {"video_id", "channel_id", "type", "title", "description",
                                   "duration_seconds", "published_at", "thumbnail_url", "metadata_updated_at", "metadata_error"})

    def test_valid_partial_metadata_is_saved_with_a_timestamp(self):
        data = {"playabilityStatus": {"status": "UNPLAYABLE"},
                "videoDetails": {"videoId": self.video, "title": "Partial title"}}
        result = {"video_id": self.video, "status": "ok", "rows_updated": 0,
                  "attempts": 1, "request_body_bytes": 100, "response_body_bytes": 100,
                  "decoded_body_bytes": 100, **parse_metadata(data, self.video)}
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(video_id=[self.video], limit=100,
                                   concurrency=1, retries=10, client_version=CLIENT_VERSION,
                                   output=Path(folder)/"run")
            with patch("collect_video_metadata.open_database", return_value=nullcontext(self.conn)), \
                    patch("collect_video_metadata.fetch_metadata", new_callable=AsyncMock, return_value=result), \
                    patch("builtins.print"):
                self.assertEqual(asyncio.run(collect(args)), 0)
            summary = json.loads((args.output/"summary.json").read_text())
            self.assertEqual(summary["saved"], 1)
            self.assertEqual(summary["metadata_complete"], 0)
        self.assertEqual(self.conn.execute("SELECT title FROM public.videos WHERE video_id=%s",
                                           (self.video,)).fetchone()[0], "Partial title")
        self.assertEqual(select_videos(self.conn, args), [])
        self.assertIsNotNone(self.conn.execute('SELECT metadata_updated_at FROM public.videos WHERE video_id=%s',
                                               (self.video,)).fetchone()[0])

    def test_recovered_challenge_saves_metadata_and_continues_without_refetching_success(self):
        other = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')",
                          (other, self.channel))
        requests = []
        def respond(request):
            video_id = json.loads(request.content)["videoId"]
            requests.append(video_id)
            if len(requests) == 1:
                return httpx.Response(200, json={"playabilityStatus": {
                    "status": "LOGIN_REQUIRED", "reason": "Sign in to confirm you’re not a bot",
                }})
            return httpx.Response(200, json=payload(video_id))
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(video_id=[self.video, other], limit=100,
                                   concurrency=1, retries=2, client_version=CLIENT_VERSION,
                                   output=Path(folder)/"run")
            with patch("collect_video_metadata.open_database", return_value=nullcontext(self.conn)), \
                    patch("collect_video_metadata.httpx.AsyncClient", return_value=client), \
                    patch("collect_video_metadata.asyncio.sleep", new_callable=AsyncMock, wraps=asyncio.sleep) as sleep, \
                    patch("builtins.print"):
                self.assertEqual(asyncio.run(collect(args)), 0)
            self.assertTrue(all(call.args == (10,) for call in sleep.await_args_list))
            self.assertEqual(requests, [self.video, self.video, other])
            summary = json.loads((args.output/"summary.json").read_text())
            self.assertEqual(summary["http_attempts"], 3)
            self.assertEqual(summary["completed"], 2)
            self.assertEqual(summary["saved"], 2)
            self.assertEqual(summary["metadata_complete"], 2)
            self.assertEqual(summary["retry_counts"], {"0": 1, "1": 1})
            self.assertEqual(summary["unprocessed"], 0)
            self.assertIsNone(summary["stopped_reason"])
        rows = dict(self.conn.execute("SELECT video_id,title FROM public.videos WHERE video_id=ANY(%s)",
                                      ([self.video, other],)))
        self.assertEqual(rows, {self.video: "Test title", other: "Test title"})

    def test_fixed_selection_uses_128_workers_and_defers_new_videos(self):
        ids = [self.video] + [uuid.uuid4().hex[:11] for _ in range(139)]
        for video_id in ids[1:]:
            self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')",
                              (video_id, self.channel))
        late_id = uuid.uuid4().hex[:11]
        requests, active, peak = [], 0, 0
        async def fetch(client, video_id, client_version, retry_limit, *, on_attempt=None):
            nonlocal active, peak
            requests.append(video_id)
            active += 1
            peak = max(peak, active)
            if len(requests) == 1:
                self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'short')",
                                  (late_id, self.channel))
                args.video_id.append(late_id)
            await asyncio.sleep(0)
            active -= 1
            return {"video_id": video_id, "status": "ok", "rows_updated": 0,
                    "attempts": 1, "request_body_bytes": 100, "response_body_bytes": 100,
                    "decoded_body_bytes": 100, **parse_metadata(payload(video_id), video_id)}
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(video_id=ids.copy() + [self.video], limit=100,
                                   concurrency=128, retries=10, client_version=CLIENT_VERSION,
                                   output=Path(folder)/"run")
            with patch("collect_video_metadata.open_database", return_value=nullcontext(self.conn)), \
                    patch("collect_video_metadata.fetch_metadata", side_effect=fetch), patch("builtins.print"):
                self.assertEqual(asyncio.run(collect(args)), 0)
            self.assertEqual(Counter(requests), Counter(ids))
            self.assertEqual(peak, 128)
            self.assertEqual(select_videos(self.conn, args), [(late_id, 'short')])
            self.assertEqual(self.conn.execute('SELECT count(metadata_updated_at) FROM public.videos WHERE video_id=ANY(%s)',
                                               (ids,)).fetchone()[0], 140)
            summary = json.loads((args.output/'summary.json').read_text())
            self.assertEqual(summary['selected'], 140)
            self.assertEqual(summary['retry_counts'], {'0': 140})

    def test_database_error_stops_new_requests_without_logging_video_details(self):
        other = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')",
                          (other, self.channel))
        result = {"video_id": self.video, "status": "ok", "rows_updated": 0,
                  "attempts": 1, "request_body_bytes": 100, "response_body_bytes": 100,
                  "decoded_body_bytes": 100, **parse_metadata(payload(self.video), self.video)}
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(video_id=[self.video, other], limit=100,
                                   concurrency=1, retries=10, client_version=CLIENT_VERSION,
                                   output=Path(folder)/"run")
            with patch("collect_video_metadata.open_database", return_value=nullcontext(self.conn)), \
                    patch("collect_video_metadata.fetch_metadata", new_callable=AsyncMock, return_value=result) as fetch, \
                    patch("collect_video_metadata.save_metadata", side_effect=psycopg.OperationalError('private error detail')), \
                    patch("builtins.print") as printed:
                self.assertEqual(asyncio.run(collect(args)), 2)
            self.assertEqual(fetch.await_count, 1)
            summary = json.loads((args.output/'summary.json').read_text())
            self.assertEqual(summary['stopped_reason'], 'database_error')
            self.assertEqual(summary['unprocessed'], 1)
            self.assertEqual(summary['saved'], 0)
            logged = (args.output/'summary.json').read_text() + str(printed.call_args_list)
            self.assertNotIn(self.video, logged)
            self.assertNotIn('private error detail', logged)
        self.assertEqual(select_videos(self.conn, args), [(self.video, 'short'), (other, 'video')])

    def test_database_error_while_recording_failure_stops_new_requests(self):
        other = uuid.uuid4().hex[:11]
        self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) VALUES (%s,%s,'video')", (other,self.channel))
        result = {"video_id":self.video, "status":"error", "metadata":None, "rows_updated":0,
                  "attempts":1, "request_body_bytes":100, "response_body_bytes":0, "decoded_body_bytes":0,
                  "error":"ReadTimeout"}
        with tempfile.TemporaryDirectory() as folder:
            args = SimpleNamespace(video_id=[self.video,other], limit=100, concurrency=1, retries=0,
                                   client_version=CLIENT_VERSION, output=Path(folder)/'run')
            with patch('collect_video_metadata.open_database', return_value=nullcontext(self.conn)), \
                    patch('collect_video_metadata.fetch_metadata', new_callable=AsyncMock, return_value=result) as fetch, \
                    patch('collect_video_metadata.save_metadata_error', side_effect=psycopg.OperationalError('unavailable')), \
                    patch('builtins.print'):
                self.assertEqual(asyncio.run(collect(args)), 2)
            self.assertEqual(fetch.await_count, 1)
            summary = json.loads((args.output/'summary.json').read_text())
            self.assertEqual(summary['stopped_reason'], 'database_error')
            self.assertEqual(summary['unprocessed'], 1)


if __name__ == "__main__":
    unittest.main()
