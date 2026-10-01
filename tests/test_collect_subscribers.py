import gzip
import json
import unittest
from unittest.mock import Mock, AsyncMock, patch

import httpx

from collect_subscribers import ResponseShapeError, extract_count, fetch_count, parse_count, save_count


def modern(*texts):
    return {"header": {"pageHeaderRenderer": {"content": {"pageHeaderViewModel": {
        "metadata": {"contentMetadataViewModel": {"metadataRows": [
            {"metadataParts": [{"text": {"content": text}} for text in texts]}
        ]}}
    }}}}}


class CountTests(unittest.TestCase):
    def test_public_count_formats(self):
        for text, expected in [
            ("4.16M subscribers", 4160000), ("12.3K subscribers", 12300),
            ("1,234 subscribers", 1234), ("123 subscribers", 123),
            ("1 subscriber", 1), ("0 subscribers", 0), ("No subscribers", 0),
            ("1.23\u00a0M subscribers", 1230000), ("1B subscribers", 1000000000),
        ]:
            with self.subTest(text=text):
                self.assertEqual(parse_count(text), expected)

    def test_reject_invalid_and_ambiguous_numbers(self):
        for text in ["-2 subscribers", "1,23 subscribers", "1.5 subscribers",
                     "lots of subscribers", "1K views", "9223372036854775808 subscribers"]:
            with self.subTest(text=text), self.assertRaises(ResponseShapeError):
                parse_count(text)

    def test_metadata_order_is_not_significant(self):
        self.assertEqual(extract_count(modern("13K videos", "@example", "4.16M subscribers")), 4160000)

    def test_missing_count_is_unknown(self):
        self.assertIsNone(extract_count(modern("@example", "13K videos")))

    def test_empty_payload_is_not_zero_subscribers(self):
        with self.assertRaises(ResponseShapeError):
            extract_count({})

    def test_conflicting_counts_fail(self):
        with self.assertRaises(ResponseShapeError):
            extract_count(modern("2K subscribers", "3K subscribers"))

    def test_legacy_header(self):
        self.assertEqual(extract_count({"header": {"c4TabbedHeaderRenderer": {
            "subscriberCountText": {"runs": [{"text": "10K"}, {"text": " subscribers"}]}
        }}}), 10000)

    def test_failed_or_missing_results_never_write(self):
        for status in ["error", "blocked", "missing_count", "unexpected_response", "unavailable"]:
            connection = Mock()
            self.assertEqual(save_count(connection, {"status": status, "subscriber_count": None}), 0)
            connection.execute.assert_not_called()

    def test_explicit_zero_is_written(self):
        connection = Mock()
        connection.execute.return_value.rowcount = 1
        self.assertEqual(save_count(connection, {
            "status": "ok", "subscriber_count": 0, "channel_id": "example"
        }), 1)
        self.assertEqual(connection.execute.call_args.args[1], (0, "example"))


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_compressed_response(self):
        payload = gzip.compress(json.dumps(modern("4.16M subscribers")).encode())
        def respond(request):
            self.assertEqual(json.loads(request.content)["browseId"], "example")
            self.assertIn("fields", request.url.params)
            return httpx.Response(200, content=payload, headers={"Content-Encoding": "gzip"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await fetch_count(client, "example")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["subscriber_count"], 4160000)
        self.assertEqual(result["attempts"], 1)

    async def test_rate_limit_stops_without_retry(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(429, headers={"Retry-After": "60"})
        )) as client:
            result = await fetch_count(client, "example")
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["attempts"], 1)
        self.assertEqual(result["retry_after"], "60")

    async def test_empty_success_is_unexpected(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={})
        )) as client:
            result = await fetch_count(client, "example")
        self.assertEqual(result["status"], "unexpected_response")
        self.assertIsNone(result["subscriber_count"])

    async def test_temporary_server_error_retries(self):
        responses = iter([httpx.Response(503), httpx.Response(200, json=modern("12 subscribers"))])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: next(responses))) as client:
            with patch("collect_subscribers.asyncio.sleep", new_callable=AsyncMock):
                result = await fetch_count(client, "example")
        self.assertEqual(result["subscriber_count"], 12)
        self.assertEqual(result["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
