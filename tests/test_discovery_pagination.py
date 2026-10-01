import asyncio
from contextlib import nullcontext
import json
import os
import unittest
import uuid
from unittest.mock import patch

import httpx
import psycopg

from discover_videos import (
    ResponseShapeError, fetch_page, field_mask, parse_continuation, save_videos, scan_tab, select_tabs,
)


def items(ids, token=None, kind="video"):
    cards = []
    for video_id in ids:
        content = {"lockupViewModel": {"contentId": video_id}}
        if kind == "short":
            content = {"shortsLockupViewModel": {"onTap": {"innertubeCommand": {
                "reelWatchEndpoint": {"videoId": video_id}
            }}}}
        cards.append({"richItemRenderer": {"content": content}})
    if token:
        cards.append({"continuationItemRenderer": {"continuationEndpoint": {
            "continuationCommand": {"token": token}
        }}})
    return cards


def first_page(ids, token=None, kind="video"):
    return {"contents": {"twoColumnBrowseResultsRenderer": {"tabs": [{"tabRenderer": {
        "title": "Videos" if kind == "video" else "Shorts", "selected": True,
        "content": {"richGridRenderer": {
            "contents": items(ids, token, kind),
            "header": {"chipBarViewModel": {"chips": [{"chipViewModel": {
                "text": "Latest", "selected": True,
            }}]}},
        }},
    }}]}}}


def next_page(ids, token=None, kind="video", root="onResponseReceivedActions"):
    return {root: [{"appendContinuationItemsAction": {"continuationItems": items(ids, token, kind)}}]}


class ContinuationTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_types_and_continuation_envelopes(self):
        for kind in ("video", "short"):
            for root in ("onResponseReceivedActions", "onResponseReceivedEndpoints"):
                result = parse_continuation(next_page(["RtXBV0X1v1Q"], "next", kind, root), "channel", kind)
                self.assertEqual(result["videos"], [{"video_id": "RtXBV0X1v1Q", "channel_id": "channel", "type": kind}])
                self.assertEqual(result["continuation"], "next")

    async def test_filtered_continuation_request_uses_returned_token(self):
        def respond(request):
            body = json.loads(request.content)
            self.assertEqual(body["continuation"], "opaque-token")
            self.assertNotIn("browseId", body)
            self.assertNotIn("params", body)
            self.assertEqual(request.url.params["fields"], field_mask("video", continuation=True))
            return httpx.Response(200, json=next_page(["RtXBV0X1v1Q"]))
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await fetch_page(client, "channel", "video", continuation="opaque-token")
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["complete_tab"])

    async def test_missing_and_ambiguous_continuations_are_rejected(self):
        both = {**next_page([]), **next_page([], root="onResponseReceivedEndpoints")}
        for payload in ({}, {"alerts": [{}]}, both, {"onResponseReceivedActions": []}):
            with self.subTest(payload=payload), self.assertRaises(ResponseShapeError):
                parse_continuation(payload, "channel", "video")


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class PaginationDatabaseTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.conn = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"])
        self.addCleanup(self.conn.close)
        transaction = self.conn.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__, None, None, None)
        self.channel = "UC" + uuid.uuid4().hex[:22]
        self.ids = [uuid.uuid4().hex[:11] for _ in range(5)]
        self.conn.execute("INSERT INTO public.channels (channel_id) VALUES (%s)", (self.channel,))

    def initialize(self, ids=()):
        self.conn.execute("INSERT INTO public.channel_scan_state (channel_id,type) VALUES (%s,'video')",
                          (self.channel,))
        if ids:
            self.conn.execute("INSERT INTO public.videos (video_id,channel_id,type) SELECT unnest(%s::text[]),%s,'video'",
                              (list(ids), self.channel))

    def stored_ids(self):
        return {r[0] for r in self.conn.execute("SELECT video_id FROM public.videos WHERE channel_id=%s", (self.channel,))}

    def initialized(self):
        return self.conn.execute("SELECT EXISTS (SELECT 1 FROM public.channel_scan_state WHERE channel_id=%s)",
                                 (self.channel,)).fetchone()[0]

    def test_resume_selects_only_unfinished_tabs(self):
        with patch("discover_videos.open_database", return_value=nullcontext(self.conn)):
            self.assertEqual(select_tabs([self.channel], initial_only=True),
                             [(self.channel, "video"), (self.channel, "short")])
            self.initialize()
            self.assertEqual(select_tabs([self.channel], initial_only=True), [(self.channel, "short")])
            self.assertEqual(select_tabs([self.channel]),
                             [(self.channel, "video"), (self.channel, "short")])
            self.conn.execute("INSERT INTO public.channel_scan_state (channel_id,type) VALUES (%s,'short')",
                              (self.channel,))
            self.assertEqual(select_tabs([self.channel], initial_only=True), [])

    async def scan(self, responses, **kwargs):
        self.request_tokens = []
        async def respond(request):
            token = json.loads(request.content).get("continuation")
            self.request_tokens.append(token)
            response = responses[token]
            if isinstance(response, BaseException):
                raise response
            if isinstance(response, httpx.Response):
                return response
            return httpx.Response(200, json=response)
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            return await scan_tab(client, self.conn, self.channel, "video", retries=0, **kwargs)

    async def test_initial_scan_saves_only_one_page_and_repeat_stops_at_known_range(self):
        responses = {None: first_page(self.ids[:2], "next")}
        result = await self.scan(responses)
        self.assertEqual(result["stop_reason"], "initial_page")
        self.assertEqual(result["videos_inserted"], 2)
        self.assertTrue(self.initialized())
        self.assertEqual(self.request_tokens, [None])
        repeated = await self.scan(responses)
        self.assertEqual(repeated["stop_reason"], "known_range")
        self.assertEqual(repeated["videos_inserted"], 0)
        self.assertEqual(self.request_tokens, [None])

    async def test_previously_empty_tab_collects_multiple_pages(self):
        empty = await self.scan({None: first_page([])})
        self.assertTrue(empty["scan_complete"])
        self.assertTrue(self.initialized())
        result = await self.scan({None: first_page(self.ids[:2], "next"), "next": next_page(self.ids[2:4])})
        self.assertEqual(result["pages_fetched"], 2)
        self.assertEqual(result["stop_reason"], "end_of_tab")
        self.assertEqual(self.stored_ids(), set(self.ids[:4]))

    async def test_whole_pages_are_processed_before_stopping(self):
        old, a, b = self.ids[:3]
        self.initialize([old])
        # An existing ID at the start does not hide new IDs later in this page or the next.
        result = await self.scan({None: first_page([old, a], "next"),
                                  "next": next_page([b, old], "older")})
        self.assertEqual(self.request_tokens, [None, "next"])
        self.assertEqual(result["stop_reason"], "known_range")
        self.assertEqual(set(result["inserted_video_ids"]), {a, b})
        self.assertEqual(self.stored_ids(), {old, a, b})

    async def test_failed_second_page_saves_nothing_and_restart_recovers_all_ids(self):
        old, a, b = self.ids[:3]
        self.initialize([old])
        result = await self.scan({None: first_page([a], "next"), "next": httpx.Response(503)})
        self.assertFalse(result["scan_complete"])
        self.assertEqual(result["videos_inserted"], 0)
        self.assertEqual(self.stored_ids(), {old})
        recovered = await self.scan({None: first_page([a], "fresh-token"),
                                    "fresh-token": next_page([b, old], "older")})
        self.assertEqual(self.request_tokens, [None, "fresh-token"])
        self.assertEqual(set(recovered["inserted_video_ids"]), {a, b})

    async def test_cancellation_does_not_save_partial_results(self):
        old, a, b = self.ids[:3]
        self.initialize([old])
        with self.assertRaises(asyncio.CancelledError):
            await self.scan({None: first_page([a], "next"), "next": asyncio.CancelledError()})
        self.assertEqual(self.stored_ids(), {old})
        recovered = await self.scan({None: first_page([a], "next"), "next": next_page([b, old])})
        self.assertEqual(set(recovered["inserted_video_ids"]), {a, b})

    async def test_page_limit_is_incomplete_and_does_not_write(self):
        self.initialize([self.ids[0]])
        result = await self.scan({None: first_page([self.ids[1]], "next")}, max_pages=1)
        self.assertEqual(result["status"], "page_limit")
        self.assertFalse(result["scan_complete"])
        self.assertEqual(self.stored_ids(), {self.ids[0]})

    async def test_repeated_token_and_repeated_page_fail_without_writing(self):
        self.initialize([self.ids[0]])
        for second in (next_page([self.ids[2]], "next"), next_page([self.ids[1]], "different-token")):
            result = await self.scan({None: first_page([self.ids[1]], "next"), "next": second})
            self.assertEqual(result["status"], "pagination_loop")
            self.assertEqual(self.stored_ids(), {self.ids[0]})

    async def test_invalid_continuation_and_failed_first_scan_do_not_advance_state(self):
        first = await self.scan({None: {}})
        self.assertEqual(first["status"], "unexpected_response")
        self.assertFalse(self.initialized())
        self.initialize([self.ids[0]])
        later = await self.scan({None: first_page([self.ids[1]], "next"), "next": {}})
        self.assertEqual(later["status"], "unexpected_response")
        self.assertEqual(self.stored_ids(), {self.ids[0]})

    async def test_database_failure_rolls_back_ids_and_initialization(self):
        def fail_after_insert(conn, result):
            save_videos(conn, result)
            raise psycopg.IntegrityError("simulated failure before scan initialization")
        with patch("discover_videos.save_videos", side_effect=fail_after_insert):
            result = await self.scan({None: first_page(self.ids[:2])})
        self.assertEqual(result["status"], "database_error")
        self.assertFalse(result["scan_complete"])
        self.assertFalse(self.initialized())
        self.assertEqual(self.stored_ids(), set())


if __name__ == "__main__":
    unittest.main()
