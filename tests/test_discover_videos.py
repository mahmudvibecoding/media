import copy
import gzip
import json
import os
import unittest
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import httpx
import psycopg

from discover_videos import (
    MAX_BODY_BYTES, ResponseShapeError, SortNotVerified, TABS,
    fetch_page, field_mask, parse_page, save_videos,
)


CHANNEL = "UCVPst_iSyaVYpuOP4ogRhlw"
FIRST = "RtXBV0X1v1Q"
SECOND = "l1gixBkAMH8"


def card(video_id=FIRST, renderer="lockupViewModel"):
    value = {"contentId": video_id} if renderer == "lockupViewModel" else {"videoId": video_id}
    if renderer == "shortsLockupViewModel":
        value = {"onTap": {"innertubeCommand": {"reelWatchEndpoint": {"videoId": video_id}}}}
    return {"richItemRenderer": {"content": {renderer: value}}}


def page(video_type="video", items=None, selected="Latest"):
    return {"contents": {"twoColumnBrowseResultsRenderer": {"tabs": [{"tabRenderer": {
        "title": TABS[video_type]["title"], "selected": True,
        "content": {"richGridRenderer": {
            "contents": [card()] if items is None else items,
            "header": {"chipBarViewModel": {"chips": [{"chipViewModel": {
                "text": selected, "selected": True,
            }}]}},
        }},
    }}]}}}


def grid(payload):
    return payload["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][0]["tabRenderer"]["content"]["richGridRenderer"]


class DiscoveryParsingTests(unittest.TestCase):
    def test_video_renderer_variants_preserve_ids_and_type(self):
        for renderer in ("videoRenderer", "lockupViewModel"):
            result = parse_page(page(items=[card(renderer=renderer)]), CHANNEL, "video")
            self.assertEqual(result["videos"], [{"video_id": FIRST, "channel_id": CHANNEL, "type": "video"}])
            self.assertTrue(result["latest_verified"])

    def test_shorts_renderer_variants_preserve_ids_and_type(self):
        for renderer in ("reelItemRenderer", "shortsLockupViewModel"):
            result = parse_page(page("short", [card(SECOND, renderer)]), CHANNEL, "short")
            self.assertEqual(result["videos"], [{"video_id": SECOND, "channel_id": CHANNEL, "type": "short"}])

    def test_duplicate_cards_and_pagination(self):
        token = "opaque-pagination-token"
        continuation = {"continuationItemRenderer": {"continuationEndpoint": {
            "continuationCommand": {"token": token}
        }}}
        result = parse_page(page(items=[card(), card(), card(SECOND), continuation]), CHANNEL, "video")
        self.assertEqual([v["video_id"] for v in result["videos"]], [FIRST, SECOND])
        self.assertEqual(result["continuation"], token)

    def test_ignores_recommendations_and_unselected_tabs(self):
        payload = page()
        unrelated = copy.deepcopy(payload["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][0])
        unrelated["tabRenderer"].update(title="Home", selected=False)
        unrelated["tabRenderer"]["content"]["richGridRenderer"]["contents"] = [card(SECOND)]
        payload["contents"]["twoColumnBrowseResultsRenderer"]["tabs"].insert(0, unrelated)
        payload["recommendations"] = {"videoId": SECOND}
        self.assertEqual(len(parse_page(payload, CHANNEL, "video")["videos"]), 1)

    def test_old_sort_chips_are_supported(self):
        payload = page()
        grid(payload)["header"] = {"feedFilterChipBarRenderer": {"contents": [{
            "chipCloudChipRenderer": {"text": {"runs": [{"text": "Latest"}]}, "isSelected": True}
        }]}}
        self.assertTrue(parse_page(payload, CHANNEL, "video")["latest_verified"])

    def dropdown_page(self, selected):
        payload = page()
        grid(payload)["header"] = {"chipBarViewModel": {"chips": [
            {"chipViewModel": {"text": "Latest", "tapCommand": {"innertubeCommand": {
                "showSheetCommand": {"panelLoadingStrategy": {"inlineContent": {
                    "sheetViewModel": {"content": {"listViewModel": {"listItems": [
                        {"listItemViewModel": {"title": {"content": title}, "isSelected": title in selected}}
                        for title in ("Latest", "Popular", "Oldest")
                    ]}}}
                }}}
            }}}},
            {"chipViewModel": {"text": "Members only", "selected": False}},
            {"chipViewModel": {"text": "Public", "selected": False}},
        ]}}
        return payload

    def test_latest_selected_inside_dropdown_is_supported(self):
        result = parse_page(self.dropdown_page(["Latest"]), CHANNEL, "video")
        self.assertTrue(result["latest_verified"])
        self.assertEqual(result["videos"][0]["video_id"], FIRST)

    def test_dropdown_label_alone_does_not_verify_order(self):
        for selected in ([], ["Popular"], ["Latest", "Popular"]):
            with self.subTest(selected=selected), self.assertRaises(SortNotVerified):
                parse_page(self.dropdown_page(selected), CHANNEL, "video")

    def test_popular_and_unknown_sort_controls_are_rejected(self):
        with self.assertRaises(SortNotVerified):
            parse_page(page(selected="Popular"), CHANNEL, "video")
        payload = page()
        grid(payload)["header"] = {}
        with self.assertRaises(SortNotVerified):
            parse_page(payload, CHANNEL, "video")

    def test_complete_tab_without_sort_controls_is_accepted(self):
        payload = page(items=[card(), card(SECOND)])
        grid(payload).pop("header")
        result = parse_page(payload, CHANNEL, "video")
        self.assertEqual(len(result["videos"]), 2)
        self.assertTrue(result["complete_tab"])
        self.assertFalse(result["latest_verified"])

    def test_paginated_tab_without_sort_controls_is_rejected(self):
        payload = page(items=[card(), {"continuationItemRenderer": {"continuationEndpoint": {
            "continuationCommand": {"token": "next-page"}
        }}}])
        grid(payload).pop("header")
        with self.assertRaises(SortNotVerified):
            parse_page(payload, CHANNEL, "video")

    def test_home_redirect_means_tab_absent(self):
        payload = page()
        payload["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][0]["tabRenderer"]["title"] = "Home"
        result = parse_page(payload, CHANNEL, "short")
        self.assertEqual(result["status"], "tab_absent")
        self.assertEqual(result["videos"], [])

    def test_unselected_requested_tab_is_not_accepted(self):
        payload = page()
        payload["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][0]["tabRenderer"]["selected"] = False
        with self.assertRaises(ResponseShapeError):
            parse_page(payload, CHANNEL, "video")

    def test_unavailable_channel_is_not_empty(self):
        result = parse_page({"alerts": [{"alertRenderer": {
            "type": "ERROR", "text": {"simpleText": "This channel does not exist."}
        }}]}, CHANNEL, "video")
        self.assertEqual(result["status"], "unavailable")
        self.assertEqual(result["videos"], [])

    def test_missing_or_masked_content_is_not_empty(self):
        payloads = [{}, page(items=[{}]), page(items=[{"richItemRenderer": {"content": {}}}]),
                    page(items=[card("not-a-video-id")])]
        for payload in payloads:
            with self.subTest(payload=payload), self.assertRaises(ResponseShapeError):
                parse_page(payload, CHANNEL, "video")

    def test_explicit_empty_grid(self):
        self.assertEqual(parse_page(page(items=[]), CHANNEL, "video")["status"], "empty")

    def test_channel_owner_empty_state_requires_explicit_empty_message(self):
        payload = page("short")
        tab = payload["contents"]["twoColumnBrowseResultsRenderer"]["tabs"][0]["tabRenderer"]
        description = {"simpleText": "This channel has no videos."}
        tab["content"] = {"sectionListRenderer": {"contents": [
            {"channelOwnerEmptyStateRenderer": {"description": description}}
        ]}}
        result = parse_page(payload, CHANNEL, "short")
        self.assertEqual(result["status"], "empty")
        self.assertTrue(result["complete_tab"])
        description["simpleText"] = "Unable to load this channel."
        with self.assertRaises(ResponseShapeError):
            parse_page(payload, CHANNEL, "short")

    def test_pagination_without_cards_is_not_empty(self):
        payload = page(items=[{"continuationItemRenderer": {"continuationEndpoint": {
            "continuationCommand": {"token": "token"}
        }}}])
        with self.assertRaises(ResponseShapeError):
            parse_page(payload, CHANNEL, "video")


class DiscoveryHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_filters_response_and_decompresses_gzip(self):
        def respond(request):
            body = json.loads(request.content)
            self.assertEqual(body["browseId"], CHANNEL)
            self.assertEqual(body["params"], TABS["video"]["params"])
            self.assertEqual(request.url.params["fields"], field_mask("video"))
            self.assertEqual(request.url.params["prettyPrint"], "false")
            return httpx.Response(200, content=gzip.compress(json.dumps(page()).encode()),
                                  headers={"Content-Encoding": "gzip"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await fetch_page(client, CHANNEL, "video")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["attempts"], 1)
        self.assertEqual(result["videos"][0]["video_id"], FIRST)

    async def test_rate_limit_and_auth_failures_stop_without_retry(self):
        for status in (400, 401, 403, 429):
            async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda request: httpx.Response(status, headers={"Retry-After": "60"})
            )) as client:
                result = await fetch_page(client, CHANNEL, "video")
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(result["attempts"], 1)

    async def test_transient_error_retries(self):
        responses = iter([httpx.Response(503), httpx.Response(200, json=page())])
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: next(responses))) as client:
            with patch("discover_videos.asyncio.sleep", new_callable=AsyncMock):
                result = await fetch_page(client, CHANNEL, "video")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["attempts"], 2)

    async def test_invalid_json_or_response_shape_is_not_success(self):
        for response in (httpx.Response(200, text="not JSON"), httpx.Response(200, json={})):
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
                result = await fetch_page(client, CHANNEL, "video")
            self.assertEqual(result["status"], "unexpected_response")
            self.assertEqual(result["videos"], [])

    async def test_unverified_sort_does_not_release_ids(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=page(selected="Popular"))
        )) as client:
            result = await fetch_page(client, CHANNEL, "video")
        self.assertEqual(result["status"], "sort_unverified")
        self.assertEqual(result["videos"], [])

    async def test_response_size_limit(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=b"x" * (MAX_BODY_BYTES + 1))
        )) as client:
            result = await fetch_page(client, CHANNEL, "video")
        self.assertEqual(result["status"], "unexpected_response")
        self.assertEqual(result["attempts"], 1)


class DiscoverySaveTests(unittest.TestCase):
    def test_unsuccessful_pages_never_write(self):
        for status in ("blocked", "error", "unexpected_response", "sort_unverified",
                       "empty", "tab_absent", "unavailable"):
            conn = Mock()
            result = {"status": status, "videos": [{"video_id": FIRST}]}
            self.assertEqual(save_videos(conn, result), [])
            conn.execute.assert_not_called()

    def test_no_insert_for_empty_id_list(self):
        conn = Mock()
        self.assertEqual(save_videos(conn, {"status": "ok", "videos": []}), [])
        conn.execute.assert_not_called()


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class DiscoveryDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"])
        self.addCleanup(self.conn.close)
        transaction = self.conn.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__, None, None, None)
        row = self.conn.execute("SELECT channel_id FROM public.channels ORDER BY channel_id LIMIT 1").fetchone()
        if row is None:
            self.skipTest("Database needs at least one channel")
        self.channel = row[0]
        self.ids = [uuid.uuid4().hex[:11] for _ in range(3)]

    def result(self, ids, video_type="video"):
        return {"status": "ok", "channel_id": self.channel, "type": video_type,
                "videos": [{"video_id": video_id, "channel_id": self.channel, "type": video_type}
                           for video_id in ids]}

    def test_initial_batch_repeat_and_shared_id_space(self):
        result = self.result(self.ids[:2])
        self.assertEqual(set(save_videos(self.conn, result)), set(self.ids[:2]))
        self.assertEqual(save_videos(self.conn, result), [])
        # A known ID on the other tab is skipped, while a new Short is saved.
        self.assertEqual(save_videos(self.conn, self.result([self.ids[0], self.ids[2]], "short")),
                         [self.ids[2]])
        rows = self.conn.execute(
            "SELECT video_id, channel_id, type, published_at FROM public.videos WHERE video_id = ANY(%s)",
            (self.ids,),
        ).fetchall()
        self.assertEqual(len(rows), 3)
        types = {row[0]: row[2] for row in rows}
        self.assertEqual(types, {self.ids[0]: "video", self.ids[1]: "video", self.ids[2]: "short"})
        self.assertTrue(all(row[1] == self.channel and row[3] is None for row in rows))

    def test_existing_metadata_is_preserved(self):
        published = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        self.conn.execute(
            "INSERT INTO public.videos (video_id, channel_id, type, published_at) VALUES (%s,%s,%s,%s)",
            (self.ids[0], self.channel, "short", published),
        )
        self.assertEqual(save_videos(self.conn, self.result([self.ids[0]], "video")), [])
        row = self.conn.execute("SELECT type, published_at FROM public.videos WHERE video_id = %s",
                                (self.ids[0],)).fetchone()
        self.assertEqual(row, ("short", published))


if __name__ == "__main__":
    unittest.main()
