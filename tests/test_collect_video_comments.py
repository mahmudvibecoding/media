import asyncio
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid

import httpx
import psycopg
from psycopg import sql

import collect_video_comments as comments


VIDEO = "ymjtt5KJiR4"
AUTHOR = "UC" + "a" * 22


def header(*, newest=False, count="40", token="newest"):
    return {"commentsHeaderRenderer": {
        "commentsCount": {"simpleText": count},
        "sortMenu": {"sortFilterSubMenuRenderer": {"subMenuItems": [
            {"title": "Top", "selected": not newest, "serviceEndpoint": {"continuationCommand": {"token": "top"}}},
            {"title": "Newest", "selected": newest, "serviceEndpoint": {"continuationCommand": {"token": token}}},
        ]}},
    }}


def action(items, *, target="comments-section", header=False, append=False):
    data = {"targetId": target, "continuationItems": items}
    if not append:
        data["slot"] = "RELOAD_CONTINUATION_SLOT_HEADER" if header else "RELOAD_CONTINUATION_SLOT_BODY"
    return {"appendContinuationItemsAction" if append else "reloadContinuationItemsCommand": data}


def initial():
    return {"onResponseReceivedEndpoints": [action([header()], header=True)]}


def page(ids=(), *, token=None, pinned=(), unknown=(), newest=True, with_header=True, message=None, legacy=False):
    items, mutations = [], []
    for cid in ids:
        if legacy:
            model = {"commentId": cid, "contentText": {"runs": [{"text": "Hello\n"}, {"text": "👋 " + cid}]},
                     "authorText": {"simpleText": "Author"}, "authorEndpoint": {"browseEndpoint": {"browseId": AUTHOR}}}
            if cid in pinned:
                model["pinnedCommentBadge"] = {"pinnedCommentBadgeRenderer": {}}
            thread = {"comment": {"commentRenderer": model}}
        else:
            model = {"commentId": cid, "commentKey": "key:" + cid}
            if cid in pinned:
                model["pinnedText"] = "Pinned by creator"
            if cid in unknown:
                model["pinnedText"] = None
            thread = {"commentViewModel": {"commentViewModel": model}}
            mutations.append({"entityKey": "key:" + cid, "payload": {"commentEntityPayload": {
                "key": "key:" + cid, "properties": {"commentId": cid, "content": {"content": "Hello\n👋 " + cid}, "replyLevel": 0},
                "author": {"channelId": AUTHOR, "displayName": "Author"},
            }}})
        # This branch must never be collected or used for pagination.
        thread["replies"] = {"commentRepliesRenderer": {"contents": [
            {"commentRenderer": {"commentId": "reply-to-" + cid, "contentText": {"simpleText": "Reply"}}},
            {"continuationItemRenderer": {"continuationEndpoint": {"continuationCommand": {"token": "reply-token"}}}},
        ]}}
        items.append({"commentThreadRenderer": thread})
    if token:
        items.append({"continuationItemRenderer": {"continuationEndpoint": {"continuationCommand": {"token": token}}}})
    if message is not None:
        items.append({"messageRenderer": {"text": {"simpleText": message}}})
    actions = [action([header(newest=newest)], header=True)] if with_header else []
    actions.append(action(items, append=not with_header))
    return {"onResponseReceivedEndpoints": actions,
            "frameworkUpdates": {"entityBatchUpdate": {"mutations": mutations}}}


def entity(payload, index=0):
    return payload["frameworkUpdates"]["entityBatchUpdate"]["mutations"][index]["payload"]["commentEntityPayload"]


class ParserTests(unittest.TestCase):
    def test_scoped_omitted_append_ends_pagination_but_cannot_select_sort(self):
        batch = {"targetId": "comments-section"}
        payload = {"onResponseReceivedEndpoints": [{"appendContinuationItemsAction": batch}]}
        self.assertEqual(comments.parse_page(payload, VIDEO),
                         {"comments": [], "continuation": None, "state": None})
        with self.assertRaisesRegex(comments.CommentError, "Missing comment items"):
            comments.comment_items(payload, selecting_sort=True)
        for invalid in ({"continuationItems": None}, {"unknownField": True}):
            batch.update(invalid)
            with self.assertRaisesRegex(comments.CommentError, "Missing comment items"):
                comments.parse_page(payload, VIDEO)
            for key in invalid:
                batch.pop(key)

    def test_zero_count_header_validates_omitted_empty_body(self):
        payload = {"onResponseReceivedEndpoints": [
            action([header(count="0 Comments", newest=True)], header=True),
            {"reloadContinuationItemsCommand": {"targetId": "comments-section",
                "slot": "RELOAD_CONTINUATION_SLOT_BODY"}},
        ]}
        result = comments.parse_page(payload, VIDEO)
        self.assertEqual(result["comments"], [])
        self.assertIsNone(result["continuation"])
        for count in ("40", "unknown"):
            payload["onResponseReceivedEndpoints"][0] = action([header(count=count, newest=True)], header=True)
            with self.assertRaisesRegex(comments.CommentError, "Missing comment items"):
                comments.parse_page(payload, VIDEO)
        payload["onResponseReceivedEndpoints"].pop(0)
        with self.assertRaisesRegex(comments.CommentError, "Missing comment items"):
            comments.parse_page(payload, VIDEO)

    def test_entity_order_is_not_thread_order_and_replies_are_excluded(self):
        payload = page(["a", "b"], pinned=["a"], token="next")
        mutations = payload["frameworkUpdates"]["entityBatchUpdate"]["mutations"]
        mutations.reverse()
        mutations.append({"entityKey": "reply", "payload": {"commentEntityPayload": {
            "properties": {"commentId": "reply", "content": {"content": "Reply"}, "replyLevel": 1}}}})
        payload["onResponseReceivedEndpoints"].append(action(
            [{"commentRenderer": {"commentId": "other-reply"}}], target="comment-replies-item-a"))
        result = comments.parse_page(payload, VIDEO)
        self.assertEqual([r["comment_id"] for r in result["comments"]], ["a", "b"])
        self.assertEqual(result["continuation"], "next")
        for row in result["comments"]:
            self.assertEqual(set(row), set(comments.FIELDS))
            self.assertEqual(row["author_channel_id"], AUTHOR)
        self.assertTrue(result["comments"][0]["is_pinned"])
        self.assertFalse(result["comments"][1]["is_pinned"])
        self.assertEqual(result["comments"][0]["text"], "Hello\n👋 a")

    def test_legacy_comments_and_pins(self):
        records = comments.parse_page(page(["a", "b"], pinned=["a"], legacy=True), VIDEO)["comments"]
        self.assertEqual([r["is_pinned"] for r in records], [True, False])
        self.assertEqual(records[0]["text"], "Hello\n👋 a")

    def test_nullable_author_unknown_pin_and_empty_text(self):
        payload = page(["a"], unknown=["a"])
        entity(payload).pop("author")
        entity(payload)["properties"]["content"]["content"] = ""
        row = comments.parse_page(payload, VIDEO)["comments"][0]
        self.assertEqual([row[k] for k in ("author_name", "author_channel_id", "is_pinned", "text")], [None, None, None, ""])

    def test_bad_shapes_cannot_be_successful_empty_pages(self):
        fixtures = [{}, {"onResponseReceivedEndpoints": []}, page(["a"], newest=False)]
        missing_entity = page(["a"])
        missing_entity.pop("frameworkUpdates")
        fixtures.append(missing_entity)
        for key, value in (("commentId", "other"), ("content", {}), ("replyLevel", 1)):
            payload = page(["a"])
            entity(payload)["properties"][key] = value
            fixtures.append(payload)
        payload = page(["a"])
        entity(payload)["properties"] = []
        fixtures.append(payload)
        payload = page(["a"])
        payload["onResponseReceivedEndpoints"][1]["reloadContinuationItemsCommand"]["continuationItems"].append({"unexpectedRenderer": {}})
        fixtures.append(payload)
        payload = page(["a"])
        payload["onResponseReceivedEndpoints"].append(action([]))
        fixtures.append(payload)
        payload = page(["a"])
        payload["currentVideoEndpoint"] = {"watchEndpoint": {"videoId": "anotherID00"}}
        fixtures.append(payload)
        fixtures.extend([page([], token="next"), page(["a"], message="Comments are turned off.")])
        for payload in fixtures:
            with self.subTest(payload=payload), self.assertRaises(comments.CommentError):
                comments.parse_page(payload, VIDEO)

    def test_field_mask_omits_unwanted_comment_fields(self):
        for name in ("replies", "publishedTime", "likeCount", "emojiPicker", "authorThumbnail"):
            self.assertNotIn(name, comments.COMMENT_FIELD_MASK)

    def test_unavailable_watch_message_and_identity_checks(self):
        payload = {"contents": {"twoColumnWatchNextResults": {"results": {"results": {"contents": [
            {"itemSectionRenderer": {"contents": [{"backgroundPromoRenderer": {
                "title": {"simpleText": "This video isn't available anymore"}}}]}}
        ]}}}}}
        self.assertEqual(comments.watch_status(payload, VIDEO), "unavailable")
        payload["contents"]["twoColumnWatchNextResults"]["results"]["results"]["contents"][0]["itemSectionRenderer"]["contents"][0]["backgroundPromoRenderer"]["title"]["simpleText"] = "Sign in to confirm you're not a bot"
        self.assertIsNone(comments.watch_status(payload, VIDEO))

    def test_live_disabled_message_without_section_target(self):
        payload = {"currentVideoEndpoint": {"watchEndpoint": {"videoId": VIDEO}},
            "contents": {"twoColumnWatchNextResults": {"results": {"results": {"contents": [
                {"itemSectionRenderer": {"contents": [{"messageRenderer": {"text": {"runs": [
                    {"text": "Comments are turned off. "}, {"text": "Learn more"}
                ]}}}]}}
            ]}}}}}
        self.assertEqual(comments.watch_status(payload, VIDEO), "disabled")
        del payload["currentVideoEndpoint"]
        self.assertIsNone(comments.watch_status(payload, VIDEO))


class CollectionTests(unittest.IsolatedAsyncioTestCase):
    def buffer(self, history=()):
        buffer = comments.CommentBuffer()
        self.addCleanup(buffer.close)
        buffer.load_history(history)
        return buffer

    async def collect(self, responses, *, buffer=None, **kwargs):
        self.requests = []
        iterator = iter(responses)
        async def handler(request):
            self.requests.append(json.loads(request.content))
            self.assertEqual(request.url.path, "/youtubei/v1/next")
            response = next(iterator)
            if isinstance(response, BaseException):
                raise response
            return response if isinstance(response, httpx.Response) else httpx.Response(200, json=response)
        buffer = buffer or self.buffer()
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await comments.collect_comments(client, VIDEO, buffer, **kwargs)
        return result, buffer

    async def test_direct_newest_consumes_first_response_without_setup_request(self):
        result, buffer = await self.collect([page(["new", "old"], token="unused")], buffer=self.buffer({"old": False}))
        self.assertEqual((result["complete"], result["pages"], result["stop_reason"]), (True, 1, "saved_history"))
        self.assertEqual((len(self.requests), result["attempts"], buffer.count()), (1, 1, 2))
        import base64
        raw = base64.b64decode(self.requests[0]["continuation"])
        self.assertIn(VIDEO.encode() + b"\x30\x01\x78\x02", raw)

    async def test_direct_newest_continues_to_next_page(self):
        result, buffer = await self.collect([page(["one"], token="second"), page(["two"], with_header=False)])
        self.assertEqual((result["complete"], result["pages"], result["comments"]), (True, 2, 2))
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.requests[1]["continuation"], "second")

    async def test_direct_newest_terminal_message_finishes_in_one_request(self):
        result, buffer = await self.collect([page([], message="No comments yet")])
        self.assertEqual((result["complete"], result["status"], result["attempts"]), (True, "empty", 1))
        self.assertEqual(buffer.count(), 0)

    async def test_first_scan_paginates_then_repeat_stops_at_saved_history(self):
        result, buffer = await self.collect([initial(), page(["pin", "new"], pinned=["pin"], token="second"),
                                             page(["old"], with_header=False)])
        self.assertTrue(result["complete"])
        self.assertEqual((result["comments"], result["pages"], result["stop_reason"]), (3, 2, "end"))
        self.assertEqual([r.get("continuation") for r in self.requests][1:], ["newest", "second"])
        history = {row[1]: row[-1] for row in buffer.rows()}
        result, repeat = await self.collect([initial(), page(["pin", "new", "arrival"], pinned=["pin"], token="unused")],
                                            buffer=self.buffer(history))
        self.assertEqual((result["pages"], result["stop_reason"]), (1, "saved_history"))
        self.assertEqual([r[1] for r in repeat.rows()], ["pin", "new", "arrival"])

    async def test_pinned_formerly_pinned_and_unknown_history_do_not_stop_scan(self):
        buffer = self.buffer({"pin": False, "was-pinned": True, "unknown-old": None, "unknown-now": False, "old": False})
        result, buffer = await self.collect([initial(), page(["pin", "was-pinned", "unknown-old", "unknown-now"],
            pinned=["pin"], unknown=["unknown-now"], token="second"), page(["old", "new-after-old"], token="unused", with_header=False)], buffer=buffer)
        self.assertEqual((result["complete"], result["pages"], result["stop_reason"]), (True, 2, "saved_history"))
        self.assertIn("new-after-old", [r[1] for r in buffer.rows()])

    async def test_duplicate_pinned_comment_later_in_scan_does_not_stop(self):
        result, buffer = await self.collect([initial(), page(["pin", "a"], pinned=["pin"], token="second"),
            page(["pin", "b"], token="third", with_header=False), page(["old"], with_header=False)],
            buffer=self.buffer({"pin": False, "old": False}))
        self.assertEqual(result["pages"], 3)
        self.assertEqual(result["comments"], 4)
        self.assertTrue(next(row[-1] for row in buffer.rows() if row[1] == "pin"))

    async def test_failed_page_retry_uses_same_token_and_deduplicates(self):
        result, buffer = await self.collect([initial(), page(["a"], token="second"),
            httpx.ReadTimeout("private transport details"), page(["b"], with_header=False)], retries=1)
        self.assertTrue(result["complete"])
        self.assertEqual(result["attempts"], 4)
        self.assertEqual([r.get("continuation") for r in self.requests][-2:], ["second", "second"])

    async def test_failed_scan_restart_does_not_use_its_partial_rows(self):
        result, partial = await self.collect([initial(), page(["new"], token="second"),
            httpx.ReadTimeout("contains credentials")], retries=0, buffer=self.buffer({"old": False}))
        self.assertFalse(result["complete"])
        self.assertEqual(result["status"], "request_error")
        self.assertNotIn("credentials", result["error"])
        self.assertEqual(partial.count(), 1)
        result, retried = await self.collect([initial(), page(["new"], token="second"),
            page(["gap", "old"], with_header=False)], buffer=self.buffer({"old": False}))
        self.assertTrue(result["complete"])
        self.assertIn("gap", [r[1] for r in retried.rows()])

    async def test_empty_disabled_unavailable_and_ambiguous_responses(self):
        unavailable = {"contents": {"twoColumnWatchNextResults": {"results": {"results": {"contents": [
            {"itemSectionRenderer": {"contents": [{"backgroundPromoRenderer": {"title": {"simpleText": "This video isn't available anymore"}}}]}}
        ]}}}}}
        cases = [
            ([initial(), page([], message="No comments yet")], "empty", True),
            ([initial(), page([], with_header=False)], "empty", True),
            ([{"onResponseReceivedEndpoints": [action([{"messageRenderer": {"text": {"simpleText": "Comments are turned off."}}}])]}], "disabled", True),
            ([{"onResponseReceivedEndpoints": [action([{"commentsHeaderRenderer": {"commentsCount": {"simpleText": "0"}}}], header=True)]}], "empty", True),
            ([{}, unavailable], "unavailable", False),
            ([{}, {}], "unexpected_response", False),
            ([initial(), {}], "unexpected_response", False),
        ]
        for responses, status, complete in cases:
            with self.subTest(status=status):
                result, _ = await self.collect(responses)
                self.assertEqual((result["status"], result["complete"]), (status, complete))

    async def test_limit_and_pagination_loops_are_incomplete(self):
        cases = [
            ([initial(), page(["a"], token="next")], {"max_pages": 1}, "page_limit"),
            ([initial(), page(["a"], token="newest")], {}, "pagination_loop"),
            ([initial(), page(["a"], token="next"), page(["a"], token="third", with_header=False)], {}, "pagination_loop"),
        ]
        for responses, kwargs, status in cases:
            with self.subTest(status=status):
                result, _ = await self.collect(responses, **kwargs)
                self.assertEqual(result["status"], status)
                self.assertFalse(result["complete"])

    async def test_blocked_html_oversized_and_server_retry(self):
        cases = [([httpx.Response(429)], "blocked"), ([httpx.Response(200, text="<html>challenge</html>")], "unexpected_response"),
                 ([httpx.Response(503), httpx.Response(503)], "http_error")]
        for responses, status in cases:
            with self.subTest(status=status):
                result, _ = await self.collect(responses, retries=1)
                self.assertEqual(result["status"], status)
                self.assertFalse(result["complete"])
        with patch.object(comments, "MAX_BODY_BYTES", 8):
            result, _ = await self.collect([initial()])
        self.assertEqual(result["status"], "unexpected_response")
        result, _ = await self.collect([httpx.Response(503), initial(), page(["a"])], retries=1)
        self.assertTrue(result["complete"])

    async def test_cancel_propagates_without_marking_completion(self):
        buffer = self.buffer()
        with self.assertRaises(asyncio.CancelledError):
            await self.collect([initial(), page(["a"], token="second"), asyncio.CancelledError()], buffer=buffer)
        self.assertEqual(buffer.count(), 1)

    async def test_total_request_timeout(self):
        async def handler(request):
            await asyncio.sleep(0.1)
            return httpx.Response(200, json=initial())
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await comments.collect_comments(client, VIDEO, self.buffer(), retries=0, timeout=0.001)
        self.assertEqual(result["status"], "request_error")
        self.assertIn("timeout", result["error"])


class SchemaConnection:
    def __init__(self, conn, schema):
        self.conn, self.schema = conn, schema

    def execute(self, source, params=None):
        return self.conn.execute(source.replace("public.", f'"{self.schema}".'), params)

    def transaction(self):
        return self.conn.transaction()


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class PersistenceTests(unittest.TestCase):
    def setUp(self):
        raw = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"])
        self.addCleanup(raw.close)
        transaction = raw.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__, None, None, None)
        schema = "comment_collector_test_" + uuid.uuid4().hex
        raw.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        self.conn = SchemaConnection(raw, schema)
        self.conn.execute((Path(comments.ROOT) / "db/schema.sql").read_text())
        self.conn.execute("INSERT INTO public.channels(channel_id) VALUES (%s)", (AUTHOR,))
        self.conn.execute("INSERT INTO public.videos(video_id,channel_id,type) VALUES (%s,%s,'video')", (VIDEO, AUTHOR))
        self.old_time = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def buffer(self, ids, *, pinned=()):
        buffer = comments.CommentBuffer()
        self.addCleanup(buffer.close)
        buffer.add_page(comments.parse_page(page(ids, pinned=pinned), VIDEO)["comments"])
        return buffer

    def progress(self):
        return self.conn.execute("SELECT comments_updated_at,comments_error FROM public.videos WHERE video_id=%s", (VIDEO,)).fetchone()

    def complete(self, buffer):
        return comments.save_scan(self.conn, {"video_id": VIDEO, "status": "ok", "complete": True}, buffer)

    def test_success_import_repeat_refresh_and_completion_are_atomic(self):
        self.assertEqual(self.complete(self.buffer(["a", "b"], pinned=["a"])), 2)
        self.assertIsNotNone(self.progress()[0])
        self.assertIsNone(self.progress()[1])
        buffer = self.buffer(["a", "c"])
        buffer.conn.execute("UPDATE comments SET text='Edited' WHERE comment_id='a'")
        self.assertEqual(self.complete(buffer), 1)
        self.assertEqual(self.conn.execute("SELECT comment_id,text,is_pinned FROM public.comments ORDER BY comment_id").fetchall(),
                         [("a", "Edited", False), ("b", "Hello\n👋 b", False), ("c", "Hello\n👋 c", False)])

    def test_failure_preserves_history_and_completion_time_then_retry_fills_gap(self):
        self.complete(self.buffer(["old"]))
        self.conn.execute("UPDATE public.videos SET comments_updated_at=%s", (self.old_time,))
        partial = self.buffer(["new"])
        for status in ("request_error", "page_limit", "interrupted", "unavailable", "unexpected_response"):
            result = {"video_id": VIDEO, "status": status, "complete": False, "error": "Test failure"}
            self.assertEqual(comments.save_scan(self.conn, result, partial), 0)
            self.assertEqual(self.progress(), (self.old_time, status + ": Test failure"))
            self.assertEqual(self.conn.execute("SELECT comment_id FROM public.comments").fetchall(), [("old",)])
        self.assertEqual(self.complete(self.buffer(["new", "gap", "old"])), 2)
        self.assertIsNone(self.progress()[1])

    def test_invalid_import_rolls_back_rows_and_success_timestamp(self):
        buffer = self.buffer(["a", "b"])
        buffer.conn.execute("UPDATE comments SET video_id='other-video' WHERE comment_id='b'")
        with self.assertRaises(ValueError):
            self.complete(buffer)
        self.assertEqual(self.progress(), (None, None))
        self.assertEqual(self.conn.execute("SELECT count(*) FROM public.comments").fetchone()[0], 0)

    def test_only_completed_history_is_loaded(self):
        self.conn.execute("INSERT INTO public.comments(video_id,comment_id,text,is_pinned) VALUES (%s,'partial','Partial',false)", (VIDEO,))
        buffer = self.buffer([])
        comments.load_saved_history(self.conn, VIDEO, buffer)
        self.assertEqual(buffer.conn.execute("SELECT count(*) FROM history").fetchone()[0], 0)
        self.conn.execute("UPDATE public.videos SET comments_updated_at=%s", (self.old_time,))
        comments.load_saved_history(self.conn, VIDEO, buffer)
        self.assertEqual(buffer.conn.execute("SELECT * FROM history").fetchall(), [("partial", 0)])
        with self.assertRaises(ValueError):
            comments.load_saved_history(self.conn, "missing", self.buffer([]))

    def test_empty_and_disabled_complete_without_deleting_saved_comments(self):
        self.complete(self.buffer(["old"]))
        for status in ("empty", "disabled"):
            self.assertEqual(comments.save_scan(self.conn, {"video_id": VIDEO, "status": status, "complete": True}, self.buffer([])), 0)
            self.assertIsNotNone(self.progress()[0])
            self.assertIsNone(self.progress()[1])
            self.assertEqual(self.conn.execute("SELECT count(*) FROM public.comments").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
