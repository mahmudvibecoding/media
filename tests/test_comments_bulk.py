import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext, redirect_stdout
from datetime import datetime, timezone
import gzip
import io
import json
import os
from pathlib import Path
import ssl
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import uuid

import httpx
import psycopg
from psycopg import sql

import comments_bulk as bulk
import comments_bulk_import as importer
from collect_video_comments import CommentError, fetch_json, CLIENT_VERSION
from metadata_bulk import atomic_json, digest, export_events
from proxy_catalog import CatalogProxy
from proxy_statistics import AttemptOutcome
from test_collect_video_comments import AUTHOR, VIDEO, action, header, initial, page, SchemaConnection


def proxy(number=1):
    return CatalogProxy(number, bytes([number]) * 32, "127.0.0.1", 8080 + number, "http", "http", {})


class Fixture:
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.run_id = str(uuid.uuid4())

    def initialize(self, videos=None, history=(), *, max_attempts=3, max_pages=100):
        self.videos = videos or [{"video_id": VIDEO, "kind": "video", "baseline_at": None, "baseline_error": None}]
        files = {}
        for name, rows in (("videos.jsonl.gz", self.videos), ("history.jsonl.gz", history),
                           ("proxies.jsonl.gz", [proxy(1).bridge_record(), proxy(2).bridge_record()])):
            with bulk.gzip_writer(self.folder / name) as write:
                for row in rows:
                    write(row)
            files[name] = {"sha256": digest(self.folder / name)}
        atomic_json(self.folder / "manifest.json", {"version": 1, "collector": "comments", "run_id": self.run_id,
            "videos": len(self.videos), "proxies": 2, "files": files})
        bulk.initialize(self.folder, concurrency=2, max_attempts=max_attempts, max_pages=max_pages)
        self.queue = bulk.Queue(self.folder)
        self.addCleanup(self.queue.close)

    def step(self, payload=None, *, error=None, number=1, observed=True, status=200, claimed=None):
        job = claimed or self.queue.claim()
        self.assertIsNotNone(job)
        parsed = bulk.parse_step(job["phase"], payload, job["video_id"]) if payload is not None else None
        observation = AttemptOutcome(datetime.now(timezone.utc), status is not None, status, False,
                                     connected=status is not None) if observed else None
        metrics = dict(attempts=1, request_body_bytes=10, response_body_bytes=20, decoded_body_bytes=50)
        event, outcome = bulk.build_event(job, proxy(number), metrics, [observation] if observation else [], parsed, error, 0.1)
        try:
            self.queue.finish(job, parsed, error, event, outcome)
        except CommentError as exc:
            event, outcome = bulk.build_event(job, proxy(number), metrics, [observation] if observation else [], None, exc, 0.1)
            self.queue.finish(job, None, exc, event, outcome)
        return job

    def job(self, video_id=VIDEO):
        return dict(self.queue.conn.execute("SELECT * FROM jobs WHERE video_id=?", (video_id,)).fetchone())

    def records(self):
        return [dict(r) for r in self.queue.conn.execute("SELECT * FROM comments ORDER BY comment_id")]

    def finish_video(self, ids=("one", "two")):
        self.step(initial())
        self.step(page(ids))

    def export(self):
        while bulk.export_scans(self.folder):
            pass
        while export_events(self.folder):
            pass
        return sorted((self.folder / "outbox" / "scans").glob("*.gz"))

    def journal(self):
        journal = importer.Journal(self.folder)
        self.addCleanup(journal.close)
        return journal


class QueueTests(Fixture, unittest.TestCase):
    def test_targeted_retry_leaves_other_failed_videos_unchanged(self):
        ids = ["aaaaaaaaaaa", "bbbbbbbbbbb"]
        self.initialize(videos=[{"video_id": v, "kind": "video", "baseline_at": None,
                                 "baseline_error": None} for v in ids], max_attempts=1)
        for _ in ids:
            self.step(error=CommentError("request_error", "connect:timeout"), status=None)
        self.assertEqual(self.queue.retry_failed(video_ids=ids[:1]), 1)
        self.assertEqual([(self.job(v)["state"], self.job(v)["generation"]) for v in ids],
                         [("ready", 1), ("failed", 0)])

    def test_scoped_empty_append_completes_a_saved_page_chain(self):
        self.initialize()
        self.step(page(["one", "two"], token="final"))
        self.step({"onResponseReceivedEndpoints": [{"appendContinuationItemsAction": {
            "targetId": "comments-section"}}]})
        self.assertEqual((self.job()["state"], self.job()["stop_reason"], self.job()["pages"]),
                         ("completed", "end", 2))
        self.assertEqual(len(self.records()), 2)

    def test_prefetched_claims_are_distinct_and_recover_without_consuming_pages(self):
        self.initialize(videos=[{"video_id": f"{i:011d}", "kind": "video", "baseline_at": None,
                                 "baseline_error": None} for i in range(5)])
        first, second = self.queue.claim_many(3), self.queue.claim_many(3)
        self.assertEqual((len(first), len(second)), (3, 2))
        self.assertEqual(len({j["lease"] for j in first + second}), 5)
        self.assertEqual(self.queue.recover(), 5)
        self.assertEqual(self.queue.conn.execute("SELECT sum(pages) FROM jobs").fetchone()[0], 0)
        self.assertEqual({j["video_id"] for j in self.queue.claim_many(5)}, {j["video_id"] for j in first + second})

    def test_direct_newest_first_page_is_committed_and_next_page_resumes(self):
        self.initialize()
        self.step(page(["one"], token="second"))
        self.assertEqual((self.job()["phase"], self.job()["token"], self.job()["pages"]), ("page", "second", 1))
        self.queue.claim()
        self.queue.recover()
        self.step(page(["two"], with_header=False))
        self.assertEqual((self.job()["state"], self.job()["pages"], len(self.records())), ("completed", 2, 2))

    def test_direct_newest_can_finish_on_first_saved_history_page(self):
        self.initialize(history=[(VIDEO, "old", False)])
        self.step(page(["new", "old"], token="unused"))
        self.assertEqual((self.job()["state"], self.job()["pages"], self.job()["stop_reason"]), ("completed", 1, "saved_history"))
        self.assertIsNone(self.queue.claim())

    def test_initial_top_can_omit_comments_that_newest_returns(self):
        self.initialize()
        payload = {"onResponseReceivedEndpoints": [
            action([header(count="1 Comment")], header=True),
            {"reloadContinuationItemsCommand": {"targetId": "comments-section",
                "slot": "RELOAD_CONTINUATION_SLOT_BODY"}},
        ]}
        self.step(payload)
        self.assertEqual((self.job()["phase"], self.job()["state"]), ("page", "ready"))
        self.step(page(["visible-only-in-newest"]))
        self.assertEqual((self.job()["state"], len(self.records())), ("completed", 1))

    def test_live_zero_count_with_omitted_body_selects_newest_and_completes(self):
        self.initialize()
        def empty(newest):
            return {"onResponseReceivedEndpoints": [
                action([header(count="0 Comments", newest=newest)], header=True),
                {"reloadContinuationItemsCommand": {"targetId": "comments-section",
                    "slot": "RELOAD_CONTINUATION_SLOT_BODY"}},
            ]}
        self.step(empty(False))
        self.assertEqual((self.job()["phase"], self.job()["token"]), ("page", "newest"))
        self.step(empty(True))
        self.assertEqual(self.job()["state"], "completed")
        payload = json.loads(self.queue.conn.execute("SELECT payload FROM completions").fetchone()[0])
        self.assertEqual((payload["status"], payload["comment_count"]), ("empty", 0))

    def test_resume_uses_saved_page_and_finishes_without_extra_head_pass(self):
        self.initialize()
        self.step(initial())
        self.step(page(["one"], token="second"))
        old_lease = self.queue.claim()
        self.assertEqual(old_lease["token"], "second")
        self.assertEqual(self.queue.recover(), 1)
        resumed = self.queue.claim()
        self.assertEqual((resumed["phase"], resumed["token"]), ("page", "second"))
        self.step(page(["two"], with_header=False), claimed=resumed)
        self.assertEqual(self.job()["state"], "completed")
        self.assertIsNone(self.queue.claim())
        self.assertEqual(len(self.records()), 2)
        self.assertEqual(self.job()["pages"], 2)

    def test_stale_lease_cannot_commit_a_page(self):
        self.initialize()
        old = self.queue.claim()
        self.queue.recover()
        fresh = self.queue.claim()
        with self.assertRaisesRegex(ValueError, "Stale"):
            self.step(initial(), claimed=old)
        self.step(initial(), claimed=fresh)
        self.assertEqual(self.job()["phase"], "page")

    def test_frozen_history_pins_and_whole_boundary_page(self):
        self.initialize(history=[(VIDEO, "pin", False), (VIDEO, "old-pin", True), (VIDEO, "unknown", None), (VIDEO, "old", False)])
        self.step(initial())
        self.step(page(["pin", "new", "old-pin", "unknown"], pinned=["pin"], token="second"))
        self.assertEqual(self.job()["state"], "ready")
        self.step(page(["pin", "new", "old", "after-old"], token="unused", with_header=False))
        self.assertEqual((self.job()["state"], self.job()["stop_reason"]), ("completed", "saved_history"))
        self.assertIn("after-old", [r["comment_id"] for r in self.records()])
        self.assertEqual(next(r["is_pinned"] for r in self.records() if r["comment_id"] == "pin"), 1)

    def test_partial_scan_rows_never_become_the_history_boundary(self):
        self.initialize(history=[(VIDEO, "old", False)])
        self.step(initial())
        self.step(page(["new"], token="second"))
        self.step(error=CommentError("request_error", "connect:timeout"), status=None)
        self.step(page(["new", "gap"], token="third", with_header=False), number=2)
        self.assertEqual(self.job()["state"], "ready")
        self.step(page(["old"], with_header=False))
        self.assertEqual(self.job()["stop_reason"], "saved_history")
        self.assertEqual({r["comment_id"] for r in self.records()}, {"new", "gap", "old"})

    def test_expired_token_restart_keeps_original_history_and_deduplicates(self):
        self.initialize(history=[(VIDEO, "old", False)])
        self.step(initial())
        self.step(page(["new"], token="expired"))
        for number in (1, 2):
            self.step(error=CommentError("unexpected_response", "Missing comment body"), number=number)
        self.assertEqual((self.job()["phase"], self.job()["token_restarts"]), ("initial", 1))
        self.step(initial())
        self.step(page(["new"], token="fresh"))
        self.assertEqual(self.job()["state"], "ready")
        self.step(page(["gap", "old"], with_header=False))
        self.assertEqual(len(self.records()), 3)
        self.assertEqual(self.job()["stop_reason"], "saved_history")

    def test_request_budget_is_per_step_and_cancel_does_not_consume_it(self):
        self.initialize(max_attempts=2)
        self.step(error=CommentError("request_error", "connect:timeout"), status=None)
        self.step(initial(), number=2)
        self.assertEqual(self.job()["failures"], 0)
        self.step(page(["one"], token="second"))
        self.step(error=CommentError("interrupted", "Interrupted"), observed=False)
        self.assertEqual((self.job()["failures"], self.job()["token"]), (0, "second"))
        for number in (1, 2):
            self.step(error=CommentError("request_error", "connect:timeout"), status=None, number=number)
        self.assertEqual(self.job()["state"], "failed")
        self.assertEqual(len(self.records()), 1)
        self.queue.retry_failed()
        self.assertEqual((self.job()["generation"], self.job()["token"]), (1, "second"))
        self.step(page(["two"], with_header=False))
        headers = [json.loads(r[0]) for r in self.queue.conn.execute("SELECT payload FROM completions ORDER BY seq")]
        self.assertEqual([h["complete"] for h in headers], [False, True])
        self.assertEqual([h["comment_count"] for h in headers], [0, 2])

    def test_unavailable_needs_two_routes_and_does_not_reduce_proxy_quality(self):
        self.initialize(max_attempts=4)
        self.step({})
        unavailable = {"contents": {"twoColumnWatchNextResults": {"results": {"results": {"contents": [
            {"itemSectionRenderer": {"contents": [{"backgroundPromoRenderer": {"title": {"simpleText": "This video isn't available anymore"}}}]}}
        ]}}}}}
        self.step(unavailable)
        self.assertEqual(self.job()["state"], "ready")
        self.step(unavailable, number=2)
        self.assertEqual((self.job()["state"], self.job()["stop_reason"]), ("failed", "unavailable"))
        for row in self.queue.conn.execute("SELECT samples,failure_streak FROM proxy_performance"):
            self.assertEqual(tuple(row), (0, 0))
        self.assertEqual(self.queue.retry_failed(include_unavailable=False), 0)
        self.assertEqual((self.job()["state"], self.job()["generation"]), ("failed", 0))

    def test_retry_restarts_missing_section_diagnostic_when_no_page_was_saved(self):
        self.initialize(max_attempts=1)
        self.step({})
        self.assertEqual(self.job()["phase"], "watch")
        self.step(error=CommentError("unexpected_response", "No recognized comment section or terminal message"))
        self.assertEqual(self.job()["state"], "failed")
        self.assertEqual(self.queue.retry_failed(include_unavailable=False), 1)
        self.assertEqual((self.job()["phase"], self.job()["token"], self.job()["generation"]), ("initial", None, 1))
        self.step(page(["recovered"]))
        self.assertEqual(self.job()["state"], "completed")
        self.assertEqual([r["comment_id"] for r in self.records()], ["recovered"])

    def test_page_limit_and_loop_are_incomplete(self):
        self.initialize(max_attempts=1, max_pages=1)
        self.step(initial())
        self.step(page(["one"], token="second"))
        self.assertEqual((self.job()["state"], self.job()["stop_reason"]), ("failed", "page_limit"))
        self.queue.set("max_pages", 10)
        self.queue.retry_failed()
        self.step(page(["one"], token="third", with_header=False))
        self.assertEqual(self.job()["state"], "failed")
        self.assertIn("pagination_loop", self.job()["last_error"])

    def test_empty_and_disabled_finish_without_fabricating_rows(self):
        self.initialize()
        self.step({"onResponseReceivedEndpoints": [{"reloadContinuationItemsCommand": {
            "targetId": "comments-section", "continuationItems": [{"messageRenderer": {"text": {"simpleText": "Comments are turned off."}}}]}}]})
        self.assertEqual(self.job()["state"], "completed")
        self.assertEqual(self.records(), [])


class ExportTests(Fixture, unittest.TestCase):
    def test_only_completed_scans_export_and_cursor_recovery_preserves_bytes(self):
        self.initialize()
        self.step(initial())
        self.step(page(["one"], token="second"))
        self.assertEqual(bulk.export_scans(self.folder), 0)
        self.step(page(["two"], with_header=False))
        paths = self.export()
        original = paths[0].read_bytes()
        self.queue.set("scans_exported_seq", 0)
        self.assertEqual(bulk.export_scans(self.folder), 1)
        self.assertEqual(paths[0].read_bytes(), original)
        journal = self.journal()
        info = journal.stage(paths[0])
        self.assertEqual((info["scans"], info["comments"]), (1, 2))
        self.assertEqual(journal.conn.execute("SELECT count(*) FROM stage_comments").fetchone()[0], 2)

    def test_checksum_identity_extra_fields_and_duplicate_rows_are_rejected(self):
        self.initialize()
        self.finish_video()
        path = self.export()[0]
        journal = self.journal()
        original, receipt = path.read_bytes(), json.loads(path.with_suffix(path.suffix + ".json").read_text())
        with path.open("ab") as target:
            target.write(b"corrupt")
        with self.assertRaisesRegex(ValueError, "checksum"):
            journal.stage(path)
        path.write_bytes(original)
        with gzip.open(path, "rt") as source:
            rows = [json.loads(line) for line in source]
        cases = []
        wrong = json.loads(json.dumps(rows))
        wrong[1]["data"]["video_id"] = "00000000000"
        cases.append(wrong)
        extra = json.loads(json.dumps(rows))
        extra[1]["data"]["like_count"] = 10
        cases.append(extra)
        duplicate = json.loads(json.dumps(rows))
        duplicate[2] = duplicate[1]
        cases.append(duplicate)
        for changed in cases:
            with self.subTest(changed=changed[1]):
                with gzip.open(path, "wt") as out:
                    for row in changed:
                        out.write(json.dumps(row) + "\n")
                atomic_json(path.with_suffix(path.suffix + ".json"), {**receipt, "sha256": digest(path)})
                with self.assertRaises((ValueError, sqlite3.IntegrityError)):
                    journal.stage(path)

    def test_proxy_batch_preserves_actual_attempt_counts_and_neutral_outcomes(self):
        self.initialize()
        self.finish_video()
        self.export()
        journal = self.journal()
        path = next((self.folder / "outbox").glob("events-*.gz"))
        info, batch = importer.proxy_batch(journal, path)
        self.assertEqual(info["rows"], 2)
        aggregate = next(iter(batch.aggregates.values()))
        self.assertEqual((aggregate.requests_sent, aggregate.responses_received, aggregate.successful_data_received), (2, 2, 2))
        connection = type("Connection", (), {"transaction": lambda _: nullcontext(),
                                              "execute": lambda *args: None})()
        with patch.object(importer, "write_batch", return_value={"unmatched_attempts": 0, "stale_attempts": 0, "updated_proxies": 1}) as write:
            importer.apply_proxy_batch(connection, journal, path)
            replay = importer.apply_proxy_batch(connection, journal, path)
            self.assertTrue(replay["replayed_batch"])
            self.assertEqual(write.call_count, 1)


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_tls_error_is_recorded_as_one_request_failure(self):
        def broken(request):
            raise ssl.SSLError("DECRYPTION_FAILED_OR_BAD_RECORD_MAC")
        observations = []
        metrics = {name: 0 for name in bulk.METRICS}
        async with httpx.AsyncClient(transport=httpx.MockTransport(broken)) as client:
            with self.assertRaises(CommentError) as caught:
                await fetch_json(client, {"continuation": "test"}, metrics, client_version=CLIENT_VERSION,
                                 retries=0, timeout=1, on_attempt=observations.append)
        self.assertEqual(caught.exception.status, "request_error")
        self.assertEqual((metrics["attempts"], len(observations)), (1, 1))
        self.assertFalse(observations[0].data_received)

    async def test_local_transport_failure_is_excluded_from_proxy_statistics(self):
        def broken(request):
            raise httpx.PoolTimeout("local pool is full")
        observations = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(broken)) as client:
            with self.assertRaises(CommentError) as caught:
                await fetch_json(client, {}, {name: 0 for name in bulk.METRICS}, client_version=CLIENT_VERSION,
                                 retries=0, timeout=1, on_attempt=observations.append)
        self.assertEqual(caught.exception.status, "local_error")
        self.assertEqual(observations, [])


class RunnerTests(Fixture, unittest.IsolatedAsyncioTestCase):
    async def test_page_and_next_lease_commit_together_and_resume_keeps_token(self):
        self.initialize()
        job = self.queue.claim()
        parsed = bulk.parse_step("initial", page(["one"], token="next"), VIDEO)
        observed = AttemptOutcome(datetime.now(timezone.utc), True, 200, False, connected=True)
        event, outcome = bulk.build_event(job, proxy(), {}, [observed], parsed, None, 0.1)
        pending, claimed, statements = asyncio.Queue(), deque(), []
        future = asyncio.get_running_loop().create_future()
        pending.put_nowait((job, parsed, None, event, outcome, future))
        pending.put_nowait(None)
        self.queue.conn.set_trace_callback(statements.append)
        try:
            await bulk.save_checkpoints(self.queue, pending, interval=0, claimed_jobs=claimed)
        finally:
            self.queue.conn.set_trace_callback(None)
        self.assertIsNone(future.exception())
        self.assertEqual(sum(s == "COMMIT" for s in statements), 1)
        self.assertEqual(len(claimed), 1)
        self.assertEqual((claimed[0]["token"], claimed[0]["pages"]), ("next", 1))
        self.assertEqual(self.job()["lease"], claimed[0]["lease"])
        self.assertEqual(self.queue.recover(), 1)
        resumed = self.queue.claim()
        self.assertEqual((resumed["token"], resumed["pages"]), ("next", 1))
        self.assertEqual(len(self.records()), 1)

    async def test_slow_storage_keeps_loop_responsive_and_ack_waits_for_commit(self):
        self.initialize()
        loop = asyncio.get_running_loop()
        entered, release = threading.Event(), threading.Event()
        with ThreadPoolExecutor(max_workers=1) as executor:
            queue = await loop.run_in_executor(executor, bulk.Queue, self.folder)
            try:
                job = await loop.run_in_executor(executor, queue.claim)
                parsed = {"terminal": "disabled", "valid_data": True}
                observed = AttemptOutcome(datetime.now(timezone.utc), True, 200, False, connected=True)
                event, outcome = bulk.build_event(job, proxy(), {}, [observed], parsed, None, 0.1)
                future, pending = loop.create_future(), asyncio.Queue()
                pending.put_nowait((job, parsed, None, event, outcome, future))
                pending.put_nowait(None)
                original = queue.finish
                def slow_finish(*args):
                    entered.set()
                    if not release.wait(3):
                        raise RuntimeError("Event loop did not release storage")
                    return original(*args)
                with patch.object(queue, "finish", side_effect=slow_finish):
                    writer = asyncio.create_task(bulk.save_checkpoints(queue, pending, interval=0, executor=executor))
                    try:
                        self.assertTrue(await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), 3))
                        # This coroutine is running while the storage thread is
                        # blocked, and the durable acknowledgement is pending.
                        self.assertFalse(future.done())
                        self.assertEqual(self.queue.conn.execute("SELECT count(*) FROM events").fetchone()[0], 0)
                    finally:
                        release.set()
                    await writer
                self.assertTrue(future.done())
                self.assertIsNone(future.exception())
                self.assertEqual(self.queue.conn.execute("SELECT count(*) FROM events").fetchone()[0], 1)
                self.assertEqual(self.job()["state"], "completed")
            finally:
                release.set()
                await loop.run_in_executor(executor, queue.close)

    async def test_checkpoint_acknowledgements_follow_one_durable_batch_commit(self):
        videos = [{"video_id": f"{i:011d}", "kind": "video", "baseline_at": None,
                   "baseline_error": None} for i in range(2)]
        self.initialize(videos=videos)
        pending = asyncio.Queue()
        futures = []
        statements = []
        for job in self.queue.claim_many(2):
            metrics = dict(attempts=1, request_body_bytes=10, response_body_bytes=20, decoded_body_bytes=50)
            observation = AttemptOutcome(datetime.now(timezone.utc), True, 200, False, connected=True)
            parsed = {"terminal": "disabled", "valid_data": True}
            event, outcome = bulk.build_event(job, proxy(), metrics, [observation], parsed, None, 0.1)
            future = asyncio.get_running_loop().create_future()
            futures.append(future)
            pending.put_nowait((job, parsed, None, event, outcome, future))
        pending.put_nowait(None)
        self.queue.conn.set_trace_callback(statements.append)
        await bulk.save_checkpoints(self.queue, pending, interval=0)
        self.queue.conn.set_trace_callback(None)
        self.assertTrue(all(f.done() and not f.exception() for f in futures))
        self.assertEqual(sum(s == "COMMIT" for s in statements), 1)
        with sqlite3.connect(self.folder / "queue.sqlite3") as reader:
            self.assertEqual(reader.execute("SELECT count(*) FROM jobs WHERE state='completed'").fetchone()[0], 2)
            self.assertEqual(reader.execute("SELECT count(*) FROM events").fetchone()[0], 2)

    async def test_failed_batch_rolls_back_all_pages_before_acknowledging(self):
        videos = [{"video_id": f"{i:011d}", "kind": "video", "baseline_at": None,
                   "baseline_error": None} for i in range(2)]
        self.initialize(videos=videos)
        pending, futures = asyncio.Queue(), []
        jobs = self.queue.claim_many(2)
        for job in jobs:
            parsed = {"terminal": "disabled", "valid_data": True}
            observed = AttemptOutcome(datetime.now(timezone.utc), True, 200, False, connected=True)
            event, outcome = bulk.build_event(job, proxy(), {}, [observed], parsed, None, 0.1)
            future = asyncio.get_running_loop().create_future()
            futures.append(future)
            pending.put_nowait((job, parsed, None, event, outcome, future))
        original = self.queue.finish
        def fail_second(job, *args):
            if job["video_id"] == jobs[1]["video_id"]:
                raise sqlite3.OperationalError("simulated storage failure")
            return original(job, *args)
        with patch.object(self.queue, "finish", side_effect=fail_second):
            with self.assertRaisesRegex(sqlite3.OperationalError, "storage failure"):
                await bulk.save_checkpoints(self.queue, pending, interval=0)
        self.assertTrue(all(isinstance(f.exception(), sqlite3.OperationalError) for f in futures))
        self.assertEqual(self.queue.conn.execute("SELECT count(*) FROM events").fetchone()[0], 0)
        self.assertEqual(self.queue.recover(), 2)

    async def test_runner_restarts_at_checkpoint_and_importable_outbox_finishes(self):
        self.initialize()
        self.step(initial())
        self.step(page(["one"], token="second"))
        self.queue.claim()  # Simulate a process that died after claiming its next page.
        requests = []
        async def handler(request):
            body = json.loads(request.content)
            requests.append(body)
            self.assertEqual(body.get("continuation"), "second")
            return httpx.Response(200, json=page(["two"], with_header=False))
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        class Clients:
            def __init__(self, *args, **kwargs):
                self.process = type("Process", (), {"pid": os.getpid()})()
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                await client.aclose()
            def __getitem__(self, index):
                return client
        with patch.object(bulk, "CatalogClients", Clients), redirect_stdout(io.StringIO()):
            result = await bulk.run(self.folder)
        self.assertEqual(result["state"], "complete")
        self.assertEqual(len(requests), 1)
        self.assertEqual(result["jobs"], {"completed": 1})
        self.assertEqual(result["pages"], 2)
        self.assertEqual(result["scans_exported_seq"], 1)
        self.journal().stage(next((self.folder / "outbox/scans").glob("*.gz")))


class ImportConnection(SchemaConnection):
    def cursor(self):
        return self.conn.cursor()


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class DatabaseTests(Fixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        raw = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"])
        self.addCleanup(raw.close)
        transaction = raw.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__, None, None, None)
        schema = "bulk_comments_test_" + uuid.uuid4().hex
        raw.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        self.media = ImportConnection(raw, schema)
        self.media.execute((bulk.Path(__file__).resolve().parents[1] / "db/schema.sql").read_text())
        self.media.execute("INSERT INTO public.channels(channel_id) VALUES (%s)", (AUTHOR,))
        self.media.execute("INSERT INTO public.videos(video_id,channel_id,type) VALUES (%s,%s,'video')", (VIDEO, AUTHOR))

    def progress(self):
        return self.media.execute("SELECT comments_updated_at,comments_error FROM public.videos WHERE video_id=%s", (VIDEO,)).fetchone()

    def count(self):
        return self.media.execute("SELECT count(*) FROM public.comments").fetchone()[0]

    def disabled_pair(self):
        other = "BBBBBBBBBBB"
        self.media.execute("INSERT INTO public.videos(video_id,channel_id,type) VALUES (%s,%s,'video')", (other, AUTHOR))
        self.initialize(videos=[{"video_id": video_id, "kind": "video", "baseline_at": None,
                                 "baseline_error": None} for video_id in (VIDEO, other)])
        jobs = self.queue.claim_many(2)
        for job in jobs:
            parsed = {"terminal": "disabled", "valid_data": True}
            observed = AttemptOutcome(datetime.now(timezone.utc), True, 200, False, connected=True)
            event, outcome = bulk.build_event(job, proxy(), {}, [observed], parsed, None, 0.1)
            self.queue.finish(job, parsed, None, event, outcome)
        return self.export()[0], [job["video_id"] for job in jobs]

    def test_group_commit_lost_ack_recovers_every_video_without_changing_timestamps(self):
        path, videos = self.disabled_pair()
        journal = self.journal()
        acknowledge = journal.acknowledge
        calls = []
        def fail_second(scan_id):
            calls.append(scan_id)
            if len(calls) == 2:
                raise RuntimeError("lost group acknowledgement")
            acknowledge(scan_id)
        with patch.object(journal, "acknowledge", side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError, "lost group"):
                importer.apply_scan_batch(self.media, journal, path)
        before = self.media.execute("SELECT video_id,comments_updated_at FROM public.videos ORDER BY video_id").fetchall()
        self.assertTrue(all(row[1] is not None for row in before))
        self.assertEqual(journal.conn.execute("SELECT count(*) FROM imports WHERE state='prepared'").fetchone()[0], 2)
        result = importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual(result["replayed"], 2)
        self.assertEqual(self.media.execute("SELECT video_id,comments_updated_at FROM public.videos ORDER BY video_id").fetchall(), before)
        self.assertEqual(journal.conn.execute("SELECT count(*) FROM imports WHERE state='applied'").fetchone()[0], 2)

    def test_group_failure_rolls_back_all_videos_and_uncommitted_receipts(self):
        path, videos = self.disabled_pair()
        journal = self.journal()
        execute = self.media.execute
        def fail_second(source, params=None):
            if source.startswith("UPDATE public.videos SET comments_updated_at") and params[-1] == videos[-1]:
                raise RuntimeError("second video write failed")
            return execute(source, params)
        with patch.object(self.media, "execute", side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError, "second video"):
                importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual(self.media.execute("SELECT count(*) FROM public.videos WHERE comments_updated_at IS NOT NULL").fetchone()[0], 0)
        self.assertEqual(journal.conn.execute("SELECT count(*) FROM imports").fetchone()[0], 0)
        result = importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual((result["scans"], result["replayed"]), (2, 0))

    def test_group_staging_keeps_each_videos_comments_separate(self):
        other = "BBBBBBBBBBB"
        self.media.execute("INSERT INTO public.videos(video_id,channel_id,type) VALUES (%s,%s,'video')", (other, AUTHOR))
        self.initialize(videos=[{"video_id": video_id, "kind": "video", "baseline_at": None,
                                 "baseline_error": None} for video_id in (VIDEO, other)])
        for job in self.queue.claim_many(2):
            parsed = bulk.parse_step("initial", page(["same-id"]), job["video_id"])
            parsed["comments"][0]["text"] = "Body for " + job["video_id"]
            observed = AttemptOutcome(datetime.now(timezone.utc), True, 200, False, connected=True)
            event, outcome = bulk.build_event(job, proxy(), {}, [observed], parsed, None, 0.1)
            self.queue.finish(job, parsed, None, event, outcome)
        path = self.export()[0]
        result = importer.apply_scan_batch(self.media, self.journal(), path)
        self.assertEqual(result["inserted"], 2)
        rows = self.media.execute("SELECT video_id,comment_id,text FROM public.comments ORDER BY video_id").fetchall()
        self.assertEqual(rows, [(video_id, "same-id", "Body for " + video_id) for video_id in sorted((VIDEO, other))])


    def test_parallel_statistics_import_waits_for_existing_writer(self):
        self.initialize()
        self.finish_video()
        self.export()
        journal = self.journal()
        path = next((self.folder / "outbox").glob("events-*.gz"))
        acquired = threading.Event()
        release = threading.Event()
        def owner():
            with psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"]) as connection:
                connection.execute("SELECT pg_advisory_xact_lock(%s)", (importer.IMPORT_LOCK,))
                acquired.set()
                release.wait(5)
        thread = threading.Thread(target=owner)
        thread.start()
        self.assertTrue(acquired.wait(5))
        timer = threading.Timer(0.2, release.set)
        timer.start()
        started = time.monotonic()
        try:
            with psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"], autocommit=True) as connection:
                with patch.object(importer, "write_batch", return_value={"unmatched_attempts": 0, "stale_attempts": 0, "updated_proxies": 1}):
                    result = importer.apply_proxy_batch(connection, journal, path)
            self.assertGreaterEqual(time.monotonic() - started, 0.15)
            self.assertEqual(result["updated_proxies"], 1)
            self.assertEqual(journal.cursor("events"), 2)
        finally:
            release.set()
            timer.cancel()
            thread.join(5)

    def test_complete_import_and_replay_preserve_rows_and_timestamp(self):
        self.initialize()
        self.finish_video()
        path = self.export()[0]
        journal = self.journal()
        first = importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual((first["inserted"], self.count()), (2, 2))
        before = self.progress()
        second = importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual((second["inserted"], second["replayed"]), (0, 1))
        self.assertEqual(self.progress(), before)
        self.assertEqual(self.media.execute("SELECT text,author_channel_id,author_name,is_pinned FROM public.comments ORDER BY comment_id").fetchall(),
                         [("Hello\n👋 one", AUTHOR, "Author", False), ("Hello\n👋 two", AUTHOR, "Author", False)])

    def test_lost_acknowledgement_after_pg_commit_is_recovered_without_reapplying(self):
        self.initialize()
        self.finish_video()
        path = self.export()[0]
        journal = self.journal()
        with patch.object(journal, "acknowledge", side_effect=RuntimeError("Simulated lost acknowledgement")):
            with self.assertRaisesRegex(RuntimeError, "lost acknowledgement"):
                importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual(self.count(), 2)
        before = self.progress()
        self.assertEqual(journal.conn.execute("SELECT state FROM imports").fetchone()[0], "prepared")
        result = importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual((result["inserted"], result["replayed"]), (0, 1))
        self.assertEqual(self.progress(), before)
        self.assertEqual(journal.conn.execute("SELECT inserted,state FROM imports").fetchone()[:], (2, "applied"))

    def test_pg_failure_after_preparing_receipt_rolls_back_then_retries(self):
        self.initialize()
        self.finish_video()
        path = self.export()[0]
        journal = self.journal()
        execute = self.media.execute
        def fail(source, params=None):
            if source.startswith("UPDATE public.videos SET comments_updated_at"):
                raise RuntimeError("Simulated failure before PostgreSQL commit")
            return execute(source, params)
        with patch.object(self.media, "execute", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "before PostgreSQL commit"):
                importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual((self.count(), self.progress()), (0, (None, None)))
        result = importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual((result["inserted"], self.count()), (2, 2))

    def test_failed_partial_scan_preserves_old_comments_then_new_generation_imports(self):
        at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        self.media.execute("UPDATE public.videos SET comments_updated_at=%s", (at,))
        self.media.execute("INSERT INTO public.comments(video_id,comment_id,text,is_pinned) VALUES (%s,'old','Old',false)", (VIDEO,))
        self.initialize(videos=[{"video_id": VIDEO, "kind": "video", "baseline_at": at.isoformat(), "baseline_error": None}],
                        history=[(VIDEO, "old", False)], max_attempts=1)
        self.step(initial())
        self.step(page(["new"], token="second"))
        self.step(error=CommentError("request_error", "connect:timeout"), status=None)
        failed_path = self.export()[0]
        journal = self.journal()
        importer.apply_scan_batch(self.media, journal, failed_path)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.progress(), (at, "request_error: connect:timeout"))
        self.queue.retry_failed()
        self.step(page(["gap", "old"], with_header=False))
        success_path = self.export()[-1]
        result = importer.apply_scan_batch(self.media, journal, success_path)
        self.assertEqual(result["inserted"], 2)
        self.assertEqual(self.count(), 3)
        self.assertIsNone(self.progress()[1])
        before = self.progress()
        journal.conn.execute("DELETE FROM imports WHERE generation=0")
        importer.apply_scan_batch(self.media, journal, failed_path)
        self.assertEqual(self.progress(), before)

    def test_newer_scan_is_not_overwritten_by_old_outbox(self):
        self.initialize()
        self.finish_video()
        path = self.export()[0]
        at = datetime.now(timezone.utc)
        self.media.execute("UPDATE public.videos SET comments_updated_at=%s", (at,))
        self.media.execute("INSERT INTO public.comments(video_id,comment_id,text) VALUES (%s,'one','Newer text')", (VIDEO,))
        result = importer.apply_scan_batch(self.media, self.journal(), path)
        self.assertEqual(result["skipped_newer_scan"], 1)
        self.assertEqual(self.count(), 1)
        self.assertEqual(self.progress(), (at, None))
        self.assertEqual(self.media.execute("SELECT text FROM public.comments").fetchone()[0], "Newer text")

    def test_entire_batch_is_validated_before_first_scan_reaches_postgres(self):
        videos = [{"video_id": v, "kind": "video", "baseline_at": None, "baseline_error": None} for v in (VIDEO, "00000000000")]
        self.initialize(videos=videos)
        self.media.execute("INSERT INTO public.videos(video_id,channel_id,type) VALUES ('00000000000',%s,'video')", (AUTHOR,))
        while job := self.queue.claim():
            self.step(initial() if job["phase"] == "initial" else page([job["video_id"]]), claimed=job)
        path = self.export()[0]
        with gzip.open(path, "rt") as source:
            rows = [json.loads(line) for line in source]
        rows[-2]["data"]["is_pinned"] = "false"
        with gzip.open(path, "wt") as output:
            for row in rows:
                output.write(json.dumps(row) + "\n")
        info_path = path.with_suffix(path.suffix + ".json")
        info = json.loads(info_path.read_text())
        atomic_json(info_path, {**info, "sha256": digest(path)})
        with self.assertRaisesRegex(ValueError, "pinned status"):
            importer.apply_scan_batch(self.media, self.journal(), path)
        self.assertEqual(self.count(), 0)
        self.assertEqual(self.progress(), (None, None))

    def test_newer_error_is_preserved_and_disabled_scan_can_complete(self):
        self.initialize(max_attempts=1)
        self.step(error=CommentError("request_error", "connect:timeout"), status=None)
        path = self.export()[0]
        self.media.execute("UPDATE public.videos SET comments_error='More recent failure'")
        journal = self.journal()
        result = importer.apply_scan_batch(self.media, journal, path)
        self.assertEqual(result["skipped_newer_error"], 1)
        self.assertEqual(self.progress(), (None, "More recent failure"))
        self.queue.retry_failed()
        self.step({"onResponseReceivedEndpoints": [{"reloadContinuationItemsCommand": {
            "targetId": "comments-section", "continuationItems": [{"messageRenderer": {"text": {"simpleText": "Comments are turned off."}}}]}}]})
        importer.apply_scan_batch(self.media, journal, self.export()[-1])
        self.assertEqual(self.count(), 0)
        self.assertIsNotNone(self.progress()[0])
        self.assertIsNone(self.progress()[1])

    def test_retry_updates_its_own_prior_error(self):
        self.initialize(max_attempts=1)
        self.step(error=CommentError("request_error", "connect:timeout"), status=None)
        journal = self.journal()
        importer.apply_scan_batch(self.media, journal, self.export()[0])
        self.assertEqual(self.progress(), (None, "request_error: connect:timeout"))
        self.queue.retry_failed()
        self.step(error=CommentError("blocked", "HTTP 403"), status=403)
        result = importer.apply_scan_batch(self.media, journal, self.export()[-1])
        self.assertEqual(result["errors_recorded"], 1)
        self.assertEqual(self.progress(), (None, "blocked: HTTP 403"))
