from contextlib import redirect_stdout
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
import uuid

import psycopg
from psycopg import sql

import comments_bulk_audit as audit
import comments_bulk_import as importer
from collect_video_comments import FIELDS, CommentError
from metadata_bulk import atomic_json, digest
from test_comments_bulk import Fixture, ImportConnection
from test_collect_video_comments import AUTHOR, VIDEO, initial, page


class ExportTests(Fixture, unittest.TestCase):
    def export_audit(self):
        self.export()
        self.queue.set("state", "complete")
        (self.folder / "controller.lock").touch()
        output = self.folder / "audit"
        result = audit.export_shard((self.folder, output))
        with gzip.open(output / "videos.jsonl.gz", "rt") as source:
            rows = [json.loads(line) for line in source]
        return result, rows, output

    def test_complete_source_hash_and_export_reuse(self):
        self.initialize()
        self.finish_video()
        result, rows, output = self.export_audit()
        digest_ = hashlib.sha256()
        for record in self.records():
            record["is_pinned"] = bool(record["is_pinned"])
            digest_.update(audit.row_digest(record))
        self.assertEqual(rows[0]["content_sha256"], digest_.hexdigest())
        self.assertEqual(rows[0]["expected_count"], 2)
        self.assertEqual(result, audit.export_shard((self.folder, output)))

    def test_failed_partial_rows_are_excluded_from_expected_database(self):
        self.initialize(max_attempts=1)
        self.step(initial())
        self.step(page(["partial"], token="next"))
        self.step(error=CommentError("request_error", "timeout"), status=None)
        result, rows, _ = self.export_audit()
        self.assertEqual((result["counts"]["failed"], rows[0]["expected_count"], rows[0]["visited_count"]), (1, 0, 0))
        self.assertIsNone(rows[0]["content_sha256"])

    def test_incremental_expectations_preserve_unvisited_history(self):
        self.initialize(videos=[{"video_id": VIDEO, "kind": "video", "baseline_at": "2026-01-01T00:00:00+00:00", "baseline_error": None}],
                        history=[(VIDEO, "old", False), (VIDEO, "unvisited", False)])
        self.finish_video(("new", "old"))
        result, rows, output = self.export_audit()
        self.assertEqual((rows[0]["expected_count"], rows[0]["visited_count"], result["counts"]["revisited_rows"]), (3, 2, 2))
        self.assertIsNone(rows[0]["content_sha256"])
        with gzip.open(output / "revisited.jsonl.gz", "rt") as source:
            self.assertEqual({json.loads(line)["comment_id"] for line in source}, {"new", "old"})


class AuditConnection(ImportConnection):
    def __enter__(self):
        self.audit_transaction = self.conn.transaction(force_rollback=True)
        self.audit_transaction.__enter__()
        return self

    def __exit__(self, *args):
        return self.audit_transaction.__exit__(*args)

    def execute(self, source, params=None):
        source = source.replace("table_schema='public'", "table_schema='" + self.schema + "'")
        return super().execute(source, params)


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class DatabaseAuditTests(Fixture, unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.root = self.folder
        self.fleet = self.root / "fleet"
        self.folder = self.fleet / "shard-00"
        self.folder.mkdir(parents=True)
        self.raw = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"], autocommit=True)
        self.addCleanup(self.raw.close)
        self.schema = "comments_audit_test_" + uuid.uuid4().hex
        self.raw.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))
        self.addCleanup(self.raw.execute, sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))
        self.media = AuditConnection(self.raw, self.schema)
        self.media.execute((Path(__file__).resolve().parents[1] / "db/schema.sql").read_text())
        self.media.execute("INSERT INTO public.channels(channel_id) VALUES (%s)", (AUTHOR,))
        self.media.execute("INSERT INTO public.videos(video_id,channel_id,type) VALUES (%s,%s,'video')", (VIDEO, AUTHOR))

    def test_unicode_and_null_row_hashes_match_postgresql_exactly(self):
        cases = [("a", "Hello\n😊\t\\\" / <>", AUTHOR, "Имя", False),
                 ("Z", "\u2028नमस्ते\r\b", None, "", True), ("0", "", None, None, None)]
        for values in cases:
            record = dict(zip(FIELDS, (VIDEO, *values)))
            pg_hash = self.raw.execute("""SELECT encode(sha256(convert_to(jsonb_build_array(
                %s::text,%s::text,%s::text,%s::text,%s::boolean)::text,'UTF8')),'hex')""", values).fetchone()[0]
            self.assertEqual(audit.row_digest(record).decode(), pg_hash)

    def test_full_audit_detects_database_text_corruption(self):
        self.initialize()
        self.finish_video()
        path = self.export()[0]
        journal = self.journal()
        importer.apply_scan_batch(self.media, journal, path)
        for event in (self.folder / "outbox").glob("events-*.gz.json"):
            journal.record_batch("events", json.loads(event.read_text()))
        self.queue.set("state", "complete")
        (self.folder / "controller.lock").touch()
        (self.folder / "import.lock").touch()
        parent = str(uuid.uuid4())
        atomic_json(self.fleet / "fleet.json", {"parent_run_id": parent, "totals": {"videos": 1, "history_comments": 0},
            "shards": [{"name": "shard-00", "run_id": self.run_id}]})
        expected = self.root / "expected"
        audit.export_shard((self.folder, expected / "shard-00"))
        atomic_json(expected / "manifest.json", {"parent_run_id": parent, "counts": {"videos": 1, "completed": 1},
            "shards": [{"name": "shard-00", "receipt_sha256": digest(expected / "shard-00/receipt.json")}]})
        with patch.object(audit, "open_database", return_value=self.media), redirect_stdout(io.StringIO()):
            result = audit.validate(self.fleet, expected, self.root / "result.json")
        self.assertTrue(result["passed"], result)
        self.assertEqual(result["checks"]["comments"], 2)
        self.assertIsInstance(result["checks"]["comments"], int)
        self.assertEqual(json.loads(json.dumps(result)), result)
        self.media.execute("UPDATE public.comments SET text='corrupted' WHERE comment_id='one'")
        with patch.object(audit, "open_database", return_value=self.media), redirect_stdout(io.StringIO()):
            result = audit.validate(self.fleet, expected, self.root / "result.json")
        self.assertFalse(result["passed"])
        self.assertEqual(result["checks"]["content_hash_mismatches"], 1)
        self.assertEqual(result["checks"]["comment_count_mismatches"], 0)


if __name__ == "__main__":
    unittest.main()
