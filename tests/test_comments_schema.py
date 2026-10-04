from datetime import datetime, timezone
import os
from pathlib import Path
import unittest
import uuid

import psycopg
from psycopg import sql


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class CommentsSchemaTests(unittest.TestCase):
    def setUp(self):
        self.conn = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"])
        self.addCleanup(self.conn.close)
        transaction = self.conn.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__, None, None, None)
        self.schema = "comments_test_" + uuid.uuid4().hex
        self.create_schema(self.schema)
        self.execute((ROOT / "db/schema.sql").read_text())
        self.channel = "UC" + uuid.uuid4().hex[:22]
        self.videos = [uuid.uuid4().hex[:11] for _ in range(2)]
        self.seed(self.schema)

    def create_schema(self, name):
        self.conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(name)))

    def execute(self, source, params=None, *, schema=None):
        schema = schema or self.schema
        return self.conn.execute(source.replace("public.", f'"{schema}".'), params)

    def seed(self, schema):
        self.execute("INSERT INTO public.channels (channel_id,subscriber_count) VALUES (%s,123)",
                     (self.channel,), schema=schema)
        observed = datetime(2026, 10, 3, tzinfo=timezone.utc)
        self.execute(
            "INSERT INTO public.videos (video_id,channel_id,type,title,description,"
            "metadata_updated_at,view_count,like_count,stats_updated_at) "
            "SELECT unnest(%s::text[]),%s,'video','Existing title','Existing description',%s,12345,67,%s",
            (self.videos, self.channel, observed, observed), schema=schema,
        )

    def insert(self, comment_id="comment-1", *, video=None, text="Comment text", pinned=None):
        return self.execute(
            "INSERT INTO public.comments (video_id,comment_id,text,is_pinned) VALUES (%s,%s,%s,%s)",
            (video or self.videos[0], comment_id, text, pinned),
        )

    def columns(self, table, *, schema=None):
        return self.conn.execute(
            "SELECT column_name,data_type,is_nullable,column_default FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (schema or self.schema, table),
        ).fetchall()

    def test_exact_fields_optional_nulls_and_full_text(self):
        self.assertEqual(self.columns("comments"), [
            ("video_id", "text", "NO", None),
            ("comment_id", "text", "NO", None),
            ("text", "text", "NO", None),
            ("author_channel_id", "text", "YES", None),
            ("author_name", "text", "YES", None),
            ("is_pinned", "boolean", "YES", None),
        ])
        text = "Salom 👋\nКирилл matni — o‘zbekcha"
        self.insert(text=text)
        self.assertEqual(self.execute("SELECT * FROM public.comments").fetchone(),
                         (self.videos[0], "comment-1", text, None, None, None))
        self.insert("empty-text", text="")
        self.assertEqual(self.execute("SELECT text FROM public.comments WHERE comment_id='empty-text'").fetchone(),
                         ("",))

    def test_duplicate_pair_is_rejected_and_replay_preserves_original(self):
        self.insert()
        with self.assertRaises(psycopg.errors.UniqueViolation), self.conn.transaction():
            self.insert(text="Duplicate")
        replay = self.execute(
            "INSERT INTO public.comments (video_id,comment_id,text) VALUES (%s,'comment-1','Duplicate') "
            "ON CONFLICT (video_id,comment_id) DO NOTHING RETURNING comment_id",
            (self.videos[0],),
        )
        self.assertIsNone(replay.fetchone())
        self.assertEqual(self.execute("SELECT text FROM public.comments").fetchone(), ("Comment text",))
        self.insert(video=self.videos[1])
        self.assertEqual(self.execute("SELECT count(*) FROM public.comments").fetchone()[0], 2)

    def test_required_fields_reject_null(self):
        for field in ("video_id", "comment_id", "text"):
            row = {"video_id": self.videos[0], "comment_id": "comment-1", "text": "Text"}
            row[field] = None
            with self.subTest(field=field), self.assertRaises(psycopg.errors.NotNullViolation), self.conn.transaction():
                self.execute("INSERT INTO public.comments (video_id,comment_id,text) VALUES (%s,%s,%s)",
                             (row["video_id"], row["comment_id"], row["text"]))
        self.assertEqual(self.execute("SELECT count(*) FROM public.comments").fetchone()[0], 0)

    def test_foreign_key_rejects_missing_video_and_protects_referenced_video(self):
        with self.assertRaises(psycopg.errors.ForeignKeyViolation), self.conn.transaction():
            self.insert(video="missing-video")
        self.insert()
        with self.assertRaises(psycopg.errors.ForeignKeyViolation), self.conn.transaction():
            self.execute("DELETE FROM public.videos WHERE video_id=%s", (self.videos[0],))
        self.assertEqual(self.execute("SELECT count(*) FROM public.comments").fetchone()[0], 1)
        self.assertEqual(self.execute("SELECT count(*) FROM public.videos").fetchone()[0], 2)

    def test_pinned_state_can_be_true_false_or_unknown(self):
        for number, pinned in enumerate((True, False, None)):
            self.insert(str(number), pinned=pinned)
        self.assertEqual(self.execute("SELECT is_pinned FROM public.comments ORDER BY comment_id").fetchall(),
                         [(True,), (False,), (None,)])

    def test_page_insert_does_not_mark_scan_complete_and_pending_index_is_valid(self):
        self.insert()
        self.assertEqual(self.execute(
            "SELECT comments_updated_at,comments_error FROM public.videos ORDER BY video_id"
        ).fetchall(), [(None, None), (None, None)])
        index = self.conn.execute(
            "SELECT i.indisvalid,i.indisready,pg_get_expr(i.indpred,i.indrelid) "
            "FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname='videos_comments_pending_idx'",
            (self.schema,),
        ).fetchone()
        self.assertEqual(index, (True, True, "(comments_updated_at IS NULL)"))

    def test_upgrade_preserves_existing_data_and_matches_fresh_schema(self):
        old = "comments_upgrade_" + uuid.uuid4().hex
        self.create_schema(old)
        self.execute("CREATE TABLE public.channels (channel_id TEXT PRIMARY KEY)", schema=old)
        for migration in sorted((ROOT / "db/migrations").glob("*.sql")):
            if int(migration.name.split("_", 1)[0]) <= 10:
                self.execute(migration.read_text(), schema=old)
        self.seed(old)
        channels_before = self.execute("SELECT * FROM public.channels ORDER BY channel_id", schema=old).fetchall()
        videos_before = self.execute("SELECT to_jsonb(v) FROM public.videos v ORDER BY video_id", schema=old).fetchall()
        self.execute((ROOT / "db/migrations/011_video_comments.sql").read_text(), schema=old)
        self.assertEqual(self.execute("SELECT * FROM public.channels ORDER BY channel_id", schema=old).fetchall(),
                         channels_before)
        self.assertEqual(self.execute(
            "SELECT to_jsonb(v)-ARRAY['comments_updated_at','comments_error'] FROM public.videos v ORDER BY video_id",
            schema=old,
        ).fetchall(), videos_before)
        self.assertEqual(self.execute(
            "SELECT comments_updated_at,comments_error FROM public.videos ORDER BY video_id", schema=old,
        ).fetchall(), [(None, None), (None, None)])
        for table in ("comments", "videos"):
            self.assertEqual(self.columns(table, schema=old), self.columns(table))
        self.assertEqual(self.execute("SELECT count(*) FROM public.comments", schema=old).fetchone()[0], 0)
        for removed in ("channel_scan_state", "comment_scan_state"):
            self.assertIsNone(self.conn.execute("SELECT to_regclass(%s)", (f"{old}.{removed}",)).fetchone()[0])

    def test_existing_viewer_can_read_but_cannot_write(self):
        if self.conn.execute("SELECT 1 FROM pg_roles WHERE rolname='media_viewer'").fetchone() is None:
            self.skipTest("Local media_viewer role is not configured")
        self.insert()
        self.conn.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO media_viewer").format(sql.Identifier(self.schema)))
        self.execute("GRANT SELECT ON public.comments,public.videos TO media_viewer")
        self.conn.execute("SET LOCAL ROLE media_viewer")
        try:
            self.assertEqual(self.execute("SELECT count(*) FROM public.comments").fetchone()[0], 1)
            self.assertEqual(self.execute(
                "SELECT comments_updated_at,comments_error FROM public.videos ORDER BY video_id"
            ).fetchall(), [(None, None), (None, None)])
            for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"):
                self.assertFalse(self.conn.execute("SELECT has_table_privilege(current_user,%s,%s)",
                                                  (f"{self.schema}.comments", privilege)).fetchone()[0])
            with self.assertRaises(psycopg.errors.InsufficientPrivilege), self.conn.transaction():
                self.insert("forbidden")
            with self.assertRaises(psycopg.errors.InsufficientPrivilege), self.conn.transaction():
                self.execute("UPDATE public.videos SET comments_error='forbidden'")
        finally:
            self.conn.execute("RESET ROLE")


if __name__ == "__main__":
    unittest.main()
