from contextlib import nullcontext
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
import uuid

import psycopg

import manage


class SchemaConnection:
    def __init__(self, conn, schema):
        self.conn, self.schema = conn, schema

    def execute(self, source, params=None):
        source = source.replace("public.", self.schema + ".").replace("'public'", "'" + self.schema + "'")
        source = source.replace("app_meta", self.schema + "_meta")
        return self.conn.execute(source, params)

    def transaction(self):
        return self.conn.transaction()


@unittest.skipUnless(os.environ.get("MEDIA_TEST_DATABASE_URL"), "Set MEDIA_TEST_DATABASE_URL for database tests")
class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.raw = psycopg.connect(os.environ["MEDIA_TEST_DATABASE_URL"])
        self.addCleanup(self.raw.close)
        transaction = self.raw.transaction(force_rollback=True)
        transaction.__enter__()
        self.addCleanup(transaction.__exit__, None, None, None)
        schema = "setup_test_" + uuid.uuid4().hex
        self.raw.execute("CREATE SCHEMA " + schema)
        self.conn = SchemaConnection(self.raw, schema)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name) / "db"
        shutil.copytree(manage.SCHEMAS["media"], self.folder)
        self.version = len(manage.migration_files(self.folder))

    def add_migration(self, number, source):
        path = self.folder / "migrations" / f"{number:03d}_test.sql"
        path.write_text(source)
        return path

    def test_fresh_setup_and_restart_preserve_rows_without_reapplying(self):
        first = manage.migrate_connection(self.conn, self.folder)
        self.assertTrue(first["initialized"])
        channel = "UC" + uuid.uuid4().hex[:22]
        with patch("manage.connect_database", return_value=nullcontext(self.conn)):
            self.assertEqual(manage.add_channels([channel, channel]), {"selected": 1, "inserted": 1})
        with patch("manage.connect_database", return_value=nullcontext(self.conn)):
            self.assertEqual(manage.add_channels([channel]), {"selected": 1, "inserted": 0})
        self.assertEqual(manage.migrate_connection(self.conn, self.folder),
                         {"initialized": False, "version": self.version, "applied": []})
        self.assertEqual(self.conn.execute("SELECT channel_id FROM public.channels").fetchall(), [(channel,)])

    def test_later_migration_is_atomic_and_only_applied_once(self):
        manage.migrate_connection(self.conn, self.folder)
        migration = self.add_migration(self.version + 1,
            "BEGIN;\nALTER TABLE public.videos ADD COLUMN container_check INTEGER;\nCOMMIT;\n")
        self.assertEqual(manage.migrate_connection(self.conn, self.folder)["applied"], [migration.name])
        self.assertEqual(manage.migrate_connection(self.conn, self.folder)["applied"], [])
        self.add_migration(self.version + 2,
            "ALTER TABLE public.videos ADD COLUMN rolled_back INTEGER; SELECT 1/0;")
        with self.assertRaises(psycopg.errors.DivisionByZero):
            manage.migrate_connection(self.conn, self.folder)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM app_meta.schema_migrations").fetchone()[0],
                         self.version + 1)
        columns = self.raw.execute("SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name='videos'",
                                   (self.conn.schema,)).fetchall()
        self.assertIn(("container_check",), columns)
        self.assertNotIn(("rolled_back",), columns)

    def test_changed_history_and_older_checkout_are_rejected(self):
        manage.migrate_connection(self.conn, self.folder)
        path = manage.migration_files(self.folder)[0][0]
        path.write_text(path.read_text() + "\n-- changed\n")
        with self.assertRaisesRegex(ValueError, "Applied migrations changed"):
            manage.migrate_connection(self.conn, self.folder)
        self.assertEqual(self.conn.execute("SELECT count(*) FROM app_meta.schema_migrations").fetchone()[0], self.version)

    def test_existing_untracked_database_is_preserved(self):
        self.conn.execute("CREATE TABLE public.keep_me(value TEXT)")
        self.conn.execute("INSERT INTO public.keep_me VALUES ('original')")
        with self.assertRaisesRegex(ValueError, "no migration history"):
            manage.migrate_connection(self.conn, self.folder)
        self.assertEqual(self.conn.execute("SELECT * FROM public.keep_me").fetchall(), [("original",)])
        self.assertIsNone(self.conn.execute("SELECT to_regclass('app_meta.schema_migrations')").fetchone()[0])
