"""Initialize the collection databases, inspect status, and add channel IDs."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import re

from psycopg import sql

from runtime_config import ROOT, connect_database


CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}\Z")
SCHEMAS = {"media": ROOT / "db", "proxy": ROOT / "db/proxy"}


def migration_files(folder):
    files = sorted((folder / "migrations").glob("*.sql"))
    if not files or any(not re.fullmatch(r"\d{3}_[a-z0-9_]+\.sql", path.name) for path in files):
        raise ValueError("Missing or invalid database migrations")
    versions = [int(path.name.split("_", 1)[0]) for path in files]
    if versions != list(range(1, len(files) + 1)):
        raise ValueError("Database migrations must be consecutive and unique")
    return [(path, hashlib.sha256(path.read_bytes()).hexdigest()) for path in files]


def migrate_connection(conn, folder):
    """Bootstrap the current schema or apply each later migration once."""
    files = migration_files(Path(folder))
    with conn.transaction():
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended('media:schema-migrations',0))")
        tracked = conn.execute("SELECT to_regclass('app_meta.schema_migrations')").fetchone()[0]
        if tracked is None:
            tables = conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'").fetchall()
            if tables:
                raise ValueError("Existing database has no migration history; use a new database for Docker setup")
            conn.execute((Path(folder) / "schema.sql").read_text())
            conn.execute("CREATE SCHEMA IF NOT EXISTS app_meta")
            conn.execute("""CREATE TABLE app_meta.schema_migrations (
                name TEXT PRIMARY KEY, sha256 TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now(), baseline BOOLEAN NOT NULL)""")
            for path, checksum in files:
                conn.execute("INSERT INTO app_meta.schema_migrations(name,sha256,baseline) VALUES (%s,%s,true)",
                             (path.name, checksum))
            return {"initialized": True, "version": len(files), "applied": []}
        saved = conn.execute("SELECT name,sha256 FROM app_meta.schema_migrations ORDER BY name").fetchall()
        expected = [(path.name, checksum) for path, checksum in files]
        if not saved or saved != expected[:len(saved)]:
            raise ValueError("Applied migrations changed or this checkout is older than the database")
        applied = []
        for path, checksum in files[len(saved):]:
            # Historical SQL files include transaction wrappers. Keep both the
            # DDL and its receipt inside this outer transaction instead.
            source = re.sub(r"(?m)^(?:BEGIN|COMMIT);[ \t]*$", "", path.read_text())
            conn.execute(source)
            conn.execute("INSERT INTO app_meta.schema_migrations(name,sha256,baseline) VALUES (%s,%s,false)",
                         (path.name, checksum))
            applied.append(path.name)
        return {"initialized": False, "version": len(files), "applied": applied}


def migrate():
    result = {}
    for name, folder in SCHEMAS.items():
        with connect_database(name, autocommit=True) as conn:
            result[name] = migrate_connection(conn, folder)
    return result


def status():
    result = {}
    for name, tables in (("media", ("channels", "videos", "comments")),
                         ("proxy", ("proxies", "proxy_lists"))):
        with connect_database(name) as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            counts = {table: conn.execute(sql.SQL("SELECT count(*) FROM public.{}").format(
                sql.Identifier(table))).fetchone()[0] for table in tables}
            version = conn.execute("SELECT count(*) FROM app_meta.schema_migrations").fetchone()[0]
            result[name] = {"database": conn.info.dbname, "schema_version": version, **counts}
    return result


def add_channels(identifiers):
    identifiers = list(dict.fromkeys(identifiers))
    if not identifiers or any(not isinstance(value, str) or not CHANNEL_ID.fullmatch(value) for value in identifiers):
        raise ValueError("Provide valid 24-character YouTube channel IDs")
    with connect_database("media") as conn:
        inserted = conn.execute("""INSERT INTO public.channels(channel_id)
            SELECT unnest(%s::text[]) ON CONFLICT(channel_id) DO NOTHING""", (identifiers,)).rowcount
    return {"selected": len(identifiers), "inserted": inserted}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("migrate")
    commands.add_parser("status")
    add = commands.add_parser("add-channels")
    add.add_argument("channel_id", nargs="+")
    seed = commands.add_parser("import-channels")
    seed.add_argument("--file", type=Path, default=ROOT / "data/channels.csv")
    args = parser.parse_args()
    if args.command == "migrate":
        result = migrate()
    elif args.command == "status":
        result = status()
    elif args.command == "add-channels":
        result = add_channels(args.channel_id)
    else:
        with args.file.open(newline="") as source:
            result = add_channels(row["channel_id"] for row in csv.DictReader(source))
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
