"""Download a published catalog, verify a staging restore, and merge it safely."""
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
from urllib.parse import quote

import httpx
import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from proxy_service_common import SERVICE_DIR, available_memory, digest, read_json, run_owned, utcnow, write_json
from runtime_config import connect_database, database_options


DEFAULT_REPOSITORY = "mahmudvibecoding/proxy-catalog"
TABLES = ("proxies", "proxy_stats", "proxy_lists")
HASH = re.compile(r"[a-f0-9]{64}\Z")
REPOSITORY = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")


def progress(stage, **fields):
    print(json.dumps({"event": "catalog_sync", "stage": stage, "at": utcnow(), **fields}, default=str), flush=True)


def validate_repository(value):
    if not isinstance(value, str) or not REPOSITORY.fullmatch(value) or ".." in value:
        raise ValueError("Expected a GitHub owner/repository")
    return value


def fetch_document(client, url, *, expected_hash=None, limit=2 * 1024 * 1024):
    data = bytearray()
    with client.stream("GET", url) as response:
        response.raise_for_status()
        for block in response.iter_bytes():
            data.extend(block)
            if len(data) > limit:
                raise ValueError("Catalog document exceeded its size limit")
    if expected_hash and hashlib.sha256(data).hexdigest() != expected_hash:
        raise ValueError("Manifest checksum differs from the published pointer")
    value = json.loads(data)
    if not isinstance(value, dict):
        raise ValueError("Catalog document must be an object")
    return value, bytes(data)


def validate_manifest(manifest, repository):
    if manifest.get("format_version") != 1 or manifest.get("repository") != repository:
        raise ValueError("Unsupported catalog manifest or repository")
    if not isinstance(manifest.get("dump_sha256"), str) or not HASH.fullmatch(manifest["dump_sha256"]):
        raise ValueError("Invalid dump checksum")
    if type(manifest.get("dump_bytes")) is not int or manifest["dump_bytes"] <= 0:
        raise ValueError("Invalid dump size")
    created = datetime.fromisoformat(manifest["created_at"].replace("Z", "+00:00"))
    if created.tzinfo is None:
        raise ValueError("Snapshot timestamp must include a time zone")
    version = re.match(r"^(\d+)\.", str(manifest.get("postgres_version", "")))
    if not version:
        raise ValueError("Missing PostgreSQL version")
    metrics = manifest.get("tables", {})
    if set(metrics) != {*TABLES, "identity_sequence"}:
        raise ValueError("Missing catalog verification metrics")
    for table in TABLES:
        if type(metrics[table].get("rows")) is not int or metrics[table]["rows"] < 0:
            raise ValueError("Invalid catalog row count")
        if not re.fullmatch(r"-?\d+", str(metrics[table].get("content_fingerprint", ""))):
            raise ValueError("Invalid catalog fingerprint")
    sequence = metrics["identity_sequence"]
    if type(sequence.get("last_value")) is not int or sequence["last_value"] < 1 or type(sequence.get("is_called")) is not bool:
        raise ValueError("Invalid identity sequence")
    assets = manifest.get("assets")
    if not isinstance(assets, list) or not assets:
        raise ValueError("Missing database assets")
    names, parts = set(), []
    for item in assets:
        name = item.get("name")
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name) or name in names:
            raise ValueError("Invalid or duplicate asset name")
        names.add(name)
        if type(item.get("bytes")) is not int or item["bytes"] <= 0 or not HASH.fullmatch(str(item.get("sha256", ""))):
            raise ValueError("Invalid asset size or checksum")
        if item.get("role") == "database":
            if name != f"catalog.dump.part-{len(parts):04d}":
                raise ValueError("Database parts are missing or out of order")
            parts.append(item)
    if not parts or sum(item["bytes"] for item in parts) != manifest["dump_bytes"]:
        raise ValueError("Database parts do not match the dump size")
    return parts


def download_asset(client, url, path, expected, *, stop=None):
    """Resume byte ranges when supported; a complete file always passes SHA-256."""
    path = Path(path)
    if path.exists() and path.stat().st_size == expected["bytes"] and digest(path) == expected["sha256"]:
        return
    partial = path.with_name(path.name + ".partial")
    offset = partial.stat().st_size if partial.exists() else 0
    if offset > expected["bytes"]:
        partial.unlink()
        offset = 0
    # An interrupted rename may leave a complete, valid partial file.
    if offset == expected["bytes"]:
        if digest(partial) == expected["sha256"]:
            partial.replace(path)
            return
        partial.unlink()
        offset = 0
    headers = {"Accept-Encoding": "identity"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    with client.stream("GET", url, headers=headers) as response:
        response.raise_for_status()
        if response.status_code == 206:
            match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", response.headers.get("content-range", ""))
            if not match or int(match[1]) != offset or int(match[3]) != expected["bytes"] or int(match[2]) != expected["bytes"] - 1:
                raise ValueError("Unexpected resumed download range")
        elif response.status_code == 200:
            offset = 0
        else:
            raise ValueError("Unexpected asset response")
        if response.headers.get("content-encoding", "identity").lower() != "identity":
            raise ValueError("Database assets must use their original byte encoding")
        with partial.open("ab" if offset else "wb") as output:
            os.chmod(partial, 0o600)
            last_report = time.monotonic()
            for block in response.iter_raw(chunk_size=1024 * 1024):
                if stop is not None and stop.is_set():
                    raise InterruptedError("Catalog download checkpoint retained")
                offset += len(block)
                if offset > expected["bytes"]:
                    raise ValueError("Downloaded asset exceeded its declared size")
                output.write(block)
                if time.monotonic() - last_report >= 10:
                    progress("download", asset=path.name, bytes=offset, total=expected["bytes"])
                    last_report = time.monotonic()
            output.flush()
            os.fsync(output.fileno())
    if offset != expected["bytes"]:
        raise ValueError("Incomplete database asset; saved partial download")
    if digest(partial) != expected["sha256"]:
        partial.unlink()
        raise ValueError("Database asset checksum mismatch")
    partial.replace(path)


def download_snapshot(client, repository, pointer, folder, *, stop=None):
    tag, expected = pointer.get("tag"), pointer.get("manifest_sha256")
    if not isinstance(tag, str) or not re.fullmatch(r"catalog-[A-Za-z0-9_.-]{1,150}", tag):
        raise ValueError("Invalid catalog release tag")
    if not isinstance(expected, str) or not HASH.fullmatch(expected):
        raise ValueError("Invalid published manifest checksum")
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    base = f"https://github.com/{repository}/releases/download/{quote(tag, safe='')}"
    manifest, raw = fetch_document(client, base + "/manifest.json", expected_hash=expected)
    parts = validate_manifest(manifest, repository)
    temporary = folder / "manifest.json.partial"
    temporary.write_bytes(raw)
    temporary.replace(folder / "manifest.json")
    dump = folder / "catalog.dump"
    if dump.exists() and dump.stat().st_size == manifest["dump_bytes"] and digest(dump) == manifest["dump_sha256"]:
        return manifest
    if shutil.disk_usage(folder).free < manifest["dump_bytes"] * 2:
        raise RuntimeError("Insufficient free space for catalog download and assembly")
    with ThreadPoolExecutor(max_workers=min(4, len(parts))) as workers:
        downloads = [workers.submit(download_asset, client, base + "/" + item["name"],
                     folder / item["name"], item, stop=stop) for item in parts]
        for download in downloads:
            download.result()
    temporary = folder / "catalog.dump.partial"
    with temporary.open("wb") as output:
        os.chmod(temporary, 0o600)
        for item in parts:
            if stop is not None and stop.is_set():
                raise InterruptedError("Catalog assembly checkpoint retained")
            with (folder / item["name"]).open("rb") as source:
                shutil.copyfileobj(source, output, 4 * 1024 * 1024)
        output.flush()
        os.fsync(output.fileno())
    if temporary.stat().st_size != manifest["dump_bytes"] or digest(temporary) != manifest["dump_sha256"]:
        raise ValueError("Reassembled database checksum mismatch")
    temporary.replace(dump)
    return manifest


def table_metrics(conn):
    result = {}
    for name in TABLES:
        rows, fingerprint = conn.execute(sql.SQL("""SELECT count(*),
            coalesce(sum(hashtextextended(row_to_json(t)::text,0)::numeric),0)::text
            FROM public.{} t""").format(sql.Identifier(name))).fetchone()
        result[name] = {"rows": rows, "content_fingerprint": fingerprint}
    sequence = conn.execute("SELECT pg_get_serial_sequence('public.proxies','proxy_id')").fetchone()[0]
    if not sequence:
        raise ValueError("Catalog has no proxy identity sequence")
    value, called = conn.execute(sql.SQL("SELECT last_value,is_called FROM {}").format(
        sql.Identifier(*sequence.split(".")))).fetchone()
    result["identity_sequence"] = {"last_value": value, "is_called": called}
    return result


def staging_options():
    dsn = os.environ.get("PROXY_STAGING_DATABASE_URL")
    return conninfo_to_dict(dsn) if dsn else {**database_options("proxy"), "dbname": "proxy_stage"}


def restore_snapshot(live, folder, manifest, *, options=None, executable=None, stop=None):
    options = staging_options() if options is None else dict(options)
    # The staging database is created by setup; collectors need no CREATEDB role.
    stage = psycopg.connect(**options, autocommit=True, application_name="proxy-catalog-staging")
    try:
        if stage.info.dbname == live.info.dbname or not stage.info.dbname.endswith(("_stage", "_staging")):
            raise ValueError("Staging database must differ from the live catalog")
        major = int(manifest["postgres_version"].split(".")[0])
        if stage.info.server_version // 10000 != major or live.info.server_version // 10000 != major:
            raise ValueError("Catalog and destination PostgreSQL major versions must match")
        executable = executable or os.environ.get("MEDIA_PG_RESTORE", "pg_restore")
        version = subprocess.check_output([str(executable), "--version"], text=True)
        if not re.search(rf"PostgreSQL\)?\s+{major}\.", version):
            raise ValueError("Install matching PostgreSQL client tools")
        dump = Path(folder) / "catalog.dump"
        if digest(dump) != manifest["dump_sha256"]:
            raise ValueError("Database checksum changed before restore")
        marker = read_json(Path(folder) / "restore-verification.json", {})
        if marker.get("dump_sha256") == manifest["dump_sha256"]:
            try:
                stage.execute("SET TIME ZONE 'UTC'")
                if table_metrics(stage) == manifest["tables"]:
                    return stage
            except psycopg.Error:
                pass
        progress("restore", configurations=manifest["tables"]["proxies"]["rows"])
        schemas = stage.execute("""SELECT nspname FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'""").fetchall()
        for (name,) in schemas:
            stage.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(name)))
        stage.execute("CREATE SCHEMA public")
        connection = dict(options)
        env = os.environ.copy()
        if "password" in connection:
            env["PGPASSWORD"] = connection.pop("password")
        maintenance_mb = min(256, max(16, available_memory() // (1024 * 1024 * 32)))
        env["PGOPTIONS"] = f"-c timezone=UTC -c maintenance_work_mem={maintenance_mb}MB"
        with (Path(folder) / "restore.log").open("w") as log:
            code = run_owned([str(executable), "--no-owner", "--no-acl", "--exit-on-error",
                "--jobs=4", "--dbname", make_conninfo(**connection), str(dump)],
                stop=stop, env=env, stdout=log, stderr=log)
        if code:
            raise RuntimeError("Catalog staging restore failed; see its private restore.log")
        stage.execute("SET TIME ZONE 'UTC'")
        if table_metrics(stage) != manifest["tables"]:
            raise ValueError("Restored counts, fingerprints, or identity sequence differ")
        write_json(Path(folder) / "restore-verification.json", {
            "dump_sha256": manifest["dump_sha256"], "verified_at": utcnow()})
        return stage
    except BaseException:
        stage.close()
        raise


def columns(conn, table):
    return conn.execute("""SELECT attname,format_type(atttypid,atttypmod)
        FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum""",
        ("public." + table,)).fetchall()


def copy_between(source, destination, query, target, names):
    command = sql.SQL("COPY {} ({}) FROM STDIN (FORMAT BINARY)").format(
        sql.Identifier(target), sql.SQL(",").join(map(sql.Identifier, names)))
    with source.cursor().copy(sql.SQL("COPY ({}) TO STDOUT (FORMAT BINARY)").format(query)) as reader:
        with destination.cursor().copy(command) as writer:
            for block in reader:
                writer.write(block)


def merge_snapshot(live, stage, manifest, pointer):
    """Only commit the new generation after every table and identity validates."""
    repository, tag = manifest["repository"], pointer["tag"]
    expected = {name: columns(live, name) for name in TABLES}
    if any(dict(columns(stage, name)) != dict(expected[name]) for name in TABLES):
        raise ValueError("Published catalog schema is incompatible with this application")
    with live.transaction(), stage.transaction():
        stage.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        work_mb = min(128, max(4, available_memory() // (1024 * 1024 * 64)))
        for connection in (live, stage):
            connection.execute("SELECT set_config('work_mem',%s,true)", (f"{work_mb}MB",))
            connection.execute("SELECT set_config('maintenance_work_mem',%s,true)", (f"{min(256, work_mb*2)}MB",))
        existing = live.execute("SELECT manifest_sha256,inserted FROM app_meta.catalog_imports WHERE repository=%s AND tag=%s",
                                (repository, tag)).fetchone()
        if existing:
            if existing[0] != pointer["manifest_sha256"]:
                raise ValueError("Previously imported release changed its manifest")
            return {"tag": tag, "already_imported": True, "inserted": 0}
        latest = live.execute("SELECT max(snapshot_at) FROM app_meta.catalog_imports WHERE repository=%s", (repository,)).fetchone()[0]
        snapshot_at = datetime.fromisoformat(manifest["created_at"].replace("Z", "+00:00"))
        if latest is not None and snapshot_at < latest:
            raise ValueError("Refusing to replace a newer catalog with an older snapshot")
        for name in TABLES:
            live.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE public.{}) ON COMMIT DROP").format(
                sql.Identifier("incoming_" + name), sql.Identifier(name)))
        proxy_columns = [name for name, _ in expected["proxies"]]
        copy_between(stage, live, sql.SQL("SELECT {} FROM public.proxies").format(
            sql.SQL(",").join(map(sql.Identifier, proxy_columns))), "incoming_proxies", proxy_columns)
        live.execute("CREATE UNIQUE INDEX ON incoming_proxies(proxy_id)")
        live.execute("CREATE UNIQUE INDEX ON incoming_proxies(connection_key)")
        live.execute("ANALYZE incoming_proxies")
        conflicting = live.execute("""SELECT EXISTS(
            SELECT 1 FROM incoming_proxies s JOIN public.proxies p USING(proxy_id)
            WHERE (p.connection_key,p.address,p.port,p.connection_settings)
                IS DISTINCT FROM (s.connection_key,s.address,s.port,s.connection_settings)
            UNION ALL SELECT 1 FROM incoming_proxies s JOIN public.proxies p USING(connection_key)
            WHERE p.proxy_id<>s.proxy_id)""").fetchone()[0]
        if conflicting:
            raise ValueError("Catalog proxy IDs or connection identities conflict with local data")
        live.execute("CREATE TEMP TABLE catalog_new_ids(proxy_id BIGINT PRIMARY KEY) ON COMMIT DROP")
        fields = sql.SQL(",").join(map(sql.Identifier, proxy_columns))
        inserted = live.execute(sql.SQL("""WITH added AS (
            INSERT INTO public.proxies ({}) OVERRIDING SYSTEM VALUE
            SELECT {} FROM incoming_proxies s WHERE NOT EXISTS
                (SELECT 1 FROM public.proxies p WHERE p.proxy_id=s.proxy_id)
            RETURNING proxy_id)
            INSERT INTO catalog_new_ids SELECT proxy_id FROM added""").format(fields, fields)).rowcount
        live.execute("""UPDATE public.proxies p SET last_seen_at=s.last_seen_at
            FROM incoming_proxies s WHERE p.proxy_id=s.proxy_id AND s.last_seen_at>p.last_seen_at""")
        stage.execute("CREATE TEMP TABLE catalog_new_ids(proxy_id BIGINT PRIMARY KEY) ON COMMIT DROP")
        copy_between(live, stage, sql.SQL("SELECT proxy_id FROM catalog_new_ids"), "catalog_new_ids", ["proxy_id"])
        stage.execute("ANALYZE catalog_new_ids")
        stat_names = [name for name, _ in expected["proxy_stats"]]
        copy_between(stage, live, sql.SQL("SELECT {} FROM public.proxy_stats s JOIN catalog_new_ids n USING(proxy_id)").format(
                         sql.SQL(",").join(sql.Identifier("s", name) for name in stat_names)),
                     "incoming_proxy_stats", stat_names)
        live.execute("INSERT INTO public.proxy_stats SELECT * FROM incoming_proxy_stats ON CONFLICT(proxy_id) DO NOTHING")
        list_names = [name for name, _ in expected["proxy_lists"]]
        copy_between(stage, live, sql.SQL("SELECT {} FROM public.proxy_lists").format(
            sql.SQL(",").join(map(sql.Identifier, list_names))), "incoming_proxy_lists", list_names)
        assignments = sql.SQL(",").join(sql.SQL("{}=excluded.{}").format(sql.Identifier(name), sql.Identifier(name))
                                        for name in list_names if name != "url")
        live.execute(sql.SQL("INSERT INTO public.proxy_lists SELECT * FROM incoming_proxy_lists ON CONFLICT(url) DO UPDATE SET {}").format(assignments))
        sequence = live.execute("SELECT pg_get_serial_sequence('public.proxies','proxy_id')").fetchone()[0]
        current_value = live.execute(sql.SQL("SELECT last_value FROM {}").format(sql.Identifier(*sequence.split(".")))).fetchone()[0]
        maximum = live.execute("SELECT max(proxy_id) FROM public.proxies").fetchone()[0]
        if maximum is not None:
            live.execute("SELECT setval(%s,%s,true)", (sequence, max(maximum, current_value, manifest["tables"]["identity_sequence"]["last_value"])))
        live.execute("""INSERT INTO app_meta.catalog_imports
            (repository,tag,manifest_sha256,dump_sha256,snapshot_at,configurations,inserted)
            VALUES (%s,%s,%s,%s,%s,%s,%s)""", (repository, tag, pointer["manifest_sha256"], manifest["dump_sha256"],
                snapshot_at, manifest["tables"]["proxies"]["rows"], inserted))
    return {"tag": tag, "already_imported": False, "inserted": inserted,
            "configurations": manifest["tables"]["proxies"]["rows"]}


def clean_catalogs(live, home):
    receipts = live.execute("""SELECT manifest_sha256 FROM app_meta.catalog_imports
        ORDER BY imported_at DESC""").fetchall()
    retained = {row[0] for row in receipts[:2]}
    for (checksum,) in receipts[2:]:
        folder = Path(home) / "catalogs" / checksum
        if checksum not in retained and HASH.fullmatch(checksum) and folder.is_dir() and not folder.is_symlink():
            shutil.rmtree(folder)


def synchronize(repository=DEFAULT_REPOSITORY, *, home=SERVICE_DIR, client=None, stop=None):
    repository = validate_repository(repository)
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    own_client = client is None
    client = client or httpx.Client(follow_redirects=True, timeout=httpx.Timeout(30, connect=15),
                                   headers={"User-Agent": "media-proxy-service/1"})
    with (client if own_client else nullcontext(client)) as client, connect_database("proxy", autocommit=True) as live:
        if not live.execute("SELECT pg_try_advisory_lock(hashtextextended('media:catalog_sync',0))").fetchone()[0]:
            raise RuntimeError("Another catalog synchronization is running")
        pointer, _ = fetch_document(client, f"https://raw.githubusercontent.com/{repository}/main/latest.json", limit=65536)
        tag = pointer.get("tag")
        known = live.execute("SELECT manifest_sha256 FROM app_meta.catalog_imports WHERE repository=%s AND tag=%s",
                             (repository, tag)).fetchone()
        if known:
            if known[0] != pointer.get("manifest_sha256"):
                raise ValueError("Previously imported release changed its manifest")
            clean_catalogs(live, home)
            return {"tag": tag, "already_imported": True, "inserted": 0}
        # The manifest hash pins every download even if a newer release appears.
        expected = pointer.get("manifest_sha256")
        if not isinstance(expected, str) or not HASH.fullmatch(expected):
            raise ValueError("Invalid published manifest checksum")
        folder = home / "catalogs" / expected
        started = time.monotonic()
        progress("download", tag=tag)
        manifest = download_snapshot(client, repository, pointer, folder, stop=stop)
        downloaded = time.monotonic()
        progress("download_complete", seconds=round(downloaded-started, 2), bytes=manifest["dump_bytes"])
        with restore_snapshot(live, folder, manifest, stop=stop) as stage:
            restored = time.monotonic()
            progress("restore_verified", seconds=round(restored-downloaded, 2))
            if stop is not None and stop.is_set():
                raise InterruptedError("Verified catalog retained before import")
            progress("merge", tag=tag)
            result = merge_snapshot(live, stage, manifest, pointer)
        result["seconds"] = {"download": round(downloaded-started, 2), "restore": round(restored-downloaded, 2),
                             "merge": round(time.monotonic()-restored, 2), "total": round(time.monotonic()-started, 2)}
        write_json(home / "catalog-status.json", {**result, "updated_at": utcnow(), "manifest_sha256": expected})
        clean_catalogs(live, home)
        progress("complete", **result)
        return result
