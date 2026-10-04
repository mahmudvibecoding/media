"""Validate immutable comment batches and import each complete scan atomically."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import subprocess
import time

import psycopg

from collect_video_comments import CHANNEL_ID, FIELDS, ROOT, SUCCESS, VIDEO_ID
from comments_bulk import scan_identity
from discover_videos import open_database
from runtime_config import STATE_DIR, connect_database
from metadata_bulk import atomic_json, digest, load_proxies, utcnow
from proxy_statistics import Aggregate, AttemptOutcome, IMPORT_LOCK, ProxyTarget, StatisticsBatch, write_batch


def timestamp(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("Timestamp needs a timezone")
    return parsed


def checked_receipt(path, run_id, *, kind):
    path = Path(path)
    info = json.loads(path.with_suffix(path.suffix + ".json").read_text())
    pattern = r"scans-\d{12}-\d{12}\.jsonl\.gz" if kind == "scans" else r"events-\d{12}-\d{12}\.jsonl\.gz"
    if (not re.fullmatch(pattern, path.name) or info.get("name") != path.name or info.get("run_id") != run_id
            or digest(path) != info.get("sha256")):
        raise ValueError("Batch identity or checksum mismatch")
    if (type(info.get("first_seq")) is not int or type(info.get("last_seq")) is not int
            or not 1 <= info["first_seq"] <= info["last_seq"]):
        raise ValueError("Invalid batch sequence")
    return info


def validate_comment(record, video_id):
    if not isinstance(record, dict) or set(record) != set(FIELDS) or record["video_id"] != video_id:
        raise ValueError("Comment fields or video identity mismatch")
    if not isinstance(record["comment_id"], str) or not record["comment_id"]:
        raise ValueError("Invalid comment ID")
    if not isinstance(record["text"], str):
        raise ValueError("Invalid comment text")
    for name in ("author_channel_id", "author_name"):
        if record[name] is not None and not isinstance(record[name], str):
            raise ValueError("Invalid author field")
    if record["author_channel_id"] is not None and not CHANNEL_ID.fullmatch(record["author_channel_id"]):
        raise ValueError("Invalid author channel ID")
    if record["is_pinned"] is not None and type(record["is_pinned"]) is not bool:
        raise ValueError("Invalid pinned status")
    if any(isinstance(v, str) and "\x00" in v for v in record.values()):
        raise ValueError("NUL is not supported in PostgreSQL text")


class Journal:
    def __init__(self, folder):
        self.folder = Path(folder)
        self.manifest = json.loads((self.folder / "manifest.json").read_text())
        if self.manifest.get("collector") != "comments":
            raise ValueError("Not a comment run")
        self.run_id = self.manifest["run_id"]
        for name in ("videos.jsonl.gz", "proxies.jsonl.gz"):
            if digest(self.folder / name) != self.manifest["files"][name]["sha256"]:
                raise ValueError("Snapshot checksum mismatch")
        self.proxies = {p.proxy_id: p for p in load_proxies(self.folder)}
        self.conn = sqlite3.connect(self.folder / "import.sqlite3", isolation_level=None, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS selected(video_id TEXT PRIMARY KEY,kind TEXT,baseline_at TEXT,baseline_error TEXT);
            CREATE TABLE IF NOT EXISTS imports(scan_id TEXT PRIMARY KEY,video_id TEXT NOT NULL,generation INTEGER NOT NULL,
                batch_sha256 TEXT NOT NULL,state TEXT NOT NULL,outcome TEXT,imported_at TEXT,
                inserted INTEGER NOT NULL DEFAULT 0,refreshed INTEGER NOT NULL DEFAULT 0,comment_count INTEGER NOT NULL,
                recorded_error TEXT);
            CREATE INDEX IF NOT EXISTS imports_video ON imports(video_id,generation,state);
            CREATE TABLE IF NOT EXISTS batches(kind TEXT,name TEXT,sha256 TEXT,first_seq INTEGER,last_seq INTEGER,
                PRIMARY KEY(kind,name));
            CREATE INDEX IF NOT EXISTS batches_cursor ON batches(kind,last_seq);
            CREATE TABLE IF NOT EXISTS stage_scans(seq INTEGER PRIMARY KEY,scan_id TEXT UNIQUE,header TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS stage_comments(scan_id TEXT,video_id TEXT,comment_id TEXT,text TEXT,
                author_channel_id TEXT,author_name TEXT,is_pinned INTEGER,PRIMARY KEY(scan_id,comment_id));
        """)
        if "recorded_error" not in {r[1] for r in self.conn.execute("PRAGMA table_info(imports)")}:
            self.conn.execute("ALTER TABLE imports ADD COLUMN recorded_error TEXT")
        existing = self.conn.execute("SELECT value FROM settings WHERE key='run_id'").fetchone()
        if existing and existing[0] != self.run_id:
            raise ValueError("Import journal belongs to another run")
        if existing is None:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                with gzip.open(self.folder / "videos.jsonl.gz", "rt") as source:
                    for line in source:
                        row = json.loads(line)
                        if not VIDEO_ID.fullmatch(row["video_id"]) or row["kind"] not in ("video", "short"):
                            raise ValueError("Invalid selected video")
                        timestamp(row["baseline_at"])
                        self.conn.execute("INSERT INTO selected VALUES (?,?,?,?)", tuple(row[k] for k in
                            ("video_id", "kind", "baseline_at", "baseline_error")))
                if self.conn.execute("SELECT count(*) FROM selected").fetchone()[0] != self.manifest["videos"]:
                    raise ValueError("Selected video count mismatch")
                self.conn.execute("INSERT INTO settings VALUES ('run_id',?)", (self.run_id,))
                self.conn.commit()
            except BaseException:
                self.conn.rollback()
                raise

    def close(self):
        self.conn.close()

    def cursor(self, kind):
        return self.conn.execute("SELECT coalesce(max(last_seq),0) FROM batches WHERE kind=?", (kind,)).fetchone()[0]

    def record_batch(self, kind, info):
        previous = self.conn.execute("SELECT sha256 FROM batches WHERE kind=? AND name=?", (kind, info["name"])).fetchone()
        if previous:
            if previous[0] != info["sha256"]:
                raise ValueError("Imported batch changed")
            return
        if info["first_seq"] != self.cursor(kind) + 1:
            raise ValueError("Batch sequence has a gap")
        self.conn.execute("INSERT INTO batches VALUES (?,?,?,?,?)",
                          (kind, info["name"], info["sha256"], info["first_seq"], info["last_seq"]))

    def validate_header(self, header):
        video_id, generation = header.get("video_id"), header.get("generation")
        if (not isinstance(video_id, str) or not VIDEO_ID.fullmatch(video_id)
                or type(generation) is not int or generation < 0
                or header.get("run_id") != self.run_id or header.get("version") != 1
                or header.get("scan_id") != scan_identity(self.run_id, video_id, generation)):
            raise ValueError("Scan identity mismatch")
        selected = self.conn.execute("SELECT * FROM selected WHERE video_id=?", (video_id,)).fetchone()
        if (selected is None or selected["kind"] != header.get("kind")
                or timestamp(selected["baseline_at"]) != timestamp(header.get("baseline_at"))
                or selected["baseline_error"] != header.get("baseline_error")):
            raise ValueError("Scan does not match its frozen selection")
        if type(header.get("complete")) is not bool or header["complete"] != (header.get("status") in SUCCESS):
            raise ValueError("Invalid scan completion state")
        if header["complete"] and header.get("stop_reason") not in {"end", "saved_history", "empty", "disabled"}:
            raise ValueError("Scan did not reach its stopping condition")
        if type(header.get("comment_count")) is not int or header["comment_count"] < 0:
            raise ValueError("Invalid scan comment count")
        if not header["complete"] and (header["comment_count"] != 0 or not isinstance(header.get("error"), str)):
            raise ValueError("Failed scan contains rows or lacks an error")
        started, finished = timestamp(header.get("started_at")), timestamp(header.get("finished_at"))
        if started is None or finished is None or finished < started:
            raise ValueError("Invalid scan timestamps")

    def stage(self, path):
        """Validate the complete file before making any changes in PostgreSQL."""
        info = checked_receipt(path, self.run_id, kind="scans")
        current, count, total, sequence = None, 0, 0, []
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("DELETE FROM stage_comments")
            self.conn.execute("DELETE FROM stage_scans")
            with gzip.open(path, "rt") as source:
                for line in source:
                    row = json.loads(line)
                    if row.get("type") == "scan":
                        if current is not None:
                            raise ValueError("Scan is missing its end record")
                        current = row["scan"]
                        self.validate_header(current)
                        seq = row.get("seq")
                        if type(seq) is not int:
                            raise ValueError("Invalid scan sequence")
                        sequence.append(seq)
                        self.conn.execute("INSERT INTO stage_scans VALUES (?,?,?)", (seq, current["scan_id"], json.dumps(current)))
                        count = 0
                    elif row.get("type") == "comment":
                        if current is None or not current["complete"]:
                            raise ValueError("Comment outside a complete scan")
                        record = row["data"]
                        validate_comment(record, current["video_id"])
                        self.conn.execute("INSERT INTO stage_comments VALUES (?,?,?,?,?,?,?)",
                                          (current["scan_id"], *(record[k] for k in FIELDS)))
                        count += 1
                    elif row.get("type") == "end":
                        if (current is None or row.get("scan_id") != current["scan_id"]
                                or row.get("comments") != count or count != current["comment_count"]):
                            raise ValueError("Scan comment count or end identity mismatch")
                        total += count
                        current = None
                    else:
                        raise ValueError("Unknown scan record")
            if (current is not None or sequence != list(range(info["first_seq"], info["last_seq"] + 1))
                    or len(sequence) != info["scans"] or total != info["comments"]):
                raise ValueError("Incomplete or duplicated scan batch")
            self.conn.commit()
        except BaseException:
            self.conn.rollback()
            raise
        return info

    def prepare(self, header, sha, *, outcome, imported_at=None, inserted=0, refreshed=0):
        previous = self.conn.execute("SELECT batch_sha256 FROM imports WHERE scan_id=?", (header["scan_id"],)).fetchone()
        if previous and previous[0] != sha:
            raise ValueError("Scan receipt checksum changed")
        self.conn.execute("""INSERT INTO imports(scan_id,video_id,generation,batch_sha256,state,outcome,
            imported_at,inserted,refreshed,comment_count,recorded_error) VALUES (?,?,?,?,'prepared',?,?,?,?,?,?)
            ON CONFLICT(scan_id) DO UPDATE SET state='prepared',outcome=excluded.outcome,
            imported_at=excluded.imported_at,inserted=excluded.inserted,refreshed=excluded.refreshed,
            recorded_error=excluded.recorded_error""",
            (header["scan_id"], header["video_id"], header["generation"], sha, outcome,
             imported_at.isoformat() if imported_at else None, inserted, refreshed, header["comment_count"],
             header["error"] if outcome == "error" else None))

    def acknowledge(self, scan_id):
        self.conn.execute("UPDATE imports SET state='applied' WHERE scan_id=?", (scan_id,))


def ensure_import_stage(media):
    media.execute("""CREATE TEMP TABLE IF NOT EXISTS comments_import_stage(
        video_id TEXT,comment_id TEXT PRIMARY KEY,text TEXT,author_channel_id TEXT,author_name TEXT,is_pinned BOOLEAN)
        ON COMMIT DELETE ROWS""")


def apply_scan(media, journal, header, sha, *, acknowledge=True, stage_ready=False):
    existing = journal.conn.execute("SELECT * FROM imports WHERE scan_id=?", (header["scan_id"],)).fetchone()
    if existing and existing["batch_sha256"] != sha:
        raise ValueError("Scan receipt checksum changed")
    if existing and existing["state"] == "applied":
        return {"replayed": 1, "inserted": 0, "refreshed": 0}
    newer = journal.conn.execute("SELECT 1 FROM imports WHERE video_id=? AND generation>? AND state='applied' LIMIT 1",
                                 (header["video_id"], header["generation"])).fetchone()
    result = {"replayed": 0, "inserted": 0, "refreshed": 0}
    with media.transaction():
        if not media.execute("SELECT pg_try_advisory_xact_lock(hashtextextended(%s,0))",
                             ("media.video-comments:" + header["video_id"],)).fetchone()[0]:
            raise RuntimeError("Another comment writer holds this video")
        current = media.execute("SELECT comments_updated_at,comments_error FROM public.videos WHERE video_id=%s FOR UPDATE",
                                (header["video_id"],)).fetchone()
        if current is None:
            raise ValueError("A selected video is missing from PostgreSQL")
        prepared = timestamp(existing["imported_at"]) if existing else None
        if prepared is not None and current[0] == prepared:
            # PostgreSQL committed before the previous process acknowledged it.
            result.update(replayed=1)
        elif newer or current[0] != timestamp(header["baseline_at"]):
            journal.prepare(header, sha, outcome="skipped_newer_scan")
            result["skipped_newer_scan"] = 1
        elif not header["complete"]:
            prior_error = journal.conn.execute("""SELECT recorded_error FROM imports WHERE video_id=?
                AND generation<? AND state='applied' AND outcome='error' ORDER BY generation DESC LIMIT 1""",
                (header["video_id"], header["generation"])).fetchone()
            own_error = prior_error and prior_error[0] is not None and current[1] == prior_error[0]
            if current[1] not in (header["baseline_error"], header["error"]) and not own_error:
                journal.prepare(header, sha, outcome="skipped_newer_error")
                result["skipped_newer_error"] = 1
            else:
                journal.prepare(header, sha, outcome="error")
                result["errors_recorded"] = media.execute("UPDATE public.videos SET comments_error=%s WHERE video_id=%s AND comments_error IS DISTINCT FROM %s",
                    (header["error"], header["video_id"], header["error"])).rowcount
        else:
            inserted = refreshed = 0
            if header["comment_count"]:
                if not stage_ready:
                    ensure_import_stage(media)
                # TRUNCATE for every video repeatedly rewrites pg_class and
                # stalls concurrent importers. COMMIT clears the table once per
                # group; ordinary DELETE reuses it between videos in that group.
                media.execute("DELETE FROM comments_import_stage")
                with media.cursor().copy("COPY comments_import_stage FROM STDIN") as copy:
                    for row in journal.conn.execute("SELECT video_id,comment_id,text,author_channel_id,author_name,is_pinned FROM stage_comments WHERE scan_id=? ORDER BY comment_id", (header["scan_id"],)):
                        values = tuple(row)
                        copy.write_row((*values[:-1], bool(values[-1]) if values[-1] is not None else None))
                inserted = media.execute("""INSERT INTO public.comments SELECT * FROM comments_import_stage
                    ON CONFLICT(video_id,comment_id) DO NOTHING""").rowcount
                refreshed = media.execute("""UPDATE public.comments c SET text=s.text,author_channel_id=s.author_channel_id,
                    author_name=s.author_name,is_pinned=s.is_pinned FROM comments_import_stage s
                    WHERE c.video_id=s.video_id AND c.comment_id=s.comment_id AND
                    (c.text,c.author_channel_id,c.author_name,c.is_pinned) IS DISTINCT FROM
                    (s.text,s.author_channel_id,s.author_name,s.is_pinned)""").rowcount
            at = media.execute("SELECT clock_timestamp()").fetchone()[0]
            # Persist this exact completion timestamp before the PostgreSQL
            # commit. It identifies a successful commit after a lost acknowledgement.
            journal.prepare(header, sha, outcome="success", imported_at=at, inserted=inserted, refreshed=refreshed)
            media.execute("UPDATE public.videos SET comments_updated_at=%s,comments_error=NULL WHERE video_id=%s", (at, header["video_id"]))
            result.update(inserted=inserted, refreshed=refreshed)
    if acknowledge:
        journal.acknowledge(header["scan_id"])
    return result


def apply_scan_batch(media, journal, path):
    info = journal.stage(path)
    existing = journal.conn.execute("SELECT sha256 FROM batches WHERE kind='scans' AND name=?", (info["name"],)).fetchone()
    if existing is None and info["first_seq"] != journal.cursor("scans") + 1:
        raise ValueError("Scan batches have a gap")
    if existing and existing[0] != info["sha256"]:
        raise ValueError("Imported scan batch changed")
    counts = Counter()
    headers = [json.loads(row[0]) for row in journal.conn.execute("SELECT header FROM stage_scans ORDER BY seq")]
    stage_ready = any(header["comment_count"] for header in headers)
    if stage_ready:
        ensure_import_stage(media)
    while headers:
        group, seen = [], set()
        for header in headers:
            # A retry of the same video must see the previous generation's
            # committed receipt before deciding whether its error is ours.
            if len(group) >= 64 or header["video_id"] in seen:
                break
            group.append(header)
            seen.add(header["video_id"])
        headers = headers[len(group):]
        with media.transaction():
            journal.conn.execute("BEGIN IMMEDIATE")
            try:
                for header in group:
                    counts.update(apply_scan(media, journal, header, info["sha256"], acknowledge=False,
                                             stage_ready=stage_ready))
                # Receipts must be durable before PostgreSQL can commit.
                journal.conn.commit()
            except BaseException:
                journal.conn.rollback()
                raise
        journal.conn.execute("BEGIN IMMEDIATE")
        try:
            for header in group:
                journal.acknowledge(header["scan_id"])
            journal.conn.commit()
        except BaseException:
            journal.conn.rollback()
            raise
    journal.record_batch("scans", info)
    return {"file": info["name"], "scans": info["scans"], "comments": info["comments"], **counts}


def proxy_batch(journal, path):
    info = checked_receipt(path, journal.run_id, kind="events")
    aggregates, sequence = {}, []
    with gzip.open(path, "rt") as source:
        for line in source:
            row = json.loads(line)
            event = row["event"]
            sequence.append(row["seq"])
            if event.get("collector") != "comments" or not isinstance(event.get("video_id"), str):
                raise ValueError("Invalid comment attempt event")
            if journal.conn.execute("SELECT 1 FROM selected WHERE video_id=?", (event["video_id"],)).fetchone() is None:
                raise ValueError("Attempt belongs to an unselected video")
            proxy = event["proxy"]
            actual = journal.proxies.get(proxy.get("id"))
            if (actual is None or actual.connection_key.hex() != proxy.get("key")
                    or actual.working_protocol != proxy.get("protocol")):
                raise ValueError("Attempt proxy identity mismatch")
            observed, category = event.get("observation"), event.get("category")
            if observed is None:
                if category not in ("local_error", "cancelled"):
                    raise ValueError("Missing attempt observation")
                continue
            outcome = AttemptOutcome(**{**observed, "checked_at": timestamp(observed["checked_at"])})
            outcome.validate()
            expected = {"data": True, "video_error": None, "response_pending": None,
                        "connection_error": False, "http_error": False, "response_error": False}
            if category not in expected or outcome.data_received is not expected[category]:
                raise ValueError("Attempt and proxy scoring disagree")
            if category in ("data", "video_error", "response_pending") and (outcome.http_status != 200 or outcome.connection_error):
                raise ValueError("Invalid successful or neutral response observation")
            target = ProxyTarget.from_catalog(actual.proxy_id, actual.connection_key, actual.working_protocol)
            aggregates.setdefault(target, Aggregate()).add(outcome, target.protocol)
    if sequence != list(range(info["first_seq"], info["last_seq"] + 1)) or len(sequence) != info["rows"]:
        raise ValueError("Attempt batch sequence is incomplete or duplicated")
    key = hashlib.sha256((journal.run_id + ":" + info["sha256"]).encode()).digest()
    return info, StatisticsBatch(aggregates, key=key)


def apply_proxy_batch(proxy_conn, journal, path):
    info, batch = proxy_batch(journal, path)
    existing = journal.conn.execute("SELECT sha256 FROM batches WHERE kind='events' AND name=?", (info["name"],)).fetchone()
    if existing:
        if existing[0] != info["sha256"]:
            raise ValueError("Imported attempt batch changed")
        return {"file": info["name"], "replayed_batch": True}
    if info["first_seq"] != journal.cursor("events") + 1:
        raise ValueError("Attempt batches have a gap")
    with proxy_conn.transaction():
        # Parallel video importers share the existing statistics writer. Queue
        # at its transaction lock instead of rejecting competing importers.
        proxy_conn.execute("SELECT pg_advisory_xact_lock(%s)", (IMPORT_LOCK,))
        result = write_batch(proxy_conn, batch)
        if result["unmatched_attempts"] or result["stale_attempts"]:
            raise ValueError("Proxy identity changed or attempt statistics overlap")
    journal.record_batch("events", info)
    return {"file": info["name"], **result}


def ready_batches(folder, *, kind, after=0):
    folder = Path(folder) / "outbox"
    if kind == "scans":
        folder /= "scans"
    for receipt in sorted(folder.glob(kind + "-*.jsonl.gz.json")):
        path = receipt.with_suffix("")
        match = re.fullmatch(kind + r"-\d{12}-(\d{12})\.jsonl\.gz", path.name)
        if match is None:
            raise ValueError("Invalid published batch filename")
        if int(match[1]) <= after:
            continue
        if not path.is_file():
            break
        yield path


def import_status(journal, remote):
    outcomes = dict(journal.conn.execute("SELECT outcome,count(*) FROM imports WHERE state='applied' GROUP BY outcome"))
    totals = journal.conn.execute("SELECT coalesce(sum(inserted),0),coalesce(sum(refreshed),0) FROM imports WHERE state='applied'").fetchone()
    return {"run_id": journal.run_id, "updated_at": utcnow(), "outcomes": outcomes,
        "inserted": totals[0], "refreshed": totals[1], "scan_seq": journal.cursor("scans"), "event_seq": journal.cursor("events"),
        "complete": (remote.get("state") == "complete" and remote.get("completions") == journal.cursor("scans")
                     and remote.get("events") == journal.cursor("events")), "worker": remote}


def sync_remote(folder, host, remote):
    destination = Path(folder)
    remote = str(remote).rstrip("/")
    socket_id = hashlib.sha256((host + str(destination.resolve())).encode()).hexdigest()[:16]
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ControlMaster=auto",
           "-o", "ControlPersist=60", "-o", "ControlPath=" + str(STATE_DIR / ("comments-ssh-" + socket_id))]
    subprocess.run(["rsync", "-a", "--ignore-existing", "--include=*/", "--include=*.jsonl.gz",
        "--include=*.jsonl.gz.json", "--exclude=*", "-e", shlex.join(ssh),
        host + ":" + shlex.quote(remote + "/outbox/"), str(destination / "outbox") + "/"],
        check=True, timeout=60, stdout=subprocess.DEVNULL)
    response = subprocess.run([*ssh, host,
        "cat " + shlex.quote(remote + "/manifest.json") + " " + shlex.quote(remote + "/status.json")],
        check=True, timeout=20, text=True, capture_output=True)
    # Both files are one-line atomic JSON documents.
    manifest, state = [json.loads(line) for line in response.stdout.splitlines() if line.strip()]
    local = json.loads((destination / "manifest.json").read_text())
    if manifest != local:
        raise ValueError("Remote run manifest differs from the local snapshot")
    atomic_json(destination / "remote-status.json", state)
    return state


def sync(folder, *, host=None, remote=None, watch=False, interval=5):
    folder = Path(folder).resolve()
    if bool(host) != bool(remote) or interval <= 0 or interval > 60:
        raise ValueError("Provide both host and remote; interval must be in (0,60]")
    with (folder / "import.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        journal = Journal(folder)
        media = proxy = None
        worker, last_sync = {}, 0
        try:
            while True:
                errors, processed = [], 0
                try:
                    if not worker or time.monotonic() - last_sync >= interval:
                        worker = sync_remote(folder, host, remote) if host else json.loads((folder / "status.json").read_text())
                        last_sync = time.monotonic()
                    if media is None or media.closed:
                        media = open_database(autocommit=True)
                    started = time.monotonic()
                    for path in ready_batches(folder, kind="scans", after=journal.cursor("scans")):
                        result = apply_scan_batch(media, journal, path)
                        processed += 1
                        print(json.dumps({"scan_batch": result}), flush=True)
                        if watch and time.monotonic() - started >= 2:
                            break
                    if proxy is None or proxy.closed:
                        proxy = connect_database("proxy", autocommit=True, application_name="bulk-comments-import")
                    started = time.monotonic()
                    for path in ready_batches(folder, kind="events", after=journal.cursor("events")):
                        result = apply_proxy_batch(proxy, journal, path)
                        processed += 1
                        print(json.dumps({"attempt_batch": result}), flush=True)
                        if watch and time.monotonic() - started >= 2:
                            break
                except (OSError, subprocess.SubprocessError, psycopg.Error, RuntimeError) as exc:
                    errors.append(type(exc).__name__)
                    for connection in (media, proxy):
                        if connection is not None:
                            connection.close()
                    media = proxy = None
                state = import_status(journal, worker)
                state["errors"] = errors
                if errors:
                    state["complete"] = False
                atomic_json(folder / "import-status.json", state)
                print(json.dumps(state), flush=True)
                if not watch or state["complete"]:
                    return state
                if not processed or errors:
                    time.sleep(min(interval, max(0.2, interval - (time.monotonic() - last_sync))))
        finally:
            for connection in (media, proxy):
                if connection is not None:
                    connection.close()
            journal.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--host")
    parser.add_argument("--remote")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=5)
    args = parser.parse_args()
    result = sync(args.run, host=args.host, remote=args.remote, watch=args.watch, interval=args.interval)
    if result["errors"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
