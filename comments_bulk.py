"""Checkpointed top-level comment collection using the existing ranked proxy pool."""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, replace
from datetime import datetime, timezone
import fcntl
from functools import partial
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import sqlite3
import time
import uuid

from collect_subscribers import CLIENT_VERSION
from collect_video_comments import (CommentError, FIELDS, VIDEO_ID, check_payload,
    comment_items, fetch_json, header_info, initial_continuation, message_status, parse_page, watch_status)
from collection_policy import CollectionOutcome, ProxyPerformance
from discover_videos import open_database
from metadata_bulk import (ProxyPool, atomic_json, connect_queue, digest, export_events,
                           load_proxies, setting, utcnow)
from proxy_catalog import CatalogClients, load_catalog


METRICS = ("attempts", "request_body_bytes", "response_body_bytes", "decoded_body_bytes")


def scan_identity(run_id, video_id, generation=0):
    return str(uuid.uuid5(uuid.UUID(run_id), f"{video_id}:{generation}"))


@contextmanager
def gzip_writer(path):
    with Path(path).open("xb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0, compresslevel=1) as compressed:
            yield lambda value: compressed.write((json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode())
        raw.flush()
        os.fsync(raw.fileno())


def snapshot(folder, ranked, *, limit=1000, include=(), from_run=None, proxy_limit=512):
    folder, ranked = Path(folder), Path(ranked)
    if limit < 0 or proxy_limit < 1:
        raise ValueError("Invalid snapshot limits")
    folder.mkdir(parents=True, exist_ok=False, mode=0o700)
    ranking = [json.loads(line) for line in ranked.read_text().splitlines()]
    identifiers = [row["proxy_id"] for row in ranking[:proxy_limit]]
    if not identifiers or len(set(identifiers)) != len(identifiers):
        raise ValueError("Ranked proxy IDs must be nonempty and unique")
    proxies = {p.proxy_id: p for p in load_catalog(identifiers)}
    manifest = {"version": 1, "collector": "comments", "run_id": str(uuid.uuid4()),
                "created_at": utcnow(), "files": {}, "ranked_pool_sha256": digest(ranked),
                "selection": "previous_selection" if from_run else "all" if not limit else "deterministic_sample"}
    include = list(dict.fromkeys(include))
    if any(not VIDEO_ID.fullmatch(v) for v in include) or limit and len(include) > limit:
        raise ValueError("Invalid included video IDs")
    counts = Counter()
    with open_database() as conn:
        conn.execute("CREATE TEMP TABLE comment_export_selection(video_id TEXT PRIMARY KEY)")
        if from_run:
            source = Path(from_run) / "videos.jsonl.gz"
            previous = json.loads((Path(from_run) / "manifest.json").read_text())
            if digest(source) != previous["files"][source.name]["sha256"]:
                raise ValueError("Previous selection checksum mismatch")
            with conn.cursor().copy("COPY comment_export_selection FROM STDIN") as copy, gzip.open(source, "rt") as rows:
                for line in rows:
                    copy.write_row((json.loads(line)["video_id"],))
        else:
            if include:
                conn.execute("INSERT INTO comment_export_selection SELECT unnest(%s::text[])", (include,))
            query = "INSERT INTO comment_export_selection SELECT video_id FROM public.videos WHERE NOT (video_id=ANY(%s::text[]))"
            params = [include]
            if limit:
                query += " ORDER BY md5(video_id),video_id LIMIT %s"
                params.append(limit - len(include))
            conn.execute(query, params)
        selected = conn.execute("SELECT count(*) FROM comment_export_selection").fetchone()[0]
        conn.commit()
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        with gzip_writer(folder / "videos.jsonl.gz") as write, conn.cursor(name="comments_video_snapshot") as cursor:
            cursor.itersize = 10000
            cursor.execute("""SELECT v.video_id,v.type,v.comments_updated_at,v.comments_error
                FROM public.videos v JOIN comment_export_selection s USING(video_id) ORDER BY v.video_id""")
            for video_id, kind, baseline, error in cursor:
                write({"video_id": video_id, "kind": kind, "baseline_at": baseline.isoformat() if baseline else None,
                       "baseline_error": error})
                counts.update(videos=1, **{kind: 1}, with_history=int(baseline is not None))
        if counts["videos"] != selected or selected == 0:
            raise ValueError("Selection contains missing videos or is empty")
        with gzip_writer(folder / "history.jsonl.gz") as write, conn.cursor(name="comments_history_snapshot") as cursor:
            cursor.itersize = 10000
            cursor.execute("""SELECT c.video_id,c.comment_id,c.is_pinned FROM public.comments c
                JOIN comment_export_selection s USING(video_id) JOIN public.videos v USING(video_id)
                WHERE v.comments_updated_at IS NOT NULL ORDER BY c.video_id,c.comment_id""")
            for row in cursor:
                write(list(row))
                counts.update(history_comments=1)
    with gzip_writer(folder / "proxies.jsonl.gz") as write:
        for identifier in identifiers:
            write(proxies[identifier].bridge_record())
    manifest.update(counts, proxies=len(proxies), protocols=dict(Counter(p.working_protocol for p in proxies.values())))
    for name in ("videos.jsonl.gz", "history.jsonl.gz", "proxies.jsonl.gz"):
        path = folder / name
        path.chmod(0o600)
        manifest["files"][name] = {"sha256": digest(path), "bytes": path.stat().st_size}
    atomic_json(folder / "manifest.json", manifest)
    return manifest


def initialize(folder, *, concurrency=64, max_attempts=6, max_pages=10000, timeout=20):
    folder = Path(folder)
    manifest = json.loads((folder / "manifest.json").read_text())
    if manifest.get("collector") != "comments" or manifest.get("version") != 1:
        raise ValueError("Not a comments snapshot")
    if not 1 <= concurrency <= 1024 or min(max_attempts, max_pages, timeout) < 1:
        raise ValueError("Invalid runner limits")
    for name, info in manifest["files"].items():
        if name not in {"videos.jsonl.gz", "history.jsonl.gz", "proxies.jsonl.gz"} or digest(folder / name) != info["sha256"]:
            raise ValueError("Snapshot checksum mismatch")
    path = folder / "queue.sqlite3"
    if path.exists():
        raise ValueError("Queue already exists; use run to resume it")
    with connect_queue(path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript("""
            CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE jobs(video_id TEXT PRIMARY KEY,kind TEXT NOT NULL,baseline_at TEXT,baseline_error TEXT,
                scan_id TEXT NOT NULL,generation INTEGER NOT NULL DEFAULT 0,state TEXT NOT NULL DEFAULT 'ready',
                phase TEXT NOT NULL DEFAULT 'initial',token TEXT,lease TEXT,pages INTEGER NOT NULL DEFAULT 0,
                pass_pages INTEGER NOT NULL DEFAULT 0,token_restarts INTEGER NOT NULL DEFAULT 0,
                failures INTEGER NOT NULL DEFAULT 0,local_failures INTEGER NOT NULL DEFAULT 0,last_proxy_id INTEGER,
                error_proxy_id INTEGER,error_reason TEXT,error_seen_at REAL,started_at TEXT,finished_at TEXT,
                stop_reason TEXT,last_error TEXT,metrics TEXT NOT NULL DEFAULT '{}',turn INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX jobs_ready ON jobs(state,failures,turn,video_id);
            CREATE TABLE history(video_id TEXT,scope INTEGER,comment_id TEXT,is_pinned INTEGER,
                PRIMARY KEY(video_id,scope,comment_id));
            CREATE TABLE comments(video_id TEXT,comment_id TEXT,text TEXT NOT NULL,
                author_channel_id TEXT,author_name TEXT,is_pinned INTEGER,PRIMARY KEY(video_id,comment_id));
            CREATE TABLE pass_seen(video_id TEXT,comment_id TEXT,PRIMARY KEY(video_id,comment_id));
            CREATE TABLE tokens(video_id TEXT,token_hash TEXT,PRIMARY KEY(video_id,token_hash));
            CREATE TABLE completions(seq INTEGER PRIMARY KEY AUTOINCREMENT,scan_id TEXT UNIQUE NOT NULL,
                video_id TEXT NOT NULL,payload TEXT NOT NULL);
            CREATE TABLE events(seq INTEGER PRIMARY KEY AUTOINCREMENT,payload TEXT NOT NULL);
            CREATE TABLE proxy_usage(proxy_id INTEGER PRIMARY KEY,attempts INTEGER NOT NULL DEFAULT 0,
                successes INTEGER NOT NULL DEFAULT 0,local_failures INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE proxy_performance(proxy_id INTEGER PRIMARY KEY,quality REAL NOT NULL,latency_seconds REAL,
                samples INTEGER NOT NULL,failure_streak INTEGER NOT NULL,updated_at REAL NOT NULL,cooldown_until REAL NOT NULL);
        """)
        conn.execute("BEGIN IMMEDIATE")
        config = {"run_id": manifest["run_id"], "state": "ready", "started_at": None, "active_seconds": 0,
                  "exported_seq": 0, "scans_exported_seq": 0, "recoveries": 0,
                  "concurrency": concurrency, "max_attempts": max_attempts, "max_pages": max_pages, "timeout": timeout}
        conn.executemany("INSERT INTO settings VALUES (?,?)", [(k, json.dumps(v)) for k, v in config.items()])
        with gzip.open(folder / "videos.jsonl.gz", "rt") as source:
            for line in source:
                row = json.loads(line)
                conn.execute("INSERT INTO jobs(video_id,kind,baseline_at,baseline_error,scan_id) VALUES (?,?,?,?,?)",
                    (row["video_id"], row["kind"], row["baseline_at"], row["baseline_error"],
                     scan_identity(manifest["run_id"], row["video_id"])))
        with gzip.open(folder / "history.jsonl.gz", "rt") as source:
            conn.executemany("INSERT INTO history VALUES (?,0,?,?)", (json.loads(line) for line in source))
        if conn.execute("SELECT count(*) FROM jobs").fetchone()[0] != manifest["videos"]:
            raise ValueError("Snapshot count mismatch")
        conn.commit()
    (folder / "outbox" / "scans").mkdir(parents=True)
    atomic_json(folder / "control.json", {"concurrency": concurrency, "stop": False})
    return {"initialized": manifest["videos"], "concurrency": concurrency}


def parse_step(phase, payload, video_id):
    check_payload(payload, video_id)
    if phase == "page":
        return {"phase": "page", "valid_data": True, **parse_page(payload, video_id)}
    if phase == "watch":
        state = watch_status(payload, video_id)
        if state is None:
            raise CommentError("unexpected_response", "No recognized comment section or terminal message")
        return {"terminal": state, "valid_data": None if state == "unavailable" else True}
    if phase != "initial":
        raise ValueError("Invalid scan phase")
    items, found, has_body = comment_items(payload, selecting_sort=True)
    header, state = header_info(items), message_status(items)
    if has_body and header and header["newest_selected"]:
        return {"phase": "page", "valid_data": True, **parse_page(payload, video_id)}
    token = header["token"] if header else None
    if isinstance(token, str) and token:
        if state is not None:
            raise CommentError("unexpected_response", "Sort option conflicts with comment message")
        return {"phase": "page", "token": token, "valid_data": True}
    if any("commentThreadRenderer" in item or "continuationItemRenderer" in item for item in items):
        raise CommentError("sort_unverified", "Cannot select Newest for existing comments")
    state = state or ("empty" if found and header and header["empty"] else None)
    return {"terminal": state, "valid_data": True} if state else {"phase": "watch", "valid_data": None}


class Queue:
    """The runner owns this connection; its storage thread serializes page commits."""
    def __init__(self, folder):
        self.folder = Path(folder)
        self.conn = sqlite3.connect(self.folder / "queue.sqlite3", isolation_level=None, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA synchronous=FULL")
        # Keep the active comment indexes in memory during large backfills.
        # This is a connection-local cache; commit durability stays FULL.
        self.conn.execute("PRAGMA cache_size=-131072")
        self.run_id = setting(self.conn, "run_id")

    def close(self):
        self.conn.close()

    @contextmanager
    def transaction(self):
        nested = self.conn.in_transaction
        self.conn.execute("SAVEPOINT comment_checkpoint" if nested else "BEGIN IMMEDIATE")
        try:
            yield
            if nested:
                self.conn.execute("RELEASE SAVEPOINT comment_checkpoint")
            else:
                self.conn.commit()
        except BaseException:
            if nested:
                self.conn.execute("ROLLBACK TO SAVEPOINT comment_checkpoint")
                self.conn.execute("RELEASE SAVEPOINT comment_checkpoint")
            else:
                self.conn.rollback()
            raise

    def set(self, key, value):
        self.conn.execute("UPDATE settings SET value=? WHERE key=?", (json.dumps(value), key))

    def recover(self):
        with self.transaction():
            count = self.conn.execute("SELECT count(*) FROM jobs WHERE state='leased'").fetchone()[0]
            self.conn.execute("UPDATE jobs SET state='ready',lease=NULL WHERE state='leased'")
            self.set("recoveries", setting(self.conn, "recoveries") + count)
        return count

    def claim(self):
        rows = self.claim_many(1)
        return rows[0] if rows else None

    def claim_many(self, count):
        if not 1 <= count <= 1024:
            raise ValueError("Invalid claim batch size")
        with self.transaction():
            rows = self.conn.execute("SELECT * FROM jobs WHERE state='ready' ORDER BY failures,turn,video_id LIMIT ?", (count,)).fetchall()
            claimed = []
            for row in rows:
                lease = str(uuid.uuid4())
                self.conn.execute("UPDATE jobs SET state='leased',lease=?,started_at=coalesce(started_at,?) WHERE video_id=?",
                                  (lease, utcnow(), row["video_id"]))
                claimed.append({**dict(row), "lease": lease})
            return claimed

    def _record(self, event, outcome):
        seq = self.conn.execute("INSERT INTO events(payload) VALUES (?)",
                                (json.dumps(event, separators=(",", ":"), default=str),)).lastrowid
        proxy_id = event["proxy"]["id"]
        observed = event["observation"] is not None
        row = self.conn.execute("SELECT * FROM proxy_performance WHERE proxy_id=?", (proxy_id,)).fetchone()
        profile = ProxyPerformance.restore(row, {}, time.time())
        profile.observe(outcome, event["seconds"], time.time())
        self.conn.execute("""INSERT INTO proxy_performance VALUES (?,?,?,?,?,?,?) ON CONFLICT(proxy_id) DO UPDATE SET
            quality=excluded.quality,latency_seconds=excluded.latency_seconds,samples=excluded.samples,
            failure_streak=excluded.failure_streak,updated_at=excluded.updated_at,cooldown_until=excluded.cooldown_until""",
            (proxy_id, *asdict(profile).values()))
        self.conn.execute("""INSERT INTO proxy_usage VALUES (?,?,?,?) ON CONFLICT(proxy_id) DO UPDATE SET
            attempts=attempts+excluded.attempts,successes=successes+excluded.successes,local_failures=local_failures+excluded.local_failures""",
            (proxy_id, int(observed), int(outcome.proxy_success is True), int(not observed)))
        return seq

    def _reset_pass(self, video_id):
        self.conn.execute("DELETE FROM pass_seen WHERE video_id=?", (video_id,))
        self.conn.execute("DELETE FROM tokens WHERE video_id=?", (video_id,))

    def _complete(self, video_id, status, reason, *, error=None):
        job = dict(self.conn.execute("SELECT * FROM jobs WHERE video_id=?", (video_id,)).fetchone())
        count = self.conn.execute("SELECT count(*) FROM comments WHERE video_id=?", (video_id,)).fetchone()[0]
        successful = status in ("ok", "empty", "disabled")
        at = utcnow()
        self.conn.execute("UPDATE jobs SET state=?,finished_at=?,stop_reason=?,last_error=?,lease=NULL WHERE video_id=?",
                          ("completed" if successful else "failed", at, reason, error, video_id))
        payload = {"version": 1, "run_id": self.run_id, "scan_id": job["scan_id"], "generation": job["generation"],
            "video_id": video_id, "kind": job["kind"], "baseline_at": job["baseline_at"], "baseline_error": job["baseline_error"],
            "status": status, "complete": successful, "stop_reason": reason, "error": error,
            "started_at": job["started_at"], "finished_at": at, "pages": job["pages"],
            "token_restarts": job["token_restarts"],
            "comment_count": count if successful else 0, "buffered_comments": count, "metrics": json.loads(job["metrics"])}
        self.conn.execute("INSERT INTO completions(scan_id,video_id,payload) VALUES (?,?,?)",
                          (job["scan_id"], video_id, json.dumps(payload, separators=(",", ":"))))

    def finish(self, claimed, parsed, error, event, outcome):
        with self.transaction():
            current = self.conn.execute("SELECT * FROM jobs WHERE video_id=?", (claimed["video_id"],)).fetchone()
            if current is None or current["state"] != "leased" or current["lease"] != claimed["lease"]:
                raise ValueError("Stale or unowned comment checkpoint")
            job, video_id = dict(current), current["video_id"]
            seq = self._record(event, outcome)
            totals = Counter(json.loads(job["metrics"]))
            totals.update({name: event["metrics"].get(name, 0) for name in METRICS})
            self.conn.execute("UPDATE jobs SET metrics=?,turn=?,lease=NULL,last_proxy_id=? WHERE video_id=?",
                              (json.dumps(totals), seq, event["proxy"]["id"], video_id))
            if error is not None:
                if error.status == "interrupted":
                    self.conn.execute("UPDATE jobs SET state='ready' WHERE video_id=?", (video_id,))
                    return
                local = error.status == "local_error"
                failures, locals_ = job["failures"] + int(not local), job["local_failures"] + int(local)
                self.conn.execute("UPDATE jobs SET failures=?,local_failures=?,last_error=?,state='ready' WHERE video_id=?",
                                  (failures, locals_, error.status + ": " + str(error), video_id))
                if (job["phase"] == "page" and job["pages"] and failures >= 2
                        and job["last_proxy_id"] != event["proxy"]["id"] and job["token_restarts"] < 2
                        and (error.status == "unexpected_response" or str(error) == "HTTP 400")):
                    self._reset_pass(video_id)
                    self.conn.execute("""UPDATE jobs SET phase='initial',token=NULL,pass_pages=0,failures=0,
                        token_restarts=token_restarts+1 WHERE video_id=?""", (video_id,))
                elif failures >= setting(self.conn, "max_attempts") or locals_ >= 5:
                    self._complete(video_id, error.status, "error", error=error.status + ": " + str(error))
                return
            terminal = parsed.get("terminal")
            if terminal == "unavailable":
                confirmed = (job["error_reason"] == terminal and job["error_proxy_id"] != event["proxy"]["id"]
                             and time.time() - (job["error_seen_at"] or 0) < 3600)
                self.conn.execute("""UPDATE jobs SET phase='watch',state='ready',failures=failures+1,
                    error_proxy_id=?,error_reason=?,error_seen_at=? WHERE video_id=?""",
                    (event["proxy"]["id"], terminal, time.time(), video_id))
                if confirmed or job["failures"] + 1 >= setting(self.conn, "max_attempts"):
                    reason = "unavailable" if confirmed else "unavailable_unconfirmed"
                    self._complete(video_id, reason, reason, error=reason)
                return
            self.conn.execute("UPDATE jobs SET state='ready',failures=0,local_failures=0,last_error=NULL WHERE video_id=?", (video_id,))
            if terminal:
                if job["pages"]:
                    raise CommentError("unexpected_response", "Comment section changed during scan")
                self._complete(video_id, terminal, terminal)
                return
            if "comments" not in parsed:
                self.conn.execute("UPDATE jobs SET phase=?,token=? WHERE video_id=?",
                                  (parsed["phase"], parsed.get("token"), video_id))
                return
            if parsed["state"] and job["pass_pages"]:
                raise CommentError("unexpected_response", "Comment section changed during pagination")
            requested_token = initial_continuation(video_id) if job["phase"] == "initial" else job["token"]
            token_hash = hashlib.sha256(requested_token.encode()).hexdigest()
            if self.conn.execute("SELECT 1 FROM tokens WHERE video_id=? AND token_hash=?", (video_id, token_hash)).fetchone():
                raise CommentError("pagination_loop", "Repeated comment continuation")
            self.conn.execute("INSERT INTO tokens VALUES (?,?)", (video_id, token_hash))
            blocked_ids = {r["comment_id"] for r in parsed["comments"] if r["is_pinned"] is not False}
            boundary, progress = False, 0
            for record in parsed["comments"]:
                cid = record["comment_id"]
                old = self.conn.execute("SELECT is_pinned FROM history WHERE video_id=? AND scope=? AND comment_id=?",
                                        (video_id, 0, cid)).fetchone()
                seen = self.conn.execute("SELECT is_pinned FROM comments WHERE video_id=? AND comment_id=?", (video_id, cid)).fetchone()
                boundary |= (record["is_pinned"] is False and cid not in blocked_ids and old is not None and old[0] == 0
                             and (seen is None or seen[0] == 0))
                progress += self.conn.execute("INSERT OR IGNORE INTO pass_seen VALUES (?,?)", (video_id, cid)).rowcount
                self.conn.execute("""INSERT INTO comments VALUES (?,?,?,?,?,?) ON CONFLICT(video_id,comment_id)
                    DO UPDATE SET text=excluded.text,author_channel_id=excluded.author_channel_id,author_name=excluded.author_name,
                    is_pinned=CASE WHEN comments.is_pinned=1 OR excluded.is_pinned=1 THEN 1
                        WHEN comments.is_pinned IS NULL OR excluded.is_pinned IS NULL THEN NULL ELSE 0 END""",
                    tuple(record[f] for f in FIELDS))
            token = parsed["continuation"]
            self.conn.execute("UPDATE jobs SET phase='page',pages=pages+1,pass_pages=pass_pages+1,token=? WHERE video_id=?", (token, video_id))
            if boundary or token is None:
                total = self.conn.execute("SELECT count(*) FROM comments WHERE video_id=?", (video_id,)).fetchone()[0]
                self._complete(video_id, parsed["state"] or ("ok" if total else "empty"),
                               "saved_history" if boundary else parsed["state"] or "end")
            elif not progress:
                raise CommentError("pagination_loop", "Comment page made no progress")
            elif job["pages"] + 1 >= setting(self.conn, "max_pages"):
                self._complete(video_id, "page_limit", "page_limit", error="page_limit: scan is incomplete")

    def retry_failed(self, *, include_unavailable=True, video_ids=None):
        identifiers = None if video_ids is None else list(dict.fromkeys(video_ids))
        if identifiers is not None and any(not isinstance(v, str) or not VIDEO_ID.fullmatch(v) for v in identifiers):
            raise ValueError("Invalid retry video ID")
        if identifiers == []:
            return 0
        where = "" if identifiers is None else " AND video_id IN (" + ",".join("?" for _ in identifiers) + ")"
        with self.transaction():
            rows = self.conn.execute("""SELECT video_id,generation,pages FROM jobs WHERE state='failed'
                AND (? OR last_error IS NULL OR last_error!='unavailable')""" + where,
                [include_unavailable, *(identifiers or [])]).fetchall()
            for row in rows:
                generation = row["generation"] + 1
                self.conn.execute("""UPDATE jobs SET state='ready',scan_id=?,generation=?,finished_at=NULL,
                    failures=0,local_failures=0,last_error=NULL,token_restarts=0,
                    phase=CASE WHEN pages=0 THEN 'initial' ELSE phase END,
                    token=CASE WHEN pages=0 THEN NULL ELSE token END WHERE video_id=?""",
                    (scan_identity(self.run_id, row["video_id"], generation), generation, row["video_id"]))
            self.set("state", "ready")
        return len(rows)


def export_scans(folder, *, max_scans=50, max_comments=5000):
    """Publish whole completed scans. A receipt is the file's commit marker."""
    folder = Path(folder)
    outbox = folder / "outbox" / "scans"
    with connect_queue(folder / "queue.sqlite3") as conn:
        after, run_id = setting(conn, "scans_exported_seq"), setting(conn, "run_id")
        published = list(outbox.glob(f"scans-{after + 1:012d}-*.jsonl.gz.json"))
        if published:
            if len(published) != 1:
                raise ValueError("Conflicting scan batch boundaries")
            info = json.loads(published[0].read_text())
            if info["run_id"] != run_id or info["first_seq"] != after + 1 or digest(outbox / info["name"]) != info["sha256"]:
                raise ValueError("Published scan batch changed")
            conn.execute("UPDATE settings SET value=? WHERE key='scans_exported_seq'", (json.dumps(info["last_seq"]),))
            return info["scans"]
        selected, count = [], 0
        for row in conn.execute("SELECT * FROM completions WHERE seq>? ORDER BY seq LIMIT ?", (after, max_scans)):
            header = json.loads(row["payload"])
            if selected and count + header["comment_count"] > max_comments:
                break
            selected.append((row["seq"], header))
            count += header["comment_count"]
        if not selected:
            return 0
        first, last = selected[0][0], selected[-1][0]
        name = f"scans-{first:012d}-{last:012d}.jsonl.gz"
        path, temporary = outbox / name, outbox / (name + ".tmp")
        temporary.unlink(missing_ok=True)
        with gzip_writer(temporary) as write:
            for seq, header in selected:
                write({"type": "scan", "seq": seq, "scan": header})
                written = 0
                if header["complete"]:
                    for row in conn.execute("SELECT * FROM comments WHERE video_id=? ORDER BY comment_id", (header["video_id"],)):
                        record = dict(row)
                        record["is_pinned"] = bool(record["is_pinned"]) if record["is_pinned"] is not None else None
                        write({"type": "comment", "data": record})
                        written += 1
                if written != header["comment_count"]:
                    raise ValueError("Completed scan comment count changed")
                write({"type": "end", "scan_id": header["scan_id"], "comments": written})
        if path.exists():
            if digest(path) != digest(temporary):
                raise ValueError("Unacknowledged scan file has different contents")
            temporary.unlink()
        else:
            temporary.replace(path)
        atomic_json(path.with_suffix(path.suffix + ".json"), {"version": 1, "run_id": run_id, "name": name,
            "first_seq": first, "last_seq": last, "scans": len(selected), "comments": count,
            "sha256": digest(path), "bytes": path.stat().st_size})
        conn.execute("UPDATE settings SET value=? WHERE key='scans_exported_seq'", (json.dumps(last),))
        return len(selected)


def status(folder):
    with connect_queue(Path(folder) / "queue.sqlite3") as conn:
        conn.execute("BEGIN")
        jobs = dict(conn.execute("SELECT state,count(*) FROM jobs GROUP BY state"))
        sums = conn.execute("""SELECT coalesce(sum(pages),0),coalesce(sum(token_restarts),0),
            coalesce(sum(json_extract(metrics,'$.attempts')),0),
            coalesce(sum(json_extract(metrics,'$.request_body_bytes')),0),
            coalesce(sum(json_extract(metrics,'$.response_body_bytes')),0),
            coalesce(sum(json_extract(metrics,'$.decoded_body_bytes')),0) FROM jobs""").fetchone()
        metrics = dict(zip(METRICS, sums[2:]))
        return {"run_id": setting(conn, "run_id"), "state": setting(conn, "state"), "updated_at": utcnow(),
            "pid": os.getpid(), "started_at": setting(conn, "started_at"), "active_seconds": setting(conn, "active_seconds"),
            "jobs": jobs, "pages": sums[0],
            "buffered_comments": conn.execute("SELECT count(*) FROM comments").fetchone()[0],
            "recoveries": setting(conn, "recoveries"), "token_restarts": sums[1],
            "events": conn.execute("SELECT coalesce(max(seq),0) FROM events").fetchone()[0],
            "completions": conn.execute("SELECT coalesce(max(seq),0) FROM completions").fetchone()[0],
            "exported_seq": setting(conn, "exported_seq"), "scans_exported_seq": setting(conn, "scans_exported_seq"),
            "metrics": metrics}


def build_event(job, proxy, metrics, observations, parsed, error, seconds):
    observed = observations[0] if observations else None
    valid = parsed.get("valid_data") if parsed else False
    if observed is None:
        category = "cancelled" if error and error.status == "interrupted" else "local_error"
        outcome = CollectionOutcome(category, None)
    elif error:
        category = "connection_error" if observed.connection_error or observed.http_status is None else (
            "http_error" if observed.http_status != 200 else "response_error")
        outcome = CollectionOutcome(category, False)
        observed = replace(observed, data_received=False,
                           website_error=observed.website_error or "comments:" + error.status)
    else:
        outcome = CollectionOutcome("data" if valid else "video_error" if parsed.get("terminal") == "unavailable" else "response_pending",
                                    valid, "VIDEO_UNAVAILABLE" if parsed.get("terminal") == "unavailable" else None)
        observed = replace(observed, data_received=valid,
                           website_error="video:unavailable" if outcome.video_error else None)
    event = {"collector": "comments", "video_id": job["video_id"], "scan_id": job["scan_id"],
        "at": utcnow(), "phase": job["phase"], "category": outcome.category,
        "status": error.status if error else parsed.get("terminal") or "ok", "error": str(error) if error else None,
        "proxy": {"id": proxy.proxy_id, "key": proxy.connection_key.hex(), "protocol": proxy.working_protocol},
        "observation": asdict(observed) if observed else None, "metrics": metrics, "seconds": seconds}
    return event, outcome


def commit_checkpoints(queue, batch, claim_count=0):
    outcomes = []
    with queue.transaction():
        for job, parsed, error, event, outcome, future in batch:
            try:
                queue.finish(job, parsed, error, event, outcome)
            except CommentError as exc:
                category = "response_error" if event["observation"] is not None else "local_error"
                event = {**event, "category": category, "status": exc.status, "error": str(exc)}
                if event["observation"] is not None:
                    event["observation"] = {**event["observation"], "data_received": False,
                                            "website_error": "comments:" + exc.status}
                outcome = CollectionOutcome(category, False if category == "response_error" else None)
                queue.finish(job, None, exc, event, outcome)
            outcomes.append(outcome)
        claimed = queue.claim_many(claim_count) if claim_count else []
    return outcomes, claimed


async def save_checkpoints(queue, pending, *, batch_size=64, interval=0.01, executor=None, claimed_jobs=None):
    """Acknowledge pages only after their shared durable transaction commits."""
    while True:
        first = await pending.get()
        if first is None:
            return
        await asyncio.sleep(interval)
        batch, closing = [first], False
        while len(batch) < batch_size:
            try:
                entry = pending.get_nowait()
            except asyncio.QueueEmpty:
                break
            if entry is None:
                closing = True
                break
            batch.append(entry)
        try:
            claim_count = min(64, len(batch)) if claimed_jobs is not None else 0
            outcomes, claimed = (await asyncio.get_running_loop().run_in_executor(
                executor, commit_checkpoints, queue, batch, claim_count)
                if executor is not None else commit_checkpoints(queue, batch, claim_count))
        except BaseException as exc:
            for *_, future in batch:
                if not future.done():
                    future.set_exception(exc)
            raise
        if claimed_jobs is not None:
            # These leases and the completed pages became durable together.
            claimed_jobs.extend(claimed)
        for entry, outcome in zip(batch, outcomes):
            if not entry[-1].done():
                entry[-1].set_result(outcome)
        if closing:
            return


async def run(folder, *, connect_timeout=5, stop_after_pages=0):
    # One thread owns all SQLite work. A slow disk checkpoint must not block
    # network responses or make healthy requests hit their timeout.
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="comments-queue") as executor:
        return await run_with_storage(folder, executor, connect_timeout=connect_timeout,
                                      stop_after_pages=stop_after_pages)


async def run_with_storage(folder, executor, *, connect_timeout=5, stop_after_pages=0):
    folder = Path(folder).resolve()
    lock = (folder / "controller.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    loop = asyncio.get_running_loop()
    async def storage(function, *args, **kwargs):
        return await loop.run_in_executor(executor, partial(function, *args, **kwargs))
    queue = await storage(Queue, folder)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    started, prior_seconds = time.monotonic(), await storage(setting, queue.conn, "active_seconds")
    await storage(queue.recover)
    await storage(queue.set, "state", "running")
    if await storage(setting, queue.conn, "started_at") is None:
        await storage(queue.set, "started_at", utcnow())
    control = json.loads((folder / "control.json").read_text())
    control["stop"] = False
    atomic_json(folder / "control.json", control)
    proxies = list(load_proxies(folder))
    usage = await storage(lambda: {r["proxy_id"]: dict(r) for r in queue.conn.execute("SELECT * FROM proxy_usage")})
    performance = await storage(lambda: {r["proxy_id"]: dict(r) for r in queue.conn.execute("SELECT * FROM proxy_performance")})
    pool = ProxyPool(proxies, usage, performance)
    pool.recovery = bool(usage)
    timeout = await storage(setting, queue.conn, "timeout")
    tasks, stopped_for_test = [], False
    claimed_jobs, pending = deque(), asyncio.Queue()
    writer = asyncio.create_task(save_checkpoints(queue, pending, executor=executor, claimed_jobs=claimed_jobs))
    claim_lock, next_claim_at = asyncio.Lock(), 0

    async def worker(number, clients):
        nonlocal next_claim_at
        while not stop.is_set():
            if number >= control["concurrency"]:
                await asyncio.sleep(0.1)
                continue
            async with claim_lock:
                if not claimed_jobs and time.monotonic() >= next_claim_at:
                    claimed_jobs.extend(await storage(queue.claim_many, min(64, control["concurrency"])))
                    # Share an empty result across idle workers for 100 ms.
                    next_claim_at = 0 if claimed_jobs else time.monotonic() + 0.1
                job = claimed_jobs.popleft() if claimed_jobs else None
            if job is None:
                await asyncio.sleep(0.1)
                continue
            index = None
            metrics = {name: 0 for name in METRICS}
            observations, parsed, error = [], None, None
            handed_off = False
            outcome = CollectionOutcome("cancelled", None)
            request_started = time.monotonic()
            try:
                index = await pool.acquire(job["last_proxy_id"] if job["failures"] else None)
                proxy = proxies[index]
                request_started = time.monotonic()
                body = ({"videoId": job["video_id"]} if job["phase"] == "watch" else
                        {"continuation": initial_continuation(job["video_id"]) if job["phase"] == "initial" else job["token"]})
                try:
                    payload = await fetch_json(clients[index], body, metrics, client_version=CLIENT_VERSION,
                        retries=0, timeout=timeout, watch=job["phase"] == "watch", on_attempt=observations.append)
                    parsed = parse_step(job["phase"], payload, job["video_id"])
                except CommentError as exc:
                    error = exc
                event, outcome = build_event(job, proxy, metrics, observations, parsed, error, time.monotonic() - request_started)
                saved = loop.create_future()
                pending.put_nowait((job, parsed, error, event, outcome, saved))
                handed_off = True
                outcome = await asyncio.shield(saved)
            except asyncio.CancelledError:
                if index is not None and not handed_off:
                    error = CommentError("interrupted", "Comment request interrupted")
                    event, outcome = build_event(job, proxies[index], metrics, [], None, error, time.monotonic() - request_started)
                    pending.put_nowait((job, None, error, event, outcome, loop.create_future()))
                raise
            finally:
                if index is not None:
                    pool.release(index, outcome=outcome, seconds=time.monotonic() - request_started)

    try:
        # Each proxy has one active request. HTTP/1.1 keeps connection reuse and
        # avoids httpcore's HTTP/2 semaphore cleanup failure after timeouts.
        async with CatalogClients(proxies, 1024, connect_timeout=connect_timeout,
                                  request_timeout=timeout, per_proxy_connections=1, keepalive_expiry=120,
                                  http2=False) as clients:
            last_report = last_export = 0
            while not stop.is_set():
                control = json.loads((folder / "control.json").read_text())
                if not 1 <= control["concurrency"] <= 1024:
                    raise ValueError("Concurrency must be between 1 and 1024")
                while len(tasks) < control["concurrency"]:
                    tasks.append(asyncio.create_task(worker(len(tasks), clients)))
                for task in tasks:
                    if task.done():
                        task.result()
                if writer.done():
                    writer.result()
                if time.monotonic() - last_export >= 2:
                    await storage(export_events, folder, size=2000)
                    await storage(export_scans, folder, max_scans=500)
                    last_export = time.monotonic()
                if time.monotonic() - last_report >= 5:
                    await storage(queue.set, "active_seconds", prior_seconds + time.monotonic() - started)
                    current = await storage(status, folder)
                    current["bridge_pid"] = clients.process.pid
                    await storage(atomic_json, folder / "status.json", current)
                    print(json.dumps(current), flush=True)
                    last_report = time.monotonic()
                    if stop_after_pages and current["pages"] >= stop_after_pages:
                        stopped_for_test = True
                        stop.set()
                if control.get("stop") or not await storage(lambda: queue.conn.execute(
                        "SELECT 1 FROM jobs WHERE state IN ('ready','leased') LIMIT 1").fetchone()):
                    stop.set()
                await asyncio.sleep(0.2)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        pending.put_nowait(None)
        writer_result = (await asyncio.gather(writer, return_exceptions=True))[0]
        await storage(queue.recover)
        while await storage(export_events, folder, size=2000):
            pass
        while await storage(export_scans, folder, max_scans=500):
            pass
        unfinished = await storage(lambda: queue.conn.execute("SELECT count(*) FROM jobs WHERE state IN ('ready','leased')").fetchone()[0])
        await storage(queue.set, "state", "paused" if unfinished else "complete")
        await storage(queue.set, "active_seconds", prior_seconds + time.monotonic() - started)
        final = await storage(status, folder)
        final["stopped_for_test"] = stopped_for_test
        await storage(atomic_json, folder / "status.json", final)
        print(json.dumps(final), flush=True)
        await storage(queue.close)
        lock.close()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
    if isinstance(writer_result, BaseException):
        raise writer_result
    return final


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    export = sub.add_parser("export")
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--ranked", type=Path, required=True)
    export.add_argument("--limit", type=int, default=1000, help="0 selects all videos")
    export.add_argument("--include", action="append", default=[])
    export.add_argument("--from-run", type=Path)
    export.add_argument("--proxy-limit", type=int, default=512)
    init = sub.add_parser("init")
    init.add_argument("--run", type=Path, required=True)
    init.add_argument("--concurrency", type=int, default=64)
    init.add_argument("--max-attempts", type=int, default=6)
    init.add_argument("--max-pages", type=int, default=10000)
    init.add_argument("--timeout", type=int, default=20)
    execute = sub.add_parser("run")
    execute.add_argument("--run", type=Path, required=True)
    execute.add_argument("--stop-after-pages", type=int, default=0, help="Pause after this many pages for recovery verification")
    execute.add_argument("--connect-timeout", type=int, default=5)
    for name in ("status", "retry"):
        command = sub.add_parser(name)
        command.add_argument("--run", type=Path, required=True)
        if name == "retry":
            command.add_argument("--recoverable-only", action="store_true",
                                 help="Skip videos confirmed unavailable on multiple routes")
            command.add_argument("--video-id", action="append", help="Retry only this video; repeat for multiple IDs")
    args = parser.parse_args()
    if args.command == "export":
        result = snapshot(args.output, args.ranked, limit=args.limit, include=args.include,
                          from_run=args.from_run, proxy_limit=args.proxy_limit)
    elif args.command == "init":
        result = initialize(args.run, concurrency=args.concurrency, max_attempts=args.max_attempts,
                            max_pages=args.max_pages, timeout=args.timeout)
    elif args.command == "run":
        if args.connect_timeout < 1 or args.stop_after_pages < 0:
            parser.error("Invalid run limits")
        result = asyncio.run(run(args.run, connect_timeout=args.connect_timeout, stop_after_pages=args.stop_after_pages))
    elif args.command == "retry":
        with (args.run / "controller.lock").open("w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            queue = Queue(args.run)
            try:
                result = {"requeued": queue.retry_failed(include_unavailable=not args.recoverable_only,
                                                        video_ids=args.video_id)}
            finally:
                queue.close()
    else:
        result = status(args.run)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
