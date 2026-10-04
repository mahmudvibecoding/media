"""Independently compare completed comment queues, import receipts, and PostgreSQL."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3

from comments_bulk import FIELDS, gzip_writer
from discover_videos import open_database
from metadata_bulk import atomic_json, digest, setting, utcnow


EMPTY_HASH = hashlib.sha256(b"").hexdigest()


def row_digest(record):
    """Match PostgreSQL jsonb_build_array(... )::text, including array spacing."""
    value = [record[field] for field in FIELDS[1:]]
    return hashlib.sha256(json.dumps(value, ensure_ascii=False).encode()).hexdigest().encode()


def export_shard(arguments):
    folder, output = map(Path, arguments)
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (folder / "controller.lock").open() as lock:
        fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        conn = sqlite3.connect("file:" + str(folder / "queue.sqlite3") + "?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        try:
            if setting(conn, "state") != "complete":
                raise ValueError("Collection must finish before its final audit")
            if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Queue integrity check failed")
            stamp = {"run_id": setting(conn, "run_id"),
                "events": conn.execute("SELECT coalesce(max(seq),0) FROM events").fetchone()[0],
                "completions": conn.execute("SELECT coalesce(max(seq),0) FROM completions").fetchone()[0]}
            if stamp["events"] != setting(conn, "exported_seq") or stamp["completions"] != setting(conn, "scans_exported_seq"):
                raise ValueError("Unpublished queue records remain")
            receipt = output / "receipt.json"
            if receipt.exists():
                previous = json.loads(receipt.read_text())
                if any(previous.get(key) != value for key, value in stamp.items()):
                    raise ValueError("Queue changed since the existing audit export")
                if any(digest(output / name) != info["sha256"] for name, info in previous["files"].items()):
                    raise ValueError("Audit export checksum mismatch")
                return previous
            counts = Counter()
            for name in ("videos.jsonl.gz.tmp", "revisited.jsonl.gz.tmp"):
                (output / name).unlink(missing_ok=True)
            history = {}
            for row in conn.execute("SELECT video_id,comment_id FROM history WHERE scope=0"):
                history.setdefault(row[0], set()).add(row[1])
            comments = iter(conn.execute("SELECT * FROM comments ORDER BY video_id,comment_id"))
            current = next(comments, None)
            files = {}
            with gzip_writer(output / "videos.jsonl.gz.tmp") as write_video, gzip_writer(output / "revisited.jsonl.gz.tmp") as write_revisit:
                for job in conn.execute("SELECT * FROM jobs ORDER BY video_id"):
                    if job["state"] not in ("completed", "failed"):
                        raise ValueError("Unfinished job in completed queue")
                    video_id = job["video_id"]
                    hashed, visited, new = hashlib.sha256(), 0, 0
                    old = history.get(video_id, set())
                    while current is not None and current["video_id"] == video_id:
                        if job["state"] == "completed":
                            record = dict(current)
                            record["is_pinned"] = bool(record["is_pinned"]) if record["is_pinned"] is not None else None
                            hashed.update(row_digest(record))
                            visited += 1
                            new += record["comment_id"] not in old
                            if job["baseline_at"] is not None:
                                write_revisit(record)
                                counts.update(revisited_rows=1)
                        current = next(comments, None)
                    if current is not None and current["video_id"] < video_id:
                        raise ValueError("Unowned queue comment")
                    expected = len(old) + new if job["state"] == "completed" else len(old)
                    full_hash = hashed.hexdigest() if job["state"] == "completed" and job["baseline_at"] is None else None
                    write_video({"video_id": video_id, "state": job["state"], "generation": job["generation"],
                        "baseline_at": job["baseline_at"], "baseline_error": job["baseline_error"], "error": job["last_error"],
                        "expected_count": expected, "visited_count": visited, "content_sha256": full_hash})
                    counts.update(videos=1, **{job["state"]: 1}, expected_comments=expected, visited_comments=visited)
            if current is not None:
                raise ValueError("Comments remain outside selected videos")
            for name in ("videos.jsonl.gz", "revisited.jsonl.gz"):
                temporary, path = output / (name + ".tmp"), output / name
                temporary.replace(path)
                files[name] = {"sha256": digest(path), "bytes": path.stat().st_size}
            result = {"version": 1, "created_at": utcnow(), **stamp, "counts": dict(counts), "files": files,
                "content_hash": "sha256(concatenated hex sha256 of JSON field arrays in comment_id binary order)"}
            atomic_json(receipt, result)
            return result
        finally:
            conn.close()


def export_fleet(folder, output, workers=16):
    folder, output = Path(folder).resolve(), Path(output).resolve()
    manifest = json.loads((folder / "fleet.json").read_text())
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    with ProcessPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(export_shard, [(folder / s["name"], output / s["name"]) for s in manifest["shards"]]))
    totals = sum((Counter(result["counts"]) for result in results), Counter())
    if totals["videos"] != manifest["totals"]["videos"]:
        raise ValueError("Fleet audit does not cover the frozen selection")
    result = {"parent_run_id": manifest["parent_run_id"], "created_at": utcnow(), "counts": dict(totals),
              "shards": [{"name": s["name"], "receipt_sha256": digest(output / s["name"] / "receipt.json")}
                         for s in manifest["shards"]]}
    atomic_json(output / "manifest.json", result)
    return result


def verify_files(folder):
    folder = Path(folder)
    receipt = json.loads((folder / "receipt.json").read_text())
    for name, info in receipt["files"].items():
        if name not in ("videos.jsonl.gz", "revisited.jsonl.gz") or digest(folder / name) != info["sha256"]:
            raise ValueError("Audit artifact checksum mismatch")
    return receipt


def validate(folder, expected, output):
    folder, expected, output = Path(folder), Path(expected), Path(output)
    fleet = json.loads((folder / "fleet.json").read_text())
    exported = json.loads((expected / "manifest.json").read_text())
    if fleet["parent_run_id"] != exported["parent_run_id"]:
        raise ValueError("Audit belongs to another fleet")
    receipts, journals, locks = {}, [], []
    totals, outcomes = Counter(), Counter()
    try:
        for shard in fleet["shards"]:
            name, source = shard["name"], folder / shard["name"]
            expected_entry = next(s for s in exported["shards"] if s["name"] == name)
            if digest(expected / name / "receipt.json") != expected_entry["receipt_sha256"]:
                raise ValueError("Audit receipt changed")
            receipts[name] = receipt = verify_files(expected / name)
            if receipt["run_id"] != shard["run_id"]:
                raise ValueError("Audit partition identity mismatch")
            lock = (source / "import.lock").open()
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
            locks.append(lock)
            journal = sqlite3.connect("file:" + str(source / "import.sqlite3") + "?mode=ro", uri=True)
            journal.row_factory = sqlite3.Row
            journals.append((name, journal))
            if journal.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise ValueError("Import journal integrity check failed")
            cursors = dict(journal.execute("SELECT kind,max(last_seq) FROM batches GROUP BY kind"))
            if cursors.get("scans", 0) != receipt["completions"] or cursors.get("events", 0) != receipt["events"]:
                raise ValueError("Imports have not caught up with collection")
            if journal.execute("SELECT count(*) FROM imports WHERE state!='applied'").fetchone()[0]:
                raise ValueError("Unacknowledged imports remain")
            for row in journal.execute("SELECT outcome,count(*) FROM imports GROUP BY outcome"):
                outcomes[row[0]] += row[1]
            row = journal.execute("SELECT coalesce(sum(inserted),0),coalesce(sum(refreshed),0) FROM imports").fetchone()
            totals.update(inserted=row[0], refreshed=row[1])
        with open_database() as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            conn.execute("SET LOCAL work_mem='128MB'")
            conn.execute("""CREATE TEMP TABLE audit_expected(video_id TEXT PRIMARY KEY,state TEXT,generation INTEGER,
                baseline_at TIMESTAMPTZ,error TEXT,expected_count BIGINT,content_sha256 TEXT) ON COMMIT DROP""")
            conn.execute("""CREATE TEMP TABLE audit_receipts(video_id TEXT PRIMARY KEY,generation INTEGER,
                imported_at TIMESTAMPTZ,outcome TEXT,recorded_error TEXT) ON COMMIT DROP""")
            conn.execute("""CREATE TEMP TABLE audit_revisited(video_id TEXT,comment_id TEXT,text TEXT,
                author_channel_id TEXT,author_name TEXT,is_pinned BOOLEAN,PRIMARY KEY(video_id,comment_id)) ON COMMIT DROP""")
            conn.execute("CREATE TEMP TABLE audit_history(video_id TEXT,comment_id TEXT,PRIMARY KEY(video_id,comment_id)) ON COMMIT DROP")
            with conn.cursor().copy("COPY audit_expected FROM STDIN") as copy:
                for shard in fleet["shards"]:
                    with gzip.open(expected / shard["name"] / "videos.jsonl.gz", "rt") as source:
                        for line in source:
                            row = json.loads(line)
                            copy.write_row(tuple(row[k] for k in ("video_id", "state", "generation", "baseline_at", "error", "expected_count", "content_sha256")))
            with conn.cursor().copy("COPY audit_receipts FROM STDIN") as copy:
                for _, journal in journals:
                    for row in journal.execute("""SELECT i.video_id,i.generation,i.imported_at,i.outcome,i.recorded_error
                        FROM imports i JOIN (SELECT video_id,max(generation) AS generation FROM imports GROUP BY video_id) latest
                        USING(video_id,generation)"""):
                        copy.write_row(tuple(row))
            with conn.cursor().copy("COPY audit_revisited FROM STDIN") as copy:
                for shard in fleet["shards"]:
                    with gzip.open(expected / shard["name"] / "revisited.jsonl.gz", "rt") as source:
                        for line in source:
                            row = json.loads(line)
                            copy.write_row(tuple(row[k] for k in FIELDS))
            with conn.cursor().copy("COPY audit_history FROM STDIN") as copy:
                for shard in fleet["shards"]:
                    with gzip.open(folder / shard["name"] / "history.jsonl.gz", "rt") as source:
                        for line in source:
                            copy.write_row(tuple(json.loads(line)[:2]))
            for table in ("audit_expected", "audit_receipts", "audit_revisited", "audit_history"):
                conn.execute("ANALYZE " + table)
            print(json.dumps({"phase": "snapshots_loaded", "at": utcnow()}), flush=True)
            checks = {}
            checks["selected"] = conn.execute("SELECT count(*) FROM audit_expected").fetchone()[0]
            checks["receipt_coverage_mismatches"] = conn.execute("""SELECT count(*) FROM audit_expected e
                FULL JOIN audit_receipts r USING(video_id) WHERE e.video_id IS NULL OR r.video_id IS NULL OR e.generation<>r.generation""").fetchone()[0]
            checks["video_state_mismatches"] = conn.execute("""SELECT count(*) FROM audit_expected e
                JOIN audit_receipts r USING(video_id) LEFT JOIN public.videos v USING(video_id)
                WHERE v.video_id IS NULL OR (e.state='completed' AND (r.outcome<>'success'
                    OR v.comments_updated_at IS DISTINCT FROM r.imported_at OR v.comments_error IS NOT NULL))
                OR (e.state='failed' AND (r.outcome<>'error' OR v.comments_updated_at IS DISTINCT FROM e.baseline_at
                    OR v.comments_error IS DISTINCT FROM r.recorded_error OR r.recorded_error IS DISTINCT FROM e.error))""").fetchone()[0]
            conn.execute("""CREATE TEMP TABLE audit_actual ON COMMIT DROP AS
                SELECT c.video_id,count(*) AS comment_count,
                    encode(sha256(convert_to(string_agg(encode(sha256(convert_to(
                        jsonb_build_array(c.comment_id,c.text,c.author_channel_id,c.author_name,c.is_pinned)::text,
                        'UTF8')),'hex'),'' ORDER BY c.comment_id COLLATE "C"),'UTF8')),'hex') AS content_sha256
                FROM public.comments c JOIN audit_expected e USING(video_id) GROUP BY c.video_id""")
            conn.execute("CREATE UNIQUE INDEX audit_actual_video ON audit_actual(video_id)")
            conn.execute("ANALYZE audit_actual")
            checks["comment_count_mismatches"] = conn.execute("""SELECT count(*) FROM audit_expected e
                LEFT JOIN audit_actual a USING(video_id) WHERE e.expected_count<>coalesce(a.comment_count,0)""").fetchone()[0]
            checks["content_hash_mismatches"] = conn.execute("""SELECT count(*) FROM audit_expected e
                LEFT JOIN audit_actual a USING(video_id) WHERE e.content_sha256 IS NOT NULL
                AND e.content_sha256<>coalesce(a.content_sha256,%s)""", (EMPTY_HASH,)).fetchone()[0]
            checks["revisited_field_mismatches"] = conn.execute("""SELECT count(*) FROM audit_revisited r
                LEFT JOIN public.comments c USING(video_id,comment_id) WHERE c.comment_id IS NULL OR
                (c.text,c.author_channel_id,c.author_name,c.is_pinned) IS DISTINCT FROM
                (r.text,r.author_channel_id,r.author_name,r.is_pinned)""").fetchone()[0]
            checks["missing_previous_comments"] = conn.execute("""SELECT count(*) FROM audit_history h
                LEFT JOIN public.comments c USING(video_id,comment_id) WHERE c.comment_id IS NULL""").fetchone()[0]
            checks["comments"] = int(conn.execute("SELECT coalesce(sum(comment_count),0) FROM audit_actual").fetchone()[0])
            checks["comment_table_bytes"] = conn.execute("SELECT pg_total_relation_size('public.comments')").fetchone()[0]
            checks["media_database_bytes"] = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
            checks["comment_columns"] = [r[0] for r in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='comments' ORDER BY ordinal_position")]
            mismatches = {key: value for key, value in checks.items() if key.endswith("mismatches") or key == "missing_previous_comments"}
            passed = (not any(mismatches.values()) and checks["selected"] == fleet["totals"]["videos"]
                and checks["comment_columns"] == list(FIELDS)
                and checks["comments"] == fleet["totals"]["history_comments"] + totals["inserted"])
            result = {"parent_run_id": fleet["parent_run_id"], "audited_at": utcnow(), "passed": passed,
                "source_counts": exported["counts"], "import_outcomes": dict(outcomes), "import_totals": dict(totals), "checks": checks}
            output.parent.mkdir(parents=True, exist_ok=True)
            atomic_json(output, result)
            return result
    finally:
        for _, journal in journals:
            journal.close()
        for lock in locks:
            lock.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("export", "validate"):
        command = commands.add_parser(name)
        command.add_argument("--fleet", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        if name == "export":
            command.add_argument("--workers", type=int, default=16)
        else:
            command.add_argument("--expected", type=Path, required=True)
    args = parser.parse_args()
    result = export_fleet(args.fleet, args.output, args.workers) if args.command == "export" else validate(args.fleet, args.expected, args.output)
    print(json.dumps(result), flush=True)
    if result.get("passed") is False:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
