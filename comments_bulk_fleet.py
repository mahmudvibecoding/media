"""Partition a frozen comment run and supervise independent collectors/importers."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import ExitStack
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

from comments_bulk import gzip_writer
from metadata_bulk import atomic_json, digest, utcnow


ROOT = Path(__file__).resolve().parent


def partition_index(video_id, count):
    return int.from_bytes(hashlib.sha256(video_id.encode()).digest()[:8], "big") % count


def partition(source, destination, *, shards=16, concurrency=512):
    source, destination = Path(source), Path(destination)
    manifest = json.loads((source / "manifest.json").read_text())
    if manifest.get("collector") != "comments" or not 1 <= shards <= 64 or not 1 <= concurrency <= 1024:
        raise ValueError("Invalid fleet configuration")
    if manifest["proxies"] < shards:
        raise ValueError("Every shard needs at least one proxy")
    for name in ("videos.jsonl.gz", "history.jsonl.gz", "proxies.jsonl.gz"):
        if digest(source / name) != manifest["files"][name]["sha256"]:
            raise ValueError("Source snapshot checksum mismatch")
    destination.mkdir(parents=True, exist_ok=False, mode=0o700)
    folders = [destination / f"shard-{i:02d}" for i in range(shards)]
    counts, protocols = [Counter() for _ in folders], [Counter() for _ in folders]
    for folder in folders:
        folder.mkdir(mode=0o700)
    for name in ("videos.jsonl.gz", "history.jsonl.gz", "proxies.jsonl.gz"):
        with ExitStack() as stack:
            writers = [stack.enter_context(gzip_writer(folder / name)) for folder in folders]
            with gzip.open(source / name, "rt") as rows:
                for sequence, line in enumerate(rows):
                    row = json.loads(line)
                    if name == "videos.jsonl.gz":
                        index = partition_index(row["video_id"], shards)
                        counts[index].update(videos=1, with_history=int(row["baseline_at"] is not None), **{row["kind"]: 1})
                    elif name == "history.jsonl.gz":
                        index = partition_index(row[0], shards)
                        counts[index].update(history_comments=1)
                    else:
                        # Interleave ranks so all workers receive fast and slow routes.
                        index = sequence % shards
                        counts[index].update(proxies=1)
                        protocols[index].update([row["working_protocol"]])
                    writers[index](row)
    totals = sum(counts, Counter())
    for key in ("videos", "history_comments", "proxies"):
        if totals[key] != manifest.get(key, 0):
            raise ValueError("Partition count differs from frozen source")
    entries = []
    for index, folder in enumerate(folders):
        shard_counts = {key: counts[index][key] for key in
                        ("videos", "video", "short", "with_history", "history_comments", "proxies")}
        child = {**manifest, **shard_counts, "selection": "partition",
            "run_id": str(uuid.uuid5(uuid.UUID(manifest["run_id"]), f"shard:{index}:{shards}")),
            "parent_run_id": manifest["run_id"], "partition_index": index, "partition_count": shards,
            "protocols": dict(protocols[index]), "files": {}}
        for name in ("videos.jsonl.gz", "history.jsonl.gz", "proxies.jsonl.gz"):
            path = folder / name
            path.chmod(0o600)
            child["files"][name] = {"sha256": digest(path), "bytes": path.stat().st_size}
        atomic_json(folder / "manifest.json", child)
        entries.append({"name": folder.name, "run_id": child["run_id"], **shard_counts})
    result = {"version": 1, "collector": "comments-fleet", "created_at": utcnow(),
        "parent_run_id": manifest["run_id"], "source_manifest_sha256": digest(source / "manifest.json"),
        "concurrency_per_shard": concurrency, "totals": dict(totals), "shards": entries}
    atomic_json(destination / "fleet.json", result)
    return result


def read_json(path, default=None):
    try:
        return json.loads(Path(path).read_text())
    except FileNotFoundError:
        return {} if default is None else default


def status(folder):
    folder = Path(folder)
    fleet = read_json(folder / "fleet.json")
    jobs, metrics, imports = Counter(), Counter(), Counter()
    pages = comments = events = completed_shards = imported_shards = 0
    details = []
    for shard in fleet["shards"]:
        path = folder / shard["name"]
        imported = read_json(path / "import-status.json")
        worker = read_json(path / "status.json") or read_json(path / "remote-status.json") or imported.get("worker", {})
        jobs.update(worker.get("jobs", {}))
        metrics.update(worker.get("metrics", {}))
        imports.update(imported.get("outcomes", {}))
        pages += worker.get("pages", 0)
        comments += worker.get("buffered_comments", 0)
        events += worker.get("events", 0)
        completed_shards += worker.get("state") == "complete"
        imported_shards += bool(imported.get("complete"))
        details.append({"name": shard["name"], "worker_updated_at": worker.get("updated_at"),
            "state": worker.get("state", "pending"), "jobs": worker.get("jobs", {}),
            "import_updated_at": imported.get("updated_at"), "inserted": imported.get("inserted", 0),
            "refreshed": imported.get("refreshed", 0), "scan_seq": imported.get("scan_seq", 0),
            "event_seq": imported.get("event_seq", 0), "errors": imported.get("errors", [])})
    return {"parent_run_id": fleet["parent_run_id"], "updated_at": utcnow(), "selected": fleet["totals"]["videos"],
        "jobs": dict(jobs), "pages": pages, "buffered_comments": comments, "events": events, "metrics": dict(metrics),
        "import_outcomes": dict(imports), "inserted": sum(s["inserted"] for s in details),
        "refreshed": sum(s["refreshed"] for s in details), "completed_shards": completed_shards,
        "imported_shards": imported_shards, "shards": details}


def supervise(folder, *, mode, host=None, remote=None, concurrency=None):
    folder = Path(folder).resolve()
    fleet = read_json(folder / "fleet.json")
    if mode == "import" and (not host or not remote):
        raise ValueError("Remote import requires host and remote fleet directory")
    concurrency = concurrency or fleet["concurrency_per_shard"]
    if not 1 <= concurrency <= 1024:
        raise ValueError("Invalid concurrency")
    stopped = False
    def stop(signum, frame):
        nonlocal stopped
        stopped = True
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop)
    with (folder / (mode + ".lock")).open("w") as lock, ExitStack() as stack:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        children = []
        for shard in fleet["shards"]:
            path = folder / shard["name"]
            log = stack.enter_context((path / (mode + ".log")).open("ab", buffering=0))
            if mode == "collect":
                if not (path / "queue.sqlite3").exists():
                    # Initialization is independent for each partition.
                    command = [sys.executable, str(ROOT / "comments_bulk.py"), "init", "--run", str(path),
                        "--concurrency", str(concurrency), "--max-pages", "1000000", "--max-attempts", "8"]
                    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                    phase = "init"
                else:
                    process = None
                    phase = "run"
                command = [sys.executable, str(ROOT / "comments_bulk.py"), "run", "--run", str(path)]
            else:
                command = [sys.executable, str(ROOT / "comments_bulk_import.py"), "--run", str(path), "--watch",
                    "--host", host, "--remote", str(Path(remote) / shard["name"]), "--interval", "10"]
                process, phase = None, "run"
            children.append({"name": shard["name"], "process": process, "command": command,
                             "phase": phase, "log": log, "restarts": 0, "done": False, "exit_code": None})
        last_report = 0
        try:
            while not stopped:
                for child in children:
                    if child["done"]:
                        continue
                    process = child["process"]
                    if process is None:
                        child["process"] = subprocess.Popen(child["command"], stdout=child["log"], stderr=subprocess.STDOUT)
                        continue
                    code = process.poll()
                    if code is None:
                        continue
                    if child["phase"] == "init" and code == 0:
                        child["phase"], child["process"] = "run", None
                    elif code != 0 and child["phase"] == "run" and child["restarts"] < 3:
                        child["restarts"] += 1
                        child["process"] = None
                    else:
                        child["done"], child["exit_code"] = True, code
                if time.monotonic() - last_report >= 10 or all(c["done"] for c in children):
                    current = status(folder)
                    current.update(mode=mode, supervisor_pid=os.getpid(), processes=[{
                        "name": c["name"], "phase": c["phase"], "pid": c["process"].pid if c["process"] else None,
                        "restarts": c["restarts"], "done": c["done"], "exit_code": c["exit_code"]} for c in children])
                    atomic_json(folder / (mode + "-status.json"), current)
                    print(json.dumps({k: v for k, v in current.items() if k not in ("shards", "processes")}), flush=True)
                    last_report = time.monotonic()
                if all(c["done"] for c in children):
                    return 1 if any(c["exit_code"] for c in children) else 0
                time.sleep(1)
        finally:
            for child in children:
                process = child["process"]
                if process is not None and process.poll() is None:
                    process.terminate()
            for child in children:
                process = child["process"]
                if process is not None:
                    process.wait()
    return 0


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    split = commands.add_parser("partition")
    split.add_argument("--source", type=Path, required=True)
    split.add_argument("--output", type=Path, required=True)
    split.add_argument("--shards", type=int, default=16)
    split.add_argument("--concurrency", type=int, default=512)
    for name in ("collect", "import", "status", "control"):
        sub = commands.add_parser(name)
        sub.add_argument("--fleet", type=Path, required=True)
        if name == "import":
            sub.add_argument("--host", required=True)
            sub.add_argument("--remote", required=True)
        if name in ("collect", "control"):
            sub.add_argument("--concurrency", type=int)
        if name == "control":
            sub.add_argument("--stop", action="store_true")
    args = parser.parse_args()
    if args.command == "partition":
        result = partition(args.source, args.output, shards=args.shards, concurrency=args.concurrency)
    elif args.command in ("collect", "import"):
        raise SystemExit(supervise(args.fleet, mode=args.command, host=getattr(args, "host", None),
            remote=getattr(args, "remote", None), concurrency=getattr(args, "concurrency", None)))
    elif args.command == "control":
        if args.concurrency is not None and not 1 <= args.concurrency <= 1024:
            raise ValueError("Invalid concurrency")
        for child in read_json(args.fleet / "fleet.json")["shards"]:
            path = args.fleet / child["name"] / "control.json"
            value = read_json(path)
            value["stop"] = args.stop
            if args.concurrency is not None:
                value["concurrency"] = args.concurrency
            atomic_json(path, value)
        result = {"updated": len(read_json(args.fleet / "fleet.json")["shards"]),
                  "concurrency": args.concurrency, "stop": args.stop}
    else:
        result = status(args.fleet)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
