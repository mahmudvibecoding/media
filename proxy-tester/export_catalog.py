"""Export a consistent, resumable-input snapshot without changing the proxy database."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import time

import psycopg


ROOT = Path(__file__).resolve().parent.parent


def encoded(record):
    return (json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n").encode()


def write_sample(path, records):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=1, mtime=0) as compressed:
            for record in records:
                compressed.write(encoded(record))
        raw.flush()
        os.fsync(raw.fileno())
    temporary.replace(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shard-size", type=int, default=100_000)
    parser.add_argument("--sample-size", type=int, default=200_000)
    args = parser.parse_args()
    if args.shard_size < 1 or args.sample_size < 1:
        parser.error("sizes must be positive")
    os.umask(0o077)
    args.output.mkdir(parents=True, exist_ok=True)
    if any(args.output.iterdir()):
        raise SystemExit("Export directory must be empty; preserve existing snapshots")
    manifest = {"schema_version": 1, "created_at": datetime.now(timezone.utc).isoformat(),
                "source_database": "proxy", "shards": [], "protocol_counts": {}}
    sample, counts, rng = [], Counter(), random.Random(20261001)
    started, last_progress, total = time.monotonic(), 0.0, 0
    compressed = raw = temporary = None

    def finish_shard():
        nonlocal compressed, raw, temporary
        compressed.close()
        raw.flush()
        os.fsync(raw.fileno())
        raw.close()
        destination = temporary.with_suffix("")
        temporary.replace(destination)
        with destination.open("rb") as source:
            checksum = hashlib.file_digest(source, "sha256").hexdigest()
        manifest["shards"][-1].update(bytes=destination.stat().st_size, sha256=checksum)
        compressed = raw = temporary = None

    with psycopg.connect(dbname="proxy", user="mahmud", host=str(ROOT / ".local/postgres/socket"),
                         port=5432, connect_timeout=5,
                         options="-c default_transaction_read_only=on") as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        expected = conn.execute("SELECT count(*) FROM public.proxies").fetchone()[0]
        manifest["expected_records"] = expected
        with conn.cursor(name="proxy_snapshot") as cursor:
            cursor.itersize = 10_000
            cursor.execute("SELECT proxy_id, encode(connection_key, 'hex'), address, port, protocol, "
                           "connection_settings FROM public.proxies ORDER BY proxy_id")
            for proxy_id, key, address, port, protocol, settings in cursor:
                if compressed is None:
                    filename = f"catalog-{len(manifest['shards']):04d}.jsonl.gz"
                    temporary = args.output / (filename + ".tmp")
                    raw = temporary.open("xb")
                    compressed = gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=1, mtime=0)
                    manifest["shards"].append({"file": filename, "records": 0})
                if isinstance(settings, str):
                    try:
                        settings = json.loads(settings)
                    except (ValueError, TypeError):
                        settings = {"_invalid_settings": True}
                if not isinstance(settings, dict):
                    settings = {"_invalid_settings": True}
                record = {"id": proxy_id, "key": key, "address": address, "port": port,
                          "protocol": protocol, "settings": settings}
                compressed.write(encoded(record))
                total += 1
                counts[protocol] += 1
                manifest["shards"][-1]["records"] += 1
                if len(sample) < args.sample_size:
                    sample.append(record)
                else:
                    position = rng.randrange(total)
                    if position < len(sample):
                        sample[position] = record
                if manifest["shards"][-1]["records"] == args.shard_size:
                    finish_shard()
                if time.monotonic() - last_progress >= 10:
                    print(json.dumps({"exported": total, "expected": expected,
                                      "seconds": round(time.monotonic() - started, 1)}), flush=True)
                    last_progress = time.monotonic()
    if compressed is not None:
        finish_shard()
    if total != manifest["expected_records"]:
        raise RuntimeError("Snapshot count does not match its transaction")
    rng.shuffle(sample)
    write_sample(args.output / "sample.jsonl.gz", sample)
    manifest.update(records=total, protocol_counts=dict(counts), sample_records=len(sample),
                    seconds=round(time.monotonic() - started, 3))
    temporary_manifest = args.output / "manifest.json.tmp"
    temporary_manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    temporary_manifest.replace(args.output / "manifest.json")
    print(json.dumps({"complete": True, "records": total, "shards": len(manifest["shards"]),
                      "sample_records": len(sample), "seconds": manifest["seconds"]}), flush=True)


if __name__ == "__main__":
    main()
