"""Compress finished journals for transfer, preserving originals and recording hashes."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/opt/media-proxy-tester"))
    parser.add_argument("--run", default="20261001")
    args = parser.parse_args()
    os.umask(0o077)
    folder = args.root / "runs" / args.run
    state = json.loads((folder / "controller.json").read_text())
    phases = ["bench-" + str(r["concurrency"]) for r in state.get("benchmarks", [])]
    phases += ["large-bench-" + str(r["concurrency"]) for r in state.get("large_benchmarks", [])]
    if "tuning" in state["completed_phases"]:
        phases.append("timeout-comparison")
    phases += [p for p in ("full", "recovery", "stability") if p in state["completed_phases"]]
    for phase in phases:
        journal = folder / phase / "results.jsonl"
        destination = Path(str(journal) + ".gz")
        sealed = Path(str(journal) + ".sealed.json")
        if destination.exists() and sealed.exists():
            continue
        before = journal.stat()
        checksum, lines = hashlib.sha256(), 0
        temporary = Path(str(destination) + ".tmp")
        with journal.open("rb") as source, temporary.open("wb") as raw:
            with gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=1, mtime=0) as out:
                for line in source:
                    if not line.endswith(b"\n"):
                        raise ValueError("Finished journal has an incomplete line")
                    checksum.update(line)
                    lines += 1
                    out.write(line)
            raw.flush()
            os.fsync(raw.fileno())
        after = journal.stat()
        if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
            raise ValueError("Journal changed during compression")
        temporary.replace(destination)
        with destination.open("rb") as compressed:
            compressed_hash = hashlib.file_digest(compressed, "sha256").hexdigest()
        evidence = {"phase": phase, "lines": lines, "uncompressed_bytes": before.st_size,
                    "uncompressed_sha256": checksum.hexdigest(), "compressed_bytes": destination.stat().st_size,
                    "compressed_sha256": compressed_hash}
        sealed_temporary = Path(str(sealed) + ".tmp")
        with sealed_temporary.open("w") as saved:
            saved.write(json.dumps(evidence, indent=2) + "\n")
            saved.flush()
            os.fsync(saved.fileno())
        sealed_temporary.replace(sealed)
        print(json.dumps(evidence), flush=True)


if __name__ == "__main__":
    main()
