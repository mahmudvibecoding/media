"""Create a reproducible sample spread uniformly across verified catalog shards."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import random


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--size", type=int, default=1000000)
    args = parser.parse_args()
    os.umask(0o077)
    manifest = json.loads(args.manifest.read_text())
    if args.output.exists():
        raise SystemExit("Output already exists; preserve existing samples")
    if not 0 < args.size <= manifest["records"]:
        parser.error("sample size must be between one and the catalog size")
    rng = random.Random(20261002)
    quotas = [args.size * shard["records"] // manifest["records"] for shard in manifest["shards"]]
    for i in range(args.size - sum(quotas)):
        quotas[i] += 1
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    count = 0
    with temporary.open("xb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=1, mtime=0) as output:
            for shard, quota in zip(manifest["shards"], quotas):
                name = shard["file"]
                if Path(name).name != name:
                    raise ValueError("Invalid shard filename")
                with (args.manifest.parent / name).open("rb") as source:
                    if hashlib.file_digest(source, "sha256").hexdigest() != shard["sha256"]:
                        raise ValueError("Shard checksum mismatch")
                    source.seek(0)
                    chosen = set(rng.sample(range(shard["records"]), quota))
                    with gzip.GzipFile(fileobj=source) as decoded:
                        records = 0
                        for index, line in enumerate(decoded):
                            records += 1
                            if index in chosen:
                                output.write(line)
                                count += 1
                    if records != shard["records"]:
                        raise ValueError("Shard count mismatch")
        raw.flush()
        os.fsync(raw.fileno())
    if count != args.size:
        raise ValueError("Sample count mismatch")
    temporary.replace(args.output)
    print(json.dumps({"records": count, "bytes": args.output.stat().st_size}))


if __name__ == "__main__":
    main()
