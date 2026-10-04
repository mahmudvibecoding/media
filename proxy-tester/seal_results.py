"""Compress finished journals for transfer, preserving originals and recording hashes."""

import argparse
from concurrent.futures import ProcessPoolExecutor
import gzip
import hashlib
import json
import os
from pathlib import Path


def seal(journal):
    journal = Path(journal)
    destination = Path(str(journal) + '.gz')
    sealed = Path(str(journal) + '.sealed.json')
    before = journal.stat()
    if destination.exists() and sealed.exists():
        evidence = json.loads(sealed.read_text())
        if (evidence.get('uncompressed_bytes'), evidence.get('source_mtime_ns')) == (before.st_size, before.st_mtime_ns):
            return evidence
        raise ValueError('An existing sealed journal changed; preserve its original artifacts')
    summary = json.loads(Path(str(journal) + '.summary.json').read_text())
    if summary.get('state') not in ('complete', 'interrupted', 'local_overload'):
        raise ValueError('Only stopped journals can be sealed')
    checksum, lines, last = hashlib.sha256(), 0, b''
    temporary = Path(str(destination) + '.tmp')
    with journal.open('rb') as source, temporary.open('wb') as raw:
        with gzip.GzipFile(filename='', fileobj=raw, mode='wb', compresslevel=1, mtime=0) as out:
            while chunk := source.read(1 << 20):
                checksum.update(chunk)
                lines += chunk.count(b'\n')
                last = chunk[-1:]
                out.write(chunk)
        raw.flush()
        os.fsync(raw.fileno())
    if last and last != b'\n':
        raise ValueError('Finished journal has an incomplete line')
    after = journal.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise ValueError('Journal changed during compression')
    temporary.replace(destination)
    with destination.open('rb') as compressed:
        compressed_hash = hashlib.file_digest(compressed, 'sha256').hexdigest()
    evidence = {'phase':journal.parent.name, 'journal':str(journal), 'lines':lines,
                'uncompressed_bytes':before.st_size, 'source_mtime_ns':before.st_mtime_ns,
                'uncompressed_sha256':checksum.hexdigest(), 'compressed_bytes':destination.stat().st_size,
                'compressed_sha256':compressed_hash}
    sealed_temporary = Path(str(sealed) + '.tmp')
    with sealed_temporary.open('w') as saved:
        saved.write(json.dumps(evidence, indent=2) + '\n')
        saved.flush()
        os.fsync(saved.fileno())
    sealed_temporary.replace(sealed)
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/opt/media-proxy-tester"))
    parser.add_argument("--run", default="20261001")
    parser.add_argument('--journals', nargs='+', type=Path)
    parser.add_argument('--workers', type=int, default=1)
    args = parser.parse_args()
    if args.workers < 1:
        parser.error('Workers must be positive')
    os.umask(0o077)
    if args.journals:
        with ProcessPoolExecutor(max_workers=args.workers) as workers:
            for evidence in workers.map(seal, args.journals):
                print(json.dumps(evidence), flush=True)
        return
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
        print(json.dumps(seal(journal)), flush=True)


if __name__ == "__main__":
    main()
