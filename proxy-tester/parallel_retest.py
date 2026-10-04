"""Run disjoint catalog shards in parallel with the existing proxy tester."""

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import random
import signal
import subprocess
import time


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('w') as output:
        json.dump(value, output, indent=2)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def load(path):
    return json.loads(path.read_text()) if path.exists() else {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--run-dir', type=Path, required=True)
    parser.add_argument('--processes', type=int, default=4)
    parser.add_argument('--concurrency', type=int, default=20000, help='Checks per process')
    parser.add_argument('--gomaxprocs', type=int, default=0)
    parser.add_argument('--sample-shards', type=int, default=0)
    parser.add_argument('--duration', type=int, default=0, help='Seconds per benchmark; zero scans every shard')
    parser.add_argument('--connect-timeout', default='3s')
    parser.add_argument('--total-timeout', default='10s')
    args = parser.parse_args()
    if args.processes < 1 or args.concurrency < 1 or args.gomaxprocs < 0 or args.sample_shards < 0 or args.duration < 0:
        parser.error('Invalid process, concurrency, CPU, or sample setting')
    os.umask(0o077)
    source = args.input.resolve()
    manifest = load(source)
    shards = manifest['shards']
    if sum(s['records'] for s in shards) != manifest['records']:
        raise ValueError('Source manifest count mismatch')
    if len({s['file'] for s in shards}) != len(shards):
        raise ValueError('Source manifest repeats a shard')
    for shard in shards:
        if Path(shard['file']).name != shard['file'] or not (source.parent / shard['file']).is_file():
            raise ValueError('Invalid source shard path')
    if args.sample_shards:
        shards = random.Random(20261003).sample(shards, min(args.sample_shards, len(shards)))
    if args.processes > len(shards):
        parser.error('There must be at least one shard per process')
    groups = [[] for _ in range(args.processes)]
    sizes = [0] * args.processes
    for shard in sorted(shards, key=lambda item: item['records'], reverse=True):
        index = min(range(args.processes), key=sizes.__getitem__)
        groups[index].append(shard)
        sizes[index] += shard['records']
    folder = args.run_dir.resolve()
    folder.mkdir(parents=True, exist_ok=True)
    lock = (folder / 'controller.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    cpu_count = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count() or 1
    gomaxprocs = args.gomaxprocs or max(1, cpu_count // args.processes)
    config = {'source':str(source), 'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
              'binary':str(args.binary.resolve()), 'processes':args.processes,
              'concurrency_per_process':args.concurrency, 'gomaxprocs_per_process':gomaxprocs,
              'connect_timeout':args.connect_timeout, 'total_timeout':args.total_timeout,
              'duration_seconds':args.duration,
              'groups':groups, 'records':sum(sizes)}
    previous = load(folder / 'config.json')
    if previous and previous != config:
        raise ValueError('Resume configuration mismatch; use a new run directory')
    save(folder / 'config.json', config)
    children, summaries, logs = [], [], []
    stopped = False
    resumed_completed = sum(load(folder / f'part-{number:02d}' / 'results.jsonl.summary.json')
                            .get('counters',{}).get('completed',0) for number in range(args.processes))

    def stop(signum, frame):
        nonlocal stopped
        stopped = True
        for child in children:
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    points = []
    peak_rss_kib = 0
    failed = False
    try:
        for number, group in enumerate(groups):
            part = folder / f'part-{number:02d}'
            part.mkdir(exist_ok=True)
            for shard in group:
                link = part / shard['file']
                target = source.parent / shard['file']
                if link.is_symlink():
                    if link.resolve() != target:
                        raise ValueError('Existing shard link points to another catalog')
                elif link.exists():
                    raise ValueError('Refusing to replace an existing shard file')
                else:
                    link.symlink_to(target)
            save(part / 'manifest.json', {'records':sizes[number], 'shards':group})
            output = part / 'results.jsonl'
            summary_path = part / 'results.jsonl.summary.json'
            summaries.append(summary_path)
            if load(summary_path).get('state') == 'complete':
                continue
            log = (part / 'progress.log').open('a')
            logs.append(log)
            command = [str(args.binary.resolve()), '--input',str(part / 'manifest.json'),
                       '--output',str(output), '--run-id',f'{folder.name}-part-{number:02d}',
                       '--concurrency',str(args.concurrency), '--connect-timeout',args.connect_timeout,
                       '--total-timeout',args.total_timeout, '--progress-interval','2s']
            if args.duration:
                command += ['--duration', f'{args.duration}s']
            children.append(subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                            env={**os.environ,'GOMAXPROCS':str(gomaxprocs)}))
        while True:
            live = sum(child.poll() is None for child in children)
            failed = any(child.returncode not in (None, 0) for child in children)
            if failed and not stopped:
                stop(None, None)
            data = [load(path) for path in summaries]
            completed = sum(row.get('counters',{}).get('completed',0) for row in data)
            responses = sum(row.get('counters',{}).get('youtube_responses',0) for row in data)
            errors = sum(row.get('counters',{}).get('local_errors',0) for row in data)
            elapsed = time.monotonic() - started
            rss = 0
            for child in children:
                try:
                    lines = Path(f'/proc/{child.pid}/status').read_text().splitlines()
                    rss += next(int(line.split()[1]) for line in lines if line.startswith('VmRSS:'))
                except (FileNotFoundError, ProcessLookupError, StopIteration):
                    pass
            peak_rss_kib = max(peak_rss_kib, rss)
            points.append((elapsed, completed))
            complete = not live and not failed and all(row.get('state') == 'complete' for row in data)
            benchmark_complete = bool(args.duration) and not stopped and not live and not failed and all(
                row.get('state') in ('complete','interrupted') for row in data)
            report = {'state':'complete' if complete else 'benchmark_complete' if benchmark_complete else 'stopped' if not live else 'running',
                      'records':sum(sizes), 'completed':completed, 'responses':responses,
                      'resumed_completed':resumed_completed, 'new_completed':completed-resumed_completed,
                      'local_errors':errors, 'seconds':round(elapsed,3),
                      'checks_per_second':round((completed-resumed_completed) / max(elapsed,.001),2),
                      'active_processes':live, 'peak_rss_kib':peak_rss_kib,
                      'processes':args.processes, 'concurrency_per_process':args.concurrency,
                      'gomaxprocs_per_process':gomaxprocs,
                      'exit_codes':[child.poll() for child in children]}
            middle = [point for point in points if sum(sizes)*.1 <= point[1] <= sum(sizes)*.85]
            if len(middle) >= 2 and not resumed_completed:
                report['active_checks_per_second'] = round((middle[-1][1]-middle[0][1]) /
                                                          (middle[-1][0]-middle[0][0]),2)
            save(folder / 'summary.json', report)
            print(json.dumps(report), flush=True)
            if not live:
                if not (complete or benchmark_complete) or errors or (complete and completed != sum(sizes)):
                    raise SystemExit(1)
                break
            time.sleep(5)
    finally:
        stop(None, None)
        for child in children:
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
