"""Read batch imports from a local run or an SSH worker."""
import argparse
import json
import math
from pathlib import Path
import shlex
import subprocess


def parse_import_args(description, collector, argv=None):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--run', type=Path, help='Import a local run; read its manifest for the run ID')
    parser.add_argument('--host')
    parser.add_argument('--remote')
    parser.add_argument('--local', type=Path)
    parser.add_argument('--run-id')
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--once', action='store_true')
    args = parser.parse_args(argv)
    remote_args = (args.host, args.remote, args.local, args.run_id)
    if args.run:
        if any(remote_args):
            parser.error('--run cannot be combined with --host, --remote, --local, or --run-id')
        manifest = json.loads((args.run / 'manifest.json').read_text())
        if manifest.get('collector', 'metadata') != collector:
            parser.error(f'The run is not a {collector} collection')
        args.local = args.run
        args.run_id = manifest['run_id']
    elif not all(remote_args):
        parser.error('Use --run, or supply --host, --remote, --local, and --run-id together')
    if not math.isfinite(args.interval) or args.interval <= 0:
        parser.error('--interval must be positive and finite')
    return args


def sync_outbox(args, folder):
    if args.host:
        subprocess.run(['rsync', '-a', '--ignore-existing', '--include=*.jsonl.gz',
            '--include=*.jsonl.gz.json', '--exclude=*', '-e', 'ssh -o BatchMode=yes -o ConnectTimeout=10',
            f'{args.host}:{args.remote}/outbox/', str(folder / 'outbox') + '/'], check=True, timeout=120,
            stdout=subprocess.DEVNULL)


def source_status(args, folder):
    if not args.host:
        return json.loads((folder / 'status.json').read_text())
    command = 'cat ' + shlex.quote(str(Path(args.remote) / 'status.json'))
    response = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', args.host, command],
        check=True, timeout=20, text=True, capture_output=True)
    return json.loads(response.stdout)
