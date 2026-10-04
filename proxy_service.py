"""Import the catalog, refresh its sources, then test and rank every proxy."""
import argparse
import asyncio
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import signal
import threading
import time

from catalog_sync import DEFAULT_REPOSITORY, synchronize, validate_repository
from collect_proxies import argument_parser as source_parser, main as collect_sources
from proxy_file_test import test_catalog
from proxy_service_common import SERVICE_DIR, available_memory, file_lock, read_json, utcnow, write_json
from runtime_config import connect_database


def automatic_concurrency():
    cpus = getattr(os, "process_cpu_count", os.cpu_count)() or 2
    quota = Path("/sys/fs/cgroup/cpu.max")
    if quota.exists():
        maximum, period = quota.read_text().split()
        if maximum != "max":
            cpus = min(cpus, max(1, math.ceil(int(maximum) / int(period))))
    memory = available_memory()
    # Reserve most memory for PostgreSQL, TLS, the OS, and other application work.
    return max(64, min(cpus * 6144, int(memory * 0.30) // (96 * 1024)))


@dataclass(frozen=True)
class Settings:
    repository: str = DEFAULT_REPOSITORY
    test_concurrency: int = 0
    test_workers: int = 4
    connect_timeout: int = 3
    request_timeout: int = 10
    source_concurrency: int = 128
    source_per_host: int = 16
    source_github_concurrency: int = 32

    @classmethod
    def environment(cls):
        repository = os.environ.get("MEDIA_PROXY_REPOSITORY", DEFAULT_REPOSITORY)
        validate_repository(repository)
        concurrency = int(os.environ.get("MEDIA_PROXY_TEST_CONCURRENCY", "0"))
        workers = int(os.environ.get("MEDIA_PROXY_TEST_WORKERS", "0"))
        if concurrency < 0 or workers < 0:
            raise ValueError("Test sizes must be positive; zero selects automatic sizing")
        sources = {name: int(os.environ.get(env,default)) for name,env,default in (
            ('source_concurrency','MEDIA_PROXY_SOURCE_CONCURRENCY','128'),
            ('source_per_host','MEDIA_PROXY_SOURCE_PER_HOST','16'),
            ('source_github_concurrency','MEDIA_PROXY_SOURCE_GITHUB_CONCURRENCY','32'))}
        if min(sources.values()) < 1:
            raise ValueError('Source concurrency settings must be positive')
        cpus = getattr(os, "process_cpu_count", os.cpu_count)() or 2
        return cls(repository=repository, test_concurrency=concurrency or automatic_concurrency(),
                   test_workers=workers or max(1, min(4, cpus // 12)), **sources)


class Status:
    def __init__(self, home, stop):
        self.home, self.stop = Path(home), stop
        self.lock = threading.Lock()
        self.value = {"pid": os.getpid(), "started_at": utcnow().isoformat(), "state": "starting"}
        self.thread = threading.Thread(target=self.heartbeat, name="proxy-heartbeat", daemon=True)

    def update(self, **fields):
        with self.lock:
            self.value.update(fields)

    def heartbeat(self):
        while True:
            with self.lock:
                write_json(self.home / "service-status.json", {**self.value, "heartbeat_at": time.time()})
            if self.stop.wait(5):
                break

def refresh(settings, home, stop, status, owner, *, test_only=False):
    started = time.monotonic()
    stages = {}
    if not test_only:
        status.update(state='importing_catalog')
        stages['catalog'] = synchronize(settings.repository,home=home,stop=stop,exclusive_owner=owner)
        if stop.is_set():
            raise InterruptedError('Refresh stopped after the catalog import')
        status.update(state='refreshing_sources',catalog=stages['catalog'])
        options = source_parser().parse_args([])
        options.concurrency = settings.source_concurrency
        options.per_host = settings.source_per_host
        options.github_concurrency = settings.source_github_concurrency
        options.auto_resume,options.inventory = True,False
        stages['sources'] = asyncio.run(collect_sources(options,exclusive_owner=owner,stop=stop,
            on_progress=lambda value:status.update(sources=value)))
        if stages['sources']['pending_sources'] or stages['sources']['status']=='failed':
            raise RuntimeError('Source refresh is unfinished; the existing ranking was preserved')
        status.update(sources=stages['sources'])
    if stop.is_set():
        raise InterruptedError('Refresh stopped before proxy testing')
    result = test_catalog(home,settings,stop,status,owner)
    result.update(stages,workflow_seconds=round(time.monotonic()-started,2),test_only=test_only)
    write_json(Path(home)/'last-refresh.json',result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("refresh", "sync", "status"))
    parser.add_argument("--home", type=Path, default=SERVICE_DIR)
    parser.add_argument('--test-only',action='store_true',help='Test the current catalog without importing or refreshing sources')
    args = parser.parse_args()
    if args.test_only and args.command!='refresh':
        parser.error('--test-only applies to refresh')
    os.umask(0o077)
    if args.command == "status":
        print(json.dumps({"service": read_json(args.home / "service-status.json"),
                          "result": read_json(args.home / "last-refresh.json"),
                          "catalog": read_json(args.home / "catalog-status.json")}, indent=2))
        return 0
    settings = Settings.environment()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    args.home.mkdir(parents=True, exist_ok=True, mode=0o700)
    with file_lock(args.home / "service.lock"), connect_database("proxy", autocommit=True) as owner:
        if not owner.execute("SELECT pg_try_advisory_lock(hashtextextended('media:proxy_service',0))").fetchone()[0]:
            raise RuntimeError("Another proxy test or catalog import is running")
        status = Status(args.home, stop)
        status.thread.start()
        state = "failed"
        try:
            if args.command == "sync":
                status.update(state="importing_catalog")
                result = synchronize(settings.repository, home=args.home, stop=stop,exclusive_owner=owner)
            else:
                result = refresh(settings,args.home,stop,status,owner,test_only=args.test_only)
            state = "complete"
            print(json.dumps(result, default=str), flush=True)
        finally:
            stop.set()
            status.thread.join(timeout=6)
            write_json(args.home / "service-status.json", {"state":state,"finished_at":utcnow()})
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        # Database/parser exceptions can contain private configuration values.
        def names(error):
            if isinstance(error,BaseExceptionGroup):
                return [name for inner in error.exceptions for name in names(inner)]
            return [type(error).__name__]
        print(json.dumps({'refresh_failed':names(exc)}),flush=True)
        raise SystemExit(1)
