"""Test the stored proxy catalog three times and publish one ranked file."""
import argparse
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import signal
import threading
import time

from catalog_sync import DEFAULT_REPOSITORY, synchronize, validate_repository
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

    @classmethod
    def environment(cls):
        repository = os.environ.get("MEDIA_PROXY_REPOSITORY", DEFAULT_REPOSITORY)
        validate_repository(repository)
        concurrency = int(os.environ.get("MEDIA_PROXY_TEST_CONCURRENCY", "0"))
        workers = int(os.environ.get("MEDIA_PROXY_TEST_WORKERS", "0"))
        if concurrency < 0 or workers < 0:
            raise ValueError("Test sizes must be positive; zero selects automatic sizing")
        cpus = getattr(os, "process_cpu_count", os.cpu_count)() or 2
        return cls(repository=repository, test_concurrency=concurrency or automatic_concurrency(),
                   test_workers=workers or max(1, min(4, cpus // 12)))


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



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("refresh", "sync", "status"))
    parser.add_argument("--home", type=Path, default=SERVICE_DIR)
    args = parser.parse_args()
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
                result = synchronize(settings.repository, home=args.home, stop=stop)
            else:
                from proxy_file_test import test_catalog
                result = test_catalog(args.home, settings, stop, status, owner)
            state = "complete"
            print(json.dumps(result, default=str), flush=True)
        finally:
            stop.set()
            status.thread.join(timeout=6)
            write_json(args.home / "service-status.json", {"state":state,"finished_at":utcnow()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
