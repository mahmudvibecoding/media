"""Continuously synchronize the catalog, test proxies, and publish a ranked pool."""
import argparse
import asyncio
from dataclasses import asdict, dataclass, replace
from datetime import datetime
import json
import math
import os
from pathlib import Path
import signal
import shutil
import threading
import time
import uuid

from catalog_sync import DEFAULT_REPOSITORY, synchronize, validate_repository
from collect_video_metadata import CLIENT_VERSION, fetch_metadata
from proxy_catalog import CatalogClients, SUPPORTED_PROTOCOLS, load_catalog
from proxy_pool import Observation, apply_observations, export_due, export_pool, import_test_journal, pool_status
from proxy_service_common import SERVICE_DIR, available_memory, digest, file_lock, read_json, run_owned, utcnow, write_json
from runtime_config import BRIDGE_BINARY, connect_database


def automatic_concurrency():
    cpus = getattr(os, "process_cpu_count", os.cpu_count)() or 2
    quota = Path("/sys/fs/cgroup/cpu.max")
    if quota.exists():
        maximum, period = quota.read_text().split()
        if maximum != "max":
            cpus = min(cpus, max(1, math.ceil(int(maximum) / int(period))))
    memory = available_memory()
    # Reserve most memory for PostgreSQL, TLS, the OS, and other application work.
    return max(64, min(80000, cpus * 2048, int(memory * 0.15) // (128 * 1024)))


@dataclass(frozen=True)
class Settings:
    repository: str = DEFAULT_REPOSITORY
    sync_interval: int = 3600
    retest_interval: int = 21600
    test_concurrency: int = 0
    batch_size: int = 0
    quality_sample: int = 64
    quality_concurrency: int = 32
    quality_interval: int = 3600
    journal_retention: int = 86400
    video_id: str = "1hzvCKusdpc"
    client_version: str = CLIENT_VERSION
    connect_timeout: int = 3
    request_timeout: int = 10

    @classmethod
    def environment(cls):
        values = {"repository": os.environ.get("MEDIA_PROXY_REPOSITORY", DEFAULT_REPOSITORY)}
        names = {"sync_interval": "SYNC_INTERVAL", "retest_interval": "RETEST_INTERVAL",
                 "test_concurrency": "TEST_CONCURRENCY", "batch_size": "TEST_BATCH_SIZE",
                 "quality_sample": "QUALITY_SAMPLE", "quality_concurrency": "QUALITY_CONCURRENCY",
                 "quality_interval": "QUALITY_INTERVAL", "journal_retention": "JOURNAL_RETENTION"}
        for field, name in names.items():
            if "MEDIA_PROXY_" + name in os.environ:
                values[field] = int(os.environ["MEDIA_PROXY_" + name])
        result = cls(**values)
        validate_repository(result.repository)
        if (result.sync_interval < 1 or result.retest_interval < 1 or result.quality_interval < 1
                or result.test_concurrency < 0 or result.batch_size < 0 or result.quality_sample < 0
                or result.quality_concurrency < 1 or result.journal_retention < 1):
            raise ValueError("Proxy service settings must be positive; zero selects automatic test sizing")
        concurrency = result.test_concurrency or automatic_concurrency()
        return replace(result, test_concurrency=concurrency, batch_size=result.batch_size or min(1000000, concurrency * 16))


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


def safe_error(exc):
    # Exception bodies can contain database URLs or proxy credentials.
    return type(exc).__name__


def event(kind, **fields):
    print(json.dumps({"event": kind, "at": utcnow(), **fields}, default=str), flush=True)


def test_batch(home, settings, stop, status, *, after_id=None, max_id=None, retain_checkpoint=False,
               round_id=None, pass_number=None):
    started = time.monotonic()
    home = Path(home)
    checkpoint = home / "current-test.json"
    current = read_json(checkpoint)
    if current is None:
        run_id = str(uuid.uuid4())
        folder = home / "runs" / run_id
        with connect_database("proxy", autocommit=True) as conn:
            manifest = export_due(conn, folder, limit=settings.batch_size, after_id=after_id, max_id=max_id)
        if manifest is None:
            return None
        current = {"run_id": run_id, "manifest_sha256": digest(folder / "manifest.json"),
                   "settings": asdict(settings), "phase": "testing",
                   "after_id": after_id, "max_id": max_id, "last_proxy_id": manifest["last_proxy_id"],
                   "round_id": round_id, "pass_number": pass_number}
        write_json(checkpoint, current)
    if current.get("after_id") != after_id or current.get("max_id") != max_id:
        raise ValueError("Saved batch belongs to a different catalog range")
    if current.get("round_id") != round_id or current.get("pass_number") != pass_number:
        raise ValueError("Saved batch belongs to a different scoring pass")
    if str(uuid.UUID(current["run_id"])) != current["run_id"]:
        raise ValueError("Invalid saved test run")
    folder = home / "runs" / current["run_id"]
    manifest = folder / "manifest.json"
    if digest(manifest) != current["manifest_sha256"]:
        raise ValueError("Saved test input changed")
    exported = time.monotonic()
    saved = Settings(**current["settings"])
    journal = folder / "results.jsonl"
    concurrency = min(settings.test_concurrency, current.get("concurrency", settings.test_concurrency))
    status.update(state="testing", run_id=current["run_id"], concurrency=concurrency)
    if current["phase"] == "testing":
        event("proxy_test_started", run_id=current["run_id"], concurrency=concurrency,
              selected=read_json(manifest)["records"])
        with (folder / "tester.log").open("a") as log:
            code = run_owned([str(BRIDGE_BINARY), "--input", str(manifest), "--output", str(journal),
                "--run-id", current["run_id"], "--concurrency", str(concurrency),
                "--connect-timeout", f"{saved.connect_timeout}s", "--total-timeout", f"{saved.request_timeout}s",
                "--video-id", saved.video_id, "--client-version", saved.client_version],
                stop=stop, stdout=log, stderr=log)
        summary = read_json(Path(str(journal) + ".summary.json"), {})
        if summary.get("state") == "local_overload":
            current["concurrency"] = max(1, concurrency // 2)
            write_json(checkpoint, current)
            event("proxy_test_reduced_concurrency", previous=concurrency, next=current["concurrency"])
        if code or summary.get("state") != "complete":
            raise RuntimeError("Proxy tester retained an unfinished checkpoint")
        metadata = read_json(Path(str(journal) + ".meta.json"), {})
        if metadata.get("input_sha256") != current["manifest_sha256"]:
            raise ValueError("Tester journal does not match its input")
        current.update(phase="importing", journal_sha256=digest(journal))
        write_json(checkpoint, current)
    if stop.is_set():
        raise InterruptedError("Test results are saved for import on restart")
    if digest(journal) != current["journal_sha256"]:
        raise ValueError("Stopped test journal changed")
    tested = time.monotonic()
    status.update(state="importing_tests")
    with connect_database("proxy", autocommit=True) as conn:
        report = import_test_journal(conn, journal, retest_seconds=settings.retest_interval,
                                     round_id=round_id, pass_number=pass_number)
    report["seconds"] = {"prepare": round(exported-started, 2), "test": round(tested-exported, 2),
                         "import": round(time.monotonic()-tested, 2), "total": round(time.monotonic()-started, 2)}
    write_json(folder / "completed.json", {**report, "completed_at": utcnow().isoformat()})
    if not retain_checkpoint:
        checkpoint.unlink()
    status.update(last_test=report)
    event("proxy_test_imported", run_id=current["run_id"], **report)
    return report


def refresh(home, settings, stop, status, owner):
    """One fixed catalog, three complete passes, with durable batch progress."""
    home = Path(home)
    checkpoint = home / "current-refresh.json"
    current = read_json(checkpoint)
    for previous in (home / "manual-runs").glob("*"):
        if previous.is_dir() and not previous.is_symlink():
            clean_completed(previous, settings.journal_retention)
    if current is None:
        status.update(state="syncing")
        catalog = synchronize(settings.repository, home=home, stop=stop)
        if stop.is_set():
            raise InterruptedError("Catalog update stopped; invoke the command to resume")
        with connect_database("proxy", autocommit=True) as conn:
            maximum, count = conn.execute("SELECT coalesce(max(proxy_id),0),count(*) FROM public.proxies").fetchone()
        current = {"run_id": str(uuid.uuid4()), "started_at": utcnow().isoformat(),
                   "catalog": catalog, "max_id": maximum, "configurations": count,
                   "pass": 1, "after_id": 0, "observations": 0, "last_batch": None}
        write_json(checkpoint, current)
    batch_home = home / "manual-runs" / current["run_id"]
    batch_home.mkdir(parents=True, exist_ok=True)
    batch_checkpoint = batch_home / "current-test.json"
    while current["pass"] <= 3:
        if stop.is_set():
            raise InterruptedError("Progress saved; invoke the command to resume")
        owner.execute("SELECT 1")
        saved = read_json(batch_checkpoint)
        # The cursor may already be committed when checkpoint deletion is interrupted.
        if saved and saved["run_id"] == current["last_batch"]:
            batch_checkpoint.unlink()
        status.update(pass_number=current["pass"], configurations=current["configurations"],
                      observations=current["observations"])
        report = test_batch(batch_home, settings, stop, status, after_id=current["after_id"],
                            max_id=current["max_id"], retain_checkpoint=True,
                            round_id=current["run_id"], pass_number=current["pass"])
        if report is None:
            event("proxy_pass_completed", pass_number=current["pass"], configurations=current["configurations"])
            current.update({"pass": current["pass"] + 1, "after_id": 0})
        else:
            saved = read_json(batch_checkpoint)
            current.update(after_id=saved["last_proxy_id"], last_batch=saved["run_id"],
                           observations=current["observations"] + report["observations"])
        write_json(checkpoint, current)
        if report is not None:
            batch_checkpoint.unlink()
        with connect_database("proxy", autocommit=True) as conn:
            export_pool(conn, home)
        clean_completed(batch_home, settings.journal_retention)
    if current["observations"] != current["configurations"] * 3:
        raise RuntimeError("Catalog changed during the run; saved counts require inspection")
    with connect_database("proxy", autocommit=True) as conn:
        scored = conn.execute("SELECT count(*) FROM app_meta.proxy_round_results WHERE round_id=%s AND passes=7",
                              (current["run_id"],)).fetchone()[0]
        if scored != current["configurations"]:
            raise RuntimeError("Scoring is incomplete; the saved run is retained for inspection")
        pool = export_pool(conn, home)
    result = {**current, "passes_completed": 3, "pool": pool, "completed_at": utcnow().isoformat(),
              "seconds": round((utcnow()-datetime.fromisoformat(current["started_at"])).total_seconds(), 2)}
    write_json(home / "last-refresh.json", result)
    checkpoint.unlink()
    return result


def read_quality(path):
    """Only a final interrupted line can be discarded before resuming probes."""
    path = Path(path)
    if not path.exists():
        return []
    records, end = [], 0
    with path.open("r+b") as source:
        for line in source:
            if not line.endswith(b"\n"):
                source.truncate(end)
                break
            records.append(json.loads(line))
            end += len(line)
    identifiers = [r["proxy_id"] for r in records]
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("Duplicate proxy in the data-quality journal")
    return records


async def collect_quality(proxies, path, settings, stop, *, pool_factory=CatalogClients):
    path = Path(path)
    completed = {row["proxy_id"] for row in read_quality(path)}
    remaining = [proxy for proxy in proxies if proxy.proxy_id not in completed]
    if not remaining:
        return
    semaphore = asyncio.Semaphore(settings.quality_concurrency)
    with path.open("a") as output:
        os.chmod(path, 0o600)
        async with pool_factory(remaining, settings.quality_concurrency,
                connect_timeout=5, request_timeout=15, per_proxy_connections=1) as clients:
            async def one(number, proxy):
                async with semaphore:
                    if stop.is_set():
                        return
                    observations = []
                    result = await fetch_metadata(clients[number], settings.video_id,
                        settings.client_version, retries=0, on_attempt=observations.append, total_timeout=15)
                    record = {"proxy_id": proxy.proxy_id, "connection_key": proxy.connection_key.hex(),
                              "declared_protocol": proxy.transport, "protocol": proxy.working_protocol,
                              "seconds": result["seconds"], "checked_at": utcnow().isoformat(),
                              "outcome": asdict(observations[0]) if observations else None}
                    output.write(json.dumps(record, default=str, separators=(",", ":")) + "\n")
                    output.flush()
                    os.fsync(output.fileno())
            pending = {asyncio.create_task(one(i, proxy)) for i, proxy in enumerate(remaining)}
            try:
                while pending:
                    done, pending = await asyncio.wait(pending, timeout=0.25, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()
                    if stop.is_set():
                        raise InterruptedError("Quality probes are checkpointed")
            finally:
                for task in pending:
                    task.cancel()
                await asyncio.gather(*pending, return_exceptions=True)


def quality_batch(home, settings, stop, status):
    if not settings.quality_sample:
        return None
    home = Path(home)
    checkpoint = home / "current-quality.json"
    current = read_json(checkpoint)
    if current is None:
        with connect_database("proxy") as conn:
            identifiers = [r[0] for r in conn.execute("""SELECT q.proxy_id
                FROM app_meta.proxy_test_state q JOIN public.proxies p USING(proxy_id)
                JOIN public.proxy_stats s USING(proxy_id)
                WHERE q.last_response_at>=statement_timestamp()-interval '24 hours'
                  AND (q.quality_checked_at IS NULL OR q.quality_checked_at<statement_timestamp()-make_interval(secs=>%s))
                  AND s.working_protocol=ANY(%s)
                ORDER BY q.quality_checked_at ASC NULLS FIRST,q.proxy_id LIMIT %s""",
                (settings.quality_interval, sorted(SUPPORTED_PROTOCOLS), settings.quality_sample))]
        if not identifiers:
            return None
        current = {"run_id": str(uuid.uuid4()), "proxy_ids": identifiers, "settings": asdict(settings)}
        write_json(checkpoint, current)
    run_id = str(uuid.UUID(current["run_id"]))
    if run_id != current["run_id"]:
        raise ValueError("Invalid quality run")
    folder = home / "quality" / run_id
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    journal = folder / "observations.jsonl"
    status.update(quality={"state": "probing", "run_id": run_id, "selected": len(current["proxy_ids"])})
    proxies = load_catalog(current["proxy_ids"])
    asyncio.run(collect_quality(proxies, journal, Settings(**current["settings"]), stop))
    records = read_quality(journal)
    if {r["proxy_id"] for r in records} != set(current["proxy_ids"]):
        raise ValueError("Quality probe coverage is incomplete")
    observations = []
    for record in records:
        outcome = record["outcome"]
        if outcome is None:
            continue
        checked = datetime.fromisoformat(outcome["checked_at"])
        observations.append(Observation(record["proxy_id"], bytes.fromhex(record["connection_key"]),
            checked, record["declared_protocol"], record["protocol"] if outcome["http_status"] is not None else None,
            True, outcome["connected"] is True or outcome["request_sent"], outcome["request_sent"],
            outcome["http_status"], outcome["data_received"], outcome["connection_error"],
            outcome["website_error"], record["seconds"] * 1000))
    status.update(quality={"state": "importing", "run_id": run_id})
    with connect_database("proxy", autocommit=True) as conn:
        report = apply_observations(conn, observations, bytes.fromhex(digest(journal)), run_id,
                                    retest_seconds=settings.retest_interval, quality=True)
        # A local bridge error is not a proxy failure; defer its probe without
        # changing reachability or recording an invented network attempt.
        skipped = [r["proxy_id"] for r in records if r["outcome"] is None]
        if skipped:
            conn.execute("UPDATE app_meta.proxy_test_state SET quality_checked_at=clock_timestamp() WHERE proxy_id=ANY(%s)", (skipped,))
    write_json(folder / "completed.json", {**report, "completed_at": utcnow().isoformat()})
    checkpoint.unlink()
    status.update(quality={"state": "ready", "last_run_id": run_id})
    return report


def quality_worker(home, settings, stop, dirty, status):
    while not stop.is_set():
        try:
            report = quality_batch(home, settings, stop, status)
            if report:
                dirty.set()
            elif stop.wait(5):
                break
        except InterruptedError:
            break
        except Exception as exc:
            status.update(quality={"state": "retrying", "error_type": safe_error(exc)})
            event("quality_retry", error_type=safe_error(exc))
            stop.wait(3)


def pool_worker(home, stop, dirty, status):
    while not stop.is_set():
        if not dirty.wait(1):
            continue
        dirty.clear()
        try:
            with connect_database("proxy", autocommit=True) as conn:
                report = export_pool(conn, home)
            status.update(pool=report)
        except Exception as exc:
            dirty.set()
            status.update(pool={"state": "retrying", "error_type": safe_error(exc)})
        stop.wait(5)


def clean_completed(home, retention):
    """Expire only imported run files; interrupted work is always retained."""
    home = Path(home)
    active = {value.get("run_id") for name in ("current-test.json", "current-quality.json")
              if (value := read_json(home / name, {}))}
    cutoff = time.time() - retention
    for kind in ("runs", "quality"):
        for folder in (home / kind).glob("*"):
            marker = folder / "completed.json"
            if folder.is_dir() and not folder.is_symlink() and folder.name not in active and marker.is_file():
                completed = read_json(marker)
                if datetime.fromisoformat(completed["completed_at"]).timestamp() < cutoff:
                    shutil.rmtree(folder)


def sync_worker(home, settings, stop, dirty, status):
    state_path = Path(home) / "schedule.json"
    schedule = read_json(state_path, {})
    while not stop.is_set():
        if time.time() < schedule.get("next_sync", 0):
            stop.wait(min(5, max(0, schedule["next_sync"] - time.time())))
            continue
        try:
            status.update(catalog={"state": "synchronizing"})
            result = synchronize(settings.repository, home=home, stop=stop)
            status.update(catalog=result)
            schedule.update(next_sync=time.time() + settings.sync_interval, sync_error=None)
            dirty.set()
            clean_completed(home, settings.journal_retention)
        except InterruptedError:
            break
        except Exception as exc:
            schedule.update(next_sync=time.time() + 60, sync_error=safe_error(exc))
            status.update(catalog={"state": "retrying", "error_type": safe_error(exc)})
            event("catalog_retry", error_type=safe_error(exc))
        write_json(state_path, schedule)


def serve(home, settings, stop, status, owner):
    home = Path(home)
    dirty = threading.Event()
    dirty.set()
    workers = [threading.Thread(target=pool_worker, args=(home, stop, dirty, status), daemon=True),
               threading.Thread(target=sync_worker, args=(home, settings, stop, dirty, status), daemon=True)]
    if settings.quality_sample:
        workers.append(threading.Thread(target=quality_worker, args=(home, settings, stop, dirty, status), daemon=True))
    for worker in workers:
        worker.start()
    try:
        while not stop.is_set():
            # Restart if the connection holding the singleton lock was lost.
            owner.execute("SELECT 1")
            try:
                report = test_batch(home, settings, stop, status)
                if report:
                    dirty.set()
                else:
                    status.update(state="waiting")
                    stop.wait(1)
            except InterruptedError:
                break
            except Exception as exc:
                status.update(state="test_retry", error_type=safe_error(exc))
                event("proxy_test_retry", error_type=safe_error(exc))
                stop.wait(3)
    finally:
        stop.set()
        for worker in workers:
            worker.join(timeout=25)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("run", "refresh", "sync", "test", "pool", "status", "health"))
    parser.add_argument("--home", type=Path, default=SERVICE_DIR)
    parser.add_argument("--limit", type=int, help="Number of due proxies in a manual test batch")
    parser.add_argument("--concurrency", type=int)
    args = parser.parse_args()
    os.umask(0o077)
    if args.command == "health":
        value = read_json(args.home / "service-status.json", {})
        return 0 if time.time() - value.get("heartbeat_at", 0) < 45 else 1
    if args.command == "status":
        with connect_database("proxy", autocommit=True) as conn:
            result = {"service": read_json(args.home / "service-status.json"), "counts": pool_status(conn),
                      "pool": read_json(args.home / "pool-status.json"), "catalog": read_json(args.home / "catalog-status.json")}
        print(json.dumps(result, default=str, indent=2))
        return 0
    settings = Settings.environment()
    if args.limit is not None:
        if args.limit < 1:
            parser.error("--limit must be positive")
        settings = replace(settings, batch_size=args.limit)
    if args.concurrency is not None:
        if args.concurrency < 1:
            parser.error("--concurrency must be positive")
        settings = replace(settings, test_concurrency=args.concurrency)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    args.home.mkdir(parents=True, exist_ok=True, mode=0o700)
    status = Status(args.home, stop)
    if args.command == "sync":
        print(json.dumps(synchronize(settings.repository, home=args.home, stop=stop)))
        return 0
    if args.command == "pool":
        with connect_database("proxy", autocommit=True) as conn:
            print(json.dumps(export_pool(conn, args.home)))
        return 0
    if args.command == "run" and os.environ.get("MEDIA_PROXY_SERVICE_ENABLED", "1") == "0":
        status.update(state="disabled")
        status.thread.start()
        stop.wait()
        return 0
    with file_lock(args.home / "service.lock"), connect_database("proxy", autocommit=True) as owner:
        if not owner.execute("SELECT pg_try_advisory_lock(hashtextextended('media:proxy_service',0))").fetchone()[0]:
            raise RuntimeError("Another proxy service owns this database")
        # Also seed scheduling for an existing, manually populated installation.
        owner.execute("""INSERT INTO app_meta.proxy_test_state(proxy_id)
            SELECT p.proxy_id FROM public.proxies p WHERE NOT EXISTS
              (SELECT 1 FROM app_meta.proxy_test_state q WHERE q.proxy_id=p.proxy_id)
            ON CONFLICT DO NOTHING""")
        if args.command == "refresh":
            status.thread.start()
            try:
                print(json.dumps(refresh(args.home, settings, stop, status, owner), default=str))
            finally:
                stop.set()
                status.thread.join(timeout=6)
                write_json(args.home / "service-status.json", {"state": "stopped", "heartbeat_at": time.time()})
        elif args.command == "test":
            result = test_batch(args.home, settings, stop, status)
            quality_batch(args.home, settings, stop, status)
            with connect_database("proxy", autocommit=True) as conn:
                pool = export_pool(conn, args.home)
            print(json.dumps({"test": result, "pool": pool}))
        else:
            status.thread.start()
            try:
                serve(args.home, settings, stop, status, owner)
            finally:
                stop.set()
                status.thread.join(timeout=6)
                write_json(args.home / "service-status.json", {"state": "stopped", "heartbeat_at": time.time()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
