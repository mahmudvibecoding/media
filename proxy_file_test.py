"""Stream the catalog to Go; keep scores in RAM and publish one ranked file."""
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import uuid

from psycopg import sql

from proxy_service_common import digest, utcnow, write_json
from runtime_config import BRIDGE_BINARY, connect_database


def test_catalog(home, settings, stop, status, owner):
    home = Path(home)
    started = time.monotonic()
    run_id = str(uuid.uuid4())
    maximum, count = owner.execute("SELECT coalesce(max(proxy_id),0),count(*) FROM public.proxies").fetchone()
    workers = min(settings.test_workers, settings.test_concurrency, count) if count else 0
    offsets = [count*n//workers for n in range(workers+1)] if workers else [0]
    bounds = [0]
    for offset in offsets[1:-1]:
        bounds.append(owner.execute("SELECT proxy_id FROM public.proxies WHERE proxy_id<=%s ORDER BY proxy_id OFFSET %s LIMIT 1",
                                    (maximum, offset)).fetchone()[0])
    bounds.append(maximum+1)
    processes = []
    status.update(state="testing_in_memory", run_id=run_id, configurations=count,
                  concurrency=settings.test_concurrency, test_workers=workers, checkpoints=False)
    with tempfile.TemporaryDirectory(prefix="proxy-results-", dir=home) as temporary:
        folder = Path(temporary)

        def partition(index):
            output = folder / f"working-{index}.jsonl"
            environment = os.environ.copy()
            cpus = getattr(os, "process_cpu_count", os.cpu_count)() or 2
            environment.setdefault("GOMAXPROCS", str(max(1, cpus // max(workers,1))))
            command = [str(BRIDGE_BINARY), "score", "--run-id", run_id, "--output", str(output),
                       "--expected", str(offsets[index+1]-offsets[index]),
                       "--concurrency", str(max(1, settings.test_concurrency//workers)),
                       "--connect-timeout", f"{settings.connect_timeout}s",
                       "--total-timeout", f"{settings.request_timeout}s"]
            query = sql.SQL("""COPY (SELECT json_build_object(
                'id',p.proxy_id,'key',encode(p.connection_key,'hex'),'address',p.address,'port',p.port,
                'protocol',p.connection_settings->>'transport','settings',
                CASE WHEN jsonb_typeof(p.connection_settings->'options')='string'
                    THEN (p.connection_settings->>'options')::json ELSE (p.connection_settings->'options')::json END)
                FROM public.proxies p
                WHERE p.proxy_id>={} AND p.proxy_id<{} ORDER BY p.proxy_id)
                TO STDOUT WITH (FORMAT csv, DELIMITER E'\\x02', QUOTE E'\\x01', ESCAPE E'\\x01')""").format(
                    sql.Literal(bounds[index]),sql.Literal(bounds[index+1]))
            with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  env=environment, start_new_session=True) as process:
                processes.append(process)
                try:
                    with connect_database("proxy", autocommit=True) as conn, conn.transaction():
                        conn.execute("SET TRANSACTION READ ONLY")
                        conn.execute("SET LOCAL work_mem='512MB'")
                        conn.execute("SET LOCAL jit=off")
                        with conn.cursor().copy(query) as source:
                            for block in source:
                                if stop.is_set():
                                    raise InterruptedError("Testing interrupted; no checkpoint was written")
                                process.stdin.write(block)
                    process.stdin.close()
                    summary = process.stdout.read()
                    code = process.wait()
                    if code:
                        raise RuntimeError(f"Proxy scorer exited with status {code}")
                    return json.loads(summary)
                except BaseException as exc:
                    try:
                        process.stdin.close()
                    except BrokenPipeError:
                        pass
                    if process.poll() is None:
                        try:
                            os.killpg(process.pid, signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                        try:
                            process.wait(timeout=20)
                        except subprocess.TimeoutExpired:
                            os.killpg(process.pid, signal.SIGKILL)
                            process.wait()
                    if isinstance(exc, BrokenPipeError):
                        raise RuntimeError(f"Proxy scorer stopped with exit status {process.returncode}") from exc
                    raise

        reports = []
        with ThreadPoolExecutor(max_workers=max(1,workers)) as executor:
            futures = [executor.submit(partition,n) for n in range(workers)]
            pending = set(futures)
            try:
                while pending:
                    if stop.is_set():
                        raise InterruptedError("Testing interrupted; no checkpoint was written")
                    owner.execute("SELECT 1")
                    done, pending = wait(pending,timeout=0.5,return_when=FIRST_COMPLETED)
                    for future in done:
                        reports.append(future.result())
            except BaseException:
                stop.set()
                for process in processes:
                    if process.poll() is None:
                        try:
                            os.killpg(process.pid,signal.SIGTERM)
                        except ProcessLookupError:
                            pass
                raise
        performed = sum(r["new_checks"] for r in reports)
        if sum(r["configurations"] for r in reports) != count or performed != 3*count:
            raise RuntimeError("The test did not cover the complete catalog")
        rows = []
        for part in folder.glob("working-*.jsonl"):
            with part.open() as source:
                rows.extend(json.loads(line) for line in source)
        if len({row["proxy_id"] for row in rows}) != len(rows):
            raise RuntimeError("Duplicate proxy in the final result")
        rows.sort(key=lambda r:(-r["score"],r["average_response_ms"],r["proxy_id"]))
        path = home / "ranked-proxies.jsonl"
        output = folder / "ranked-proxies.jsonl"
        with output.open("w") as stream:
            os.chmod(output,0o600)
            for row in rows:
                stream.write(json.dumps(row,separators=(",",":"))+"\n")
            stream.flush()
            os.fsync(stream.fileno())
        output.replace(path)
        pool = {"updated_at":utcnow().isoformat(),"exported":len(rows),"limit":0,
                "scoring":"youtube_responses_last_three","path":str(path),"sha256":digest(path),
                "storage":"file"}
        write_json(home/"pool-status.json",pool)
        response_count = sum(r["responses"] for r in reports)
        result = {"run_id":run_id,"configurations":count,"passes_completed":3,"observations":3*count,
                  "new_checks":performed,"pool":pool,"checkpoints":False,
                  "scores":{str(n):sum(r["score"]==n for r in rows) for n in (3,2,1)} | {"0":count-len(rows)},
                  "responses":response_count,"average_response_ms":(
                      sum(r["response_time_ms"] for r in reports)/response_count if response_count else None),
                  "completed_at":utcnow().isoformat(),"seconds":round(time.monotonic()-started,2)}
        write_json(home/"last-refresh.json",result)
        return result
