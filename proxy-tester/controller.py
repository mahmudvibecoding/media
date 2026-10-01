"""Run measured concurrency tuning, a complete scan, recovery, and stability checks."""

import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    temporary.replace(path)


def load(path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


def resources(pid):
    def fields(path):
        result = {}
        for line in path.read_text().splitlines():
            key, _, value = line.partition(":")
            parts = value.strip().split()
            if parts and parts[0].isdigit():
                result[key] = int(parts[0])
        return result
    memory = fields(Path("/proc/meminfo"))
    try:
        process = fields(Path(f"/proc/{pid}/status"))
        descriptors = len(list(Path(f"/proc/{pid}/fd").iterdir()))
    except FileNotFoundError:
        process, descriptors = {}, 0
    return {"time": time.time(), "rss_kib": process.get("VmRSS", 0), "threads": process.get("Threads", 0),
            "file_descriptors": descriptors, "available_kib": memory["MemAvailable"], "total_kib": memory["MemTotal"]}


def active_rate(path, concurrency, records):
    """Use elapsed time over the middle of the sample, excluding startup and drain."""
    points = []
    for line in path.read_text().splitlines():
        try:
            current = json.loads(line)
        except ValueError:
            continue
        if current.get("state") != "running" or current["seconds"] <= 0:
            continue
        if points and current["seconds"] < points[-1]["seconds"]:
            points = []
        if records * .1 <= current["completed"] <= records * .85:
            points.append(current)
    if len(points) < 2:
        return 0
    first, last = points[0], points[-1]
    return (last["completed"] - first["completed"]) / max(1, last["seconds"] - first["seconds"])


def response_overlap(journal, reference):
    responding, checked_reference = set(), set()
    with journal.open() as source:
        for line in source:
            result = json.loads(line)
            if result["status"] == "local_error":
                continue
            if result["id"] in reference:
                checked_reference.add(result["id"])
            if result["responds"]:
                responding.add(result["id"])
    return responding, len(checked_reference), len(responding & checked_reference)


def selection(journals, destination, mode):
    """Each completed ID is unique within a round; merge responses across rounds."""
    selected = set()
    for journal in journals:
        with journal.open() as f:
            for line in f:
                record = json.loads(line)
                if mode == "responds" and record["responds"]:
                    selected.add(record["id"])
                elif mode == "retry" and record["status"] in ("not_responding", "internal_error", "local_error"):
                    selected.add(record["id"])
    temporary = destination.with_suffix(".tmp")
    with temporary.open("w") as f:
        for proxy_id in sorted(selected):
            f.write(f"{proxy_id}\n")
        f.flush()
        os.fsync(f.fileno())
    temporary.replace(destination)
    return len(selected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/opt/media-proxy-tester"))
    parser.add_argument("--run", default="20261001")
    parser.add_argument("--benchmark-only", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    root = args.root.resolve()
    folder = root / "runs" / args.run
    folder.mkdir(parents=True, exist_ok=True)
    lock = (folder / "controller.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    state_path = folder / "controller.json"
    state = load(state_path, {"run": args.run, "phase": "starting", "benchmarks": [], "completed_phases": []})
    child = None
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True
        if child is not None and child.poll() is None:
            child.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def update(phase, **fields):
        state.update(phase=phase, updated_at=time.time(), **fields)
        save(state_path, state)
        print(json.dumps({"phase": phase, **fields}), flush=True)

    def execute(name, concurrency, connect, total, sample=False, duration=0, ids=None):
        nonlocal child
        output = folder / name / "results.jsonl"
        output.parent.mkdir(parents=True, exist_ok=True)
        summary_path = output.with_suffix(".jsonl.summary.json")
        summary = load(summary_path)
        if summary and summary["state"] == "complete":
            return summary
        input_name = sample if isinstance(sample, str) else "sample.jsonl.gz" if sample else "manifest.json"
        command = [str(root / "bin/proxy-tester"), "--input", str(root / "catalog" / input_name),
                   "--output", str(output), "--run-id", args.run + "-" + name,
                   "--concurrency", str(concurrency), "--connect-timeout", f"{connect}s", "--total-timeout", f"{total}s"]
        if duration:
            command += ["--duration", f"{duration}s", "--progress-interval", "1s"]
        if ids is not None:
            command += ["--only-ids", str(ids)]
        update(name, concurrency=concurrency, connect_timeout=connect, total_timeout=total)
        with (output.parent / "progress.log").open("a") as log:
            child = subprocess.Popen(command, stdout=log, stderr=log)
            pressure = False
            peak_rss = peak_fds = 0
            with (output.parent / "resources.jsonl").open("a") as telemetry:
                while child.poll() is None:
                    try:
                        child.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        measured = resources(child.pid)
                        peak_rss = max(peak_rss, measured["rss_kib"])
                        peak_fds = max(peak_fds, measured["file_descriptors"])
                        telemetry.write(json.dumps(measured) + "\n")
                        telemetry.flush()
                        if measured["available_kib"] < max(512 * 1024, measured["total_kib"] * .03):
                            pressure = True
                            child.send_signal(signal.SIGTERM)
                code = child.returncode
        child = None
        if stopping:
            raise SystemExit(0)
        summary = load(summary_path)
        if code not in (0, 75) or summary is None:
            raise RuntimeError(f"{name} failed with exit {code}; see its progress.log")
        summary["exit_code"] = code
        summary["peak_rss_kib"], summary["peak_file_descriptors"] = peak_rss, peak_fds
        if pressure:
            summary.update(exit_code=75, state="local_overload", overload_reason="memory_pressure")
        save(summary_path, summary)
        return summary

    if "tuning" not in state["completed_phases"]:
        # Stop climbing when extra work stops increasing throughput or local errors appear.
        best = None
        declines = 0
        completed = {r["concurrency"]: r for r in state["benchmarks"]}
        for concurrency in (1000, 2500, 5000, 10000, 20000, 40000, 80000, 160000):
            if concurrency in completed:
                result = completed[concurrency]
            else:
                summary = execute(f"bench-{concurrency}", concurrency, 3, 10, sample=True, duration=45)
                counters = summary["counters"]
                result = {"concurrency": concurrency, "per_second": summary["completed_per_second"],
                          "completed": counters["completed"], "responds": counters["youtube_responses"],
                          "local_errors": counters["local_errors"], "exit_code": summary["exit_code"],
                          "peak_rss_kib": summary.get("peak_rss_kib"), "peak_file_descriptors": summary.get("peak_file_descriptors")}
                state["benchmarks"].append(result)
                save(state_path, state)
            if result["local_errors"] or result["exit_code"] == 75:
                break
            if best is None or result["per_second"] > best["per_second"] * 1.05:
                best, declines = result, 0
            else:
                declines += 1
            if declines >= 2:
                break
        if best is None:
            raise RuntimeError("Even the first benchmark hit local resource errors")
        state["selected_concurrency"] = best["concurrency"]
        comparison = execute("timeout-comparison", best["concurrency"], 5, 15, sample=True, duration=45)
        state["longer_timeout_benchmark"] = {"per_second": comparison["completed_per_second"], "counters": comparison["counters"]}
        state["completed_phases"].append("tuning")
        update("tuning-complete", selected_concurrency=best["concurrency"])

    if "large-tuning" not in state["completed_phases"]:
        best, declines, reference = None, 0, set()
        existing = {r["concurrency"]: r for r in state.get("large_benchmarks", [])}
        state.setdefault("large_benchmarks", [])
        for concurrency in (40000, 80000, 160000, 320000, 450000):
            name = f"large-bench-{concurrency}"
            if concurrency in existing:
                result = existing[concurrency]
            else:
                summary = execute(name, concurrency, 3, 10, sample="sample-large.jsonl.gz", duration=45)
                responding, reference_checked, reference_retained = response_overlap(folder / name / "results.jsonl", reference)
                result = {"concurrency": concurrency, "per_second": summary["completed_per_second"],
                          "active_per_second": active_rate(folder / name / "progress.log", concurrency, 1000000),
                          "completed": summary["counters"]["completed"], "responds": summary["counters"]["youtube_responses"],
                          "local_errors": summary["counters"]["local_errors"], "exit_code": summary["exit_code"],
                          "reference_checked": reference_checked, "reference_retained": reference_retained,
                          "peak_rss_kib": summary.get("peak_rss_kib"), "peak_file_descriptors": summary.get("peak_file_descriptors")}
                state["large_benchmarks"].append(result)
                save(state_path, state)
            degraded = result["reference_checked"] >= 100 and result["reference_retained"] < result["reference_checked"] * .7
            if result["local_errors"] or result["exit_code"] == 75 or degraded:
                break
            if best is None or result["active_per_second"] > best["active_per_second"] * 1.05:
                best, declines = result, 0
                reference, _, _ = response_overlap(folder / name / "results.jsonl", set())
            else:
                declines += 1
            if declines >= 2:
                break
        if best is not None:
            state["selected_concurrency"] = best["concurrency"]
        state["completed_phases"].append("large-tuning")
        update("large-tuning-complete", selected_concurrency=state["selected_concurrency"])
    if state.get("performance_metric_version") != 2:
        eligible = []
        for result in state.get("large_benchmarks", []):
            result["active_per_second"] = active_rate(folder / f"large-bench-{result['concurrency']}" / "progress.log", result["concurrency"], 1000000)
            degraded = result["reference_checked"] >= 100 and result["reference_retained"] < result["reference_checked"] * .7
            if not result["local_errors"] and result["exit_code"] == 0 and not degraded:
                eligible.append(result)
        if eligible:
            state["selected_concurrency"] = max(eligible, key=lambda r: r["active_per_second"])["concurrency"]
        state["performance_metric_version"] = 2
        update("throughput-selection-complete", selected_concurrency=state["selected_concurrency"])
    if args.benchmark_only:
        return

    def complete_round(name, connect, total, ids=None):
        concurrency = state["selected_concurrency"]
        while True:
            summary = execute(name, concurrency, connect, total, ids=ids)
            if summary["state"] == "complete":
                if name not in state["completed_phases"]:
                    state["completed_phases"].append(name)
                save(state_path, state)
                return summary
            if summary["state"] == "local_overload":
                if concurrency <= 250:
                    raise RuntimeError("Local resource failures persist at 250 concurrent checks")
                concurrency = max(250, concurrency // 2)
                state["selected_concurrency"] = concurrency
                update(name + "-backoff", selected_concurrency=concurrency)
            elif summary["state"] == "interrupted":
                raise RuntimeError(f"Unexpected interruption in {name}; resume service")
            else:
                raise RuntimeError(f"Unexpected state for {name}: {summary['state']}")

    full = complete_round("full", 3, 10)
    retries = folder / "retry.ids"
    if not retries.exists():
        update("selecting-retries")
        state["retry_count"] = selection([folder / "full/results.jsonl"], retries, "retry")
        save(state_path, state)
    recovery = complete_round("recovery", 5, 15, ids=retries)
    working = folder / "responding.ids"
    if not working.exists():
        update("selecting-stability-checks")
        journals = sorted(folder.glob("*/results.jsonl"))
        canary = root / "runs/canary/results.jsonl"
        if canary.exists():
            journals.append(canary)
        state["responding_count"] = selection(journals, working, "responds")
        state["stability_after"] = time.time() + 60
        save(state_path, state)
    while time.time() < state.get("stability_after", 0):
        if stopping:
            return
        update("waiting-for-stability-check")
        time.sleep(min(10, state["stability_after"] - time.time()))
    stability = complete_round("stability", 5, 15, ids=working)
    update("complete", full_count=full["counters"]["completed"], recovery_count=recovery["counters"]["completed"],
           responding_count=state.get("responding_count"), stability_responses=stability["counters"]["youtube_responses"])


if __name__ == "__main__":
    main()
