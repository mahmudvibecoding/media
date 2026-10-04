"""Compare compact proxy totals and latest health with a completed saved run."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from runtime_config import connect_database


def read_json(path):
    return json.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--extra-run-dir", action="append", default=[], type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)

    controller = read_json(args.run_dir / "controller.json")
    manifest = read_json(args.manifest)
    summaries = sorted(args.run_dir.glob("*/results.jsonl.summary.json"))
    summaries += [path / "results.jsonl.summary.json" for path in args.extra_run_dir]
    expected = {}
    for path in summaries:
        metadata = read_json(path.with_name("results.jsonl.meta.json"))
        summary = read_json(path)
        if metadata["run_id"] in expected:
            raise ValueError("Duplicate run identifier in saved files")
        expected[metadata["run_id"]] = {"metadata": metadata, "summary": summary}

    full_key = read_json(args.run_dir / "full/results.jsonl.meta.json")["run_id"]
    checks = {
        "controller_complete": controller["phase"] == "complete",
        "saved_full_count_matches_snapshot": expected[full_key]["summary"]["counters"]["completed"] == manifest["records"],
        "no_local_resource_errors": all(item["summary"]["counters"]["local_errors"] == 0 for item in expected.values()),
    }
    encodings, versions = Counter(), Counter()
    stability_requests = 0
    multiple_requests = 0
    unverified_responses = 0
    incomplete_bodies = 0
    stability_rows = []
    stability_digest = hashlib.sha256()
    with gzip.open(args.run_dir / "stability/results.jsonl.gz", "rb") as source:
        for line in source:
            stability_digest.update(line)
            result = json.loads(line)
            if result['status'] != 'local_error':
                replies = [a for a in result['attempts'] if a['status'] == 'responds']
                stability_rows.append(dict(proxy_id=result['id'],
                    checked_at=datetime.fromisoformat(result['tested_at'].replace('Z','+00:00')).isoformat(),
                    status=result['status'],attempted=result['attempted'],responds=result['responds'],
                    detected_protocol=result.get('detected_protocol'),
                    http_status=replies[0]['http_status'] if replies else None))
            requests = sum(bool(a.get("request_sent")) for a in result["attempts"])
            stability_requests += requests
            multiple_requests += requests > 1
            for attempt in result["attempts"]:
                if attempt["status"] == "responds":
                    encodings[attempt.get("content_encoding") or "none"] += 1
                    versions[attempt.get("http_version", "unknown")] += 1
                    unverified_responses += not attempt.get("tls_verified", False)
                    incomplete_bodies += not attempt.get("body_complete", False)
    checks["stability_has_at_most_one_metadata_request_per_configuration"] = multiple_requests == 0
    checks["stability_responses_have_verified_youtube_tls"] = unverified_responses == 0
    checks["stability_journal_response_count"] = sum(encodings.values()) == controller["stability_responses"]
    with connect_database("proxy", connect_timeout=5, row_factory=dict_row) as conn:
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        conn.execute("SET LOCAL statement_timeout = '10min'")
        conn.execute("SET LOCAL work_mem = '128MB'")
        totals = dict(results=sum(x['summary']['counters']['completed'] for x in expected.values()),
                      network_checks=sum(x['summary']['counters']['attempted'] for x in expected.values()),
                      youtube_responses=sum(x['summary']['counters']['youtube_responses'] for x in expected.values()))
        run_reports = [dict(run_key=key,expected_results=value['summary']['counters']['completed'])
                       for key,value in expected.items()]

        print("Checking catalog coverage and latest health...", flush=True)
        catalog = conn.execute("""
            SELECT count(*) AS configurations,
                   (SELECT count(*) FROM public.proxy_stats WHERE connection_attempts > 0) AS attempted_configurations
            FROM public.proxies
        """).fetchone()
        health = conn.execute("""
            SELECT count(*) AS configurations,
                   sum(connection_attempts) AS network_checks, sum(youtube_responses_received) AS youtube_responses,
                   sum(youtube_requests_sent) AS requests_sent,
                   count(*) FILTER (WHERE youtube_last_http_status IS NOT NULL) AS latest_responding,
                   count(*) FILTER (WHERE youtube_last_response_at IS NOT NULL) AS ever_responding,
                   count(*) FILTER (WHERE youtube_last_response_at IS NOT NULL AND youtube_last_http_status IS NULL) AS intermittent,
                   min(youtube_last_attempt_at) AS earliest_latest_attempt, max(youtube_last_attempt_at) AS latest_attempt
            FROM public.proxy_stats
        """).fetchone()
        checks["catalog_coverage"] = catalog["configurations"] == health["configurations"] == manifest["records"]
        checks["attempted_catalog_count"] = catalog["attempted_configurations"] == expected[full_key]["summary"]["counters"]["attempted"]
        checks["health_network_check_count"] = health["network_checks"] == totals["network_checks"]
        checks["health_response_count"] = health["youtube_responses"] == totals["youtube_responses"]
        checks["ever_responding_count"] = health["ever_responding"] == controller["responding_count"]
        checks["latest_responding_count"] = health["latest_responding"] == controller["stability_responses"]
        latest = conn.execute("""
            SELECT count(*) AS configurations,
                   count(*) FILTER (WHERE h.proxy_id IS NULL OR h.youtube_last_import_key IS DISTINCT FROM %s) AS wrong_latest_round,
                   count(*) FILTER (WHERE
                     (coalesce(h.youtube_last_attempt_at=r.checked_at,false),
                      coalesce(h.youtube_last_attempt_at=r.checked_at AND h.youtube_last_http_status IS NOT NULL,false),
                      CASE WHEN h.youtube_last_attempt_at=r.checked_at AND h.youtube_last_http_status IS NOT NULL
                           THEN h.working_protocol END,
                      CASE WHEN h.youtube_last_attempt_at=r.checked_at THEN h.youtube_last_http_status END)
                     IS DISTINCT FROM
                     (r.attempted,r.responds,r.detected_protocol,r.http_status)
                   ) AS mismatched_latest_result
            FROM jsonb_to_recordset(%s::jsonb) AS r(proxy_id bigint,checked_at timestamptz,
                status text,attempted boolean,responds boolean,detected_protocol text,
                http_status smallint)
            LEFT JOIN public.proxy_stats h USING(proxy_id)
        """, (stability_digest.digest(),Jsonb(stability_rows))).fetchone()
        checks['all_responders_rechecked'] = latest['configurations'] == controller['responding_count']
        checks['stability_is_latest'] = latest['wrong_latest_round'] == 0
        checks['latest_matches_saved_stability_journal'] = latest['mismatched_latest_result'] == 0
        checks['only_three_base_tables'] = {row['tablename'] for row in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'")} == {'proxies','proxy_stats','proxy_lists'}
        checks['column_counts'] = {row['table_name']: row['n'] for row in conn.execute("""
            SELECT table_name,count(*) AS n FROM information_schema.columns
            WHERE table_schema='public' AND table_name IN ('proxies','proxy_stats','proxy_lists')
            GROUP BY table_name""")} == {'proxies':6,'proxy_stats':18,'proxy_lists':8}
        statuses = conn.execute("""
            SELECT youtube_responded, count(*) AS configurations
            FROM public.proxy_health GROUP BY youtube_responded ORDER BY configurations DESC
        """).fetchall()
        response_protocols = conn.execute("""
            SELECT working_protocol AS detected_protocol, count(*) AS configurations
            FROM public.proxy_stats WHERE youtube_last_http_status IS NOT NULL
            GROUP BY working_protocol ORDER BY configurations DESC
        """).fetchall()
        response_statuses = conn.execute("""
            SELECT youtube_last_http_status AS http_status, count(*) AS configurations
            FROM public.proxy_stats WHERE youtube_last_http_status IS NOT NULL
            GROUP BY youtube_last_http_status ORDER BY youtube_last_http_status
        """).fetchall()
        checks["viewer_can_read_health"] = conn.execute(
            "SELECT has_table_privilege('media_viewer', 'public.proxy_health', 'SELECT') AS allowed"
        ).fetchone()["allowed"]

    report = {
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "all_checks_passed": all(checks.values()),
        "checks": checks,
        "catalog": catalog,
        "expected_totals_from_saved_summaries": totals,
        "health": health,
        "latest_statuses": statuses,
        "latest_response_protocols": response_protocols,
        "latest_http_statuses": response_statuses,
        "runs": run_reports,
        "selected_concurrency": controller["selected_concurrency"],
        "large_benchmarks": controller["large_benchmarks"],
        "probe": expected[full_key]["metadata"],
        "stability_request_observations": {
            "metadata_requests_sent": stability_requests,
            "response_encodings": dict(encodings),
            "http_versions": dict(versions),
            "incomplete_response_bodies": incomplete_bodies,
        },
        "count_unit": "Saved connection configurations; multiple configurations can share an endpoint.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    print(json.dumps({"all_checks_passed": report["all_checks_passed"], "report": str(args.output),
                      "configurations": health["configurations"], "ever_responding": health["ever_responding"],
                      "latest_responding": health["latest_responding"], "network_checks": health["network_checks"]},default=str))
    if not report["all_checks_passed"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
