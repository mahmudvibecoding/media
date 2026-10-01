"""Collect public, rounded YouTube subscriber counts with small JSON responses."""

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import time

import httpx
import psycopg

ROOT = Path(__file__).resolve().parent
ENDPOINT = "https://www.youtube.com/youtubei/v1/browse"
FIELD_MASK = (
    "header(pageHeaderRenderer/content/pageHeaderViewModel/metadata/"
    "contentMetadataViewModel/metadataRows/metadataParts/text/content,"
    "c4TabbedHeaderRenderer/subscriberCountText)"
)
CLIENT_VERSION = "2.20260925.01.00"
COUNT_PATTERN = re.compile(
    r"((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)\s*([KMB]?)\s+subscribers?",
    re.IGNORECASE,
)
MAX_BODY_BYTES = 65_536


class ResponseShapeError(ValueError):
    pass


def parse_count(text):
    text = " ".join(text.replace("\u200e", "").replace("\u200f", "").split())
    if text.lower() == "no subscribers":
        return 0
    match = COUNT_PATTERN.fullmatch(text)
    if not match:
        raise ResponseShapeError("Unrecognized subscriber count text")
    number, suffix = match.groups()
    value = Decimal(number.replace(",", "")) * {
        "": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000
    }[suffix.upper()]
    if value != value.to_integral_value() or not 0 <= value <= 2**63 - 1:
        raise ResponseShapeError("Subscriber count is outside the supported range")
    return int(value)


def extract_count(payload):
    """Read only channel-header fields; absent counts are never assumed zero."""
    header = payload.get("header") if isinstance(payload, dict) else None
    if not isinstance(header, dict) or not header:
        raise ResponseShapeError("Missing channel header")
    texts = []
    modern = header.get("pageHeaderRenderer")
    legacy = header.get("c4TabbedHeaderRenderer")
    if modern is None and legacy is None:
        raise ResponseShapeError("Unknown channel header format")
    if modern is not None:
        try:
            rows = modern["content"]["pageHeaderViewModel"]["metadata"][
                "contentMetadataViewModel"
            ]["metadataRows"]
            for row in rows:
                for part in row.get("metadataParts", []):
                    text = part.get("text", {}).get("content")
                    if isinstance(text, str):
                        texts.append(text)
        except (KeyError, TypeError, AttributeError) as exc:
            raise ResponseShapeError("Incomplete channel header metadata") from exc
    if legacy is not None:
        value = legacy.get("subscriberCountText", {})
        if "simpleText" in value:
            texts.append(value["simpleText"])
        elif "runs" in value:
            texts.append("".join(run.get("text", "") for run in value["runs"]))
    counts = {parse_count(text) for text in texts if "subscriber" in text.lower()}
    if len(counts) > 1:
        raise ResponseShapeError("Conflicting subscriber counts")
    return next(iter(counts)) if counts else None


async def fetch_count(client, channel_id, client_version=CLIENT_VERSION, retries=2):
    body = json.dumps({
        "context": {"client": {
            "clientName": "WEB", "clientVersion": client_version, "hl": "en"
        }},
        "browseId": channel_id,
    }, separators=(",", ":")).encode()
    result = {
        "channel_id": channel_id, "status": "error", "subscriber_count": None,
        "attempts": 0, "request_body_bytes": 0, "response_body_bytes": 0,
        "decoded_body_bytes": 0,
    }
    started = time.monotonic()
    for attempt in range(retries + 1):
        result["attempts"] += 1
        result["request_body_bytes"] += len(body)
        try:
            async with client.stream(
                "POST", ENDPOINT,
                params={"prettyPrint": "false", "fields": FIELD_MASK},
                content=body,
            ) as response:
                result["http_status"] = response.status_code
                result["http_version"] = response.http_version
                result["content_encoding"] = response.headers.get("content-encoding")
                stream = response.extensions.get("network_stream")
                if stream is not None:
                    result["local_address"] = stream.get_extra_info("client_addr")
                    result["remote_address"] = stream.get_extra_info("server_addr")
                chunks = []
                size = 0
                try:
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_BODY_BYTES:
                            raise ResponseShapeError("Response exceeded the expected size")
                        chunks.append(chunk)
                finally:
                    result["response_body_bytes"] += response.num_bytes_downloaded
                    result["decoded_body_bytes"] += size
                if response.status_code in (400, 401, 403, 429):
                    result["status"] = "blocked"
                    result["error"] = f"HTTP {response.status_code}; collection stopped"
                    result["retry_after"] = response.headers.get("retry-after")
                    break
                if response.status_code == 404:
                    result["status"] = "unavailable"
                    break
                if response.status_code >= 500 and attempt < retries:
                    await asyncio.sleep(2**attempt)
                    continue
                response.raise_for_status()
                payload = json.loads(b"".join(chunks))
                count = extract_count(payload)
                result["subscriber_count"] = count
                result["status"] = "ok" if count is not None else "missing_count"
                result.pop("error", None)
                break
        except (ResponseShapeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            result["status"] = "unexpected_response"
            result["error"] = str(exc)
            break
        except httpx.HTTPError as exc:
            result["error"] = type(exc).__name__
            if isinstance(exc, httpx.TransportError) and attempt < retries:
                await asyncio.sleep(2**attempt)
                continue
            break
    result["seconds"] = round(time.monotonic() - started, 4)
    return result


def save_count(conn, result):
    count = result.get("subscriber_count")
    if result["status"] != "ok" or type(count) is not int or count < 0:
        return 0
    return conn.execute(
        "UPDATE public.channels SET subscriber_count = %s WHERE channel_id = %s",
        (count, result["channel_id"]),
    ).rowcount


async def collect(args):
    dsn = os.environ.get("MEDIA_DATABASE_URL")
    connection = psycopg.connect(dsn, autocommit=True) if dsn else psycopg.connect(
        host=str(ROOT / ".local/postgres/socket"), port=5432,
        user="mahmud", dbname="media", autocommit=True,
    )
    with connection as conn:
        locked = conn.execute(
            "SELECT pg_try_advisory_lock(hashtext('media.subscriber-count'))"
        ).fetchone()[0]
        if not locked:
            raise RuntimeError("Another subscriber collector is running for this database")
        query = "SELECT channel_id FROM public.channels"
        if not args.refresh:
            query += " WHERE subscriber_count IS NULL"
        query += " ORDER BY channel_id"
        if not args.all:
            query += " LIMIT %s"
        channels = [row[0] for row in conn.execute(
            query, None if args.all else (args.limit,)
        ).fetchall()]
        run_dir = args.output or ROOT / "outputs" / (
            "subscribers-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        )
        run_dir.mkdir(parents=True, exist_ok=False)
        summary = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "selected": len(channels), "completed": 0, "saved": 0,
            "concurrency": args.concurrency, "client_version": args.client_version,
            "refresh": args.refresh, "http_attempts": 0,
            "request_body_bytes": 0, "response_body_bytes": 0,
            "decoded_body_bytes": 0, "stopped_reason": None,
            "traffic_scope": "HTTP bodies only; excludes headers, TLS and TCP/IP overhead",
            "request_byte_scope": "Constructed bodies for all attempts, including failed connections",
            "output_directory": str(run_dir),
        }
        counts = Counter()
        versions = Counter()
        stop = asyncio.Event()
        iterator = iter(channels)
        queue = asyncio.Queue(maxsize=args.concurrency * 2)
        started = time.monotonic()
        last_report = started
        malformed_streak = 0
        print(json.dumps({"event": "started", "selected": len(channels),
                          "output_directory": str(run_dir)}), flush=True)
        try:
            async with httpx.AsyncClient(
                http2=True, trust_env=False, follow_redirects=False,
                proxy=os.environ.get("MEDIA_PROXY_URL"),
                headers={"Content-Type": "application/json", "Accept-Encoding": "gzip"},
                timeout=httpx.Timeout(20, connect=10),
                limits=httpx.Limits(max_connections=args.concurrency,
                                    max_keepalive_connections=args.concurrency),
            ) as client:
                async def worker():
                    for channel_id in iterator:
                        if stop.is_set():
                            break
                        result = await fetch_count(
                            client, channel_id, args.client_version, args.retries
                        )
                        if result["status"] == "blocked":
                            stop.set()
                        await queue.put(result)
                    await queue.put(None)

                with (run_dir / "results.jsonl").open("x", buffering=1) as output:
                    async with asyncio.TaskGroup() as group:
                        for _ in range(args.concurrency):
                            group.create_task(worker())
                        finished = 0
                        while finished < args.concurrency:
                            result = await queue.get()
                            if result is None:
                                finished += 1
                                continue
                            summary["saved"] += save_count(conn, result)
                            output.write(json.dumps(result, separators=(",", ":")) + "\n")
                            summary["completed"] += 1
                            counts[result["status"]] += 1
                            versions[result.get("http_version", "no_response")] += 1
                            summary["http_attempts"] += result["attempts"]
                            for key in ("request_body_bytes", "response_body_bytes", "decoded_body_bytes"):
                                summary[key] += result[key]
                            malformed_streak = (
                                malformed_streak + 1 if result["status"] == "unexpected_response" else 0
                            )
                            if result["status"] == "blocked" or malformed_streak >= 5:
                                stop.set()
                                summary["stopped_reason"] = result.get("error", result["status"])
                            now = time.monotonic()
                            if now - last_report >= 10 or summary["completed"] == len(channels):
                                print(json.dumps({"event": "progress", "completed": summary["completed"],
                                                  "selected": len(channels), "saved": summary["saved"],
                                                  "outcomes": dict(counts),
                                                  "seconds": round(now-started, 2)}), flush=True)
                                last_report = now
        finally:
            summary["finished_at"] = datetime.now(timezone.utc).isoformat()
            summary["seconds"] = round(time.monotonic() - started, 3)
            summary["outcomes"] = dict(counts)
            summary["http_versions"] = dict(versions)
            summary["unprocessed"] = len(channels) - summary["completed"]
            (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(json.dumps({"event": "finished", **summary}), flush=True)
        return 2 if summary["stopped_reason"] else 0


def positive_int(value):
    value = int(value)
    if value < 1:
        raise argparse.ArgumentTypeError("Must be at least 1")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--limit", type=positive_int, default=100)
    selection.add_argument("--all", action="store_true", help="Process all selected channels")
    parser.add_argument("--refresh", action="store_true", help="Include channels that already have counts")
    parser.add_argument("--concurrency", type=positive_int, default=8)
    parser.add_argument("--retries", type=int, choices=range(4), default=2)
    parser.add_argument("--client-version", default=os.environ.get("YOUTUBE_CLIENT_VERSION", CLIENT_VERSION))
    parser.add_argument("--output", type=Path, help="New directory for results and summary")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(collect(args)))


if __name__ == "__main__":
    main()
