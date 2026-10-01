"""Discover Videos and Shorts through the previously stored range.

The initial scan saves one page. Later scans follow pagination as needed and
commit a complete tab scan atomically. Failed scans can restart from page one.
Publication times and recurring polling are separate steps.
"""

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import time

import httpx
import psycopg

from collect_subscribers import CLIENT_VERSION, ENDPOINT, ROOT, positive_int


TABS = {
    "video": {"title": "Videos", "params": "EgZ2aWRlb3PyBgQKAjoA"},
    "short": {"title": "Shorts", "params": "EgZzaG9ydHPyBgUKA5oBAA%3D%3D"},
}
VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}\Z")
MAX_BODY_BYTES = 131_072
BAD_RESULTS = {"blocked", "error", "unexpected_response", "sort_unverified", "database_error",
               "page_limit", "pagination_loop", "interrupted", "unavailable"}


class ResponseShapeError(ValueError):
    pass


class SortNotVerified(ResponseShapeError):
    pass


def field_mask(video_type, *, continuation=False):
    ids = {
        "video": "videoRenderer/videoId,lockupViewModel/contentId",
        "short": (
            "reelItemRenderer/videoId,"
            "shortsLockupViewModel/onTap/innertubeCommand/reelWatchEndpoint/videoId"
        ),
    }[video_type]
    items = (f"richItemRenderer/content({ids}),"
             "continuationItemRenderer/continuationEndpoint/continuationCommand/token")
    if continuation:
        return ",".join(
            f"{root}/appendContinuationItemsAction/continuationItems({items})"
            for root in ("onResponseReceivedActions", "onResponseReceivedEndpoints")
        ) + ",alerts"
    dropdown = (
        "tapCommand/innertubeCommand/showSheetCommand/panelLoadingStrategy/inlineContent/"
        "sheetViewModel/content/listViewModel/listItems/listItemViewModel(title/content,isSelected)"
    )
    sort = (
        f"header(chipBarViewModel/chips/chipViewModel(text,selected,{dropdown}),"
        "feedFilterChipBarRenderer/contents/chipCloudChipRenderer(text,isSelected))"
    )
    grid = f"richGridRenderer({sort},contents({items}))"
    return (
        "contents/twoColumnBrowseResultsRenderer/tabs/tabRenderer(title,selected,"
        f"content({grid},sectionListRenderer/contents("
        "itemSectionRenderer/contents/messageRenderer/text,"
        "channelOwnerEmptyStateRenderer/description))),alerts"
    )


def text_value(value):
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return value.get("simpleText") or "".join(
            run.get("text", "") for run in value.get("runs", [])
        )
    return ""


def dig(value, *keys):
    for key in keys:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def parse_items(items, channel_id, video_type):
    if not isinstance(items, list):
        raise ResponseShapeError("Missing video items")
    videos, seen, token = [], set(), None
    for item in items:
        continuation = item.get("continuationItemRenderer")
        if continuation is not None:
            value = dig(continuation, "continuationEndpoint", "continuationCommand", "token")
            if not isinstance(value, str) or not value or token is not None:
                raise ResponseShapeError("Missing or multiple pagination tokens")
            token = value
            continue
        card = dig(item, "richItemRenderer", "content")
        paths = {
            "video": [("videoRenderer", "videoId"), ("lockupViewModel", "contentId")],
            "short": [("reelItemRenderer", "videoId"),
                      ("shortsLockupViewModel", "onTap", "innertubeCommand",
                       "reelWatchEndpoint", "videoId")],
        }[video_type]
        ids = [value for path in paths if (value := dig(card, *path)) is not None]
        if len(ids) != 1 or not isinstance(ids[0], str) or not VIDEO_ID.fullmatch(ids[0]):
            raise ResponseShapeError("Unknown or malformed video card")
        if ids[0] not in seen:
            seen.add(ids[0])
            videos.append({"video_id": ids[0], "channel_id": channel_id, "type": video_type})
    if not videos and token:
        raise ResponseShapeError("Pagination without any video cards")
    return videos, token


def parse_continuation(payload, channel_id, video_type):
    """Read only continuation items; ordering is inherited from the first page."""
    if not isinstance(payload, dict) or payload.get("alerts"):
        raise ResponseShapeError("Invalid or alerted continuation response")
    batches = []
    for root in ("onResponseReceivedActions", "onResponseReceivedEndpoints"):
        for action in payload.get(root, []):
            if "appendContinuationItemsAction" in action:
                batches.append(dig(action, "appendContinuationItemsAction", "continuationItems"))
    if len(batches) != 1:
        raise ResponseShapeError("Missing or ambiguous continuation items")
    videos, token = parse_items(batches[0], channel_id, video_type)
    return {"status": "ok" if videos else "empty", "videos": videos, "continuation": token,
            "latest_verified": True, "complete_tab": token is None, "alerts": []}


def parse_page(payload, channel_id, video_type):
    """Accept the requested tab when sorted by Latest or fully listed without sort controls."""
    if not isinstance(payload, dict):
        raise ResponseShapeError("Response is not an object")
    alerts = []
    for item in payload.get("alerts", []):
        alert = item.get("alertRenderer", {})
        alerts.append({"type": alert.get("type"), "text": text_value(alert.get("text"))})
    result = {"status": "ok", "videos": [], "continuation": None,
              "latest_verified": False, "complete_tab": False, "alerts": alerts}
    if any(alert["type"] == "ERROR" for alert in alerts):
        return {**result, "status": "unavailable"}

    tabs = dig(payload, "contents", "twoColumnBrowseResultsRenderer", "tabs")
    if not isinstance(tabs, list) or not tabs:
        raise ResponseShapeError("Missing channel tabs")
    tabs = [item["tabRenderer"] for item in tabs if isinstance(item.get("tabRenderer"), dict)]
    matches = [tab for tab in tabs if tab.get("title") == TABS[video_type]["title"]]
    if not matches:
        if not any(tab.get("selected") is True and tab.get("title") for tab in tabs):
            raise ResponseShapeError("Missing requested tab and no selected alternative")
        return {**result, "status": "tab_absent"}
    if len(matches) != 1 or matches[0].get("selected") is not True:
        raise ResponseShapeError("Requested tab was not selected")
    content = matches[0].get("content", {})
    grid = content.get("richGridRenderer")
    if not isinstance(grid, dict):
        sections = dig(content, "sectionListRenderer", "contents") or []
        messages = [text_value(dig(item, "messageRenderer", "text"))
                    for section in sections
                    for item in (dig(section, "itemSectionRenderer", "contents") or [])]
        messages += [text_value(dig(section, "channelOwnerEmptyStateRenderer", "description"))
                     for section in sections]
        if any(re.search(r"(?:doesn't have any|has no|hasn't posted any) (?:videos|shorts|content)",
                         message, re.IGNORECASE) for message in messages):
            return {**result, "status": "empty", "complete_tab": True, "message": " ".join(messages)}
        raise ResponseShapeError("Missing video grid or recognized empty-tab message")

    header = grid.get("header", {})
    modern = dig(header, "chipBarViewModel", "chips") or []
    legacy = dig(header, "feedFilterChipBarRenderer", "contents") or []
    chips = [item.get("chipViewModel", {}) for item in modern]
    chips += [item.get("chipCloudChipRenderer", {}) for item in legacy]
    selected = [text_value(chip.get("text")) for chip in chips
                if chip.get("selected", chip.get("isSelected")) is True]
    for chip in chips:
        options = dig(chip, "tapCommand", "innertubeCommand", "showSheetCommand",
                      "panelLoadingStrategy", "inlineContent", "sheetViewModel", "content",
                      "listViewModel", "listItems") or []
        selected.extend(dig(option, "listItemViewModel", "title", "content")
                        for option in options
                        if dig(option, "listItemViewModel", "isSelected") is True)
    result["videos"], result["continuation"] = parse_items(grid.get("contents"), channel_id, video_type)
    result["complete_tab"] = result["continuation"] is None
    if selected == ["Latest"]:
        result["latest_verified"] = True
    elif "header" not in grid and result["complete_tab"]:
        # YouTube omits sort controls on some small tabs. All cards have been
        # parsed and no continuation exists, so discovery does not depend on order.
        pass
    else:
        raise SortNotVerified(f"Latest ordering not verified; selected sort: {selected}")
    if not result["videos"]:
        if result["continuation"]:
            raise ResponseShapeError("Pagination without any video cards")
        result["status"] = "empty"
    return result


async def fetch_page(client, channel_id, video_type, client_version=CLIENT_VERSION, retries=2,
                     *, continuation=None):
    payload = {"context": {"client": {"clientName": "WEB", "clientVersion": client_version, "hl": "en"}}}
    if continuation is None:
        payload.update(browseId=channel_id, params=TABS[video_type]["params"])
    else:
        payload["continuation"] = continuation
    body = json.dumps(payload, separators=(",", ":")).encode()
    result = {"channel_id": channel_id, "type": video_type, "status": "error",
              "videos": [], "continuation": None, "latest_verified": False, "complete_tab": False,
              "attempts": 0, "request_body_bytes": 0,
              "response_body_bytes": 0, "decoded_body_bytes": 0}
    started = time.monotonic()
    for attempt in range(retries + 1):
        result["attempts"] += 1
        result["request_body_bytes"] += len(body)
        raw = b""
        try:
            async with client.stream("POST", ENDPOINT,
                                     params={"prettyPrint": "false", "fields": field_mask(
                                         video_type, continuation=continuation is not None)},
                                     content=body) as response:
                result["http_status"] = response.status_code
                result["http_version"] = response.http_version
                result["content_encoding"] = response.headers.get("content-encoding")
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
                raw = b"".join(chunks)
                if response.status_code in (400, 401, 403, 429):
                    result.update(status="blocked", error=f"HTTP {response.status_code}",
                                  retry_after=response.headers.get("retry-after"))
                    break
                if response.status_code >= 500 and attempt < retries:
                    await asyncio.sleep(2**attempt)
                    continue
                response.raise_for_status()
                parser = parse_page if continuation is None else parse_continuation
                result.update(parser(json.loads(raw), channel_id, video_type))
                result.pop("error", None)
                break
        except SortNotVerified as exc:
            result.update(status="sort_unverified", error=str(exc))
            if raw:
                result["response_sample"] = raw[:8192].decode("utf-8", errors="replace")
            break
        except (ResponseShapeError, json.JSONDecodeError, UnicodeDecodeError,
                TypeError, AttributeError) as exc:
            result.update(status="unexpected_response", error=str(exc))
            if raw:
                result["response_sample"] = raw[:8192].decode("utf-8", errors="replace")
            break
        except httpx.HTTPError as exc:
            result["error"] = type(exc).__name__
            if isinstance(exc, httpx.TransportError) and attempt < retries:
                await asyncio.sleep(2**attempt)
                continue
            break
    result["seconds"] = round(time.monotonic() - started, 4)
    return result


def open_database(*, autocommit=False):
    dsn = os.environ.get("MEDIA_DATABASE_URL")
    return psycopg.connect(dsn, autocommit=autocommit) if dsn else psycopg.connect(
        host=str(ROOT / ".local/postgres/socket"), port=5432, user="mahmud", dbname="media",
        autocommit=autocommit,
    )


def select_channels(args):
    with open_database() as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        if args.channel_id:
            requested = list(dict.fromkeys(args.channel_id))
            found = {row[0] for row in conn.execute(
                "SELECT channel_id FROM public.channels WHERE channel_id = ANY(%s)", (requested,)
            )}
            if found != set(requested):
                raise ValueError("Requested channel ID is not in the channels table")
            return requested
        return [row[0] for row in conn.execute(
            "SELECT channel_id FROM public.channels ORDER BY channel_id LIMIT %s", (args.limit,)
        )]


def select_tabs(channels, *, initial_only=False):
    jobs = [(channel_id, video_type) for channel_id in channels for video_type in TABS]
    if initial_only and channels:
        with open_database() as conn:
            initialized = set(conn.execute(
                "SELECT channel_id,type FROM public.channel_scan_state WHERE channel_id=ANY(%s)",
                (channels,),
            ))
        jobs = [job for job in jobs if job not in initialized]
    return jobs


def save_videos(conn, result):
    """Insert validated IDs without committing the caller's transaction."""
    if result["status"] != "ok" or not result["videos"]:
        return []
    ids = [video["video_id"] for video in result["videos"]]
    rows = conn.execute(
        """INSERT INTO public.videos (video_id, channel_id, type)
           SELECT unnest(%s::text[]), %s, %s
           ON CONFLICT (video_id) DO NOTHING
           RETURNING video_id""",
        (ids, result["channel_id"], result["type"]),
    ).fetchall()
    return [row[0] for row in rows]


async def scan_tab(client, conn, channel_id, video_type, client_version=CLIENT_VERSION,
                   retries=2, max_pages=100, stop=None):
    """Buffer a complete scan so partial results cannot become a stopping point."""
    result = {
        "channel_id": channel_id, "type": video_type, "status": "error", "videos": [],
        "continuation": None, "latest_verified": False, "complete_tab": False,
        "scan_complete": False, "stop_reason": None, "initialized_before": False,
        "attempts": 0, "request_body_bytes": 0, "response_body_bytes": 0, "decoded_body_bytes": 0,
        "pages_fetched": 0, "pages": [], "inserted_video_ids": [],
        "videos_inserted": 0, "videos_already_present": 0,
    }
    started = time.monotonic()
    try:
        result["initialized_before"] = conn.execute(
            "SELECT EXISTS (SELECT 1 FROM public.channel_scan_state WHERE channel_id=%s AND type=%s)",
            (channel_id, video_type),
        ).fetchone()[0]
        buffered, seen_tokens, seen_pages = {}, set(), set()
        continuation = None
        for page_number in range(1, max_pages + 1):
            if stop is not None and stop.is_set():
                result.update(status="interrupted", error="Collection stopped before the scan finished")
                return result
            page = await fetch_page(client, channel_id, video_type, client_version, retries,
                                    continuation=continuation)
            result["pages_fetched"] += 1
            for key in ("attempts", "request_body_bytes", "response_body_bytes", "decoded_body_bytes"):
                result[key] += page[key]
            detail = {"page": page_number, "status": page["status"], "video_ids": len(page["videos"]),
                      "http_status": page.get("http_status"), "http_version": page.get("http_version"),
                      "content_encoding": page.get("content_encoding"), "attempts": page["attempts"],
                      "latest_verified": page["latest_verified"],
                      "response_body_bytes": page["response_body_bytes"]}
            result["pages"].append(detail)
            for key in ("http_status", "http_version", "content_encoding", "continuation", "complete_tab"):
                result[key] = page.get(key)
            if page_number == 1:
                result["latest_verified"] = page["latest_verified"]
            if page["status"] not in ("ok", "empty", "tab_absent"):
                result["status"] = page["status"]
                for key in ("error", "alerts", "response_sample", "retry_after"):
                    if key in page:
                        result[key] = page[key]
                return result

            ids = [video["video_id"] for video in page["videos"]]
            known = {row[0] for row in conn.execute(
                """SELECT video_id FROM public.videos
                   WHERE channel_id=%s AND type=%s AND video_id=ANY(%s)""",
                (channel_id, video_type, ids),
            )} if ids else set()
            detail["previously_stored_ids"] = len(known)
            for video in page["videos"]:
                buffered.setdefault(video["video_id"], video)
            result["videos"] = list(buffered.values())

            # Process the entire page. A known item at its start is insufficient:
            # new items can still follow it, including on the next page.
            if not result["initialized_before"]:
                result["stop_reason"] = "initial_page"
            elif page["continuation"] is None:
                result["stop_reason"] = "end_of_tab"
            elif ids and ids[-1] in known:
                result["stop_reason"] = "known_range"
            if result["stop_reason"]:
                result["status"] = "ok" if buffered else page["status"]
                break

            signature = tuple(ids)
            if page["continuation"] in seen_tokens or signature in seen_pages:
                result.update(status="pagination_loop", error="Pagination repeated a token or page")
                return result
            seen_tokens.add(page["continuation"])
            seen_pages.add(signature)
            continuation = page["continuation"]
        else:
            result.update(status="page_limit", error=f"Scan needs more than {max_pages} pages; nothing saved")
            return result

        # No transaction is held during HTTP requests. IDs and the first-scan
        # marker commit together, or both roll back if either database write fails.
        with conn.transaction():
            inserted = save_videos(conn, result)
            if not result["initialized_before"]:
                conn.execute(
                    """INSERT INTO public.channel_scan_state (channel_id, type) VALUES (%s,%s)
                       ON CONFLICT (channel_id, type) DO NOTHING""",
                    (channel_id, video_type),
                )
        result.update(inserted_video_ids=inserted, videos_inserted=len(inserted),
                      videos_already_present=len(result["videos"]) - len(inserted), scan_complete=True)
    except psycopg.Error as exc:
        result.update(status="database_error", error=type(exc).__name__)
    finally:
        result["seconds"] = round(time.monotonic() - started, 4)
    return result


async def collect(args):
    channels = select_channels(args)
    selected_tabs = select_tabs(channels, initial_only=args.initial_only)
    selected_channels = len({channel_id for channel_id, _ in selected_tabs})
    out = args.output or ROOT / "outputs" / (
        "discovery-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    )
    out.mkdir(parents=True, exist_ok=False)
    summary = {
        "started_at": datetime.now(timezone.utc).isoformat(), "channels_selected": selected_channels,
        "channels_considered": len(channels), "initial_only": args.initial_only,
        "tabs_skipped_initialized": 2 * len(channels) - len(selected_tabs),
        "tabs_planned": len(selected_tabs), "tabs_completed": 0, "video_ids_returned": 0,
        "scans_completed": 0, "pages_fetched": 0, "max_pages_per_tab": args.max_pages,
        "http_attempts": 0, "request_body_bytes": 0, "response_body_bytes": 0,
        "decoded_body_bytes": 0, "concurrency": args.concurrency, "client_version": args.client_version,
        "proxy_used": bool(os.environ.get("MEDIA_PROXY_URL")), "latest_verified_pages": 0,
        "complete_tabs_without_sort": 0,
        "stopped_reason": None, "videos_inserted": 0, "videos_already_present": 0,
        "output_directory": str(out.resolve()),
        "scope": "Initial scan saves one page; subsequent scans paginate through the stored range",
        "traffic_scope": "HTTP bodies only; excludes headers, TLS and TCP/IP overhead",
        "request_byte_scope": "Constructed request bodies for all attempts, including transport failures",
    }
    outcomes, versions, encodings, types, inserted_types, stop_reasons = (
        Counter(), Counter(), Counter(), Counter(), Counter(), Counter()
    )
    jobs = iter(selected_tabs)
    stop = asyncio.Event()
    malformed_streak = 0
    started = last_report = time.monotonic()
    print(json.dumps({"event": "started", "channels": selected_channels, "tabs": len(selected_tabs)}), flush=True)
    try:
        async with httpx.AsyncClient(
            http2=True, trust_env=False, follow_redirects=False, proxy=os.environ.get("MEDIA_PROXY_URL"),
            headers={"Content-Type": "application/json", "Accept-Encoding": "gzip"},
            timeout=httpx.Timeout(20, connect=10),
            limits=httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency),
        ) as client:
            with open_database(autocommit=True) as conn, (out / "results.jsonl").open("x", buffering=1) as output:
                async def worker():
                    nonlocal malformed_streak, last_report
                    for channel_id, video_type in jobs:
                        if stop.is_set():
                            break
                        result = await scan_tab(client, conn, channel_id, video_type,
                                                args.client_version, args.retries, args.max_pages, stop)
                        output.write(json.dumps(result, separators=(",", ":")) + "\n")
                        summary["tabs_completed"] += 1
                        summary["scans_completed"] += int(result["scan_complete"])
                        summary["pages_fetched"] += result["pages_fetched"]
                        summary["video_ids_returned"] += len(result["videos"])
                        summary["videos_inserted"] += result["videos_inserted"]
                        summary["videos_already_present"] += result["videos_already_present"]
                        summary["latest_verified_pages"] += sum(int(p["latest_verified"]) for p in result["pages"])
                        summary["complete_tabs_without_sort"] += int(
                            result["complete_tab"] and not result["latest_verified"]
                        )
                        summary["http_attempts"] += result["attempts"]
                        for key in ("request_body_bytes", "response_body_bytes", "decoded_body_bytes"):
                            summary[key] += result[key]
                        outcomes[result["status"]] += 1
                        types[video_type] += len(result["videos"])
                        inserted_types[video_type] += result["videos_inserted"]
                        stop_reasons[result["stop_reason"] or "incomplete"] += 1
                        for page in result["pages"]:
                            versions[page.get("http_version") or "no_response"] += 1
                            encodings[page.get("content_encoding") or "identity"] += 1
                        malformed_streak = (malformed_streak + 1
                                            if result["status"] in ("unexpected_response", "sort_unverified")
                                            else 0)
                        if result["status"] in ("blocked", "database_error") or malformed_streak >= 5:
                            stop.set()
                            summary["stopped_reason"] = result.get("error", result["status"])
                        now = time.monotonic()
                        if now - last_report >= 10:
                            print(json.dumps({"event": "progress", "tabs": summary["tabs_completed"],
                                              "inserted": summary["videos_inserted"],
                                              "already_present": summary["videos_already_present"],
                                              "outcomes": dict(outcomes)}), flush=True)
                            last_report = now
                async with asyncio.TaskGroup() as group:
                    for _ in range(args.concurrency):
                        group.create_task(worker())
    finally:
        summary.update(finished_at=datetime.now(timezone.utc).isoformat(),
                       seconds=round(time.monotonic() - started, 3), outcomes=dict(outcomes),
                       http_versions=dict(versions), content_encodings=dict(encodings),
                       video_types=dict(types), inserted_video_types=dict(inserted_types),
                       stop_reasons=dict(stop_reasons),
                       tabs_unprocessed=summary["tabs_planned"] - summary["tabs_completed"])
        (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"event": "finished", **summary}), flush=True)
    return 2 if summary["tabs_unprocessed"] or any(outcomes[s] for s in BAD_RESULTS) else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--limit", type=positive_int, default=100)
    selection.add_argument("--channel-id", action="append", help="An existing channel ID; repeat for more")
    parser.add_argument("--initial-only", action="store_true",
                        help="Skip completed initial tab scans; resume an unfinished baseline")
    parser.add_argument("--concurrency", type=positive_int, default=4)
    parser.add_argument("--retries", type=int, choices=range(4), default=2)
    parser.add_argument("--max-pages", type=positive_int, default=100,
                        help="Maximum pages per tab; reaching the limit before overlap saves nothing")
    parser.add_argument("--client-version", default=os.environ.get("YOUTUBE_CLIENT_VERSION", CLIENT_VERSION))
    parser.add_argument("--output", type=Path, help="New directory for results and summary")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(collect(args)))


if __name__ == "__main__":
    main()
