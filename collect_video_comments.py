"""Collect one video's top-level comments in Newest order through /next.

A temporary SQLite buffer holds the fixed pre-scan history and new pages. Only
a completed scan is imported into PostgreSQL. Retrying a failed scan therefore
starts from the same saved history; durable page resume belongs to the bulk queue.
"""

import argparse
import asyncio
import base64
from collections.abc import Mapping
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sqlite3
import ssl
import tempfile
import time

import httpx
import psycopg

from collect_subscribers import CLIENT_VERSION, ROOT, positive_int
from collect_video_metadata import RequestTrace, local_worker_error, request_error_label
from discover_videos import VIDEO_ID, dig, open_database
from proxy_statistics import AttemptOutcome, proxy_connection_error


ENDPOINT = "https://www.youtube.com/youtubei/v1/next"
MAX_BODY_BYTES = 2 * 1024 * 1024
FIELDS = ("video_id", "comment_id", "text", "author_channel_id", "author_name", "is_pinned")
SUCCESS = frozenset(("ok", "empty", "disabled"))
COMMENT_TARGET = "comments-section"
CHANNEL_ID = re.compile(r"UC[A-Za-z0-9_-]{22}\Z")

# Exclude reply branches, dates, likes, avatars, and the large emoji picker.
HEADER_FIELDS = (
    "commentsHeaderRenderer(countText,commentsCount,"
    "sortMenu/sortFilterSubMenuRenderer/subMenuItems(title,selected,"
    "serviceEndpoint/continuationCommand/token))"
)
ITEM_FIELDS = (
    HEADER_FIELDS + ",messageRenderer/text,"
    "commentThreadRenderer(renderingPriority,"
    "commentViewModel/commentViewModel(commentKey,commentId,pinnedText),"
    "comment/commentRenderer(commentId,contentText,authorText,"
    "authorEndpoint/browseEndpoint/browseId,pinnedCommentBadge)),"
    "continuationItemRenderer/continuationEndpoint/continuationCommand/token"
)
COMMENT_FIELD_MASK = ",".join(
    f"{root}/{action}(targetId,{('slot,' if action == 'reloadContinuationItemsCommand' else '')}continuationItems({ITEM_FIELDS}))"
    for root, action in (
        ("onResponseReceivedEndpoints", "reloadContinuationItemsCommand"),
        ("onResponseReceivedEndpoints", "appendContinuationItemsAction"),
    )
) + (
    ",frameworkUpdates/entityBatchUpdate/mutations(entityKey,"
    "payload/commentEntityPayload(key,properties(commentId,content/content,replyLevel),"
    "author(channelId,displayName))),alerts,currentVideoEndpoint/watchEndpoint/videoId"
)
WATCH_FIELD_MASK = (
    "currentVideoEndpoint/watchEndpoint/videoId,"
    "contents/twoColumnWatchNextResults/results/results/contents/"
    "itemSectionRenderer(targetId,contents(messageRenderer/text,backgroundPromoRenderer/title)),"
    "engagementPanels/engagementPanelSectionListRenderer(targetId,"
    "content/sectionListRenderer/contents/itemSectionRenderer(targetId,"
    "contents/messageRenderer/text)),alerts"
)


class CommentError(ValueError):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def shape(message):
    raise CommentError("unexpected_response", message)


def text_value(value):
    """Preserve text exactly, including empty text and line breaks."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if isinstance(value.get("simpleText"), str):
            return value["simpleText"]
        runs = value.get("runs")
        if isinstance(runs, list) and all(isinstance(r, dict) and isinstance(r.get("text"), str) for r in runs):
            return "".join(r["text"] for r in runs)
    return None


def initial_continuation(video_id):
    if not VIDEO_ID.fullmatch(video_id):
        raise ValueError("Expected an 11-character video ID")
    # WEB's Newest comments continuation. Verify the returned sort header
    # before consuming this first page.
    raw = f'\x12\r\x12\x0b{video_id}\x18\x062%"\x11"\x0b{video_id}0\x01x\x02B\x10comments-section'
    return base64.b64encode(raw.encode()).decode()


def check_payload(payload, video_id):
    if not isinstance(payload, dict):
        shape("Response is not an object")
    if payload.get("error") or payload.get("alerts"):
        shape("API error or alert in comment response")
    identity = dig(payload, "currentVideoEndpoint", "watchEndpoint", "videoId")
    if identity is not None and identity != video_id:
        shape("Response belongs to another video")


def comment_items(payload, *, selecting_sort=False, allow_omitted_append=False):
    """Read direct comments-section actions only, never nested reply actions."""
    items, found, bodies, omitted_bodies = [], False, 0, 0
    for root in ("onResponseReceivedEndpoints", "onResponseReceivedActions"):
        actions = payload.get(root, [])
        if not isinstance(actions, list):
            shape("Invalid comment actions")
        for action in actions:
            if not isinstance(action, dict):
                shape("Invalid comment action")
            for key in ("reloadContinuationItemsCommand", "appendContinuationItemsAction"):
                batch = action.get(key)
                if not isinstance(batch, dict) or batch.get("targetId") != COMMENT_TARGET:
                    continue
                found = True
                values = batch.get("continuationItems")
                if ("continuationItems" not in batch and key == "reloadContinuationItemsCommand"
                        and batch.get("slot") == "RELOAD_CONTINUATION_SLOT_BODY"):
                    # WEB omits empty repeated fields, including when Top
                    # hides comments that are returned by Newest.
                    omitted_bodies += 1
                    values = []
                elif (allow_omitted_append and key == "appendContinuationItemsAction"
                      and set(batch) == {"targetId"}):
                    # A verified continuation can end with a scoped append
                    # action whose empty item list is omitted. Initial requests
                    # still require their Newest header or terminal message.
                    values = []
                if not isinstance(values, list) or not all(isinstance(v, dict) for v in values):
                    shape("Missing comment items")
                if batch.get("slot") != "RELOAD_CONTINUATION_SLOT_HEADER":
                    bodies += 1
                items.extend(values)
    if bodies > 1:
        shape("Multiple comment body batches")
    if omitted_bodies:
        header = header_info(items)
        can_select = (header and selecting_sort and isinstance(header["token"], str) and header["token"])
        if (header is None or not (header["empty"] or can_select)
                or any("commentsHeaderRenderer" not in item for item in items)):
            shape("Missing comment items")
    return items, found, bodies > 0


def header_info(items):
    headers = [i["commentsHeaderRenderer"] for i in items if "commentsHeaderRenderer" in i]
    if len(headers) > 1:
        shape("Multiple comment headers")
    if not headers:
        return None
    header = headers[0]
    if not isinstance(header, dict):
        shape("Invalid comment header")
    menu = dig(header, "sortMenu", "sortFilterSubMenuRenderer", "subMenuItems")
    newest, selected = [], []
    if menu is not None:
        if not isinstance(menu, list) or not all(isinstance(i, dict) for i in menu):
            shape("Invalid comment sort menu")
        newest = [i for i in menu if str(i.get("title", "")).strip().lower() in ("newest", "newest first")]
        selected = [i for i in menu if i.get("selected") is True]
        if len(newest) != 1 or len(selected) != 1:
            raise CommentError("sort_unverified", "Missing or ambiguous Newest sort option")
    count = text_value(header.get("commentsCount")) or text_value(header.get("countText"))
    empty = count is not None and count.strip().lower() in ("0", "0 comments", "no comments")
    return {"token": dig(newest[0], "serviceEndpoint", "continuationCommand", "token") if newest else None,
            "newest_selected": bool(newest and selected and newest[0] is selected[0]), "empty": empty}


def message_status(items):
    messages = [text_value(dig(i, "messageRenderer", "text")) for i in items if "messageRenderer" in i]
    states = set()
    for message in messages:
        value = " ".join((message or "").lower().split()).rstrip(".! ")
        value = re.sub(r"\.\s*learn more$", "", value)
        if value in ("comments are turned off", "comments are turned off for this video", "comments are disabled for this video"):
            states.add("disabled")
        elif value in ("no comments yet", "be the first to comment", "no comments"):
            states.add("empty")
        else:
            shape("Unrecognized comment message")
    if len(states) > 1:
        shape("Conflicting comment messages")
    return next(iter(states), None)


def nullable_string(value, field):
    if value is not None and not isinstance(value, str):
        shape(f"Invalid {field}")
    return value or None


def pinned_state(thread, renderer, *, modern):
    field = "pinnedText" if modern else "pinnedCommentBadge"
    value = renderer.get(field)
    if value:
        return True if isinstance(value, str if modern else dict) else None
    if field in renderer:
        return None
    priority = thread.get("renderingPriority")
    if priority == "RENDERING_PRIORITY_PINNED_COMMENT":
        return True
    if priority not in (None, "RENDERING_PRIORITY_UNKNOWN", "RENDERING_PRIORITY_NORMAL"):
        return None
    # Absence of the pin marker is meaningful in these recognized renderers.
    return False


def parse_page(payload, video_id):
    check_payload(payload, video_id)
    items, found, has_body = comment_items(payload, allow_omitted_append=True)
    header = header_info(items)
    if header is not None and not header["newest_selected"] and not header["empty"]:
        raise CommentError("sort_unverified", "Comment page is not sorted by Newest")
    if not found or not has_body:
        shape("Missing comment body")
    mutations = dig(payload, "frameworkUpdates", "entityBatchUpdate", "mutations") or []
    if not isinstance(mutations, list):
        shape("Invalid comment entities")
    entities = {}
    for mutation in mutations:
        entity = dig(mutation, "payload", "commentEntityPayload")
        if entity is None:
            continue
        key = mutation.get("entityKey")
        if not isinstance(entity, dict) or not isinstance(key, str) or not key or key in entities:
            shape("Invalid or duplicate comment entity key")
        if entity.get("key", key) != key:
            shape("Mismatched comment entity key")
        entities[key] = entity
    records, token = [], None
    state = message_status(items)
    for item in items:
        if "commentsHeaderRenderer" in item or "messageRenderer" in item:
            continue
        if "continuationItemRenderer" in item:
            value = dig(item, "continuationItemRenderer", "continuationEndpoint", "continuationCommand", "token")
            if token is not None or not isinstance(value, str) or not value:
                shape("Missing or multiple pagination tokens")
            token = value
            continue
        thread = item.get("commentThreadRenderer")
        if not isinstance(thread, dict):
            shape("Unknown comment item")
        modern = dig(thread, "commentViewModel", "commentViewModel")
        legacy = dig(thread, "comment", "commentRenderer")
        if isinstance(modern, dict) and legacy is None:
            key = modern.get("commentKey")
            entity = entities.get(key) if isinstance(key, str) else None
            if entity is None:
                shape("Missing entity for a top-level comment")
            props = entity.get("properties", {})
            if not isinstance(props, dict):
                shape("Invalid comment properties")
            cid = props.get("commentId")
            if modern.get("commentId", cid) != cid or props.get("replyLevel", 0) != 0:
                shape("Comment ID or top-level identity mismatch")
            text = dig(props, "content", "content")
            author_id, author_name = dig(entity, "author", "channelId"), dig(entity, "author", "displayName")
            pinned = pinned_state(thread, modern, modern=True)
        elif isinstance(legacy, dict) and modern is None:
            cid, text = legacy.get("commentId"), text_value(legacy.get("contentText"))
            author_id = dig(legacy, "authorEndpoint", "browseEndpoint", "browseId")
            author_name = text_value(legacy.get("authorText"))
            pinned = pinned_state(thread, legacy, modern=False)
        else:
            shape("Missing or ambiguous top-level comment renderer")
        if not isinstance(cid, str) or not cid or "\x00" in cid or not isinstance(text, str) or "\x00" in text:
            shape("Missing or invalid comment ID or text")
        author_id = nullable_string(author_id, "author channel ID")
        author_name = nullable_string(author_name, "author name")
        if author_id is not None and not CHANNEL_ID.fullmatch(author_id):
            shape("Invalid author channel ID")
        if author_name is not None and "\x00" in author_name:
            shape("Invalid author name")
        records.append(dict(zip(FIELDS, (video_id, cid, text, author_id, author_name, pinned))))
    if state and (records or token):
        shape("Comment message conflicts with page contents")
    if not records and token:
        shape("Pagination without top-level comments")
    if header and header["empty"] and records:
        shape("Zero comment header conflicts with page contents")
    return {"comments": records, "continuation": token, "state": state}


def watch_status(payload, video_id):
    """Use /next's watch response to diagnose a missing comment section."""
    check_payload(payload, video_id)
    main = dig(payload, "contents", "twoColumnWatchNextResults", "results", "results", "contents") or []
    sections = [i.get("itemSectionRenderer", {}) for i in main if isinstance(i, dict)]
    for section in sections:
        for item in section.get("contents", []):
            title = text_value(dig(item, "backgroundPromoRenderer", "title"))
            if title and title.strip().lower().rstrip(".! ") in (
                "this video isn't available anymore", "this video is unavailable", "video unavailable",
                "this video is private", "this video has been removed",
            ):
                return "unavailable"
    # A matching video ID is required before accepting disabled/empty comments.
    if dig(payload, "currentVideoEndpoint", "watchEndpoint", "videoId") != video_id:
        return None
    for panel in payload.get("engagementPanels", []):
        renderer = panel.get("engagementPanelSectionListRenderer", {})
        if renderer.get("targetId") != "engagement-panel-comments-section":
            continue
        sections.extend(i.get("itemSectionRenderer", {}) for i in
                        (dig(renderer, "content", "sectionListRenderer", "contents") or []))
    for section in sections:
        # WEB also places its disabled-comments message in an unlabelled main
        # item section. The matching watch video ID above scopes that message.
        if section.get("targetId") in (None, COMMENT_TARGET, "engagement-panel-comments-section"):
            state = message_status(section.get("contents", []))
            if state:
                return state
    return None


class CommentBuffer:
    """Disk-backed scan data and an immutable copy of the prior completed history."""
    def __init__(self, path=":memory:"):
        self.conn = sqlite3.connect(path)
        self.conn.executescript("""
            CREATE TABLE history (comment_id TEXT PRIMARY KEY, is_pinned INTEGER);
            CREATE TABLE comments (video_id TEXT NOT NULL, comment_id TEXT PRIMARY KEY,
                text TEXT NOT NULL, author_channel_id TEXT, author_name TEXT, is_pinned INTEGER);
        """)

    def close(self):
        self.conn.close()

    def load_history(self, rows):
        if isinstance(rows, Mapping):
            rows = rows.items()
        with self.conn:
            self.conn.executemany("INSERT INTO history VALUES (?,?)", rows)

    def add_page(self, comments):
        boundary = False
        before = self.count()
        # Check the entire page before deciding to stop. A pinned occurrence of
        # an ID anywhere in this scan excludes its later ordinary occurrence.
        pinned_on_page = {r["comment_id"] for r in comments if r["is_pinned"] is not False}
        with self.conn:
            for record in comments:
                cid = record["comment_id"]
                history = self.conn.execute("SELECT is_pinned FROM history WHERE comment_id=?", (cid,)).fetchone()
                previous = self.conn.execute("SELECT is_pinned FROM comments WHERE comment_id=?", (cid,)).fetchone()
                boundary |= (record["is_pinned"] is False and cid not in pinned_on_page
                             and history == (0,) and (previous is None or previous == (0,)))
                self.conn.execute("""INSERT INTO comments VALUES (?,?,?,?,?,?)
                    ON CONFLICT(comment_id) DO UPDATE SET text=excluded.text,
                        author_channel_id=excluded.author_channel_id,author_name=excluded.author_name,
                        is_pinned=CASE WHEN comments.is_pinned=1 OR excluded.is_pinned=1 THEN 1
                            WHEN comments.is_pinned IS NULL OR excluded.is_pinned IS NULL THEN NULL ELSE 0 END
                    """, tuple(record[f] for f in FIELDS))
        return boundary, self.count() - before

    def count(self):
        return self.conn.execute("SELECT count(*) FROM comments").fetchone()[0]

    def rows(self):
        for row in self.conn.execute("SELECT * FROM comments ORDER BY rowid"):
            yield (*row[:-1], bool(row[-1]) if row[-1] is not None else None)


async def fetch_json(client, body, metrics, *, client_version, retries, timeout, watch=False,
                     on_attempt=None):
    content = json.dumps({"context": {"client": {
        "clientName": "WEB", "clientVersion": client_version, "hl": "en", "gl": "US",
    }}, **body}, separators=(",", ":")).encode()
    for attempt in range(retries + 1):
        metrics["attempts"] += 1
        metrics["request_body_bytes"] += len(content)
        bridge = getattr(client, "catalog_bridge", None)
        trace = RequestTrace(bridge=bridge is not None)
        status, connection_error, website_error, excluded = None, None, None, False
        try:
            async with asyncio.timeout(timeout):
                async with client.stream("POST", ENDPOINT, content=content,
                    params={"prettyPrint": "false", "fields": WATCH_FIELD_MASK if watch else COMMENT_FIELD_MASK},
                    headers={"Content-Type": "application/json", "Accept-Encoding": "gzip"},
                    extensions={"trace": trace}) as response:
                    metrics["last_http_status"] = response.status_code
                    status = response.status_code
                    chunks, size = [], 0
                    try:
                        async for chunk in response.aiter_bytes():
                            size += len(chunk)
                            if size > MAX_BODY_BYTES:
                                shape("Comment response exceeded the size limit")
                            chunks.append(chunk)
                    finally:
                        metrics["response_body_bytes"] += response.num_bytes_downloaded
                        metrics["decoded_body_bytes"] += size
                    status = response.status_code
                    if 500 <= status <= 599 and attempt < retries:
                        continue
                    if status != 200:
                        raise CommentError("blocked" if status in (401, 403, 429) else "http_error", f"HTTP {status}")
                    try:
                        return json.loads(b"".join(chunks))
                    except (ValueError, UnicodeDecodeError) as exc:
                        raise CommentError("unexpected_response", "Invalid JSON response") from exc
        except CommentError as exc:
            website_error = f"youtube:http_{status}" if status != 200 else "youtube:unexpected_response"
            raise
        except (httpx.HTTPError, TimeoutError, ssl.SSLError) as exc:
            excluded = (local_worker_error(exc) or trace.local_failure
                        or bridge is not None and bridge.local_error(exc))
            connection_error = request_error_label(exc, trace)
            if attempt < retries:
                continue
            raise CommentError("local_error" if excluded else "request_error", connection_error) from exc
        except BaseException:
            excluded = True
            raise
        finally:
            if on_attempt is not None and not excluded:
                # The caller sets data_received after validating the page. A
                # parsed JSON object alone is not evidence of comment data.
                on_attempt(AttemptOutcome(
                    checked_at=datetime.now(timezone.utc),
                    request_sent=trace.request_sent or status is not None,
                    http_status=status, data_received=False,
                    connected=True if trace.request_sent or status is not None else trace.connected,
                    connection_error=proxy_connection_error(connection_error),
                    website_error=website_error or connection_error,
                ))


async def collect_comments(client, video_id, buffer, *, client_version=CLIENT_VERSION,
                           retries=2, timeout=30, max_pages=10000, on_page=None):
    """Collect into buffer; success means history reached or pagination exhausted."""
    if not isinstance(video_id, str) or not VIDEO_ID.fullmatch(video_id):
        raise ValueError("Expected an 11-character video ID")
    if retries < 0 or timeout <= 0 or max_pages < 1:
        raise ValueError("Invalid request limits")
    if buffer.count():
        raise ValueError("Start each scan with an empty comment buffer")
    started = time.monotonic()
    result = {"video_id": video_id, "status": "error", "complete": False, "pages": 0,
              "comments": 0, "newest_verified": False, "stop_reason": None,
              "attempts": 0, "request_body_bytes": 0, "response_body_bytes": 0, "decoded_body_bytes": 0}

    async def request(body, *, watch=False):
        return await fetch_json(client, body, result, client_version=client_version,
                                retries=retries, timeout=timeout, watch=watch)

    try:
        initial = await request({"continuation": initial_continuation(video_id)})
        check_payload(initial, video_id)
        items, found, has_body = comment_items(initial, selecting_sort=True)
        header = header_info(items)
        state = message_status(items)
        token = header["token"] if header else None
        first_page = initial if has_body and header and header["newest_selected"] else None
        if first_page is not None:
            token = initial_continuation(video_id)
        if not isinstance(token, str) or not token:
            if any("commentThreadRenderer" in i or "continuationItemRenderer" in i for i in items):
                raise CommentError("sort_unverified", "Cannot select Newest for existing comments")
            state = state or ("empty" if header and header["empty"] and found else None)
            if state is None:
                state = watch_status(await request({"videoId": video_id}, watch=True), video_id)
            if state in ("empty", "disabled"):
                result.update(status=state, complete=True, stop_reason=state)
            elif state == "unavailable":
                raise CommentError("unavailable", "Video is unavailable")
            else:
                shape("No recognized comment section or terminal message")
        else:
            if state is not None and first_page is None:
                shape("Comment sort conflicts with terminal message")
            seen_tokens = set()
            for index in range(max_pages):
                if token in seen_tokens:
                    raise CommentError("pagination_loop", "Repeated comment continuation")
                seen_tokens.add(token)
                payload = first_page if index == 0 and first_page is not None else await request({"continuation": token})
                page = parse_page(payload, video_id)
                result["newest_verified"] = True
                result["pages"] += 1
                if page["state"] and result["pages"] > 1:
                    shape("Comment section changed during pagination")
                boundary, added = buffer.add_page(page["comments"])
                result["comments"] = buffer.count()
                if on_page is not None:
                    on_page(dict(result))
                token = page["continuation"]
                if boundary or token is None:
                    result.update(status=page["state"] or ("ok" if result["comments"] else "empty"),
                                  complete=True, stop_reason="saved_history" if boundary else page["state"] or "end")
                    break
                if added == 0:
                    raise CommentError("pagination_loop", "Comment page made no progress")
            else:
                raise CommentError("page_limit", "Comment page limit reached before completion")
    except CommentError as exc:
        result.update(status=exc.status, error=str(exc))
    except asyncio.CancelledError:
        result.update(status="interrupted", error="Comment scan interrupted")
        raise
    finally:
        result["comments"] = buffer.count()
        result["seconds"] = round(time.monotonic() - started, 4)
    return result


def load_saved_history(conn, video_id, buffer):
    row = conn.execute("SELECT comments_updated_at FROM public.videos WHERE video_id=%s", (video_id,)).fetchone()
    if row is None:
        raise ValueError("Video must already exist in public.videos")
    # Rows from an unfinished first import must never define a stopping point.
    if row[0] is not None:
        buffer.load_history(conn.execute("SELECT comment_id,is_pinned FROM public.comments WHERE video_id=%s", (video_id,)))


def save_scan(conn, result, buffer):
    """Import all six fields and advance completion in the same transaction."""
    video_id = result["video_id"]
    complete = result.get("complete") is True and result.get("status") in SUCCESS
    with conn.transaction():
        if not complete:
            conn.execute("UPDATE public.videos SET comments_error=%s WHERE video_id=%s",
                         (result.get("status", "error") + ": " + result.get("error", "Incomplete comment scan"), video_id))
            return 0
        if not conn.execute("SELECT 1 FROM public.videos WHERE video_id=%s FOR UPDATE", (video_id,)).fetchone():
            raise ValueError("Video no longer exists")
        saved = 0
        for row in buffer.rows():
            if row[0] != video_id:
                raise ValueError("Buffered comment belongs to another video")
            inserted = conn.execute("""INSERT INTO public.comments
                (video_id,comment_id,text,author_channel_id,author_name,is_pinned)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (video_id,comment_id) DO NOTHING""", row).rowcount
            saved += inserted
            if not inserted:
                # Refresh only the comments actually revisited by this scan.
                conn.execute("""UPDATE public.comments SET text=%s,author_channel_id=%s,
                    author_name=%s,is_pinned=%s WHERE video_id=%s AND comment_id=%s""",
                    (row[2], row[3], row[4], row[5], row[0], row[1]))
        conn.execute("UPDATE public.videos SET comments_updated_at=clock_timestamp(),comments_error=NULL WHERE video_id=%s", (video_id,))
        return saved


async def run(args):
    output = (args.output or ROOT / "outputs" / ("comments-" + args.video_id + "-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))).resolve()
    output.mkdir(parents=True, exist_ok=False)
    with open_database(autocommit=True) as conn, tempfile.TemporaryDirectory(prefix="comment-scan-") as folder:
        lock = ("media.video-comments:" + args.video_id,)
        if not conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", lock).fetchone()[0]:
            raise RuntimeError("Another comment collector is running for this video")
        buffer = CommentBuffer(str(Path(folder) / "scan.sqlite"))
        try:
            load_saved_history(conn, args.video_id, buffer)
            async with httpx.AsyncClient(http2=False, trust_env=False, follow_redirects=False,
                proxy=os.environ.get("MEDIA_PROXY_URL"), timeout=httpx.Timeout(args.timeout, connect=min(10, args.timeout))) as client:
                result = await collect_comments(client, args.video_id, buffer, client_version=args.client_version,
                    retries=args.retries, timeout=args.timeout, max_pages=args.max_pages,
                    on_page=lambda p: print(json.dumps({"event": "page", "pages": p["pages"], "comments": p["comments"]}), flush=True))
            try:
                result["inserted"] = save_scan(conn, result, buffer)
            except (psycopg.Error, ValueError) as exc:
                result.update(status="database_error", complete=False, error=type(exc).__name__, inserted=0)
                save_scan(conn, result, buffer)
            result["output_directory"] = str(output)
            result["traffic_scope"] = "HTTP bodies only; excludes headers, TLS and TCP/IP overhead"
            (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
            print(json.dumps(result), flush=True)
            return 0 if result["complete"] else 1
        except (asyncio.CancelledError, KeyboardInterrupt):
            save_scan(conn, {"video_id": args.video_id, "status": "interrupted", "error": "Comment scan interrupted"}, buffer)
            raise
        finally:
            buffer.close()
            conn.execute("SELECT pg_advisory_unlock(hashtextextended(%s,0))", lock)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("video_id", help="An existing public.videos ID")
    parser.add_argument("--client-version", default=CLIENT_VERSION)
    parser.add_argument("--max-pages", type=positive_int, default=10000)
    parser.add_argument("--timeout", type=positive_int, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not VIDEO_ID.fullmatch(args.video_id) or args.retries < 0:
        parser.error("Use an 11-character video ID and nonnegative retries")
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
