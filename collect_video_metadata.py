"""Collect a fixed list of pending video metadata, saving each success immediately."""

import argparse
import asyncio
from collections import Counter
from contextlib import AsyncExitStack
from datetime import date, datetime, timezone
import errno
from http import HTTPStatus
import json
import os
from pathlib import Path
import re
import socket
import ssl
import time

import httpx
import psycopg

from collect_subscribers import CLIENT_VERSION, ROOT, positive_int
from collection_policy import video_error_reason
from discover_videos import open_database
from runtime_config import OUTPUT_DIR
from proxy_statistics import AttemptOutcome, ProxyStatistics, proxy_connection_error
from proxy_catalog import CatalogClients, DEFAULT_BRIDGE_BINARY, SUPPORTED_PROTOCOLS, load_catalog


ENDPOINT = "https://www.youtube.com/youtubei/v1/player"
FIELD_MASK = (
    "videoDetails(videoId,title,shortDescription,lengthSeconds,thumbnail/thumbnails/url),"
    "microformat/playerMicroformatRenderer/publishDate,playabilityStatus(status,reason)"
)
MAX_BODY_BYTES = 65_536
MAX_METADATA_ERROR_CHARS = 500
EXACT_TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})\Z")


class ResponseShapeError(ValueError):
    pass


def object_field(parent, key):
    value = parent.get(key, {})
    if not isinstance(value, dict):
        raise ResponseShapeError(f"Invalid {key} object")
    return value


def publication_time(value):
    """Return only a second-precision timestamp with an explicit timezone."""
    if value is None:
        return None, "missing"
    if not isinstance(value, str):
        raise ResponseShapeError("Invalid publication date")
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            date.fromisoformat(value)
            return None, "date"
        if not EXACT_TIMESTAMP.fullmatch(value):
            return None, "insufficient"
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc).isoformat(), "timestamp"
    except ValueError as exc:
        raise ResponseShapeError("Invalid publication date") from exc


def parse_metadata(payload, video_id):
    if not isinstance(payload, dict):
        raise ResponseShapeError("Response is not an object")
    playback = object_field(payload, "playabilityStatus")
    status = playback.get("status")
    if not isinstance(status, str) or not re.fullmatch(r"[A-Z_]{1,64}", status):
        raise ResponseShapeError("Missing or invalid player status")
    details = object_field(payload, "videoDetails")
    if details and details.get("videoId") != video_id:
        raise ResponseShapeError("Response video ID does not match the requested ID")
    if status == "OK" and not details:
        raise ResponseShapeError("Playable response is missing video details")
    title, description = details.get("title"), details.get("shortDescription")
    if any(value is not None and not isinstance(value, str) for value in (title, description)):
        raise ResponseShapeError("Invalid title or description")
    duration = details.get("lengthSeconds")
    if duration is not None:
        if not ((type(duration) is int) or
                (isinstance(duration, str) and re.fullmatch(r"[0-9]+", duration))):
            raise ResponseShapeError("Invalid duration")
        duration = int(duration)
        if not 0 <= duration <= 2**31 - 1:
            raise ResponseShapeError("Duration is outside the supported range")
    microformat = object_field(object_field(payload, "microformat"), "playerMicroformatRenderer")
    raw_date = microformat.get("publishDate")
    published_at, precision = publication_time(raw_date)
    thumbnails = object_field(details, "thumbnail").get("thumbnails", [])
    if not isinstance(thumbnails, list):
        raise ResponseShapeError("Invalid thumbnails list")
    thumbnail_url = None
    for thumbnail in thumbnails:
        if not isinstance(thumbnail, dict):
            raise ResponseShapeError("Invalid thumbnail object")
        url = thumbnail.get("url")
        if url is not None:
            if not isinstance(url, str) or not url.strip():
                raise ResponseShapeError("Invalid thumbnail URL")
            thumbnail_url = url
    metadata = {"title": title, "description": description, "duration_seconds": duration,
                "published_at": published_at, "thumbnail_url": thumbnail_url}
    if any(value is not None for value in metadata.values()) and details.get("videoId") != video_id:
        raise ResponseShapeError("Metadata response is missing the requested video ID")
    return {
        "metadata": metadata, "metadata_complete": all(value is not None for value in metadata.values()),
        "player_status": status,
        "publication_precision": precision, "publish_date_raw": raw_date,
        "player_reason": playback.get("reason") if isinstance(playback.get("reason"), str) else None,
        "access_challenge": status == "LOGIN_REQUIRED" and isinstance(playback.get("reason"), str)
                            and "not a bot" in playback["reason"].lower(),
    }


def has_metadata(result):
    return result["status"] == "ok" and any(
        value is not None for value in (result.get("metadata") or {}).values()
    )


class RequestTrace:
    """Observe the real proxy connection and target request independently."""
    def __init__(self, *, bridge=False):
        self.request_sent = False
        self.connected = None
        self.connection_error = None
        self.stage = None
        self.bridge = bridge
        self.local_failure = False
        self._response_is_connect = False
        self._sending = {}

    async def __call__(self, name, info):
        try:
            if name.endswith('.connect_tcp.started'):
                self.stage = 'bridge' if self.bridge else 'connect'
            elif name.endswith('.connect_tcp.complete') and not self.bridge:
                self.connected = True
            elif name.endswith('.connect_tcp.failed'):
                if self.bridge:
                    self.local_failure = True
                elif self.connected is None:
                    self.connected = False
            elif name.endswith('.start_tls.started'):
                self.stage = ('youtube_https' if info.get('server_hostname') in
                              (b'www.youtube.com', 'www.youtube.com') else 'proxy_tls')
            elif name.endswith('.send_request_headers.started'):
                self._response_is_connect = info['request'].method in (b'CONNECT', 'CONNECT')
                self.stage = 'proxy_handshake' if self._response_is_connect else 'youtube_https'
                if not self._response_is_connect:
                    # This also covers an already established, reused tunnel.
                    self.connected = True
            elif name.endswith('.receive_response_headers.complete') and self._response_is_connect:
                _, status, _, headers = info['return_value']
                if self.bridge:
                    headers = {key.lower(): value for key, value in headers}
                    value = headers.get(b'x-proxy-connected')
                    if value in (b'true', b'false'):
                        self.connected = value == b'true'
                    elif 200 <= status < 300:
                        self.connected = True
                    label = headers.get(b'x-proxy-error', b'').decode('ascii')
                    if re.fullmatch(r'[a-z][a-z0-9_]*:[a-z][a-z0-9_]*', label):
                        self.connection_error = label
                else:
                    self.connected = True
                    if not 200 <= status < 300:
                        self.connection_error = f'proxy_handshake:proxy_http_{status}'
            elif name.endswith('.receive_response_body.started') and not self._response_is_connect:
                self.stage = 'youtube_body'
            if name.endswith('.send_request_body.started'):
                request = info['request']
                self._sending[name.removesuffix('.started')] = (
                    request.method in (b'POST', 'POST')
                    and request.url.host in (b'www.youtube.com', 'www.youtube.com')
                    and request.url.scheme in (b'https', 'https'))
            elif name.endswith('.send_request_body.complete'):
                self.request_sent |= self._sending.pop(name.removesuffix('.complete'), False)
            elif name.endswith('.send_request_body.failed'):
                self._sending.pop(name.removesuffix('.failed'), None)
        except Exception:
            # Telemetry must not raise into the HTTP request.
            pass


def request_error_label(error, trace):
    """Safe stage:code labels only, never raw exceptions or proxy credentials."""
    if trace.connection_error is not None:
        return trace.connection_error
    if isinstance(error, httpx.HTTPStatusError):
        return None
    stage = trace.stage or ('connect' if isinstance(error, (httpx.ConnectError, httpx.ConnectTimeout)) else
                            'proxy_handshake' if isinstance(error, httpx.ProxyError) else 'youtube_https')
    if isinstance(error, httpx.ProxyError) and not trace.bridge:
        match = re.match(r'([1-5][0-9]{2})\s', str(error))
        if match:
            trace.connected = True
            return f'proxy_handshake:proxy_http_{match[1]}'
    pending, seen, code = [error], set(), 'network_or_protocol_error'
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (httpx.TimeoutException, TimeoutError)):
            code = 'timeout'
        if isinstance(current, ssl.SSLCertVerificationError):
            code = 'youtube_certificate_verification_failed' if stage == 'youtube_https' else 'certificate_verification_failed'
            break
        if isinstance(current, socket.gaierror):
            stage, code = 'resolve', 'dns_not_found' if current.errno == socket.EAI_NONAME else 'dns_error'
            break
        if isinstance(current, OSError) and current.errno in {
            errno.ECONNREFUSED, errno.ECONNRESET, errno.ENETUNREACH, errno.EHOSTUNREACH, errno.EPIPE,
        }:
            code = {errno.ECONNREFUSED:'connection_refused', errno.ECONNRESET:'connection_reset',
                    errno.ENETUNREACH:'network_unreachable', errno.EHOSTUNREACH:'host_unreachable',
                    errno.EPIPE:'broken_pipe'}[current.errno]
            break
        pending.extend(value for value in (current.__cause__, current.__context__) if value is not None)
    return f'{stage}:{code}'


def local_worker_error(error):
    """Exclude local resource exhaustion and client-side errors from proxy scoring."""
    seen = set()
    pending = [error]
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, (httpx.PoolTimeout, httpx.LocalProtocolError, httpx.UnsupportedProtocol)):
            return True
        if isinstance(current, OSError) and current.errno in {
            errno.EMFILE, errno.ENFILE, errno.ENOMEM, errno.ENOBUFS, errno.EADDRNOTAVAIL, errno.EADDRINUSE,
        }:
            return True
        pending.extend(value for value in (current.__cause__, current.__context__) if value is not None)
    return False


def youtube_error_label(result, connection_error):
    """Describe the collector's existing result without extra data validation."""
    if connection_error is not None:
        return connection_error
    status = result.get('http_status')
    if status is not None and status >= 300:
        return f'http:http_{status}'
    if has_metadata(result):
        return None
    if result['status'] == 'access_challenge':
        return 'data:access_challenge'
    if result['status'] == 'unexpected_response':
        return 'data:unexpected_response'
    if result['status'] == 'ok':
        return 'data:no_usable_data'
    return 'request:failed'


async def fetch_metadata(client, video_id, client_version=CLIENT_VERSION, retries=10, *, on_attempt=None,
                         total_timeout=None):
    body = json.dumps({"context": {"client": {
        "clientName": "WEB", "clientVersion": client_version, "hl": "en",
    }}, "videoId": video_id}, separators=(",", ":")).encode()
    totals = {"video_id": video_id, "attempts": 0, "request_body_bytes": 0,
              "response_body_bytes": 0, "decoded_body_bytes": 0, "rows_updated": 0}
    started = time.monotonic()
    for _ in range(retries + 1):
        result = {"status": "error", "metadata": None}
        bridge = getattr(client, 'catalog_bridge', None)
        trace = RequestTrace(bridge=bridge is not None) if on_attempt is not None else None
        connection_error = None
        excluded = False
        totals["attempts"] += 1
        totals["request_body_bytes"] += len(body)
        try:
            async with asyncio.timeout(total_timeout), client.stream("POST", ENDPOINT,
                                     params={"prettyPrint": "false", "fields": FIELD_MASK},
                                     content=body,
                                     extensions={"trace": trace} if trace is not None else None) as response:
                result.update(http_status=response.status_code, http_version=response.http_version,
                              content_encoding=response.headers.get("content-encoding"))
                if response.status_code in (400, 401, 403, 429):
                    result.update(status="blocked", error=f"HTTP {response.status_code}",
                                  retry_after=response.headers.get("retry-after"))
                    continue
                chunks, size = [], 0
                try:
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_BODY_BYTES:
                            raise ResponseShapeError("Response exceeded the expected size")
                        chunks.append(chunk)
                finally:
                    totals["response_body_bytes"] += response.num_bytes_downloaded
                    totals["decoded_body_bytes"] += size
                response.raise_for_status()
                parsed = parse_metadata(json.loads(b"".join(chunks)), video_id)
                result.update(parsed, status="access_challenge" if parsed["access_challenge"] else "ok")
                result.pop("error", None)
                if parsed["access_challenge"]:
                    result["error"] = "YouTube requires sign-in for bot verification"
        except (ResponseShapeError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            result.update(status="unexpected_response", error=str(exc))
        except (httpx.HTTPError, TimeoutError, ssl.SSLError) as exc:
            excluded = (local_worker_error(exc) or (trace is not None and trace.local_failure)
                        or (bridge is not None and bridge.local_error(exc)))
            if trace is not None:
                connection_error = request_error_label(exc, trace)
            result["error"] = type(exc).__name__
        except BaseException:
            # Cancellation and unexpected worker failures are not proxy failures.
            excluded = True
            raise
        finally:
            if on_attempt is not None and not excluded:
                try:
                    video_error = (video_error_reason({**result, 'video_id': video_id})
                                   if not has_metadata(result) and not connection_error else None)
                    on_attempt(AttemptOutcome(
                        checked_at=datetime.now(timezone.utc),
                        request_sent=trace.request_sent or result.get('http_status') is not None,
                        http_status=result.get('http_status'), data_received=None if video_error else has_metadata(result),
                        connected=True if trace.request_sent or result.get('http_status') is not None else trace.connected,
                        connection_error=proxy_connection_error(connection_error),
                        website_error=('video:' + video_error.lower()) if video_error else
                                      youtube_error_label(result, connection_error)))
                except Exception:
                    pass
        if has_metadata(result):
            break
    return {**totals, **result, "seconds": round(time.monotonic() - started, 4)}


def parse_proxy_urls(value):
    message = "MEDIA_PROXY_URLS must be a non-empty JSON array of unique HTTP(S) proxy URLs"
    try:
        urls = json.loads(value)
        if not isinstance(urls, list) or not urls or any(not isinstance(url, str) for url in urls):
            raise ValueError
        endpoints = set()
        for value in urls:
            url = httpx.URL(value)
            if url.scheme not in ("http", "https") or not url.host:
                raise ValueError
            endpoint = (url.host, url.port or (443 if url.scheme == "https" else 80))
            if endpoint in endpoints:
                raise ValueError
            endpoints.add(endpoint)
    except (ValueError, httpx.InvalidURL):
        raise ValueError(message) from None
    return urls


async def fetch_metadata_with_proxies(clients, video_id, client_version=CLIENT_VERSION, *, on_attempts=None, proxy_index=0):
    """Make one attempt through the proxy assigned to this video."""
    if not clients:
        raise ValueError("At least one proxy client is required")
    index = proxy_index % len(clients)
    observer = on_attempts[index] if on_attempts and index < len(on_attempts) else None
    result = await fetch_metadata(clients[index], video_id, client_version, retries=0, on_attempt=observer)
    return {**result, "proxy_number": index + 1}


def select_videos(conn, args):
    skip_errors = getattr(args, "skip_errors", False)
    if args.video_id:
        requested = list(dict.fromkeys(args.video_id))
        rows = {row[0]: row for row in conn.execute(
            "SELECT video_id,type,metadata_updated_at,metadata_error FROM public.videos WHERE video_id=ANY(%s)",
            (requested,),
        )}
        if set(rows) != set(requested):
            raise ValueError("Requested video ID is not in the videos table")
        return [rows[video_id][:2] for video_id in requested
                if rows[video_id][2] is None and (not skip_errors or rows[video_id][3] is None)]
    return conn.execute(
        """SELECT video_id,type FROM public.videos WHERE metadata_updated_at IS NULL
           AND (NOT %s OR metadata_error IS NULL)
           ORDER BY video_id LIMIT %s""", (skip_errors, args.limit)
    ).fetchall()


def save_metadata(conn, result):
    if not has_metadata(result):
        return 0
    metadata = result["metadata"]
    published = datetime.fromisoformat(metadata["published_at"]) if metadata["published_at"] else None
    count = conn.execute(
        """UPDATE public.videos SET
           title=COALESCE(%s,title), description=COALESCE(%s,description),
           duration_seconds=COALESCE(%s,duration_seconds),
           published_at=COALESCE(%s,published_at), thumbnail_url=COALESCE(%s,thumbnail_url),
           metadata_updated_at=clock_timestamp(), metadata_error=NULL
           WHERE video_id=%s""",
        (metadata["title"], metadata["description"], metadata["duration_seconds"],
         published, metadata["thumbnail_url"], result["video_id"]),
    ).rowcount
    if count != 1:
        raise psycopg.IntegrityError("Selected video row no longer exists")
    return count


def metadata_error_reason(result):
    """Describe a final unsuccessful response without storing a response body."""
    http_status = result.get("http_status")
    if isinstance(http_status, int) and not 200 <= http_status < 300:
        code = f"HTTP_{http_status}"
        try:
            message = HTTPStatus(http_status).phrase
        except ValueError:
            message = None
    elif result["status"] == "unexpected_response":
        code, message = "INVALID_RESPONSE", result.get("error")
    elif result.get("player_status") and result["player_status"] != "OK":
        code, message = result["player_status"], result.get("player_reason")
    elif result.get("error") in {"TimeoutError", "TimeoutException", "ConnectTimeout", "ReadTimeout", "WriteTimeout", "PoolTimeout"}:
        code, message = "TIMEOUT", None
    elif result.get("error"):
        code, message = "REQUEST_ERROR", result["error"]
    else:
        code, message = "NO_METADATA", "No metadata returned"
    message = " ".join(message.replace("\x00", "").split()) if isinstance(message, str) else ""
    return (f"{code}: {message}" if message else code)[:MAX_METADATA_ERROR_CHARS]


def save_metadata_error(conn, result):
    """Record only the final failure; preserve metadata and its successful timestamp."""
    count = conn.execute(
        "UPDATE public.videos SET metadata_error=%s WHERE video_id=%s",
        (metadata_error_reason(result), result["video_id"]),
    ).rowcount
    if count != 1:
        raise psycopg.IntegrityError("Selected video row no longer exists")
    return count


async def collect(args):
    catalog_mode = getattr(args, 'proxy_catalog', False) or bool(getattr(args, 'proxy_id', None))
    if catalog_mode and (os.environ.get('MEDIA_PROXY_URLS') is not None or os.environ.get('MEDIA_PROXY_URL')):
        raise ValueError('Choose catalog proxies or MEDIA_PROXY_URL(S), not both')
    if getattr(args, 'proxy_protocol', None) and not catalog_mode:
        raise ValueError('--proxy-protocol requires --proxy-catalog or --proxy-id')
    catalog = load_catalog(getattr(args, 'proxy_id', None), getattr(args, 'proxy_protocol', None), website='youtube') if catalog_mode else None
    proxy_list = os.environ.get("MEDIA_PROXY_URLS")
    use_proxy_list = proxy_list is not None or catalog_mode
    proxy_urls = parse_proxy_urls(proxy_list) if proxy_list is not None else [os.environ.get("MEDIA_PROXY_URL")]
    proxy_count = len(catalog) if catalog_mode else sum(bool(url) for url in proxy_urls)
    with open_database(autocommit=True) as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(hashtext('media.video-metadata'))").fetchone()[0]:
            raise RuntimeError("Another video metadata collector is running for this database")
        videos = select_videos(conn, args)
        out = args.output or OUTPUT_DIR / (
            "video-metadata-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        )
        out.mkdir(parents=True, exist_ok=False)
        summary = {
            "started_at": datetime.now(timezone.utc).isoformat(), "selected": len(videos),
            "completed": 0, "saved": 0, "metadata_complete": 0,
            "concurrency": args.concurrency, "client_version": args.client_version,
            "proxy_used": bool(proxy_count), "proxy_count": proxy_count,
            "retry_policy": "one_attempt_per_video" if use_proxy_list else "immediate_retries",
            "http_attempts": 0, "request_body_bytes": 0, "response_body_bytes": 0,
            "decoded_body_bytes": 0, "stopped_reason": None, "output_directory": str(out.resolve()),
            "traffic_scope": "HTTP bodies consumed by this worker; excludes headers, TLS and TCP/IP overhead",
        }
        if use_proxy_list:
            summary['proxy_selection'] = 'round_robin'
        if catalog_mode:
            summary['proxy_protocols'] = dict(Counter(proxy.working_protocol for proxy in catalog))
            summary['proxy_outcomes_by_protocol'] = {protocol: dict(connection_attempts=0,
                youtube_requests_sent=0, youtube_responses_received=0, youtube_successful_data_received=0)
                for protocol in summary['proxy_protocols']}
        precisions, kinds, versions, encodings, retries = (Counter() for _ in range(5))
        jobs, stop = iter(videos), asyncio.Event()
        started = time.monotonic()
        print(json.dumps({"event": "started", "selected": len(videos), "output_directory": str(out)}), flush=True)
        statistics = None
        observers = [None]*(len(catalog) if catalog_mode else len(proxy_urls))
        proxy_cursor = 0
        try:
            if proxy_count:
                try:
                    statistics = (ProxyStatistics([proxy.statistics_target for proxy in catalog], website='youtube') if catalog_mode else
                                  ProxyStatistics.from_urls(proxy_urls, os.environ.get('MEDIA_PROXY_IDS'), website='youtube')).start()
                    observers = [statistics.observer(number) for number in range(len(observers))]
                except Exception as exc:
                    if statistics is not None:
                        try:
                            statistics.close()
                        except Exception:
                            pass
                    statistics = None
                    summary['proxy_statistics'] = {'mode': 'best_effort', 'setup_error_type': type(exc).__name__}
            if catalog_mode:
                for number, proxy in enumerate(catalog):
                    previous = observers[number]
                    def observed(outcome, previous=previous, protocol=proxy.working_protocol):
                        totals = summary['proxy_outcomes_by_protocol'][protocol]
                        totals['connection_attempts'] += 1
                        totals['youtube_requests_sent'] += int(outcome.request_sent)
                        totals['youtube_responses_received'] += int(outcome.http_status is not None)
                        totals['youtube_successful_data_received'] += int(outcome.data_received is True)
                        if previous is not None:
                            previous(outcome)
                    observers[number] = observed
            async with AsyncExitStack() as stack:
                clients = (await stack.enter_async_context(CatalogClients(catalog, args.concurrency,
                    getattr(args, 'proxy_bridge_binary', DEFAULT_BRIDGE_BINARY)))) if catalog_mode else [await stack.enter_async_context(httpx.AsyncClient(
                    http2=True, trust_env=False, follow_redirects=False, proxy=proxy,
                    headers={"Content-Type": "application/json", "Accept-Encoding": "gzip"},
                    timeout=httpx.Timeout(20, connect=10),
                    limits=httpx.Limits(max_connections=args.concurrency, max_keepalive_connections=args.concurrency),
                )) for proxy in proxy_urls]
                async def worker():
                    nonlocal proxy_cursor
                    while not stop.is_set():
                        try:
                            video_id, kind = next(jobs)
                        except StopIteration:
                            return
                        proxy_index = proxy_cursor % len(clients)
                        proxy_cursor += 1
                        result = (await fetch_metadata_with_proxies(clients, video_id, args.client_version,
                                                                    on_attempts=observers, proxy_index=proxy_index)
                                  if use_proxy_list else
                                  await fetch_metadata(clients[0], video_id, args.client_version, args.retries,
                                                       on_attempt=observers[0]))
                        if not stop.is_set():
                            try:
                                result["rows_updated"] = save_metadata(conn, result)
                                if not result["rows_updated"]:
                                    save_metadata_error(conn, result)
                            except psycopg.Error:
                                stop.set()
                                summary["stopped_reason"] = "database_error"
                        summary["completed"] += 1
                        summary["saved"] += result["rows_updated"]
                        summary["http_attempts"] += result["attempts"]
                        for key in ("request_body_bytes", "response_body_bytes", "decoded_body_bytes"):
                            summary[key] += result[key]
                        if result["rows_updated"]:
                            summary["metadata_complete"] += int(result["metadata_complete"])
                            precisions[result["publication_precision"]] += 1
                        retries[result["attempts"] - 1] += 1
                        kinds[kind] += 1
                        versions[result.get("http_version") or "no_response"] += 1
                        encodings[result.get("content_encoding") or "identity"] += 1
                async def report_progress():
                    while True:
                        await asyncio.sleep(10)
                        progress = {"event": "progress", "completed": summary["completed"],
                                    "saved": summary["saved"], "selected": len(videos)}
                        if statistics is not None:
                            try:
                                progress['proxy_statistics'] = statistics.snapshot()
                            except Exception:
                                pass
                        print(json.dumps(progress), flush=True)
                reporter = asyncio.create_task(report_progress())
                try:
                    async with asyncio.TaskGroup() as group:
                        for _ in range(min(args.concurrency, len(videos))):
                            group.create_task(worker())
                finally:
                    reporter.cancel()
                    await asyncio.gather(reporter, return_exceptions=True)
        finally:
            if statistics is not None:
                try:
                    statistics.close()  # No join or final synchronous database flush.
                    summary['proxy_statistics'] = statistics.snapshot()
                except Exception as exc:
                    summary['proxy_statistics'] = {'mode': 'best_effort', 'report_error_type': type(exc).__name__}
            summary.update(finished_at=datetime.now(timezone.utc).isoformat(),
                           seconds=round(time.monotonic() - started, 3),
                           unprocessed=len(videos) - summary["completed"],
                           unsaved=summary["completed"] - summary["saved"],
                           retry_counts=dict(sorted(retries.items())), publication_precisions=dict(precisions),
                           video_types=dict(kinds), http_versions=dict(versions), content_encodings=dict(encodings))
            (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
            print(json.dumps({"event": "finished", **summary}), flush=True)
        return 0 if summary["saved"] == len(videos) else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--limit", type=positive_int, default=100,
                           help="Select at most this many pending IDs at startup (default: %(default)s)")
    selection.add_argument("--all", dest="limit", action="store_const", const=None,
                           help="Select every pending ID at startup")
    selection.add_argument("--video-id", action="append", help="Existing ID; repeat for a sample; saved IDs are skipped")
    parser.add_argument("--skip-errors", action="store_true",
                        help="Skip IDs whose metadata_error is not NULL")
    parser.add_argument("--concurrency", type=positive_int, default=128,
                        help="Concurrent metadata workers (default: %(default)s)")
    parser.add_argument("--retries", type=int, choices=range(11), default=10,
                        help="Immediate retries per ID without a proxy list or catalog (default: %(default)s)")
    parser.add_argument("--client-version", default=os.environ.get("YOUTUBE_CLIENT_VERSION", CLIENT_VERSION))
    parser.add_argument("--output", type=Path, help="New directory for the aggregate run summary")
    catalog_selection = parser.add_mutually_exclusive_group()
    catalog_selection.add_argument('--proxy-catalog', action='store_true',
                                   help='Use saved configurations that have responded, across supported protocols')
    catalog_selection.add_argument('--proxy-id', type=positive_int, action='append',
                                   help='Use a specific catalog configuration; repeat to select several')
    parser.add_argument('--proxy-protocol', choices=sorted(SUPPORTED_PROTOCOLS), action='append',
                        help='Select working protocols in catalog mode; defaults to all supported protocols')
    parser.add_argument('--proxy-bridge-binary', type=Path, default=DEFAULT_BRIDGE_BINARY,
                        help='Path to the compiled proxy-tester transport bridge')
    raise SystemExit(asyncio.run(collect(parser.parse_args())))


if __name__ == "__main__":
    main()
