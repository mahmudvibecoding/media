"""Import local proxy observations and rank every verified YouTube responder."""
from dataclasses import asdict, dataclass
from email.utils import parsedate_to_datetime
import importlib.util
import json
import math
import os
from pathlib import Path

from proxy_formats import unpack_connection_settings
from proxy_service_common import SERVICE_DIR, digest, file_lock, utcnow, write_json
from proxy_statistics import AttemptOutcome, IMPORT_LOCK
from runtime_config import ROOT


_spec = importlib.util.spec_from_file_location("proxy_journal_import", ROOT / "proxy-tester/import_results.py")
_journal = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_journal)


@dataclass(frozen=True)
class Observation:
    proxy_id: int
    connection_key: bytes
    checked_at: datetime
    declared_protocol: str
    working_protocol: str | None
    attempted: bool
    connected: bool
    request_sent: bool
    http_status: int | None
    data_received: bool | None
    connection_error: str | None
    website_error: str | None
    latency_ms: float
    retry_after_seconds: float = 0

    def validate(self):
        if type(self.proxy_id) is not int or self.proxy_id < 1 or len(self.connection_key) != 32:
            raise ValueError("Invalid observed proxy identity")
        if type(self.attempted) is not bool or (self.connected or self.request_sent) and not self.attempted:
            raise ValueError("Invalid proxy attempt classification")
        if bool(self.working_protocol) != (self.http_status is not None):
            raise ValueError("Only a verified YouTube response confirms its working protocol")
        if not math.isfinite(self.latency_ms) or self.latency_ms < 0 or not math.isfinite(self.retry_after_seconds) or self.retry_after_seconds < 0:
            raise ValueError("Invalid observation timing")
        AttemptOutcome(self.checked_at, self.request_sent, self.http_status, self.data_received,
                       self.connected, self.connection_error, self.website_error).validate()

    def row(self):
        self.validate()
        return tuple(asdict(self).values())


def retry_after(value, at):
    if not value:
        return 0
    try:
        if value.isdigit():
            seconds = float(value)
        else:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                return 0
            seconds = max(0, (date - at).total_seconds())
        return seconds if math.isfinite(seconds) and 0 <= seconds <= 365 * 86400 else 0
    except (ValueError, OverflowError, TypeError):
        return 0


def apply_observations(conn, observations, journal_key, run_id, *, retest_seconds=21600, quality=False, _staged=False, _retry_delays=None):
    """Receipts make late and replayed batches safe without resetting newer fields."""
    if len(journal_key) != 32 or not 0 < retest_seconds <= 365 * 86400:
        raise ValueError("Invalid observation import settings")
    rows = [] if _staged else [observation.row() for observation in observations]
    count = conn.execute("SELECT count(*) FROM import_proxy_results").fetchone()[0] if _staged else len(rows)
    with conn.transaction():
        if not conn.execute("SELECT pg_try_advisory_xact_lock(%s)", (IMPORT_LOCK,)).fetchone()[0]:
            raise RuntimeError("Another statistics writer is active; saved observations will retry")
        saved = conn.execute("""INSERT INTO app_meta.proxy_observation_imports
            (journal_sha256,run_id,observations) VALUES (%s,%s,%s)
            ON CONFLICT DO NOTHING RETURNING observations""", (journal_key, run_id, count)).fetchone()
        if saved is None:
            return {"observations": count, "already_imported": True}
        conn.execute("""CREATE TEMP TABLE IF NOT EXISTS service_observations (
            proxy_id BIGINT PRIMARY KEY, connection_key BYTEA NOT NULL, checked_at TIMESTAMPTZ NOT NULL,
            declared_protocol TEXT NOT NULL, working_protocol TEXT,
            attempted BOOLEAN NOT NULL, connected BOOLEAN NOT NULL, request_sent BOOLEAN NOT NULL,
            http_status SMALLINT, data_received BOOLEAN, connection_error TEXT, website_error TEXT,
            latency_ms DOUBLE PRECISION NOT NULL, retry_after_seconds DOUBLE PRECISION NOT NULL
        ) ON COMMIT DROP""")
        conn.execute("TRUNCATE service_observations")
        if _staged:
            conn.execute("""INSERT INTO service_observations
                SELECT proxy_id,connection_key,checked_at,declared_protocol,detected_protocol,
                    attempted,connected,(requests_sent>0),http_status,NULL,connection_error,
                    website_error,total_ms,0 FROM import_proxy_results""")
            if _retry_delays:
                with conn.cursor() as cursor:
                    cursor.executemany("UPDATE service_observations SET retry_after_seconds=%s WHERE proxy_id=%s",
                                       [(seconds, identifier) for identifier, seconds in _retry_delays.items()])
            if conn.execute("""SELECT EXISTS(SELECT 1 FROM service_observations
                WHERE latency_ms<0 OR latency_ms IN ('NaN'::float8,'Infinity'::float8)
                   OR http_status NOT BETWEEN 100 AND 599 OR octet_length(connection_key)<>32)""").fetchone()[0]:
                raise ValueError("Invalid staged observation")
        else:
            with conn.cursor().copy("COPY service_observations FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(row)
        if conn.execute("""SELECT EXISTS(SELECT 1 FROM service_observations s
            LEFT JOIN public.proxies p USING(proxy_id) WHERE p.proxy_id IS NULL
            OR p.connection_key<>s.connection_key
            OR p.connection_settings->>'transport' IS DISTINCT FROM s.declared_protocol)""").fetchone()[0]:
            raise ValueError("Proxy observation does not match its catalog identity")
        conn.execute("""INSERT INTO public.proxy_stats AS h
            (proxy_id,connection_attempts,successful_connections,last_connection_attempt_at,
             last_connected_at,last_connection_error,working_protocol,youtube_last_attempt_at,
             youtube_last_http_status,youtube_last_response_at,youtube_last_error,
             youtube_requests_sent,youtube_responses_received,youtube_successful_data_received,
             youtube_weighted_attempts,youtube_weighted_successful_data_received,
             youtube_last_scored_attempt_at,youtube_last_import_key)
            SELECT proxy_id,attempted::integer,connected::integer,
                CASE WHEN attempted THEN checked_at END,CASE WHEN connected THEN checked_at END,
                connection_error,working_protocol,CASE WHEN attempted THEN checked_at END,
                http_status,CASE WHEN http_status IS NOT NULL THEN checked_at END,website_error,
                request_sent::integer,(http_status IS NOT NULL)::integer,
                (data_received IS TRUE)::integer,(data_received IS NOT NULL)::integer,
                (data_received IS TRUE)::integer,CASE WHEN data_received IS NOT NULL THEN checked_at END,%s
            FROM service_observations ORDER BY proxy_id
            ON CONFLICT(proxy_id) DO UPDATE SET
                connection_attempts=h.connection_attempts+excluded.connection_attempts,
                successful_connections=h.successful_connections+excluded.successful_connections,
                last_connection_attempt_at=greatest(h.last_connection_attempt_at,excluded.last_connection_attempt_at),
                last_connected_at=greatest(h.last_connected_at,excluded.last_connected_at),
                last_connection_error=CASE WHEN excluded.last_connection_attempt_at IS NOT NULL
                    AND (h.last_connection_attempt_at IS NULL OR excluded.last_connection_attempt_at>=h.last_connection_attempt_at)
                    THEN excluded.last_connection_error ELSE h.last_connection_error END,
                working_protocol=CASE WHEN excluded.youtube_last_response_at IS NOT NULL
                    AND (h.youtube_last_response_at IS NULL OR excluded.youtube_last_response_at>=h.youtube_last_response_at)
                    THEN excluded.working_protocol ELSE h.working_protocol END,
                youtube_last_attempt_at=greatest(h.youtube_last_attempt_at,excluded.youtube_last_attempt_at),
                youtube_last_http_status=CASE WHEN excluded.youtube_last_attempt_at IS NOT NULL
                    AND (h.youtube_last_attempt_at IS NULL OR excluded.youtube_last_attempt_at>=h.youtube_last_attempt_at)
                    THEN excluded.youtube_last_http_status ELSE h.youtube_last_http_status END,
                youtube_last_response_at=greatest(h.youtube_last_response_at,excluded.youtube_last_response_at),
                youtube_last_error=CASE WHEN excluded.youtube_last_attempt_at IS NOT NULL
                    AND (h.youtube_last_attempt_at IS NULL OR excluded.youtube_last_attempt_at>=h.youtube_last_attempt_at)
                    THEN excluded.youtube_last_error ELSE h.youtube_last_error END,
                youtube_requests_sent=h.youtube_requests_sent+excluded.youtube_requests_sent,
                youtube_responses_received=h.youtube_responses_received+excluded.youtube_responses_received,
                youtube_successful_data_received=h.youtube_successful_data_received+excluded.youtube_successful_data_received,
                youtube_weighted_attempts=CASE WHEN excluded.youtube_last_scored_attempt_at IS NULL
                    THEN h.youtube_weighted_attempts ELSE
                    coalesce(public.proxy_decayed_weight(h.youtube_weighted_attempts,h.youtube_last_scored_attempt_at,
                        greatest(h.youtube_last_scored_attempt_at,excluded.youtube_last_scored_attempt_at)),0)
                    + public.proxy_decayed_weight(excluded.youtube_weighted_attempts,excluded.youtube_last_scored_attempt_at,
                        greatest(h.youtube_last_scored_attempt_at,excluded.youtube_last_scored_attempt_at)) END,
                youtube_weighted_successful_data_received=CASE WHEN excluded.youtube_last_scored_attempt_at IS NULL
                    THEN h.youtube_weighted_successful_data_received ELSE
                    coalesce(public.proxy_decayed_weight(h.youtube_weighted_successful_data_received,h.youtube_last_scored_attempt_at,
                        greatest(h.youtube_last_scored_attempt_at,excluded.youtube_last_scored_attempt_at)),0)
                    + public.proxy_decayed_weight(excluded.youtube_weighted_successful_data_received,excluded.youtube_last_scored_attempt_at,
                        greatest(h.youtube_last_scored_attempt_at,excluded.youtube_last_scored_attempt_at)) END,
                youtube_last_scored_attempt_at=greatest(h.youtube_last_scored_attempt_at,excluded.youtube_last_scored_attempt_at),
                youtube_last_import_key=CASE WHEN h.youtube_last_attempt_at IS NULL
                    OR excluded.youtube_last_attempt_at>=h.youtube_last_attempt_at
                    THEN excluded.youtube_last_import_key ELSE h.youtube_last_import_key END
        """, (journal_key,))
        # Reachability depends only on receiving a verified response. A 403,
        # challenge, or incomplete body remains a response and stays eligible.
        conn.execute("""INSERT INTO app_meta.proxy_test_state AS q
            (proxy_id,checked_at,next_test_at,last_response_at,latency_ms,failure_streak,last_error)
            SELECT proxy_id,checked_at,checked_at + make_interval(secs=>CASE WHEN http_status IS NOT NULL
                    THEN greatest(%s,retry_after_seconds) ELSE 300 END),
                CASE WHEN http_status IS NOT NULL THEN checked_at END,
                CASE WHEN http_status IS NOT NULL THEN latency_ms END,
                (http_status IS NULL)::integer,website_error
            FROM service_observations ORDER BY proxy_id
            ON CONFLICT(proxy_id) DO UPDATE SET
                checked_at=greatest(q.checked_at,excluded.checked_at),
                next_test_at=CASE WHEN q.checked_at IS NULL OR excluded.checked_at>=q.checked_at
                    THEN CASE WHEN excluded.last_response_at IS NOT NULL THEN excluded.next_test_at
                        ELSE excluded.checked_at + make_interval(secs=>least(%s,300*power(2,least(q.failure_streak,8)))) END
                    ELSE q.next_test_at END,
                last_response_at=greatest(q.last_response_at,excluded.last_response_at),
                latency_ms=CASE WHEN excluded.last_response_at IS NOT NULL
                    AND (q.last_response_at IS NULL OR excluded.last_response_at>=q.last_response_at)
                    THEN excluded.latency_ms ELSE q.latency_ms END,
                failure_streak=CASE WHEN q.checked_at IS NULL OR excluded.checked_at>=q.checked_at
                    THEN CASE WHEN excluded.last_response_at IS NOT NULL THEN 0 ELSE least(q.failure_streak+1,1000) END
                    ELSE q.failure_streak END,
                last_error=CASE WHEN q.checked_at IS NULL OR excluded.checked_at>=q.checked_at
                    THEN excluded.last_error ELSE q.last_error END
        """, (retest_seconds, retest_seconds))
        if quality:
            conn.execute("""UPDATE app_meta.proxy_test_state q
                SET quality_checked_at=greatest(q.quality_checked_at,s.checked_at)
                FROM service_observations s WHERE q.proxy_id=s.proxy_id""")
    return {"observations": count, "already_imported": False}


def import_test_journal(conn, path, *, retest_seconds=21600):
    # Validate and parse each line once, then transfer staged rows inside PostgreSQL.
    retry_delays = {}
    def observe(result, checked):
        replies = [a for a in result.get("attempts", []) if a.get("status") == "responds"]
        if replies:
            seconds = retry_after(replies[0].get("retry_after"), checked)
            if seconds:
                retry_delays[result["id"]] = seconds
    report = _journal.import_journal(conn, path, 10000, stage_only=True, on_result=observe)
    return apply_observations(conn, (), report["journal_key"], report["run"],
                              retest_seconds=retest_seconds, _staged=True, _retry_delays=retry_delays)


def export_due(conn, folder, *, limit=2000, now=None, after_id=None, max_id=None):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=False, mode=0o700)
    now = now or utcnow()
    # Stream large batches through a server cursor instead of retaining every
    # configuration in Python while the Go worker uses its own memory budget.
    source = folder / "input.jsonl"
    count = 0
    with conn.transaction(), conn.cursor(name="proxy_due_export") as cursor, source.open("w", buffering=1024*1024) as output:
        os.chmod(source, 0o600)
        cursor.itersize = 10000
        if after_id is not None:
            cursor.execute("""SELECT proxy_id,connection_key,address,port,connection_settings
                FROM public.proxies WHERE proxy_id>%s AND proxy_id<=%s
                ORDER BY proxy_id LIMIT %s""", (after_id, max_id, limit))
        else:
            cursor.execute("""SELECT p.proxy_id,p.connection_key,p.address,p.port,p.connection_settings
            FROM app_meta.proxy_test_state q JOIN public.proxies p USING(proxy_id)
            WHERE q.next_test_at<=%s ORDER BY q.next_test_at,q.proxy_id LIMIT %s""", (now, limit))
        last_id = None
        for identifier, key, address, port, settings in cursor:
            transport, options = unpack_connection_settings(settings)
            output.write(json.dumps({"id": identifier, "key": bytes(key).hex(), "address": address,
                "port": port, "protocol": transport, "settings": options}, separators=(",", ":")) + "\n")
            count += 1
            last_id = identifier
        output.flush()
        os.fsync(output.fileno())
    if not count:
        source.unlink()
        folder.rmdir()
        return None
    manifest = {"schema_version": 1, "records": count, "last_proxy_id": last_id, "shards": [
        {"file": source.name, "records": count, "bytes": source.stat().st_size, "sha256": digest(source)}]}
    write_json(folder / "manifest.json", manifest)
    return manifest


def ranked_rows(conn, *, limit=0, max_age_seconds=86400):
    if limit < 0 or max_age_seconds <= 0:
        raise ValueError("Invalid pool limits")
    with conn.transaction(), conn.cursor(name="ranked_pool_export") as cursor:
        cursor.itersize = 10000
        cursor.execute("""SELECT h.proxy_id,encode(p.connection_key,'hex'),h.working_protocol,
            coalesce(h.youtube_score,50)/(1+q.failure_streak) AS rank_score,
            h.youtube_successful_data_received,h.youtube_responses_received,
            q.last_response_at,q.latency_ms,h.youtube_last_http_status
        FROM app_meta.proxy_test_state q JOIN public.proxy_health h USING(proxy_id)
        JOIN public.proxies p USING(proxy_id)
        WHERE q.last_response_at>=statement_timestamp()-make_interval(secs=>%s)
          AND h.working_protocol IS NOT NULL
        ORDER BY rank_score DESC,h.youtube_weighted_successful_data_received DESC,
            q.latency_ms ASC NULLS LAST,q.last_response_at DESC,h.proxy_id
        LIMIT %s""", (max_age_seconds, limit or None))
        yield from cursor


def export_pool(conn, home=SERVICE_DIR, *, limit=0, max_age_seconds=86400):
    """Export IDs and ranks; connection credentials remain in the private database."""
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    with file_lock(home / "pool.lock"):
        path = home / "ranked-proxies.jsonl"
        temporary = path.with_name(path.name + ".partial")
        count = 0
        with temporary.open("w") as output:
            os.chmod(temporary, 0o600)
            for row in ranked_rows(conn, limit=limit, max_age_seconds=max_age_seconds):
                identifier, key, protocol, score, successes, responses, last_response, latency, status = row
                output.write(json.dumps({"proxy_id": identifier, "connection_key": key, "protocol": protocol,
                    "score": float(score), "successful_data_received": successes, "youtube_responses": responses,
                    "last_response_at": last_response.isoformat(), "latency_ms": latency,
                    "last_http_status": status}, separators=(",", ":")) + "\n")
                count += 1
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
        result = {"updated_at": utcnow().isoformat(), "exported": count, "limit": limit,
                  "max_age_seconds": max_age_seconds, "path": str(path), "sha256": digest(path)}
        write_json(home / "pool-status.json", result)
        return result


def pool_status(conn):
    result = dict(zip(("scheduled", "tested", "responded", "due"), conn.execute("""SELECT count(*),
        count(*) FILTER(WHERE checked_at IS NOT NULL),count(*) FILTER(WHERE last_response_at IS NOT NULL),
        count(*) FILTER(WHERE next_test_at<=statement_timestamp()) FROM app_meta.proxy_test_state""").fetchone()))
    result["fresh_responders"] = conn.execute("""SELECT count(*) FROM app_meta.proxy_test_state
        WHERE last_response_at>=statement_timestamp()-interval '24 hours'""").fetchone()[0]
    return result
