"""Import local proxy observations and rank every verified YouTube responder."""
from dataclasses import asdict, dataclass
from email.utils import parsedate_to_datetime
import importlib.util
import json
import math
import os
from pathlib import Path
import uuid

from psycopg import sql

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


def apply_observations(conn, observations, journal_key, run_id, *, retest_seconds=21600, quality=False, _staged=False, _retry_delays=None, _parallel=False):
    """Receipts make late and replayed batches safe without resetting newer fields."""
    if len(journal_key) != 32 or not 0 < retest_seconds <= 365 * 86400:
        raise ValueError("Invalid observation import settings")
    rows = [] if _staged else [observation.row() for observation in observations]
    count = conn.execute("SELECT count(*) FROM import_proxy_results").fetchone()[0] if _staged else len(rows)
    with conn.transaction():
        lock = "pg_try_advisory_xact_lock_shared" if _parallel else "pg_try_advisory_xact_lock"
        if not conn.execute(sql.SQL("SELECT {}(%s)").format(sql.Identifier(lock)), (IMPORT_LOCK,)).fetchone()[0]:
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
        conn.execute("ANALYZE service_observations")
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


def record_round_results(conn, round_id, pass_number):
    """Record one validated pass and publish only complete three-check results."""
    round_id = str(uuid.UUID(round_id))
    if type(pass_number) is not int or not 1 <= pass_number <= 3:
        raise ValueError("A test pass must be 1, 2, or 3")
    bit = 1 << (pass_number - 1)
    conn.execute("""INSERT INTO app_meta.proxy_round_results AS r
        (round_id,proxy_id,passes,response_count,response_time_ms,checked_at,
         last_response_at,working_protocol,last_http_status)
        SELECT %s,proxy_id,%s,(http_status IS NOT NULL)::integer,
            CASE WHEN http_status IS NOT NULL THEN total_ms ELSE 0 END,checked_at,
            CASE WHEN http_status IS NOT NULL THEN checked_at END,detected_protocol,http_status
        FROM import_proxy_results ORDER BY proxy_id
        ON CONFLICT (round_id,proxy_id) DO UPDATE SET
            passes=r.passes | excluded.passes,
            response_count=r.response_count+excluded.response_count,
            response_time_ms=r.response_time_ms+excluded.response_time_ms,
            checked_at=greatest(r.checked_at,excluded.checked_at),
            last_response_at=greatest(r.last_response_at,excluded.last_response_at),
            working_protocol=CASE WHEN excluded.last_response_at IS NOT NULL
                AND (r.last_response_at IS NULL OR excluded.last_response_at>=r.last_response_at)
                THEN excluded.working_protocol ELSE r.working_protocol END,
            last_http_status=CASE WHEN excluded.last_response_at IS NOT NULL
                AND (r.last_response_at IS NULL OR excluded.last_response_at>=r.last_response_at)
                THEN excluded.last_http_status ELSE r.last_http_status END
        WHERE (r.passes & excluded.passes)=0""", (round_id, bit))
    conn.execute("""INSERT INTO app_meta.proxy_pool_results AS p
        (proxy_id,round_id,response_count,average_response_ms,completed_at,
         last_response_at,working_protocol,last_http_status)
        SELECT r.proxy_id,r.round_id,r.response_count,
            r.response_time_ms/nullif(r.response_count,0),r.checked_at,
            r.last_response_at,r.working_protocol,r.last_http_status
        FROM app_meta.proxy_round_results r JOIN import_proxy_results s USING(proxy_id)
        WHERE r.round_id=%s AND r.passes=7 ORDER BY r.proxy_id
        ON CONFLICT(proxy_id) DO UPDATE SET
            round_id=excluded.round_id,response_count=excluded.response_count,
            average_response_ms=excluded.average_response_ms,completed_at=excluded.completed_at,
            last_response_at=excluded.last_response_at,working_protocol=excluded.working_protocol,
            last_http_status=excluded.last_http_status
        WHERE excluded.completed_at>p.completed_at""", (round_id,))


def import_test_journal(conn, path, *, retest_seconds=21600, round_id=None, pass_number=None, parallel=False):
    if (round_id is None) != (pass_number is None):
        raise ValueError("A scored batch needs both its run and pass number")
    # Large batches otherwise spill sorts and joins with PostgreSQL's 4 MB default.
    conn.execute("SET work_mem = '128MB'")
    conn.execute("SET jit = off")
    # Validate and parse each line once, then transfer staged rows inside PostgreSQL.
    retry_delays = {}
    def observe(result, checked):
        replies = [a for a in result.get("attempts", []) if a.get("status") == "responds"]
        if replies:
            seconds = retry_after(replies[0].get("retry_after"), checked)
            if seconds:
                retry_delays[result["id"]] = seconds
    report = _journal.import_journal(conn, path, 10000, stage_only=True, on_result=observe)
    conn.execute("ANALYZE import_proxy_results")
    with conn.transaction():
        result = apply_observations(conn, (), report["journal_key"], report["run"],
                                    retest_seconds=retest_seconds, _staged=True, _retry_delays=retry_delays, _parallel=parallel)
        if round_id is not None:
            record_round_results(conn, round_id, pass_number)
    return result


def export_due(conn, folder, *, limit=2000, now=None, after_id=None, max_id=None):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=False, mode=0o700)
    now = now or utcnow()
    if after_id is not None:
        # PostgreSQL already has the JSON. COPY avoids decoding and encoding a
        # million option dictionaries in Python for every batch and every pass.
        source = folder / "input.jsonl"
        selection = sql.SQL("""SELECT proxy_id,connection_key,address,port,connection_settings
            FROM public.proxies WHERE proxy_id>{} AND proxy_id<={}
            ORDER BY proxy_id LIMIT {}""").format(sql.Literal(after_id), sql.Literal(max_id), sql.Literal(limit))
        query = sql.SQL("""COPY (SELECT json_build_object(
            'id',proxy_id,'key',encode(connection_key,'hex'),'address',address,'port',port,
            'protocol',connection_settings->>'transport','settings',
            CASE WHEN jsonb_typeof(connection_settings->'options')='string'
                 THEN (connection_settings->>'options')::json
                 ELSE (connection_settings->'options')::json END)
            FROM ({}) selected) TO STDOUT
            WITH (FORMAT csv, DELIMITER E'\\x02', QUOTE E'\\x01', ESCAPE E'\\x01')""").format(selection)
        with conn.transaction(), source.open("wb", buffering=1024*1024) as output:
            os.chmod(source, 0o600)
            with conn.cursor().copy(query) as copy:
                for block in copy:
                    output.write(block)
            count, last_id = conn.execute(sql.SQL("SELECT count(*),max(proxy_id) FROM ({}) selected").format(selection)).fetchone()
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


def ranked_rows(conn, *, limit=0):
    if limit < 0:
        raise ValueError("Invalid pool limits")
    with conn.transaction(), conn.cursor(name="ranked_pool_export") as cursor:
        cursor.itersize = 10000
        cursor.execute("""SELECT r.proxy_id,encode(p.connection_key,'hex'),r.working_protocol,
            r.response_count,r.average_response_ms,r.last_response_at,r.last_http_status,
            r.round_id,r.completed_at
        FROM app_meta.proxy_pool_results r JOIN public.proxies p USING(proxy_id)
        WHERE r.response_count>0
        ORDER BY r.response_count DESC,r.average_response_ms,r.proxy_id
        LIMIT %s""", (limit or None,))
        yield from cursor


def export_pool(conn, home=SERVICE_DIR, *, limit=0):
    """Export IDs and ranks; connection credentials remain in the private database."""
    home = Path(home)
    home.mkdir(parents=True, exist_ok=True, mode=0o700)
    with file_lock(home / "pool.lock"):
        path = home / "ranked-proxies.jsonl"
        temporary = path.with_name(path.name + ".partial")
        count = 0
        with temporary.open("w") as output:
            os.chmod(temporary, 0o600)
            for row in ranked_rows(conn, limit=limit):
                identifier, key, protocol, score, latency, last_response, status, round_id, completed_at = row
                output.write(json.dumps({"proxy_id": identifier, "connection_key": key, "protocol": protocol,
                    "score": score, "youtube_responses": score, "checks": 3,
                    "average_response_ms": latency, "last_response_at": last_response.isoformat(),
                    "last_http_status": status, "test_run_id": str(round_id),
                    "tested_at": completed_at.isoformat()}, separators=(",", ":")) + "\n")
                count += 1
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
        result = {"updated_at": utcnow().isoformat(), "exported": count, "limit": limit,
                  "scoring": "youtube_responses_last_three", "path": str(path), "sha256": digest(path)}
        write_json(home / "pool-status.json", result)
        return result


def pool_status(conn):
    result = dict(zip(("scheduled", "tested", "responded", "due"), conn.execute("""SELECT count(*),
        count(*) FILTER(WHERE checked_at IS NOT NULL),count(*) FILTER(WHERE last_response_at IS NOT NULL),
        count(*) FILTER(WHERE next_test_at<=statement_timestamp()) FROM app_meta.proxy_test_state""").fetchone()))
    result["fresh_responders"] = conn.execute("""SELECT count(*) FROM app_meta.proxy_test_state
        WHERE last_response_at>=statement_timestamp()-interval '24 hours'""").fetchone()[0]
    result["scored"] = conn.execute("SELECT count(*) FROM app_meta.proxy_pool_results").fetchone()[0]
    result["working"] = conn.execute("SELECT count(*) FROM app_meta.proxy_pool_results WHERE response_count>0").fetchone()[0]
    return result
