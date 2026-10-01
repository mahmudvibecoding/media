"""Best-effort proxy statistics; collection never waits for a database write.

Only per-configuration aggregates are retained in memory. The daemon writer owns
one immutable batch at a time while collection continues accumulating new results.
Shutdown does not wait for it: unacknowledged statistics may be lost.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import math
from pathlib import Path
import re
import threading
from urllib.parse import unquote, urlsplit
import uuid

import psycopg

from proxy_formats import Proxy


ROOT = Path(__file__).resolve().parent
IMPORT_LOCK = 6389247650123
HALF_LIFE_SECONDS = 3600.0
WEBSITES = frozenset({'youtube'})
# Target tunnel refusals and ambiguous adapter errors belong to the website.
# Only observations that identify the proxy endpoint, TLS or authentication
# can set the shared connection error. Keep the database check in sync.
CONNECTION_ERROR_PATTERN = (
    r'(?:(?:resolve|connect|proxy_tls):[a-z][a-z0-9_]*'
    r'|proxy_handshake:(?:proxy_authentication_required|proxy_authentication_failed|proxy_http_407'
    r'|not_socks5|invalid_socks5_address|socks4_reply_92|socks4_reply_93|socks5_reply_7|socks5_reply_8))'
)


def proxy_connection_error(error):
    return error if error is not None and re.fullmatch(CONNECTION_ERROR_PATTERN, error) else None


def website_prefix(website):
    """Only fixed, installed column groups may be selected by a collector."""
    if website not in WEBSITES:
        raise ValueError('Add the website column group before recording its statistics')
    return website + '_'


def decayed_weight(weight, measured_at, at_time):
    if not weight or measured_at is None:
        return 0.0
    if at_time <= measured_at:
        return float(weight)
    log_weight = math.log(weight) - math.log(2) * (at_time-measured_at).total_seconds() / HALF_LIFE_SECONDS
    return 0.0 if log_weight < -700 else math.exp(log_weight)


@dataclass(frozen=True, repr=False)
class ProxyTarget:
    """Identity hashes only; never put proxy credentials in statistics or logs."""
    protocol: str
    candidate_keys: tuple[bytes, ...]
    proxy_id: int | None = None

    @classmethod
    def from_catalog(cls, proxy_id, connection_key, working_protocol):
        """Keep original identity when a local bridge supplies the connection."""
        key = bytes(connection_key)
        if type(proxy_id) is not int or proxy_id < 1 or len(key) != 32 or not working_protocol:
            raise ValueError('Invalid catalog proxy identity')
        return cls(working_protocol, (key,), proxy_id)

    @classmethod
    def from_url(cls, value, proxy_id=None):
        if value is None:
            return None
        parsed = urlsplit(value)
        if parsed.scheme not in ('http', 'https') or not parsed.hostname:
            raise ValueError('Statistics require an HTTP(S) proxy URL')
        if parsed.path not in ('', '/') or parsed.query:
            raise ValueError('Proxy URL contains unsupported connection options')
        if proxy_id is not None and (type(proxy_id) is not int or proxy_id < 1):
            raise ValueError('Proxy identifiers must be positive integers')
        address = parsed.hostname.rstrip('.')
        try:
            address = ipaddress.ip_address(address).compressed
        except ValueError:
            address = address.encode('idna').decode().lower()
        settings = {}
        if parsed.username is not None:
            settings['username'] = unquote(parsed.username)
        if parsed.password is not None:
            settings['password'] = unquote(parsed.password)
        protocols = ('http', 'unknown', 'https') if parsed.scheme == 'http' else ('https', 'unknown')
        keys = tuple(Proxy(address, parsed.port or (443 if parsed.scheme == 'https' else 80),
                           protocol, settings).key for protocol in protocols)
        return cls(parsed.scheme, keys, proxy_id)


@dataclass(frozen=True)
class AttemptOutcome:
    checked_at: datetime
    request_sent: bool
    http_status: int | None
    data_received: bool | None
    connected: bool | None = None
    connection_error: str | None = None
    website_error: str | None = None

    @property
    def connection_confirmed(self):
        # A sent target request also proves a proxy connection was available.
        return self.connected is True or self.request_sent or self.http_status is not None

    def validate(self):
        if self.checked_at.tzinfo is None:
            raise ValueError('Invalid statistics timestamp')
        if type(self.request_sent) is not bool or (self.data_received is not None and type(self.data_received) is not bool):
            raise ValueError('Invalid statistics flags')
        if self.http_status is not None and (type(self.http_status) is not int or not 100 <= self.http_status <= 599):
            raise ValueError('Invalid response status')
        if self.http_status is not None and not self.request_sent:
            raise ValueError('A response requires a sent request')
        if self.data_received and self.http_status is None:
            raise ValueError('Data success requires a response')
        if self.connected is not None and type(self.connected) is not bool:
            raise ValueError('Invalid connection observation')
        if self.connected is False and self.request_sent:
            raise ValueError('A sent request requires a connection')
        for error in (self.connection_error, self.website_error):
            if error is not None and (not isinstance(error, str)
                    or re.fullmatch(r'[a-z][a-z0-9_]*:[a-z][a-z0-9_]*', error) is None):
                raise ValueError('Statistics errors must be stage:code labels')


@dataclass
class Aggregate:
    connection_attempts: int = 0
    successful_connections: int = 0
    last_connected_at: datetime | None = None
    last_connection_error: str | None = None
    requests_sent: int = 0
    responses_received: int = 0
    successful_data_received: int = 0
    last_website_error: str | None = None
    first_checked_at: datetime | None = None
    checked_at: datetime | None = None
    last_http_status: int | None = None
    last_response_at: datetime | None = None
    working_protocol: str | None = None
    weighted_attempts: float = 0.0
    weighted_successful_data_received: float = 0.0
    last_scored_attempt_at: datetime | None = None

    def add(self, outcome, protocol):
        outcome.validate()
        self.connection_attempts += 1
        self.successful_connections += outcome.connection_confirmed
        if outcome.connection_confirmed:
            self.last_connected_at = max(self.last_connected_at or outcome.checked_at, outcome.checked_at)
        self.requests_sent += outcome.request_sent
        self.responses_received += outcome.http_status is not None
        self.successful_data_received += outcome.data_received is True
        self.first_checked_at = min(self.first_checked_at or outcome.checked_at, outcome.checked_at)
        if self.checked_at is None or outcome.checked_at >= self.checked_at:
            self.checked_at = outcome.checked_at
            self.last_http_status = outcome.http_status
            self.last_connection_error = proxy_connection_error(outcome.connection_error)
            self.last_website_error = outcome.website_error or outcome.connection_error
        if outcome.http_status is not None and (self.last_response_at is None or outcome.checked_at >= self.last_response_at):
            self.last_response_at, self.working_protocol = outcome.checked_at, protocol
        if outcome.data_received is not None:
            at = max(self.last_scored_attempt_at or outcome.checked_at, outcome.checked_at)
            self.weighted_attempts = (
                decayed_weight(self.weighted_attempts, self.last_scored_attempt_at, at)
                + decayed_weight(1, outcome.checked_at, at))
            self.weighted_successful_data_received = (
                decayed_weight(self.weighted_successful_data_received, self.last_scored_attempt_at, at)
                + decayed_weight(int(outcome.data_received), outcome.checked_at, at))
            self.last_scored_attempt_at = at

    def merge(self, other):
        for name in ('connection_attempts', 'successful_connections', 'requests_sent', 'responses_received',
                     'successful_data_received'):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.first_checked_at = min(self.first_checked_at or other.first_checked_at, other.first_checked_at)
        if self.checked_at is None or other.checked_at >= self.checked_at:
            self.checked_at = other.checked_at
            self.last_http_status = other.last_http_status
            self.last_connection_error = other.last_connection_error
            self.last_website_error = other.last_website_error
        if other.last_connected_at is not None:
            self.last_connected_at = max(self.last_connected_at or other.last_connected_at, other.last_connected_at)
        if other.last_response_at is not None and (self.last_response_at is None or other.last_response_at >= self.last_response_at):
            self.last_response_at, self.working_protocol = other.last_response_at, other.working_protocol
        times = [value for value in (self.last_scored_attempt_at, other.last_scored_attempt_at) if value is not None]
        if times:
            at = max(times)
            for name in ('weighted_attempts', 'weighted_successful_data_received'):
                setattr(self, name, decayed_weight(getattr(self, name), self.last_scored_attempt_at, at)
                        + decayed_weight(getattr(other, name), other.last_scored_attempt_at, at))
            self.last_scored_attempt_at = at

    def row(self, proxy_id):
        return (proxy_id, self.checked_at, self.first_checked_at,
                self.working_protocol, self.last_http_status, self.last_response_at,
                self.connection_attempts, self.requests_sent, self.responses_received,
                self.successful_data_received, self.weighted_attempts,
                self.weighted_successful_data_received, self.last_scored_attempt_at,
                self.successful_connections, self.last_connected_at, self.last_connection_error, self.last_website_error)


@dataclass(frozen=True)
class StatisticsBatch:
    aggregates: dict[ProxyTarget, Aggregate] = field(repr=False)
    # Created once and reused verbatim after uncertain acknowledgements.
    key: bytes = field(default_factory=lambda: hashlib.sha256(uuid.uuid4().bytes).digest())
    website: str = 'youtube'


def write_batch(conn, batch):
    """One ordered, atomic import. Old overlapping batches are skipped, never recounted."""
    site = website_prefix(batch.website)
    with conn.transaction():
        if not conn.execute('SELECT pg_try_advisory_xact_lock(%s)', (IMPORT_LOCK,)).fetchone()[0]:
            raise RuntimeError('Another statistics import or migration is running')
        keys = list({key for target in batch.aggregates for key in target.candidate_keys})
        identities = {bytes(key): proxy_id for proxy_id, key in conn.execute(
            'SELECT proxy_id,connection_key FROM proxies WHERE connection_key=ANY(%s)', (keys,))}
        resolved, unmatched = {}, 0
        for target, aggregate in batch.aggregates.items():
            candidates = [identities[key] for key in target.candidate_keys if key in identities]
            if target.proxy_id is not None:
                proxy_id = target.proxy_id if target.proxy_id in candidates else None
            elif target.candidate_keys[0] in identities:
                proxy_id = identities[target.candidate_keys[0]]
            else:
                proxy_id = candidates[0] if len(candidates) == 1 else None
            if proxy_id is None:
                unmatched += aggregate.connection_attempts
                continue
            resolved.setdefault(proxy_id, Aggregate()).merge(aggregate)
        conn.execute('''CREATE TEMP TABLE IF NOT EXISTS proxy_statistics_batch (
            proxy_id BIGINT PRIMARY KEY, checked_at TIMESTAMPTZ, first_checked_at TIMESTAMPTZ,
            working_protocol TEXT, last_http_status SMALLINT,
            last_response_at TIMESTAMPTZ,
            connection_attempts BIGINT, requests_sent BIGINT, responses_received BIGINT,
            successful_data_received BIGINT, weighted_attempts DOUBLE PRECISION,
            weighted_successful_data_received DOUBLE PRECISION, last_scored_attempt_at TIMESTAMPTZ,
            successful_connections BIGINT, last_connected_at TIMESTAMPTZ, last_connection_error TEXT,
            last_website_error TEXT
        ) ON COMMIT DROP''')
        conn.execute('TRUNCATE proxy_statistics_batch')
        with conn.cursor().copy('COPY proxy_statistics_batch FROM STDIN') as copy:
            for proxy_id, aggregate in resolved.items():
                copy.write_row(aggregate.row(proxy_id))
        replayed, stale = conn.execute(f'''SELECT
            coalesce(sum(s.connection_attempts) FILTER (WHERE h.{site}last_import_key=%s),0),
            coalesce(sum(s.connection_attempts) FILTER
                (WHERE h.{site}last_import_key IS DISTINCT FROM %s
                 AND s.first_checked_at<=h.{site}last_attempt_at),0)
            FROM proxy_statistics_batch s JOIN proxy_stats h USING(proxy_id)''', (batch.key, batch.key)).fetchone()
        changed = conn.execute(f'''INSERT INTO proxy_stats AS h
            (proxy_id,connection_attempts,successful_connections,last_connection_attempt_at,
             last_connected_at,last_connection_error,
             {site}last_attempt_at,working_protocol,{site}last_http_status,
             {site}last_response_at,{site}requests_sent,{site}responses_received,
             {site}successful_data_received,{site}weighted_attempts,{site}weighted_successful_data_received,
             {site}last_scored_attempt_at,{site}last_import_key,{site}last_error)
            SELECT s.proxy_id,s.connection_attempts,s.successful_connections,s.checked_at,
                   s.last_connected_at,s.last_connection_error,
                   s.checked_at,s.working_protocol,s.last_http_status,
                   s.last_response_at,s.requests_sent,s.responses_received,
                   s.successful_data_received,s.weighted_attempts,s.weighted_successful_data_received,
                   s.last_scored_attempt_at,%s,s.last_website_error
            FROM proxy_statistics_batch s LEFT JOIN proxy_stats previous USING(proxy_id)
            WHERE previous.{site}last_import_key IS DISTINCT FROM %s
              AND (previous.{site}last_attempt_at IS NULL
                   OR s.first_checked_at>previous.{site}last_attempt_at)
            ON CONFLICT (proxy_id) DO UPDATE SET
                connection_attempts=h.connection_attempts+excluded.connection_attempts,
                successful_connections=h.successful_connections+excluded.successful_connections,
                last_connection_attempt_at=greatest(h.last_connection_attempt_at,excluded.last_connection_attempt_at),
                last_connected_at=greatest(h.last_connected_at,excluded.last_connected_at),
                last_connection_error=CASE WHEN h.last_connection_attempt_at IS NULL
                    OR excluded.last_connection_attempt_at>=h.last_connection_attempt_at
                    THEN excluded.last_connection_error ELSE h.last_connection_error END,
                {site}last_attempt_at=excluded.{site}last_attempt_at,
                working_protocol=coalesce(excluded.working_protocol,h.working_protocol),
                {site}last_http_status=excluded.{site}last_http_status,
                {site}last_response_at=coalesce(excluded.{site}last_response_at,h.{site}last_response_at),
                {site}last_error=excluded.{site}last_error,
                {site}requests_sent=h.{site}requests_sent+excluded.{site}requests_sent,
                {site}responses_received=h.{site}responses_received+excluded.{site}responses_received,
                {site}successful_data_received=h.{site}successful_data_received+excluded.{site}successful_data_received,
                {site}weighted_attempts=CASE WHEN excluded.{site}last_scored_attempt_at IS NULL
                    THEN h.{site}weighted_attempts
                    ELSE coalesce(proxy_decayed_weight(h.{site}weighted_attempts,
                         h.{site}last_scored_attempt_at,excluded.{site}last_scored_attempt_at),0)
                         +excluded.{site}weighted_attempts END,
                {site}weighted_successful_data_received=CASE WHEN excluded.{site}last_scored_attempt_at IS NULL
                    THEN h.{site}weighted_successful_data_received
                    ELSE coalesce(proxy_decayed_weight(h.{site}weighted_successful_data_received,
                         h.{site}last_scored_attempt_at,excluded.{site}last_scored_attempt_at),0)
                         +excluded.{site}weighted_successful_data_received END,
                {site}last_scored_attempt_at=coalesce(excluded.{site}last_scored_attempt_at,h.{site}last_scored_attempt_at),
                {site}last_import_key=excluded.{site}last_import_key
        ''', (batch.key, batch.key)).rowcount
        return dict(updated_proxies=changed, replayed_attempts=int(replayed), stale_attempts=int(stale),
                    unmatched_attempts=unmatched,
                    accepted_attempts=sum(a.connection_attempts for a in resolved.values())-int(stale))


def write_to_database(batch):
    with psycopg.connect(dbname='proxy', user='mahmud', host=str(ROOT/'.local/postgres/socket'),
                         port=5432, connect_timeout=5, autocommit=True,
                         application_name='proxy-statistics') as conn:
        return write_batch(conn, batch)


class ProxyStatistics:
    """Non-waiting producer and a separate daemon database writer.

    The interval only batches statistics writes. It never changes collection
    concurrency, request timing or retry policy. There is no queue size limit.
    """
    def __init__(self, targets, *, website='youtube', writer=write_to_database, interval=1.0):
        website_prefix(website)
        self.website = website
        self.targets = targets
        self.writer, self.interval = writer, interval
        self._pending = {}
        self._lock = threading.Lock()
        self._closing = threading.Event()
        self._thread = None
        self.recorded_attempts = self.dropped_attempts = self.accepted_attempts = 0
        self.unmatched_attempts = self.stale_attempts = self.write_failures = 0
        self.last_error_type = None
        self._inflight_attempts = 0

    @classmethod
    def from_urls(cls, urls, proxy_ids=None, **kwargs):
        identifiers = json.loads(proxy_ids) if proxy_ids is not None else [None]*len(urls)
        if not isinstance(identifiers, list) or len(identifiers) != len(urls):
            raise ValueError('MEDIA_PROXY_IDS must match the configured proxy URLs')
        targets = []
        for url, proxy_id in zip(urls, identifiers):
            try:
                targets.append(ProxyTarget.from_url(url, proxy_id))
            except (ValueError, UnicodeError):
                targets.append(None)
        return cls(targets, **kwargs)

    def start(self):
        self._thread = threading.Thread(target=self._run, name='proxy-statistics', daemon=True)
        self._thread.start()
        return self

    def observer(self, number):
        target = self.targets[number]
        return lambda outcome: self.record(target, outcome)

    def record(self, target, outcome):
        # Even the memory lock is best effort. It is never held around I/O.
        if target is None or self._closing.is_set() or not self._lock.acquire(blocking=False):
            self.dropped_attempts += 1
            return False
        try:
            outcome.validate()
            self._pending.setdefault(target, Aggregate()).add(outcome, target.protocol)
            self.recorded_attempts += 1
            return True
        except Exception:
            self.dropped_attempts += 1
            return False
        finally:
            self._lock.release()

    def _take_batch(self):
        with self._lock:
            pending, self._pending = self._pending, {}
        if not pending:
            return None
        self._inflight_attempts = sum(a.connection_attempts for a in pending.values())
        return StatisticsBatch(pending, website=self.website)

    def _run(self):
        batch = None
        while True:
            closing = self._closing.wait(self.interval)
            batch = batch or self._take_batch()
            if batch is None:
                if closing:
                    return
                continue
            try:
                result = self.writer(batch)
            except Exception as exc:
                self.write_failures += 1
                self.last_error_type = type(exc).__name__
                if closing:
                    return
                continue
            self.accepted_attempts += result.get('accepted_attempts', 0)
            self.unmatched_attempts += result.get('unmatched_attempts', 0)
            self.stale_attempts += result.get('stale_attempts', 0)
            self.last_error_type = None
            self._inflight_attempts = 0
            batch = None
            # On close, drain what is already in memory without making the
            # caller join this thread or wait for the database.

    def close(self):
        self._closing.set()

    def snapshot(self):
        return dict(mode='best_effort', recorded_attempts=self.recorded_attempts,
                    acknowledged_attempts=self.accepted_attempts, dropped_attempts=self.dropped_attempts,
                    unmatched_attempts=self.unmatched_attempts, stale_attempts=self.stale_attempts,
                    write_failures=self.write_failures, last_error_type=self.last_error_type,
                    unacknowledged_attempts=max(0, self.recorded_attempts-self.accepted_attempts
                                               -self.unmatched_attempts-self.stale_attempts))
