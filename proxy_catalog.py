"""Use catalog configurations through the tester's reusable protocol transports."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
from pathlib import Path
import secrets
import ssl
import tempfile

import httpx
import psycopg

from proxy_statistics import ProxyTarget, website_prefix
from proxy_formats import unpack_connection_settings
from runtime_config import BRIDGE_BINARY, STATE_DIR, connect_database


ROOT = Path(__file__).resolve().parent
DEFAULT_BRIDGE_BINARY = BRIDGE_BINARY
SUPPORTED_PROTOCOLS = frozenset(('http', 'https', 'socks4', 'socks5', 'vmess', 'vless', 'trojan',
    'shadowsocks', 'shadowsocksr', 'hysteria', 'hysteria2', 'tuic', 'wireguard', 'anytls'))


@dataclass(frozen=True, repr=False)
class CatalogProxy:
    proxy_id: int
    connection_key: bytes
    address: str
    port: int
    transport: str
    working_protocol: str
    settings: dict = field(repr=False)

    @property
    def statistics_target(self):
        return ProxyTarget.from_catalog(self.proxy_id, self.connection_key, self.working_protocol)

    def bridge_record(self):
        return dict(id=self.proxy_id, key=self.connection_key.hex(), address=self.address, port=self.port,
                    protocol=self.transport, working_protocol=self.working_protocol, settings=self.settings)


def load_catalog(proxy_ids=None, protocols=None, *, website='youtube', connection=None):
    """Load known responders, or explicitly selected IDs, preserving full settings."""
    if connection is None:
        with connect_database("proxy", application_name='metadata-proxy-catalog') as conn:
            return load_catalog(proxy_ids, protocols, website=website, connection=conn)
    site = website_prefix(website)
    identifiers = list(dict.fromkeys(proxy_ids or []))
    if any(type(value) is not int or value < 1 for value in identifiers):
        raise ValueError('Catalog IDs must be positive integers')
    chosen_protocols = set(protocols or SUPPORTED_PROTOCOLS)
    if not chosen_protocols <= SUPPORTED_PROTOCOLS:
        raise ValueError('Unsupported catalog protocol selection')
    where = 'p.proxy_id=ANY(%s)' if identifiers else f's.{site}last_response_at IS NOT NULL'
    rows = connection.execute(f'''SELECT p.proxy_id,p.connection_key,p.address,p.port,
               s.working_protocol,p.connection_settings
        FROM proxies p LEFT JOIN proxy_stats s USING(proxy_id)
        WHERE {where}
        ORDER BY (s.{site}last_http_status IS NOT NULL) DESC,
                 s.{site}last_response_at DESC NULLS LAST,p.proxy_id''',
        (identifiers,) if identifiers else ()).fetchall()
    if identifiers and {r[0] for r in rows} != set(identifiers):
        raise ValueError('A selected proxy ID is missing from the catalog')
    result = []
    for proxy_id, key, address, port, working, configuration in rows:
        transport, settings = unpack_connection_settings(configuration)
        working = working or transport
        if working not in chosen_protocols:
            if identifiers:
                raise ValueError(f'Selected proxy {proxy_id} has no supported selected protocol')
            continue
        result.append(CatalogProxy(proxy_id, bytes(key), address, port, transport, working, settings))
    if not result:
        raise ValueError('No compatible catalog configurations matched')
    return result


class CatalogClients:
    """Create each proxy's HTTP client lazily, reusing clients and a shared CA store."""
    def __init__(self, proxies, concurrency, binary=DEFAULT_BRIDGE_BINARY, *,
                 connect_timeout=10, request_timeout=20, per_proxy_connections=None, keepalive_expiry=5,
                 http2=True):
        self.proxies, self.concurrency, self.binary = proxies, concurrency, Path(binary)
        self.connect_timeout, self.request_timeout = connect_timeout, request_timeout
        self.per_proxy_connections = per_proxy_connections or concurrency
        self.keepalive_expiry = keepalive_expiry
        self.http2 = http2
        self.process = None
        self._directory = None
        self._clients = {}
        self._tls = ssl.create_default_context()

    async def __aenter__(self):
        if not self.binary.is_file():
            raise RuntimeError('Build the catalog bridge: cd proxy-tester && go build -o ../.local/bin/proxy-tester .')
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        self._directory = tempfile.TemporaryDirectory(prefix='proxy-bridge-', dir=STATE_DIR)
        folder = Path(self._directory.name)
        manifest, auth_file = folder/'catalog.jsonl', folder/'auth'
        self._token = secrets.token_hex(32)
        with manifest.open('w') as output:
            for proxy in self.proxies:
                output.write(json.dumps(proxy.bridge_record(), separators=(',', ':')) + '\n')
        manifest.chmod(0o600)
        auth_file.write_text(self._token)
        auth_file.chmod(0o600)
        try:
            self.process = await asyncio.create_subprocess_exec(str(self.binary), 'bridge', '--input', str(manifest),
                '--auth-file', str(auth_file), '--connect-timeout', f'{self.connect_timeout:g}s',
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
            ready = json.loads(await asyncio.wait_for(self.process.stdout.readline(), timeout=30))
            host, port = ready['address'].rsplit(':', 1)
            if (ready.get('event') != 'ready' or host != '127.0.0.1' or not 0 < int(port) < 65536
                    or ready.get('configurations') != len(self.proxies)):
                raise ValueError('Invalid bridge startup response')
            self._address = ready['address']
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    def __len__(self):
        return len(self.proxies)

    def __getitem__(self, index):
        if self.process is None or self.process.returncode is not None:
            raise RuntimeError('Catalog transport bridge is unavailable')
        if not 0 <= index < len(self.proxies):
            raise IndexError(index)
        if index not in self._clients:
            proxy = self.proxies[index]
            client = httpx.AsyncClient(http2=self.http2, trust_env=False, follow_redirects=False,
                proxy=httpx.Proxy('http://' + self._address, auth=(str(proxy.proxy_id), self._token)),
                verify=self._tls, headers={'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
                timeout=httpx.Timeout(self.request_timeout, connect=self.connect_timeout),
                limits=httpx.Limits(max_connections=self.per_proxy_connections,
                                   max_keepalive_connections=self.per_proxy_connections,
                                   keepalive_expiry=self.keepalive_expiry))
            client.catalog_bridge = self
            self._clients[index] = client
        return self._clients[index]

    def local_error(self, error):
        return (self.process is None or self.process.returncode is not None
                or isinstance(error, httpx.ProxyError) and str(error).endswith(' Proxy Bridge Local Error'))

    async def __aexit__(self, *exc):
        try:
            await asyncio.gather(*(client.aclose() for client in self._clients.values()), return_exceptions=True)
            if self.process is not None and self.process.returncode is None:
                try:
                    self.process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(self.process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    self.process.kill()
                    await self.process.wait()
        finally:
            if self._directory is not None:
                self._directory.cleanup()
