"""Read published proxy formats as data; never connect to or execute their contents."""
from __future__ import annotations

import ast
import base64
import collections
import csv
import hashlib
import io
import ipaddress
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from urllib.parse import parse_qs, unquote, urlsplit

import yaml

PARSER_VERSION = 4
ALIASES = {
    'socks': 'socks5', 'socks5h': 'socks5', 'socks4a': 'socks4',
    'ss': 'shadowsocks', 'ssr': 'shadowsocksr', 'hy': 'hysteria', 'hy2': 'hysteria2',
    'wg': 'wireguard', 'mtp': 'mtproto',
}
PROTOCOLS = {
    'unknown', 'http', 'https', 'socks4', 'socks5', 'shadowsocks',
    'shadowsocksr', 'vmess', 'vless', 'trojan', 'hysteria', 'hysteria2',
    'tuic', 'wireguard', 'anytls', 'mtproto', 'ssh', 'snell', 'mieru',
    'naive', 'hysteria-2', 'http2', 'masque', 'trusttunnel', 'warp',
}
DISPLAY_KEYS = {
    'name', 'ps', 'remarks', 'remark', 'tag', 'tags', 'label',
    'country', 'country_code', 'countryCode', 'city', 'region', 'anonymity',
    'latency', 'delay', 'speed', 'uptime', 'last_checked', 'lastChecked',
    'last_seen', 'lastSeen', 'responseTime', 'google', 'isGoogle',
    'org', 'asn', 'isp', 'created_at', 'updated_at', 'alive', 'working',
    'score', 'geo', 'id', '_id', 'source', 'sources', 'checked', 'ipVersion',
}
CONFIG_METADATA_KEYS = {
    'dns', 'rules', 'routing', 'proxy-groups', 'rule-providers', 'proxy-providers',
    'inbounds', 'listeners', 'tun', 'profile', 'sniffer',
}
HOST_KEYS = ('server', 'ip', 'host', 'ipAddress', 'ip_address', 'address', 'hostname', 'add')
PORT_KEYS = ('port', 'server_port', 'proxy_port')
URI_RE = re.compile(
    r'(?i)(?:https?://t\.me/(?:proxy|socks)\?|tg://(?:proxy|socks)\?'
    r'|(?:https?|socks4a?|socks5h?|socks|ssr?|vmess|vless|trojan|hysteria2?|hy2?|tuic|wireguard|anytls|ssh|warp)://)'
    r'[^\s<>"\x00-\x20]+'
)
ENDPOINT_RE = re.compile(r'^(\[[0-9a-fA-F:.]+\]|[^\s:/,;@]+):(\d{1,5})(?::([^:]*):(.*))?$')
BASE64_RE = re.compile(r'^[A-Za-z0-9+/=_\-\s]+$')


def protocol_name(value):
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    value = ALIASES.get(value, value)
    return value if value in PROTOCOLS else None


def public_address(value):
    value = str(value).strip().strip('[]').rstrip('.')
    if not value or any(c in value for c in '/@?#%:'):
        # A colon is valid only as part of a literal IPv6 address.
        try:
            ip = ipaddress.ip_address(value)
            return ip.compressed if ip.is_global else None
        except ValueError:
            return None
    try:
        ip = ipaddress.ip_address(value)
        return ip.compressed if ip.is_global else None
    except ValueError:
        if re.fullmatch(r'[\d.]+', value):
            return None
    try:
        value = value.encode('idna').decode('ascii').lower()
    except (UnicodeError, ValueError):
        return None
    if len(value) > 253 or '.' not in value:
        return None
    if value in {'example.com', 'example.net', 'example.org'} or value.endswith(
        ('.local', '.localhost', '.internal', '.lan', '.home', '.test', '.example', '.invalid',
         '.example.com', '.example.net', '.example.org', '.onion')
    ):
        return None
    if not all(re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', s) for s in value.split('.')):
        return None
    return value


def decode_base64(value):
    value = re.sub(r'\s+', '', unquote(value))
    return base64.b64decode(value + '=' * (-len(value) % 4), altchars=b'-_', validate=True)


def clean_json(value, depth=0):
    if depth > 24:
        raise ValueError('settings_too_deep')
    if isinstance(value, dict):
        return {str(k): clean_json(v, depth + 1) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean_json(v, depth + 1) for v in value]
    if value is None or isinstance(value, (str, int, bool, float)):
        return value
    return str(value)


def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def pack_connection_settings(protocol, settings):
    """Keep the transport and its options together without changing proxy identity."""
    if not isinstance(protocol, str) or not protocol or not isinstance(settings, dict):
        raise ValueError('Invalid proxy transport configuration')
    encoded = canonical_json(settings)
    # JSONB cannot contain U+0000, including in an object key. Preserve those
    # options as escaped JSON text inside the otherwise ordinary envelope.
    return {'transport': protocol, 'options': encoded if '\\u0000' in encoded else settings}


def unpack_connection_settings(configuration):
    """Recover exactly the transport/options used to calculate connection_key."""
    if not isinstance(configuration, dict):
        raise ValueError('Invalid proxy transport configuration')
    protocol, settings = configuration.get('transport'), configuration.get('options')
    if isinstance(settings, str):
        try:
            settings = json.loads(settings)
        except (ValueError, TypeError):
            raise ValueError('Invalid proxy connection options') from None
    if not isinstance(protocol, str) or not protocol or not isinstance(settings, dict):
        raise ValueError('Invalid proxy transport configuration')
    return protocol, settings


@dataclass(repr=False)
class Proxy:
    address: str
    port: int
    protocol: str
    settings: dict

    @property
    def key(self):
        return hashlib.sha256(canonical_json([
            self.address, self.port, self.protocol, self.settings
        ]).encode()).digest()

    def as_dict(self):
        return {'address': self.address, 'port': self.port, 'protocol': self.protocol,
                'settings': self.settings}


@dataclass
class Parsed:
    proxies: dict[bytes, Proxy] = field(default_factory=dict, repr=False)
    entries_found: int = 0
    invalid: collections.Counter = field(default_factory=collections.Counter)
    formats: collections.Counter = field(default_factory=collections.Counter)
    warnings: collections.Counter = field(default_factory=collections.Counter)

    def add(self, address, port, protocol='unknown', settings=None):
        address = public_address(address)
        try:
            if isinstance(port, bool) or not re.fullmatch(r'\d{1,5}', str(port).strip()):
                raise ValueError()
            port = int(port)
            if not address or not 1 <= port <= 65535:
                raise ValueError()
        except (ValueError, TypeError):
            self.invalid['invalid_address_or_port'] += 1
            return
        protocol = protocol_name(protocol) or 'unknown'
        try:
            proxy = Proxy(address, port, protocol, clean_json(settings or {}))
            key = proxy.key
        except (ValueError, TypeError, RecursionError):
            self.invalid['invalid_settings'] += 1
            return
        self.entries_found += 1
        self.proxies.setdefault(key, proxy)

    def summary(self):
        return {
            'parser_version': PARSER_VERSION,
            'entries_found': self.entries_found,
            'unique_entries': len(self.proxies),
            'duplicates_in_list': self.entries_found - len(self.proxies),
            'invalid_entries': sum(self.invalid.values()),
            'invalid_reasons': dict(self.invalid),
            'formats': dict(self.formats), 'warnings': dict(self.warnings),
            'protocols': dict(collections.Counter(p.protocol for p in self.proxies.values())),
        }


class Parser:
    def __init__(self, hints=()):
        hints = {protocol_name(x) for x in hints} - {None, 'unknown'}
        self.default_protocol = next(iter(hints)) if len(hints) == 1 else 'unknown'
        self.result = Parsed()

    def uri(self, uri):
        uri = uri.strip().rstrip('),;').replace('&amp;', '&')
        # Some published lists append a country code after the port.
        uri = re.sub(r'(?i)^((?:https?|socks4|socks5)://[^/:?#@]+:\d{1,5}):[A-Z]{2}$', r'\1', uri)
        try:
            p = urlsplit(uri)
            scheme = p.scheme.lower()
            if scheme == 'tg' or (p.hostname == 't.me' and p.path in ('/proxy', '/socks')):
                params = {k: v[-1] for k, v in parse_qs(p.query, keep_blank_values=True).items()}
                host, port = params.pop('server', None), params.pop('port', None)
                is_socks = (p.hostname == 'socks') if scheme == 'tg' else p.path == '/socks'
                proto = 'socks5' if is_socks else 'mtproto'
                if not is_socks and not params.get('secret'):
                    raise ValueError('missing_secret')
                if is_socks:
                    if 'user' in params:
                        params['username'] = params.pop('user')
                    if 'pass' in params:
                        params['password'] = params.pop('pass')
                self.result.add(host, port, proto, params)
            elif scheme == 'vmess' and '@' not in p.netloc:
                obj = json.loads(decode_base64(uri[8:].split('#')[0]).decode())
                if not isinstance(obj, dict):
                    raise ValueError()
                host, port = obj.get('add', obj.get('server')), obj.get('port')
                settings = {k: v for k, v in obj.items() if k not in {'add', 'server', 'port', 'ps', 'v'} }
                if 'id' in settings:
                    settings['uuid'] = settings.pop('id')
                self.result.add(host, port, 'vmess', settings)
            elif scheme == 'ssr':
                encoded = uri[6:].split('#')[0]
                decoded = unquote(encoded) if encoded.count(':') >= 5 else decode_base64(encoded).decode()
                core, _, query = decoded.partition('/?')
                host, port, obfs_protocol, cipher, obfs, password = core.rsplit(':', 5)
                settings = {'password': decode_base64(password).decode(), 'cipher': cipher,
                            'obfs': obfs, 'ssr_protocol': obfs_protocol}
                settings.update({k: v[-1] for k, v in parse_qs(query, keep_blank_values=True).items()
                                 if k not in {'remarks', 'group'}})
                self.result.add(host, port, 'shadowsocksr', settings)
            else:
                if scheme == 'ss':
                    # SIP002 supports both base64 userinfo and legacy base64 of the entire endpoint.
                    core = uri[5:].split('#')[0]
                    core, _, query = core.partition('?')
                    if '@' not in core:
                        core = decode_base64(core).decode()
                    userinfo, _, endpoint = core.rpartition('@')
                    if ':' not in userinfo:
                        userinfo = decode_base64(userinfo).decode()
                    cipher, sep, password = unquote(userinfo).partition(':')
                    if not sep:
                        raise ValueError()
                    p = urlsplit('ss://' + endpoint)
                    settings = {'cipher': cipher, 'password': password}
                    settings.update({k: v[-1] if len(v) == 1 else v
                                     for k, v in parse_qs(query, keep_blank_values=True).items()})
                    proto = 'shadowsocks'
                else:
                    proto = protocol_name(scheme)
                    if not proto:
                        raise ValueError('unsupported_uri')
                    if proto in {'http', 'https', 'socks4', 'socks5'} and (p.path not in ('', '/') or p.port is None):
                        # A download URL or website URL is not itself a proxy.
                        return False
                    settings = {k: v[-1] if len(v) == 1 else v
                                for k, v in parse_qs(p.query, keep_blank_values=True).items()}
                    if p.username is not None:
                        user = unquote(p.username)
                        key = 'uuid' if proto in {'vless', 'vmess', 'tuic'} else (
                            'password' if proto in {'trojan', 'hysteria2', 'hysteria', 'anytls'} else 'username')
                        settings[key] = user
                    if p.password is not None:
                        settings['password'] = unquote(p.password)
                    if p.path not in ('', '/'):
                        settings['path'] = p.path
                self.result.add(p.hostname, p.port, proto, settings)
            self.result.formats['uri'] += 1
            return True
        except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError):
            self.result.invalid['invalid_uri'] += 1
            return True

    def structured(self, obj, hint=None, depth=0, ancestry=frozenset()):
        if depth > 32 or id(obj) in ancestry:
            self.result.warnings['recursive_or_deep_structure'] += 1
            return
        if isinstance(obj, str):
            self.line(obj, hint)
            return
        if not isinstance(obj, (dict, list)):
            return
        ancestry = ancestry | {id(obj)}
        if isinstance(obj, list):
            for value in obj:
                self.structured(value, hint, depth + 1, ancestry)
            return
        declared_kind = obj.get('protocol', obj.get('type'))
        if isinstance(declared_kind, str) and declared_kind in {'direct', 'block', 'dns', 'freedom', 'blackhole'}:
            return
        # A complete connect string carries credentials/transport options that
        # the accompanying address and port summary may omit.
        if isinstance(obj.get('connect_string'), str):
            self.line(obj['connect_string'], hint)
            return
        # V2Ray/Xray JSON outbounds place endpoint and credentials under settings.
        proto = protocol_name(obj.get('protocol'))
        if proto and isinstance(obj.get('settings'), dict):
            settings = obj['settings']
            endpoints = settings.get('vnext', settings.get('servers'))
            if isinstance(endpoints, list):
                for endpoint in endpoints:
                    if not isinstance(endpoint, dict):
                        continue
                    users = endpoint.get('users') or [{}]
                    for user in users:
                        extra = {k: v for k, v in obj.items() if k not in {'tag', 'protocol', 'settings'}}
                        extra['endpoint'] = {k: v for k, v in endpoint.items() if k not in {'address', 'server', 'port', 'users'}}
                        if user:
                            extra['user'] = user
                        self.result.add(endpoint.get('address', endpoint.get('server')), endpoint.get('port'), proto, extra)
                self.result.formats['v2ray_json'] += 1
                return
        host = next((obj[k] for k in HOST_KEYS if k in obj and isinstance(obj[k], (str, int))), None)
        port = next((obj[k] for k in PORT_KEYS if k in obj), None)
        embedded_auth = {}
        if isinstance(host, str) and port is None:
            endpoint = ENDPOINT_RE.fullmatch(host)
            if endpoint:
                host, port, user, password = endpoint.groups()
                if user is not None:
                    embedded_auth = {'username': user, 'password': password}
            elif URI_RE.fullmatch(host):
                self.uri(host)
                return
        if host is not None and port is not None:
            declared = obj.get('protocols', obj.get('protocol', obj.get('type', obj.get('scheme'))))
            if isinstance(declared, str):
                declared = re.split(r'[, /|]+', declared)
            choices = [protocol_name(x) for x in declared] if isinstance(declared, list) else []
            choices = [x for x in choices if x]
            if not choices:
                choices = ['mtproto'] if obj.get('secret') else [hint or self.default_protocol]
            configuration = any(p not in ('unknown','http','https','socks4','socks5') for p in choices)
            if configuration:
                excluded = set(HOST_KEYS) | set(PORT_KEYS) | DISPLAY_KEYS | {'type', 'protocol', 'protocols', 'scheme'}
                if any(p in ('vmess','vless','tuic') for p in choices):
                    excluded.discard('id')
                extra = {k: v for k, v in obj.items() if k not in excluded}
            else:
                extra = {k: obj[k] for k in ('username', 'password', 'user', 'pass', 'secret', 'uuid',
                                            'auth', 'tls', 'ssl', 'sni', 'servername', 'headers', 'cipher',
                                            'network', 'ws-opts', 'grpc-opts', 'reality-opts', 'transport') if k in obj}
                if 'user' in extra:
                    extra.setdefault('username', extra.pop('user'))
                if 'pass' in extra:
                    extra.setdefault('password', extra.pop('pass'))
            for choice in sorted(set(choices)):
                self.result.add(host, port, choice, {**embedded_auth, **extra})
            self.result.formats['structured_endpoint'] += 1
            return
        # Some APIs store an entire endpoint under the proxy key.
        if isinstance(obj.get('proxy'), str):
            self.line(obj['proxy'], protocol_name(obj.get('protocol')) or hint)
        for key, value in obj.items():
            if key == 'proxy' or key in DISPLAY_KEYS or key in CONFIG_METADATA_KEYS or key in {'url', 'website', 'homepage'}:
                continue
            if isinstance(key, str) and ENDPOINT_RE.fullmatch(key):
                claimed = protocol_name(value.get('type', value.get('protocol'))) if isinstance(value, dict) else None
                self.line(key, claimed or hint)
                continue
            if isinstance(value, (dict, list)):
                self.structured(value, protocol_name(key) or hint, depth + 1, ancestry)
            elif isinstance(value, str) and key in {'uri', 'link', 'proxy_url', 'data', 'list', 'proxies', 'proxy_list', 'content', 'body', 'result'}:
                for line in value.splitlines():
                    self.line(line, hint)

    def line(self, line, hint=None):
        line = line.strip().lstrip('\ufeff')
        if not line or line.startswith(('#', '//', ';')):
            return
        if re.search(r'<br\s*/?>', line, re.I):
            for part in re.split(r'<br\s*/?>', line, flags=re.I):
                self.line(part, hint)
            return
        matches = list(URI_RE.finditer(line))
        if matches:
            for match in matches:
                uri = match.group()
                if match.start() and line[match.start() - 1] == "'":
                    uri = uri.split("'", 1)[0]
                self.uri(uri)
            return
        if line.startswith('{'):
            try:
                self.structured(json.loads(line), hint)
                self.result.formats['json_lines'] += 1
                return
            except (ValueError, RecursionError):
                self.result.warnings['invalid_json_line'] += 1
                return
        # ip:port or ip:port:user:password, optionally followed by public-list annotations.
        token = line.split()[0].strip('"\'').rstrip(',;')
        for separator in ('@', '|'):
            endpoint, found, declared = token.rpartition(separator)
            if found and protocol_name(declared) in {'http', 'https', 'socks4', 'socks5'} and ENDPOINT_RE.fullmatch(endpoint):
                self.line(endpoint, protocol_name(declared))
                return
        country_suffix = re.fullmatch(r"(\[[0-9a-fA-F:.]+\]|[^\s:/,;@]+):(\d{1,5}):[A-Za-z][A-Za-z .()'\-]{1,60}", line)
        if country_suffix:
            self.result.add(country_suffix[1], country_suffix[2], hint or self.default_protocol)
            self.result.formats['country_suffix'] += 1
            return
        m = ENDPOINT_RE.fullmatch(token)
        if m:
            host, port, user, password = m.groups()
            settings = {} if user is None else {'username': user, 'password': password}
            self.result.add(host, port, hint or self.default_protocol, settings)
            self.result.formats['plain'] += 1
            return
        if '@' in token and '://' not in token:
            try:
                p = urlsplit('unknown://' + token)
                if p.port and p.username is not None:
                    self.result.add(p.hostname, p.port, hint or self.default_protocol,
                                    {'username': unquote(p.username), 'password': unquote(p.password or '')})
                    self.result.formats['plain_auth'] += 1
                    return
            except ValueError:
                self.result.invalid['invalid_endpoint'] += 1
                return
        # Checked-list text sometimes uses: protocol country latency ip:port.
        words = line.split()
        if len(words) > 1 and re.fullmatch(r'[\U0001F1E6-\U0001F1FF]{2}', words[0]):
            if ENDPOINT_RE.fullmatch(words[1]):
                self.line(words[1], hint)
                return
        if ' -> ' in line and ENDPOINT_RE.fullmatch(words[-1]):
            self.line(words[-1], hint)
            return
        if len(words) > 1 and protocol_name(words[0]) in {'http','https','socks4','socks5'}:
            for word in reversed(words[1:]):
                if ENDPOINT_RE.fullmatch(word):
                    self.line(word, protocol_name(words[0]))
                    return
        # Plain whitespace, semicolon or comma separated address and port columns.
        columns = re.split(r'[,;\t ]+', line.strip('"'))
        if len(columns) >= 2 and re.fullmatch(r'\d{1,5}', columns[1].strip('"')):
            self.result.add(columns[0].strip('"'), columns[1].strip('"'), hint or self.default_protocol)
            self.result.formats['columns'] += 1
        elif re.match(r'^(?:\d{1,3}\.){3}\d{1,3}:', line):
            self.result.invalid['invalid_endpoint'] += 1

    def parse(self, body, content_type='', depth=0):
        if depth > 2:
            self.result.warnings['nested_encoding_limit'] += 1
            return self.result
        text = body.decode('utf-8-sig', errors='replace').strip()
        if not text:
            return self.result
        if '<html' in text[:3000].lower() or '<!doctype html' in text[:3000].lower():
            self.result.warnings['html_response'] += 1
            return self.result
        if text.startswith(('[', '{')):
            try:
                self.structured(json.loads(text))
                self.result.formats['json_document'] += 1
                return self.result
            except (ValueError, RecursionError):
                # A few publishers emit Python None/True/False in otherwise
                # JSON-shaped dictionaries. Literal evaluation never runs code.
                if re.search(r'\b(?:None|True|False)\b', text):
                    try:
                        self.structured(ast.literal_eval(text))
                        self.result.formats['python_literal'] += 1
                        return self.result
                    except (ValueError, SyntaxError, RecursionError):
                        pass
        if text.startswith('<'):
            if '<!DOCTYPE' in text.upper() or '<!ENTITY' in text.upper():
                self.result.warnings['xml_external_declaration'] += 1
                return self.result
            try:
                root = ET.fromstring(text)
                for elem in root.iter():
                    value = (elem.text or '').strip()
                    if ENDPOINT_RE.fullmatch(value) or URI_RE.fullmatch(value):
                        self.line(value)
                    values = dict(elem.attrib)
                    values.update({child.tag.rsplit('}', 1)[-1]: (child.text or '').strip()
                                   for child in elem if len(child) == 0})
                    if any(k in values for k in HOST_KEYS) and any(k in values for k in PORT_KEYS):
                        self.structured(values)
                self.result.formats['xml_document'] += 1
                return self.result
            except ET.ParseError:
                self.result.warnings['invalid_xml'] += 1
                return self.result
        if re.search(r'(?m)^\s*(?:proxies|outbounds|servers|https?|socks[45]):(?=\s|$)|^\s*-\s*(?:name|server|type|ip|host|address):', text[:100000]):
            try:
                for data in yaml.load_all(text, Loader=yaml.CSafeLoader):
                    self.structured(data)
                self.result.formats['yaml_document'] += 1
                return self.result
            except (yaml.YAMLError, ValueError, RecursionError):
                self.result.warnings['invalid_yaml'] += 1
                self.recover_yaml_proxies(text)
                return self.result
        # Only treat a whole response as a subscription encoding when it contains no punctuation outside base64.
        if len(text) > 24 and BASE64_RE.fullmatch(text):
            try:
                decoded = decode_base64(text)
                if b'://' in decoded or re.search(rb'\d+\.\d+\.\d+\.\d+:\d+', decoded):
                    self.result.formats['base64_subscription'] += 1
                    return self.parse(decoded, depth=depth + 1)
            except (ValueError, UnicodeError):
                self.result.warnings['invalid_base64'] += 1
        first_line = text.split('\n', 1)[0]
        delimiter = max((',', ';', '\t'), key=first_line.count)
        if delimiter in first_line:
            headers = [s.strip().strip('"').lower().replace(' ', '_') for s in first_line.split(delimiter)]
            if headers and all(protocol_name(h) in {'http', 'https', 'socks4', 'socks5'} for h in headers):
                for row in csv.reader(io.StringIO(text), delimiter=delimiter):
                    for protocol, cell in zip(headers, row):
                        self.line(cell.strip(), protocol_name(protocol))
                self.result.formats['csv_protocol_columns'] += 1
                return self.result
            if any(k in headers for k in ('ip', 'host', 'ip_address', 'ipaddress', 'server', 'address', 'proxy', 'ip:port', 'ip_port', 'proxy_address')):
                try:
                    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
                    for row in reader:
                        row = {str(k).strip().lower().replace(' ', '_'): v for k, v in row.items() if k is not None}
                        if 'ipaddress' in row:
                            row['ip_address'] = row.pop('ipaddress')
                        for alias in ('ip:port', 'ip_port', 'proxy_address'):
                            if alias in row:
                                row.setdefault('address', row.pop(alias))
                        self.structured(row)
                    self.result.formats['csv_document'] += 1
                    return self.result
                except csv.Error:
                    self.result.warnings['invalid_csv'] += 1
            else:
                # Several large CSV publishers omit a header and place the
                # proxy URI in the first column, followed by country/latency.
                def endpoint_cell(cell):
                    if ENDPOINT_RE.fullmatch(cell.strip()):
                        return True
                    try:
                        p = urlsplit(cell.strip())
                        return (protocol_name(p.scheme) in {'http','https','socks4','socks5'}
                                and p.port is not None and p.path in ('','/') and not p.query)
                    except ValueError:
                        return False
                try:
                    first_fields = next(csv.reader([first_line], delimiter=delimiter))
                    if first_fields and endpoint_cell(first_fields[0]):
                        for row in csv.reader(io.StringIO(text), delimiter=delimiter):
                            for cell in row:
                                if endpoint_cell(cell):
                                    self.line(cell.strip())
                        self.result.formats['csv_without_header'] += 1
                        return self.result
                except csv.Error:
                    self.result.warnings['invalid_csv'] += 1
        for line in text.splitlines():
            self.line(line)
        if not self.result.entries_found:
            self.result.warnings['no_supported_entries'] += 1
        return self.result

    def recover_yaml_proxies(self, text):
        """Recover separately valid proxy records when unrelated YAML is malformed."""
        lines = text.splitlines(keepends=True)
        for start, line in enumerate(lines):
            if not re.match(r'^proxies:\s*(?:#.*)?$', line.strip('\r\n')):
                continue
            block = []
            for following in lines[start + 1:]:
                if re.match(r'^[A-Za-z_][\w-]*:', following) or following.startswith('---'):
                    break
                block.append(following)
            first = next((re.match(r'^(\s*)-\s+', s) for s in block if re.match(r'^(\s*)-\s+', s)), None)
            if not first:
                continue
            indent = first.group(1)
            starts = [i for i, s in enumerate(block) if re.match(r'^' + re.escape(indent) + r'-\s+', s)]
            for i, at in enumerate(starts):
                end = starts[i+1] if i+1 < len(starts) else len(block)
                candidate = 'proxies:\n' + ''.join(block[at:end])
                try:
                    self.structured(yaml.load(candidate, Loader=yaml.CSafeLoader))
                    self.result.formats['recovered_yaml_record'] += 1
                except (yaml.YAMLError, ValueError, RecursionError):
                    # Malformed control characters in display names do not
                    # change connection settings. Remove only a complete name
                    # property line, leaving every connection option untouched.
                    without_names = re.sub(r'(?m)^(\s*(?:-\s*)?)name:.*$', r'\1name: ""', candidate)
                    try:
                        self.structured(yaml.load(without_names, Loader=yaml.CSafeLoader))
                        self.result.formats['recovered_yaml_record'] += 1
                    except (yaml.YAMLError, ValueError, RecursionError):
                        self.result.invalid['invalid_yaml_record'] += 1


def parse_proxies(body, hints=(), content_type=''):
    return Parser(hints).parse(body, content_type)
