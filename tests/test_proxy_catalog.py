"""Catalog transport lifecycle, attempt attribution and local-failure behavior."""
import asyncio
from contextlib import nullcontext
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import httpx

from collect_video_metadata import collect, fetch_metadata, fetch_metadata_with_proxies, has_metadata
from proxy_catalog import CatalogClients, CatalogProxy, DEFAULT_BRIDGE_BINARY
from proxy_statistics import ProxyTarget


VIDEO = 'RtXBV0X1v1Q'
DATA = {'videoDetails': {'videoId': VIDEO, 'title': 'Available metadata'},
        'playabilityStatus': {'status': 'OK'}}
PROXY = CatalogProxy(55, b'x'*32, '127.0.0.1', 1, 'http', 'http', {})


class CatalogCollectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_assignment_wraps_and_credits_only_the_assigned_proxy(self):
        calls, outcomes = [], [[], [], []]
        def handler(number, success):
            def handle(request):
                calls.append(number)
                return httpx.Response(200, json=DATA if success else {'playabilityStatus': {'status': 'LOGIN_REQUIRED'}})
            return handle
        clients = [httpx.AsyncClient(transport=httpx.MockTransport(handler(i, i == 0))) for i in range(3)]
        try:
            failed = await fetch_metadata_with_proxies(clients, VIDEO,
                on_attempts=[items.append for items in outcomes], proxy_index=2)
            self.assertEqual(calls, [2])
            self.assertEqual([len(items) for items in outcomes], [0, 0, 1])
            result = await fetch_metadata_with_proxies(clients, VIDEO,
                on_attempts=[items.append for items in outcomes], proxy_index=3)
        finally:
            await asyncio.gather(*(client.aclose() for client in clients))
        self.assertEqual(calls, [2, 0])
        self.assertEqual((failed['attempts'], failed['proxy_number']), (1, 3))
        self.assertFalse(has_metadata(failed))
        self.assertEqual((result['attempts'], result['proxy_number']), (1, 1))
        self.assertTrue(outcomes[0][0].data_received)
        self.assertEqual(outcomes[1], [])
        self.assertFalse(outcomes[2][0].data_received)

    async def test_bridge_local_failure_is_excluded_but_upstream_failure_is_scored(self):
        pool = CatalogClients([PROXY], 128)
        pool.process = SimpleNamespace(returncode=None)
        outcomes = []
        def handle(request):
            raise httpx.ProxyError('503 Proxy Bridge Local Error')
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            client.catalog_bridge = pool
            await fetch_metadata(client, VIDEO, retries=0, on_attempt=outcomes.append)
        self.assertEqual(outcomes, [])
        def handle(request):
            raise httpx.ProxyError('502 Bad Gateway')
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            client.catalog_bridge = pool
            await fetch_metadata(client, VIDEO, retries=0, on_attempt=outcomes.append)
        self.assertEqual(len(outcomes), 1)
        self.assertFalse(outcomes[0].request_sent)
        self.assertFalse(outcomes[0].data_received)
        self.assertIsNone(outcomes[0].http_status)

    async def test_catalog_mode_keeps_collecting_if_statistics_setup_fails(self):
        class Pool:
            def __init__(self, *args):
                self.clients = [httpx.AsyncClient(transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json=DATA)))]
            async def __aenter__(self):
                return self.clients
            async def __aexit__(self, *exc):
                await self.clients[0].aclose()
        conn = Mock()
        conn.execute.return_value.fetchone.return_value = (True,)
        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(video_id=None, limit=100, concurrency=128, retries=10,
                client_version='test', output=Path(directory)/'run', proxy_catalog=True)
            with patch.dict(os.environ, {}, clear=True), \
                    patch('collect_video_metadata.load_catalog', return_value=[PROXY]), \
                    patch('collect_video_metadata.CatalogClients', Pool), \
                    patch('collect_video_metadata.open_database', return_value=nullcontext(conn)), \
                    patch('collect_video_metadata.select_videos', return_value=[(VIDEO, 'video')]), \
                    patch('collect_video_metadata.save_metadata', return_value=1) as saved, \
                    patch('collect_video_metadata.ProxyStatistics.start', side_effect=OSError('offline')), \
                    patch('builtins.print'):
                self.assertEqual(await collect(args), 0)
            self.assertEqual(saved.call_count, 1)
            summary = json.loads((args.output/'summary.json').read_text())
            self.assertEqual((summary['saved'], summary['http_attempts'], summary['proxy_count']), (1, 1, 1))
            self.assertEqual(summary['retry_policy'], 'one_attempt_per_video')
            self.assertEqual(summary['proxy_statistics']['setup_error_type'], 'OSError')
            self.assertEqual(summary['proxy_outcomes_by_protocol']['http'], dict(connection_attempts=1,
                youtube_requests_sent=1, youtube_responses_received=1, youtube_successful_data_received=1))

    async def test_catalog_identity_does_not_include_local_bridge_address_or_credentials(self):
        proxy = CatalogProxy(55, b'x'*32, '8.8.8.8', 1234, 'vless', 'vless', {'uuid': 'private'})
        target = proxy.statistics_target
        self.assertEqual((target.proxy_id, target.protocol, target.candidate_keys), (55, 'vless', (b'x'*32,)))
        self.assertNotIn('private', repr(proxy))
        self.assertNotIn('private', repr(target))
        with self.assertRaises(ValueError):
            ProxyTarget.from_catalog(55, b'x', 'vless')


@unittest.skipUnless(DEFAULT_BRIDGE_BINARY.is_file(), 'Build the Go bridge to run process integration tests')
class BridgeProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_bridge_lifecycle_private_manifest_and_no_false_youtube_response(self):
        pool = CatalogClients([PROXY], 128)
        observed = []
        async with pool:
            directory = Path(pool._directory.name)
            process = pool.process
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual((directory/'catalog.jsonl').stat().st_mode & 0o777, 0o600)
            self.assertIs(pool[0], pool[0])
            # A private upstream is rejected before any outbound TCP connection.
            await fetch_metadata(pool[0], VIDEO, retries=0, on_attempt=observed.append)
        self.assertEqual(process.returncode, 0)
        self.assertFalse(directory.exists())
        self.assertEqual(len(observed), 1)
        self.assertFalse(observed[0].request_sent)
        self.assertIsNone(observed[0].http_status)

    async def test_invalid_adapter_configuration_never_counts_as_a_proxy_attempt(self):
        invalid = CatalogProxy(56, b'y'*32, '8.8.8.8', 443, 'vless', 'vless', {})
        observed = []
        async with CatalogClients([invalid], 128) as pool:
            result = await fetch_metadata(pool[0], VIDEO, retries=0, on_attempt=observed.append)
        self.assertEqual(result['status'], 'error')
        self.assertEqual(observed, [])


if __name__ == '__main__':
    unittest.main()
