from dataclasses import asdict
import unittest

import httpx

from collect_video_metadata import fetch_metadata, has_metadata
from collection_policy import (CollectionOutcome, PERFORMANCE_MAX_AGE, ProxyPerformance,
                               classify_outcome, video_error_reason)
from proxy_statistics import Aggregate


class OutcomeTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_video_response_is_neutral_for_proxy_score(self):
        observed = []
        payload = {'playabilityStatus': {'status': 'LOGIN_REQUIRED', 'reason': 'Private video'}}
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=payload))) as client:
            result = await fetch_metadata(client, '00000000000', retries=0, on_attempt=observed.append)
        self.assertFalse(has_metadata(result))
        self.assertEqual(video_error_reason(result), 'VIDEO_PRIVATE')
        self.assertIsNone(observed[0].data_received)
        aggregate = Aggregate()
        aggregate.add(observed[0], 'http')
        self.assertEqual(aggregate.successful_connections, 1)
        self.assertEqual(aggregate.responses_received, 1)
        self.assertEqual(aggregate.successful_data_received, 0)
        self.assertEqual(aggregate.weighted_attempts, 0)
        self.assertEqual(aggregate.last_website_error, 'video:video_private')

    async def test_bot_challenge_is_retryable_and_affects_request_success_score(self):
        observed = []
        payload = {'playabilityStatus': {'status': 'LOGIN_REQUIRED',
                   'reason': "Sign in to confirm you’re not a bot"}}
        async with httpx.AsyncClient(transport=httpx.MockTransport(
                lambda _: httpx.Response(200, json=payload))) as client:
            result = await fetch_metadata(client, '00000000000', retries=0, on_attempt=observed.append)
        decision = classify_outcome(result, asdict(observed[0]), has_metadata(result))
        self.assertEqual(decision.category, 'response_error')
        self.assertIsNone(decision.video_error)
        self.assertIs(decision.proxy_success, False)
        self.assertIs(observed[0].data_received, False)

    def test_regional_or_unknown_player_error_is_not_terminal_evidence(self):
        for reason in ('This video is not available in your country', 'Try again later', 'Unknown error'):
            with self.subTest(reason=reason):
                result = {'status': 'ok', 'http_status': 200, 'player_status': 'UNPLAYABLE',
                          'player_reason': reason}
                self.assertIsNone(video_error_reason(result))

    def test_failed_http_observation_cannot_confirm_video_unavailability(self):
        result = {'status': 'ok', 'http_status': 200, 'player_status': 'ERROR',
                  'player_reason': 'Video unavailable'}
        decision = classify_outcome(result, {'http_status': 503}, False)
        self.assertIsNone(decision.video_error)
        self.assertEqual(decision.category, 'http_error')


class PerformanceTests(unittest.TestCase):
    def test_unavailable_and_local_errors_do_not_lower_recent_success(self):
        profile = ProxyPerformance()
        profile.observe(CollectionOutcome('data', True), 0.25, 100)
        score, samples = profile.score, profile.samples
        profile.observe(CollectionOutcome('video_error', None, 'VIDEO_UNAVAILABLE'), 0.01, 101)
        self.assertEqual((profile.score, profile.samples), (score, samples))
        self.assertEqual(profile.cooldown_until, 101)
        profile.observe(CollectionOutcome('local_error', None), 10, 102)
        self.assertEqual((profile.score, profile.samples, profile.failure_streak), (score, samples, 0))

    def test_consecutive_temporary_failures_back_off_and_success_resets_them(self):
        profile = ProxyPerformance()
        for failures in range(10):
            profile.observe(CollectionOutcome('connection_error', False), 5, 100 + failures)
            self.assertGreater(profile.cooldown_until, 100 + failures)
            self.assertLessEqual(profile.cooldown_until - (100 + failures), 60)
        failed_score = profile.score
        profile.observe(CollectionOutcome('data', True), 0.3, 200)
        self.assertGreater(profile.score, failed_score)
        self.assertEqual(profile.failure_streak, 0)
        self.assertEqual(profile.cooldown_until, 200)

    def test_old_performance_expires_on_resume(self):
        old = ProxyPerformance(quality=1, latency_seconds=0.01, samples=1000, updated_at=100)
        restored = ProxyPerformance.restore(asdict(old), {'successes': 100000}, 101 + PERFORMANCE_MAX_AGE)
        self.assertEqual(restored.samples, 0)
        self.assertIsNone(restored.latency_seconds)
        self.assertLess(restored.score, old.score)

    def test_clock_change_cannot_extend_restored_cooldown_past_one_minute(self):
        profile = ProxyPerformance(quality=0.1, updated_at=1000, cooldown_until=1060)
        resumed = ProxyPerformance.restore(asdict(profile), {}, 900)
        self.assertLessEqual(resumed.cooldown_until - 900, 60)

    def test_fast_failures_cannot_improve_measured_response_speed(self):
        profile = ProxyPerformance()
        profile.observe(CollectionOutcome('data', True), 5, 100)
        before = profile.score
        profile.observe(CollectionOutcome('http_error', False), 0.001, 101)
        self.assertEqual(profile.latency_seconds, 5)
        self.assertLess(profile.score, before)

    def test_only_failed_requests_cannot_outrank_slow_successful_proxy(self):
        slow, failed = ProxyPerformance(), ProxyPerformance()
        slow.observe(CollectionOutcome('data', True), 20, 100)
        failed.observe(CollectionOutcome('http_error', False), 0.001, 100)
        self.assertGreater(slow.score, failed.score)

    def test_missing_or_invalid_timing_cannot_poison_selection(self):
        profile = ProxyPerformance()
        for seconds in (None, float('nan'), float('inf'), -1, True, '0.01'):
            profile.observe(CollectionOutcome('data', True), seconds, 100)
        self.assertIsNone(profile.latency_seconds)
        self.assertGreater(profile.score, 0)


if __name__ == '__main__':
    unittest.main()
