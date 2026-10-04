"""Separate video availability from request failures and score recent proxy results."""
from dataclasses import dataclass
import math


PERFORMANCE_MAX_AGE = 3600
VIDEO_CONFIRMATION_MAX_AGE = 3600
RECENT_WEIGHT = 0.2


def video_error_reason(result):
    """Only a parsed YouTube response can provide video-level evidence."""
    if result.get('status') != 'ok' or result.get('http_status') != 200:
        return None
    if result.get('access_challenge'):
        return None
    if 'stats_missing' in result:
        evidence = result.get('evidence') or {}
        if not result.get('video_id') or evidence.get('response_video_id') != result['video_id']:
            return None
        missing = set(result.get('stats_missing') or [])
        if missing == {'VIDEO_UNAVAILABLE'} and result.get('stats') is None:
            return 'VIDEO_UNAVAILABLE'
        counts = result.get('stats') or {}
        if (missing == {'VIEWS_NOT_EXPOSED', 'LIKES_NOT_EXPOSED'} and
                all(counts.get(name) is None for name in ('view_count', 'like_count'))):
            return 'COUNTS_NOT_EXPOSED'
        return None
    reason = (result.get('player_reason') or '').lower()
    # Generic sign-in challenges, regional restrictions and malformed responses
    # may depend on the route. They must not end a video's retry budget early.
    if result.get('player_status') in {'ERROR', 'UNPLAYABLE', 'LOGIN_REQUIRED'}:
        if 'private video' in reason or 'video is private' in reason:
            return 'VIDEO_PRIVATE'
        if any(text in reason for text in ('video has been removed', 'removed by the uploader',
                                          'video has been deleted', 'video was deleted')):
            return 'VIDEO_REMOVED'
        if reason.strip().rstrip('.') in {'video unavailable', 'this video is unavailable'}:
            return 'VIDEO_UNAVAILABLE'
    return None


def data_observation_matches(result, successful, data_received, http_status):
    # Old journals recorded False for video errors. New journals use None to
    # leave those responses out of the proxy's data-success score.
    return (data_received is successful or
            (not successful and data_received is None and http_status == 200 and
             video_error_reason(result) is not None))


@dataclass(frozen=True)
class CollectionOutcome:
    category: str
    proxy_success: bool | None
    video_error: str | None = None


def classify_outcome(result, observation, successful):
    if observation is None:
        return CollectionOutcome('local_error', None)
    if successful:
        return CollectionOutcome('data', True)
    status = observation.get('http_status')
    reason = video_error_reason(result) if status == 200 and not observation.get('connection_error') else None
    if reason:
        return CollectionOutcome('video_error', None, reason)
    if observation.get('connection_error') or status is None:
        return CollectionOutcome('connection_error', False)
    if not 200 <= status < 300:
        return CollectionOutcome('http_error', False)
    return CollectionOutcome('response_error', False)


@dataclass
class ProxyPerformance:
    quality: float = 0.5
    latency_seconds: float | None = None
    samples: int = 0
    failure_streak: int = 0
    updated_at: float = 0.0
    cooldown_until: float = 0.0

    @classmethod
    def restore(cls, row, usage, now):
        if row:
            result = cls(**{name: row[name] for name in cls.__dataclass_fields__})
            result.expire(now)
            result.cooldown_until = min(result.cooldown_until, now + 60)
            return result
        # Support queues created before recent performance was recorded. These
        # counters are only a starting estimate, replaced by new observations.
        attempts = usage.get('attempts', 1 if usage else 0)
        successes = usage.get('successes', 0)
        if attempts or successes:
            return cls(quality=successes / max(attempts, successes),
                       failure_streak=0 if successes else min(6, attempts), updated_at=now,
                       cooldown_until=now if successes else now + 60)
        return cls()

    def expire(self, now):
        if self.updated_at and now - self.updated_at > PERFORMANCE_MAX_AGE:
            self.quality, self.latency_seconds, self.samples = 0.5, None, 0
            self.failure_streak, self.updated_at = 0, 0.0

    @property
    def score(self):
        return self.quality / max(0.05, self.latency_seconds or 1.0)

    def observe(self, outcome, seconds, now):
        self.expire(now)
        if outcome.category == 'local_error':
            self.cooldown_until = now + 60
            return
        if outcome.proxy_success is not None:
            if not self.samples and not self.updated_at:
                self.quality = float(outcome.proxy_success)
            else:
                self.quality = (1 - RECENT_WEIGHT) * self.quality + RECENT_WEIGHT * int(outcome.proxy_success)
            self.samples += 1
            self.updated_at = now
        if outcome.proxy_success is True:
            if type(seconds) in (int, float) and math.isfinite(seconds) and seconds >= 0:
                seconds = max(0.05, seconds)
                self.latency_seconds = seconds if self.latency_seconds is None else (
                    (1 - RECENT_WEIGHT) * self.latency_seconds + RECENT_WEIGHT * seconds)
        if outcome.proxy_success is False:
            self.failure_streak += 1
            self.cooldown_until = now + min(60, 2 ** min(6, self.failure_streak))
        else:
            # A genuine video error proves the request completed, but says
            # nothing about whether this proxy can retrieve another video's data.
            self.failure_streak = 0
            self.cooldown_until = now
