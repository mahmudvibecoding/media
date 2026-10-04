"""Prefer useful listing responses per second, while sampling other proxies."""
import asyncio
from dataclasses import dataclass
import heapq
import time


@dataclass
class Performance:
    quality: float = 0.5
    seconds: float = 1.0
    samples: int = 0
    failures: int = 0
    successes: int = 0

    @property
    def score(self):
        return self.quality / max(0.05, self.seconds)

    def observe(self, usable, seconds):
        if usable is None:
            return
        self.quality = 0.8*self.quality + 0.2*bool(usable)
        self.seconds = 0.8*self.seconds + 0.2*max(0.05, seconds)
        self.samples += 1
        self.successes += bool(usable)
        self.failures = 0 if usable else self.failures + 1


class DiscoveryPool:
    """Proxy partitions are exclusive to workers; one active request per proxy."""
    def __init__(self, ranks):
        self.performance = [Performance(seconds=max(0.05, r['average_response_ms']/1000)) for r in ranks]
        self.versions = [0]*len(ranks)
        self.available, self.active = {}, set()
        self.ranked, self.oldest, self.cooling = [], [], []
        self.order = self.selections = 0
        self.changed = asyncio.Event()
        for index in range(len(ranks)):
            self._schedule(index, 0)

    def _schedule(self, index, delay):
        self.versions[index] += 1
        heapq.heappush(self.cooling, (time.monotonic()+delay, index, self.versions[index]))

    def _key(self, index, version):
        profile = self.performance[index]
        group = 0 if profile.successes else 1 if not profile.samples else 2
        return (group, -profile.score if profile.samples else 0, index, version)

    def _promote(self):
        now = time.monotonic()
        while self.cooling and self.cooling[0][0] <= now:
            _, index, version = heapq.heappop(self.cooling)
            self.order += 1
            self.available[index] = (version, self.order)
            heapq.heappush(self.ranked, self._key(index, version))
            heapq.heappush(self.oldest, (self.order, index, version))

    def _take(self, heap, previous):
        held, chosen = [], None
        while heap:
            item = heapq.heappop(heap)
            index, version = item[-2:]
            current = self.available.get(index)
            if current is None or current[0] != version:
                continue
            if index == previous:
                held.append(item)
                continue
            chosen = index
            break
        for item in held:
            heapq.heappush(heap, item)
        if chosen is not None:
            self.available.pop(chosen)
            self.active.add(chosen)
            self.selections += 1
            if max(len(self.ranked), len(self.oldest)) > max(64, 4*len(self.performance)):
                self.ranked = [self._key(i, version) for i, (version, _) in self.available.items()]
                self.oldest = [(order, i, version) for i, (version, order) in self.available.items()]
                heapq.heapify(self.ranked)
                heapq.heapify(self.oldest)
        return chosen

    async def acquire(self, previous=None):
        while True:
            self._promote()
            heap = self.oldest if self.selections % 20 == 19 else self.ranked
            chosen = self._take(heap, previous)
            if chosen is None and self.available:
                chosen = self._take(heap, None)
            if chosen is not None:
                return chosen
            self.changed.clear()
            delay = max(0.01, self.cooling[0][0]-time.monotonic()) if self.cooling else 1
            try:
                await asyncio.wait_for(self.changed.wait(), min(delay, 1))
            except TimeoutError:
                pass

    def release(self, index, usable, seconds):
        if index not in self.active:
            raise ValueError('Proxy was not active')
        self.active.remove(index)
        profile = self.performance[index]
        profile.observe(usable, seconds)
        self._schedule(index, min(60, 2**min(6, profile.failures)) if usable is False else 0)
        self.changed.set()


class ConcurrencyTuner:
    """Compare useful-page rates at increasing concurrency, then keep the best."""
    def __init__(self, initial, maximum, interval=30):
        self.current, self.maximum, self.interval = initial, maximum, interval
        self.best_limit, self.best_rate = initial, 0.0
        self.last_time = self.last_pages = 0
        self.hold_until = 0.0
        self.regressions = 0

    def observe(self, now, pages, remaining, backlog=False):
        if not self.last_time:
            self.last_time, self.last_pages = now, pages
            return None
        elapsed = now-self.last_time
        if elapsed < self.interval:
            return None
        rate = (pages-self.last_pages)/elapsed
        self.last_time, self.last_pages = now, pages
        # A draining queue has insufficient work to compare concurrency levels.
        if remaining < max(500, 2*self.current) or now < self.hold_until:
            return None
        old = self.current
        if backlog:
            self.current = max(1, min(self.best_limit, self.current//2))
            self.hold_until = now + 2*self.interval
            reason = 'writer_backlog'
        elif self.best_rate and rate < 0.8*self.best_rate:
            self.regressions += 1
            if self.regressions < 2:
                return None
            self.current = self.best_limit
            self.hold_until = now + 2*self.interval
            self.regressions = 0
            reason = 'useful_page_rate_decreased'
        else:
            self.regressions = 0
            if rate > self.best_rate:
                self.best_rate, self.best_limit = rate, self.current
            self.current = min(self.maximum, self.current*2)
            reason = 'testing_more_concurrency'
        if self.current == old:
            return None
        return dict(previous=old, concurrency=self.current, useful_pages_per_second=round(rate, 3),
                    best_concurrency=self.best_limit, reason=reason)
