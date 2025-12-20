#!/usr/bin/env python3
"""Async rate limiting helpers."""

from __future__ import annotations

from asyncio import Lock, sleep
from time import time

from config import get_logger

logger = get_logger("utils.rate_limiter")


class RateLimiter:
    """A token bucket rate limiter for controlling request rates."""

    def __init__(self, requests_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.min_interval = 60.0 / requests_per_minute if requests_per_minute > 0 else 0
        self.last_request_time = 0
        self._lock = Lock()

    async def acquire(self):
        if self.min_interval <= 0:
            return

        async with self._lock:
            current_time = time()
            time_since_last = current_time - self.last_request_time

            if time_since_last < self.min_interval:
                wait_time = self.min_interval - time_since_last
                logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
                await sleep(wait_time)

            self.last_request_time = time()
