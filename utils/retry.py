#!/usr/bin/env python3
"""Retry helpers."""

from __future__ import annotations

from asyncio import sleep

from config import get_logger

logger = get_logger("utils.retry")


class RetryHelper:
    """Helper class for implementing retry logic with exponential backoff."""

    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int) -> float:
        delay = self.base_delay * (2**attempt)
        return min(delay, self.max_delay)

    async def sleep_for_attempt(self, attempt: int):
        delay = self.calculate_delay(attempt)
        if delay > 0:
            logger.debug(f"Retry delay: sleeping for {delay:.2f} seconds")
            await sleep(delay)
