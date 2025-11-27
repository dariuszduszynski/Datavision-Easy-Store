"""Async token bucket rate limiter used by the marker worker."""

from __future__ import annotations

import asyncio
import time
from typing import Optional


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for database operations.

    Prevents overwhelming the source database with too many queries.
    """

    def __init__(self, rate: float, capacity: Optional[int] = None):
        """
        Args:
            rate: Tokens per second (e.g., 10.0 = 10 ops/sec).
            capacity: Max tokens in bucket (default: 2x rate).
        """
        self.rate = rate
        self.capacity = capacity or int(rate * 2)
        self.tokens = float(self.capacity)
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """
        Acquire tokens, waiting if necessary.

        Args:
            tokens: Number of tokens to acquire.
        """
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update

                # Refill tokens based on elapsed time.
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                wait_time = (tokens - self.tokens) / self.rate
                await asyncio.sleep(wait_time)


__all__ = ["TokenBucketRateLimiter"]
