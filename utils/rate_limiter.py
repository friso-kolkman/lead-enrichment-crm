"""Async sliding window rate limiter for API calls."""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    requests_per_minute: int = 60
    requests_per_second: int | None = None
    burst_size: int = 10
    retry_after_seconds: float = 1.0
    max_retries: int = 3


@dataclass
class SlidingWindowCounter:
    """Sliding window rate limit counter."""

    window_size: float = 60.0  # seconds
    max_requests: int = 60
    timestamps: list[float] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def acquire(self) -> bool:
        """Try to acquire a request slot. Returns True if allowed."""
        async with self._lock:
            now = time.monotonic()
            window_start = now - self.window_size

            # Remove expired timestamps
            self.timestamps = [ts for ts in self.timestamps if ts > window_start]

            if len(self.timestamps) < self.max_requests:
                self.timestamps.append(now)
                return True
            return False

    async def wait_and_acquire(self, timeout: float = 60.0) -> bool:
        """Wait until a slot is available, then acquire it."""
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            if await self.acquire():
                return True

            # Calculate wait time until oldest request expires
            async with self._lock:
                if self.timestamps:
                    oldest = min(self.timestamps)
                    wait_time = oldest + self.window_size - time.monotonic()
                    if wait_time > 0:
                        await asyncio.sleep(min(wait_time + 0.1, 1.0))
                else:
                    await asyncio.sleep(0.1)

        return False

    @property
    def current_count(self) -> int:
        """Get current request count in the window."""
        now = time.monotonic()
        window_start = now - self.window_size
        return len([ts for ts in self.timestamps if ts > window_start])


class RateLimiter:
    """Async rate limiter with per-provider limits."""

    def __init__(self):
        self._limiters: dict[str, SlidingWindowCounter] = {}
        self._configs: dict[str, RateLimitConfig] = {}
        self._global_limiter: SlidingWindowCounter | None = None

    def configure(
        self,
        provider: str,
        requests_per_minute: int = 60,
        requests_per_second: int | None = None,
        burst_size: int = 10,
    ) -> None:
        """Configure rate limits for a provider."""
        # Use requests_per_second if specified, otherwise use per minute
        if requests_per_second:
            window_size = 1.0
            max_requests = requests_per_second
        else:
            window_size = 60.0
            max_requests = requests_per_minute

        self._limiters[provider] = SlidingWindowCounter(
            window_size=window_size,
            max_requests=max_requests,
        )
        self._configs[provider] = RateLimitConfig(
            requests_per_minute=requests_per_minute,
            requests_per_second=requests_per_second,
            burst_size=burst_size,
        )
        logger.debug(f"Configured rate limiter for {provider}: {max_requests} req/{window_size}s")

    def set_global_limit(self, requests_per_minute: int = 300) -> None:
        """Set a global rate limit across all providers."""
        self._global_limiter = SlidingWindowCounter(
            window_size=60.0,
            max_requests=requests_per_minute,
        )

    async def acquire(self, provider: str, timeout: float = 60.0) -> bool:
        """Acquire a rate limit slot for a provider."""
        # Check global limit first
        if self._global_limiter:
            if not await self._global_limiter.wait_and_acquire(timeout):
                logger.warning(f"Global rate limit exceeded for {provider}")
                return False

        # Check provider-specific limit
        if provider not in self._limiters:
            # Create default limiter
            self.configure(provider)

        return await self._limiters[provider].wait_and_acquire(timeout)

    async def execute_with_retry(
        self,
        provider: str,
        func: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        max_retries: int = 3,
        **kwargs: Any,
    ) -> T:
        """Execute a function with rate limiting and retry logic."""
        config = self._configs.get(provider, RateLimitConfig())
        last_error: Exception | None = None

        for attempt in range(max_retries):
            # Acquire rate limit slot
            if not await self.acquire(provider):
                raise RateLimitExceeded(f"Rate limit exceeded for {provider}")

            try:
                return await func(*args, **kwargs)
            except RateLimitError as e:
                last_error = e
                retry_after = getattr(e, "retry_after", config.retry_after_seconds)
                logger.warning(
                    f"Rate limited by {provider}, waiting {retry_after}s (attempt {attempt + 1}/{max_retries})"
                )
                await asyncio.sleep(retry_after)
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    await asyncio.sleep(config.retry_after_seconds * (attempt + 1))
                    logger.warning(
                        f"Error from {provider}, retrying (attempt {attempt + 1}/{max_retries}): {e}"
                    )
                else:
                    raise

        raise last_error or RateLimitExceeded(f"Max retries exceeded for {provider}")

    def get_status(self, provider: str) -> dict[str, Any]:
        """Get rate limit status for a provider."""
        if provider not in self._limiters:
            return {"provider": provider, "configured": False}

        limiter = self._limiters[provider]
        config = self._configs[provider]

        return {
            "provider": provider,
            "configured": True,
            "current_count": limiter.current_count,
            "max_requests": limiter.max_requests,
            "window_size": limiter.window_size,
            "available": limiter.max_requests - limiter.current_count,
        }

    def get_all_status(self) -> dict[str, dict[str, Any]]:
        """Get rate limit status for all providers."""
        status = {}
        for provider in self._limiters:
            status[provider] = self.get_status(provider)

        if self._global_limiter:
            status["_global"] = {
                "configured": True,
                "current_count": self._global_limiter.current_count,
                "max_requests": self._global_limiter.max_requests,
                "available": self._global_limiter.max_requests - self._global_limiter.current_count,
            }

        return status


class RateLimitError(Exception):
    """Raised when an API returns a rate limit error."""

    def __init__(self, message: str, retry_after: float = 1.0):
        super().__init__(message)
        self.retry_after = retry_after


class RateLimitExceeded(Exception):
    """Raised when local rate limit is exceeded."""

    pass


# Global rate limiter instance
rate_limiter = RateLimiter()


def configure_provider_limits() -> None:
    """Configure rate limits for all enrichment providers."""
    # Apollo.io - 100 req/min
    rate_limiter.configure("apollo", requests_per_minute=100)

    # Clearbit - 60 req/min
    rate_limiter.configure("clearbit", requests_per_minute=60)

    # Hunter.io - 100 req/min
    rate_limiter.configure("hunter", requests_per_minute=100)

    # Prospeo - 60 req/min
    rate_limiter.configure("prospeo", requests_per_minute=60)

    # Dropcontact - 50 req/min
    rate_limiter.configure("dropcontact", requests_per_minute=50)

    # ZeroBounce - 200 req/min
    rate_limiter.configure("zerobounce", requests_per_minute=200)

    # OpenAI - 60 req/min (depends on tier)
    rate_limiter.configure("openai", requests_per_minute=60)

    # Perplexity - 20 req/min (free tier)
    rate_limiter.configure("perplexity", requests_per_minute=20)

    # Attio CRM - 100 req/min
    rate_limiter.configure("attio", requests_per_minute=100)

    # Resend - 100 emails/day, ~7/min for free tier
    rate_limiter.configure("resend", requests_per_minute=7)

    # Set global limit
    rate_limiter.set_global_limit(requests_per_minute=300)

    logger.info("Rate limiters configured for all providers")
