import asyncio
import logging
from typing import Any

import aiohttp

from .auth_manager import AuthenticationManager
from ..utils.rate_limiter import RateLimiter
from ..utils.retry_handler import RetryStrategy, RetryConfig
from ..utils.exceptions import SharePointAPIError

logger = logging.getLogger(__name__)


class SharePointAPIClient:
    """Client for interacting with SharePoint REST APIs with retry and throttling."""

    def __init__(
        self,
        auth_manager: AuthenticationManager,
        retry_strategy: RetryStrategy | None = None,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self.auth_manager = auth_manager
        self.retry_strategy = retry_strategy or RetryStrategy(RetryConfig())
        self.rate_limiter = rate_limiter or RateLimiter()

    async def get_with_retry(self, url: str, **kwargs) -> Any:
        async def _do_get():
            await self.rate_limiter.acquire("simple_get")
            async with aiohttp.ClientSession() as session:
                resp = await session.get(url, **kwargs)
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    raise SharePointAPIError(
                        "Too Many Requests",
                        status_code=429,
                        retry_after=retry_after,
                    )
                if resp.status >= 400:
                    raise SharePointAPIError(f"HTTP {resp.status}", status_code=resp.status)
                return await resp.json()

        return await self.retry_strategy.execute_with_retry(url, _do_get)
