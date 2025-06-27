import asyncio
import logging
import time
from typing import Any

import aiohttp

from .auth_manager import AuthenticationManager
from ..utils.rate_limiter import RateLimiter
from ..utils.retry_handler import RetryStrategy, RetryConfig
from ..utils.exceptions import GraphAPIError

logger = logging.getLogger(__name__)


class GraphAPIClient:
    """Client for Microsoft Graph API interactions with retry logic."""

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
            start = time.time()
            async with aiohttp.ClientSession() as session:
                resp = await session.get(url, **kwargs)
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    raise GraphAPIError(
                        "Too Many Requests",
                        status_code=429,
                        retry_after=retry_after,
                    )
                if resp.status >= 400:
                    logger.error("GET %s returned HTTP %s", url, resp.status)
                    raise GraphAPIError(f"HTTP {resp.status}", status_code=resp.status)
                data = await resp.json()
                logger.info("GET %s succeeded in %.2fs", url, time.time() - start)
                return data

        return await self.retry_strategy.execute_with_retry(url, _do_get)

    async def post_with_retry(self, url: str, **kwargs) -> Any:
        async def _do_post():
            await self.rate_limiter.acquire("simple_get")
            start = time.time()
            async with aiohttp.ClientSession() as session:
                resp = await session.post(url, **kwargs)
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    raise GraphAPIError(
                        "Too Many Requests",
                        status_code=429,
                        retry_after=retry_after,
                    )
                if resp.status >= 400:
                    logger.error("POST %s returned HTTP %s", url, resp.status)
                    raise GraphAPIError(f"HTTP {resp.status}", status_code=resp.status)
                data = await resp.json()
                logger.info("POST %s succeeded in %.2fs", url, time.time() - start)
                return data

        return await self.retry_strategy.execute_with_retry(url, _do_post)

    async def batch_request(self, url: str, requests: list[dict]) -> Any:
        async def _do_batch():
            await self.rate_limiter.acquire("batch_request")
            payload = {"requests": requests}
            start = time.time()
            async with aiohttp.ClientSession() as session:
                resp = await session.post(url, json=payload)
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    raise GraphAPIError(
                        "Too Many Requests",
                        status_code=429,
                        retry_after=retry_after,
                    )
                if resp.status >= 400:
                    logger.error("BATCH %s returned HTTP %s", url, resp.status)
                    raise GraphAPIError(f"HTTP {resp.status}", status_code=resp.status)
                data = await resp.json()
                logger.info("BATCH %s succeeded in %.2fs", url, time.time() - start)
                return data

        operation_id = f"batch:{url}"
        return await self.retry_strategy.execute_with_retry(operation_id, _do_batch)
