import asyncio
import logging
import time
from typing import Any, Optional

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
        self._token_cache: Optional[dict] = None
        self._token_expires_at: float = 0

    async def _get_auth_headers(self) -> dict[str, str]:
        """Get authentication headers with a valid access token."""
        current_time = time.time()

        # Check if we have a valid cached token
        if self._token_cache and current_time < self._token_expires_at - 300:  # 5 min buffer
            return {"Authorization": f"Bearer {self._token_cache['token']}"}

        # Get a new token
        try:
            # Get the credential directly from auth manager
            credential = await self.auth_manager.get_credential()

            logger.info(f"Credential type: {type(credential).__name__} from module: {type(credential).__module__}")
            logger.info(f"Credential has get_token: {hasattr(credential, 'get_token')}")

            # Get token for Graph API
            # Handle both sync and async credential methods
            if hasattr(credential, 'get_token'):
                # For async credentials
                if asyncio.iscoroutinefunction(credential.get_token):
                    token_response = await credential.get_token("https://graph.microsoft.com/.default")
                else:
                    # For sync credentials (like ClientCertificateCredential)
                    token_response = credential.get_token("https://graph.microsoft.com/.default")
            else:
                raise AttributeError("Credential object does not have get_token method")

            self._token_cache = {"token": token_response.token}
            # Azure tokens typically expire in 1 hour
            self._token_expires_at = current_time + 3600

            return {"Authorization": f"Bearer {token_response.token}"}
        except Exception as e:
            logger.error(f"Failed to get authentication token: {e}")
            raise GraphAPIError(f"Authentication failed: {e}")

    async def get_with_retry(self, url: str, **kwargs) -> Any:
        async def _do_get():
            await self.rate_limiter.acquire("simple_get")
            start = time.time()

            # Get auth headers and merge with any provided headers
            auth_headers = await self._get_auth_headers()
            headers = kwargs.get("headers", {})
            headers.update(auth_headers)
            kwargs["headers"] = headers

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

            # Get auth headers and merge with any provided headers
            auth_headers = await self._get_auth_headers()
            headers = kwargs.get("headers", {})
            headers.update(auth_headers)
            kwargs["headers"] = headers

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

            # Get auth headers
            auth_headers = await self._get_auth_headers()

            async with aiohttp.ClientSession() as session:
                resp = await session.post(url, json=payload, headers=auth_headers)
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

    async def get_all_sites_delta(self, delta_token: str | None = None) -> Any:
        """Retrieve all sites using the delta query."""
        url = "https://graph.microsoft.com/v1.0/sites/delta"
        if delta_token:
            url += f"?token={delta_token}"

        # Return the raw data - let the discovery module handle conversion
        return await self.get_with_retry(url)

    async def expand_group_members_transitive(self, group_id: str) -> list[dict[str, Any]]:
        """
        Get all members of a group including nested group members.

        Uses the /transitiveMembers endpoint to get all members recursively.
        """
        url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/transitiveMembers"
        members = []

        while url:
            response = await self.get_with_retry(url)
            members.extend(response.get("value", []))

            # Handle pagination
            url = response.get("@odata.nextLink")

        return members

    async def get_group_info(self, group_id: str) -> dict[str, Any]:
        """Get basic information about a group."""
        url = f"https://graph.microsoft.com/v1.0/groups/{group_id}"
        return await self.get_with_retry(url)

    async def get_user_info(self, user_id: str) -> dict[str, Any]:
        """Get basic information about a user."""
        url = f"https://graph.microsoft.com/v1.0/users/{user_id}"
        return await self.get_with_retry(url)

    async def batch_get_users(self, user_ids: list[str]) -> dict[str, Any]:
        """Get information for multiple users in a single batch request."""
        if not user_ids:
            return {}

        # Graph API batch requests are limited to 20 requests per batch
        batch_size = 20
        all_users = {}

        for i in range(0, len(user_ids), batch_size):
            batch_ids = user_ids[i:i + batch_size]
            requests = [
                {
                    "id": user_id,
                    "method": "GET",
                    "url": f"/users/{user_id}"
                }
                for user_id in batch_ids
            ]

            batch_url = "https://graph.microsoft.com/v1.0/$batch"
            response = await self.batch_request(batch_url, requests)

            # Process batch response
            for resp in response.get("responses", []):
                if resp.get("status") == 200:
                    user_data = resp.get("body", {})
                    all_users[user_data.get("id", resp.get("id"))] = user_data

        return all_users

    async def check_external_user(self, user_principal_name: str) -> bool:
        """Check if a user is an external/guest user."""
        # External users typically have #EXT# in their UPN
        if "#EXT#" in user_principal_name:
            return True

        # Try to get user info to check userType
        try:
            user_info = await self.get_user_info(user_principal_name)
            return user_info.get("userType", "").lower() == "guest"
        except GraphAPIError:
            # If we can't get user info, assume based on UPN pattern
            return "#EXT#" in user_principal_name or "_" in user_principal_name.split("@")[0]
