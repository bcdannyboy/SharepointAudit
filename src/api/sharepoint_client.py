import asyncio
import logging
import re
import time
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
            start = time.time()

            # Get authentication token for SharePoint
            credential = await self.auth_manager.get_credential()
            # Extract tenant name from URL
            match = re.search(r'https://([^.]+)\.sharepoint\.com', url)
            if match:
                tenant_name = match.group(1)
                scope = f"https://{tenant_name}.sharepoint.com/.default"
            else:
                # Fallback to Graph API scope
                scope = "https://graph.microsoft.com/.default"
            token = credential.get_token(scope)

            # Add authorization header
            headers = kwargs.get("headers", {})
            headers["Authorization"] = f"Bearer {token.token}"
            headers["Accept"] = "application/json"
            kwargs["headers"] = headers

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
                    logger.error("GET %s returned HTTP %s", url, resp.status)
                    raise SharePointAPIError(f"HTTP {resp.status}", status_code=resp.status)
                data = await resp.json()
                logger.info(
                    "GET %s succeeded in %.2fs", url, time.time() - start
                )
                return data

        return await self.retry_strategy.execute_with_retry(url, _do_get)

    async def post_with_retry(self, url: str, **kwargs) -> Any:
        async def _do_post():
            await self.rate_limiter.acquire("simple_get")
            start = time.time()

            # Get authentication token for SharePoint
            credential = await self.auth_manager.get_credential()
            # Extract tenant name from URL
            match = re.search(r'https://([^.]+)\.sharepoint\.com', url)
            if match:
                tenant_name = match.group(1)
                scope = f"https://{tenant_name}.sharepoint.com/.default"
            else:
                # Fallback to Graph API scope
                scope = "https://graph.microsoft.com/.default"
            token = credential.get_token(scope)

            # Add authorization header
            headers = kwargs.get("headers", {})
            headers["Authorization"] = f"Bearer {token.token}"
            headers["Accept"] = "application/json"
            headers["Content-Type"] = "application/json"
            kwargs["headers"] = headers

            async with aiohttp.ClientSession() as session:
                resp = await session.post(url, **kwargs)
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    raise SharePointAPIError(
                        "Too Many Requests",
                        status_code=429,
                        retry_after=retry_after,
                    )
                if resp.status >= 400:
                    logger.error("POST %s returned HTTP %s", url, resp.status)
                    raise SharePointAPIError(f"HTTP {resp.status}", status_code=resp.status)
                data = await resp.json()
                logger.info("POST %s succeeded in %.2fs", url, time.time() - start)
                return data

        return await self.retry_strategy.execute_with_retry(url, _do_post)

    async def batch_request(self, url: str, requests: list[dict]) -> Any:
        async def _do_batch():
            await self.rate_limiter.acquire("batch_request")
            payload = {"requests": requests}
            start = time.time()

            # Get authentication token for SharePoint
            credential = await self.auth_manager.get_credential()
            # Extract tenant name from URL
            match = re.search(r'https://([^.]+)\.sharepoint\.com', url)
            if match:
                tenant_name = match.group(1)
                scope = f"https://{tenant_name}.sharepoint.com/.default"
            else:
                # Fallback to Graph API scope
                scope = "https://graph.microsoft.com/.default"
            token = credential.get_token(scope)

            # Add authorization header
            headers = {
                "Authorization": f"Bearer {token.token}",
                "Accept": "application/json",
                "Content-Type": "application/json"
            }

            async with aiohttp.ClientSession() as session:
                resp = await session.post(url, json=payload, headers=headers)
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", "1"))
                    raise SharePointAPIError(
                        "Too Many Requests",
                        status_code=429,
                        retry_after=retry_after,
                    )
                if resp.status >= 400:
                    logger.error("BATCH %s returned HTTP %s", url, resp.status)
                    raise SharePointAPIError(f"HTTP {resp.status}", status_code=resp.status)
                data = await resp.json()
                logger.info("BATCH %s succeeded in %.2fs", url, time.time() - start)
                return data

        operation_id = f"batch:{url}"
        return await self.retry_strategy.execute_with_retry(operation_id, _do_batch)

    async def get_site_permissions(self, site_url: str) -> list[dict[str, Any]]:
        """Get role assignments for a SharePoint site."""
        api_url = f"{site_url}/_api/web/roleassignments?$expand=Member,RoleDefinitionBindings"

        try:
            response = await self.get_with_retry(api_url)
            return response.get("value", [])
        except Exception as e:
            logger.error(f"Failed to get site permissions for {site_url}: {e}")
            raise SharePointAPIError(f"Failed to get site permissions: {e}")

    async def get_library_permissions(
        self,
        site_url: str,
        library_id: str
    ) -> list[dict[str, Any]]:
        """Get role assignments for a document library."""
        api_url = f"{site_url}/_api/web/lists(guid'{library_id}')/roleassignments?$expand=Member,RoleDefinitionBindings"

        try:
            response = await self.get_with_retry(api_url)
            return response.get("value", [])
        except Exception as e:
            logger.error(f"Failed to get library permissions for {library_id}: {e}")
            raise SharePointAPIError(f"Failed to get library permissions: {e}")

    async def get_item_permissions(
        self,
        site_url: str,
        library_id: str,
        item_id: int
    ) -> list[dict[str, Any]]:
        """Get role assignments for a specific item (file or folder)."""
        api_url = f"{site_url}/_api/web/lists(guid'{library_id}')/items({item_id})/roleassignments?$expand=Member,RoleDefinitionBindings"

        try:
            response = await self.get_with_retry(api_url)
            return response.get("value", [])
        except Exception as e:
            logger.error(f"Failed to get item permissions for {item_id}: {e}")
            raise SharePointAPIError(f"Failed to get item permissions: {e}")

    async def check_unique_permissions(
        self,
        site_url: str,
        library_id: str,
        item_id: int
    ) -> bool:
        """Check if an item has unique permissions."""
        api_url = f"{site_url}/_api/web/lists(guid'{library_id}')/items({item_id})/HasUniqueRoleAssignments"

        try:
            response = await self.get_with_retry(api_url)
            return response.get("value", False)
        except Exception as e:
            logger.error(f"Failed to check unique permissions for {item_id}: {e}")
            return False

    async def get_sharing_links(
        self,
        site_url: str,
        item_url: str
    ) -> list[dict[str, Any]]:
        """Get sharing links for an item."""
        api_url = f"{site_url}/_api/SP.Sharing.DocumentSharingManager.GetSharingInformation"

        payload = {
            "request": {
                "url": item_url,
                "permissionsOnly": False,
                "additionalProperties": {
                    "includeAnonymousLinks": True,
                    "includeSharingLinks": True
                }
            }
        }

        try:
            response = await self.post_with_retry(api_url, json=payload)
            return response.get("sharingLinks", [])
        except Exception as e:
            logger.error(f"Failed to get sharing links for {item_url}: {e}")
            return []
