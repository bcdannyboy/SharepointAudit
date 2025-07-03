import asyncio
import logging
import re
import time
from typing import Any

import aiohttp

from api.auth_manager import AuthenticationManager
from utils.rate_limiter import RateLimiter
from utils.retry_handler import RetryStrategy, RetryConfig
from utils.exceptions import SharePointAPIError

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
        # Fix: Add connection pooling configuration
        self._connector = None
        self._session = None

    def _is_valid_guid(self, guid_str: str) -> bool:
        """Validate GUID format for SharePoint library IDs."""
        if not guid_str or not isinstance(guid_str, str):
            return False

        # Remove dashes and check if it's a valid hex string of 32 characters
        guid_clean = guid_str.replace('-', '')
        if len(guid_clean) != 32:
            return False

        try:
            int(guid_clean, 16)
            return True
        except ValueError:
            return False

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session with proper connection pooling."""
        if self._session is None or self._session.closed:
            # Fix: Add proper connection pooling configuration
            self._connector = aiohttp.TCPConnector(
                limit=100,  # Total connection limit
                limit_per_host=10,  # Per-host connection limit
                ttl_dns_cache=300,  # DNS cache TTL
                use_dns_cache=True,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )

            timeout = aiohttp.ClientTimeout(total=60, connect=10)
            self._session = aiohttp.ClientSession(
                connector=self._connector,
                timeout=timeout
            )
        return self._session

    async def close(self):
        """Close the aiohttp session and connector."""
        if self._session and not self._session.closed:
            await self._session.close()
        if self._connector:
            await self._connector.close()

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
                # Fix: Use correct SharePoint REST API scope format
                scope = f"https://{tenant_name}.sharepoint.com/.default"
            else:
                # Fallback to Graph API scope
                scope = "https://graph.microsoft.com/.default"

            # DIAGNOSTIC LOGGING for HTTP 400 debugging
            logger.info(f"DIAGNOSTIC: Making HTTP GET request")
            logger.info(f"DIAGNOSTIC: URL = {url}")
            logger.info(f"DIAGNOSTIC: Extracted tenant_name = {tenant_name if match else 'None'}")
            logger.info(f"DIAGNOSTIC: Using scope = {scope}")

            token = credential.get_token(scope)
            logger.info(f"DIAGNOSTIC: Token acquired successfully, expires_on = {token.expires_on}")

            # Add authorization header
            headers = kwargs.get("headers", {})
            headers["Authorization"] = f"Bearer {token.token}"
            headers["Accept"] = "application/json"
            kwargs["headers"] = headers

            logger.info(f"DIAGNOSTIC: Request headers = {headers}")

            # Fix: Use managed session with connection pooling
            session = await self._get_session()
            resp = await session.get(url, **kwargs)

            # DIAGNOSTIC LOGGING for response
            logger.info(f"DIAGNOSTIC: Response status = {resp.status}")
            logger.info(f"DIAGNOSTIC: Response headers = {dict(resp.headers)}")

            if resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", "1"))
                raise SharePointAPIError(
                    "Too Many Requests",
                    status_code=429,
                    retry_after=retry_after,
                )
            if resp.status >= 400:
                # Get response body for diagnostic purposes
                try:
                    error_body = await resp.text()
                    logger.error(f"DIAGNOSTIC: HTTP {resp.status} Error response body: {error_body}")
                except:
                    logger.error(f"DIAGNOSTIC: Could not read error response body")

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
                # Fix: Use correct SharePoint REST API scope format
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

            # Fix: Use managed session with connection pooling
            session = await self._get_session()
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
                # Fix: Use correct SharePoint REST API scope format
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

            # Fix: Use managed session with connection pooling
            session = await self._get_session()
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
            # SharePoint might return the results directly or in a value property
            if isinstance(response, list):
                results = response
            else:
                results = response.get("value", response.get("d", {}).get("results", []))
            logger.debug(f"SharePoint API returned {len(results)} role assignments for site {site_url}")
            return results
        except Exception as e:
            logger.error(f"Failed to get site permissions for {site_url}: {e}")
            raise SharePointAPIError(f"Failed to get site permissions: {e}")

    async def get_library_permissions(
        self,
        site_url: str,
        library_id: str
    ) -> list[dict[str, Any]]:
        """Get role assignments for a document library."""
        # Validate library_id format
        if not self._is_valid_guid(library_id):
            raise SharePointAPIError(f"Invalid library_id format: {library_id}")

        # Fix: Use correct SharePoint REST API URL format
        api_url = f"{site_url}/_api/web/lists/getbyid('{library_id}')/roleassignments?$expand=Member,RoleDefinitionBindings"

        try:
            response = await self.get_with_retry(api_url)
            # SharePoint might return the results directly or in a value property
            if isinstance(response, list):
                results = response
            else:
                results = response.get("value", response.get("d", {}).get("results", []))
            logger.debug(f"SharePoint API returned {len(results)} role assignments for library {library_id}")
            return results
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
        # Validate inputs
        if not self._is_valid_guid(library_id):
            raise SharePointAPIError(f"Invalid library_id format: {library_id}")
        if not isinstance(item_id, int) or item_id <= 0:
            raise SharePointAPIError(f"Invalid item_id: {item_id} (must be positive integer)")

        # Fix: Use correct SharePoint REST API URL format
        api_url = f"{site_url}/_api/web/lists/getbyid('{library_id}')/items({item_id})/roleassignments?$expand=Member,RoleDefinitionBindings"

        # DIAGNOSTIC LOGGING for HTTP 400 debugging
        logger.info(f"DIAGNOSTIC: Getting item permissions")
        logger.info(f"DIAGNOSTIC: site_url = {site_url}")
        logger.info(f"DIAGNOSTIC: library_id = {library_id}")
        logger.info(f"DIAGNOSTIC: item_id = {item_id} (type: {type(item_id)})")
        logger.info(f"DIAGNOSTIC: Constructed URL = {api_url}")

        try:
            response = await self.get_with_retry(api_url)
            # SharePoint might return the results directly or in a value property
            if isinstance(response, list):
                results = response
            else:
                results = response.get("value", response.get("d", {}).get("results", []))
            logger.debug(f"SharePoint API returned {len(results)} role assignments for item {item_id}")
            return results
        except SharePointAPIError as e:
            logger.error(f"DIAGNOSTIC: SharePointAPIError in get_item_permissions for item {item_id}")
            logger.error(f"DIAGNOSTIC: Full URL that failed: {api_url}")
            logger.error(f"DIAGNOSTIC: Status code: {e.status_code}")
            logger.error(f"DIAGNOSTIC: Error message: {e}")
            raise e
        except Exception as e:
            logger.error(f"DIAGNOSTIC: Unexpected error in get_item_permissions for item {item_id}")
            logger.error(f"DIAGNOSTIC: Full URL that failed: {api_url}")
            logger.error(f"DIAGNOSTIC: Error type: {type(e)}")
            logger.error(f"DIAGNOSTIC: Error message: {e}")
            logger.error(f"Failed to get item permissions for {item_id}: {e}")
            raise SharePointAPIError(f"Failed to get item permissions: {e}")

    async def check_unique_permissions(
        self,
        site_url: str,
        library_id: str,
        item_id: int
    ) -> bool:
        """Check if an item has unique permissions."""
        # Validate inputs
        if not self._is_valid_guid(library_id):
            raise SharePointAPIError(f"Invalid library_id format: {library_id}")
        if not isinstance(item_id, int) or item_id <= 0:
            raise SharePointAPIError(f"Invalid item_id: {item_id} (must be positive integer)")

        # Fix: Use correct SharePoint REST API URL format
        api_url = f"{site_url}/_api/web/lists/getbyid('{library_id}')/items({item_id})?$select=Id,HasUniqueRoleAssignments"

        # DIAGNOSTIC LOGGING for HTTP 400 debugging
        logger.info(f"DIAGNOSTIC: Checking unique permissions")
        logger.info(f"DIAGNOSTIC: site_url = {site_url}")
        logger.info(f"DIAGNOSTIC: library_id = {library_id}")
        logger.info(f"DIAGNOSTIC: item_id = {item_id} (type: {type(item_id)})")
        logger.info(f"DIAGNOSTIC: Constructed URL = {api_url}")

        try:
            response = await self.get_with_retry(api_url)
            # Handle different response formats
            if isinstance(response, dict):
                # Check for value in different formats
                if "HasUniqueRoleAssignments" in response:
                    return response["HasUniqueRoleAssignments"]
                elif "d" in response and "HasUniqueRoleAssignments" in response["d"]:
                    return response["d"]["HasUniqueRoleAssignments"]
            return False
        except SharePointAPIError as e:
            # Some items (like certain system items) might not support this property
            # In such cases, assume they inherit permissions
            if e.status_code == 400:
                logger.error(f"DIAGNOSTIC: HTTP 400 Error for item {item_id}")
                logger.error(f"DIAGNOSTIC: Full URL that failed: {api_url}")
                logger.error(f"DIAGNOSTIC: Error message: {e}")
                logger.debug(f"Item {item_id} doesn't support HasUniqueRoleAssignments property, assuming inherited permissions")
                return False
            else:
                logger.error(f"Failed to check unique permissions for {item_id}: {e}")
                return False
        except Exception as e:
            logger.error(f"DIAGNOSTIC: Unexpected error for item {item_id}")
            logger.error(f"DIAGNOSTIC: Full URL that failed: {api_url}")
            logger.error(f"DIAGNOSTIC: Error message: {e}")
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
