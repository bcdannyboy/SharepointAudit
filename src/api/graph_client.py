import asyncio
import logging
import time
from typing import Any, Optional
from datetime import datetime, timezone

import aiohttp

from api.auth_manager import AuthenticationManager
from utils.rate_limiter import RateLimiter
from utils.retry_handler import RetryStrategy, RetryConfig
from utils.exceptions import GraphAPIError

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
        self._session: Optional[aiohttp.ClientSession] = None

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
            raise GraphAPIError(f"Authentication failed: {e}") from e

    async def get_with_retry(self, url: str, **kwargs) -> Any:
        async def _do_get():
            logger.debug(f"[DEBUG API] Starting GET request to: {url}")
            await self.rate_limiter.acquire("simple_get")
            start = time.time()

            # Get auth headers and merge with any provided headers
            logger.debug("[DEBUG API] Getting auth headers")
            auth_headers = await self._get_auth_headers()
            headers = kwargs.get("headers", {})
            headers.update(auth_headers)
            kwargs["headers"] = headers

            logger.debug("[DEBUG API] Getting session and sending request")
            session = await self._get_session()
            logger.debug(f"[DEBUG API] Sending GET request (timeout: {kwargs.get('timeout', 'default')})")
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
            logger.debug("[DEBUG API] Response received, parsing JSON")
            data = await resp.json()
            elapsed = time.time() - start
            logger.info("GET %s succeeded in %.2fs", url, elapsed)
            logger.debug(f"[DEBUG API] Response contains {len(data.get('value', []))} items")
            return data

        logger.debug(f"[DEBUG API] Executing with retry strategy for: {url}")
        try:
            result = await self.retry_strategy.execute_with_retry(url, _do_get)
            logger.debug("[DEBUG API] Retry strategy completed successfully")
            return result
        except Exception as e:
            logger.error(f"[DEBUG API] Request failed after retries: {url} - {str(e)}")
            raise

    async def post_with_retry(self, url: str, **kwargs) -> Any:
        async def _do_post():
            await self.rate_limiter.acquire("simple_get")
            start = time.time()

            # Get auth headers and merge with any provided headers
            auth_headers = await self._get_auth_headers()
            headers = kwargs.get("headers", {})
            headers.update(auth_headers)
            kwargs["headers"] = headers

            session = await self._get_session()
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

    async def get_all_sites_delta(self, delta_token: str | None = None, active_only: bool = False) -> Any:
        """Retrieve all sites using delta query or optimized search when filtering active sites.

        Args:
            delta_token: Optional delta token for incremental queries
            active_only: If True, use server-side filtering to return only active sites
        """
        # Performance measurement
        start_time = time.time()

        if active_only:
            # Use optimized Search API with server-side filtering instead of delta API
            logger.info("Active-only mode: Using optimized Search API with comprehensive server-side filtering")
            return await self._get_active_sites_optimized()
        else:
            # Use standard delta API for backward compatibility when active_only=False
            logger.info("Using standard delta API for all sites")
            url = "https://graph.microsoft.com/v1.0/sites/delta"
            if delta_token:
                url += f"?token={delta_token}"

            result = await self.get_with_retry(url)
            elapsed_time = time.time() - start_time
            sites_count = len(result.get('value', [])) if isinstance(result, dict) else 0
            logger.info(f"Delta API completed in {elapsed_time:.2f}s, retrieved {sites_count} sites")
            return result

    async def _get_active_sites_optimized(self) -> Any:
        """Optimized server-side filtering using Search API for active sites only.

        This method implements comprehensive filtering at the API level to avoid
        fetching all sites and then filtering client-side.
        """
        start_time = time.time()

        # Enhanced Search API query with comprehensive server-side filtering
        search_url = "https://graph.microsoft.com/v1.0/search/query"

        # Comprehensive query string that excludes:
        # - Personal sites (OneDrive): NOT path:*/personal/*
        # - Archived sites: NOT IsArchived:true
        # - System templates: NOT WebTemplate:SPSMSITEHOST AND NOT WebTemplate:RedirectSite
        # - App catalog sites: NOT path:*/appcatalog/*
        # - Common archived naming patterns: NOT displayName:*archived* etc.
        query_string = (
            "contentclass:STS_Site AND "
            "NOT path:*/personal/* AND "
            "NOT IsArchived:true AND "
            "NOT WebTemplate:SPSMSITEHOST AND "
            "NOT WebTemplate:RedirectSite AND "
            "NOT path:*/appcatalog/* AND "
            "NOT displayName:*archived* AND "
            "NOT displayName:*test* AND "
            "NOT displayName:*demo* AND "
            "NOT displayName:*old* AND "
            "NOT displayName:*backup*"
        )

        search_body = {
            "requests": [{
                "entityTypes": ["site"],
                "query": {
                    "queryString": query_string
                },
                "from": 0,
                "size": 500,  # Maximum allowed per request
                "sortProperties": [
                    {
                        "name": "lastModifiedTime",
                        "isDescending": True
                    }
                ]
            }]
        }

        all_active_sites = []
        seen_site_ids = set()
        api_calls_made = 0
        total_filtered_out = 0
        filtering_stats = {
            "personal_sites": 0,
            "archived_sites": 0,
            "system_sites": 0,
            "naming_pattern_filtered": 0,
            "duplicates": 0
        }

        logger.info(f"Starting optimized search with query: {query_string}")

        # Paginate through search results with enhanced error handling
        while True:
            try:
                api_calls_made += 1
                logger.debug(f"Making Search API call #{api_calls_made} (from: {search_body['requests'][0]['from']})")

                search_result = await self.post_with_retry(search_url, json=search_body)

                if not search_result or 'value' not in search_result or not search_result['value']:
                    logger.debug("No more search results available")
                    break

                hits_containers = search_result['value'][0].get('hitsContainers', [])
                if not hits_containers:
                    logger.debug("No hits containers found in search result")
                    break

                found_results_in_page = False

                for container in hits_containers:
                    hits = container.get('hits', [])
                    logger.debug(f"Processing {len(hits)} hits from container")

                    for hit in hits:
                        found_results_in_page = True
                        resource = hit.get('resource', {})

                        # Convert search result to match delta format
                        site_data = {
                            'id': resource.get('id', ''),
                            'webUrl': resource.get('webUrl', ''),
                            'displayName': resource.get('displayName', resource.get('name', '')),
                            'name': resource.get('name', ''),
                            'createdDateTime': resource.get('createdDateTime'),
                            'lastModifiedDateTime': resource.get('lastModifiedDateTime'),
                            'description': resource.get('description', ''),
                            'webTemplate': resource.get('webTemplate', ''),
                            'isArchived': resource.get('isArchived', False)
                        }

                        # Apply additional client-side validation for edge cases
                        site_url = site_data.get('webUrl', '').lower()
                        site_name = site_data.get('displayName', '').lower()
                        site_id = site_data.get('id', '')

                        # Skip if no valid site ID
                        if not site_id:
                            logger.debug("Skipping site with no ID")
                            continue

                        # Check for duplicates
                        if site_id in seen_site_ids:
                            filtering_stats["duplicates"] += 1
                            logger.debug(f"Skipping duplicate site: {site_data.get('displayName', 'Unknown')} (ID: {site_id})")
                            continue

                        # Additional validation for personal sites (edge case protection)
                        if any(pattern in site_url for pattern in ['/personal/', '-my.sharepoint.com']):
                            filtering_stats["personal_sites"] += 1
                            logger.debug(f"Filtering out personal site: {site_data.get('displayName', 'Unknown')}")
                            continue

                        # Additional validation for system sites
                        if any(pattern in site_url for pattern in ['/appcatalog/', '/sites/appcatalog']):
                            filtering_stats["system_sites"] += 1
                            logger.debug(f"Filtering out system site: {site_data.get('displayName', 'Unknown')}")
                            continue

                        # Additional naming pattern validation (edge case protection)
                        if any(pattern in site_name for pattern in [
                            'archived', '_archive', 'test-', '_test', 'demo-', '_demo',
                            'old-', '_old', 'backup', '_backup', 'teamchannel', 'template'
                        ]):
                            filtering_stats["naming_pattern_filtered"] += 1
                            logger.debug(f"Filtering out by naming pattern: {site_data.get('displayName', 'Unknown')}")
                            continue

                        # Site passed all filters
                        seen_site_ids.add(site_id)
                        all_active_sites.append(site_data)

                    # Check for more results in this container
                    if container.get('moreResultsAvailable', False):
                        # Update pagination
                        search_body['requests'][0]['from'] += search_body['requests'][0]['size']
                        logger.debug(f"More results available, setting next from to: {search_body['requests'][0]['from']}")
                    else:
                        logger.debug("No more results available in container")
                        found_results_in_page = False
                        break

                # If no results found in this page, we're done
                if not found_results_in_page:
                    logger.debug("No results found in page, ending pagination")
                    break

                # Safety limit to prevent excessive API calls
                if len(all_active_sites) >= 5000:
                    logger.warning(f"Reached safety limit of 5000 sites (API calls: {api_calls_made})")
                    break

                if api_calls_made >= 20:  # Reasonable limit for API calls
                    logger.warning(f"Reached API call limit of 20 (sites found: {len(all_active_sites)})")
                    break

            except Exception as e:
                logger.error(f"Search API call #{api_calls_made} failed: {e}")

                # If this is the first call, fall back to delta API with client-side filtering
                if api_calls_made == 1:
                    logger.warning("Search API completely failed, falling back to delta API with client-side filtering")
                    return await self._fallback_to_delta_with_filtering()
                else:
                    # If we've already got some results, continue with what we have
                    logger.warning(f"Search API failed after {api_calls_made} calls, using {len(all_active_sites)} sites collected so far")
                    break

        # Calculate performance metrics
        elapsed_time = time.time() - start_time
        total_filtered_out = sum(filtering_stats.values())

        # Log comprehensive filtering statistics
        logger.info(f"Optimized Search API completed in {elapsed_time:.2f}s:")
        logger.info(f"  - API calls made: {api_calls_made}")
        logger.info(f"  - Active sites found: {len(all_active_sites)}")
        logger.info(f"  - Sites filtered out: {total_filtered_out}")
        logger.info(f"    * Personal sites: {filtering_stats['personal_sites']}")
        logger.info(f"    * Archived sites: {filtering_stats['archived_sites']}")
        logger.info(f"    * System sites: {filtering_stats['system_sites']}")
        logger.info(f"    * Naming patterns: {filtering_stats['naming_pattern_filtered']}")
        logger.info(f"    * Duplicates: {filtering_stats['duplicates']}")
        logger.info(f"  - Average sites per API call: {len(all_active_sites) / max(api_calls_made, 1):.1f}")

        # Return in delta API compatible format
        result = {
            'value': all_active_sites,
            # Don't include delta token since we're using search
            '@odata.context': 'https://graph.microsoft.com/v1.0/$metadata#sites',
            '_search_metadata': {
                'elapsed_time': elapsed_time,
                'api_calls_made': api_calls_made,
                'filtering_stats': filtering_stats,
                'query_used': query_string
            }
        }

        return result

    async def _fallback_to_delta_with_filtering(self) -> Any:
        """Fallback method when Search API fails - uses delta API with client-side filtering."""
        logger.info("Executing fallback: Delta API with enhanced client-side filtering")
        start_time = time.time()

        try:
            # Get all sites via delta API
            url = "https://graph.microsoft.com/v1.0/sites/delta"
            result = await self.get_with_retry(url)

            if not isinstance(result, dict) or 'value' not in result:
                logger.error("Invalid response from delta API")
                return {'value': []}

            all_sites = result['value']
            logger.info(f"Delta API returned {len(all_sites)} total sites")

            # Apply comprehensive client-side filtering
            filtered_sites = []
            filtering_stats = {
                "personal_sites": 0,
                "archived_sites": 0,
                "system_sites": 0,
                "naming_pattern_filtered": 0,
                "template_filtered": 0,
                "old_sites": 0
            }

            # Current date for age-based filtering
            one_year_ago = datetime.now(timezone.utc).replace(year=datetime.now().year - 1)

            for site in all_sites:
                site_url = site.get('webUrl', '').lower()
                site_name = site.get('displayName', site.get('name', '')).lower()

                # Filter out personal sites (OneDrive)
                if any(pattern in site_url for pattern in ['/personal/', '-my.sharepoint.com']):
                    filtering_stats["personal_sites"] += 1
                    continue

                # Filter out archived sites
                if site.get('isArchived', False):
                    filtering_stats["archived_sites"] += 1
                    continue

                # Filter out system sites
                if any(pattern in site_url for pattern in ['/appcatalog/', '/sites/appcatalog']):
                    filtering_stats["system_sites"] += 1
                    continue

                # Filter by naming patterns
                if any(pattern in site_name for pattern in [
                    'archived', '_archive', 'test-', '_test', 'demo-', '_demo',
                    'old-', '_old', 'backup', '_backup', 'template'
                ]):
                    filtering_stats["naming_pattern_filtered"] += 1
                    continue

                # Filter by site template (if available)
                web_template = site.get('webTemplate', '').upper()
                if web_template in ['SPSMSITEHOST', 'REDIRECTSITE', 'TEAMCHANNEL#1', 'APPCATALOG#0']:
                    filtering_stats["template_filtered"] += 1
                    continue

                # Filter by last modified date (sites not modified in over a year)
                if 'lastModifiedDateTime' in site:
                    try:
                        last_modified = datetime.fromisoformat(site['lastModifiedDateTime'].replace('Z', '+00:00'))
                        if last_modified < one_year_ago:
                            filtering_stats["old_sites"] += 1
                            continue
                    except (ValueError, TypeError):
                        # If we can't parse the date, include the site
                        pass

                # Site passed all filters
                filtered_sites.append(site)

            elapsed_time = time.time() - start_time
            total_filtered_out = sum(filtering_stats.values())

            # Log filtering results
            logger.info(f"Client-side filtering completed in {elapsed_time:.2f}s:")
            logger.info(f"  - Total sites processed: {len(all_sites)}")
            logger.info(f"  - Active sites found: {len(filtered_sites)}")
            logger.info(f"  - Sites filtered out: {total_filtered_out}")
            logger.info(f"    * Personal sites: {filtering_stats['personal_sites']}")
            logger.info(f"    * Archived sites: {filtering_stats['archived_sites']}")
            logger.info(f"    * System sites: {filtering_stats['system_sites']}")
            logger.info(f"    * Naming patterns: {filtering_stats['naming_pattern_filtered']}")
            logger.info(f"    * Templates: {filtering_stats['template_filtered']}")
            logger.info(f"    * Old sites (1+ year): {filtering_stats['old_sites']}")

            # Update result with filtered sites and remove pagination links
            result['value'] = filtered_sites

            # CRITICAL FIX: Remove pagination links to prevent discovery module
            # from continuing to fetch unfiltered pages
            if '@odata.nextLink' in result:
                del result['@odata.nextLink']
                logger.info("Removed @odata.nextLink to prevent unfiltered pagination")

            if '@odata.deltaLink' in result:
                del result['@odata.deltaLink']
                logger.info("Removed @odata.deltaLink to prevent unfiltered pagination")

            result['_fallback_metadata'] = {
                'elapsed_time': elapsed_time,
                'filtering_stats': filtering_stats,
                'fallback_reason': 'Search API failed',
                'pagination_removed': True,
                'is_complete_result': True
            }

            logger.info(f"Fallback filtering complete: returning {len(filtered_sites)} sites with no pagination")
            return result

        except Exception as e:
            logger.error(f"Fallback filtering failed: {e}")
            # Return empty result rather than failing completely
            return {
                'value': [],
                '_error_metadata': {
                    'error': str(e),
                    'fallback_failed': True
                }
            }

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

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        # Simple approach: always create new session if current one is None or closed
        if self._session is None:
            self._session = aiohttp.ClientSession()
        else:
            # Check if session is closed using try/catch for robustness
            try:
                # Try to access the closed property - handle any exceptions gracefully
                if getattr(self._session, 'closed', True):  # Default to True if attribute doesn't exist
                    self._session = aiohttp.ClientSession()
            except Exception:
                # If any error accessing session state, create new session
                self._session = aiohttp.ClientSession()

        return self._session

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session:
            try:
                # Only close if not already closed
                if not getattr(self._session, 'closed', True):
                    await self._session.close()
            except Exception:
                # Ignore any errors during close
                pass
            finally:
                self._session = None
