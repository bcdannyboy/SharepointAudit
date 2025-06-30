from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional
from types import SimpleNamespace

from ..api.graph_client import GraphAPIClient
from ..api.sharepoint_client import SharePointAPIClient
from ..database.repository import DatabaseRepository
from ..cache.cache_manager import CacheManager
from .concurrency import ConcurrencyManager
from .progress_tracker import ProgressTracker
from ..utils.checkpoint_manager import CheckpointManager

logger = logging.getLogger(__name__)


class DiscoveryModule:
    """Discovers SharePoint sites and their contents."""

    def __init__(
        self,
        graph_client: GraphAPIClient,
        sp_client: SharePointAPIClient,
        db_repo: DatabaseRepository,
        checkpoint_manager: CheckpointManager,
        cache_manager: "CacheManager | None" = None,
        concurrency_manager: "ConcurrencyManager | None" = None,
        max_concurrent_sites: int = 20,
        max_concurrent_operations: int = 50,
    ) -> None:
        self.graph_client = graph_client
        self.sp_client = sp_client
        self.db_repo = db_repo
        self.checkpoints = checkpoint_manager
        self.cache = cache_manager
        self.concurrency_manager = concurrency_manager
        self.progress_tracker = ProgressTracker()
        self.site_semaphore = asyncio.Semaphore(max_concurrent_sites)
        self.operation_semaphore = asyncio.Semaphore(max_concurrent_operations)

    async def _run_api_task(self, coro):
        if self.concurrency_manager:
            return await self.concurrency_manager.run_api_task(coro)
        return await coro

    async def run_discovery(self, run_id: str) -> None:
        """Orchestrates the full discovery process."""
        self.progress_tracker.start("Site Discovery")

        # Get delta token from checkpoint
        sites_delta_token = await self.checkpoints.restore_checkpoint(run_id, "sites_delta_token")

        # Discover all sites using delta query
        result = await self._run_api_task(
            self.graph_client.get_all_sites_delta(sites_delta_token)
        )

        # Save sites to database
        if isinstance(result, dict) and 'value' in result:
            site_records = []
            for site in result['value']:
                site_data = self._site_to_dict(site)
                if site_data:
                    site_records.append(site_data)

            if site_records:
                await self.db_repo.bulk_upsert('sites', site_records, unique_columns=['site_id'])
                logger.info(f"Saved/updated {len(site_records)} sites in database")

        # Save new delta token
        if isinstance(result, dict) and '@odata.deltaLink' in result:
            # Extract the delta token from the deltaLink URL
            import urllib.parse
            parsed_url = urllib.parse.urlparse(result['@odata.deltaLink'])
            query_params = urllib.parse.parse_qs(parsed_url.query)
            if 'token' in query_params:
                delta_token = query_params['token'][0]
                await self.checkpoints.save_checkpoint(run_id, "sites_delta_token", delta_token)

        sites_count = len(result.get('value', [])) if isinstance(result, dict) else 0
        self.progress_tracker.finish("Site Discovery", f"Found {sites_count} sites")

        # Discover content for each site in parallel with semaphore control
        if isinstance(result, dict) and 'value' in result:
            tasks = []
            for site in result['value']:
                task = self._discover_site_with_semaphore(run_id, site)
                tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=True)

    async def _discover_site_with_semaphore(self, run_id: str, site: Any) -> None:
        """Discovers site content with semaphore control."""
        async with self.site_semaphore:
            try:
                await self.discover_site_content(run_id, site)
            except Exception as e:
                site_id = site.get('id', 'unknown') if isinstance(site, dict) else getattr(site, 'id', 'unknown')
                logger.error(f"Error discovering site {site_id}: {e}")

    async def discover_site_content(self, run_id: str, site: Any) -> None:
        """Discovers all content for a single site."""
        # Handle both dict and object access
        if isinstance(site, dict):
            site_id = site.get('id', '')
            site_title = site.get('displayName', site.get('name', 'Unknown'))
        else:
            site_id = getattr(site, 'id', '')
            site_title = getattr(site, 'displayName', getattr(site, 'name', 'Unknown'))

        # Check if this site was already processed
        checkpoint_key = f"site_{site_id}_status"
        status = await self.checkpoints.restore_checkpoint(run_id, checkpoint_key)
        if status == "completed":
            self.progress_tracker.skip(f"Site {site_title}", "Already processed")
            return

        self.progress_tracker.start(f"Site {site_title}")

        try:
            # Discover libraries, lists, and subsites in parallel
            tasks = [
                self._discover_libraries(site),
                self._discover_lists(site),
                self._discover_subsites(run_id, site),
            ]

            libraries, lists, subsites = await asyncio.gather(*tasks, return_exceptions=True)

            # Discover folders and files within libraries
            if isinstance(libraries, list):
                library_tasks = []
                for library in libraries:
                    if isinstance(library, dict):
                        task = self._discover_library_contents(site, library)
                        library_tasks.append(task)

                if library_tasks:
                    await asyncio.gather(*library_tasks, return_exceptions=True)

            # Mark site as completed
            await self.checkpoints.save_checkpoint(run_id, checkpoint_key, "completed")
            self.progress_tracker.finish(f"Site {site_title}")

        except Exception as e:
            logger.error(f"Error processing site {site_title}: {e}")
            self.progress_tracker.finish(f"Site {site_title}", f"Error: {str(e)}")

    async def _discover_libraries(self, site: Any) -> List[Dict[str, Any]]:
        """Enumerate document libraries for the given site."""
        async with self.operation_semaphore:
            # Handle both dict and object access
            if isinstance(site, dict):
                site_id = site.get('id')
            else:
                site_id = getattr(site, 'id', None)
            if not site_id:
                return []

            try:
                cache_key = f"site_libraries:{site_id}"
                if self.cache:
                    cached = await self.cache.get(cache_key)
                    if cached is not None:
                        return cached

                url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
                data = await self._run_api_task(
                    self.graph_client.get_with_retry(url)
                )
                libraries = data.get("value", [])

                # Save libraries to database
                if libraries:
                    records = []
                    for lib in libraries:
                        record = {
                            "library_id": lib.get("id"),
                            "site_id": site_id,
                            "name": lib.get("name", ""),
                            "description": lib.get("description"),
                            "created_at": lib.get("createdDateTime"),
                        }
                        records.append(record)

                    if records:
                        await self.db_repo.bulk_insert("libraries", records)
                        logger.debug(f"Saved {len(records)} libraries for site {site_id}")

                if self.cache:
                    await self.cache.set(cache_key, libraries, ttl=3600)

                return libraries

            except Exception as e:
                logger.error(f"Error discovering libraries for site {site_id}: {e}")
                return []

    async def _discover_lists(self, site: Any) -> List[Dict[str, Any]]:
        """Enumerate lists for the given site."""
        async with self.operation_semaphore:
            # Handle both dict and object access
            if isinstance(site, dict):
                site_id = site.get('id')
            else:
                site_id = getattr(site, 'id', None)
            if not site_id:
                return []

            try:
                cache_key = f"site_lists:{site_id}"
                if self.cache:
                    cached = await self.cache.get(cache_key)
                    if cached is not None:
                        return cached

                url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
                data = await self._run_api_task(
                    self.graph_client.get_with_retry(url)
                )
                lists = data.get("value", [])

                # Note: Lists are not document libraries, so we might want to store them separately
                # For now, we'll just return them

                if self.cache:
                    await self.cache.set(cache_key, lists, ttl=3600)

                return lists

            except Exception as e:
                logger.error(f"Error discovering lists for site {site_id}: {e}")
                return []

    async def _discover_subsites(self, run_id: str, site: Any) -> List[Dict[str, Any]]:
        """Discover subsites and recursively process their contents."""
        async with self.operation_semaphore:
            # Handle both dict and object access
            if isinstance(site, dict):
                site_id = site.get('id')
            else:
                site_id = getattr(site, 'id', None)
            if not site_id:
                return []

            try:
                cache_key = f"subsites:{site_id}"
                if self.cache:
                    cached = await self.cache.get(cache_key)
                    if cached is not None:
                        subsites = cached
                    else:
                        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/sites"
                        data = await self._run_api_task(
                            self.graph_client.get_with_retry(url)
                        )
                        subsites = data.get("value", [])
                        await self.cache.set(cache_key, subsites, ttl=3600)
                else:
                    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/sites"
                    data = await self._run_api_task(
                        self.graph_client.get_with_retry(url)
                    )
                    subsites = data.get("value", [])

                # Recursively discover content for each subsite
                if subsites:
                    tasks = []
                    for subsite in subsites:
                        # Convert dict to object-like structure if needed
                        if isinstance(subsite, dict):
                            subsite_obj = SimpleNamespace(**subsite)
                        else:
                            subsite_obj = subsite

                        task = self._discover_site_with_semaphore(run_id, subsite_obj)
                        tasks.append(task)

                    await asyncio.gather(*tasks, return_exceptions=True)

                return subsites

            except Exception as e:
                logger.error(f"Error discovering subsites for site {site_id}: {e}")
                return []

    async def _discover_library_contents(self, site: Any, library: Dict[str, Any]) -> None:
        """Discover folders and files within a library."""
        library_id = library.get("id")
        library_name = library.get("name", "Unknown")

        if not library_id:
            return

        try:
            # Start with root folder
            await self._discover_folder_contents(site, library, None, "/")

        except Exception as e:
            logger.error(f"Error discovering contents of library {library_name}: {e}")

    async def _discover_folder_contents(
        self,
        site: Any,
        library: Dict[str, Any],
        parent_folder_id: Optional[str],
        folder_path: str,
        page_size: int = 200
    ) -> None:
        """Recursively discover folders and files within a folder."""
        async with self.operation_semaphore:
            # Handle both dict and object access
            if isinstance(site, dict):
                site_id = site.get('id')
            else:
                site_id = getattr(site, 'id', None)
            library_id = library.get("id")

            if not site_id or not library_id:
                return

            try:
                # Build the URL for listing folder contents
                if folder_path == "/":
                    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{library_id}/root/children"
                else:
                    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{library_id}/root:{folder_path}:/children"

                # Add query parameters for batching
                url += f"?$top={page_size}"

                # Keep fetching pages until we get all items
                all_folders = []
                all_files = []

                while url:
                    data = await self._run_api_task(
                        self.graph_client.get_with_retry(url)
                    )
                    items = data.get("value", [])

                    # Separate folders and files
                    folders = []
                    files = []

                    for item in items:
                        if "folder" in item:  # It's a folder
                            folder_record = {
                                "folder_id": item.get("id"),
                                "library_id": library_id,
                                "parent_folder_id": parent_folder_id,
                                "name": item.get("name", ""),
                                "server_relative_url": item.get("webUrl", ""),
                                "created_at": item.get("createdDateTime"),
                                "created_by": item.get("createdBy", {}).get("user", {}).get("email"),
                                "modified_at": item.get("lastModifiedDateTime"),
                                "modified_by": item.get("lastModifiedBy", {}).get("user", {}).get("email"),
                            }
                            folders.append(folder_record)
                            all_folders.append(folder_record)
                        else:  # It's a file
                            file_record = {
                                "file_id": item.get("id"),
                                "folder_id": parent_folder_id,
                                "library_id": library_id,
                                "name": item.get("name", ""),
                                "server_relative_url": item.get("webUrl", ""),
                                "size_bytes": item.get("size", 0),
                                "content_type": item.get("file", {}).get("mimeType"),
                                "created_at": item.get("createdDateTime"),
                                "created_by": item.get("createdBy", {}).get("user", {}).get("email"),
                                "modified_at": item.get("lastModifiedDateTime"),
                                "modified_by": item.get("lastModifiedBy", {}).get("user", {}).get("email"),
                            }
                            files.append(file_record)
                            all_files.append(file_record)

                    # Get next page URL if available
                    url = data.get("@odata.nextLink")

                # Save folders and files to database in batches
                if all_folders:
                    await self.db_repo.bulk_insert("folders", all_folders)
                    logger.debug(f"Saved {len(all_folders)} folders from {folder_path}")

                if all_files:
                    await self.db_repo.bulk_insert("files", all_files)
                    logger.debug(f"Saved {len(all_files)} files from {folder_path}")

                # Recursively discover contents of subfolders
                if all_folders:
                    tasks = []
                    for folder in all_folders:
                        folder_name = folder["name"]
                        folder_id = folder["folder_id"]
                        new_path = f"{folder_path}/{folder_name}" if folder_path != "/" else f"/{folder_name}"

                        task = self._discover_folder_contents(
                            site, library, folder_id, new_path, page_size
                        )
                        tasks.append(task)

                    # Process subfolders with limited concurrency
                    if tasks:
                        # Process in smaller batches to avoid overwhelming the API
                        batch_size = 5
                        for i in range(0, len(tasks), batch_size):
                            batch = tasks[i:i + batch_size]
                            await asyncio.gather(*batch, return_exceptions=True)

            except Exception as e:
                logger.error(f"Error discovering folder contents at {folder_path}: {e}")

    def _site_to_dict(self, site: Any) -> Optional[Dict[str, Any]]:
        """Convert a site object to a dictionary for database storage."""
        try:
            # Handle both dict and object-like structures
            if isinstance(site, dict):
                site_dict = site
            else:
                # Try to extract attributes
                site_dict = {
                    "id": getattr(site, "id", None),
                    "displayName": getattr(site, "displayName", None),
                    "name": getattr(site, "name", None),
                    "webUrl": getattr(site, "webUrl", None),
                    "description": getattr(site, "description", None),
                    "createdDateTime": getattr(site, "createdDateTime", None),
                    "lastModifiedDateTime": getattr(site, "lastModifiedDateTime", None),
                }

            return {
                "site_id": site_dict.get("id"),
                "url": site_dict.get("webUrl", ""),
                "title": site_dict.get("displayName") or site_dict.get("name", ""),
                "description": site_dict.get("description"),
                "created_at": site_dict.get("createdDateTime"),
                "last_modified": site_dict.get("lastModifiedDateTime"),
            }

        except Exception as e:
            logger.error(f"Error converting site to dict: {e}")
            return None
