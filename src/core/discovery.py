from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from types import SimpleNamespace
from datetime import datetime, timezone

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
        active_only: bool = False,
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
        self.active_only = active_only

        # Store max values for monitoring
        self.max_concurrent_sites = max_concurrent_sites
        self.max_concurrent_operations = max_concurrent_operations

        # Progress tracking counters
        self.discovered_counts = {
            "sites": 0,
            "libraries": 0,
            "folders": 0,
            "files": 0
        }
        self.processed_sites = 0

        # Progress update tracking
        self.last_progress_time = time.time()
        self.progress_interval = 30  # Update progress every 30 seconds

        # Semaphore monitoring
        self.last_semaphore_log_time = time.time()
        self.semaphore_log_interval = 60  # Log semaphore status every minute

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
            self.graph_client.get_all_sites_delta(sites_delta_token, active_only=self.active_only)
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
        self.discovered_counts["sites"] = sites_count
        self.progress_tracker.finish("Site Discovery", f"Found {sites_count} sites")

        # Discover content for each site in parallel with semaphore control
        if isinstance(result, dict) and 'value' in result:
            tasks = []
            for site in result['value']:
                task = self._discover_site_with_semaphore(run_id, site)
                tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=True)

        # Final progress summary
        logger.info(
            f"\n=== Discovery Complete ===\n"
            f"Sites discovered: {self.discovered_counts['sites']}\n"
            f"Libraries discovered: {self.discovered_counts['libraries']}\n"
            f"Folders discovered: {self.discovered_counts['folders']}\n"
            f"Files discovered: {self.discovered_counts['files']}\n"
            f"========================="
        )

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
            self.processed_sites += 1

            # Log completion with detailed progress
            logger.info(f"[PROGRESS] Finished: Site {site_title} - Done")
            logger.info(
                f"Site {self.processed_sites}/{self.discovered_counts['sites']}: {site_title} - "
                f"Total progress: {self.discovered_counts['libraries']} libraries, "
                f"{self.discovered_counts['folders']} folders, {self.discovered_counts['files']} files"
            )
            self.progress_tracker.finish(f"Site {site_title}")

        except Exception as e:
            logger.error(f"Error processing site {site_title}: {e}", exc_info=True)
            logger.info(f"[PROGRESS] Finished: Site {site_title} - Failed")
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
                        self.discovered_counts["libraries"] += len(records)
                        logger.info(f"Discovered {len(records)} libraries in site (Total: {self.discovered_counts['libraries']})")

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
            logger.warning(f"Skipping library {library_name} - no ID found")
            return

        try:
            logger.debug(f"[DEBUG] Starting discovery of library: {library_name} (ID: {library_id})")
            start_time = time.time()

            # Start with root folder - use queue-based discovery
            logger.info(f"[QUEUE] Starting queue-based discovery for library {library_name}")
            await self._discover_library_contents_queue(site, library)

            elapsed = time.time() - start_time
            logger.debug(f"[DEBUG] Completed discovery of library {library_name} in {elapsed:.2f}s")

            # Check if it's time for a progress update
            current_time = time.time()
            if current_time - self.last_progress_time > self.progress_interval:
                self.last_progress_time = current_time
                logger.info(
                    f"\n=== Progress Update ===\n"
                    f"Sites processed: {self.processed_sites}/{self.discovered_counts['sites']}\n"
                    f"Libraries discovered: {self.discovered_counts['libraries']}\n"
                    f"Folders discovered: {self.discovered_counts['folders']}\n"
                    f"Files discovered: {self.discovered_counts['files']}\n"
                    f"=====================\n"
                )

            # Check if it's time for a semaphore status update
            if current_time - self.last_semaphore_log_time > self.semaphore_log_interval:
                self.last_semaphore_log_time = current_time
                site_available = self.site_semaphore._value if hasattr(self.site_semaphore, '_value') else 'unknown'
                op_available = self.operation_semaphore._value if hasattr(self.operation_semaphore, '_value') else 'unknown'
                logger.info(
                    f"\n=== Semaphore Status ===\n"
                    f"Site semaphore: {site_available}/{self.max_concurrent_sites} available\n"
                    f"Operation semaphore: {op_available}/{self.max_concurrent_operations} available\n"
                    f"======================\n"
                )

        except Exception as e:
            logger.error(f"Error discovering contents of library {library_name}: {e}", exc_info=True)

    async def _discover_folder_contents(
        self,
        site: Any,
        library: Dict[str, Any],
        parent_folder_id: Optional[str],
        folder_path: str,
        page_size: int = 200
    ) -> None:
        """Recursively discover folders and files within a folder."""
        logger.debug(f"[DEBUG] Entering _discover_folder_contents for path: {folder_path}")

        # Try to acquire semaphore with timeout to prevent deadlock
        acquired = False
        max_wait_time = 120  # Increased timeout
        backoff_time = 0.5
        start_wait = time.time()

        while not acquired and (time.time() - start_wait) < max_wait_time:
            try:
                # Try to acquire with a short timeout
                await asyncio.wait_for(self.operation_semaphore.acquire(), timeout=5)
                acquired = True
            except asyncio.TimeoutError:
                # Check semaphore availability
                available = self.operation_semaphore._value if hasattr(self.operation_semaphore, '_value') else 0
                logger.debug(f"[DEBUG] Semaphore not available for {folder_path}, {available} slots free. Backing off...")

                # Exponential backoff
                await asyncio.sleep(backoff_time)
                backoff_time = min(backoff_time * 2, 5)  # Cap at 5 seconds

        if not acquired:
            logger.error(f"[DEBUG] Timeout acquiring semaphore for folder {folder_path} after {max_wait_time}s - skipping")
            return

        try:
            # Handle both dict and object access
            if isinstance(site, dict):
                site_id = site.get('id')
            else:
                site_id = getattr(site, 'id', None)
            library_id = library.get("id")
            library_name = library.get("name", "Unknown")

            if not site_id or not library_id:
                logger.warning(f"[DEBUG] Missing site_id or library_id for folder {folder_path}")
                return

            try:
                logger.debug(f"[DEBUG] Processing folder {folder_path} in library {library_name}")
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
                    logger.debug(f"[DEBUG] Fetching page from: {url}")
                    fetch_start = time.time()

                    data = await self._run_api_task(
                        self.graph_client.get_with_retry(url)
                    )

                    fetch_elapsed = time.time() - fetch_start
                    logger.debug(f"[DEBUG] Fetch completed in {fetch_elapsed:.2f}s")

                    items = data.get("value", [])
                    logger.debug(f"[DEBUG] Found {len(items)} items in {folder_path}")

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
                    if url:
                        logger.debug(f"[DEBUG] Found next page link, continuing pagination")

                # Save folders and files to database in batches
                if all_folders:
                    await self.db_repo.bulk_insert("folders", all_folders)
                    self.discovered_counts["folders"] += len(all_folders)
                    logger.info(
                        f"Discovered {len(all_folders)} folders in {folder_path} "
                        f"(Total: {self.discovered_counts['folders']} folders)"
                    )

                if all_files:
                    await self.db_repo.bulk_insert("files", all_files)
                    self.discovered_counts["files"] += len(all_files)
                    logger.info(
                        f"Discovered {len(all_files)} files in {folder_path} "
                        f"(Total: {self.discovered_counts['files']} files)"
                    )

                # Recursively discover contents of subfolders
                if all_folders:
                    logger.debug(f"[DEBUG] Starting recursive discovery of {len(all_folders)} subfolders in {folder_path}")
                    tasks = []
                    for idx, folder in enumerate(all_folders):
                        folder_name = folder["name"]
                        folder_id = folder["folder_id"]
                        new_path = f"{folder_path}/{folder_name}" if folder_path != "/" else f"/{folder_name}"

                        logger.debug(f"[DEBUG] Queueing subfolder {idx+1}/{len(all_folders)}: {new_path}")
                        # Create proper asyncio task to ensure coroutine is scheduled
                        task = asyncio.create_task(
                            self._discover_folder_contents(
                                site, library, folder_id, new_path, page_size
                            )
                        )
                        tasks.append(task)

                    # Process subfolders with limited concurrency
                    if tasks:
                        # Adaptive batch size based on semaphore availability
                        available = self.operation_semaphore._value if hasattr(self.operation_semaphore, '_value') else 1
                        # Use smaller batches when semaphore is low
                        if available < 5:
                            batch_size = 1
                        elif available < 10:
                            batch_size = 2
                        elif available < 20:
                            batch_size = 3
                        else:
                            batch_size = 5

                        logger.debug(f"[DEBUG] Processing {len(tasks)} subfolder tasks in batches of {batch_size} (semaphore available: {available})")

                        for i in range(0, len(tasks), batch_size):
                            batch = tasks[i:i + batch_size]
                            batch_start = time.time()
                            logger.debug(f"[DEBUG] Processing batch {i//batch_size + 1}/{(len(tasks) + batch_size - 1)//batch_size} ({len(batch)} tasks)")

                            try:
                                results = await asyncio.gather(*batch, return_exceptions=True)

                                # Check for exceptions
                                exceptions = [(idx, r) for idx, r in enumerate(results) if isinstance(r, Exception)]
                                if exceptions:
                                    logger.warning(f"[DEBUG] {len(exceptions)} tasks failed in batch")
                                    for task_idx, exc in exceptions:
                                        folder_info = all_folders[i + task_idx]
                                        logger.error(
                                            f"[DEBUG] Failed to process folder {folder_info.get('name', 'Unknown')}: "
                                            f"{type(exc).__name__}: {exc}"
                                        )

                                batch_elapsed = time.time() - batch_start
                                logger.debug(f"[DEBUG] Batch completed in {batch_elapsed:.2f}s")

                                # Adaptive delay based on batch completion time
                                if batch_elapsed > 10:  # If batch took long, add more delay
                                    await asyncio.sleep(1.0)
                                elif batch_elapsed > 5:
                                    await asyncio.sleep(0.5)
                                else:
                                    await asyncio.sleep(0.1)

                            except Exception as e:
                                logger.error(f"[DEBUG] Critical error in batch processing: {e}", exc_info=True)
                                # Try to cancel any pending tasks in this batch
                                for task in batch:
                                    if not task.done():
                                        task.cancel()
                                        logger.debug(f"[DEBUG] Cancelled pending task")

                    logger.debug(f"[DEBUG] Completed recursive discovery for folder {folder_path}")

                # Log progress every 100 folders
                if self.discovered_counts["folders"] % 100 == 0 and self.discovered_counts["folders"] > 0:
                    logger.info(
                        f"Progress update - Processing site {self.processed_sites}/{self.discovered_counts['sites']}: "
                        f"Folders: {self.discovered_counts['folders']}, Files: {self.discovered_counts['files']}"
                    )

            except Exception as e:
                logger.error(f"Error discovering folder contents at {folder_path}: {e}", exc_info=True)
                # Don't re-raise to allow other folders to continue processing
        finally:
            # Always release the semaphore if we acquired it
            if acquired:
                self.operation_semaphore.release()
                logger.debug(f"[DEBUG] Released semaphore for folder {folder_path}")

    async def _discover_library_contents_queue(self, site: Dict[str, Any], library: Dict[str, Any]) -> None:
        """Discover folders and files in a library using queue-based approach to prevent hanging."""
        library_id = library.get('id') or library.get('library_id')
        library_name = library.get('name', 'Unknown')
        site_id = site.get('id') or site.get('site_id')
        drive_id = library.get('driveId') or library.get('drive_id', library_id)

        logger.info(f"[QUEUE] Starting queue-based discovery for library {library_name}")

        # Initialize queue with root folder
        folders_to_process = asyncio.Queue()
        await folders_to_process.put({
            'parent_id': None,
            'path': '/',
            'item_id': 'root',
            'depth': 0
        })

        # Batch collections
        folders_batch = []
        files_batch = []
        processed_count = 0
        max_depth = 10  # Prevent infinite recursion
        batch_size = 50  # Smaller batch size for better progress tracking

        while not folders_to_process.empty():
            try:
                folder_info = await asyncio.wait_for(folders_to_process.get(), timeout=5.0)
                parent_id = folder_info['parent_id']
                folder_path = folder_info['path']
                folder_item_id = folder_info['item_id']
                depth = folder_info['depth']

                if depth > max_depth:
                    logger.warning(f"Max depth {max_depth} reached at {folder_path}, skipping deeper folders")
                    continue

                # Acquire semaphore only for the API call
                acquired = False
                try:
                    await asyncio.wait_for(self.operation_semaphore.acquire(), timeout=10.0)
                    acquired = True
                except asyncio.TimeoutError:
                    logger.warning(f"[QUEUE] Timeout acquiring semaphore for {folder_path}, skipping")
                    continue

                try:
                    # Construct the URL based on Graph API patterns
                    if folder_item_id == 'root':
                        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
                    else:
                        # For subfolders, we need to use the path
                        if folder_path == '/':
                            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children"
                        else:
                            # Clean the path
                            clean_path = folder_path.strip('/').replace('//', '/')
                            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{clean_path}:/children"

                    url += "?$top=200"

                    # Fetch items
                    items = []
                    next_url = url

                    while next_url:
                        try:
                            response = await self.graph_client.get_with_retry(next_url)
                            if isinstance(response, dict):
                                items.extend(response.get('value', []))
                                next_url = response.get('@odata.nextLink')
                            else:
                                break
                        except Exception as e:
                            logger.error(f"[QUEUE] Error fetching {next_url}: {e}")
                            break

                    logger.debug(f"[QUEUE] Fetched {len(items)} items from {folder_path}")

                    # Process items
                    folder_count = 0
                    file_count = 0

                    for item in items:
                        try:
                            if item.get('folder'):
                                # It's a folder
                                folder_data = self._folder_to_dict(item, library_id, site_id, folder_path)
                                if folder_data:
                                    folders_batch.append(folder_data)
                                    folder_count += 1

                                    # Add subfolder to queue only if it has children
                                    child_count = item.get('folder', {}).get('childCount', 0)
                                    if child_count > 0 and depth < max_depth:
                                        new_path = f"{folder_path}/{item['name']}" if folder_path != "/" else f"/{item['name']}"
                                        await folders_to_process.put({
                                            'parent_id': folder_data['folder_id'],
                                            'path': new_path,
                                            'item_id': item['id'],
                                            'depth': depth + 1
                                        })

                            elif item.get('file'):
                                # It's a file
                                file_data = self._file_to_dict(item, library_id, site_id, folder_path)
                                if file_data:
                                    files_batch.append(file_data)
                                    file_count += 1
                        except Exception as e:
                            logger.error(f"[QUEUE] Error processing item: {e}")
                            continue

                    if folder_count > 0 or file_count > 0:
                        logger.info(f"Discovered {folder_count} folders, {file_count} files in {folder_path}")

                    # Update counts
                    self.discovered_counts['folders'] += folder_count
                    self.discovered_counts['files'] += file_count

                except Exception as e:
                    logger.error(f"[QUEUE] Error processing folder {folder_path}: {e}")
                finally:
                    if acquired:
                        self.operation_semaphore.release()

                # Save in batches to prevent memory issues and database locks
                if len(folders_batch) >= batch_size:
                    await self._save_items_batch('folders', folders_batch[:batch_size])
                    folders_batch = folders_batch[batch_size:]

                if len(files_batch) >= batch_size:
                    await self._save_items_batch('files', files_batch[:batch_size])
                    files_batch = files_batch[batch_size:]

                processed_count += 1

                # Progress update every 10 folders
                if processed_count % 10 == 0:
                    queue_size = folders_to_process.qsize()
                    logger.info(f"[QUEUE] Processed {processed_count} folders, {queue_size} remaining in queue, "
                               f"Total: {self.discovered_counts['folders']} folders, {self.discovered_counts['files']} files")

            except asyncio.TimeoutError:
                logger.debug("[QUEUE] Queue timeout, no more folders to process")
                break
            except asyncio.CancelledError:
                logger.warning("[QUEUE] Discovery cancelled")
                raise
            except Exception as e:
                logger.error(f"[QUEUE] Unexpected error: {e}", exc_info=True)
                continue

        # Save remaining items
        if folders_batch:
            await self._save_items_batch('folders', folders_batch)
        if files_batch:
            await self._save_items_batch('files', files_batch)

        logger.info(f"[QUEUE] Completed discovery for library {library_name}: "
                   f"{self.discovered_counts['folders']} folders, {self.discovered_counts['files']} files")

    async def _save_items_batch(self, item_type: str, items: List[Dict[str, Any]]) -> None:
        """Save a batch of items with proper error handling."""
        if not items:
            return

        try:
            # Use bulk_upsert with unique constraint handling
            saved = await self.db_repo.bulk_upsert(
                item_type,
                items,
                unique_columns=[f'{item_type[:-1]}_id'],  # 'folders' -> 'folder_id'
                batch_size=100
            )
            logger.info(f"Saved {saved} {item_type} (Total: {self.discovered_counts.get(item_type, 0)})")
        except Exception as e:
            logger.error(f"Error saving {item_type} batch: {e}")
            # Try individual saves as fallback
            for item in items:
                try:
                    await self.db_repo.bulk_upsert(
                        item_type,
                        [item],
                        unique_columns=[f'{item_type[:-1]}_id']
                    )
                except Exception as e2:
                    logger.debug(f"Failed to save {item_type[:-1]} {item.get(f'{item_type[:-1]}_id')}: {e2}")

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

    def _folder_to_dict(self, item: Dict[str, Any], library_id: str, site_id: str, parent_path: str) -> Optional[Dict[str, Any]]:
        """Convert a folder item from Graph API to database format."""
        try:
            # Get parent folder ID from the item if available
            parent_folder_id = None
            if parent_path and parent_path != "/":
                # Parent folder ID would need to be tracked separately
                # For now, we'll leave it as None
                pass

            return {
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
        except Exception as e:
            logger.error(f"Error converting folder to dict: {e}")
            return None

    def _file_to_dict(self, item: Dict[str, Any], library_id: str, site_id: str, folder_path: str) -> Optional[Dict[str, Any]]:
        """Convert a file item from Graph API to database format."""
        try:
            # Extract folder_id from parent reference if available
            folder_id = None
            parent_ref = item.get("parentReference", {})
            if parent_ref and "id" in parent_ref:
                folder_id = parent_ref["id"]

            return {
                "file_id": item.get("id"),
                "folder_id": folder_id,
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
        except Exception as e:
            logger.error(f"Error converting file to dict: {e}")
            return None
