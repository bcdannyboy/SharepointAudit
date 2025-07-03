"""Discovery module for SharePoint audit system."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, timezone
from dataclasses import dataclass
from types import SimpleNamespace

from api.graph_client import GraphAPIClient
from api.sharepoint_client import SharePointAPIClient
from database.repository import DatabaseRepository
from cache.cache_manager import CacheManager
from core.progress_tracker import ProgressTracker
from utils.checkpoint_manager import CheckpointManager
from core.concurrency import ConcurrencyManager
from core.discovery_queue_based import QueueBasedDiscovery

logger = logging.getLogger(__name__)


class DiscoveryModule(QueueBasedDiscovery):
    """Discovers and enumerates all SharePoint content."""

    def __init__(
        self,
        graph_client: GraphAPIClient,
        sp_client: SharePointAPIClient,
        db_repo: DatabaseRepository,
        cache: Optional[CacheManager] = None,
        checkpoints: Optional[CheckpointManager] = None,
        max_concurrent_operations: int = 50,
        active_only: bool = False,
    ):
        self.graph_client = graph_client
        self.sp_client = sp_client
        self.db_repo = db_repo
        self.cache = cache
        self.checkpoints = checkpoints or CheckpointManager(db_repo)
        self.progress_tracker = ProgressTracker()
        self.active_only = active_only

        # Concurrency control
        self.concurrency_manager = ConcurrencyManager(max_concurrent_operations)
        # Remove redundant semaphore to prevent deadlock
        # self.operation_semaphore = asyncio.Semaphore(max_concurrent_operations)

        # Discovery state
        self.discovered_counts = {"sites": 0, "libraries": 0, "folders": 0, "files": 0}
        self.processed_sites = 0
        self.sites_with_errors: Set[str] = set()
        self.site_limit = None  # Optional limit on number of sites to process

    async def run_discovery(
        self, run_id: str, sites_to_process: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Run the complete discovery process."""
        start_time = time.time()
        logger.info(f"Starting discovery run {run_id}")
        if sites_to_process:
            logger.info(f"Sites to process filter provided: {sites_to_process}")

        try:
            # Discover all sites
            sites = await self.discover_all_sites(run_id)

            if sites_to_process:
                # Filter to specific sites if requested using exact matching
                def _normalize(url: str) -> str:
                    """Normalize URL for comparison by removing protocol and trailing slashes."""
                    normalized = url.lower().rstrip("/")
                    # Remove protocol prefixes
                    if normalized.startswith("https://"):
                        normalized = normalized[8:]
                    elif normalized.startswith("http://"):
                        normalized = normalized[7:]
                    return normalized

                normalized_filters = {_normalize(u) for u in sites_to_process}

                filtered_sites = []
                for site in sites:
                    site_url = (
                        site.get("webUrl", "")
                        if isinstance(site, dict)
                        else getattr(site, "webUrl", "")
                    )
                    if _normalize(site_url) in normalized_filters:
                        filtered_sites.append(site)

                sites = filtered_sites
                logger.info(
                    f"Filtered to {len(sites)} sites matching: {sites_to_process}"
                )

            # Apply site limit if set
            if self.site_limit and len(sites) > self.site_limit:
                logger.info(f"Applying site limit: {self.site_limit} of {len(sites)} sites")
                sites = sites[:self.site_limit]

            self.discovered_counts["sites"] = len(sites)
            logger.info(f"Discovered {len(sites)} sites to process")

            # Process sites in batches to avoid overwhelming the system
            batch_size = 3  # Process 3 sites at a time to match max_concurrent_operations
            for i in range(0, len(sites), batch_size):
                batch = sites[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1} of {(len(sites) + batch_size - 1)//batch_size} ({len(batch)} sites)")

                tasks = [self._discover_site_with_semaphore(run_id, site) for site in batch]
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)

                    # Log any exceptions
                    for j, result in enumerate(results):
                        if isinstance(result, Exception):
                            site = batch[j]
                            site_name = site.get("displayName", "Unknown") if isinstance(site, dict) else getattr(site, "displayName", "Unknown")
                            logger.error(f"Failed to process site {site_name}: {result}")

            # Final summary
            elapsed_time = time.time() - start_time
            logger.info(
                f"Discovery completed in {elapsed_time:.2f}s - "
                f"Sites: {self.discovered_counts['sites']}, "
                f"Libraries: {self.discovered_counts['libraries']}, "
                f"Folders: {self.discovered_counts['folders']}, "
                f"Files: {self.discovered_counts['files']}"
            )

            return {
                "status": "completed",
                "elapsed_time": elapsed_time,
                "discovered_counts": self.discovered_counts,
                "processed_sites": self.processed_sites,
                "sites_with_errors": list(self.sites_with_errors),
            }

        except Exception as e:
            logger.error(f"Discovery failed: {e}", exc_info=True)
            return {
                "status": "failed",
                "error": str(e),
                "discovered_counts": self.discovered_counts,
            }

    async def discover_all_sites(self, run_id: str) -> List[Dict[str, Any]]:
        """Discover all SharePoint sites in the tenant using Graph API delta queries."""
        # Log whether active-only filtering is enabled
        if self.active_only:
            logger.info("Starting site discovery with active-only filtering enabled")
        else:
            logger.info("Starting site discovery (all sites)")

        try:
            # Check for cached sites first
            cache_key = f"all_sites_active_{self.active_only}"
            if self.cache:
                cached_sites = await self.cache.get(cache_key)
                if cached_sites:
                    logger.info(f"Using cached sites: {len(cached_sites)} sites")
                    return cached_sites

            sites = []
            delta_token = None

            # Check for saved delta token
            if self.cache:
                delta_token = await self.cache.get("sites_delta_token")

            # Use the graph client's get_all_sites_delta method which includes active filtering
            data = await self._run_api_task(
                self.graph_client.get_all_sites_delta(delta_token, active_only=self.active_only)
            )

            # Process sites with pagination
            while data:
                # Process sites from current page
                for site_data in data.get("value", []):
                    if self._is_valid_site(site_data):
                        sites.append(site_data)

                # Check for next page
                next_url = data.get("@odata.nextLink")
                if next_url:
                    data = await self._run_api_task(self.graph_client.get_with_retry(next_url))
                else:
                    # Save delta token if provided
                    if "@odata.deltaLink" in data:
                        delta_link = data["@odata.deltaLink"]
                        if "token=" in delta_link:
                            new_delta_token = delta_link.split("token=")[-1]
                            if self.cache:
                                await self.cache.set(
                                    "sites_delta_token", new_delta_token, ttl=86400
                                )
                            logger.info("Saved new delta token for incremental sync")
                    break

            # Save sites to database
            if sites:
                await self._save_sites_to_database(sites)

            # Cache the results
            if self.cache:
                await self.cache.set(cache_key, sites, ttl=3600)

            logger.info(f"Discovered {len(sites)} sites")
            return sites

        except Exception as e:
            logger.error(f"Failed to discover sites: {e}", exc_info=True)
            raise

    async def _discover_site_with_semaphore(self, run_id: str, site: Any) -> None:
        """Discover a single site with semaphore control and timeout."""
        try:
            # Add timeout to prevent hanging (5 minutes per site)
            await asyncio.wait_for(
                self.concurrency_manager.run_api_task(
                    self._discover_single_site(run_id, site)
                ),
                timeout=300  # 5 minutes timeout
            )
        except asyncio.TimeoutError:
            site_name = site.get("displayName", "Unknown") if isinstance(site, dict) else getattr(site, "displayName", "Unknown")
            logger.error(f"Timeout while processing site {site_name}")
            site_id = site.get("id", "") if isinstance(site, dict) else getattr(site, "id", "")
            if site_id:
                self.sites_with_errors.add(site_id)

    async def _discover_single_site(self, run_id: str, site: Any) -> None:
        """Discover content for a single site."""
        # Handle both dict and object access
        if isinstance(site, dict):
            site_id = site.get("id", "")
            site_title = site.get("displayName", site.get("name", "Unknown"))
            site_url = site.get("webUrl", site.get("url", ""))
        else:
            site_id = getattr(site, "id", "")
            site_title = getattr(site, "displayName", getattr(site, "name", "Unknown"))
            site_url = getattr(site, "webUrl", getattr(site, "url", ""))

        # Check if this site was already processed
        checkpoint_key = f"site_{site_id}_status"
        status = await self.checkpoints.restore_checkpoint(run_id, checkpoint_key)
        if status == "completed":
            self.progress_tracker.skip(f"Site {site_title}", "Already processed")
            return

        self.progress_tracker.start(f"Site {site_title}")

        try:
            # Pass site_url to all discovery methods
            site_with_url = (
                {**site, "site_url": site_url} if isinstance(site, dict) else site
            )

            # Discover libraries, lists, and subsites in parallel
            tasks = [
                self._discover_libraries(site_with_url),
                self._discover_lists(site_with_url),
                self._discover_subsites(run_id, site_with_url),
            ]

            libraries, lists, subsites = await asyncio.gather(
                *tasks, return_exceptions=True
            )

            # Discover folders and files within libraries
            if isinstance(libraries, list):
                library_tasks = []
                for library in libraries:
                    if isinstance(library, dict):
                        # Add site_url to library
                        library["site_url"] = site_url
                        task = self._discover_library_contents(site_with_url, library)
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
            self.sites_with_errors.add(site_id)
            logger.info(f"[PROGRESS] Finished: Site {site_title} - Failed")
            self.progress_tracker.finish(f"Site {site_title}", f"Error: {str(e)}")

    async def _discover_libraries(self, site: Any) -> List[Dict[str, Any]]:
        """Enumerate document libraries for the given site."""
        # Handle both dict and object access
        if isinstance(site, dict):
            site_id = site.get("id")
            site_url = site.get("site_url", site.get("webUrl", site.get("url", "")))
        else:
            site_id = getattr(site, "id", None)
            site_url = getattr(
                site, "site_url", getattr(site, "webUrl", getattr(site, "url", ""))
            )

        if not site_id:
            return []

        try:
            cache_key = f"site_libraries:{site_id}"
            if self.cache:
                cached = await self.cache.get(cache_key)
                if cached is not None:
                    return cached

            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
            data = await self._run_api_task(self.graph_client.get_with_retry(url))
            libraries = data.get("value", [])

            # Save libraries to database with site_url
            if libraries:
                records = []
                for lib in libraries:
                    record = {
                        "library_id": lib.get("id"),
                        "site_id": site_id,
                        "site_url": site_url,
                        "name": lib.get("name", ""),
                        "description": lib.get("description"),
                        "created_at": lib.get("createdDateTime"),
                        "drive_id": lib.get("id"),
                    }
                    records.append(record)

                if records:
                    await self.db_repo.bulk_insert("libraries", records)
                    self.discovered_counts["libraries"] += len(records)
                    logger.info(
                        f"Discovered {len(records)} libraries in site (Total: {self.discovered_counts['libraries']})"
                    )

            if self.cache:
                await self.cache.set(cache_key, libraries, ttl=3600)

            return libraries

        except Exception as e:
            logger.error(f"Error discovering libraries for site {site_id}: {e}")
            return []

    async def _discover_library_contents(
        self, site: Dict[str, Any], library: Dict[str, Any]
    ) -> None:
        """Discover folders and files within a library using queue-based approach."""
        await self._discover_library_contents_queue(site, library)

    async def _discover_library_contents_queue(
        self, site: Dict[str, Any], library: Dict[str, Any]
    ) -> None:
        """Queue-based discovery implementation."""
        library_id = library.get("library_id", library.get("id"))
        library_name = library.get("name", "Unknown")
        site_id = site.get("site_id", site.get("id"))
        site_url = site.get("site_url", site.get("webUrl", site.get("url", "")))
        drive_id = library.get("drive_id", library.get("id"))

        logger.info(
            f"[QUEUE] Starting queue-based discovery for library {library_name}"
        )

        # Initialize queue with root folder
        folders_to_process = asyncio.Queue()
        await folders_to_process.put(
            {"parent_id": None, "path": "/", "item_id": "root", "depth": 0}
        )

        # Batch collections
        folders_batch = []
        files_batch = []
        processed_count = 0
        max_depth = 10  # Prevent infinite recursion

        while not folders_to_process.empty():
            try:
                folder_info = await folders_to_process.get()
                parent_id = folder_info["parent_id"]
                folder_path = folder_info["path"]
                folder_item_id = folder_info["item_id"]
                depth = folder_info["depth"]

                if depth > max_depth:
                    logger.warning(
                        f"Max depth {max_depth} reached at {folder_path}, skipping deeper folders"
                    )
                    continue

                # Acquire semaphore only for the API call
                try:
                    # Construct the URL - include sharepointIds in the response
                    if folder_item_id == "root":
                        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children?$top=200&$select=*,sharepointIds"
                    else:
                        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{folder_item_id}/children?$top=200&$select=*,sharepointIds"

                    # Fetch items
                    start_time = time.time()
                    items = []

                    while url:
                        data = await self._run_api_task(
                            self.graph_client.get_with_retry(url)
                        )

                        items.extend(data.get("value", []))
                        url = data.get("@odata.nextLink")

                    elapsed = time.time() - start_time
                    logger.debug(
                        f"[QUEUE] Fetched {len(items)} items from {folder_path} in {elapsed:.2f}s"
                    )

                except Exception as e:
                    logger.error(
                        f"[QUEUE] Error fetching items from {folder_path}: {e}"
                    )
                    continue

                # Process items outside the semaphore
                for item in items:
                    if "folder" in item:  # It's a folder
                        folder_record = self._folder_to_dict(
                            item, library_id, site_id, site_url, folder_path
                        )
                        if folder_record:
                            folders_batch.append(folder_record)

                            # Add to queue for processing
                            child_path = (
                                f"{folder_path}/{item['name']}"
                                if folder_path != "/"
                                else f"/{item['name']}"
                            )
                            await folders_to_process.put(
                                {
                                    "parent_id": folder_record["folder_id"],
                                    "path": child_path,
                                    "item_id": item["id"],
                                    "depth": depth + 1,
                                }
                            )
                    else:  # It's a file
                        file_record = self._file_to_dict(
                            item, library_id, site_id, site_url, folder_path
                        )
                        if file_record:
                            files_batch.append(file_record)

                processed_count += 1

                # Save batches periodically
                if len(folders_batch) >= 100:
                    await self._save_folders_batch(folders_batch)
                    folders_batch = []

                if len(files_batch) >= 100:
                    await self._save_files_batch(files_batch)
                    files_batch = []

                # Log progress
                if processed_count % 10 == 0:
                    logger.debug(
                        f"[QUEUE] Processed {processed_count} folders, queue size: {folders_to_process.qsize()}"
                    )

            except Exception as e:
                logger.error(f"[QUEUE] Error in discovery loop: {e}")
                continue

        # Save any remaining items
        if folders_batch:
            await self._save_folders_batch(folders_batch)
        if files_batch:
            await self._save_files_batch(files_batch)

        logger.info(
            f"[QUEUE] Completed discovery for library {library_name}: "
            f"{self.discovered_counts['folders']} folders, {self.discovered_counts['files']} files"
        )

    def _folder_to_dict(
        self,
        folder: Dict[str, Any],
        library_id: str,
        site_id: str,
        site_url: str,
        parent_path: str,
    ) -> Optional[Dict[str, Any]]:
        """Convert API folder response to database format."""
        try:
            folder_path = (
                f"{parent_path}/{folder['name']}"
                if parent_path != "/"
                else f"/{folder['name']}"
            )

            # Extract SharePoint list item ID if available
            sharepoint_item_id = None
            if "sharepointIds" in folder and folder["sharepointIds"]:
                list_item_id = folder["sharepointIds"].get("listItemId")
                if list_item_id:
                    try:
                        sharepoint_item_id = int(list_item_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid SharePoint list item ID for folder {folder['name']}: {list_item_id}")

            return {
                "folder_id": folder["id"],
                "sharepoint_item_id": sharepoint_item_id,
                "library_id": library_id,
                "site_id": site_id,
                "site_url": site_url,
                "name": folder["name"],
                "server_relative_url": folder.get("webUrl", ""),
                "created_at": folder.get(
                    "createdDateTime", datetime.now(timezone.utc).isoformat()
                ),
                "created_by": folder.get("createdBy", {})
                .get("user", {})
                .get("email", "Unknown"),
                "modified_at": folder.get(
                    "lastModifiedDateTime", datetime.now(timezone.utc).isoformat()
                ),
                "modified_by": folder.get("lastModifiedBy", {})
                .get("user", {})
                .get("email", "Unknown"),
                "item_count": folder.get("folder", {}).get("childCount", 0),
                "is_root": parent_path == "/",
                "has_unique_permissions": folder.get("hasUniquePermissions", False),
                "path": folder_path,
            }
        except Exception as e:
            logger.error(f"Error converting folder to dict: {e}")
            return None

    def _file_to_dict(
        self,
        file: Dict[str, Any],
        library_id: str,
        site_id: str,
        site_url: str,
        folder_path: str,
    ) -> Optional[Dict[str, Any]]:
        """Convert API file response to database format."""
        try:
            # Extract SharePoint list item ID if available
            sharepoint_item_id = None
            if "sharepointIds" in file and file["sharepointIds"]:
                list_item_id = file["sharepointIds"].get("listItemId")
                if list_item_id:
                    try:
                        sharepoint_item_id = int(list_item_id)
                    except (ValueError, TypeError):
                        logger.warning(f"Invalid SharePoint list item ID for file {file['name']}: {list_item_id}")

            return {
                "file_id": file["id"],
                "sharepoint_item_id": sharepoint_item_id,
                "library_id": library_id,
                "site_id": site_id,
                "site_url": site_url,
                "name": file["name"],
                "server_relative_url": file.get("webUrl", ""),
                "size_bytes": file.get("size", 0),
                "created_at": file.get(
                    "createdDateTime", datetime.now(timezone.utc).isoformat()
                ),
                "created_by": file.get("createdBy", {})
                .get("user", {})
                .get("email", "Unknown"),
                "modified_at": file.get(
                    "lastModifiedDateTime", datetime.now(timezone.utc).isoformat()
                ),
                "modified_by": file.get("lastModifiedBy", {})
                .get("user", {})
                .get("email", "Unknown"),
                "version": file.get("file", {}).get("version", "1.0"),
                "content_type": file.get("file", {}).get("mimeType", "Unknown"),
                "has_unique_permissions": file.get("hasUniquePermissions", False),
                "is_checked_out": file.get("isCheckedOut", False),
                "checked_out_by": (
                    file.get("checkedOutBy", {}).get("user", {}).get("email")
                    if file.get("isCheckedOut")
                    else None
                ),
                "folder_path": folder_path,
            }
        except Exception as e:
            logger.error(f"Error converting file to dict: {e}")
            return None

    async def _save_folders_batch(self, folders: List[Dict[str, Any]]) -> None:
        """Save a batch of folders to the database."""
        if not folders:
            return

        try:
            await self.db_repo.bulk_insert("folders", folders)
            self.discovered_counts["folders"] += len(folders)
            logger.debug(
                f"Saved {len(folders)} folders (Total: {self.discovered_counts['folders']})"
            )
        except Exception as e:
            logger.error(f"Error saving folders batch: {e}")

    async def _save_files_batch(self, files: List[Dict[str, Any]]) -> None:
        """Save a batch of files to the database."""
        if not files:
            return

        try:
            await self.db_repo.bulk_insert("files", files)
            self.discovered_counts["files"] += len(files)
            logger.debug(
                f"Saved {len(files)} files (Total: {self.discovered_counts['files']})"
            )
        except Exception as e:
            logger.error(f"Error saving files batch: {e}")

    async def _discover_lists(self, site: Any) -> List[Dict[str, Any]]:
        """Enumerate all lists (including non-library lists) for the given site."""
        # Handle both dict and object access
        if isinstance(site, dict):
            site_id = site.get("id")
            site_url = site.get("site_url", site.get("webUrl", site.get("url", "")))
        else:
            site_id = getattr(site, "id", None)
            site_url = getattr(
                site, "site_url", getattr(site, "webUrl", getattr(site, "url", ""))
            )

        if not site_id:
            return []

        try:
            cache_key = f"site_lists:{site_id}"
            if self.cache:
                cached = await self.cache.get(cache_key)
                if cached is not None:
                    return cached

            # Get all lists for the site (not just document libraries)
            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$expand=columns&$filter=list/hidden eq false"
            data = await self._run_api_task(self.graph_client.get_with_retry(url))
            lists = data.get("value", [])

            # Filter out system lists and document libraries (which are discovered separately)
            non_library_lists = []
            for lst in lists:
                # Skip document libraries (they have an associated drive)
                if lst.get("list", {}).get("template") == "documentLibrary":
                    continue
                # Skip hidden or system lists
                if lst.get("list", {}).get("hidden", False):
                    continue
                non_library_lists.append(lst)

            # Save lists to database
            if non_library_lists:
                records = []
                for lst in non_library_lists:
                    record = {
                        "list_id": lst.get("id"),
                        "site_id": site_id,
                        "site_url": site_url,
                        "name": lst.get("displayName", lst.get("name", "")),
                        "description": lst.get("description"),
                        "created_at": lst.get("createdDateTime"),
                        "template": lst.get("list", {}).get("template"),
                        "item_count": lst.get("list", {}).get("itemCount", 0),
                    }
                    records.append(record)

                if records:
                    await self.db_repo.bulk_insert("lists", records)
                    self.discovered_counts["lists"] = self.discovered_counts.get("lists", 0) + len(records)
                    logger.info(
                        f"Discovered {len(records)} lists in site (Total: {self.discovered_counts.get('lists', 0)})"
                    )

            if self.cache:
                await self.cache.set(cache_key, non_library_lists, ttl=3600)

            return non_library_lists

        except Exception as e:
            logger.error(f"Error discovering lists for site {site_id}: {e}")
            return []

    async def _discover_subsites(self, run_id: str, site: Any) -> List[Dict[str, Any]]:
        """Discover subsites for the given site."""
        # Handle both dict and object access
        if isinstance(site, dict):
            site_id = site.get("id")
            site_url = site.get("site_url", site.get("webUrl", site.get("url", "")))
        else:
            site_id = getattr(site, "id", None)
            site_url = getattr(
                site, "site_url", getattr(site, "webUrl", getattr(site, "url", ""))
            )

        if not site_id or not site_url:
            return []

        try:
            # Note: Graph API doesn't have a direct endpoint for subsites
            # We need to use SharePoint REST API or search for sites with parent relationship
            # For now, we'll use search to find sites that are children of this site

            # Extract the site collection URL
            import urllib.parse
            parsed_url = urllib.parse.urlparse(site_url)
            site_collection_path = parsed_url.path

            # Search for subsites by URL pattern
            search_query = f"path:{site_url} AND contentClass:STS_Site"
            url = f"https://graph.microsoft.com/v1.0/search/query"

            search_body = {
                "requests": [{
                    "entityTypes": ["site"],
                    "query": {
                        "queryString": search_query
                    },
                    "from": 0,
                    "size": 100
                }]
            }

            # For now, return empty list as subsite discovery is complex
            # and requires additional SharePoint REST API calls
            logger.debug(f"Subsite discovery not fully implemented for site {site_id}")
            return []

        except Exception as e:
            logger.error(f"Error discovering subsites for site {site_id}: {e}")
            return []

    def _is_valid_site(self, site_data: Dict[str, Any]) -> bool:
        """Check if a site should be included in discovery."""
        # Filter out certain system sites or based on other criteria
        site_url = site_data.get("webUrl", "")
        if any(skip in site_url for skip in ["/personal/", "-my.sharepoint.com"]):
            return False
        return True

    async def _save_sites_to_database(self, sites: List[Dict[str, Any]]) -> None:
        """Save discovered sites to the database."""
        records = []
        for site in sites:
            record = {
                "site_id": site.get("id"),
                "url": site.get("webUrl", ""),
                "title": site.get("displayName", site.get("name", "")),
                "description": site.get("description"),
                "created_at": site.get("createdDateTime"),
            }
            records.append(record)

        if records:
            await self.db_repo.bulk_upsert("sites", records, ["site_id"])
            logger.info(f"Saved {len(records)} sites to database")

    async def _run_api_task(self, coro):
        """Run an API task with the concurrency manager."""
        return await self.concurrency_manager.run_api_task(coro)
