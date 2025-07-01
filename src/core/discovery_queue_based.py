"""Queue-based discovery implementation to prevent hanging."""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class QueueBasedDiscovery:
    """Mixin for queue-based folder/file discovery."""

    async def _discover_library_contents_queue(self, site: Dict[str, Any], library: Dict[str, Any]) -> None:
        """Discover folders and files in a library using queue-based approach."""
        library_id = library['library_id']
        library_name = library.get('name', 'Unknown')
        site_id = site['site_id']
        drive_id = library.get('drive_id', library_id)

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

        while not folders_to_process.empty():
            try:
                folder_info = await folders_to_process.get()
                parent_id = folder_info['parent_id']
                folder_path = folder_info['path']
                folder_item_id = folder_info['item_id']
                depth = folder_info['depth']

                if depth > max_depth:
                    logger.warning(f"Max depth {max_depth} reached at {folder_path}, skipping deeper folders")
                    continue

                # Acquire semaphore only for the API call
                async with self.operation_semaphore:
                    try:
                        # Construct the URL
                        if folder_item_id == 'root':
                            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root/children?$top=200"
                        else:
                            url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/items/{folder_item_id}/children?$top=200"

                        # Fetch items
                        start_time = time.time()
                        items = []

                        while url:
                            response = await self.graph_client.get_with_retry(url)
                            if isinstance(response, dict) and 'value' in response:
                                items.extend(response['value'])
                                url = response.get('@odata.nextLink')
                            else:
                                break

                        fetch_time = time.time() - start_time
                        logger.debug(f"[QUEUE] Fetched {len(items)} items from {folder_path} in {fetch_time:.2f}s")

                        # Process items
                        folder_count = 0
                        file_count = 0

                        for item in items:
                            if item.get('folder'):
                                # It's a folder
                                folder_data = self._folder_to_dict(item, library_id, site_id, folder_path)
                                if folder_data:
                                    folders_batch.append(folder_data)
                                    folder_count += 1

                                    # Add subfolder to queue
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

                        if folder_count > 0 or file_count > 0:
                            logger.info(f"Discovered {folder_count} folders, {file_count} files in {folder_path}")

                    except Exception as e:
                        logger.error(f"Error fetching contents of {folder_path}: {e}")
                        continue

                # Save in batches to prevent memory issues
                if len(folders_batch) >= 100:
                    await self._save_folders_batch(folders_batch[:100])
                    folders_batch = folders_batch[100:]

                if len(files_batch) >= 100:
                    await self._save_files_batch(files_batch[:100])
                    files_batch = files_batch[100:]

                processed_count += 1

                # Progress update every 50 folders
                if processed_count % 50 == 0:
                    queue_size = folders_to_process.qsize()
                    logger.info(f"[QUEUE] Processed {processed_count} folders, {queue_size} remaining in queue")

            except asyncio.CancelledError:
                logger.warning("[QUEUE] Discovery cancelled")
                raise
            except Exception as e:
                logger.error(f"[QUEUE] Error processing folder from queue: {e}", exc_info=True)
                continue

        # Save remaining items
        if folders_batch:
            await self._save_folders_batch(folders_batch)
        if files_batch:
            await self._save_files_batch(files_batch)

        logger.info(f"[QUEUE] Completed discovery for library {library_name}")

    async def _save_folders_batch(self, folders: List[Dict[str, Any]]) -> None:
        """Save a batch of folders with proper error handling."""
        if not folders:
            return

        try:
            # Update discovered count
            self.discovered_counts['folders'] += len(folders)

            # Save to database using INSERT OR IGNORE to handle duplicates
            saved = await self.db_repo.bulk_upsert(
                'folders',
                folders,
                unique_columns=['folder_id']
            )
            logger.info(f"Saved {saved}/{len(folders)} folders (Total: {self.discovered_counts['folders']})")
        except Exception as e:
            logger.error(f"Error saving folders batch: {e}")
            # Try individual saves
            for folder in folders:
                try:
                    await self.db_repo.bulk_upsert(
                        'folders',
                        [folder],
                        unique_columns=['folder_id']
                    )
                except Exception as e2:
                    logger.debug(f"Failed to save folder {folder.get('folder_id')}: {e2}")

    async def _save_files_batch(self, files: List[Dict[str, Any]]) -> None:
        """Save a batch of files with proper error handling."""
        if not files:
            return

        try:
            # Update discovered count
            self.discovered_counts['files'] += len(files)

            # Save to database using INSERT OR IGNORE to handle duplicates
            saved = await self.db_repo.bulk_upsert(
                'files',
                files,
                unique_columns=['file_id']
            )
            logger.info(f"Saved {saved}/{len(files)} files (Total: {self.discovered_counts['files']})")
        except Exception as e:
            logger.error(f"Error saving files batch: {e}")
            # Try individual saves
            for file in files:
                try:
                    await self.db_repo.bulk_upsert(
                        'files',
                        [file],
                        unique_columns=['file_id']
                    )
                except Exception as e2:
                    logger.debug(f"Failed to save file {file.get('file_id')}: {e2}")

    def _folder_to_dict(self, folder: Dict[str, Any], library_id: str, site_id: str, parent_path: str) -> Optional[Dict[str, Any]]:
        """Convert API folder response to database format."""
        try:
            folder_path = f"{parent_path}/{folder['name']}" if parent_path != "/" else f"/{folder['name']}"

            return {
                'folder_id': folder['id'],
                'library_id': library_id,
                'site_id': site_id,
                'name': folder['name'],
                'server_relative_url': folder.get('webUrl', ''),
                'created_at': folder.get('createdDateTime', datetime.now(timezone.utc).isoformat()),
                'created_by': folder.get('createdBy', {}).get('user', {}).get('email', 'Unknown'),
                'modified_at': folder.get('lastModifiedDateTime', datetime.now(timezone.utc).isoformat()),
                'modified_by': folder.get('lastModifiedBy', {}).get('user', {}).get('email', 'Unknown'),
                'item_count': folder.get('folder', {}).get('childCount', 0),
                'is_root': parent_path == "/",
                'has_unique_permissions': folder.get('hasUniquePermissions', False),
                'path': folder_path
            }
        except Exception as e:
            logger.error(f"Error converting folder to dict: {e}")
            return None

    def _file_to_dict(self, file: Dict[str, Any], library_id: str, site_id: str, folder_path: str) -> Optional[Dict[str, Any]]:
        """Convert API file response to database format."""
        try:
            return {
                'file_id': file['id'],
                'library_id': library_id,
                'site_id': site_id,
                'name': file['name'],
                'server_relative_url': file.get('webUrl', ''),
                'size_bytes': file.get('size', 0),
                'created_at': file.get('createdDateTime', datetime.now(timezone.utc).isoformat()),
                'created_by': file.get('createdBy', {}).get('user', {}).get('email', 'Unknown'),
                'modified_at': file.get('lastModifiedDateTime', datetime.now(timezone.utc).isoformat()),
                'modified_by': file.get('lastModifiedBy', {}).get('user', {}).get('email', 'Unknown'),
                'version': file.get('file', {}).get('version', '1.0'),
                'content_type': file.get('file', {}).get('mimeType', 'Unknown'),
                'has_unique_permissions': file.get('hasUniquePermissions', False),
                'is_checked_out': file.get('isCheckedOut', False),
                'checked_out_by': file.get('checkedOutBy', {}).get('user', {}).get('email') if file.get('isCheckedOut') else None,
                'folder_path': folder_path
            }
        except Exception as e:
            logger.error(f"Error converting file to dict: {e}")
            return None
