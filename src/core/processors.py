"""Data processing stages and transformers for the audit pipeline."""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..core.discovery import DiscoveryModule
from ..database.repository import DatabaseRepository
from .pipeline import PipelineContext, PipelineStage

logger = logging.getLogger(__name__)


class DiscoveryStage(PipelineStage):
    """Pipeline stage for discovering raw data from APIs."""

    def __init__(self, discovery_module: DiscoveryModule):
        super().__init__("discovery")
        self.discovery_module = discovery_module

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Run the discovery process."""
        self.logger.info("Starting discovery stage")

        try:
            # Get sites to process from context if provided
            sites_to_process = getattr(context, 'sites_to_process', None)
            if sites_to_process:
                self.logger.info(f"Filtering to specific sites: {sites_to_process}")

            # Run discovery
            await self.discovery_module.run_discovery(context.run_id, sites_to_process)

            # Fetch discovered data from database for pipeline processing
            if context.db_repository:
                # Get all discovered sites (filtered if specific sites were requested)
                sites = await self._fetch_sites(context.db_repository, sites_to_process)
                context.sites = sites
                context.raw_data.extend(sites)

                # Get discovered libraries (filtered by site if specific sites were requested)
                libraries = await self._fetch_libraries(context.db_repository, sites)
                context.libraries = libraries

                # Get discovered files (filtered by site if specific sites were requested)
                files = await self._fetch_files(context.db_repository, sites)
                context.files = files

                # Get discovered folders (filtered by site if specific sites were requested)
                folders = await self._fetch_folders(context.db_repository, sites)
                context.folders = folders

                # Update total item count
                context.total_items = len(sites) + len(libraries) + len(files) + len(folders)

                # Record metrics
                if context.metrics:
                    context.metrics.record_stage_items("discovery", context.total_items)

            self.logger.info(
                f"Discovery completed. Found {len(context.sites)} sites, "
                f"{len(context.libraries)} libraries, {len(context.folders)} folders, "
                f"{len(context.files)} files. Total: {context.total_items} items"
            )

        except Exception as e:
            self.logger.error(f"Discovery stage failed: {str(e)}")
            if context.metrics:
                context.metrics.record_stage_error("discovery")
            raise

        return context

    async def _fetch_sites(self, db_repo: DatabaseRepository, sites_to_process: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Fetch discovered sites from the database, optionally filtered by URL."""
        if sites_to_process:
            # Normalize URLs for comparison
            normalized_urls = [url.rstrip("/").lower() for url in sites_to_process]
            # Use parameterized query with placeholders
            placeholders = ",".join(["?" for _ in normalized_urls])
            query = f"SELECT * FROM sites WHERE LOWER(TRIM(url, '/')) IN ({placeholders})"
            return await db_repo.fetch_all(query, tuple(normalized_urls))
        else:
            query = "SELECT * FROM sites"
            return await db_repo.fetch_all(query)

    async def _fetch_libraries(self, db_repo: DatabaseRepository, sites: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch discovered libraries from the database for specific sites."""
        if not sites:
            return []

        site_ids = [site['site_id'] for site in sites]
        placeholders = ",".join(["?" for _ in site_ids])
        query = f"SELECT * FROM libraries WHERE site_id IN ({placeholders})"
        return await db_repo.fetch_all(query, tuple(site_ids))

    async def _fetch_files(self, db_repo: DatabaseRepository, sites: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch discovered files from the database for specific sites."""
        if not sites:
            return []

        site_ids = [site['site_id'] for site in sites]
        placeholders = ",".join(["?" for _ in site_ids])
        query = f"SELECT * FROM files WHERE site_id IN ({placeholders})"
        return await db_repo.fetch_all(query, tuple(site_ids))

    async def _fetch_folders(self, db_repo: DatabaseRepository, sites: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch discovered folders from the database for specific sites."""
        if not sites:
            return []

        site_ids = [site['site_id'] for site in sites]
        placeholders = ",".join(["?" for _ in site_ids])
        query = f"SELECT * FROM folders WHERE site_id IN ({placeholders})"
        return await db_repo.fetch_all(query, tuple(site_ids))


class ValidationStage(PipelineStage):
    """Pipeline stage for validating discovered data."""

    def __init__(self):
        super().__init__("validation")
        self.validation_errors = []

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Validate the discovered data."""
        self.logger.info("Starting validation stage")
        self.validation_errors = []

        # Validate sites
        for site in context.sites:
            self._validate_site(site)

        # Validate files
        for file in context.files:
            self._validate_file(file)

        # Validate folders
        for folder in context.folders:
            self._validate_folder(folder)

        # Validate raw data
        for item in context.raw_data:
            self._validate_item(item)

        # Record metrics
        if context.metrics:
            context.metrics.record_stage_items(
                "validation",
                len(context.sites) + len(context.files) + len(context.folders) + len(context.raw_data)
            )

        if self.validation_errors:
            self.logger.warning(f"Found {len(self.validation_errors)} validation errors")
            context.errors.extend(self.validation_errors)
            if context.metrics:
                for _ in self.validation_errors:
                    context.metrics.record_stage_error("validation")
        else:
            self.logger.info("All data passed validation")

        return context

    def _validate_site(self, site: Dict[str, Any]) -> None:
        """Validate a site object."""
        required_fields = ["site_id", "url"]

        for field in required_fields:
            if field not in site or not site[field]:
                self.validation_errors.append(f"Site missing required field: {field}")

        # Validate URL format
        if "url" in site and site["url"]:
            url = site["url"]
            if not url.startswith("https://") or ".sharepoint.com" not in url:
                self.validation_errors.append(f"Invalid site URL: {url}")

    def _validate_file(self, file: Dict[str, Any]) -> None:
        """Validate a file object."""
        required_fields = ["file_id", "name", "server_relative_url"]

        for field in required_fields:
            if field not in file or not file[field]:
                self.validation_errors.append(f"File missing required field: {field}")

    def _validate_folder(self, folder: Dict[str, Any]) -> None:
        """Validate a folder object."""
        required_fields = ["folder_id", "name", "server_relative_url"]

        for field in required_fields:
            if field not in folder or not folder[field]:
                self.validation_errors.append(f"Folder missing required field: {field}")

    def _validate_item(self, item: Dict[str, Any]) -> None:
        """Validate a generic item."""
        if not isinstance(item, dict):
            self.validation_errors.append(f"Invalid item type: {type(item)}")
            return

        # Check for common required fields
        if "id" not in item and "site_id" not in item and "file_id" not in item:
            self.validation_errors.append("Item missing identifier field")


class TransformationStage(PipelineStage):
    """Pipeline stage for transforming raw data into structured records."""

    def __init__(self):
        super().__init__("transformation")
        self.date_patterns = [
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%d %H:%M:%S",
        ]

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Transform raw API data into structured database records."""
        self.logger.info("Starting transformation stage")

        items_transformed = 0

        # Process raw data
        for raw_item in context.raw_data:
            try:
                processed_item = self._transform_item(raw_item)
                if processed_item:
                    context.processed_data.append(processed_item)
                    items_transformed += 1
            except Exception as e:
                self.logger.error(f"Failed to transform item: {str(e)}")
                context.errors.append(f"Transform error: {str(e)}")
                if context.metrics:
                    context.metrics.record_stage_error("transformation")

        # Transform specific data types
        if context.sites:
            context.sites = [self._transform_site(site) for site in context.sites]
            items_transformed += len(context.sites)

        if context.files:
            context.files = [self._transform_file(file) for file in context.files]
            items_transformed += len(context.files)

        if context.folders:
            context.folders = [self._transform_folder(folder) for folder in context.folders]
            items_transformed += len(context.folders)

        # Record metrics
        if context.metrics:
            context.metrics.record_stage_items("transformation", items_transformed)

        self.logger.info(f"Transformed {items_transformed} items")

        return context

    def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a generic item."""
        transformed = item.copy()

        # Normalize date fields
        date_fields = [
            "created_at", "modified_at", "createdDateTime", "lastModifiedDateTime",
            "granted_at", "added_at", "last_synced", "started_at", "completed_at"
        ]
        for field in date_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self._parse_date(transformed[field])

        # Normalize names (uppercase for consistency)
        if "name" in transformed and transformed["name"]:
            transformed["name_normalized"] = transformed["name"].upper()

        # Extract file extension if it's a file
        if "name" in transformed and transformed["name"] and "." in transformed["name"]:
            transformed["file_extension"] = Path(transformed["name"]).suffix.lower()

        return transformed

    def _transform_site(self, site: Dict[str, Any]) -> Dict[str, Any]:
        """Transform site-specific data."""
        transformed = self._transform_item(site)

        # Extract tenant name from URL
        if "url" in transformed and transformed["url"]:
            match = re.search(r"https://([^.]+)\.sharepoint\.com", transformed["url"])
            if match:
                transformed["tenant_name"] = match.group(1)

        # Normalize site type
        if "template" in transformed:
            transformed["site_type"] = self._normalize_site_type(transformed["template"])

        return transformed

    def _transform_file(self, file: Dict[str, Any]) -> Dict[str, Any]:
        """Transform file-specific data."""
        transformed = self._transform_item(file)

        # Ensure size_bytes is an integer
        if "size_bytes" in transformed and transformed["size_bytes"]:
            try:
                transformed["size_bytes"] = int(transformed["size_bytes"])
            except (ValueError, TypeError):
                transformed["size_bytes"] = 0

        return transformed

    def _transform_folder(self, folder: Dict[str, Any]) -> Dict[str, Any]:
        """Transform folder-specific data."""
        transformed = self._transform_item(folder)

        # Ensure item_count is an integer
        if "item_count" in transformed and transformed["item_count"]:
            try:
                transformed["item_count"] = int(transformed["item_count"])
            except (ValueError, TypeError):
                transformed["item_count"] = 0

        return transformed

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats."""
        if isinstance(date_str, datetime):
            return date_str

        if not date_str:
            return None

        # Handle ISO format with timezone
        if isinstance(date_str, str):
            # Remove timezone info for parsing
            date_str = date_str.replace("+00:00", "Z")

        for pattern in self.date_patterns:
            try:
                return datetime.strptime(date_str, pattern)
            except ValueError:
                continue

        self.logger.warning(f"Could not parse date: {date_str}")
        return None

    def _normalize_site_type(self, template: str) -> str:
        """Normalize site template to a standard type."""
        template_map = {
            "STS#0": "Team Site",
            "STS#3": "Team Site (Modern)",
            "GROUP#0": "Microsoft 365 Group Site",
            "SITEPAGEPUBLISHING#0": "Communication Site",
        }
        return template_map.get(template, "Other")


class EnrichmentStage(PipelineStage):
    """Pipeline stage for enriching data with calculated fields."""

    def __init__(self):
        super().__init__("enrichment")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Enrich data with calculated and derived fields."""
        self.logger.info("Starting enrichment stage")

        items_enriched = 0

        # Enrich processed data
        for item in context.processed_data:
            self._enrich_item(item)
            items_enriched += 1

        # Enrich files
        for file in context.files:
            self._enrich_file(file)
            items_enriched += 1

        # Enrich folders
        for folder in context.folders:
            self._enrich_folder(folder)
            items_enriched += 1

        # Calculate aggregated metrics
        if context.files:
            self._calculate_storage_metrics(context)

        # Record metrics
        if context.metrics:
            context.metrics.record_stage_items("enrichment", items_enriched)

        self.logger.info(f"Data enrichment completed for {items_enriched} items")

        return context

    def _enrich_item(self, item: Dict[str, Any]) -> None:
        """Add calculated fields to an item."""
        # Calculate age if created date exists
        if "created_at" in item and isinstance(item["created_at"], datetime):
            # Ensure both datetimes are timezone-aware for comparison
            created_at = item["created_at"]
            if created_at.tzinfo is None:
                # If created_at is naive, assume it's UTC
                created_at = created_at.replace(tzinfo=timezone.utc)

            age_days = (datetime.now(timezone.utc) - created_at).days
            item["age_days"] = age_days
            item["age_category"] = self._categorize_age(age_days)

        # Calculate path depth
        if "server_relative_url" in item and item["server_relative_url"]:
            item["path_depth"] = self._calculate_path_depth(item["server_relative_url"])

        # Mark external visibility
        if "principal_name" in item and item["principal_name"]:
            item["is_external"] = self._is_external_user(item["principal_name"])

    def _enrich_file(self, file: Dict[str, Any]) -> None:
        """Add file-specific enrichments."""
        self._enrich_item(file)

        # Categorize file size
        if "size_bytes" in file:
            file["size_category"] = self._categorize_size(file["size_bytes"])

        # Extract file type from extension
        if "name" in file and file["name"]:
            file["file_type"] = self._get_file_type(file["name"])

    def _enrich_folder(self, folder: Dict[str, Any]) -> None:
        """Add folder-specific enrichments."""
        self._enrich_item(folder)

        # Categorize by item count
        if "item_count" in folder:
            folder["size_category"] = self._categorize_folder_size(folder["item_count"])

    def _calculate_path_depth(self, path: str) -> int:
        """Calculate the depth of a path."""
        if not path:
            return 0

        # Remove leading/trailing slashes
        path = path.strip("/")

        # Split path and remove empty parts
        parts = [p for p in path.split("/") if p]

        # If the last part looks like a file name (has a dot), exclude it
        if parts and "." in parts[-1]:
            parts = parts[:-1]

        # Return the number of directory segments
        return len(parts)

    def _categorize_age(self, days: int) -> str:
        """Categorize item age."""
        if days < 30:
            return "Recent"
        elif days < 90:
            return "Current"
        elif days < 365:
            return "Aging"
        elif days < 730:
            return "Old"
        else:
            return "Archived"

    def _categorize_size(self, size_bytes: int) -> str:
        """Categorize file size."""
        if size_bytes == 0:
            return "Empty"

        mb = size_bytes / (1024 * 1024)

        if mb < 1:
            return "Tiny"
        elif mb < 10:
            return "Small"
        elif mb < 100:
            return "Medium"
        elif mb < 1024:
            return "Large"
        else:
            return "Huge"

    def _categorize_folder_size(self, item_count: int) -> str:
        """Categorize folder by item count."""
        if item_count == 0:
            return "Empty"
        elif item_count < 10:
            return "Small"
        elif item_count < 100:
            return "Medium"
        elif item_count < 1000:
            return "Large"
        else:
            return "Huge"

    def _get_file_type(self, filename: str) -> str:
        """Determine file type from filename."""
        if not filename:
            return "Unknown"

        ext = Path(filename).suffix.lower()

        file_types = {
            # Documents
            ".doc": "Document", ".docx": "Document", ".pdf": "Document",
            ".txt": "Document", ".rtf": "Document", ".odt": "Document",

            # Spreadsheets
            ".xls": "Spreadsheet", ".xlsx": "Spreadsheet", ".csv": "Spreadsheet",
            ".ods": "Spreadsheet",

            # Presentations
            ".ppt": "Presentation", ".pptx": "Presentation", ".odp": "Presentation",

            # Images
            ".jpg": "Image", ".jpeg": "Image", ".png": "Image", ".gif": "Image",
            ".bmp": "Image", ".svg": "Image", ".tiff": "Image",

            # Videos
            ".mp4": "Video", ".avi": "Video", ".mov": "Video", ".wmv": "Video",
            ".mkv": "Video", ".flv": "Video",

            # Archives
            ".zip": "Archive", ".rar": "Archive", ".7z": "Archive", ".tar": "Archive",
            ".gz": "Archive",

            # Code
            ".py": "Code", ".js": "Code", ".java": "Code", ".cpp": "Code",
            ".cs": "Code", ".php": "Code", ".rb": "Code", ".go": "Code",
        }

        return file_types.get(ext, "Other")

    def _is_external_user(self, principal_name: str) -> bool:
        """Check if a user is external."""
        external_indicators = [
            "#ext#", "_external", "@gmail.com", "@outlook.com",
            "@hotmail.com", "@yahoo.com"
        ]
        return any(indicator in principal_name.lower() for indicator in external_indicators)

    def _calculate_storage_metrics(self, context: PipelineContext) -> None:
        """Calculate storage-related metrics."""
        total_size = sum(f.get("size_bytes", 0) for f in context.files)
        file_count = len(context.files)

        if context.metrics:
            context.metrics.set_custom_metric("total_storage_bytes", total_size)
            context.metrics.set_custom_metric("total_storage_gb", total_size / (1024**3))
            context.metrics.set_custom_metric(
                "average_file_size_mb",
                (total_size / file_count / (1024**2)) if file_count > 0 else 0
            )
            context.metrics.set_custom_metric("total_files", file_count)


class StorageStage(PipelineStage):
    """Pipeline stage for saving processed data to the database."""

    def __init__(self, db_repo: DatabaseRepository):
        super().__init__("storage")
        self.db_repo = db_repo
        self.batch_size = 1000

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Save all processed data to the database."""
        self.logger.info("Starting storage stage")

        total_saved = 0

        try:
            # Note: We don't save sites, libraries, folders, and files here
            # because they were already saved during the discovery phase.
            # This stage is for saving any additional processed data or updates.

            # Save permissions if any were discovered
            if context.permissions:
                saved = await self._save_batch("permissions", context.permissions)
                total_saved += saved

            # If we have any updates to existing records, handle them here
            # For example, if enrichment added calculated fields that need to be saved

            # Update audit run statistics
            await self._update_audit_stats(context)

            # Record metrics
            if context.metrics:
                context.metrics.record_stage_items("storage", total_saved)

            self.logger.info(f"Storage stage completed. Saved {total_saved} new records")

        except Exception as e:
            self.logger.error(f"Storage stage failed: {str(e)}")
            if context.metrics:
                context.metrics.record_stage_error("storage")
            raise

        return context

    async def _save_batch(self, table_name: str, records: List[Dict[str, Any]]) -> int:
        """Save records in batches."""
        if not records:
            return 0

        total_saved = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            saved_count = await self.db_repo.bulk_insert(table_name, batch)
            total_saved += saved_count

            self.logger.debug(f"Saved batch {i//self.batch_size + 1} to {table_name}: {saved_count} records")

        self.logger.info(f"Saved {total_saved} records to {table_name}")
        return total_saved

    async def _update_audit_stats(self, context: PipelineContext) -> None:
        """Update audit run statistics in the database."""
        if not context.run_id:
            return

        # This would update the audit_runs table with final stats
        stats = {
            "run_id": context.run_id,
            "total_sites_processed": len(context.sites),
            "total_items_processed": context.total_items,
            "total_errors": len(context.errors),
            "status": "completed" if not context.errors else "completed_with_errors"
        }

        # Note: This would require implementing an update_audit_run method in the repository
        # For now, we'll just log the stats
        self.logger.info(f"Audit run {context.run_id} completed: {stats}")


class DataProcessor:
    """Processes and transforms audit data efficiently."""

    def __init__(self, db: DatabaseRepository):
        self.db = db
        self.batch_size = 1000
        self.processing_pool = ThreadPoolExecutor(max_workers=10)
        self.logger = logging.getLogger(__name__)

    async def process_audit_batch(self, items: List[Dict[str, Any]]) -> 'ProcessingResult':
        """Process a batch of audit items."""
        result = ProcessingResult()

        # Split items by type for optimized processing
        items_by_type = self._group_by_type(items)

        # Process each type in parallel
        futures = []
        for item_type, typed_items in items_by_type.items():
            if item_type == "File":
                futures.append(
                    self.processing_pool.submit(self._process_files, typed_items)
                )
            elif item_type == "Folder":
                futures.append(
                    self.processing_pool.submit(self._process_folders, typed_items)
                )
            elif item_type == "Permission":
                futures.append(
                    self.processing_pool.submit(self._process_permissions, typed_items)
                )

        # Wait for all processing to complete
        for future in as_completed(futures):
            try:
                partial_result = future.result()
                result.merge(partial_result)
            except Exception as e:
                result.add_error(str(e))

        # Save processed data
        await self._save_batch(result)

        return result

    def _group_by_type(self, items: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Group items by their type."""
        groups = {}
        for item in items:
            item_type = item.get("type", "Unknown")
            if item_type not in groups:
                groups[item_type] = []
            groups[item_type].append(item)
        return groups

    def _process_files(self, files: List[Dict[str, Any]]) -> 'ProcessingResult':
        """Process file audit data."""
        result = ProcessingResult()

        # Prepare bulk insert data
        file_records = []
        permission_records = []

        for file in files:
            # Transform file data
            file_record = {
                'file_id': file.get('id'),
                'name': file.get('name'),
                'server_relative_url': file.get('webUrl', ''),
                'size_bytes': file.get('size', 0),
                'created_at': file.get('createdDateTime'),
                'modified_at': file.get('lastModifiedDateTime'),
                'created_by': file.get('createdBy', {}).get('user', {}).get('email'),
                'modified_by': file.get('lastModifiedBy', {}).get('user', {}).get('email'),
                'version': file.get('version', '1.0'),
                'has_unique_permissions': file.get('hasUniquePermissions', False),
                'site_id': file.get('site_id'),
                'library_id': file.get('library_id'),
                'folder_id': file.get('folder_id')
            }
            file_records.append(file_record)

            # Process file permissions if unique
            if file.get('hasUniquePermissions') and file.get('permissions'):
                permission_records.extend(
                    self._transform_permissions(file['permissions'], file['id'], 'file')
                )

        result.file_count = len(file_records)
        result.permission_count = len(permission_records)
        result.file_records = file_records
        result.permission_records = permission_records

        return result

    def _process_folders(self, folders: List[Dict[str, Any]]) -> 'ProcessingResult':
        """Process folder audit data."""
        result = ProcessingResult()

        folder_records = []
        for folder in folders:
            folder_record = {
                'folder_id': folder.get('id'),
                'name': folder.get('name'),
                'server_relative_url': folder.get('webUrl', ''),
                'item_count': folder.get('childCount', 0),
                'has_unique_permissions': folder.get('hasUniquePermissions', False),
                'created_at': folder.get('createdDateTime'),
                'created_by': folder.get('createdBy', {}).get('user', {}).get('email'),
                'modified_at': folder.get('lastModifiedDateTime'),
                'modified_by': folder.get('lastModifiedBy', {}).get('user', {}).get('email'),
                'library_id': folder.get('library_id'),
                'parent_folder_id': folder.get('parent_folder_id')
            }
            folder_records.append(folder_record)

        result.folder_count = len(folder_records)
        result.folder_records = folder_records

        return result

    def _process_permissions(self, permissions: List[Dict[str, Any]]) -> 'ProcessingResult':
        """Process permission audit data."""
        result = ProcessingResult()

        permission_records = []
        for perm in permissions:
            permission_record = {
                'object_type': perm.get('object_type'),
                'object_id': perm.get('object_id'),
                'principal_type': perm.get('principal_type'),
                'principal_id': perm.get('principal_id'),
                'principal_name': perm.get('principal_name'),
                'permission_level': perm.get('permission_level'),
                'is_inherited': perm.get('is_inherited', True),
                'granted_at': perm.get('granted_at'),
                'granted_by': perm.get('granted_by')
            }
            permission_records.append(permission_record)

        result.permission_count = len(permission_records)
        result.permission_records = permission_records

        return result

    def _transform_permissions(
        self, permissions: List[Dict[str, Any]], object_id: str, object_type: str
    ) -> List[Dict[str, Any]]:
        """Transform permission data for storage."""
        transformed = []

        for perm in permissions:
            base_record = {
                'object_type': object_type,
                'object_id': object_id,
                'permission_level': perm.get('role', 'Unknown'),
                'is_inherited': False,  # These are unique permissions
                'granted_at': perm.get('grantedDateTime'),
                'granted_by': perm.get('grantedBy', {}).get('user', {}).get('email')
            }

            # Handle different grantee types
            grantee = perm.get('grantedTo', {})
            if 'user' in grantee:
                base_record['principal_type'] = 'user'
                base_record['principal_id'] = grantee['user'].get('id')
                base_record['principal_name'] = grantee['user'].get('email')
            elif 'group' in grantee:
                base_record['principal_type'] = 'group'
                base_record['principal_id'] = grantee['group'].get('id')
                base_record['principal_name'] = grantee['group'].get('displayName')
            elif 'application' in grantee:
                base_record['principal_type'] = 'app'
                base_record['principal_id'] = grantee['application'].get('id')
                base_record['principal_name'] = grantee['application'].get('displayName')

            transformed.append(base_record)

        return transformed

    async def _save_batch(self, result: 'ProcessingResult') -> None:
        """Save processed batch to database."""
        # Save files
        if result.file_records:
            await self.db.bulk_insert('files', result.file_records)

        # Save folders
        if result.folder_records:
            await self.db.bulk_insert('folders', result.folder_records)

        # Save permissions
        if result.permission_records:
            await self.db.bulk_insert('permissions', result.permission_records)


class ProcessingResult:
    """Container for processing results."""

    def __init__(self):
        self.file_count = 0
        self.folder_count = 0
        self.permission_count = 0
        self.file_records = []
        self.folder_records = []
        self.permission_records = []
        self.errors = []

    def merge(self, other: 'ProcessingResult') -> None:
        """Merge another result into this one."""
        self.file_count += other.file_count
        self.folder_count += other.folder_count
        self.permission_count += other.permission_count
        self.file_records.extend(other.file_records)
        self.folder_records.extend(other.folder_records)
        self.permission_records.extend(other.permission_records)
        self.errors.extend(other.errors)

    def add_error(self, error: str) -> None:
        """Add an error to the result."""
        self.errors.append(error)


class PermissionAnalysisStage(PipelineStage):
    """Pipeline stage for analyzing permissions of discovered items."""

    def __init__(self, permission_analyzer: Optional[Any] = None):
        super().__init__("permission_analysis")
        self.analyzer = permission_analyzer

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Analyze permissions for all discovered items."""
        self.logger.info("Starting permission analysis stage")

        if not self.analyzer:
            self.logger.warning("No permission analyzer configured, skipping permission analysis")
            return context

        start_time = time.time()
        total_analyzed = 0
        unique_permission_count = 0
        external_sharing_count = 0

        try:
            # Analyze permissions for all items, prioritizing those with unique permissions
            all_items = []

            # Add sites
            for site in context.sites:
                all_items.append((site, "site"))

            # Add libraries
            for library in context.libraries:
                all_items.append((library, "library"))

            # Add folders
            for folder in context.folders:
                all_items.append((folder, "folder"))

            # Add files
            for file in context.files:
                all_items.append((file, "file"))

            self.logger.info(f"Analyzing permissions for {len(all_items)} items")

            # Process in batches for better performance
            batch_size = 50
            for i in range(0, len(all_items), batch_size):
                batch = all_items[i:i + batch_size]
                # Analyze permissions for the batch
                for item, item_type in batch:
                    try:
                        # Analyze permissions
                        permission_set = await self.analyzer.analyze_item_permissions(
                            item,
                            item_type
                        )

                        total_analyzed += 1

                        # Track statistics
                        if permission_set.has_unique_permissions:
                            unique_permission_count += 1

                        if permission_set.external_users_count > 0 or permission_set.anonymous_links_count > 0:
                            external_sharing_count += 1

                        # Convert permission set to dictionary format for storage
                        for perm in permission_set.permissions:
                            permission_record = {
                                "object_type": permission_set.object_type,
                                "object_id": permission_set.object_id,
                                "principal_type": perm.principal_type.value,
                                "principal_id": perm.principal_id,
                                "principal_name": perm.principal_name,
                                "permission_level": perm.permission_level,
                                "is_inherited": perm.is_inherited,
                                "granted_at": perm.granted_at.isoformat() if perm.granted_at else None,
                                "granted_by": perm.granted_by,
                                "inheritance_source": perm.inheritance_source,
                                "is_external": perm.is_external,
                                "is_anonymous_link": perm.is_anonymous_link
                            }
                            context.permissions.append(permission_record)

                        # Record metrics
                        if context.metrics:
                            context.metrics.increment_items_processed()

                    except Exception as e:
                        self.logger.error(
                            f"Failed to analyze permissions for {item_type} {item.get('id', 'unknown')}: {str(e)}"
                        )
                        context.errors.append(f"Permission analysis failed for {item_type}: {str(e)}")
                        if context.metrics:
                            context.metrics.record_stage_error("permission_analysis")

                # Log progress
                if (i + batch_size) % 500 == 0:
                    self.logger.info(f"Analyzed {i + batch_size}/{len(all_items)} items")

            # Generate summary report
            if hasattr(self.analyzer, 'generate_permission_report'):
                # Get all permission sets for report
                report = await self.analyzer.generate_permission_report([])
                self.logger.info(f"Permission Analysis Summary: {report}")

            elapsed_time = time.time() - start_time
            self.logger.info(
                f"Permission analysis completed: "
                f"{total_analyzed} items analyzed, "
                f"{unique_permission_count} with unique permissions, "
                f"{external_sharing_count} with external sharing, "
                f"in {elapsed_time:.2f} seconds"
            )

            # Record final metrics
            if context.metrics:
                context.metrics.record_stage_completion("permission_analysis", elapsed_time)

        except Exception as e:
            self.logger.error(f"Permission analysis stage failed: {str(e)}")
            context.errors.append(f"Permission analysis stage error: {str(e)}")
            if context.metrics:
                context.metrics.record_stage_error("permission_analysis")
            raise

        return context
