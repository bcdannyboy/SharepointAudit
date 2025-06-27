"""Data processing stages and transformers for the audit pipeline."""

import asyncio
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
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
            # Run discovery
            await self.discovery_module.run_discovery(context.run_id)

            # Fetch discovered data from database for pipeline processing
            if context.db_repository:
                # Get all discovered sites
                sites = await self._fetch_sites(context.db_repository)
                context.sites = sites
                context.raw_data.extend(sites)

                # Get all discovered files for metrics
                file_count = await self._count_files(context.db_repository)
                context.total_items += file_count

            self.logger.info(f"Discovery completed. Found {len(context.sites)} sites, {context.total_items} total items")

        except Exception as e:
            self.logger.error(f"Discovery stage failed: {str(e)}")
            raise

        return context

    async def _fetch_sites(self, db_repo: DatabaseRepository) -> List[Dict[str, Any]]:
        """Fetch all discovered sites from the database."""
        # This is a simplified version - in real implementation would use proper queries
        # For now, return empty list to allow pipeline to run
        return []

    async def _count_files(self, db_repo: DatabaseRepository) -> int:
        """Count total discovered files."""
        # This is a simplified version - in real implementation would use proper queries
        # For now, return 0 to allow pipeline to run
        return 0


class ValidationStage(PipelineStage):
    """Pipeline stage for validating discovered data."""

    def __init__(self):
        super().__init__("validation")
        self.validation_errors = []

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Validate the discovered data."""
        self.logger.info("Starting validation stage")

        # Validate sites
        for site in context.sites:
            self._validate_site(site)

        # Validate raw data
        for item in context.raw_data:
            self._validate_item(item)

        if self.validation_errors:
            self.logger.warning(f"Found {len(self.validation_errors)} validation errors")
            context.errors.extend(self.validation_errors)
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
        if "url" in site:
            url = site["url"]
            if not url.startswith("https://") or ".sharepoint.com" not in url:
                self.validation_errors.append(f"Invalid site URL: {url}")

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

        # Process raw data
        for raw_item in context.raw_data:
            try:
                processed_item = self._transform_item(raw_item)
                if processed_item:
                    context.processed_data.append(processed_item)
            except Exception as e:
                self.logger.error(f"Failed to transform item: {str(e)}")
                context.errors.append(f"Transform error: {str(e)}")

        # Transform specific data types
        if context.sites:
            context.sites = [self._transform_site(site) for site in context.sites]

        self.logger.info(f"Transformed {len(context.processed_data)} items")

        return context

    def _transform_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a generic item."""
        transformed = item.copy()

        # Normalize date fields
        date_fields = ["created_at", "modified_at", "createdDateTime", "lastModifiedDateTime"]
        for field in date_fields:
            if field in transformed and transformed[field]:
                transformed[field] = self._parse_date(transformed[field])

        # Normalize names (uppercase for consistency)
        if "name" in transformed:
            transformed["name_normalized"] = transformed["name"].upper()

        # Extract file extension if it's a file
        if "name" in transformed and "." in transformed["name"]:
            transformed["file_extension"] = Path(transformed["name"]).suffix.lower()

        return transformed

    def _transform_site(self, site: Dict[str, Any]) -> Dict[str, Any]:
        """Transform site-specific data."""
        transformed = self._transform_item(site)

        # Extract tenant name from URL
        if "url" in transformed:
            match = re.search(r"https://([^.]+)\.sharepoint\.com", transformed["url"])
            if match:
                transformed["tenant_name"] = match.group(1)

        # Normalize site type
        if "template" in transformed:
            transformed["site_type"] = self._normalize_site_type(transformed["template"])

        return transformed

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse various date formats."""
        if isinstance(date_str, datetime):
            return date_str

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

        # Enrich processed data
        for item in context.processed_data:
            self._enrich_item(item)

        # Calculate aggregated metrics
        if context.files:
            self._calculate_storage_metrics(context)

        self.logger.info("Data enrichment completed")

        return context

    def _enrich_item(self, item: Dict[str, Any]) -> None:
        """Add calculated fields to an item."""
        # Calculate age if created date exists
        if "created_at" in item and isinstance(item["created_at"], datetime):
            age_days = (datetime.utcnow() - item["created_at"]).days
            item["age_days"] = age_days
            item["age_category"] = self._categorize_age(age_days)

        # Calculate path depth
        if "server_relative_url" in item:
            path = item["server_relative_url"]
            # Split path and remove empty parts
            path_parts = [p for p in path.split("/") if p]
            # In SharePoint paths: /sites/test/docs/folder1/folder2/file.pdf
            # We count the folder depth excluding the root "sites" and the file itself
            # So we subtract 2 (1 for "sites", 1 for the file)
            if len(path_parts) > 1:
                item["path_depth"] = len(path_parts) - 2
            else:
                item["path_depth"] = 0

        # Categorize file size
        if "size_bytes" in item:
            item["size_category"] = self._categorize_size(item["size_bytes"])

        # Mark external visibility
        if "principal_name" in item:
            item["is_external"] = self._is_external_user(item["principal_name"])

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

    def _is_external_user(self, principal_name: str) -> bool:
        """Check if a user is external."""
        external_indicators = ["#ext#", "_external", "@gmail.com", "@outlook.com", "@hotmail.com"]
        return any(indicator in principal_name.lower() for indicator in external_indicators)

    def _calculate_storage_metrics(self, context: PipelineContext) -> None:
        """Calculate storage-related metrics."""
        total_size = sum(f.get("size_bytes", 0) for f in context.files)

        context.metrics.custom_metrics["total_storage_bytes"] = total_size
        context.metrics.custom_metrics["total_storage_gb"] = total_size / (1024**3)
        context.metrics.custom_metrics["average_file_size_mb"] = (total_size / len(context.files) / (1024**2)) if context.files else 0


class StorageStage(PipelineStage):
    """Pipeline stage for saving processed data to the database."""

    def __init__(self, db_repo: DatabaseRepository):
        super().__init__("storage")
        self.db_repo = db_repo
        self.batch_size = 1000

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Save all processed data to the database."""
        self.logger.info("Starting storage stage")

        try:
            # Save processed generic data
            if context.processed_data:
                await self._save_batch("processed_items", context.processed_data)

            # Save specific data types
            if context.files:
                await self._save_batch("files", context.files)

            if context.folders:
                await self._save_batch("folders", context.folders)

            if context.permissions:
                await self._save_batch("permissions", context.permissions)

            # Update audit run statistics
            await self._update_audit_stats(context)

            self.logger.info("All data saved successfully")

        except Exception as e:
            self.logger.error(f"Storage stage failed: {str(e)}")
            raise

        return context

    async def _save_batch(self, table_name: str, records: List[Dict[str, Any]]) -> None:
        """Save records in batches."""
        total_saved = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            saved_count = await self.db_repo.bulk_insert(table_name, batch)
            total_saved += saved_count

            self.logger.debug(f"Saved batch {i//self.batch_size + 1} to {table_name}: {saved_count} records")

        self.logger.info(f"Saved {total_saved} records to {table_name}")

    async def _update_audit_stats(self, context: PipelineContext) -> None:
        """Update audit run statistics in the database."""
        # This would update the audit_runs table with final stats
        pass


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

    def _transform_permissions(self, permissions: List[Dict[str, Any]], object_id: str, object_type: str) -> List[Dict[str, Any]]:
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
        async with self.db.transaction() as conn:
            # Save files
            if result.file_records:
                await self.db.bulk_insert('files', result.file_records)

            # Save folders
            if hasattr(result, 'folder_records') and result.folder_records:
                await self.db.bulk_insert('folders', result.folder_records)

            # Save permissions
            if result.permission_records:
                await self.db.bulk_insert('permissions', result.permission_records)

            # Update statistics
            await self._update_stats(result)

    async def _update_stats(self, result: 'ProcessingResult') -> None:
        """Update audit statistics."""
        # This would update audit_runs table with processing stats
        pass


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

        # This is a placeholder - would integrate with the actual PermissionAnalyzer from Phase 5
        if self.analyzer:
            # Analyze permissions for files with unique permissions
            unique_permission_items = [
                item for item in context.processed_data
                if item.get("has_unique_permissions", False)
            ]

            self.logger.info(f"Found {len(unique_permission_items)} items with unique permissions")

            # Process each item
            for item in unique_permission_items:
                try:
                    permissions = await self.analyzer.analyze_item_permissions(item)
                    if permissions:
                        context.permissions.extend(permissions)
                except Exception as e:
                    self.logger.error(f"Failed to analyze permissions for item {item.get('id')}: {str(e)}")
        else:
            self.logger.warning("No permission analyzer configured, skipping permission analysis")

        return context
