# Phase 5: Permission Analysis

## Overview

Implement the comprehensive permission analysis engine. This is a critical phase that involves analyzing unique vs. inherited permissions, recursively expanding group memberships, and detecting external sharing. The results of this analysis provide the core security insights of the audit.

## Architectural Alignment

This phase implements one of the most complex and vital components of the system. Its design is heavily guided by the `ARCHITECTURE.md`:

- **[Component Architecture: Permission Analyzer](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#3-permission-analyzer)**: This is the primary blueprint, detailing the `PermissionAnalyzer` class, its methods for analyzing item permissions, and the logic for handling unique vs. inherited permissions.
- **[API Integration: Graph API Client](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#graph-api-integration)**: The logic for expanding group memberships will rely on the Graph API's `transitiveMembers` endpoint, as specified in the architecture.
- **[Performance and Scalability: Caching Strategy](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#caching-strategy)**: Permission and group membership lookups are expensive. This phase must implement the caching strategy to store these results and drastically reduce API calls on subsequent lookups.

## Prerequisites

- [Phase 4: Data Processing Pipeline](./phase_4_pipeline.md)

## Deliverables

1.  **Permission Analyzer**: A `PermissionAnalyzer` class in `src/core/permissions.py` that contains all the logic for permission resolution.
2.  **Permission Models**: Data classes in `src/core/models.py` (or similar) to represent permission structures like `PermissionSet`, `ExternalShare`, etc.
3.  **Permission Caching**: Integration with a `CacheManager` to cache expensive lookups, such as expanded group memberships.

## Detailed Implementation Guide

### 1. Implement the Permission Analyzer (`src/core/permissions.py`)

This class will contain the core logic. It needs to determine if an item has unique permissions. If so, it fetches them directly. If not, it must traverse up the object hierarchy (file -> folder -> library -> site) to find the source of the inherited permissions.

```python
# src/core/permissions.py
# from src.api.graph_client import GraphAPIClient
# from src.api.sharepoint_client import SharePointAPIClient
# from src.database.repository import DatabaseRepository
# from src.cache.cache_manager import CacheManager

class PermissionAnalyzer:
    """Analyzes and maps all permissions across SharePoint."""

    def __init__(self, graph_client, sp_client, db_repo, cache_manager):
        self.graph_client = graph_client
        self.sp_client = sp_client
        self.db_repo = db_repo
        self.cache = cache_manager

    async def analyze_item_permissions(self, item):
        """Analyzes permissions for a specific SharePoint item."""
        if item.has_unique_role_assignments:
            return await self._get_unique_permissions(item)
        else:
            return await self._get_inherited_permissions(item)

    async def _get_unique_permissions(self, item):
        """Fetches and processes unique role assignments for an item."""
        # Use SharePoint API to get role assignments for the item
        # ...
        pass

    async def _get_inherited_permissions(self, item):
        """Traverses up the hierarchy to find the source of inherited permissions."""
        # Get parent object (folder, library, site) and recursively call
        # analyze_item_permissions until a parent with unique permissions is found.
        # ...
        pass

    async def expand_group_permissions(self, group_id: str):
        """Expands a group to get all its members, including nested groups."""
        cache_key = f"group_members:{group_id}"
        cached_members = await self.cache.get(cache_key)
        if cached_members:
            return cached_members

        # Use Graph API's /transitiveMembers endpoint
        members = await self.graph_client.expand_group_members_transitive(group_id)

        await self.cache.set(cache_key, members, ttl=21600) # Cache for 6 hours
        return members

    def detect_external_sharing(self, permissions):
        """Identifies external users and anonymous links in a permission set."""
        # Logic to check for principals like 'Guest' or anonymous link types.
        # ...
        pass
```

### 2. Integrate into the Pipeline

The permission analysis logic should be encapsulated into a `PermissionAnalysisStage` and added to the `AuditPipeline` from Phase 4. This stage will iterate through the discovered items from the context, analyze their permissions, and enrich the context with the results.

```python
# src/core/processors.py (continued)

class PermissionAnalysisStage(PipelineStage):
    """Pipeline stage for analyzing permissions of discovered items."""
    def __init__(self, permission_analyzer: 'PermissionAnalyzer'):
        self.analyzer = permission_analyzer

    async def execute(self, context: PipelineContext) -> PipelineContext:
        # For each item in context.processed_data...
        #   - Call self.analyzer.analyze_item_permissions(item)
        #   - Add the resolved permissions to the item or a separate list in the context
        # ...
        return context
```

## Implementation Task Checklist

- [ ] Implement the logic to check if an item `has_unique_role_assignments`.
- [ ] If unique, fetch the item's role assignments. If not, traverse up to the parent to find the source of inherited permissions.
- [ ] For group permissions, implement the transitive member expansion using the Graph API (`/groups/{id}/transitiveMembers`).
- [ ] Implement caching for expanded group memberships with a reasonable TTL (e.g., 6 hours).
- [ ] Implement logic to detect external users and anonymous guest links.
- [ ] Integrate the permission analysis into the main data processing pipeline as a new stage.
- [ ] Save all resolved user-level permissions to the `permissions` table in the database.

## Test Plan & Cases

Testing requires mocking items with both unique and inherited permissions, as well as mocking Graph API responses for group expansion.

```python
# tests/test_permissions.py
import pytest
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_group_expansion_is_transitive(permission_analyzer, mock_graph_client):
    """Verify that group expansion correctly fetches members of nested groups."""
    await permission_analyzer.expand_group_permissions("group_with_nested")

    # Assert that the graph client was called for the transitive members endpoint
    mock_graph_client.expand_group_members_transitive.assert_called_once_with("group_with_nested")

@pytest.mark.asyncio
async def test_permission_inheritance_logic(permission_analyzer):
    """Test that the analyzer correctly fetches permissions from the parent object."""
    item_with_inherited_perms = AsyncMock(has_unique_role_assignments=False)

    # Mock the internal methods
    permission_analyzer._get_unique_permissions = AsyncMock()
    permission_analyzer._get_inherited_permissions = AsyncMock()

    await permission_analyzer.analyze_item_permissions(item_with_inherited_perms)

    # Should not call get_unique_permissions, but should call get_inherited_permissions
    permission_analyzer._get_unique_permissions.assert_not_called()
    permission_analyzer._get_inherited_permissions.assert_called_once()
```

## Verification & Validation

After running the pipeline with the new permission stage, query the database to verify the results.

```bash
# 1. Run the pipeline including the new permission analysis stage
python scripts/run_pipeline.py --config config/config.json --analyze-permissions

# 2. Query the database to verify permission data
# Check for items with unique permissions
sqlite3 audit.db "SELECT object_id, principal_name, permission_level FROM permissions WHERE is_inherited = 0 LIMIT 20;"

# Check for expanded group members
sqlite3 audit.db "SELECT g.name, gm.user_id FROM groups g JOIN group_members gm ON g.id = gm.group_id LIMIT 20;"

# Check for external shares
sqlite3 audit.db "SELECT * FROM permissions WHERE principal_type = 'Guest' OR principal_name LIKE '%#ext#%';"
```

## Done Criteria

- [ ] The system can accurately distinguish between unique and inherited permissions.
- [ ] Group memberships are fully expanded, including nested groups.
- [ ] External sharing (both guest users and anonymous links) is correctly identified and recorded.
- [ ] Permission analysis is integrated into the pipeline and its results are stored in the database.
- [ ] Caching is shown to significantly reduce the number of API calls for permission analysis during a second run.
