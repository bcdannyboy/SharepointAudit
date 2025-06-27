# Phase 3: Basic Discovery Module

## Overview

Implement the core discovery functionality to enumerate SharePoint sites, libraries, folders, and files. This phase focuses on efficiently traversing the tenant structure using modern authentication and Graph API delta queries, and storing the discovered inventory in the database established in Phase 2.

## Architectural Alignment

This phase brings together the API client and the database layer to perform the first major function of the application. It is guided by:

- **[Component Architecture: Discovery Module](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#2-discovery-module)**: This section provides the high-level design for the `DiscoveryModule`, including its responsibilities and interactions with other components.
- **[API Integration: Graph API Client](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#graph-api-integration)**: The implementation will heavily rely on the `get_all_sites_delta` method for efficient site enumeration.
- **[Error Handling and Resilience: Checkpoint Management](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#checkpoint-management)**: The discovery process must be resumable. This section guides the implementation of checkpointing to save and restore state.

## Prerequisites

- [Phase 1: Core Authentication & API Client](./phase_1_auth_api.md)
- [Phase 2: Database Layer & Models](./phase_2_database.md)

## Deliverables

1.  **Discovery Module**: A `DiscoveryModule` class in `src/core/discovery.py` that orchestrates the entire discovery process.
2.  **Progress Tracking**: A `ProgressTracker` class in `src/core/progress_tracker.py` to provide real-time feedback for long-running operations.
3.  **Checkpointing Integration**: Logic within the discovery module to save and resume its state from the `audit_checkpoints` table.

## Detailed Implementation Guide

### 1. Implement the Discovery Module (`src/core/discovery.py`)

This class is the orchestrator. It will use the `GraphAPIClient` to discover all sites and then iterate through each site to discover its content in parallel.

```python
# src/core/discovery.py
import asyncio
# from src.api.graph_client import GraphAPIClient
# from src.api.sharepoint_client import SharePointAPIClient
# from src.database.repository import DatabaseRepository
# from src.core.progress_tracker import ProgressTracker
# from src.utils.checkpoint_manager import CheckpointManager

class DiscoveryModule:
    """Discovers and enumerates all SharePoint content."""

    def __init__(self, graph_client, sp_client, db_repo, checkpoint_manager):
        self.graph_client = graph_client
        self.sp_client = sp_client
        self.db_repo = db_repo
        self.checkpoint_manager = checkpoint_manager
        self.progress_tracker = ProgressTracker()

    async def run_discovery(self, run_id: str):
        """Orchestrates the full discovery process."""
        self.progress_tracker.start("Site Discovery")

        # 1. Discover all sites using delta query
        sites_delta_token = await self.checkpoint_manager.restore_checkpoint(run_id, 'sites_delta_token')
        sites_result = await self.graph_client.get_all_sites_delta(sites_delta_token)

        # Save discovered sites to the database
        await self.db_repo.bulk_insert('sites', [site.to_dict() for site in sites_result.items])

        # Save the new delta token as a checkpoint
        if sites_result.delta_token:
            await self.checkpoint_manager.save_checkpoint(run_id, 'sites_delta_token', sites_result.delta_token)

        self.progress_tracker.finish("Site Discovery", f"Found {len(sites_result.items)} sites.")

        # 2. Discover content for each site in parallel
        tasks = [self.discover_site_content(run_id, site) for site in sites_result.items]
        await asyncio.gather(*tasks)

    async def discover_site_content(self, run_id: str, site):
        """Discovers all content for a single site."""
        # Check if this site was already processed in a previous run
        checkpoint = await self.checkpoint_manager.restore_checkpoint(run_id, f"site_{site.id}_status")
        if checkpoint == 'completed':
            self.progress_tracker.skip(f"Site {site.title}", "Already processed")
            return

        self.progress_tracker.start(f"Site {site.title}")

        # Discover libraries, lists, folders, files recursively
        # Use batching and parallel execution where possible
        # ... implementation ...

        # Save a checkpoint indicating this site is done
        await self.checkpoint_manager.save_checkpoint(run_id, f"site_{site.id}_status", 'completed')
        self.progress_tracker.finish(f"Site {site.title}")

```

### 2. Implement Progress Tracking (`src/core/progress_tracker.py`)

Create a simple class that can be used to log progress updates. This can be integrated with a library like `rich` or `tqdm` in the CLI phase.

```python
# src/core/progress_tracker.py
import logging

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Provides real-time feedback on long-running operations."""
    def start(self, task_name: str):
        logger.info(f"[PROGRESS] Starting: {task_name}")

    def finish(self, task_name: str, message: str = "Done"):
        logger.info(f"[PROGRESS] Finished: {task_name} - {message}")

    def skip(self, task_name: str, reason: str):
        logger.info(f"[PROGRESS] Skipping: {task_name} - {reason}")

    def update(self, task_name: str, current: int, total: int):
        logger.info(f"[PROGRESS] {task_name}: {current}/{total} ({current/total:.1%})")
```

### 3. Implement Checkpointing

The `DiscoveryModule` should be designed to be stateless. All state, such as delta tokens or the list of processed sites, should be managed via the `CheckpointManager`. This ensures that if the audit is interrupted, it can be resumed exactly where it left off by reading the last saved state from the `audit_checkpoints` table.

## Implementation Task Checklist

- [ ] Implement `discover_all_sites` using the Graph API's delta query functionality to fetch all sites.
- [ ] Store and reuse the `sites_delta_token` for subsequent incremental runs via the checkpointing system.
- [ ] Implement `discover_site_content` to run enumeration of libraries, lists, and subsites in parallel for a single site.
- [ ] Implement recursive traversal of folders within each library.
- [ ] Use batching for API calls (e.g., getting items in a folder) where possible.
- [ ] Integrate the `DatabaseRepository` to save all discovered items (sites, libraries, folders, files) to the database using bulk inserts.
- [ ] Implement checkpointing after each major step (e.g., after each site is fully processed).
- [ ] Implement logic to check for and restore from a checkpoint at the start of an audit.
- [ ] Integrate the `ProgressTracker` to provide real-time updates.

## Test Plan & Cases

Testing this module involves mocking the API clients and the database repository to verify the discovery logic and checkpointing behavior.

```python
# tests/test_discovery.py
import pytest
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_discover_all_sites_uses_delta(discovery_module, mock_graph_client, mock_checkpoint_manager):
    """Verify that site discovery uses the delta query and saves the token."""
    # Mock the graph client to return a delta token
    mock_graph_client.get_all_sites_delta.return_value = AsyncMock(
        items=[...], delta_token="TEST_TOKEN"
    )
    await discovery_module.run_discovery('test_run')

    # Verify the token was saved via the checkpoint manager
    mock_checkpoint_manager.save_checkpoint.assert_called_with('test_run', 'sites_delta_token', 'TEST_TOKEN')

@pytest.mark.asyncio
async def test_discovery_resumes_from_checkpoint(discovery_module, mock_db_repo, mock_checkpoint_manager):
    """Test that discovery can resume from a saved checkpoint."""
    # Pre-populate a checkpoint in the manager
    async def restore_side_effect(run_id, key):
        if key == 'site_site1_status':
            return 'completed'
        return None
    mock_checkpoint_manager.restore_checkpoint.side_effect = restore_side_effect

    # Mock the discovery function to check its inputs
    discovery_module.discover_site_content = AsyncMock()

    # Simulate a list of sites to process
    sites_to_process = [AsyncMock(id='site1'), AsyncMock(id='site2')]
    discovery_module.graph_client.get_all_sites_delta.return_value = AsyncMock(items=sites_to_process, delta_token=None)

    await discovery_module.run_discovery(run_id='test_run')

    # Verify that the already processed site was skipped
    discovery_module.discover_site_content.assert_called_once_with('test_run', sites_to_process[1])
```

## Verification & Validation

A verification script can be used to run a full discovery on a small test tenant and then inspect the database.

```bash
# 1. Run a full discovery on a small test tenant
python scripts/run_discovery.py --config config/config.json

# 2. Verify the database contains the discovered data
sqlite3 audit.db "SELECT count(*) FROM sites;"
sqlite3 audit.db "SELECT count(*) FROM libraries;"
sqlite3 audit.db "SELECT count(*) FROM files;"

# 3. Stop the discovery mid-run (Ctrl+C) and restart it
#    Verify from the logs that it resumed from a checkpoint.
```

## Done Criteria

- [ ] The module can successfully discover all sites in a test tenant.
- [ ] The module can recursively discover all libraries, folders, and files within a test site.
- [ ] Discovered data is correctly saved to the SQLite database.
- [ ] The use of delta queries for site discovery is verified.
- [ ] The process can be stopped and resumed from the last checkpoint.
- [ ] Parallel discovery of sites demonstrates a measurable performance improvement over sequential discovery.
