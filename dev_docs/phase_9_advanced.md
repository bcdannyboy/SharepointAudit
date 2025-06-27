# Phase 9: Advanced Features

## Overview

Implement advanced features that build upon the core audit functionality. This phase introduces incremental updates to avoid re-scanning entire tenants, audit scheduling for automation, and advanced reporting for compliance and security assessments.

## Architectural Alignment

This phase enhances the existing architecture with new capabilities for automation and deeper analysis. It is guided by:

- **[API Integration: Delta Queries](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#api-integration-architecture)**: While introduced earlier, this phase focuses on fully utilizing the saved delta tokens to fetch only changes since the last audit, which is the foundation of incremental updates.
- **[Deployment Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#deployment-architecture)**: The scheduling feature will likely require integration with system-level schedulers (like cron or Windows Task Scheduler) or an embedded scheduling library, which touches upon deployment considerations.
- **[Data Processing Pipeline](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#data-processing-pipeline)**: The advanced reports will be implemented as new stages or outputs of the main data processing pipeline.

## Prerequisites

- All core phases (1-8) are complete and stable.

## Deliverables

1.  **Incremental Audits**: An `IncrementalAuditor` class in `src/core/incremental.py` that uses saved delta tokens to fetch only changes.
2.  **Audit Scheduling**: An `AuditScheduler` class in `src/core/scheduler.py` (potentially using a library like `apscheduler`) and corresponding CLI commands.
3.  **Advanced Reports**: A `ReportGenerator` in `src/core/reports.py` to produce specialized reports like compliance and security assessments.

## Detailed Implementation Guide

### 1. Implement Incremental Audits (`src/core/incremental.py`)

This module will be responsible for running audits that only process changes since the last run. It will rely heavily on the delta tokens saved as checkpoints during previous runs.

```python
# src/core/incremental.py
# from src.core.discovery import DiscoveryModule
# from src.database.repository import DatabaseRepository
# from src.utils.checkpoint_manager import CheckpointManager

class IncrementalAuditor:
    """Handles incremental audits by processing delta changes."""

    def __init__(self, discovery_module, db_repo, checkpoint_manager):
        self.discovery = discovery_module
        self.db_repo = db_repo
        self.checkpoints = checkpoint_manager

    async def detect_changes(self, run_id: str):
        """Fetches and processes only the changes since the last audit."""
        # 1. Get the last delta token from checkpoints
        last_delta = await self.checkpoints.restore_checkpoint(run_id, 'sites_delta_token')
        if not last_delta:
            raise ValueError("Cannot run incremental audit without a previous full audit.")

        # 2. Fetch changes using the delta token
        changes = await self.discovery.discover_all_sites(delta_token=last_delta)

        # 3. Process the changes (new, modified, deleted items)
        #    - New items are added.
        #    - Modified items are updated.
        #    - Deleted items are marked as such in the database.
        # ...

        # 4. Save the new delta token for the next run
        if changes.delta_token:
            await self.checkpoints.save_checkpoint(run_id, 'sites_delta_token', changes.delta_token)

        return changes
```

### 2. Implement Audit Scheduling (`src/core/scheduler.py`)

Integrate a scheduling library like `apscheduler` to allow users to schedule audits to run automatically. Expose this functionality through the CLI.

```python
# src/core/scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
import logging

logger = logging.getLogger(__name__)

class AuditScheduler:
    """Schedules and manages automated audit runs."""
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def schedule_audit(self, cron_expression: str, audit_func, *args, **kwargs):
        """Schedules a function to run based on a cron expression."""
        logger.info(f"Scheduling audit with expression: '{cron_expression}'")
        job = self.scheduler.add_job(
            audit_func,
            'cron',
            **self._parse_cron(cron_expression),
            args=args,
            kwargs=kwargs
        )
        return job

    def _parse_cron(self, cron_expression: str) -> dict:
        """Parses a cron string into arguments for apscheduler."""
        parts = cron_expression.split()
        if len(parts) != 5:
            raise ValueError("Invalid cron expression. Must have 5 parts.")
        minute, hour, day, month, day_of_week = parts
        return {
            'minute': minute,
            'hour': hour,
            'day': day,
            'month': month,
            'day_of_week': day_of_week,
        }

    def shutdown(self):
        self.scheduler.shutdown()
```

### 3. Implement Advanced Reports (`src/core/reports.py`)

Create a `ReportGenerator` that can query the audit database to produce high-level insights.

```python
# src/core/reports.py
# from src.database.repository import DatabaseRepository

class ReportGenerator:
    """Generates high-level reports from the audit database."""
    def __init__(self, db_repo):
        self.db_repo = db_repo

    async def generate_security_assessment(self) -> dict:
        """Generates a security posture report."""
        # Query for high-risk indicators:
        # - Number of items with unique permissions
        # - Number of external shares
        # - Users with excessive permissions (e.g., Full Control on many sites)
        # ...
        report = {
            'external_shares': await self.db_repo.count_external_shares(),
            'unique_permission_items': await self.db_repo.count_unique_permission_items(),
            # ...
        }
        return report
```

## Implementation Task Checklist

- [ ] Enhance the `DiscoveryModule` to fully utilize delta tokens for all relevant Graph API calls.
- [ ] Implement the logic in `IncrementalAuditor` to process the delta results and update the database.
- [ ] Integrate a scheduling library and expose it through the CLI (`sharepoint-audit schedule "0 2 * * *"`).
- [ ] Develop the queries and logic for the compliance and security assessment reports.
- [ ] Add a CLI command (`sharepoint-audit report <type>`) to generate these reports.
- [ ] Implement a basic alerting system (e.g., email on critical findings) that can be configured.

## Test Plan & Cases

Testing these features involves setting up a baseline state, simulating changes, and verifying that the incremental audit correctly detects them.

```python
# tests/test_advanced.py
import pytest
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_incremental_audit_detects_new_file(incremental_auditor, mock_discovery_module):
    """Test that an incremental audit correctly identifies a new file."""
    # Simulate a delta response that contains one new item
    mock_discovery_module.discover_all_sites.return_value = AsyncMock(
        items=[{'name': 'new_document.docx', 'status': 'new'}],
        delta_token='new_token'
    )

    changes = await incremental_auditor.detect_changes(run_id='test_run')

    assert len(changes.items) == 1
    assert changes.items[0]['name'] == 'new_document.docx'

def test_audit_scheduling(audit_scheduler):
    """Test that an audit can be scheduled correctly."""
    mock_audit_func = AsyncMock()
    job = audit_scheduler.schedule_audit("0 2 * * *", mock_audit_func) # 2 AM daily
    assert job.id is not None
    # Further tests would involve checking the scheduler's job store
```

## Verification & Validation

```bash
# 1. Run a full audit.
sharepoint-audit audit --config config/config.json

# 2. Make a change in the SharePoint test tenant (e.g., add a file).

# 3. Run an incremental audit and verify from the logs that only the change was processed.
sharepoint-audit audit --config config/config.json --incremental

# 4. Schedule an audit to run in the near future.
sharepoint-audit schedule "*/1 * * * *" # Every minute for testing
#    Verify that the audit runs automatically by checking logs.

# 5. Generate a security report.
sharepoint-audit report security --output security_report.json
```

## Done Criteria

- [ ] An incremental audit runs significantly faster than a full audit and correctly identifies changes.
- [ ] Audits can be scheduled, and the scheduler correctly triggers them at the specified time.
- [ ] Advanced reports are generated correctly and provide meaningful insights.
- [ ] All tests for advanced features pass.
