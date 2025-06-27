# Phase 10: Production Hardening

## Overview

Finalize the application for production deployment. This involves adding operational features like health checks and backups, completing user and administrator documentation, performing stress testing, and preparing a distributable package. This is the last step to ensure the utility is robust, reliable, and easy for others to use.

## Architectural Alignment

This phase focuses on operational readiness and deployability, implementing the final pieces of the architecture:

- **[Deployment Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#deployment-architecture)**: This is the primary guide, detailing the `setup.py` configuration for creating a distributable package and the requirements for local installation.
- **[Backup and Recovery](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#backup-and-recovery)**: Provides the design for the `BackupManager` to ensure data can be safely backed up and restored.
- **[Monitoring and Observability](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#monitoring-and-observability)**: The `HealthChecker` is a key operational tool for monitoring the application's status.

## Prerequisites

- All previous phases (0-9) are functionally complete and stable.

## Deliverables

1.  **Operational Tools**: `HealthChecker` and `BackupManager` classes in `src/core/health.py` and `src/core/backup.py` respectively.
2.  **Comprehensive Documentation**: A `docs/` directory containing a User Guide, Admin Guide, and Troubleshooting Guide.
3.  **Distribution Package**: A finalized `setup.py` and a build process to create a distributable wheel (`.whl`) file.

## Detailed Implementation Guide

### 1. Implement Operational Tools

#### Health Checker (`src/core/health.py`)
Create a `HealthChecker` class that can verify connectivity to the SharePoint/Graph APIs and check the integrity of the local database.

```python
# src/core/health.py
# from src.api.auth_manager import AuthenticationManager
# from src.database.repository import DatabaseRepository

class HealthChecker:
    """Verifies the operational health of the application."""
    def __init__(self, auth_manager, db_repo):
        self.auth = auth_manager
        self.db = db_repo

    async def run_diagnostics(self) -> dict:
        """Runs a series of checks and returns a health report."""
        report = {}
        # Check API connectivity
        try:
            await self.auth.get_graph_client()
            report['graph_api_connectivity'] = {'status': 'healthy'}
        except Exception as e:
            report['graph_api_connectivity'] = {'status': 'unhealthy', 'error': str(e)}

        # Check database integrity
        try:
            await self.db.check_integrity()
            report['database_integrity'] = {'status': 'healthy'}
        except Exception as e:
            report['database_integrity'] = {'status': 'unhealthy', 'error': str(e)}

        return report
```

#### Backup Manager (`src/core/backup.py`)
Implement a `BackupManager` that uses the SQLite online backup API to create safe, consistent backups of the database without taking the application offline.

```python
# src/core/backup.py
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime

class BackupManager:
    """Manages database backups and recovery."""
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.backup_dir = self.db_path.parent / "backups"
        self.backup_dir.mkdir(exist_ok=True)

    def create_backup(self, description: str = "") -> Path:
        """Creates a compressed backup of the current database."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_{timestamp}{'_' + description if description else ''}.db"
        backup_path = self.backup_dir / backup_name

        # Use SQLite's online backup API
        source_conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        dest_conn = sqlite3.connect(backup_path)
        with dest_conn:
            source_conn.backup(dest_conn)
        source_conn.close()
        dest_conn.close()

        # Compress the backup
        compressed_path = self.backup_dir / f"{backup_name}.gz"
        # ... compression logic ...
        return compressed_path

    def restore_backup(self, backup_path: Path):
        """Restores the database from a backup."""
        # ... decompression and file copy logic ...
        pass
```

### 2. Write Comprehensive Documentation (`docs/`)

Create a `docs` directory and populate it with essential markdown files:
-   **`user_guide.md`**: Explain how to install the tool, run the basic commands (`audit`, `dashboard`), and interpret the results.
-   **`admin_guide.md`**: Detail the configuration file settings, how to set up the required Azure App Registration and certificate, and how to manage backups.
-   **`troubleshooting.md`**: List common errors (e.g., authentication failures, API throttling) and provide solutions.

### 3. Finalize for Distribution

Ensure the `setup.py` file is complete with all metadata and dependencies. Create a build pipeline (e.g., using GitHub Actions) to automate the process of testing and creating release artifacts.

## Implementation Task Checklist

- [ ] Implement the `HealthChecker` with checks for API and database connectivity.
- [ ] Implement the `BackupManager` using the SQLite backup API.
- [ ] Add corresponding CLI commands: `health`, `backup create`, `backup restore`.
- [ ] Write comprehensive user and administrator documentation in the `docs/` directory.
- [ ] Create stress testing scripts to simulate high-load scenarios.
- [ ] Perform a final review of all error handling and logging.
- [ ] Finalize the `setup.py` and create a build pipeline to produce release artifacts.

## Test Plan & Cases

```python
# tests/test_production.py
import pytest
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_health_check_passes_on_healthy_system(health_checker):
    """Test that the health check reports a healthy status."""
    health_checker.auth.get_graph_client = AsyncMock()
    health_checker.db.check_integrity = AsyncMock()

    report = await health_checker.run_diagnostics()
    assert report['graph_api_connectivity']['status'] == 'healthy'
    assert report['database_integrity']['status'] == 'healthy'

@pytest.mark.asyncio
async def test_backup_and_restore_cycle(backup_manager, tmp_path):
    """Test that a database can be backed up and successfully restored."""
    # Create a dummy DB file
    db_path = tmp_path / "test.db"
    db_path.touch()

    backup_manager.db_path = db_path

    # Create a backup
    backup_path = backup_manager.create_backup("test_backup")
    assert backup_path.exists()

    # "Corrupt" the original DB
    db_path.unlink()
    assert not db_path.exists()

    # Restore from backup
    backup_manager.restore_backup(backup_path)
    assert db_path.exists()
```

## Verification & Validation

```bash
# 1. Run the full test suite, including any stress tests.
pytest

# 2. Build the distribution package.
python setup.py sdist bdist_wheel

# 3. Install the package in a clean virtual environment and test core commands.
pip install dist/sharepoint_audit-*.whl
sharepoint-audit health
sharepoint-audit --help

# 4. Manually perform a backup and restore cycle.
sharepoint-audit backup create
# (manually delete the audit.db file)
sharepoint-audit backup restore <path_to_backup_file>
```

## Done Criteria

- [ ] The `health` command runs and correctly reports the system status.
- [ ] The `backup` and `restore` commands work reliably.
- [ ] The documentation is complete, accurate, and easy to understand.
- [ ] The application passes all stress tests without crashing.
- [ ] A distributable package can be built successfully.
