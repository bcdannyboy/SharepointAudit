# Phase 2: Database Layer & Models

## Overview

Implement the SQLite database layer using an asynchronous repository pattern. This includes defining all tables, views, and indexes as specified in the architecture to ensure high performance and data integrity. This phase provides the persistence layer for all discovered and processed audit data.

## Architectural Alignment

The database implementation is a cornerstone of this project and is meticulously detailed in the `ARCHITECTURE.md` document. This phase directly implements:

- **[Database Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#database-architecture)**: This is the primary reference, providing the complete schema design, table definitions, indexes, and performance-critical PRAGMA settings.
- **[Database Optimization](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#database-optimization)**: This section guides the implementation of the `DatabaseOptimizer` class for setting up a high-performance SQLite environment.
- **[Component Architecture: Data Processor](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#4-data-processor)**: While the processor itself is built later, the database models defined here are the explicit targets for the data processor's output.

## Prerequisites

- [Phase 0: Project Setup & Infrastructure](./phase_0_setup.md) must be complete.
- A clear understanding of the data model from `ARCHITECTURE.md`.

## Deliverables

1.  **Database Models**: SQLAlchemy Core or ORM models for all tables defined in `src/database/models.py`.
2.  **Repository Layer**: An async `DatabaseRepository` class in `src/database/repository.py` providing a clean, high-level interface for all database operations.
3.  **Database Initialization & Optimization**: A `DatabaseOptimizer` class in `src/database/optimizer.py` to create and configure a new database for optimal performance.

## Detailed Implementation Guide

### 1. Define Database Models (`src/database/models.py`)

Using SQLAlchemy, define the schema for all tables. This includes tables for `tenants`, `sites`, `libraries`, `folders`, `files`, `permissions`, `groups`, `group_members`, `audit_runs`, and `audit_checkpoints`. Ensure all columns, data types, relationships, and constraints match the `ARCHITECTURE.md`.

```python
# src/database/models.py
from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, String,
    DateTime, BigInteger, Boolean, ForeignKey, Text
)
from sqlalchemy.sql import func

metadata = MetaData()

# Example for the 'sites' table
sites = Table(
    'sites', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('site_id', String, unique=True, nullable=False, index=True),
    Column('tenant_id', Integer, ForeignKey('tenants.id'), index=True),
    Column('url', String, nullable=False),
    Column('title', String),
    Column('description', Text),
    Column('created_at', DateTime),
    Column('storage_used', BigInteger),
    Column('storage_quota', BigInteger),
    Column('is_hub_site', Boolean, default=False),
    Column('hub_site_id', String, index=True),
    Column('last_modified', DateTime),
)

# ... Define all other tables similarly ...

files = Table(
    'files', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('file_id', String, unique=True, nullable=False, index=True),
    Column('folder_id', Integer, ForeignKey('folders.id'), index=True),
    Column('library_id', Integer, ForeignKey('libraries.id'), index=True),
    Column('name', String, nullable=False),
    Column('server_relative_url', String, nullable=False),
    Column('size_bytes', BigInteger, index=True),
    Column('created_at', DateTime),
    Column('modified_at', DateTime, index=True),
    Column('has_unique_permissions', Boolean, default=False, index=True),
    # ... other columns
)
```

### 2. Implement the Database Repository (`src/database/repository.py`)

Create the `DatabaseRepository` class to abstract all database interactions. It should use `aiosqlite` for non-blocking I/O. Key methods will include `initialize_database`, `bulk_insert`, and an async transaction context manager.

```python
# src/database/repository.py
import aiosqlite
from contextlib import asynccontextmanager
from typing import List, Dict, Any

class DatabaseRepository:
    """Provides an async interface for all database operations."""
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def initialize_database(self):
        """Creates the full database schema and applies optimizations."""
        # Implementation will call DatabaseOptimizer
        optimizer = DatabaseOptimizer(self.db_path)
        await optimizer.initialize_database()
        async with aiosqlite.connect(self.db_path) as db:
            # Create tables from models.py
            # await db.execute(...)
            await db.commit()

    @asynccontextmanager
    async def transaction(self):
        """Provides an async transaction context."""
        conn = await aiosqlite.connect(self.db_path)
        try:
            yield conn
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise
        finally:
            await conn.close()

    async def bulk_insert(self, table_name: str, records: List[Dict[str, Any]]):
        """Performs a highly optimized bulk insert."""
        if not records:
            return 0
        # ... implementation using executemany ...
        return len(records)
```

### 3. Implement the Database Optimizer (`src/database/optimizer.py`)

This class will handle the initial setup of a new SQLite database file, applying all performance-critical PRAGMA settings as defined in the architecture.

```python
# src/database/optimizer.py
import aiosqlite

class DatabaseOptimizer:
    """Optimizes database performance for large-scale operations."""
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def initialize_database(self):
        """Initializes and optimizes a new database file."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode = WAL;")
            await db.execute("PRAGMA synchronous = NORMAL;")
            await db.execute("PRAGMA cache_size = -64000;") # 64MB cache
            await db.execute("PRAGMA temp_store = MEMORY;")
            await db.execute("PRAGMA mmap_size = 268435456;") # 256MB
            await db.commit()
```

## Implementation Task Checklist

- [ ] Define SQLAlchemy models for all tables in `src/database/models.py`.
- [ ] Implement the `DatabaseRepository` with async methods for all required database interactions.
- [ ] Implement a highly optimized `bulk_insert` method using `executemany`.
- [ ] Implement the `DatabaseOptimizer` to set PRAGMAs and create the full schema, including all indexes and views.
- [ ] Implement an async transaction context manager in the repository to ensure atomicity.
- [ ] Implement methods in the repository to query the predefined views (`vw_permission_summary`, `vw_storage_analytics`).
- [ ] Add comprehensive logging for database operations, especially for errors and long-running queries.

## Test Plan & Cases

Tests for this phase should run against a temporary, in-memory database to ensure isolation and speed.

```python
# tests/test_database.py
import pytest
# from src.database.repository import DatabaseRepository

@pytest.fixture
async def db_repo(tmp_path):
    """Create a temporary database repository for testing."""
    db_path = tmp_path / "test_audit.db"
    repo = DatabaseRepository(str(db_path))
    await repo.initialize_database()
    return repo

@pytest.mark.asyncio
async def test_full_schema_creation(db_repo):
    """Verify that all tables and views are created on initialization."""
    async with aiosqlite.connect(db_repo.db_path) as db:
        cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = {row[0] for row in await cursor.fetchall()}
        assert "sites" in tables
        assert "files" in tables
        assert "permissions" in tables

@pytest.mark.asyncio
async def test_bulk_insert_performance(db_repo):
    """Test that bulk insert is efficient for a large number of records."""
    files = [{'file_id': f'f{i}', 'name': f'doc{i}.txt', 'server_relative_url': f'/docs/doc{i}.txt', 'size_bytes': 100} for i in range(1000)]
    # This test will need adaptation based on the final model definition
    # inserted_count = await db_repo.bulk_insert('files', files)
    # assert inserted_count == 1000
    pass # Placeholder

@pytest.mark.asyncio
async def test_transaction_rollback(db_repo):
    """Test that a transaction is rolled back upon encountering an error."""
    try:
        async with db_repo.transaction() as conn:
            # This test needs a concrete save method to be implemented first
            # await conn.execute("INSERT INTO sites (...) VALUES (...)")
            raise ValueError("Simulating an error")
    except ValueError:
        pass # Expected error

    # Verify the site was not saved
    # site = await db_repo.get_site(...)
    # assert site is None
    pass # Placeholder
```

## Verification & Validation

Use the `sqlite3` command-line tool to inspect the database schema and settings after initialization.

```bash
# 1. Run a script to initialize the database
python -c "import asyncio; from src.database.repository import DatabaseRepository; asyncio.run(DatabaseRepository('audit.db').initialize_database())"

# 2. Inspect the schema of the created database
sqlite3 audit.db .schema

# 3. Verify that WAL mode is enabled
sqlite3 audit.db "PRAGMA journal_mode;"
# Expected output: wal
```

## Done Criteria

- [ ] The database initialization script successfully creates a SQLite file with the complete schema, including all tables, indexes, and views.
- [ ] The `bulk_insert` method can insert over 10,000 records in a single transaction efficiently.
- [ ] The transaction manager correctly commits on success and rolls back on failure.
- [ ] All repository methods are implemented and covered by unit tests.
- [ ] Performance-critical PRAGMA settings are applied and verified.
