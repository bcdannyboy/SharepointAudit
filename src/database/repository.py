from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, List, Dict
from datetime import datetime

from .models import SCHEMA_STATEMENTS, INDEX_STATEMENTS, VIEW_STATEMENTS
from .optimizer import DatabaseOptimizer

logger = logging.getLogger(__name__)


class DatabaseRepository:
    """Async SQLite database repository implemented with sqlite3."""

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)

    async def initialize_database(self) -> None:
        optimizer = DatabaseOptimizer(str(self.db_path))
        await optimizer.initialize_database()
        await asyncio.to_thread(self._create_schema)

    def _create_schema(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            for stmt in SCHEMA_STATEMENTS:
                conn.execute(stmt)
            for stmt in INDEX_STATEMENTS:
                conn.execute(stmt)
            for stmt in VIEW_STATEMENTS:
                conn.execute(stmt)
            conn.commit()

    @asynccontextmanager
    async def transaction(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("BEGIN")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    async def bulk_insert(
        self, table_name: str, records: Iterable[Mapping[str, Any]], batch_size: int = 1000
    ) -> int:
        records = list(records)
        if not records:
            return 0
        columns = list(records[0].keys())
        placeholders = ",".join(["?" for _ in columns])
        query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        total = 0
        async with self.transaction() as conn:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                values = [tuple(r.get(c) for c in columns) for r in batch]
                conn.executemany(query, values)
                total += len(batch)
        return total

    async def save_site(self, site_data: Mapping[str, Any], conn: Optional[sqlite3.Connection] = None) -> None:
        columns = list(site_data.keys())
        placeholders = ",".join(["?" for _ in columns])
        query = f"INSERT INTO sites ({','.join(columns)}) VALUES ({placeholders})"
        values = tuple(site_data.get(c) for c in columns)
        if conn is None:
            async with self.transaction() as conn2:
                conn2.execute(query, values)
        else:
            conn.execute(query, values)

    async def get_site(self, site_id: str) -> Optional[Mapping[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM sites WHERE site_id = ?", (site_id,))
            row = cursor.fetchone()
            if row is None:
                return None
            return {description[0]: value for description, value in zip(cursor.description, row)}

    async def get_permission_summary(self) -> list[Mapping[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT * FROM vw_permission_summary")
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, r)) for r in rows]

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint_type: str,
        checkpoint_data: Any,
    ) -> None:
        data = json.dumps(checkpoint_data)
        query = (
            "INSERT INTO audit_checkpoints (run_id, checkpoint_type, checkpoint_data)"
            " VALUES (?, ?, ?)"
        )
        async with self.transaction() as conn:
            conn.execute(query, (run_id, checkpoint_type, data))

    async def get_latest_checkpoint(
        self, run_id: str, checkpoint_type: str
    ) -> Optional[Mapping[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT checkpoint_data, created_at FROM audit_checkpoints "
                "WHERE run_id = ? AND checkpoint_type = ? ORDER BY created_at DESC LIMIT 1",
                (run_id, checkpoint_type),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return {"checkpoint_data": row[0], "created_at": row[1]}

    async def delete_checkpoints_before(self, cutoff_date: datetime) -> None:
        async with self.transaction() as conn:
            conn.execute(
                "DELETE FROM audit_checkpoints WHERE created_at < ?",
                (cutoff_date.isoformat(),),
            )

    async def fetch_all(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return all results as a list of dictionaries."""
        def _fetch():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(query, params or ())
                rows = cursor.fetchall()
                return [dict(row) for row in rows]

        return await asyncio.to_thread(_fetch)

    async def fetch_one(self, query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        """Execute a SELECT query and return the first result as a dictionary."""
        def _fetch():
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(query, params or ())
                row = cursor.fetchone()
                return dict(row) if row else None

        return await asyncio.to_thread(_fetch)

    async def execute(self, query: str, params: Optional[tuple] = None) -> None:
        """Execute a non-SELECT query."""
        async with self.transaction() as conn:
            conn.execute(query, params or ())

    async def count_rows(self, table_name: str, where_clause: Optional[str] = None) -> int:
        """Count rows in a table with optional WHERE clause."""
        query = f"SELECT COUNT(*) FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        result = await self.fetch_one(query)
        return result['COUNT(*)'] if result else 0

    async def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database."""
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = await self.fetch_one(query, (table_name,))
        return result is not None

    async def get_table_columns(self, table_name: str) -> List[str]:
        """Get column names for a table."""
        query = f"PRAGMA table_info({table_name})"

        def _get_columns():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(query)
                return [row[1] for row in cursor.fetchall()]

        return await asyncio.to_thread(_get_columns)

    async def update_audit_run(self, run_id: str, updates: Dict[str, Any]) -> None:
        """Update an audit run record."""
        if not updates:
            return

        set_clauses = [f"{key} = ?" for key in updates.keys()]
        query = f"UPDATE audit_runs SET {', '.join(set_clauses)} WHERE run_id = ?"
        params = list(updates.values()) + [run_id]

        await self.execute(query, tuple(params))

    async def create_audit_run(self, run_id: str, tenant_id: Optional[int] = None) -> None:
        """Create a new audit run record."""
        query = "INSERT INTO audit_runs (run_id, tenant_id) VALUES (?, ?)"
        await self.execute(query, (run_id, tenant_id))

    async def get_sites_summary(self) -> Dict[str, Any]:
        """Get a summary of sites in the database."""
        total_sites = await self.count_rows("sites")

        query = """
        SELECT
            COUNT(DISTINCT s.id) as site_count,
            COUNT(DISTINCT l.id) as library_count,
            COUNT(DISTINCT f.id) as file_count,
            SUM(f.size_bytes) as total_size_bytes
        FROM sites s
        LEFT JOIN libraries l ON s.id = l.site_id
        LEFT JOIN files f ON l.id = f.library_id
        """

        result = await self.fetch_one(query)

        return {
            "total_sites": result['site_count'] if result else 0,
            "total_libraries": result['library_count'] if result else 0,
            "total_files": result['file_count'] if result else 0,
            "total_size_bytes": result['total_size_bytes'] if result else 0,
        }

    async def get_permissions_summary(self) -> Dict[str, Any]:
        """Get a summary of permissions in the database."""
        total_permissions = await self.count_rows("permissions")
        unique_permissions = await self.count_rows("permissions", "is_inherited = 0")

        query = """
        SELECT permission_level, COUNT(*) as count
        FROM permissions
        GROUP BY permission_level
        """

        levels = await self.fetch_all(query)

        return {
            "total_permissions": total_permissions,
            "unique_permissions": unique_permissions,
            "permissions_by_level": {row['permission_level']: row['count'] for row in levels}
        }

    async def vacuum(self) -> None:
        """Run VACUUM to optimize database file size."""
        def _vacuum():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("VACUUM")

        await asyncio.to_thread(_vacuum)

    async def analyze(self) -> None:
        """Run ANALYZE to update database statistics."""
        def _analyze():
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("ANALYZE")

        await asyncio.to_thread(_analyze)

    async def check_integrity(self) -> bool:
        """Check database integrity."""
        query = "PRAGMA integrity_check"

        def _check():
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(query)
                result = cursor.fetchone()
                return result[0] == "ok" if result else False

        return await asyncio.to_thread(_check)
