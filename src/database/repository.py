from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, List, Dict, Tuple
from datetime import datetime, timezone

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

    async def bulk_upsert(
        self, table_name: str, records: Iterable[Mapping[str, Any]],
        unique_columns: List[str], batch_size: int = 1000
    ) -> int:
        """Insert or update records based on unique columns."""
        records = list(records)
        if not records:
            return 0

        columns = list(records[0].keys())
        placeholders = ",".join(["?" for _ in columns])

        # Build the update clause for ON CONFLICT
        update_columns = [c for c in columns if c not in unique_columns]

        if update_columns:
            # Update non-unique columns on conflict
            update_clause = ", ".join([f"{c} = excluded.{c}" for c in update_columns])
            query = f"""
                INSERT INTO {table_name} ({','.join(columns)})
                VALUES ({placeholders})
                ON CONFLICT ({','.join(unique_columns)})
                DO UPDATE SET {update_clause}
            """
        else:
            # No columns to update, just ignore duplicates
            query = f"""
                INSERT OR IGNORE INTO {table_name} ({','.join(columns)})
                VALUES ({placeholders})
            """

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
        # Get counts separately to avoid JOIN issues
        total_sites = await self.count_rows("sites")
        total_libraries = await self.count_rows("libraries")
        total_files = await self.count_rows("files")

        # Get total size
        size_query = "SELECT SUM(size_bytes) as total_size_bytes FROM files"
        size_result = await self.fetch_one(size_query)
        total_size_bytes = size_result['total_size_bytes'] if size_result and size_result['total_size_bytes'] else 0

        return {
            "total_sites": total_sites,
            "total_libraries": total_libraries,
            "total_files": total_files,
            "total_size_bytes": total_size_bytes,
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

    async def get_cache_entry(self, key: str) -> Optional[Dict[str, Any]]:
        """Get a cache entry by key."""
        query = "SELECT cache_key as key, cache_value as value, expires_at, created_at FROM cache_entries WHERE cache_key = ?"
        return await self.fetch_one(query, (key,))

    async def set_cache_entry(self, key: str, value: str, expires_at: Optional[datetime] = None) -> None:
        """Set or update a cache entry."""
        query = """
        INSERT INTO cache_entries (cache_key, cache_value, expires_at)
        VALUES (?, ?, ?)
        ON CONFLICT(cache_key) DO UPDATE SET
            cache_value = excluded.cache_value,
            expires_at = excluded.expires_at
        """
        expires_at_str = expires_at.isoformat() if expires_at else datetime.now(timezone.utc).isoformat()
        await self.execute(query, (key, value, expires_at_str))

    async def delete_cache_entry(self, key: str) -> bool:
        """Delete a cache entry."""
        query = "DELETE FROM cache_entries WHERE cache_key = ?"
        async with self.transaction() as conn:
            cursor = conn.execute(query, (key,))
            return cursor.rowcount > 0

    async def clear_cache(self) -> None:
        """Clear all cache entries."""
        await self.execute("DELETE FROM cache_entries")

    async def cleanup_expired_cache_entries(self) -> int:
        """Delete expired cache entries and return count of deleted entries."""
        query = "DELETE FROM cache_entries WHERE expires_at IS NOT NULL AND expires_at < ?"
        async with self.transaction() as conn:
            cursor = conn.execute(query, (datetime.now(timezone.utc).isoformat(),))
            return cursor.rowcount

    async def get_permissions_paginated(
        self,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get permissions with pagination and optional filters.

        Returns:
            Tuple of (permissions list, total count)
        """
        # Build WHERE clause based on filters
        where_clauses = []
        params = []

        if filters:
            if filters.get('object_type'):
                where_clauses.append("p.object_type = ?")
                params.append(filters['object_type'])
            if filters.get('principal_type'):
                where_clauses.append("p.principal_type = ?")
                params.append(filters['principal_type'])
            if filters.get('is_external') is not None:
                where_clauses.append("p.is_external = ?")
                params.append(filters['is_external'])
            if filters.get('is_inherited') is not None:
                where_clauses.append("p.is_inherited = ?")
                params.append(filters['is_inherited'])

        where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Get total count
        count_query = f"SELECT COUNT(*) as count FROM permissions p{where_clause}"
        count_result = await self.fetch_one(count_query, params)
        total_count = count_result['count'] if count_result else 0

        # Get paginated results using the view for better performance
        query = f"""
        SELECT
            p.*,
            ps.object_name,
            ps.object_path
        FROM permissions p
        JOIN vw_permission_summary ps ON p.object_type = ps.object_type
            AND p.object_id = ps.object_id
            AND p.principal_id = ps.principal_id
        {where_clause}
        ORDER BY p.id DESC
        LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        results = await self.fetch_all(query, params)
        return results, total_count

    async def get_files_paginated(
        self,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None,
        order_by: str = "modified_at DESC"
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Get files with pagination and optional filters."""
        where_clauses = []
        params = []

        if filters:
            if filters.get('site_id'):
                where_clauses.append("f.site_id = ?")
                params.append(filters['site_id'])
            if filters.get('library_id'):
                where_clauses.append("f.library_id = ?")
                params.append(filters['library_id'])
            if filters.get('content_type'):
                where_clauses.append("f.content_type LIKE ?")
                params.append(f"%{filters['content_type']}%")
            if filters.get('min_size'):
                where_clauses.append("f.size_bytes >= ?")
                params.append(filters['min_size'])

        where_clause = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        # Get total count
        count_query = f"SELECT COUNT(*) as count FROM files f{where_clause}"
        count_result = await self.fetch_one(count_query, params)
        total_count = count_result['count'] if count_result else 0

        # Get paginated results
        query = f"""
        SELECT
            f.*,
            s.title as site_title,
            s.url as site_url,
            l.name as library_name,
            fo.name as folder_name
        FROM files f
        JOIN sites s ON f.site_id = s.id
        JOIN libraries l ON f.library_id = l.id
        LEFT JOIN folders fo ON f.folder_id = fo.id
        {where_clause}
        ORDER BY f.{order_by}
        LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])

        results = await self.fetch_all(query, params)
        return results, total_count

    async def get_permission_stats_by_type(self) -> List[Dict[str, Any]]:
        """Get permission statistics grouped by object type."""
        query = """
        SELECT
            object_type,
            COUNT(*) as total_count,
            COUNT(DISTINCT object_id) as unique_objects,
            COUNT(DISTINCT principal_id) as unique_principals,
            SUM(CASE WHEN is_external = 1 THEN 1 ELSE 0 END) as external_count,
            SUM(CASE WHEN is_inherited = 0 THEN 1 ELSE 0 END) as unique_permissions
        FROM permissions
        GROUP BY object_type
        """
        return await self.fetch_all(query)

    async def get_top_users_by_permissions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get top users by permission count."""
        query = """
        SELECT
            principal_name,
            principal_id,
            principal_type,
            COUNT(*) as permission_count,
            COUNT(DISTINCT object_id) as object_count,
            SUM(CASE WHEN is_external = 1 THEN 1 ELSE 0 END) as external_access
        FROM permissions
        WHERE principal_type IN ('user', 'external')
        GROUP BY principal_id, principal_name, principal_type
        ORDER BY permission_count DESC
        LIMIT ?
        """
        return await self.fetch_all(query, (limit,))
