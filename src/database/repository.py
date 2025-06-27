from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional
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


