import asyncio
import sqlite3


class DatabaseOptimizer:
    """Optimizes SQLite database settings."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def initialize_database(self) -> None:
        await asyncio.to_thread(self._apply_pragmas)

    def _apply_pragmas(self) -> None:
        with sqlite3.connect(self.db_path) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("PRAGMA synchronous=NORMAL")
            db.execute("PRAGMA cache_size=-64000")
            db.execute("PRAGMA temp_store=MEMORY")
            db.execute("PRAGMA mmap_size=268435456")
            db.execute("PRAGMA page_size=4096")
            db.commit()

