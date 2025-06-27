import pytest
import sqlite3
import asyncio

from src.database import DatabaseRepository


@pytest.fixture
def db_repo(tmp_path):
    db_path = tmp_path / "test_audit.db"
    repo = DatabaseRepository(str(db_path))
    asyncio.run(repo.initialize_database())
    return repo


def test_full_schema_creation(db_repo):
    def read_names(query):
        with sqlite3.connect(db_repo.db_path) as db:
            cur = db.execute(query)
            return {row[0] for row in cur.fetchall()}

    async def run():
        tables = await asyncio.to_thread(read_names, "SELECT name FROM sqlite_master WHERE type='table'")
        assert "sites" in tables
        assert "permissions" in tables
        views = await asyncio.to_thread(read_names, "SELECT name FROM sqlite_master WHERE type='view'")
        assert "vw_permission_summary" in views

    asyncio.run(run())


def test_bulk_insert_performance(db_repo):
    files = [
        {
            "file_id": f"f{i}",
            "name": f"doc{i}.txt",
            "server_relative_url": f"/docs/doc{i}.txt",
        }
        for i in range(10000)
    ]

    async def run():
        inserted_count = await db_repo.bulk_insert("files", files)
        assert inserted_count == 10000

    asyncio.run(run())


def test_transaction_rollback(db_repo):
    async def run():
        try:
            async with db_repo.transaction() as conn:
                await db_repo.save_site(
                    {
                        "site_id": "s1",
                        "url": "https://example.com",
                        "title": "Test",
                    },
                    conn,
                )
                raise ValueError("Simulating an error")
        except ValueError:
            pass

        site = await db_repo.get_site("s1")
        assert site is None

    asyncio.run(run())


