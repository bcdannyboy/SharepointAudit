#!/usr/bin/env python3
"""Database migration script to add site_url columns."""

import asyncio
import aiosqlite
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def migrate_database(db_path: str):
    """Migrate the database to add site_url columns."""
    async with aiosqlite.connect(db_path) as db:
        # Enable WAL mode
        await db.execute("PRAGMA journal_mode=WAL")

        # Check if migration is needed
        cursor = await db.execute("PRAGMA table_info(libraries)")
        columns = await cursor.fetchall()
        column_names = [col[1] for col in columns]

        if 'site_url' in column_names:
            logger.info("Database already migrated")
            return

        logger.info("Starting database migration...")

        # Add new columns
        migrations = [
            "ALTER TABLE libraries ADD COLUMN site_url TEXT",
            "ALTER TABLE libraries ADD COLUMN drive_id TEXT",
            "ALTER TABLE folders ADD COLUMN site_id INTEGER",
            "ALTER TABLE folders ADD COLUMN site_url TEXT",
            "ALTER TABLE folders ADD COLUMN path TEXT",
            "ALTER TABLE files ADD COLUMN site_id INTEGER",
            "ALTER TABLE files ADD COLUMN site_url TEXT",
            "ALTER TABLE files ADD COLUMN folder_path TEXT",
            "ALTER TABLE permissions ADD COLUMN inheritance_source TEXT",
            "ALTER TABLE permissions ADD COLUMN is_external BOOLEAN DEFAULT FALSE",
            "ALTER TABLE permissions ADD COLUMN is_anonymous_link BOOLEAN DEFAULT FALSE",
            "ALTER TABLE group_members ADD COLUMN user_name TEXT",
            "ALTER TABLE group_members ADD COLUMN user_email TEXT",
        ]

        for migration in migrations:
            try:
                await db.execute(migration)
                logger.info(f"Executed: {migration}")
            except Exception as e:
                logger.warning(f"Migration already applied or failed: {migration} - {e}")

        # Update existing records with site URLs
        logger.info("Updating existing records with site URLs...")

        # Update libraries
        await db.execute("""
            UPDATE libraries
            SET site_url = (
                SELECT url FROM sites
                WHERE sites.site_id = libraries.site_id
            )
            WHERE site_url IS NULL
        """)

        # Update folders
        await db.execute("""
            UPDATE folders
            SET site_id = (
                SELECT site_id FROM libraries
                WHERE libraries.library_id = folders.library_id
            ),
            site_url = (
                SELECT url FROM sites
                WHERE sites.site_id = (
                    SELECT site_id FROM libraries
                    WHERE libraries.library_id = folders.library_id
                )
            )
            WHERE site_url IS NULL
        """)

        # Update files
        await db.execute("""
            UPDATE files
            SET site_id = (
                SELECT site_id FROM libraries
                WHERE libraries.library_id = files.library_id
            ),
            site_url = (
                SELECT url FROM sites
                WHERE sites.site_id = (
                    SELECT site_id FROM libraries
                    WHERE libraries.library_id = files.library_id
                )
            )
            WHERE site_url IS NULL
        """)

        # Create new indexes
        new_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_folders_site ON folders (site_id)",
            "CREATE INDEX IF NOT EXISTS idx_files_site ON files (site_id)",
        ]

        for index in new_indexes:
            try:
                await db.execute(index)
                logger.info(f"Created index: {index}")
            except Exception as e:
                logger.warning(f"Index creation failed: {index} - {e}")

        await db.commit()
        logger.info("Database migration completed successfully")


async def main():
    """Main entry point."""
    import sys

    if len(sys.argv) < 2:
        print("Usage: python migrate_db.py <database_path>")
        sys.exit(1)

    db_path = sys.argv[1]
    if not Path(db_path).exists():
        print(f"Database not found: {db_path}")
        sys.exit(1)

    await migrate_database(db_path)


if __name__ == "__main__":
    asyncio.run(main())
