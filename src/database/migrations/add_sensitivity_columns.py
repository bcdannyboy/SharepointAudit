"""
Migration to add sensitivity analysis columns to the database
"""

import sqlite3
from pathlib import Path
from typing import Optional

def migrate_database(db_path: str) -> None:
    """Add sensitivity columns to files and permissions tables"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if columns already exist
        cursor.execute("PRAGMA table_info(files)")
        existing_columns = [row[1] for row in cursor.fetchall()]

        # Add sensitivity columns to files table
        if 'sensitivity_score' not in existing_columns:
            print("Adding sensitivity columns to files table...")
            cursor.execute("""
                ALTER TABLE files ADD COLUMN sensitivity_score INTEGER DEFAULT 0;
            """)

        if 'sensitivity_level' not in existing_columns:
            cursor.execute("""
                ALTER TABLE files ADD COLUMN sensitivity_level TEXT DEFAULT 'LOW';
            """)

        if 'sensitivity_categories' not in existing_columns:
            cursor.execute("""
                ALTER TABLE files ADD COLUMN sensitivity_categories TEXT;
            """)

        if 'sensitivity_factors' not in existing_columns:
            cursor.execute("""
                ALTER TABLE files ADD COLUMN sensitivity_factors TEXT;
            """)

        # Create index on sensitivity score for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_sensitivity
            ON files (sensitivity_score DESC);
        """)

        # Create a view for high sensitivity files
        cursor.execute("""
            CREATE VIEW IF NOT EXISTS vw_sensitive_files AS
            SELECT
                f.*,
                s.title as site_name,
                l.name as library_name,
                COUNT(DISTINCT p.principal_id) as total_users,
                COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_users,
                COUNT(DISTINCT CASE WHEN p.permission_level IN ('Full Control', 'Edit') THEN p.principal_id END) as write_users
            FROM files f
            JOIN sites s ON f.site_id = s.id
            LEFT JOIN libraries l ON f.library_id = l.id
            LEFT JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
            WHERE f.sensitivity_score >= 40
            GROUP BY f.id;
        """)

        # Create a summary table for sensitivity analytics
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sensitivity_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id INTEGER REFERENCES audit_runs(id),
                total_files INTEGER DEFAULT 0,
                sensitive_files INTEGER DEFAULT 0,
                critical_files INTEGER DEFAULT 0,
                high_risk_files INTEGER DEFAULT 0,
                categories_found TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.commit()
        print("Sensitivity migration completed successfully")

    except Exception as e:
        conn.rollback()
        print(f"Error during migration: {e}")
        raise
    finally:
        conn.close()


def rollback_migration(db_path: str) -> None:
    """Rollback the sensitivity migration"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Note: SQLite doesn't support DROP COLUMN directly
        # Would need to recreate the table without the columns
        cursor.execute("DROP VIEW IF EXISTS vw_sensitive_files;")
        cursor.execute("DROP TABLE IF EXISTS sensitivity_summary;")
        cursor.execute("DROP INDEX IF EXISTS idx_files_sensitivity;")

        conn.commit()
        print("Sensitivity migration rolled back")

    except Exception as e:
        conn.rollback()
        print(f"Error during rollback: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python add_sensitivity_columns.py <database_path> [rollback]")
        sys.exit(1)

    db_path = sys.argv[1]

    if len(sys.argv) > 2 and sys.argv[2] == "rollback":
        rollback_migration(db_path)
    else:
        migrate_database(db_path)
