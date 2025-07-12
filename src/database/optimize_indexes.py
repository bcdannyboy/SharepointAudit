"""Script to add additional indexes for dashboard performance optimization."""

import sqlite3
import sys
from pathlib import Path

def add_performance_indexes(db_path: str):
    """Add additional indexes to improve dashboard query performance."""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Composite indexes for common dashboard queries
    indexes = [
        # Permissions table composite indexes
        "CREATE INDEX IF NOT EXISTS idx_permissions_composite_object ON permissions (object_type, object_id, principal_type, permission_level);",
        "CREATE INDEX IF NOT EXISTS idx_permissions_composite_principal ON permissions (principal_type, principal_id, object_type);",
        "CREATE INDEX IF NOT EXISTS idx_permissions_external_filter ON permissions (is_external, object_type, principal_type);",
        "CREATE INDEX IF NOT EXISTS idx_permissions_inherited_filter ON permissions (is_inherited, object_type);",

        # Files table composite indexes
        "CREATE INDEX IF NOT EXISTS idx_files_composite_site ON files (site_id, modified_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_files_composite_size ON files (size_bytes DESC, site_id);",
        "CREATE INDEX IF NOT EXISTS idx_files_content_type ON files (content_type, site_id);",

        # Folders table composite indexes
        "CREATE INDEX IF NOT EXISTS idx_folders_composite_site ON folders (site_id, has_unique_permissions);",

        # Libraries table composite indexes
        "CREATE INDEX IF NOT EXISTS idx_libraries_composite_site ON libraries (site_id, item_count DESC);",

        # Additional indexes for JOIN operations
        "CREATE INDEX IF NOT EXISTS idx_sites_site_id ON sites (site_id);",
        "CREATE INDEX IF NOT EXISTS idx_libraries_library_id ON libraries (library_id);",
        "CREATE INDEX IF NOT EXISTS idx_folders_folder_id ON folders (folder_id);",
        "CREATE INDEX IF NOT EXISTS idx_files_file_id ON files (file_id);",
    ]

    print(f"Adding performance indexes to {db_path}...")

    for idx, index_sql in enumerate(indexes, 1):
        try:
            cursor.execute(index_sql)
            print(f"✓ Index {idx}/{len(indexes)} created")
        except Exception as e:
            print(f"✗ Index {idx}/{len(indexes)} failed: {e}")

    # Analyze the database to update statistics
    print("\nAnalyzing database statistics...")
    cursor.execute("ANALYZE;")

    # Get index statistics
    cursor.execute("""
        SELECT name, tbl_name
        FROM sqlite_master
        WHERE type = 'index' AND name LIKE 'idx_%'
        ORDER BY tbl_name, name
    """)

    indexes = cursor.fetchall()
    print(f"\nTotal indexes in database: {len(indexes)}")

    # Group by table
    tables = {}
    for idx_name, tbl_name in indexes:
        if tbl_name not in tables:
            tables[tbl_name] = []
        tables[tbl_name].append(idx_name)

    print("\nIndexes by table:")
    for table, idx_list in sorted(tables.items()):
        print(f"  {table}: {len(idx_list)} indexes")

    conn.commit()
    conn.close()
    print("\n✓ Database optimization complete!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python optimize_indexes.py <database_path>")
        sys.exit(1)

    db_path = sys.argv[1]
    if not Path(db_path).exists():
        print(f"Error: Database file not found: {db_path}")
        sys.exit(1)

    add_performance_indexes(db_path)
