#!/usr/bin/env python3
"""
Debug script to investigate dashboard loading error with object_id conversion.
"""

import asyncio
import sqlite3
import sys
import logging
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.repository import DatabaseRepository

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def investigate_object_id_issue():
    """Investigate the object_id format issue causing dashboard errors."""

    print("🔍 Dashboard Object ID Investigation")
    print("=" * 50)

    # Database path
    db_path = "audit.db"

    try:
        repo = DatabaseRepository(db_path)

        # 1. Check permissions table object_id formats
        print("\n1. Checking permissions table object_id formats...")
        query = """
        SELECT object_type, object_id, principal_name, permission_level
        FROM permissions
        LIMIT 10
        """

        results = await repo.fetch_all(query)
        if results:
            print(f"Found {len(results)} permission records:")
            for i, row in enumerate(results[:5], 1):
                object_id = row['object_id']
                print(f"  {i}. Type: {row['object_type']}, ID: '{object_id}', Length: {len(str(object_id))}")

                # Test if object_id can be converted to int
                try:
                    int_val = int(object_id)
                    print(f"     ✅ Can convert to int: {int_val}")
                except ValueError as e:
                    print(f"     ❌ Cannot convert to int: {e}")
                    print(f"     📝 This is likely a SharePoint composite ID")
        else:
            print("  No permission records found in database")

        # 2. Check sites table site_id formats
        print("\n2. Checking sites table site_id formats...")
        sites_query = "SELECT site_id, url, title FROM sites LIMIT 5"
        sites_results = await repo.fetch_all(sites_query)

        if sites_results:
            print(f"Found {len(sites_results)} site records:")
            for i, row in enumerate(sites_results, 1):
                site_id = row['site_id']
                print(f"  {i}. Site ID: '{site_id}'")
                print(f"     URL: {row['url']}")
                print(f"     Title: {row['title']}")

                # Check if this looks like a SharePoint composite ID
                if ',' in str(site_id) and len(str(site_id).split(',')) == 3:
                    print(f"     📝 This appears to be a SharePoint composite ID")
                else:
                    print(f"     📝 This appears to be a simple ID")
        else:
            print("  No site records found in database")

        # 3. Check specific object_id patterns
        print("\n3. Analyzing object_id patterns...")
        pattern_query = """
        SELECT
            object_type,
            COUNT(*) as count,
            MIN(LENGTH(object_id)) as min_length,
            MAX(LENGTH(object_id)) as max_length,
            object_id as sample_id
        FROM permissions
        GROUP BY object_type
        LIMIT 10
        """

        pattern_results = await repo.fetch_all(pattern_query)
        if pattern_results:
            print("Object ID patterns by type:")
            for row in pattern_results:
                print(f"  {row['object_type']}: {row['count']} records, "
                      f"ID length {row['min_length']}-{row['max_length']}, "
                      f"Sample: '{row['sample_id']}'")

        # 4. Test dashboard conversion scenarios
        print("\n4. Testing dashboard conversion scenarios...")
        test_query = "SELECT object_id FROM permissions WHERE object_type = 'site' LIMIT 1"
        test_results = await repo.fetch_all(test_query)

        if test_results:
            test_object_id = test_results[0]['object_id']
            print(f"Testing with object_id: '{test_object_id}'")

            # Simulate the dashboard conversion
            try:
                # This is what the dashboard tries to do
                idx = int(test_object_id) % 5  # Similar to line 211 in comprehensive_app.py
                print(f"✅ Dashboard conversion successful: {idx}")
            except ValueError as e:
                print(f"❌ Dashboard conversion failed: {e}")
                print("🔍 This confirms the root cause!")

                # Show the exact error that would occur
                print(f"📝 Error would be: invalid literal for int() with base 10: '{test_object_id}'")

        # 5. Check for mixed ID formats
        print("\n5. Checking for mixed ID formats in database...")
        mixed_query = """
        SELECT
            object_type,
            object_id,
            CASE
                WHEN object_id GLOB '*[!0-9]*' THEN 'Non-numeric'
                ELSE 'Numeric'
            END as id_type
        FROM permissions
        LIMIT 20
        """

        mixed_results = await repo.fetch_all(mixed_query)
        if mixed_results:
            numeric_count = sum(1 for r in mixed_results if r['id_type'] == 'Numeric')
            non_numeric_count = sum(1 for r in mixed_results if r['id_type'] == 'Non-numeric')

            print(f"Sample of 20 records:")
            print(f"  Numeric IDs: {numeric_count}")
            print(f"  Non-numeric IDs: {non_numeric_count}")

            if non_numeric_count > 0:
                print("📝 Mixed ID formats detected - this confirms the issue!")

                # Show examples of non-numeric IDs
                non_numeric_examples = [r for r in mixed_results if r['id_type'] == 'Non-numeric'][:3]
                print("Examples of non-numeric IDs:")
                for example in non_numeric_examples:
                    print(f"  Type: {example['object_type']}, ID: '{example['object_id']}'")

        print("\n" + "=" * 50)
        print("🎯 DIAGNOSIS SUMMARY")
        print("=" * 50)

    except Exception as e:
        logger.error(f"Investigation failed: {e}", exc_info=True)


async def main():
    """Main function."""
    await investigate_object_id_issue()


if __name__ == "__main__":
    asyncio.run(main())
