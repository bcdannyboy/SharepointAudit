#!/bin/bash

# Script to optimize the SharePoint audit dashboard performance

echo "=== SharePoint Audit Dashboard Optimization ==="
echo

# Check if database path is provided
if [ -z "$1" ]; then
    DB_PATH="audit.db"
else
    DB_PATH="$1"
fi

# Check if database exists
if [ ! -f "$DB_PATH" ]; then
    echo "Error: Database file not found: $DB_PATH"
    echo "Usage: $0 [database_path]"
    exit 1
fi

echo "Optimizing database: $DB_PATH"
echo

# Add performance indexes
echo "Step 1: Adding performance indexes..."
python src/database/optimize_indexes.py "$DB_PATH"

echo
echo "Step 2: Running VACUUM to optimize database storage..."
sqlite3 "$DB_PATH" "VACUUM;"
echo "✓ Database vacuumed"

echo
echo "Step 3: Updating database statistics..."
sqlite3 "$DB_PATH" "ANALYZE;"
echo "✓ Statistics updated"

echo
echo "=== Optimization Complete ==="
echo
echo "To use the optimized dashboard, run:"
echo "  sharepoint-audit dashboard --db-path $DB_PATH --optimized"
echo
echo "Benefits of the optimized dashboard:"
echo "  ✓ Paginated data loading (100 records at a time)"
echo "  ✓ Efficient queries using database views"
echo "  ✓ Optimized indexes for common queries"
echo "  ✓ Reduced memory usage"
echo "  ✓ Faster initial load times"
