#!/bin/bash
# Script to clean database and run the audit with active-only flag

echo "Cleaning up previous audit data..."
rm -f audit.db
rm -f *.db-wal
rm -f *.db-shm

echo "Starting SharePoint audit (active sites only with reduced concurrency)..."
sharepoint-audit audit --config config/config.json --verbose --active-only "$@"
