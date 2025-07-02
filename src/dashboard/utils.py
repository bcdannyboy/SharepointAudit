"""Utility functions for the Streamlit dashboard."""

import sys
from pathlib import Path

# Add the parent directory to the Python path so we can import from src
parent_dir = Path(__file__).parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Now we can import from src
from src.database.repository import DatabaseRepository

def format_bytes(bytes_value):
    """Format bytes into human-readable format"""
    if bytes_value is None or bytes_value == 0:
        return "0 B"

    units = ['B', 'KB', 'MB', 'GB', 'TB']
    unit_index = 0

    while bytes_value >= 1024 and unit_index < len(units) - 1:
        bytes_value /= 1024
        unit_index += 1

    return f"{bytes_value:.2f} {units[unit_index]}"

def format_number(num):
    """Format large numbers with thousands separator"""
    if num is None:
        return "0"
    return f"{num:,}"

__all__ = ['DatabaseRepository', 'format_bytes', 'format_number']
