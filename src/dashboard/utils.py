"""Utility functions for the Streamlit dashboard."""

import sys
from pathlib import Path

# Add the parent directory to the Python path so we can import from src
parent_dir = Path(__file__).parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Now we can import from src
from src.database.repository import DatabaseRepository

__all__ = ['DatabaseRepository']
