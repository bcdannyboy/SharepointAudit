"""CLI entry point that properly sets up the Python path."""

import sys
import os
from pathlib import Path

# Add src directory to path for imports
src_dir = Path(__file__).parent.parent
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

# Set environment variable to help with imports
os.environ['PYTHONPATH'] = str(src_dir) + os.pathsep + os.environ.get('PYTHONPATH', '')

# Import after path is set up
from cli.main import main

if __name__ == "__main__":
    main()
