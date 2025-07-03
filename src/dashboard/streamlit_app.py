import argparse
import streamlit as st
import sys
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Import the comprehensive dashboard
try:
    if __name__ == "__main__":
        from dashboard.comprehensive_app import main as comprehensive_main
        from dashboard.pages import overview, sites, permissions, files, export
    else:
        from .comprehensive_app import main as comprehensive_main
        from .pages import overview, sites, permissions, files, export
    USE_COMPREHENSIVE = True
except ImportError:
    USE_COMPREHENSIVE = False
    # Fallback to original pages
    if __name__ == "__main__":
        from dashboard.pages import overview, sites, permissions, files, export
    else:
        from .pages import overview, sites, permissions, files, export


def main(args=None):
    parser = argparse.ArgumentParser(description="SharePoint Audit Dashboard")
    parser.add_argument("--db-path", default="audit.db", help="Path to audit database")
    parser.add_argument("--classic", action="store_true", help="Use classic dashboard instead of comprehensive")
    parsed = parser.parse_args(args)

    # Store db_path in session state for comprehensive dashboard
    if 'db_path' not in st.session_state:
        st.session_state.db_path = parsed.db_path

    # Use comprehensive dashboard by default unless --classic is specified
    if USE_COMPREHENSIVE and not parsed.classic:
        comprehensive_main()
    else:
        # Classic dashboard
        st.set_page_config(
            page_title="SharePoint Audit Dashboard",
            layout="wide",
            initial_sidebar_state="expanded",
        )

        st.sidebar.title("Navigation")
        page_options = {
            "Overview": overview.render,
            "Sites": sites.render,
            "Permissions": permissions.render,
            "Files": files.render,
            "Export": export.render,
        }
        selection = st.sidebar.radio("Go to", list(page_options.keys()))
        page_func = page_options[selection]
        page_func(parsed.db_path)


if __name__ == "__main__":
    main()
