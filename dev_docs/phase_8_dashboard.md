# Phase 8: Streamlit Dashboard

## Overview

Implement the interactive Streamlit dashboard for visualizing and analyzing the audit data stored in the SQLite database. This provides a user-friendly, web-based interface for non-technical users to explore the results of the audit.

## Architectural Alignment

The dashboard is the primary user interface for data analysis and is a key deliverable of the project. Its implementation is guided by:

- **[System Overview: Streamlit Dashboard](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#system-overview)**: Positions the dashboard as the post-audit analysis tool.
- **[Streamlit Dashboard Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#streamlit-dashboard-architecture)**: This is the main reference, providing the complete structure for the Streamlit application, including the multi-page layout, component design, and caching strategies.
- **[Database Architecture: Views](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#database-architecture)**: The dashboard will heavily query the pre-defined SQL views (`vw_permission_summary`, `vw_storage_analytics`) for aggregated data to ensure high performance.

## Prerequisites

- [Phase 2: Database Layer & Models](./phase_2_database.md)
- A sample database populated with audit data from previous phases.

## Deliverables

1.  **Main Dashboard App**: The main application entry point in `src/dashboard/streamlit_app.py`.
2.  **Dashboard Pages**: A collection of Python scripts in `src/dashboard/pages/`, with each script representing a different page or view in the dashboard (e.g., `overview.py`, `permissions.py`).
3.  **Interactive Components**: Reusable components in `src/dashboard/components/` for common UI elements like filters and data tables.

## Detailed Implementation Guide

### 1. Create the Main Dashboard App (`src/dashboard/streamlit_app.py`)

This script is the entry point for the Streamlit application. It handles page configuration, sidebar navigation, and routing to the different page modules.

```python
# src/dashboard/streamlit_app.py
import streamlit as st
from pathlib import Path
# Import page modules
from .pages import overview, permissions, files, export

def main():
    st.set_page_config(
        page_title="SharePoint Audit Dashboard",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.sidebar.title("Navigation")

    # In a real multi-page app, Streamlit handles this automatically
    # if you place pages in a `pages/` directory.
    # This is a conceptual representation.
    page_options = {
        "Overview": overview.render,
        "Permissions": permissions.render,
        "Files": files.render,
        "Export": export.render,
    }

    selection = st.sidebar.radio("Go to", list(page_options.keys()))

    # Load data - this should be cached
    db_path = "audit.db" # This should be configurable

    # Render the selected page
    page_func = page_options[selection]
    page_func(db_path)

if __name__ == "__main__":
    main()
```

### 2. Implement Dashboard Pages (`src/dashboard/pages/`)

Each page should be a separate Python file containing a `render(db_path)` function. Use Streamlit's caching decorators (`@st.cache_data`) extensively to prevent re-running expensive database queries on every interaction.

```python
# src/dashboard/pages/permissions.py
import streamlit as st
import pandas as pd
# from src.database.repository import DatabaseRepository

@st.cache_data(ttl=300) # Cache for 5 minutes
def load_permission_data(_db_path: str, site_filter: str):
    # In a real app, you'd pass the db_path to the repo
    # repo = DatabaseRepository(_db_path)
    # return repo.get_permission_summary(site_filter=site_filter)
    # For now, return dummy data
    return pd.DataFrame({
        'object_name': ['FileA.docx', 'FolderB', 'SiteC'],
        'principal_name': ['User1', 'GroupA', 'User2 (External)'],
        'permission_level': ['Edit', 'Read', 'Full Control'],
    })

def render(db_path: str):
    st.title("Permission Analysis")

    # Filters
    site_filter = st.selectbox("Filter by Site", ["All Sites", "Site A", "Site B"])

    # Load data using the cached function
    df = load_permission_data(db_path, site_filter)

    st.dataframe(df)

    # Add charts and other visualizations
    st.bar_chart(df['permission_level'].value_counts())
```

### 3. Implement Reusable Components (`src/dashboard/components/`)

For complex UI elements like a custom data table with filtering or a specialized chart, create reusable functions in the `components` directory. For example, an export component.

```python
# src/dashboard/components/export.py
import streamlit as st
import pandas as pd
import io

def create_download_button(df: pd.DataFrame, filename: str):
    """Creates a download button for a pandas DataFrame."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')

    st.download_button(
        label="📥 Download as Excel",
        data=output.getvalue(),
        file_name=f"{filename}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
```

## Implementation Task Checklist

- [ ] Create the main dashboard structure with sidebar navigation.
- [ ] Implement the Overview page with key metrics and charts.
- [ ] Implement the Sites page with a searchable table of all sites and storage analytics.
- [ ] Implement the Permissions page with visualizations for unique permissions, external shares, and a user/group permission matrix.
- [ ] Implement the Files page with filters for finding large, old, or un-accessed files.
- [ ] Implement the Export page, allowing users to download raw data as an Excel file.
- [ ] Use `@st.cache_data` to cache the results of database queries to ensure the dashboard is responsive.
- [ ] Ensure the dashboard layout is clean, responsive, and user-friendly.

## Test Plan & Cases

Streamlit has its own testing framework (`AppTest`) for programmatically testing the UI.

```python
# tests/test_dashboard.py
from streamlit.testing.v1 import AppTest

def test_dashboard_loads_and_shows_title():
    """Test that the main dashboard app loads without error."""
    at = AppTest.from_file("src/dashboard/streamlit_app.py").run()
    assert not at.exception
    assert at.title[0].value == "SharePoint Audit Dashboard" # Or whatever title you set

def test_permission_page_filtering():
    """Test that filtering on the permissions page works."""
    at = AppTest.from_file("src/dashboard/streamlit_app.py").run()

    # Navigate to the permissions page and apply a filter
    at.sidebar.radio[0].select("Permissions").run()
    at.selectbox[0].select("Site A").run()

    # Verify the displayed data in the dataframe is filtered
    # This requires mocking the data loading function to return different
    # data based on the filter.
    df = at.dataframe[0].value
    # assert all(df['site'] == 'Site A')
```

## Verification & Validation

Manual testing is crucial for dashboards.

```bash
# 1. Ensure you have a populated audit.db file.
# 2. Launch the dashboard from the command line.
sharepoint-audit dashboard --db-path audit.db
# OR directly:
# streamlit run src/dashboard/streamlit_app.py

# 3. Manually test all pages and features in a web browser:
#    - Apply different filters on the Permissions and Files pages.
#    - Interact with the charts (hover, zoom).
#    - Use the export feature and verify the downloaded file.
#    - Resize the browser window to check for responsiveness.
```

## Done Criteria

- [ ] The dashboard application launches without errors.
- [ ] All pages load and display data correctly from a test database.
- [ ] Interactive filters and charts are responsive and update correctly.
- [ ] The data export functionality produces a valid Excel/CSV file.
- [ ] The dashboard performance is acceptable, with page load times under 3 seconds for cached data.
