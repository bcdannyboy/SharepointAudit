"""
SharePoint Security Audit Dashboard
Main application that orchestrates all dashboard components
"""

import streamlit as st
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path
parent_dir = Path(__file__).parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

# Import components
from src.dashboard.components.overview import OverviewComponent
from src.dashboard.components.sensitivity import SensitivityComponent
from src.dashboard.components.external_access import ExternalAccessComponent
from src.dashboard.components.risk_analysis import RiskAnalysisComponent
from src.dashboard.components.sites import SitesComponent
from src.dashboard.components.files import FilesComponent
from src.dashboard.components.permissions import PermissionsComponent

# Page configuration
st.set_page_config(
    page_title="SharePoint Security Audit Dashboard",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced CSS
st.markdown("""
<style>
    /* Main content area */
    .main > div {
        padding-top: 1rem;
    }

    /* Metric cards */
    div[data-testid="metric-container"] {
        background-color: #f0f2f6;
        border: 1px solid #e0e0e0;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }

    /* Headers */
    h1, h2, h3 {
        color: #1f2937;
    }

    /* Sidebar */
    .css-1d391kg {
        background-color: #f8f9fa;
    }

    /* Data tables */
    .dataframe {
        font-size: 14px;
    }

    /* Alert boxes */
    .stAlert {
        padding: 12px;
        border-radius: 8px;
    }

    /* Custom risk levels */
    .risk-critical {
        background-color: #fee2e2 !important;
        color: #991b1b;
    }

    .risk-high {
        background-color: #fef3c7 !important;
        color: #dc2626;
    }

    .risk-medium {
        background-color: #fde68a !important;
        color: #f59e0b;
    }

    .risk-low {
        background-color: #d1fae5 !important;
        color: #10b981;
    }

    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #f3f4f6;
        border-radius: 8px;
    }

    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: #f3f4f6;
        border-radius: 8px 8px 0 0;
    }

    .stTabs [data-baseweb="tab"]:hover {
        background-color: #e5e7eb;
    }

    .stTabs [aria-selected="true"] {
        background-color: #3b82f6 !important;
        color: white !important;
    }
</style>
""", unsafe_allow_html=True)


class SecurityAuditDashboard:
    """Main dashboard application"""

    def __init__(self, db_path: str):
        self.db_path = db_path

        # Initialize components
        self.overview = OverviewComponent(db_path)
        self.sensitivity = SensitivityComponent(db_path)
        self.external_access = ExternalAccessComponent(db_path)
        self.risk_analysis = RiskAnalysisComponent(db_path)
        self.sites = SitesComponent(db_path)
        self.files = FilesComponent(db_path)
        self.permissions = PermissionsComponent(db_path)

    def run(self):
        """Run the dashboard application"""
        # Sidebar
        with st.sidebar:
            st.title("🔒 Security Audit")

            # Database info with better styling
            if Path(self.db_path).exists():
                db_size = Path(self.db_path).stat().st_size
                st.metric(
                    label="Database Size",
                    value=self._format_bytes(db_size),
                    help="Current audit database size"
                )

            st.markdown("---")

            # Navigation
            st.subheader("📊 Navigation")

            # Single logical navigation list
            page = st.radio(
                "Select view",
                [
                    "🏠 Overview",
                    "🏢 Sites",
                    "📁 Files",
                    "🔑 Permissions",
                    "🔍 Sensitivity Analysis",
                    "🌐 External Access",
                    "⚠️ Risk Analysis"
                ],
                key="main_nav",
                label_visibility="collapsed"
            )

            # Tools Section
            st.markdown("---")
            st.subheader("🛠️ Tools")

            # Refresh button with better styling
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔄 Refresh", use_container_width=True):
                    st.cache_data.clear()
                    st.rerun()

            with col2:
                if st.button("📥 Export", use_container_width=True):
                    st.session_state['show_export'] = True

            # Export dialog
            if st.session_state.get('show_export', False):
                with st.container():
                    st.markdown("#### Export Report")
                    export_type = st.selectbox(
                        "Select report type",
                        [
                            "Executive Summary",
                            "Full Security Report",
                            "Sensitivity Report",
                            "External Access Report",
                            "Risk Assessment",
                            "Custom Export"
                        ],
                        key="export_type"
                    )

                    # Format options
                    export_format = st.radio(
                        "Format",
                        ["PDF", "Excel", "CSV", "JSON"],
                        horizontal=True,
                        key="export_format"
                    )

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("Generate", type="primary", use_container_width=True):
                            st.info(f"Generating {export_type} as {export_format}...")
                            # TODO: Implement report generation

                    with col2:
                        if st.button("Cancel", use_container_width=True):
                            st.session_state['show_export'] = False
                            st.rerun()

            # Info Section
            st.markdown("---")
            with st.expander("ℹ️ Info", expanded=False):
                st.markdown(
                    f"""
                    **Last Updated**
                    {datetime.now().strftime('%Y-%m-%d %H:%M')}

                    **Database Path**
                    `{self.db_path}`

                    **Version**
                    v1.0.0
                    """
                )

        # Main content area
        if page == "🏠 Overview":
            self.overview.render()
        elif page == "🔍 Sensitivity Analysis":
            self.sensitivity.render()
        elif page == "🌐 External Access":
            self.external_access.render()
        elif page == "⚠️ Risk Analysis":
            self.risk_analysis.render()
        elif page == "🏢 Sites":
            self.sites.render()
        elif page == "📁 Files":
            self.files.render()
        elif page == "🔑 Permissions":
            self.permissions.render()

    def _format_bytes(self, size: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"


def check_and_apply_migrations(db_path: str):
    """Check if migrations need to be applied and apply them"""
    import sqlite3

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Check if sensitivity columns exist
        cursor.execute("PRAGMA table_info(files)")
        columns = [row[1] for row in cursor.fetchall()]

        if 'sensitivity_score' not in columns:
            st.info("🔄 Applying database migrations for sensitivity analysis...")

            # Apply migration
            migration_path = Path(__file__).parent.parent / "database" / "migrations" / "add_sensitivity_columns.py"
            if migration_path.exists():
                import subprocess
                result = subprocess.run(
                    [sys.executable, str(migration_path), db_path],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    st.success("✅ Database migration completed successfully!")
                else:
                    st.error(f"❌ Migration failed: {result.stderr}")
                    return False
            else:
                # Apply migration inline if script not found
                try:
                    cursor.execute("ALTER TABLE files ADD COLUMN sensitivity_score INTEGER DEFAULT 0;")
                    cursor.execute("ALTER TABLE files ADD COLUMN sensitivity_level TEXT DEFAULT 'LOW';")
                    cursor.execute("ALTER TABLE files ADD COLUMN sensitivity_categories TEXT;")
                    cursor.execute("ALTER TABLE files ADD COLUMN sensitivity_factors TEXT;")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_sensitivity ON files (sensitivity_score DESC);")
                    conn.commit()
                    st.success("✅ Database migration applied successfully!")
                except Exception as e:
                    st.error(f"❌ Failed to apply migration: {e}")
                    return False

        return True

    finally:
        conn.close()


def main():
    """Main entry point"""
    # Get database path from command line or use default
    db_path = "audit.db"

    if len(sys.argv) > 1:
        if "--db-path" in sys.argv:
            idx = sys.argv.index("--db-path")
            if idx + 1 < len(sys.argv):
                db_path = sys.argv[idx + 1]
        else:
            db_path = sys.argv[1]

    # Check if database exists
    if not Path(db_path).exists():
        st.error(f"❌ Database not found: {db_path}")
        st.info("Please run an audit first or specify a valid database path")
        st.code("sharepoint-audit audit --config config/config.json")
        return

    # Check and apply migrations
    if not check_and_apply_migrations(db_path):
        st.error("Failed to prepare database. Please check the logs.")
        return

    # Run dashboard
    dashboard = SecurityAuditDashboard(db_path)
    dashboard.run()


if __name__ == "__main__":
    main()
