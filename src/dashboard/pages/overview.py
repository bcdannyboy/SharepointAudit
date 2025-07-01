import asyncio
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from src.database.repository import DatabaseRepository


@st.cache_data(ttl=300)
def load_summary_data(db_path: str) -> dict:
    """Load summary statistics from the database."""

    async def _load():
        repo = DatabaseRepository(db_path)

        # Get basic counts
        sites_summary = await repo.get_sites_summary()
        permissions_summary = await repo.get_permissions_summary()

        # Get latest audit run info
        audit_query = """
        SELECT run_id, status, started_at, completed_at, error_details
        FROM audit_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
        latest_audit = await repo.fetch_one(audit_query)

        # Get file statistics
        file_stats_query = """
        SELECT
            COUNT(*) as total_files,
            SUM(size_bytes) as total_size,
            AVG(size_bytes) as avg_size,
            MAX(size_bytes) as max_size,
            COUNT(DISTINCT content_type) as file_types
        FROM files
        """
        file_stats = await repo.fetch_one(file_stats_query)

        # Get external sharing stats
        external_query = """
        SELECT COUNT(DISTINCT principal_id) as external_users
        FROM permissions
        WHERE principal_name LIKE '%#ext#%' OR principal_name LIKE '%(External)%'
        """
        external_stats = await repo.fetch_one(external_query)

        return {
            "sites": sites_summary,
            "permissions": permissions_summary,
            "latest_audit": latest_audit,
            "file_stats": file_stats,
            "external_users": (
                external_stats.get("external_users", 0) if external_stats else 0
            ),
        }

    return asyncio.run(_load())


@st.cache_data(ttl=300)
def load_permission_distribution(db_path: str) -> pd.DataFrame:
    """Load permission level distribution."""

    async def _load():
        repo = DatabaseRepository(db_path)
        query = """
        SELECT permission_level, COUNT(*) as count
        FROM permissions
        GROUP BY permission_level
        ORDER BY count DESC
        """
        return await repo.fetch_all(query)

    data = asyncio.run(_load())
    return pd.DataFrame(data)


@st.cache_data(ttl=300)
def load_storage_by_site_type(db_path: str) -> pd.DataFrame:
    """Load storage usage by site type."""

    async def _load():
        repo = DatabaseRepository(db_path)
        # Since storage_used might be empty, calculate from files instead
        query = """
        SELECT
            CASE
                WHEN s.is_hub_site THEN 'Hub Site'
                ELSE 'Regular Site'
            END as site_type,
            COUNT(DISTINCT s.site_id) as count,
            COALESCE(SUM(f.size_bytes), 0) as total_storage,
            COALESCE(AVG(f.size_bytes), 0) as avg_storage
        FROM sites s
        LEFT JOIN libraries l ON s.site_id = l.site_id
        LEFT JOIN files f ON l.library_id = f.library_id
        GROUP BY s.is_hub_site
        """
        return await repo.fetch_all(query)

    data = asyncio.run(_load())
    return pd.DataFrame(data)


@st.cache_data(ttl=300)
def load_recent_activity(db_path: str) -> pd.DataFrame:
    """Load recent file modifications."""

    async def _load():
        repo = DatabaseRepository(db_path)
        query = """
        SELECT
            f.name,
            f.modified_at,
            f.modified_by,
            f.size_bytes,
            s.title as site_title
        FROM files f
        JOIN libraries l ON f.library_id = l.library_id
        JOIN sites s ON l.site_id = s.site_id
        WHERE f.modified_at IS NOT NULL
        ORDER BY f.modified_at DESC
        LIMIT 10
        """
        return await repo.fetch_all(query)

    data = asyncio.run(_load())
    return pd.DataFrame(data)


def render(db_path: str) -> None:
    """Render the dashboard overview page."""
    st.title("📊 SharePoint Audit Overview")

    # Load summary data
    try:
        summary = load_summary_data(db_path)
    except Exception as e:
        st.error(f"Error loading summary data: {str(e)}")
        st.info("Make sure you have run an audit and the database contains data.")
        return

    # Display latest audit info
    if summary["latest_audit"]:
        audit = summary["latest_audit"]
        status_color = "green" if audit["status"] == "completed" else "red"
        st.markdown(
            f"**Latest Audit:** {audit['run_id']} - "
            f"Status: :{status_color}[{audit['status']}] - "
            f"Started: {audit['started_at']}"
        )
        if audit.get("error_details"):
            st.error(f"Error: {audit['error_details']}")

    # Key metrics
    st.subheader("Key Metrics")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        total_sites = summary['sites'].get('total_sites', 0)
        st.metric("Total Sites", f"{total_sites:,}")

    with col2:
        total_libraries = summary['sites'].get('total_libraries', 0)
        st.metric("Total Libraries", f"{total_libraries:,}")

    with col3:
        total_files = summary['sites'].get('total_files', 0)
        st.metric("Total Files", f"{total_files:,}")

    with col4:
        total_bytes = summary["sites"].get("total_size_bytes") or 0
        total_gb = total_bytes / (1024**3) if total_bytes else 0
        st.metric("Total Storage", f"{total_gb:.2f} GB")

    with col5:
        external_users = summary.get('external_users', 0)
        st.metric("External Users", f"{external_users:,}")

    # Permission insights
    st.subheader("Permission Insights")
    col1, col2, col3 = st.columns(3)

    with col1:
        total_permissions = summary['permissions'].get('total_permissions', 0)
        st.metric(
            "Total Permissions", f"{total_permissions:,}"
        )

    with col2:
        unique_permissions = summary['permissions'].get('unique_permissions', 0)
        st.metric(
            "Unique Permissions", f"{unique_permissions:,}"
        )

    with col3:
        total_perms = summary["permissions"].get("total_permissions", 0)
        unique_perms = summary["permissions"].get("unique_permissions", 0)
        unique_pct = (
            (unique_perms / total_perms * 100)
            if total_perms > 0
            else 0
        )
        st.metric("Unique %", f"{unique_pct:.1f}%")

    # Visualizations
    col1, col2 = st.columns(2)

    with col1:
        # Permission distribution pie chart
        perm_df = load_permission_distribution(db_path)
        if not perm_df.empty:
            fig_pie = px.pie(
                perm_df,
                values="count",
                names="permission_level",
                title="Permission Level Distribution",
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # Storage by site type
        storage_df = load_storage_by_site_type(db_path)
        if not storage_df.empty:
            fig_bar = px.bar(
                storage_df,
                x="site_type",
                y="total_storage",
                title="Storage Usage by Site Type",
                labels={
                    "total_storage": "Total Storage (bytes)",
                    "site_type": "Site Type",
                },
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # File statistics
    st.subheader("File Statistics")
    if summary["file_stats"]:
        stats = summary["file_stats"]
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            avg_size = stats.get("avg_size") or 0
            avg_mb = avg_size / (1024**2) if avg_size else 0
            st.metric("Avg File Size", f"{avg_mb:.2f} MB")

        with col2:
            max_size = stats.get("max_size") or 0
            max_mb = max_size / (1024**2) if max_size else 0
            st.metric("Largest File", f"{max_mb:.2f} MB")

        with col3:
            file_types = stats.get('file_types', 0)
            st.metric("File Types", f"{file_types}")

        with col4:
            total_files = stats.get("total_files", 0)
            st.metric("Total Files", f"{total_files:,}")

    # Recent activity
    st.subheader("Recent File Activity")
    recent_df = load_recent_activity(db_path)

    if not recent_df.empty:
        # Format the dataframe for display
        recent_df["size_mb"] = recent_df["size_bytes"] / (1024**2)
        recent_df["modified_at"] = pd.to_datetime(recent_df["modified_at"])

        st.dataframe(
            recent_df[["name", "site_title", "modified_by", "modified_at", "size_mb"]],
            use_container_width=True,
            hide_index=True,
            column_config={
                "name": st.column_config.TextColumn("File Name"),
                "site_title": st.column_config.TextColumn("Site"),
                "modified_by": st.column_config.TextColumn("Modified By"),
                "modified_at": st.column_config.DatetimeColumn("Modified At"),
                "size_mb": st.column_config.NumberColumn("Size (MB)", format="%.2f"),
            },
        )
    else:
        st.info("No recent file activity found.")

    # Quick insights
    st.subheader("Quick Insights")

    # Calculate some insights
    insights = []

    if summary["permissions"].get("unique_permissions", 0) > 100:
        insights.append(
            "⚠️ High number of unique permissions detected. Consider reviewing permission inheritance."
        )

    if summary.get("external_users", 0) > 0:
        insights.append(
            f"👥 {summary['external_users']} external users have access to your SharePoint content."
        )

    if summary["sites"].get("total_sites", 0) > 0 and summary["sites"].get("total_size_bytes"):
        avg_storage_per_site = (
            summary["sites"]["total_size_bytes"]
            / summary["sites"]["total_sites"]
            / (1024**3)
        )
        insights.append(f"💾 Average storage per site: {avg_storage_per_site:.2f} GB")

    if insights:
        for insight in insights:
            st.info(insight)
    else:
        st.success("✅ No immediate concerns identified.")
