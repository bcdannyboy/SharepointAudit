import asyncio
from typing import Optional
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

# Add parent directory to path for imports
parent_dir = Path(__file__).parent.parent.parent.parent
if str(parent_dir) not in sys.path:
    sys.path.insert(0, str(parent_dir))

from src.database.repository import DatabaseRepository


@st.cache_data(ttl=300)
def load_sites_data(db_path: str) -> pd.DataFrame:
    """Load sites data from the database."""

    async def _load():
        repo = DatabaseRepository(db_path)
        query = """
        SELECT
            s.site_id,
            s.title,
            s.url,
            s.created_at,
            s.last_modified,
            s.storage_used,
            s.storage_quota,
            s.is_hub_site,
            COUNT(DISTINCT l.library_id) as library_count,
            COUNT(DISTINCT f.file_id) as file_count,
            COALESCE(SUM(f.size_bytes), 0) as total_file_size
        FROM sites s
        LEFT JOIN libraries l ON s.site_id = l.site_id
        LEFT JOIN files f ON l.library_id = f.library_id
        GROUP BY s.site_id, s.title, s.url, s.created_at, s.last_modified,
                 s.storage_used, s.storage_quota, s.is_hub_site
        ORDER BY COALESCE(s.storage_used, 0) DESC
        """
        return await repo.fetch_all(query)

    data = asyncio.run(_load())
    df = pd.DataFrame(data)

    # Convert numeric columns and handle None/null values
    if not df.empty:
        # Convert count columns to numeric
        for col in ['library_count', 'file_count']:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        # Convert storage columns to numeric, handling None/null values
        if "storage_used" in df:
            df["storage_used"] = pd.to_numeric(df["storage_used"], errors='coerce').fillna(0)
            df["storage_used_gb"] = df["storage_used"] / (1024**3)
        else:
            df["storage_used_gb"] = 0

        if "storage_quota" in df:
            df["storage_quota"] = pd.to_numeric(df["storage_quota"], errors='coerce').fillna(0)
            df["storage_quota_gb"] = df["storage_quota"] / (1024**3)
        else:
            df["storage_quota_gb"] = 0

        if "total_file_size" in df:
            df["total_file_size"] = pd.to_numeric(df["total_file_size"], errors='coerce').fillna(0)
            df["total_file_size_gb"] = df["total_file_size"] / (1024**3)
        else:
            df["total_file_size_gb"] = 0

        # Calculate storage usage percentage
        if "storage_used" in df and "storage_quota" in df:
            # Avoid division by zero
            df["storage_usage_pct"] = df.apply(
                lambda row: (row["storage_used"] / row["storage_quota"] * 100)
                if row["storage_quota"] > 0 else 0,
                axis=1
            )
        else:
            df["storage_usage_pct"] = 0

    return df


@st.cache_data(ttl=300)
def load_storage_analytics(db_path: str) -> pd.DataFrame:
    """Load storage analytics from the view."""

    async def _load():
        repo = DatabaseRepository(db_path)
        return await repo.fetch_all("SELECT * FROM vw_storage_analytics")

    data = asyncio.run(_load())
    return pd.DataFrame(data)


def render(db_path: str):
    """Render the Sites page."""
    st.title("📁 Sites Analysis")

    # Load data
    try:
        sites_df = load_sites_data(db_path)
        storage_df = load_storage_analytics(db_path)
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return

    if sites_df.empty:
        st.warning("No sites data available. Please run an audit first.")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Sites", len(sites_df))

    with col2:
        if "storage_used_gb" in sites_df and not sites_df.empty:
            total_storage_gb = sites_df["storage_used_gb"].sum()
        else:
            total_storage_gb = 0
        st.metric("Total Storage Used", f"{total_storage_gb:.2f} GB")

    with col3:
        if "file_count" in sites_df and not sites_df.empty:
            avg_files_per_site = sites_df["file_count"].mean()
        else:
            avg_files_per_site = 0
        st.metric("Avg Files per Site", f"{avg_files_per_site:.0f}")

    with col4:
        hub_sites = sites_df["is_hub_site"].sum() if "is_hub_site" in sites_df else 0
        st.metric("Hub Sites", hub_sites)

    # Storage visualization
    st.subheader("Storage Distribution")

    col1, col2 = st.columns(2)

    with col1:
        # Top 10 sites by storage
        if "storage_used_gb" in sites_df and len(sites_df) > 0:
            # Ensure storage_used_gb is numeric
            sites_df["storage_used_gb"] = pd.to_numeric(sites_df["storage_used_gb"], errors='coerce').fillna(0)
            top_sites = sites_df.nlargest(min(10, len(sites_df)), "storage_used_gb")
        else:
            top_sites = pd.DataFrame()
        if not top_sites.empty:
            fig_bar = px.bar(
                top_sites,
                x="storage_used_gb",
                y="title",
                orientation="h",
                title="Top 10 Sites by Storage Usage",
                labels={"storage_used_gb": "Storage (GB)", "title": "Site"},
            )
            fig_bar.update_layout(height=400)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No storage data available for sites.")

    with col2:
        # Storage usage distribution
        if "storage_used_gb" in sites_df and len(sites_df) > 0:
            # Get top 10 sites for pie chart
            top_sites_pie = sites_df.nlargest(min(10, len(sites_df)), "storage_used_gb")
            if not top_sites_pie.empty and top_sites_pie["storage_used_gb"].sum() > 0:
                fig_pie = px.pie(
                    top_sites_pie,
                    values="storage_used_gb",
                    names="title",
                    title="Storage Distribution (Top 10 Sites)",
                )
                fig_pie.update_layout(height=400)
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No storage data to display in pie chart.")
        else:
            st.info("No storage data available.")

    # Sites table with search and filters
    st.subheader("Sites Details")

    # Search box
    search_term = st.text_input("Search sites by title or URL", "")

    # Filter options
    col1, col2, col3 = st.columns(3)

    with col1:
        show_hub_sites_only = st.checkbox("Show Hub Sites Only")

    with col2:
        min_storage = st.number_input("Min Storage (GB)", min_value=0.0, value=0.0)

    with col3:
        min_files = st.number_input("Min Files", min_value=0, value=0)

    # Apply filters
    filtered_df = sites_df.copy()

    if search_term:
        filtered_df = filtered_df[
            filtered_df["title"].str.contains(search_term, case=False, na=False)
            | filtered_df["url"].str.contains(search_term, case=False, na=False)
        ]

    if show_hub_sites_only and "is_hub_site" in filtered_df:
        filtered_df = filtered_df[filtered_df["is_hub_site"] == True]

    if min_storage > 0:
        filtered_df = filtered_df[filtered_df["storage_used_gb"] >= min_storage]

    if min_files > 0:
        filtered_df = filtered_df[filtered_df["file_count"] >= min_files]

    # Display table
    display_columns = [
        "title",
        "url",
        "storage_used_gb",
        "storage_usage_pct",
        "file_count",
        "library_count",
        "is_hub_site",
        "last_modified",
    ]

    # Only include columns that exist in the dataframe
    display_columns = [col for col in display_columns if col in filtered_df.columns]

    if display_columns and not filtered_df.empty:
        # Sort by storage if the column exists, otherwise just display
        if "storage_used_gb" in display_columns:
            filtered_df = filtered_df[display_columns].sort_values("storage_used_gb", ascending=False)
        else:
            filtered_df = filtered_df[display_columns]

        st.dataframe(
            filtered_df,
            use_container_width=True,
            hide_index=True,
        column_config={
            "title": st.column_config.TextColumn("Site Title"),
            "url": st.column_config.LinkColumn("URL"),
            "storage_used_gb": st.column_config.NumberColumn(
                "Storage (GB)", format="%.2f"
            ),
            "storage_usage_pct": st.column_config.ProgressColumn(
                "Usage %", min_value=0, max_value=100
            ),
            "file_count": st.column_config.NumberColumn("Files", format="%d"),
            "library_count": st.column_config.NumberColumn("Libraries", format="%d"),
            "is_hub_site": st.column_config.CheckboxColumn("Hub Site"),
            "last_modified": st.column_config.DatetimeColumn("Last Modified"),
        },
        )
        st.info(f"Showing {len(filtered_df)} of {len(sites_df)} sites")
    else:
        st.info("No sites match the current filters.")

    # Storage trends (if we have historical data)
    st.subheader("Storage Analytics")

    if not storage_df.empty:
        # Ensure numeric columns for scatter plot
        numeric_cols = ['file_count', 'total_size_bytes', 'library_count', 'avg_file_size']
        for col in numeric_cols:
            if col in storage_df:
                storage_df[col] = pd.to_numeric(storage_df[col], errors='coerce').fillna(0)

        # Create a scatter plot of sites by file count vs storage
        if all(col in storage_df for col in ['file_count', 'total_size_bytes', 'library_count']):
            fig_scatter = px.scatter(
                storage_df,
                x="file_count",
                y="total_size_bytes",
                size="library_count",
                hover_data=["site_title", "avg_file_size"] if "avg_file_size" in storage_df else ["site_title"],
                title="Sites: File Count vs Storage Usage",
                labels={
                    "file_count": "Number of Files",
                    "total_size_bytes": "Total Storage (bytes)",
                    "library_count": "Number of Libraries",
                },
            )
            fig_scatter.update_layout(height=500)
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("Insufficient data for storage analytics visualization.")
