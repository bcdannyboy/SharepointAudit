import asyncio
from typing import Optional
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from ...database.repository import DatabaseRepository


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
            COUNT(DISTINCT l.id) as library_count,
            COUNT(DISTINCT f.id) as file_count,
            COALESCE(SUM(f.size_bytes), 0) as total_file_size
        FROM sites s
        LEFT JOIN libraries l ON s.id = l.site_id
        LEFT JOIN files f ON l.id = f.library_id
        GROUP BY s.id
        ORDER BY s.storage_used DESC
        """
        return await repo.fetch_all(query)

    data = asyncio.run(_load())
    df = pd.DataFrame(data)

    # Convert storage to GB for better readability
    if not df.empty:
        df["storage_used_gb"] = (
            df["storage_used"] / (1024**3) if "storage_used" in df else 0
        )
        df["storage_quota_gb"] = (
            df["storage_quota"] / (1024**3) if "storage_quota" in df else 0
        )
        df["total_file_size_gb"] = (
            df["total_file_size"] / (1024**3) if "total_file_size" in df else 0
        )
        df["storage_usage_pct"] = (
            df["storage_used"] / df["storage_quota"] * 100
        ).fillna(0)

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
        total_storage_gb = sites_df["storage_used_gb"].sum()
        st.metric("Total Storage Used", f"{total_storage_gb:.2f} GB")

    with col3:
        avg_files_per_site = sites_df["file_count"].mean()
        st.metric("Avg Files per Site", f"{avg_files_per_site:.0f}")

    with col4:
        hub_sites = sites_df["is_hub_site"].sum() if "is_hub_site" in sites_df else 0
        st.metric("Hub Sites", hub_sites)

    # Storage visualization
    st.subheader("Storage Distribution")

    col1, col2 = st.columns(2)

    with col1:
        # Top 10 sites by storage
        top_sites = sites_df.nlargest(10, "storage_used_gb")
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

    with col2:
        # Storage usage distribution
        fig_pie = px.pie(
            sites_df.nlargest(10, "storage_used_gb"),
            values="storage_used_gb",
            names="title",
            title="Storage Distribution (Top 10 Sites)",
        )
        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

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

    st.dataframe(
        filtered_df[display_columns].sort_values("storage_used_gb", ascending=False),
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

    # Storage trends (if we have historical data)
    st.subheader("Storage Analytics")

    if not storage_df.empty:
        # Create a scatter plot of sites by file count vs storage
        fig_scatter = px.scatter(
            storage_df,
            x="file_count",
            y="total_size_bytes",
            size="library_count",
            hover_data=["site_title", "avg_file_size"],
            title="Sites: File Count vs Storage Usage",
            labels={
                "file_count": "Number of Files",
                "total_size_bytes": "Total Storage (bytes)",
                "library_count": "Number of Libraries",
            },
        )
        fig_scatter.update_layout(height=500)
        st.plotly_chart(fig_scatter, use_container_width=True)
