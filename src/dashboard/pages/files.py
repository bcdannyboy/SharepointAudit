import asyncio
from datetime import datetime, timedelta
from typing import List
import streamlit as st
import pandas as pd
import plotly.express as px
from ...database.repository import DatabaseRepository


@st.cache_data(ttl=300)
def load_files_data(db_path: str, filters: dict) -> pd.DataFrame:
    """Load files data from the database with filters."""

    async def _load():
        repo = DatabaseRepository(db_path)

        query = """
        SELECT
            f.file_id,
            f.name,
            f.server_relative_url,
            f.size_bytes,
            f.content_type,
            f.created_at,
            f.created_by,
            f.modified_at,
            f.modified_by,
            f.version,
            f.is_checked_out,
            f.checked_out_by,
            f.has_unique_permissions,
            l.name as library_name,
            s.title as site_title,
            s.url as site_url
        FROM files f
        JOIN libraries l ON f.library_id = l.id
        JOIN sites s ON l.site_id = s.id
        WHERE 1=1
        """

        params = []

        if filters.get("site_id"):
            query += " AND s.title = ?"
            params.append(filters["site_id"])

        if filters.get("min_size_mb"):
            min_bytes = filters["min_size_mb"] * 1024 * 1024
            query += " AND f.size_bytes >= ?"
            params.append(min_bytes)

        if filters.get("max_size_mb"):
            max_bytes = filters["max_size_mb"] * 1024 * 1024
            query += " AND f.size_bytes <= ?"
            params.append(max_bytes)

        if filters.get("file_type"):
            query += " AND f.content_type LIKE ?"
            params.append(f"%{filters['file_type']}%")

        if filters.get("days_old"):
            cutoff_date = datetime.now() - timedelta(days=filters["days_old"])
            query += " AND f.created_at < ?"
            params.append(cutoff_date.isoformat())

        if filters.get("checked_out_only"):
            query += " AND f.is_checked_out = 1"

        if filters.get("unique_permissions_only"):
            query += " AND f.has_unique_permissions = 1"

        query += " ORDER BY f.size_bytes DESC"

        if filters.get("limit"):
            query += f" LIMIT {filters['limit']}"

        return await repo.fetch_all(query, tuple(params) if params else None)

    data = asyncio.run(_load())
    return pd.DataFrame(data)


@st.cache_data(ttl=300)
def load_file_statistics(db_path: str) -> dict:
    """Load file statistics from the database."""

    async def _load():
        repo = DatabaseRepository(db_path)

        # File type distribution
        type_query = """
        SELECT
            CASE
                WHEN content_type LIKE '%word%' OR name LIKE '%.docx' OR name LIKE '%.doc' THEN 'Word'
                WHEN content_type LIKE '%excel%' OR name LIKE '%.xlsx' OR name LIKE '%.xls' THEN 'Excel'
                WHEN content_type LIKE '%powerpoint%' OR name LIKE '%.pptx' OR name LIKE '%.ppt' THEN 'PowerPoint'
                WHEN content_type LIKE '%pdf%' OR name LIKE '%.pdf' THEN 'PDF'
                WHEN content_type LIKE '%image%' OR name LIKE '%.jpg' OR name LIKE '%.png' OR name LIKE '%.gif' THEN 'Image'
                WHEN content_type LIKE '%video%' OR name LIKE '%.mp4' OR name LIKE '%.avi' THEN 'Video'
                ELSE 'Other'
            END as file_category,
            COUNT(*) as count,
            SUM(size_bytes) as total_size
        FROM files
        GROUP BY file_category
        ORDER BY total_size DESC
        """

        # Age distribution
        age_query = """
        SELECT
            CASE
                WHEN julianday('now') - julianday(created_at) <= 30 THEN '0-30 days'
                WHEN julianday('now') - julianday(created_at) <= 90 THEN '31-90 days'
                WHEN julianday('now') - julianday(created_at) <= 180 THEN '91-180 days'
                WHEN julianday('now') - julianday(created_at) <= 365 THEN '181-365 days'
                ELSE 'Over 1 year'
            END as age_group,
            COUNT(*) as count,
            SUM(size_bytes) as total_size
        FROM files
        WHERE created_at IS NOT NULL
        GROUP BY age_group
        """

        # Size distribution
        size_query = """
        SELECT
            CASE
                WHEN size_bytes < 1048576 THEN '< 1 MB'
                WHEN size_bytes < 10485760 THEN '1-10 MB'
                WHEN size_bytes < 104857600 THEN '10-100 MB'
                WHEN size_bytes < 1073741824 THEN '100 MB - 1 GB'
                ELSE '> 1 GB'
            END as size_group,
            COUNT(*) as count
        FROM files
        GROUP BY size_group
        """

        type_dist = await repo.fetch_all(type_query)
        age_dist = await repo.fetch_all(age_query)
        size_dist = await repo.fetch_all(size_query)

        return {
            "type_distribution": type_dist,
            "age_distribution": age_dist,
            "size_distribution": size_dist,
        }

    return asyncio.run(_load())


@st.cache_data(ttl=300)
def load_large_files(db_path: str, top_n: int = 20) -> pd.DataFrame:
    """Load the largest files."""

    async def _load():
        repo = DatabaseRepository(db_path)
        query = """
        SELECT
            f.name,
            f.size_bytes,
            f.content_type,
            f.modified_at,
            f.modified_by,
            l.name as library_name,
            s.title as site_title
        FROM files f
        JOIN libraries l ON f.library_id = l.id
        JOIN sites s ON l.site_id = s.id
        ORDER BY f.size_bytes DESC
        LIMIT ?
        """
        return await repo.fetch_all(query, (top_n,))

    data = asyncio.run(_load())
    return pd.DataFrame(data)


def render(db_path: str) -> None:
    """Render files analysis page."""
    st.title("📄 Files Analysis")

    # Load statistics
    try:
        stats = load_file_statistics(db_path)
        sites = asyncio.run(async_get_sites(db_path))
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    total_files = sum(item["count"] for item in stats["type_distribution"])
    total_size = sum(item["total_size"] for item in stats["type_distribution"])

    with col1:
        st.metric("Total Files", f"{total_files:,}")

    with col2:
        st.metric("Total Size", f"{total_size / (1024**3):.2f} GB")

    with col3:
        avg_size = total_size / total_files if total_files > 0 else 0
        st.metric("Average File Size", f"{avg_size / (1024**2):.2f} MB")

    with col4:
        # Count of large files (> 100 MB)
        large_files = sum(
            item["count"]
            for item in stats["size_distribution"]
            if item["size_group"] in ["100 MB - 1 GB", "> 1 GB"]
        )
        st.metric("Large Files (>100MB)", f"{large_files:,}")

    # Visualizations
    st.subheader("File Analytics")

    col1, col2 = st.columns(2)

    with col1:
        # File type distribution
        if stats["type_distribution"]:
            type_df = pd.DataFrame(stats["type_distribution"])
            fig_pie = px.pie(
                type_df,
                values="total_size",
                names="file_category",
                title="Storage by File Type",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # Size distribution
        if stats["size_distribution"]:
            size_df = pd.DataFrame(stats["size_distribution"])
            # Define order for size groups
            size_order = ["< 1 MB", "1-10 MB", "10-100 MB", "100 MB - 1 GB", "> 1 GB"]
            size_df["size_group"] = pd.Categorical(
                size_df["size_group"], categories=size_order, ordered=True
            )
            size_df = size_df.sort_values("size_group")

            fig_bar = px.bar(
                size_df,
                x="size_group",
                y="count",
                title="File Count by Size Range",
                labels={"size_group": "Size Range", "count": "Number of Files"},
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # Age distribution
    if stats["age_distribution"]:
        age_df = pd.DataFrame(stats["age_distribution"])
        age_order = [
            "0-30 days",
            "31-90 days",
            "91-180 days",
            "181-365 days",
            "Over 1 year",
        ]
        age_df["age_group"] = pd.Categorical(
            age_df["age_group"], categories=age_order, ordered=True
        )
        age_df = age_df.sort_values("age_group")

        fig_age = px.bar(
            age_df,
            x="age_group",
            y="count",
            title="File Age Distribution",
            labels={"age_group": "Age Group", "count": "Number of Files"},
        )
        st.plotly_chart(fig_age, use_container_width=True)

    # File search and filters
    st.subheader("Find Files")

    with st.expander("Search Filters", expanded=True):
        col1, col2, col3 = st.columns(3)

        with col1:
            selected_site = st.selectbox("Site", ["All Sites"] + sites)
            min_size = st.number_input("Min Size (MB)", min_value=0, value=0)

        with col2:
            file_type = st.selectbox(
                "File Type",
                ["All Types", "Word", "Excel", "PowerPoint", "PDF", "Image", "Video"],
            )
            max_size = st.number_input("Max Size (MB)", min_value=0, value=0)

        with col3:
            days_old = st.number_input("Files older than (days)", min_value=0, value=0)
            show_checked_out = st.checkbox("Checked out files only")
            show_unique_perms = st.checkbox("Files with unique permissions")

    # Build filters
    filters = {}
    if selected_site != "All Sites":
        filters["site_id"] = selected_site
    if min_size > 0:
        filters["min_size_mb"] = min_size
    if max_size > 0:
        filters["max_size_mb"] = max_size
    if file_type != "All Types":
        filters["file_type"] = file_type.lower()
    if days_old > 0:
        filters["days_old"] = days_old
    if show_checked_out:
        filters["checked_out_only"] = True
    if show_unique_perms:
        filters["unique_permissions_only"] = True

    filters["limit"] = 1000  # Limit results for performance

    # Load filtered files
    if st.button("Search Files"):
        with st.spinner("Searching..."):
            files_df = load_files_data(db_path, filters)

            if files_df.empty:
                st.warning("No files found matching the criteria.")
            else:
                st.success(f"Found {len(files_df)} files")

                # Add calculated columns
                files_df["size_mb"] = files_df["size_bytes"] / (1024**2)
                files_df["modified_date"] = pd.to_datetime(files_df["modified_at"])

                # Display results
                display_cols = [
                    "name",
                    "site_title",
                    "library_name",
                    "size_mb",
                    "content_type",
                    "modified_by",
                    "modified_date",
                ]

                if show_checked_out:
                    display_cols.extend(["is_checked_out", "checked_out_by"])
                if show_unique_perms:
                    display_cols.append("has_unique_permissions")

                st.dataframe(
                    files_df[display_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "name": st.column_config.TextColumn("File Name"),
                        "site_title": st.column_config.TextColumn("Site"),
                        "library_name": st.column_config.TextColumn("Library"),
                        "size_mb": st.column_config.NumberColumn(
                            "Size (MB)", format="%.2f"
                        ),
                        "content_type": st.column_config.TextColumn("Type"),
                        "modified_by": st.column_config.TextColumn("Modified By"),
                        "modified_date": st.column_config.DatetimeColumn(
                            "Modified Date"
                        ),
                        "is_checked_out": st.column_config.CheckboxColumn(
                            "Checked Out"
                        ),
                        "checked_out_by": st.column_config.TextColumn("Checked Out By"),
                        "has_unique_permissions": st.column_config.CheckboxColumn(
                            "Unique Perms"
                        ),
                    },
                )

    # Large files analysis
    st.subheader("Largest Files")

    large_files_df = load_large_files(db_path, 20)

    if not large_files_df.empty:
        large_files_df["size_mb"] = large_files_df["size_bytes"] / (1024**2)

        # Create bar chart of largest files
        fig_large = px.bar(
            large_files_df.head(10),
            x="size_mb",
            y="name",
            orientation="h",
            title="Top 10 Largest Files",
            labels={"size_mb": "Size (MB)", "name": "File Name"},
            hover_data=["site_title", "library_name"],
        )
        fig_large.update_layout(height=400)
        st.plotly_chart(fig_large, use_container_width=True)

        # Table of large files
        st.write("**All Large Files:**")
        st.dataframe(
            large_files_df[
                ["name", "site_title", "size_mb", "content_type", "modified_by"]
            ],
            use_container_width=True,
            hide_index=True,
            column_config={
                "name": st.column_config.TextColumn("File Name"),
                "site_title": st.column_config.TextColumn("Site"),
                "size_mb": st.column_config.NumberColumn("Size (MB)", format="%.2f"),
                "content_type": st.column_config.TextColumn("Type"),
                "modified_by": st.column_config.TextColumn("Modified By"),
            },
        )


async def async_get_sites(db_path: str) -> List[str]:
    """Get list of sites asynchronously."""
    repo = DatabaseRepository(db_path)
    query = "SELECT DISTINCT title FROM sites ORDER BY title"
    result = await repo.fetch_all(query)
    return [r["title"] for r in result]
