import asyncio
from typing import List, Optional
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

from database.repository import DatabaseRepository


@st.cache_data(ttl=300)
def load_permission_data(db_path: str, filters: dict) -> pd.DataFrame:
    """Load permission data from the database with filters."""

    async def _load():
        repo = DatabaseRepository(db_path)

        # Build query with filters
        query = """
        SELECT
            p.object_type,
            p.object_id,
            p.principal_type,
            p.principal_name,
            p.permission_level,
            p.is_inherited,
            CASE
                WHEN p.object_type = 'site' THEN s.title
                WHEN p.object_type = 'library' THEN l.name
                WHEN p.object_type = 'folder' THEN fo.name
                WHEN p.object_type = 'file' THEN fi.name
            END as object_name,
            CASE
                WHEN p.object_type = 'site' THEN s.url
                WHEN p.object_type = 'library' THEN
                    (SELECT url FROM sites WHERE id = l.site_id) || '/' || l.name
                WHEN p.object_type = 'folder' THEN fo.server_relative_url
                WHEN p.object_type = 'file' THEN fi.server_relative_url
            END as object_path,
            CASE
                WHEN p.object_type IN ('library', 'folder', 'file') THEN
                    (SELECT title FROM sites WHERE id = COALESCE(l.site_id,
                        (SELECT site_id FROM libraries WHERE id = COALESCE(fo.library_id, fi.library_id))))
                WHEN p.object_type = 'site' THEN s.title
            END as site_title
        FROM permissions p
        LEFT JOIN sites s ON p.object_type = 'site' AND p.object_id = s.site_id
        LEFT JOIN libraries l ON p.object_type = 'library' AND p.object_id = l.library_id
        LEFT JOIN folders fo ON p.object_type = 'folder' AND p.object_id = fo.folder_id
        LEFT JOIN files fi ON p.object_type = 'file' AND p.object_id = fi.file_id
        WHERE 1=1
        """

        params = []

        if filters.get("site_id"):
            query += " AND site_title = ?"
            params.append(filters["site_id"])

        if filters.get("permission_level"):
            query += " AND p.permission_level = ?"
            params.append(filters["permission_level"])

        if filters.get("principal_type"):
            query += " AND p.principal_type = ?"
            params.append(filters["principal_type"])

        if filters.get("show_unique_only"):
            query += " AND p.is_inherited = 0"

        if filters.get("external_only"):
            query += " AND (p.principal_name LIKE '%#ext#%' OR p.principal_name LIKE '%(External)%')"

        query += " ORDER BY site_title, object_type, object_name"

        return await repo.fetch_all(query, tuple(params) if params else None)

    data = asyncio.run(_load())
    return pd.DataFrame(data)


@st.cache_data(ttl=300)
def load_sites_list(db_path: str) -> List[str]:
    """Load list of available sites."""

    async def _load():
        repo = DatabaseRepository(db_path)
        query = "SELECT DISTINCT title FROM sites ORDER BY title"
        result = await repo.fetch_all(query)
        return [r["title"] for r in result]

    return asyncio.run(_load())


@st.cache_data(ttl=300)
def load_permission_levels(db_path: str) -> List[str]:
    """Load list of permission levels."""

    async def _load():
        repo = DatabaseRepository(db_path)
        query = "SELECT DISTINCT permission_level FROM permissions ORDER BY permission_level"
        result = await repo.fetch_all(query)
        return [r["permission_level"] for r in result]

    return asyncio.run(_load())


@st.cache_data(ttl=300)
def load_permission_matrix(
    db_path: str, site_filter: Optional[str] = None
) -> pd.DataFrame:
    """Load permission matrix for visualization."""

    async def _load():
        repo = DatabaseRepository(db_path)
        query = """
        SELECT
            p.principal_name,
            p.permission_level,
            COUNT(*) as count,
            SUM(CASE WHEN p.is_inherited = 0 THEN 1 ELSE 0 END) as unique_count
        FROM permissions p
        """

        if site_filter:
            query += """
            JOIN (
                SELECT site_id as object_id, 'site' as object_type, title
                FROM sites
                WHERE title = ?
                UNION ALL
                SELECT l.library_id, 'library', s.title
                FROM libraries l
                JOIN sites s ON l.site_id = s.id
                WHERE s.title = ?
                UNION ALL
                SELECT f.folder_id, 'folder', s.title
                FROM folders f
                JOIN libraries l ON f.library_id = l.id
                JOIN sites s ON l.site_id = s.id
                WHERE s.title = ?
                UNION ALL
                SELECT fi.file_id, 'file', s.title
                FROM files fi
                JOIN libraries l ON fi.library_id = l.id
                JOIN sites s ON l.site_id = s.id
                WHERE s.title = ?
            ) site_objects ON p.object_id = site_objects.object_id
                          AND p.object_type = site_objects.object_type
            """
            params = (site_filter, site_filter, site_filter, site_filter)
        else:
            params = None

        query += """
        GROUP BY p.principal_name, p.permission_level
        ORDER BY count DESC
        """

        return await repo.fetch_all(query, params)

    data = asyncio.run(_load())
    if not data:
        return pd.DataFrame()

    # Pivot the data for matrix visualization
    df = pd.DataFrame(data)
    matrix = df.pivot_table(
        index="principal_name", columns="permission_level", values="count", fill_value=0
    )
    return matrix


def render(db_path: str) -> None:
    """Render permissions analysis page."""
    st.title("🔐 Permission Analysis")

    # Load filter options
    try:
        sites = load_sites_list(db_path)
        permission_levels = load_permission_levels(db_path)
    except Exception as e:
        st.error(f"Error loading filter data: {str(e)}")
        return

    # Filters in sidebar
    st.sidebar.subheader("Filters")

    selected_site = st.sidebar.selectbox(
        "Site", ["All Sites"] + sites, key="site_filter"
    )

    selected_permission = st.sidebar.selectbox(
        "Permission Level", ["All Levels"] + permission_levels, key="permission_filter"
    )

    principal_type = st.sidebar.selectbox(
        "Principal Type", ["All", "User", "Group"], key="principal_filter"
    )

    show_unique_only = st.sidebar.checkbox(
        "Show Unique Permissions Only", key="unique_filter"
    )
    show_external_only = st.sidebar.checkbox(
        "Show External Users Only", key="external_filter"
    )

    # Build filters dict
    filters = {}
    if selected_site != "All Sites":
        filters["site_id"] = selected_site
    if selected_permission != "All Levels":
        filters["permission_level"] = selected_permission
    if principal_type != "All":
        filters["principal_type"] = principal_type
    filters["show_unique_only"] = show_unique_only
    filters["external_only"] = show_external_only

    # Load permission data
    try:
        perm_df = load_permission_data(db_path, filters)
    except Exception as e:
        st.error(f"Error loading permission data: {str(e)}")
        return

    if perm_df.empty:
        st.warning("No permissions found matching the selected filters.")
        return

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_perms = len(perm_df)
        st.metric("Total Permissions", f"{total_perms:,}")

    with col2:
        unique_perms = (
            len(perm_df[perm_df["is_inherited"] == False])
            if "is_inherited" in perm_df
            else 0
        )
        st.metric("Unique Permissions", f"{unique_perms:,}")

    with col3:
        unique_principals = perm_df["principal_name"].nunique()
        st.metric("Unique Principals", f"{unique_principals:,}")

    with col4:
        external_count = len(
            perm_df[perm_df["principal_name"].str.contains("#ext#|External", na=False)]
        )
        st.metric("External Users", f"{external_count:,}")

    # Visualizations
    st.subheader("Permission Distribution")

    col1, col2 = st.columns(2)

    with col1:
        # Permission levels distribution
        perm_dist = perm_df["permission_level"].value_counts()
        fig_pie = px.pie(
            values=perm_dist.values,
            names=perm_dist.index,
            title="Distribution by Permission Level",
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        # Object type distribution
        if "object_type" in perm_df:
            obj_dist = perm_df["object_type"].value_counts()
            fig_bar = px.bar(
                x=obj_dist.index,
                y=obj_dist.values,
                title="Permissions by Object Type",
                labels={"x": "Object Type", "y": "Count"},
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    # Permission Matrix Heatmap
    st.subheader("Permission Matrix")

    matrix_site = selected_site if selected_site != "All Sites" else None
    matrix_df = load_permission_matrix(db_path, matrix_site)

    if not matrix_df.empty:
        # Create heatmap
        fig_heatmap = go.Figure(
            data=go.Heatmap(
                z=matrix_df.values,
                x=matrix_df.columns,
                y=matrix_df.index,
                colorscale="Blues",
                hoverongaps=False,
                hovertemplate="Principal: %{y}<br>Permission: %{x}<br>Count: %{z}<extra></extra>",
            )
        )

        fig_heatmap.update_layout(
            title="Permission Matrix (Principal vs Permission Level)",
            xaxis_title="Permission Level",
            yaxis_title="Principal",
            height=min(600, 100 + len(matrix_df) * 20),
        )

        st.plotly_chart(fig_heatmap, use_container_width=True)

    # Detailed permissions table
    st.subheader("Permission Details")

    # Add search
    search_term = st.text_input("Search permissions", "")

    if search_term:
        mask = (
            perm_df["object_name"].str.contains(search_term, case=False, na=False)
            | perm_df["principal_name"].str.contains(search_term, case=False, na=False)
            | perm_df["object_path"].str.contains(search_term, case=False, na=False)
        )
        filtered_df = perm_df[mask]
    else:
        filtered_df = perm_df

    # Display table
    display_columns = [
        "object_type",
        "object_name",
        "principal_type",
        "principal_name",
        "permission_level",
        "is_inherited",
        "site_title",
    ]

    # Only include columns that exist
    display_columns = [col for col in display_columns if col in filtered_df.columns]

    st.dataframe(
        filtered_df[display_columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            "object_type": st.column_config.TextColumn("Type"),
            "object_name": st.column_config.TextColumn("Object"),
            "principal_type": st.column_config.TextColumn("Principal Type"),
            "principal_name": st.column_config.TextColumn("Principal"),
            "permission_level": st.column_config.TextColumn("Permission"),
            "is_inherited": st.column_config.CheckboxColumn("Inherited"),
            "site_title": st.column_config.TextColumn("Site"),
        },
    )

    st.info(f"Showing {len(filtered_df)} of {len(perm_df)} permissions")

    # External users analysis
    if external_count > 0:
        st.subheader("External User Analysis")

        external_df = perm_df[
            perm_df["principal_name"].str.contains("#ext#|External", na=False)
        ]

        # Group by permission level
        ext_by_perm = external_df["permission_level"].value_counts()

        fig_ext = px.bar(
            x=ext_by_perm.index,
            y=ext_by_perm.values,
            title="External User Permissions by Level",
            labels={"x": "Permission Level", "y": "Count"},
        )
        st.plotly_chart(fig_ext, use_container_width=True)

        # Show top external users
        top_external = external_df["principal_name"].value_counts().head(10)
        st.write("**Top 10 External Users by Permission Count:**")
        st.dataframe(
            pd.DataFrame(
                {
                    "External User": top_external.index,
                    "Permission Count": top_external.values,
                }
            ),
            use_container_width=True,
            hide_index=True,
        )
