"""
External Access Dashboard Component
Analyzes and displays external user access patterns
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import asyncio
from typing import Dict, List, Any, Optional
import json

from src.database.repository import DatabaseRepository


class ExternalAccessComponent:
    """Handles external access analysis and visualization"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.repo = DatabaseRepository(db_path)

    @st.cache_data(ttl=300)
    def load_external_users(_self, limit: int = 500) -> pd.DataFrame:
        """Load external users with their access details"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)
            query = f"""
                SELECT
                    p.principal_id,
                    p.principal_name,
                    p.permission_level,
                    COUNT(DISTINCT p.object_id) as object_count,
                    COUNT(DISTINCT CASE WHEN p.object_type = 'site' THEN p.object_id END) as site_access,
                    COUNT(DISTINCT CASE WHEN p.object_type = 'library' THEN p.object_id END) as library_access,
                    COUNT(DISTINCT CASE WHEN p.object_type = 'folder' THEN p.object_id END) as folder_access,
                    COUNT(DISTINCT CASE WHEN p.object_type = 'file' THEN p.object_id END) as file_access,
                    GROUP_CONCAT(DISTINCT p.object_type) as access_types,
                    COUNT(DISTINCT CASE
                        WHEN p.object_type = 'file' AND f.sensitivity_score >= 40
                        THEN p.object_id
                    END) as sensitive_file_access
                FROM permissions p
                LEFT JOIN files f ON p.object_type = 'file' AND p.object_id = f.file_id
                WHERE p.is_external = 1
                GROUP BY p.principal_id, p.principal_name, p.permission_level
                ORDER BY object_count DESC
                LIMIT {limit}
            """
            results = await repo.fetch_all(query)
            return pd.DataFrame(results) if results else pd.DataFrame()

        return asyncio.run(_load())

    @st.cache_data(ttl=300)
    def load_external_access_summary(_self) -> Dict[str, Any]:
        """Load summary statistics for external access"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)

            # Overall stats
            stats = await repo.fetch_one("""
                SELECT
                    COUNT(DISTINCT principal_id) as external_users,
                    COUNT(*) as total_permissions,
                    COUNT(DISTINCT object_id) as objects_shared,
                    COUNT(DISTINCT CASE WHEN object_type = 'site' THEN object_id END) as sites_shared,
                    COUNT(DISTINCT CASE WHEN object_type = 'file' THEN object_id END) as files_shared
                FROM permissions
                WHERE is_external = 1
            """)

            # Sensitive file access
            sensitive = await repo.fetch_one("""
                SELECT COUNT(DISTINCT f.file_id) as count
                FROM files f
                JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
                WHERE p.is_external = 1 AND f.sensitivity_score >= 40
            """)

            # Anonymous links
            anon = await repo.fetch_one("""
                SELECT COUNT(*) as count
                FROM permissions
                WHERE is_anonymous_link = 1
            """)

            # Permission distribution
            perm_dist = await repo.fetch_all("""
                SELECT
                    permission_level,
                    COUNT(*) as count
                FROM permissions
                WHERE is_external = 1
                GROUP BY permission_level
            """)

            return {
                'stats': stats,
                'sensitive_files': sensitive['count'],
                'anonymous_links': anon['count'],
                'permission_distribution': pd.DataFrame(perm_dist) if perm_dist else pd.DataFrame()
            }

        return asyncio.run(_load())

    def render(self):
        """Render the external access component"""
        st.header("🌐 External Access Analysis")

        # Load data
        summary = self.load_external_access_summary()
        external_users = self.load_external_users()

        # Summary metrics
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "External Users",
                f"{summary['stats']['external_users']:,}",
                help="Unique external users with access"
            )

        with col2:
            st.metric(
                "Objects Shared",
                f"{summary['stats']['objects_shared']:,}",
                help="Total objects shared externally"
            )

        with col3:
            st.metric(
                "Sites Shared",
                f"{summary['stats']['sites_shared']:,}",
                help="Sites with external access"
            )

        with col4:
            st.metric(
                "Sensitive Files",
                f"{summary['sensitive_files']:,}",
                help="Sensitive files with external access",
                delta_color="inverse"
            )

        with col5:
            st.metric(
                "Anonymous Links",
                f"{summary['anonymous_links']:,}",
                help="Active anonymous sharing links",
                delta_color="inverse"
            )

        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs([
            "👥 External Users",
            "📊 Access Patterns",
            "🔒 Sensitive Access",
            "📈 Trends"
        ])

        with tab1:
            self._render_external_users(external_users)

        with tab2:
            self._render_access_patterns(external_users, summary)

        with tab3:
            self._render_sensitive_access()

        with tab4:
            self._render_external_trends()

    def _render_external_users(self, df: pd.DataFrame):
        """Render external users table and details"""
        st.subheader("👥 External Users")

        if df.empty:
            st.info("No external users found.")
            return

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            search = st.text_input(
                "Search users",
                placeholder="Enter name or email...",
                key="external_user_search"
            )

        with col2:
            min_objects = st.number_input(
                "Min objects accessed",
                min_value=0,
                value=0,
                key="external_min_objects"
            )

        with col3:
            perm_filter = st.multiselect(
                "Permission levels",
                options=df['permission_level'].unique().tolist(),
                default=df['permission_level'].unique().tolist(),
                key="external_perm_filter"
            )

        # Apply filters
        filtered_df = df.copy()

        if search:
            filtered_df = filtered_df[
                filtered_df['principal_name'].str.contains(search, case=False, na=False)
            ]

        if min_objects > 0:
            filtered_df = filtered_df[filtered_df['object_count'] >= min_objects]

        if perm_filter:
            filtered_df = filtered_df[filtered_df['permission_level'].isin(perm_filter)]

        # Calculate risk score
        filtered_df['risk_score'] = (
            (filtered_df['object_count'] / 10).clip(upper=30) +
            (filtered_df['sensitive_file_access'] * 2).clip(upper=40) +
            (filtered_df['permission_level'].map({
                'Full Control': 30,
                'Design': 20,
                'Edit': 15,
                'Contribute': 10,
                'Read': 5
            }).fillna(0))
        ).astype(int)

        # Display table
        display_cols = [
            'principal_name', 'permission_level', 'object_count',
            'site_access', 'file_access', 'sensitive_file_access', 'risk_score'
        ]

        st.dataframe(
            filtered_df[display_cols].rename(columns={
                'principal_name': 'User',
                'permission_level': 'Permission',
                'object_count': 'Total Objects',
                'site_access': 'Sites',
                'file_access': 'Files',
                'sensitive_file_access': 'Sensitive Files',
                'risk_score': 'Risk Score'
            }),
            use_container_width=True,
            hide_index=True
        )

        # Visualization
        if len(filtered_df) > 0:
            col1, col2 = st.columns(2)

            with col1:
                # Top users by access
                fig = px.bar(
                    filtered_df.head(20),
                    x='object_count',
                    y='principal_name',
                    orientation='h',
                    title="Top 20 External Users by Object Access",
                    color='risk_score',
                    color_continuous_scale='Reds'
                )
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Access type distribution
                access_data = []
                for _, user in filtered_df.iterrows():
                    access_data.extend([
                        {'User': user['principal_name'][:20] + '...', 'Type': 'Sites', 'Count': user['site_access']},
                        {'User': user['principal_name'][:20] + '...', 'Type': 'Libraries', 'Count': user['library_access']},
                        {'User': user['principal_name'][:20] + '...', 'Type': 'Folders', 'Count': user['folder_access']},
                        {'User': user['principal_name'][:20] + '...', 'Type': 'Files', 'Count': user['file_access']}
                    ])

                access_df = pd.DataFrame(access_data)
                top_users = filtered_df.head(10)['principal_name'].tolist()
                access_df = access_df[access_df['User'].str[:20].isin([u[:20] for u in top_users])]

                fig = px.bar(
                    access_df,
                    x='User',
                    y='Count',
                    color='Type',
                    title="Access Distribution by Type (Top 10 Users)",
                    color_discrete_map={
                        'Sites': '#3b82f6',
                        'Libraries': '#10b981',
                        'Folders': '#f59e0b',
                        'Files': '#ef4444'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)

    def _render_access_patterns(self, users_df: pd.DataFrame, summary: Dict[str, Any]):
        """Render external access patterns and analysis"""
        st.subheader("📊 External Access Patterns")

        col1, col2 = st.columns(2)

        with col1:
            # Permission level distribution
            if not summary['permission_distribution'].empty:
                fig = px.pie(
                    summary['permission_distribution'],
                    values='count',
                    names='permission_level',
                    title="External Access by Permission Level",
                    color_discrete_map={
                        'Full Control': '#dc2626',
                        'Design': '#f59e0b',
                        'Edit': '#eab308',
                        'Contribute': '#3b82f6',
                        'Read': '#10b981',
                        'Limited Access': '#6b7280'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Object type distribution
            obj_query = """
                SELECT
                    object_type,
                    COUNT(*) as count
                FROM permissions
                WHERE is_external = 1
                GROUP BY object_type
            """
            obj_dist = pd.read_sql_query(obj_query, f"sqlite:///{self.db_path}")

            fig = px.bar(
                obj_dist,
                x='object_type',
                y='count',
                title="External Access by Object Type",
                color='object_type',
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # Access heatmap
        st.subheader("🗓️ External Access Heatmap")

        # Load daily access data
        daily_query = """
            SELECT
                DATE(granted_at) as date,
                COUNT(DISTINCT principal_id) as unique_users,
                COUNT(*) as permissions_granted
            FROM permissions
            WHERE is_external = 1 AND granted_at IS NOT NULL
            GROUP BY DATE(granted_at)
            ORDER BY date DESC
            LIMIT 90
        """

        daily_df = pd.read_sql_query(daily_query, f"sqlite:///{self.db_path}")

        if not daily_df.empty:
            daily_df['date'] = pd.to_datetime(daily_df['date'])

            fig = px.density_heatmap(
                daily_df,
                x='date',
                y='unique_users',
                z='permissions_granted',
                title="External Access Activity Heatmap",
                labels={'unique_users': 'Unique External Users', 'permissions_granted': 'Permissions Granted'}
            )
            st.plotly_chart(fig, use_container_width=True)

    def _render_sensitive_access(self):
        """Render analysis of external access to sensitive files"""
        st.subheader("🔒 External Access to Sensitive Files")

        # Load sensitive files with external access
        query = """
            SELECT
                f.file_id,
                f.name as file_name,
                f.server_relative_url as file_path,
                f.sensitivity_score,
                f.sensitivity_level,
                f.sensitivity_categories,
                f.size_bytes,
                s.title as site_name,
                COUNT(DISTINCT p.principal_id) as external_users,
                GROUP_CONCAT(DISTINCT p.principal_name) as external_user_names,
                MAX(p.permission_level) as highest_permission
            FROM files f
            JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
            JOIN sites s ON f.site_id = s.id
            WHERE p.is_external = 1 AND f.sensitivity_score >= 40
            GROUP BY f.file_id
            ORDER BY f.sensitivity_score DESC, external_users DESC
            LIMIT 100
        """

        sensitive_df = pd.read_sql_query(query, f"sqlite:///{self.db_path}")

        if sensitive_df.empty:
            st.success("✅ No sensitive files are shared externally!")
            return

        # Alert
        st.error(f"⚠️ {len(sensitive_df)} sensitive files have external access!")

        # Metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            critical_count = len(sensitive_df[sensitive_df['sensitivity_level'] == 'CRITICAL'])
            st.metric("Critical Files", critical_count)

        with col2:
            total_external = sensitive_df['external_users'].sum()
            st.metric("Total External Users", total_external)

        with col3:
            write_access = len(sensitive_df[sensitive_df['highest_permission'].isin(['Full Control', 'Edit', 'Contribute'])])
            st.metric("With Write Access", write_access)

        with col4:
            avg_score = sensitive_df['sensitivity_score'].mean()
            st.metric("Avg Sensitivity", f"{avg_score:.1f}")

        # Risk matrix
        fig = px.scatter(
            sensitive_df,
            x='external_users',
            y='sensitivity_score',
            size='size_bytes',
            color='sensitivity_level',
            hover_data=['file_name', 'site_name'],
            title="Risk Matrix: External Users vs Sensitivity",
            color_discrete_map={
                'CRITICAL': '#991b1b',
                'HIGH': '#ef4444',
                'MEDIUM': '#f59e0b',
                'LOW': '#10b981'
            }
        )

        # Add risk zones
        fig.add_shape(
            type="rect",
            x0=5, x1=sensitive_df['external_users'].max(),
            y0=80, y1=100,
            fillcolor="red",
            opacity=0.2,
            line_width=0,
        )

        st.plotly_chart(fig, use_container_width=True)

        # Detailed table
        st.subheader("📋 Sensitive Files with External Access")

        # Process for display
        display_df = sensitive_df.copy()
        display_df['categories'] = display_df['sensitivity_categories'].apply(
            lambda x: ', '.join(json.loads(x)) if x and x != 'null' else 'N/A'
        )

        st.dataframe(
            display_df[[
                'file_name', 'sensitivity_score', 'sensitivity_level',
                'categories', 'site_name', 'external_users', 'highest_permission'
            ]].rename(columns={
                'file_name': 'File',
                'sensitivity_score': 'Score',
                'sensitivity_level': 'Level',
                'categories': 'Categories',
                'site_name': 'Site',
                'external_users': 'External Users',
                'highest_permission': 'Highest Permission'
            }),
            use_container_width=True,
            hide_index=True
        )

    def _render_external_trends(self):
        """Render external access trends over time"""
        st.subheader("📈 External Access Trends")

        # Load trend data
        trends_query = """
            SELECT
                DATE(granted_at) as date,
                COUNT(DISTINCT principal_id) as unique_users,
                COUNT(*) as permissions_granted,
                COUNT(DISTINCT object_id) as objects_shared,
                COUNT(DISTINCT CASE WHEN object_type = 'file' THEN object_id END) as files_shared
            FROM permissions
            WHERE is_external = 1 AND granted_at IS NOT NULL
            GROUP BY DATE(granted_at)
            ORDER BY date DESC
            LIMIT 90
        """

        trends_df = pd.read_sql_query(trends_query, f"sqlite:///{self.db_path}")

        if trends_df.empty:
            st.info("No trend data available.")
            return

        trends_df['date'] = pd.to_datetime(trends_df['date'])

        # Multi-line chart
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=trends_df['date'],
            y=trends_df['permissions_granted'],
            mode='lines+markers',
            name='Permissions Granted',
            line=dict(color='#ef4444', width=2)
        ))

        fig.add_trace(go.Scatter(
            x=trends_df['date'],
            y=trends_df['unique_users'],
            mode='lines+markers',
            name='Unique External Users',
            line=dict(color='#3b82f6', width=2)
        ))

        fig.add_trace(go.Scatter(
            x=trends_df['date'],
            y=trends_df['files_shared'],
            mode='lines+markers',
            name='Files Shared',
            line=dict(color='#10b981', width=2)
        ))

        fig.update_layout(
            title="External Access Trends Over Time",
            xaxis_title="Date",
            yaxis_title="Count",
            hovermode='x unified',
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)
