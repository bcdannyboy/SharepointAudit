"""
Permissions Dashboard Component
Comprehensive permissions analysis, management, and recommendations
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, List, Optional, Tuple
import asyncio
from datetime import datetime, timedelta
import numpy as np
import networkx as nx
import json

from src.database.repository import DatabaseRepository


class PermissionsComponent:
    """Comprehensive permissions analysis component"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.repo = DatabaseRepository(db_path)

    @st.cache_data(ttl=300)
    def load_permissions_overview(_self) -> Dict[str, Any]:
        """Load comprehensive permissions overview statistics"""
        query = """
            SELECT
                COUNT(DISTINCT id) as total_permissions,
                COUNT(DISTINCT principal_id) as total_principals,
                COUNT(DISTINCT object_id) as total_objects,
                COUNT(DISTINCT CASE WHEN is_inherited = 0 THEN id END) as unique_permissions,
                COUNT(DISTINCT CASE WHEN is_external = 1 THEN principal_id END) as external_principals,
                COUNT(DISTINCT CASE WHEN is_anonymous_link = 1 THEN id END) as anonymous_links,
                COUNT(DISTINCT CASE WHEN permission_level = 'Full Control' THEN principal_id END) as full_control_users,
                COUNT(DISTINCT CASE WHEN granted_at < datetime('now', '-1 year') THEN id END) as stale_permissions
            FROM permissions
        """

        df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")
        return df.iloc[0].to_dict()

    @st.cache_data(ttl=300)
    def load_permissions_by_type(_self) -> pd.DataFrame:
        """Load permissions grouped by object type"""
        query = """
            SELECT
                object_type,
                COUNT(*) as permission_count,
                COUNT(DISTINCT principal_id) as unique_principals,
                COUNT(DISTINCT object_id) as unique_objects,
                COUNT(DISTINCT CASE WHEN is_inherited = 0 THEN id END) as direct_permissions,
                COUNT(DISTINCT CASE WHEN is_external = 1 THEN principal_id END) as external_users,
                COUNT(DISTINCT CASE WHEN permission_level = 'Full Control' THEN principal_id END) as admin_users
            FROM permissions
            GROUP BY object_type
        """

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    @st.cache_data(ttl=300)
    def load_permission_levels(_self) -> pd.DataFrame:
        """Load permission level distribution"""
        query = """
            SELECT
                permission_level,
                COUNT(*) as count,
                COUNT(DISTINCT principal_id) as unique_users,
                COUNT(DISTINCT object_id) as unique_objects,
                COUNT(DISTINCT CASE WHEN is_external = 1 THEN principal_id END) as external_users
            FROM permissions
            GROUP BY permission_level
            ORDER BY count DESC
        """

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    @st.cache_data(ttl=300)
    def load_principal_permissions(_self, limit: int = 1000) -> pd.DataFrame:
        """Load detailed principal permissions"""
        query = f"""
            SELECT
                p.principal_id,
                p.principal_name,
                p.principal_type,
                MAX(p.is_external) as is_external,
                COUNT(DISTINCT p.object_id) as object_count,
                COUNT(DISTINCT p.permission_level) as permission_types,
                GROUP_CONCAT(DISTINCT p.permission_level) as permission_levels,
                COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.object_id END) as full_control_count,
                COUNT(DISTINCT CASE WHEN p.permission_level = 'Edit' THEN p.object_id END) as edit_count,
                COUNT(DISTINCT CASE WHEN p.permission_level = 'Read' THEN p.object_id END) as read_count,
                COUNT(DISTINCT CASE WHEN p.object_type = 'site' THEN p.object_id END) as site_access,
                COUNT(DISTINCT CASE WHEN p.object_type = 'file' THEN p.object_id END) as file_access,
                COUNT(DISTINCT CASE WHEN p.is_inherited = 0 THEN p.object_id END) as direct_permissions,
                MIN(p.granted_at) as first_permission_date,
                MAX(p.granted_at) as last_permission_date
            FROM permissions p
            GROUP BY p.principal_id, p.principal_name, p.principal_type
            ORDER BY object_count DESC
            LIMIT {limit}
        """

        df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

        # Calculate derived metrics
        df['permission_complexity'] = df['permission_types'] * df['object_count'] / 10
        df['is_power_user'] = (df['full_control_count'] > 5) | (df['object_count'] > 50)
        df['is_admin'] = df['full_control_count'] > 10

        # Parse dates
        df['first_permission_date'] = pd.to_datetime(df['first_permission_date'])
        df['last_permission_date'] = pd.to_datetime(df['last_permission_date'])
        df['days_active'] = (df['last_permission_date'] - df['first_permission_date']).dt.days

        return df

    @st.cache_data(ttl=300)
    def load_object_permissions(_self, object_type: Optional[str] = None, limit: int = 1000) -> pd.DataFrame:
        """Load detailed object permissions"""
        where_clause = f"WHERE p.object_type = '{object_type}'" if object_type else ""

        query = f"""
            SELECT
                p.object_type,
                p.object_id,
                COUNT(DISTINCT p.principal_id) as user_count,
                COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_user_count,
                COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.principal_id END) as admin_count,
                COUNT(DISTINCT p.permission_level) as permission_variety,
                COUNT(DISTINCT CASE WHEN p.is_inherited = 0 THEN p.id END) as direct_permissions,
                COUNT(DISTINCT CASE WHEN p.is_anonymous_link = 1 THEN p.id END) as anonymous_links,
                GROUP_CONCAT(DISTINCT p.permission_level) as permission_levels,
                CASE
                    WHEN p.object_type = 'site' THEN s.title
                    WHEN p.object_type = 'library' THEN l.name
                    WHEN p.object_type = 'folder' THEN fo.name
                    WHEN p.object_type = 'file' THEN fi.name
                END as object_name,
                CASE
                    WHEN p.object_type = 'site' THEN s.url
                    WHEN p.object_type = 'library' THEN l.site_url || '/' || l.name
                    WHEN p.object_type = 'folder' THEN fo.server_relative_url
                    WHEN p.object_type = 'file' THEN fi.server_relative_url
                END as object_path
            FROM permissions p
            LEFT JOIN sites s ON p.object_type = 'site' AND p.object_id = s.site_id
            LEFT JOIN libraries l ON p.object_type = 'library' AND p.object_id = l.library_id
            LEFT JOIN folders fo ON p.object_type = 'folder' AND p.object_id = fo.folder_id
            LEFT JOIN files fi ON p.object_type = 'file' AND p.object_id = fi.file_id
            {where_clause}
            GROUP BY p.object_type, p.object_id
            ORDER BY user_count DESC
            LIMIT {limit}
        """

        df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

        # Calculate permission complexity score
        df['complexity_score'] = (
            df['user_count'] * 0.3 +
            df['external_user_count'] * 2 +
            df['permission_variety'] * 5 +
            df['direct_permissions'] * 1.5 +
            df['anonymous_links'] * 10
        ).round(1)

        return df

    @st.cache_data(ttl=300)
    def load_permission_inheritance(_self) -> pd.DataFrame:
        """Load permission inheritance analysis"""
        query = """
            SELECT
                object_type,
                COUNT(*) as total_permissions,
                COUNT(CASE WHEN is_inherited = 1 THEN 1 END) as inherited_permissions,
                COUNT(CASE WHEN is_inherited = 0 THEN 1 END) as direct_permissions,
                ROUND(COUNT(CASE WHEN is_inherited = 1 THEN 1 END) * 100.0 / COUNT(*), 2) as inheritance_rate
            FROM permissions
            GROUP BY object_type
            ORDER BY total_permissions DESC
        """

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    @st.cache_data(ttl=300)
    def load_permission_timeline(_self, days: int = 365) -> pd.DataFrame:
        """Load permission grant timeline"""
        query = f"""
            SELECT
                DATE(granted_at) as grant_date,
                COUNT(*) as permissions_granted,
                COUNT(DISTINCT principal_id) as unique_users,
                COUNT(DISTINCT object_id) as unique_objects,
                COUNT(CASE WHEN is_external = 1 THEN 1 END) as external_permissions,
                COUNT(CASE WHEN permission_level = 'Full Control' THEN 1 END) as admin_grants
            FROM permissions
            WHERE granted_at >= datetime('now', '-{days} days')
                AND granted_at IS NOT NULL
            GROUP BY DATE(granted_at)
            ORDER BY grant_date DESC
        """

        df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")
        df['grant_date'] = pd.to_datetime(df['grant_date'])
        return df

    @st.cache_data(ttl=300)
    def load_group_permissions(_self) -> pd.DataFrame:
        """Load group-based permissions"""
        query = """
            SELECT
                g.group_id,
                g.name as group_name,
                g.description,
                g.member_count,
                g.is_site_group,
                COUNT(DISTINCT p.object_id) as object_count,
                COUNT(DISTINCT p.permission_level) as permission_types,
                GROUP_CONCAT(DISTINCT p.permission_level) as permission_levels,
                COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.object_id END) as full_control_objects,
                s.title as site_name
            FROM groups g
            LEFT JOIN permissions p ON p.principal_type = 'group' AND p.principal_id = g.group_id
            LEFT JOIN sites s ON g.site_id = s.id
            GROUP BY g.group_id
            HAVING object_count > 0
            ORDER BY object_count DESC
        """

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    def render(self):
        """Render the permissions component"""
        st.header("🔑 Comprehensive Permissions Analysis")

        # Load overview data
        overview = self.load_permissions_overview()

        # Overview metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            st.metric(
                "Total Permissions",
                f"{overview['total_permissions']:,}",
                help="Total number of permission entries"
            )

        with col2:
            st.metric(
                "Unique Principals",
                f"{overview['total_principals']:,}",
                help="Unique users and groups with permissions"
            )

        with col3:
            st.metric(
                "Protected Objects",
                f"{overview['total_objects']:,}",
                help="Total objects with permissions"
            )

        with col4:
            st.metric(
                "Direct Permissions",
                f"{overview['unique_permissions']:,}",
                help="Non-inherited permissions",
                delta=f"{overview['unique_permissions']/overview['total_permissions']*100:.1f}%"
            )

        with col5:
            st.metric(
                "External Users",
                f"{overview['external_principals']:,}",
                help="External users with access",
                delta_color="inverse"
            )

        with col6:
            st.metric(
                "Anonymous Links",
                f"{overview['anonymous_links']:,}",
                help="Anonymous sharing links",
                delta_color="inverse"
            )

        # Main tabs
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "📊 Overview",
            "👥 Principal Analysis",
            "📁 Object Permissions",
            "🔄 Inheritance",
            "👪 Groups",
            "📈 Timeline",
            "🎯 Recommendations"
        ])

        with tab1:
            self._render_permissions_overview()

        with tab2:
            self._render_principal_analysis()

        with tab3:
            self._render_object_permissions()

        with tab4:
            self._render_inheritance_analysis()

        with tab5:
            self._render_group_analysis()

        with tab6:
            self._render_timeline_analysis()

        with tab7:
            self._render_recommendations()

    def _render_permissions_overview(self):
        """Render permissions overview visualizations"""
        st.subheader("📊 Permissions Distribution Overview")

        # Load data
        by_type = self.load_permissions_by_type()
        by_level = self.load_permission_levels()

        # Create visualizations
        col1, col2 = st.columns(2)

        with col1:
            # Permissions by object type
            fig = px.pie(
                by_type,
                values='permission_count',
                names='object_type',
                title="Permissions by Object Type",
                hole=0.4
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

            # Permission level distribution
            fig = px.bar(
                by_level,
                x='permission_level',
                y='count',
                title="Permission Level Distribution",
                color='count',
                color_continuous_scale='Blues',
                text='count'
            )
            fig.update_traces(texttemplate='%{text:,}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # External vs internal permissions
            external_data = pd.DataFrame([
                {'Type': 'Internal', 'Count': by_type['unique_principals'].sum() - by_type['external_users'].sum()},
                {'Type': 'External', 'Count': by_type['external_users'].sum()}
            ])

            fig = px.pie(
                external_data,
                values='Count',
                names='Type',
                title="Internal vs External Access",
                color_discrete_map={'Internal': '#3b82f6', 'External': '#ef4444'}
            )
            st.plotly_chart(fig, use_container_width=True)

            # Direct vs inherited permissions
            inheritance_data = pd.DataFrame([
                {'Type': 'Inherited', 'Count': by_type['permission_count'].sum() - by_type['direct_permissions'].sum()},
                {'Type': 'Direct', 'Count': by_type['direct_permissions'].sum()}
            ])

            fig = px.pie(
                inheritance_data,
                values='Count',
                names='Type',
                title="Permission Inheritance",
                hole=0.4,
                color_discrete_map={'Inherited': '#10b981', 'Direct': '#f59e0b'}
            )
            st.plotly_chart(fig, use_container_width=True)

        # Detailed breakdown table
        st.subheader("📋 Permission Type Breakdown")

        display_df = by_type.copy()
        display_df['direct_rate'] = (display_df['direct_permissions'] / display_df['permission_count'] * 100).round(1)
        display_df['external_rate'] = (display_df['external_users'] / display_df['unique_principals'] * 100).round(1)

        st.dataframe(
            display_df[[
                'object_type', 'permission_count', 'unique_principals', 'unique_objects',
                'direct_permissions', 'direct_rate', 'external_users', 'external_rate'
            ]].rename(columns={
                'object_type': 'Object Type',
                'permission_count': 'Total Permissions',
                'unique_principals': 'Unique Users',
                'unique_objects': 'Unique Objects',
                'direct_permissions': 'Direct Perms',
                'direct_rate': 'Direct %',
                'external_users': 'External Users',
                'external_rate': 'External %'
            }),
            use_container_width=True,
            hide_index=True
        )

    def _render_principal_analysis(self):
        """Render principal (user/group) analysis"""
        st.subheader("👥 Principal Permissions Analysis")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            principal_type = st.selectbox(
                "Principal Type",
                ["All", "Users", "Groups", "External"],
                key="principal_type_filter"
            )

        with col2:
            min_objects = st.number_input(
                "Min Objects",
                min_value=0,
                max_value=1000,
                value=0,
                step=10,
                key="min_objects_filter"
            )

        with col3:
            show_admins_only = st.checkbox(
                "Admins Only",
                key="admins_only_filter"
            )

        # Load and filter data
        principals_df = self.load_principal_permissions()

        if principal_type == "External":
            principals_df = principals_df[principals_df['is_external'] == 1]
        elif principal_type == "Users":
            principals_df = principals_df[principals_df['principal_type'] == 'user']
        elif principal_type == "Groups":
            principals_df = principals_df[principals_df['principal_type'] == 'group']

        if min_objects > 0:
            principals_df = principals_df[principals_df['object_count'] >= min_objects]

        if show_admins_only:
            principals_df = principals_df[principals_df['is_admin']]

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            # Top principals by object count
            top_principals = principals_df.head(20)

            fig = px.bar(
                top_principals,
                x='object_count',
                y='principal_name',
                orientation='h',
                title="Top 20 Principals by Object Access",
                color='is_external',
                color_discrete_map={0: '#3b82f6', 1: '#ef4444'},
                labels={'is_external': 'External'}
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Permission complexity scatter
            fig = px.scatter(
                principals_df.head(100),
                x='object_count',
                y='permission_types',
                size='full_control_count',
                color='is_power_user',
                title="Principal Permission Complexity",
                labels={
                    'object_count': 'Number of Objects',
                    'permission_types': 'Permission Variety',
                    'is_power_user': 'Power User'
                },
                color_discrete_map={True: '#ef4444', False: '#3b82f6'}
            )
            st.plotly_chart(fig, use_container_width=True)

        # Detailed principal table
        st.subheader("👤 Principal Details")

        # Search functionality
        search = st.text_input("Search principals", placeholder="Enter name or ID...")
        if search:
            principals_df = principals_df[
                principals_df['principal_name'].str.contains(search, case=False, na=False) |
                principals_df['principal_id'].str.contains(search, case=False, na=False)
            ]

        # Display table
        display_df = principals_df.head(50).copy()
        display_df['type'] = display_df.apply(
            lambda x: 'External' if x['is_external'] else x['principal_type'].title(),
            axis=1
        )

        st.dataframe(
            display_df[[
                'principal_name', 'type', 'object_count', 'full_control_count',
                'edit_count', 'read_count', 'direct_permissions', 'days_active'
            ]].rename(columns={
                'principal_name': 'Principal',
                'type': 'Type',
                'object_count': 'Total Objects',
                'full_control_count': 'Full Control',
                'edit_count': 'Edit',
                'read_count': 'Read',
                'direct_permissions': 'Direct Perms',
                'days_active': 'Days Active'
            }),
            use_container_width=True,
            hide_index=True
        )

        # Export functionality
        if st.button("Export Principal Report", key="export_principals"):
            csv = display_df.to_csv(index=False)
            st.download_button(
                "Download CSV",
                csv,
                "principal_permissions_report.csv",
                "text/csv",
                key="download_principals_csv"
            )

    def _render_object_permissions(self):
        """Render object-level permissions analysis"""
        st.subheader("📁 Object Permissions Analysis")

        # Object type selector
        object_type = st.selectbox(
            "Object Type",
            ["All", "site", "library", "folder", "file"],
            key="object_type_selector"
        )

        # Load data
        objects_df = self.load_object_permissions(
            object_type if object_type != "All" else None
        )

        if objects_df.empty:
            st.info("No objects found with the selected criteria.")
            return

        # Complexity analysis
        col1, col2 = st.columns(2)

        with col1:
            # Complexity distribution
            fig = px.histogram(
                objects_df,
                x='complexity_score',
                nbins=30,
                title="Permission Complexity Distribution",
                labels={'complexity_score': 'Complexity Score', 'count': 'Number of Objects'}
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # User count vs external users
            fig = px.scatter(
                objects_df.head(200),
                x='user_count',
                y='external_user_count',
                size='complexity_score',
                color='anonymous_links',
                title="Object Access: Total vs External Users",
                labels={
                    'user_count': 'Total Users',
                    'external_user_count': 'External Users',
                    'anonymous_links': 'Anonymous Links'
                },
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)

        # High complexity objects
        st.subheader("⚠️ High Complexity Objects")

        high_complexity = objects_df[objects_df['complexity_score'] > 50].head(30)

        if not high_complexity.empty:
            st.dataframe(
                high_complexity[[
                    'object_type', 'object_name', 'complexity_score', 'user_count',
                    'external_user_count', 'admin_count', 'direct_permissions', 'anonymous_links'
                ]].rename(columns={
                    'object_type': 'Type',
                    'object_name': 'Name',
                    'complexity_score': 'Complexity',
                    'user_count': 'Total Users',
                    'external_user_count': 'External',
                    'admin_count': 'Admins',
                    'direct_permissions': 'Direct Perms',
                    'anonymous_links': 'Anon Links'
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("No high complexity objects found!")

        # Permission variety analysis
        st.subheader("🔀 Permission Variety Analysis")

        variety_stats = objects_df.groupby('permission_variety').agg({
            'object_id': 'count',
            'user_count': 'mean',
            'external_user_count': 'mean'
        }).round(1).reset_index()

        fig = px.bar(
            variety_stats,
            x='permission_variety',
            y='object_id',
            title="Objects by Permission Variety",
            labels={
                'permission_variety': 'Number of Different Permission Levels',
                'object_id': 'Object Count'
            },
            text='object_id'
        )
        fig.update_traces(texttemplate='%{text:,}')
        st.plotly_chart(fig, use_container_width=True)

    def _render_inheritance_analysis(self):
        """Render permission inheritance analysis"""
        st.subheader("🔄 Permission Inheritance Analysis")

        # Load inheritance data
        inheritance_df = self.load_permission_inheritance()

        # Overall inheritance metrics
        col1, col2, col3 = st.columns(3)

        total_inherited = inheritance_df['inherited_permissions'].sum()
        total_direct = inheritance_df['direct_permissions'].sum()
        total_perms = inheritance_df['total_permissions'].sum()

        with col1:
            st.metric(
                "Total Inherited",
                f"{total_inherited:,}",
                f"{total_inherited/total_perms*100:.1f}%"
            )

        with col2:
            st.metric(
                "Total Direct",
                f"{total_direct:,}",
                f"{total_direct/total_perms*100:.1f}%"
            )

        with col3:
            st.metric(
                "Inheritance Rate",
                f"{total_inherited/total_perms*100:.1f}%",
                help="Percentage of permissions that are inherited"
            )

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            # Inheritance by object type
            fig = px.bar(
                inheritance_df,
                x='object_type',
                y=['inherited_permissions', 'direct_permissions'],
                title="Inheritance by Object Type",
                labels={'value': 'Permission Count', 'object_type': 'Object Type'},
                color_discrete_map={
                    'inherited_permissions': '#10b981',
                    'direct_permissions': '#f59e0b'
                }
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Inheritance rate comparison
            fig = px.bar(
                inheritance_df,
                x='object_type',
                y='inheritance_rate',
                title="Inheritance Rate by Object Type",
                color='inheritance_rate',
                color_continuous_scale='RdYlGn',
                text='inheritance_rate'
            )
            fig.update_traces(texttemplate='%{text:.1f}%')
            st.plotly_chart(fig, use_container_width=True)

        # Inheritance chain analysis
        st.subheader("🔗 Inheritance Chain Analysis")

        # Sites with broken inheritance
        broken_inheritance_query = """
            SELECT
                s.title as site_name,
                s.url as site_url,
                COUNT(DISTINCT f.file_id) as files_with_unique_perms,
                COUNT(DISTINCT fo.folder_id) as folders_with_unique_perms,
                COUNT(DISTINCT l.library_id) as libraries_with_unique_perms
            FROM sites s
            LEFT JOIN files f ON s.id = f.site_id AND f.has_unique_permissions = 1
            LEFT JOIN folders fo ON s.id = fo.site_id AND fo.has_unique_permissions = 1
            LEFT JOIN libraries l ON s.id = l.site_id
            LEFT JOIN permissions p ON p.object_type = 'library' AND p.object_id = l.library_id AND p.is_inherited = 0
            GROUP BY s.id
            HAVING files_with_unique_perms > 0 OR folders_with_unique_perms > 0 OR libraries_with_unique_perms > 0
            ORDER BY files_with_unique_perms DESC
            LIMIT 20
        """

        broken_df = pd.read_sql_query(broken_inheritance_query, f"sqlite:///{self.db_path}")

        if not broken_df.empty:
            st.warning(f"Found {len(broken_df)} sites with broken permission inheritance")

            fig = px.bar(
                broken_df.head(10),
                x='site_name',
                y=['files_with_unique_perms', 'folders_with_unique_perms', 'libraries_with_unique_perms'],
                title="Top Sites with Broken Inheritance",
                labels={'value': 'Count', 'site_name': 'Site'},
                barmode='stack'
            )
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                broken_df[[
                    'site_name', 'files_with_unique_perms',
                    'folders_with_unique_perms', 'libraries_with_unique_perms'
                ]].rename(columns={
                    'site_name': 'Site',
                    'files_with_unique_perms': 'Files',
                    'folders_with_unique_perms': 'Folders',
                    'libraries_with_unique_perms': 'Libraries'
                }),
                use_container_width=True,
                hide_index=True
            )

    def _render_group_analysis(self):
        """Render group permissions analysis"""
        st.subheader("👪 Group Permissions Analysis")

        # Load group data
        groups_df = self.load_group_permissions()

        if groups_df.empty:
            st.info("No group permissions found.")
            return

        # Group metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Groups", f"{len(groups_df):,}")

        with col2:
            site_groups = len(groups_df[groups_df['is_site_group']])
            st.metric("Site Groups", f"{site_groups:,}")

        with col3:
            large_groups = len(groups_df[groups_df['member_count'] > 50])
            st.metric("Large Groups (>50)", f"{large_groups:,}")

        with col4:
            admin_groups = len(groups_df[groups_df['full_control_objects'] > 0])
            st.metric("Admin Groups", f"{admin_groups:,}")

        # Visualizations
        col1, col2 = st.columns(2)

        with col1:
            # Top groups by object count
            top_groups = groups_df.nlargest(15, 'object_count')

            fig = px.bar(
                top_groups,
                x='object_count',
                y='group_name',
                orientation='h',
                title="Top 15 Groups by Object Access",
                color='is_site_group',
                color_discrete_map={True: '#3b82f6', False: '#10b981'},
                labels={'is_site_group': 'Site Group'}
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Group size vs permissions
            fig = px.scatter(
                groups_df,
                x='member_count',
                y='object_count',
                size='full_control_objects',
                color='is_site_group',
                title="Group Size vs Permissions",
                labels={
                    'member_count': 'Number of Members',
                    'object_count': 'Objects with Access',
                    'is_site_group': 'Site Group'
                },
                log_x=True
            )
            st.plotly_chart(fig, use_container_width=True)

        # Detailed group table
        st.subheader("📋 Group Details")

        display_df = groups_df.copy()
        display_df['type'] = display_df['is_site_group'].map({True: 'Site Group', False: 'Security Group'})

        st.dataframe(
            display_df[[
                'group_name', 'type', 'member_count', 'object_count',
                'permission_types', 'full_control_objects', 'site_name'
            ]].rename(columns={
                'group_name': 'Group Name',
                'type': 'Type',
                'member_count': 'Members',
                'object_count': 'Objects',
                'permission_types': 'Permission Types',
                'full_control_objects': 'Admin Objects',
                'site_name': 'Site'
            }),
            use_container_width=True,
            hide_index=True
        )

    def _render_timeline_analysis(self):
        """Render permission timeline analysis"""
        st.subheader("📈 Permission Grant Timeline")

        # Time range selector
        time_range = st.selectbox(
            "Time Range",
            ["Last 30 days", "Last 90 days", "Last 6 months", "Last year", "All time"],
            key="timeline_range"
        )

        days_map = {
            "Last 30 days": 30,
            "Last 90 days": 90,
            "Last 6 months": 180,
            "Last year": 365,
            "All time": 3650
        }

        # Load timeline data
        timeline_df = self.load_permission_timeline(days_map[time_range])

        if timeline_df.empty:
            st.info("No permission grants found in the selected time range.")
            return

        # Timeline visualization
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=('Permissions Granted Over Time', 'Grant Activity Heatmap'),
            specs=[[{'type': 'scatter'}], [{'type': 'scatter'}]],
            row_heights=[0.7, 0.3]
        )

        # Main timeline
        fig.add_trace(
            go.Scatter(
                x=timeline_df['grant_date'],
                y=timeline_df['permissions_granted'],
                mode='lines+markers',
                name='Total Permissions',
                line=dict(color='#3b82f6', width=2)
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=timeline_df['grant_date'],
                y=timeline_df['external_permissions'],
                mode='lines',
                name='External Permissions',
                line=dict(color='#ef4444', width=1.5)
            ),
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=timeline_df['grant_date'],
                y=timeline_df['admin_grants'],
                mode='lines',
                name='Admin Grants',
                line=dict(color='#f59e0b', width=1.5)
            ),
            row=1, col=1
        )

        # Activity heatmap
        timeline_df['weekday'] = timeline_df['grant_date'].dt.day_name()
        timeline_df['week'] = timeline_df['grant_date'].dt.isocalendar().week

        fig.update_layout(height=600, showlegend=True)
        fig.update_xaxes(title_text="Date", row=1, col=1)
        fig.update_yaxes(title_text="Permissions Granted", row=1, col=1)

        st.plotly_chart(fig, use_container_width=True)

        # Grant patterns
        st.subheader("📊 Permission Grant Patterns")

        col1, col2 = st.columns(2)

        with col1:
            # Day of week analysis
            dow_stats = timeline_df.groupby('weekday').agg({
                'permissions_granted': 'sum',
                'unique_users': 'sum'
            }).reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])

            fig = px.bar(
                dow_stats.reset_index(),
                x='weekday',
                y='permissions_granted',
                title="Permissions by Day of Week",
                color='permissions_granted',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Recent activity summary
            recent = timeline_df.head(7)
            st.markdown("### Recent Activity (Last 7 Days)")

            if not recent.empty:
                total_recent = recent['permissions_granted'].sum()
                external_recent = recent['external_permissions'].sum()
                admin_recent = recent['admin_grants'].sum()

                st.metric("Total Permissions", f"{total_recent:,}")
                st.metric("External Permissions", f"{external_recent:,}",
                         f"{external_recent/total_recent*100:.1f}%" if total_recent > 0 else "0%")
                st.metric("Admin Grants", f"{admin_recent:,}",
                         f"{admin_recent/total_recent*100:.1f}%" if total_recent > 0 else "0%")
            else:
                st.info("No recent activity")

    def _render_recommendations(self):
        """Render permission recommendations"""
        st.subheader("🎯 Permission Recommendations")

        # Load data for recommendations
        overview = self.load_permissions_overview()
        principals_df = self.load_principal_permissions()
        objects_df = self.load_object_permissions()

        # Risk scoring
        risks = []

        # Check for excessive external access
        if overview['external_principals'] > 100:
            risks.append({
                'severity': 'High',
                'category': 'External Access',
                'issue': f"High number of external users ({overview['external_principals']:,})",
                'recommendation': "Review and audit external user permissions",
                'impact': "Security risk from excessive external access"
            })

        # Check for anonymous links
        if overview['anonymous_links'] > 0:
            risks.append({
                'severity': 'Critical',
                'category': 'Anonymous Access',
                'issue': f"Anonymous links detected ({overview['anonymous_links']:,})",
                'recommendation': "Remove or expire anonymous sharing links",
                'impact': "Uncontrolled access to resources"
            })

        # Check for stale permissions
        if overview['stale_permissions'] > overview['total_permissions'] * 0.2:
            risks.append({
                'severity': 'Medium',
                'category': 'Stale Permissions',
                'issue': f"Many stale permissions ({overview['stale_permissions']:,})",
                'recommendation': "Implement regular permission reviews",
                'impact': "Unnecessary access accumulation"
            })

        # Check for over-privileged users
        if not principals_df.empty:
            admin_users = len(principals_df[principals_df['is_admin']])
            if admin_users > 20:
                risks.append({
                    'severity': 'High',
                    'category': 'Privileged Access',
                    'issue': f"Too many admin users ({admin_users})",
                    'recommendation': "Apply principle of least privilege",
                    'impact': "Excessive administrative access"
                })

        # Check for complex permissions
        if not objects_df.empty:
            complex_objects = len(objects_df[objects_df['complexity_score'] > 100])
            if complex_objects > 50:
                risks.append({
                    'severity': 'Medium',
                    'category': 'Permission Complexity',
                    'issue': f"High permission complexity ({complex_objects} objects)",
                    'recommendation': "Simplify permission structures",
                    'impact': "Difficult to manage and audit"
                })

        # Display recommendations
        if risks:
            # Summary
            severity_counts = pd.DataFrame(risks).groupby('severity').size()
            col1, col2, col3 = st.columns(3)

            with col1:
                critical = severity_counts.get('Critical', 0)
                st.metric("Critical Issues", critical, delta_color="inverse")

            with col2:
                high = severity_counts.get('High', 0)
                st.metric("High Issues", high, delta_color="inverse")

            with col3:
                medium = severity_counts.get('Medium', 0)
                st.metric("Medium Issues", medium)

            # Detailed recommendations
            for risk in sorted(risks, key=lambda x: {'Critical': 0, 'High': 1, 'Medium': 2}[x['severity']]):
                with st.expander(f"{risk['severity']} - {risk['category']}: {risk['issue']}"):
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        st.markdown(f"**Recommendation:** {risk['recommendation']}")
                        st.markdown(f"**Impact:** {risk['impact']}")

                    with col2:
                        severity_color = {
                            'Critical': '🔴',
                            'High': '🟠',
                            'Medium': '🟡'
                        }
                        st.markdown(f"### {severity_color[risk['severity']]} {risk['severity']}")
        else:
            st.success("✅ No critical permission issues detected!")

        # Best practices
        st.subheader("📚 Best Practices")

        with st.expander("Permission Management Best Practices"):
            st.markdown("""
            1. **Principle of Least Privilege**
               - Grant only the minimum permissions required
               - Regularly review and remove unnecessary access

            2. **Regular Audits**
               - Schedule monthly permission reviews
               - Focus on external and admin access

            3. **Inheritance Management**
               - Use permission inheritance where possible
               - Document reasons for breaking inheritance

            4. **Group-Based Access**
               - Manage permissions through groups
               - Avoid individual user permissions

            5. **External Access Control**
               - Minimize external user access
               - Use expiring links for temporary access

            6. **Documentation**
               - Document permission changes
               - Maintain access request records
            """)
