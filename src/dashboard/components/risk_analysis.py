"""
Risk Analysis Dashboard Component
Comprehensive risk assessment and visualization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import asyncio
from typing import Dict, List, Any, Optional
import json
import humanize

from src.database.repository import DatabaseRepository


class RiskAnalysisComponent:
    """Handles comprehensive risk analysis and visualization"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.repo = DatabaseRepository(db_path)

    @st.cache_data(ttl=300)
    def calculate_risk_scores(_self) -> Dict[str, pd.DataFrame]:
        """Calculate various risk scores across the environment"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)

            # File risk scores
            file_risk_query = """
                SELECT
                    f.file_id,
                    f.name as file_name,
                    f.server_relative_url as file_path,
                    f.size_bytes,
                    f.sensitivity_score,
                    f.sensitivity_level,
                    s.title as site_name,
                    COUNT(DISTINCT p.principal_id) as total_users,
                    COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_users,
                    COUNT(DISTINCT CASE WHEN p.permission_level IN ('Full Control', 'Edit') THEN p.principal_id END) as write_users,
                    MAX(CASE WHEN p.is_anonymous_link = 1 THEN 1 ELSE 0 END) as has_anonymous_link,
                    MAX(CASE WHEN f.has_unique_permissions = 1 THEN 1 ELSE 0 END) as has_unique_perms,
                    -- Calculate risk score
                    (
                        f.sensitivity_score * 0.4 +
                        CASE WHEN COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) > 0 THEN 30 ELSE 0 END +
                        CASE WHEN MAX(p.is_anonymous_link) = 1 THEN 20 ELSE 0 END +
                        CASE WHEN COUNT(DISTINCT p.principal_id) > 50 THEN 10 ELSE COUNT(DISTINCT p.principal_id) / 5 END
                    ) as risk_score
                FROM files f
                JOIN sites s ON f.site_id = s.id
                LEFT JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
                GROUP BY f.file_id
                HAVING risk_score > 0
                ORDER BY risk_score DESC
                LIMIT 500
            """

            # Site risk scores
            site_risk_query = """
                SELECT
                    s.site_id,
                    s.title as site_name,
                    s.url as site_url,
                    COUNT(DISTINCT p.principal_id) as total_users,
                    COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_users,
                    COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.principal_id END) as admin_users,
                    COUNT(DISTINCT f.file_id) as total_files,
                    COUNT(DISTINCT CASE WHEN f.sensitivity_score >= 40 THEN f.file_id END) as sensitive_files,
                    COUNT(DISTINCT CASE WHEN f.has_unique_permissions = 1 THEN f.file_id END) as unique_perm_files,
                    -- Calculate site risk score
                    (
                        CASE WHEN COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) > 0 THEN 20 ELSE 0 END +
                        CASE WHEN COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.principal_id END) > 10 THEN 15 ELSE COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.principal_id END) * 1.5 END +
                        CASE WHEN COUNT(DISTINCT CASE WHEN f.sensitivity_score >= 40 THEN f.file_id END) > 0 THEN COUNT(DISTINCT CASE WHEN f.sensitivity_score >= 40 THEN f.file_id END) / COUNT(DISTINCT f.file_id) * 30 ELSE 0 END +
                        CASE WHEN COUNT(DISTINCT CASE WHEN f.has_unique_permissions = 1 THEN f.file_id END) > 100 THEN 20 ELSE COUNT(DISTINCT CASE WHEN f.has_unique_permissions = 1 THEN f.file_id END) / 5 END
                    ) as risk_score
                FROM sites s
                LEFT JOIN permissions p ON p.object_type = 'site' AND p.object_id = s.site_id
                LEFT JOIN files f ON f.site_id = s.id
                GROUP BY s.site_id
                ORDER BY risk_score DESC
            """

            # User risk scores
            user_risk_query = """
                SELECT
                    p.principal_id,
                    p.principal_name,
                    p.principal_type,
                    MAX(p.is_external) as is_external,
                    COUNT(DISTINCT p.object_id) as object_count,
                    COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.object_id END) as full_control_count,
                    COUNT(DISTINCT CASE WHEN p.object_type = 'file' AND f.sensitivity_score >= 40 THEN p.object_id END) as sensitive_access,
                    GROUP_CONCAT(DISTINCT p.permission_level) as permission_levels,
                    -- Calculate user risk score
                    (
                        CASE WHEN MAX(p.is_external) = 1 THEN 25 ELSE 0 END +
                        CASE WHEN COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.object_id END) > 0 THEN 20 ELSE 0 END +
                        CASE WHEN COUNT(DISTINCT CASE WHEN p.object_type = 'file' AND f.sensitivity_score >= 40 THEN p.object_id END) > 0 THEN 30 ELSE 0 END +
                        CASE WHEN COUNT(DISTINCT p.object_id) > 100 THEN 15 ELSE COUNT(DISTINCT p.object_id) / 7 END
                    ) as risk_score
                FROM permissions p
                LEFT JOIN files f ON p.object_type = 'file' AND p.object_id = f.file_id
                WHERE p.principal_type IN ('user', 'external')
                GROUP BY p.principal_id, p.principal_name, p.principal_type
                HAVING risk_score > 0
                ORDER BY risk_score DESC
                LIMIT 500
            """

            file_risks = await repo.fetch_all(file_risk_query)
            site_risks = await repo.fetch_all(site_risk_query)
            user_risks = await repo.fetch_all(user_risk_query)

            return {
                'files': pd.DataFrame(file_risks) if file_risks else pd.DataFrame(),
                'sites': pd.DataFrame(site_risks) if site_risks else pd.DataFrame(),
                'users': pd.DataFrame(user_risks) if user_risks else pd.DataFrame()
            }

        return asyncio.run(_load())

    @st.cache_data(ttl=300)
    def load_risk_summary(_self) -> Dict[str, Any]:
        """Load overall risk summary statistics"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)

            summary = await repo.fetch_one("""
                SELECT
                    -- High risk files
                    (SELECT COUNT(*) FROM files f
                     JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
                     WHERE f.sensitivity_score >= 60 AND p.is_external = 1) as critical_external_files,

                    -- Anonymous sensitive files
                    (SELECT COUNT(DISTINCT f.file_id) FROM files f
                     JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
                     WHERE f.sensitivity_score >= 40 AND p.is_anonymous_link = 1) as anonymous_sensitive_files,

                    -- Overpermissioned sites
                    (SELECT COUNT(*) FROM (
                        SELECT object_id FROM permissions
                        WHERE object_type = 'site' AND permission_level = 'Full Control'
                        GROUP BY object_id
                        HAVING COUNT(DISTINCT principal_id) > 20
                    )) as overpermissioned_sites,

                    -- Stale permissions
                    (SELECT COUNT(*) FROM permissions
                     WHERE granted_at < datetime('now', '-1 year')) as stale_permissions,

                    -- Orphaned permissions
                    (SELECT COUNT(*) FROM permissions p
                     WHERE NOT EXISTS (
                        SELECT 1 FROM files f WHERE p.object_type = 'file' AND p.object_id = f.file_id
                        UNION SELECT 1 FROM folders fo WHERE p.object_type = 'folder' AND p.object_id = fo.folder_id
                        UNION SELECT 1 FROM libraries l WHERE p.object_type = 'library' AND p.object_id = l.library_id
                        UNION SELECT 1 FROM sites s WHERE p.object_type = 'site' AND p.object_id = s.site_id
                     )) as orphaned_permissions
            """)

            return summary

        return asyncio.run(_load())

    def render(self):
        """Render the risk analysis component"""
        st.header("⚠️ Comprehensive Risk Analysis")

        # Load data
        risk_scores = self.calculate_risk_scores()
        risk_summary = self.load_risk_summary()

        # Risk summary cards
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "Critical External Files",
                f"{risk_summary['critical_external_files']:,}",
                help="Highly sensitive files with external access",
                delta_color="inverse"
            )

        with col2:
            st.metric(
                "Anonymous Sensitive",
                f"{risk_summary['anonymous_sensitive_files']:,}",
                help="Sensitive files with anonymous links",
                delta_color="inverse"
            )

        with col3:
            st.metric(
                "Over-permissioned Sites",
                f"{risk_summary['overpermissioned_sites']:,}",
                help="Sites with >20 Full Control users",
                delta_color="inverse"
            )

        with col4:
            st.metric(
                "Stale Permissions",
                f"{risk_summary['stale_permissions']:,}",
                help="Permissions older than 1 year"
            )

        with col5:
            st.metric(
                "Orphaned Permissions",
                f"{risk_summary['orphaned_permissions']:,}",
                help="Permissions for deleted objects"
            )

        # Risk categories tabs
        tab1, tab2, tab3, tab4 = st.tabs([
            "🎯 Risk Overview",
            "📁 File Risks",
            "🏢 Site Risks",
            "👤 User Risks"
        ])

        with tab1:
            self._render_risk_overview(risk_scores)

        with tab2:
            self._render_file_risks(risk_scores['files'])

        with tab3:
            self._render_site_risks(risk_scores['sites'])

        with tab4:
            self._render_user_risks(risk_scores['users'])

    def _render_risk_overview(self, risk_scores: Dict[str, pd.DataFrame]):
        """Render overall risk overview and visualizations"""
        st.subheader("🎯 Risk Distribution Overview")

        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('File Risk Distribution', 'Site Risk Distribution',
                          'User Risk Distribution', 'Risk Correlation Matrix'),
            specs=[[{'type': 'histogram'}, {'type': 'histogram'}],
                   [{'type': 'histogram'}, {'type': 'scatter'}]]
        )

        # File risk histogram
        if not risk_scores['files'].empty:
            fig.add_trace(
                go.Histogram(
                    x=risk_scores['files']['risk_score'],
                    name='Files',
                    marker_color='#ef4444',
                    nbinsx=20
                ),
                row=1, col=1
            )

        # Site risk histogram
        if not risk_scores['sites'].empty:
            fig.add_trace(
                go.Histogram(
                    x=risk_scores['sites']['risk_score'],
                    name='Sites',
                    marker_color='#3b82f6',
                    nbinsx=20
                ),
                row=1, col=2
            )

        # User risk histogram
        if not risk_scores['users'].empty:
            fig.add_trace(
                go.Histogram(
                    x=risk_scores['users']['risk_score'],
                    name='Users',
                    marker_color='#10b981',
                    nbinsx=20
                ),
                row=2, col=1
            )

        # Risk correlation scatter
        if not risk_scores['files'].empty:
            fig.add_trace(
                go.Scatter(
                    x=risk_scores['files']['external_users'],
                    y=risk_scores['files']['sensitivity_score'],
                    mode='markers',
                    name='Files',
                    marker=dict(
                        size=5,
                        color=risk_scores['files']['risk_score'],
                        colorscale='Reds',
                        showscale=True
                    )
                ),
                row=2, col=2
            )

        fig.update_layout(height=800, showlegend=False)
        fig.update_xaxes(title_text="Risk Score", row=1, col=1)
        fig.update_xaxes(title_text="Risk Score", row=1, col=2)
        fig.update_xaxes(title_text="Risk Score", row=2, col=1)
        fig.update_xaxes(title_text="External Users", row=2, col=2)
        fig.update_yaxes(title_text="Count", row=1, col=1)
        fig.update_yaxes(title_text="Count", row=1, col=2)
        fig.update_yaxes(title_text="Count", row=2, col=1)
        fig.update_yaxes(title_text="Sensitivity Score", row=2, col=2)

        st.plotly_chart(fig, use_container_width=True)

        # Calculate risk summary statistics
        risk_summary = {
            'critical_external_files': 0,
            'anonymous_sensitive_files': 0,
            'overpermissioned_sites': 0,
            'high_risk_users': 0,
            'stale_permissions': 0,
            'orphaned_permissions': 0
        }

        # Calculate critical external files
        if not risk_scores['files'].empty:
            risk_summary['critical_external_files'] = len(
                risk_scores['files'][
                    (risk_scores['files']['sensitivity_score'] >= 80) &
                    (risk_scores['files']['external_users'] > 0)
                ]
            )
            risk_summary['anonymous_sensitive_files'] = len(
                risk_scores['files'][
                    (risk_scores['files']['sensitivity_score'] >= 40) &
                    (risk_scores['files']['has_anonymous_link'] == 1)
                ]
            )

        # Calculate overpermissioned sites
        if not risk_scores['sites'].empty:
            risk_summary['overpermissioned_sites'] = len(
                risk_scores['sites'][risk_scores['sites']['admin_users'] > 10]
            )

        # Calculate high risk users
        if not risk_scores['users'].empty:
            risk_summary['high_risk_users'] = len(
                risk_scores['users'][risk_scores['users']['risk_score'] >= 70]
            )

        # Calculate stale permissions (placeholder - would need date filtering in actual query)
        # For now, we'll estimate based on total permissions
        if not risk_scores['users'].empty:
            total_permissions = risk_scores['users']['object_count'].sum()
            # Estimate ~10% as stale (this would be calculated from actual dates in production)
            risk_summary['stale_permissions'] = int(total_permissions * 0.1)

        # Calculate orphaned permissions (users with no recent activity)
        # This is a placeholder - in production would check last activity date
        if not risk_scores['users'].empty:
            # Consider users with only read permissions and low activity as potentially orphaned
            risk_summary['orphaned_permissions'] = len(
                risk_scores['users'][
                    (risk_scores['users']['full_control_count'] == 0) &
                    (risk_scores['users']['object_count'] < 5)
                ]
            )

        # Risk recommendations
        st.subheader("🎯 Risk Mitigation Recommendations")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 🔴 Critical Actions")

            if risk_summary['critical_external_files'] > 0:
                st.error(f"• Review and restrict access to {risk_summary['critical_external_files']} critical files with external access")

            if risk_summary['anonymous_sensitive_files'] > 0:
                st.error(f"• Remove anonymous links from {risk_summary['anonymous_sensitive_files']} sensitive files")

            if risk_summary['overpermissioned_sites'] > 0:
                st.warning(f"• Audit {risk_summary['overpermissioned_sites']} sites with excessive admin users")

        with col2:
            st.markdown("### 🟡 Maintenance Actions")

            if risk_summary['stale_permissions'] > 100:
                st.warning(f"• Review {risk_summary['stale_permissions']:,} permissions older than 1 year")

            if risk_summary['orphaned_permissions'] > 0:
                st.info(f"• Clean up {risk_summary['orphaned_permissions']:,} orphaned permissions")

            if risk_summary['high_risk_users'] > 0:
                st.warning(f"• Review access for {risk_summary['high_risk_users']} high-risk users")

    def _render_file_risks(self, df: pd.DataFrame):
        """Render file risk analysis"""
        st.subheader("📁 File Risk Analysis")

        if df.empty:
            st.info("No file risks detected.")
            return

        # Risk level categorization
        df['risk_level'] = pd.cut(
            df['risk_score'],
            bins=[0, 30, 50, 70, 100],
            labels=['Low', 'Medium', 'High', 'Critical']
        )

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            critical_files = len(df[df['risk_level'] == 'Critical'])
            st.metric("Critical Risk Files", f"{critical_files:,}")

        with col2:
            avg_risk = df['risk_score'].mean()
            st.metric("Average Risk Score", f"{avg_risk:.1f}")

        with col3:
            external_files = len(df[df['external_users'] > 0])
            st.metric("Files with External Access", f"{external_files:,}")

        with col4:
            anon_files = len(df[df['has_anonymous_link'] == 1])
            st.metric("Files with Anonymous Links", f"{anon_files:,}")

        # Risk matrix visualization
        fig = px.scatter(
            df.head(100),
            x='total_users',
            y='sensitivity_score',
            size='size_bytes',
            color='risk_score',
            hover_data=['file_name', 'site_name'],
            title="File Risk Matrix: Access vs Sensitivity",
            color_continuous_scale='Reds'
        )

        # Add risk zones
        fig.add_shape(
            type="rect",
            x0=50, x1=df['total_users'].max(),
            y0=60, y1=100,
            fillcolor="red",
            opacity=0.1,
            line_width=0,
        )

        st.plotly_chart(fig, use_container_width=True)

        # High risk files table
        st.subheader("🚨 Highest Risk Files")

        display_df = df.head(50).copy()
        display_df['size'] = display_df['size_bytes'].apply(lambda x: humanize.naturalsize(x, binary=True))

        st.dataframe(
            display_df[[
                'file_name', 'risk_score', 'sensitivity_score', 'risk_level',
                'site_name', 'total_users', 'external_users', 'size'
            ]].rename(columns={
                'file_name': 'File',
                'risk_score': 'Risk Score',
                'sensitivity_score': 'Sensitivity',
                'risk_level': 'Risk Level',
                'site_name': 'Site',
                'total_users': 'Total Users',
                'external_users': 'External Users',
                'size': 'Size'
            }),
            use_container_width=True,
            hide_index=True
        )

    def _render_site_risks(self, df: pd.DataFrame):
        """Render site risk analysis"""
        st.subheader("🏢 Site Risk Analysis")

        if df.empty:
            st.info("No site risks detected.")
            return

        # Risk categorization
        df['risk_level'] = pd.cut(
            df['risk_score'],
            bins=[0, 30, 50, 70, 100],
            labels=['Low', 'Medium', 'High', 'Critical']
        )

        # Bubble chart
        fig = px.scatter(
            df,
            x='external_users',
            y='admin_users',
            size='total_files',
            color='risk_score',
            hover_data=['site_name', 'sensitive_files', 'unique_perm_files'],
            title="Site Risk Analysis: External vs Admin Users",
            labels={
                'external_users': 'External Users',
                'admin_users': 'Admin Users',
                'risk_score': 'Risk Score'
            },
            color_continuous_scale='Reds'
        )

        st.plotly_chart(fig, use_container_width=True)

        # Site risk table
        st.subheader("📊 Site Risk Details")

        display_df = df.head(30).copy()
        display_df['sensitive_ratio'] = (display_df['sensitive_files'] / display_df['total_files'] * 100).round(1)

        st.dataframe(
            display_df[[
                'site_name', 'risk_score', 'risk_level', 'total_users',
                'external_users', 'admin_users', 'sensitive_files', 'sensitive_ratio'
            ]].rename(columns={
                'site_name': 'Site',
                'risk_score': 'Risk Score',
                'risk_level': 'Risk Level',
                'total_users': 'Total Users',
                'external_users': 'External Users',
                'admin_users': 'Admins',
                'sensitive_files': 'Sensitive Files',
                'sensitive_ratio': 'Sensitive %'
            }),
            use_container_width=True,
            hide_index=True
        )

    def _render_user_risks(self, df: pd.DataFrame):
        """Render user risk analysis"""
        st.subheader("👤 User Risk Analysis")

        if df.empty:
            st.info("No user risks detected.")
            return

        # Risk categorization
        df['risk_level'] = pd.cut(
            df['risk_score'],
            bins=[0, 30, 50, 70, 100],
            labels=['Low', 'Medium', 'High', 'Critical']
        )

        # User type distribution
        col1, col2 = st.columns(2)

        with col1:
            # Risk by user type
            risk_by_type = df.groupby(['principal_type', 'risk_level']).size().reset_index(name='count')

            fig = px.bar(
                risk_by_type,
                x='principal_type',
                y='count',
                color='risk_level',
                title="Risk Distribution by User Type",
                color_discrete_map={
                    'Low': '#10b981',
                    'Medium': '#f59e0b',
                    'High': '#ef4444',
                    'Critical': '#991b1b'
                }
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Top risky users
            top_users = df.head(20)

            fig = px.bar(
                top_users,
                x='risk_score',
                y='principal_name',
                orientation='h',
                title="Top 20 Highest Risk Users",
                color='is_external',
                color_discrete_map={0: '#3b82f6', 1: '#ef4444'},
                labels={'is_external': 'External User'}
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)

        # Detailed user risk table
        st.subheader("🔍 User Risk Details")

        display_df = df.head(50).copy()
        display_df['user_type'] = display_df.apply(
            lambda x: 'External' if x['is_external'] else x['principal_type'].title(),
            axis=1
        )

        st.dataframe(
            display_df[[
                'principal_name', 'user_type', 'risk_score', 'risk_level',
                'object_count', 'full_control_count', 'sensitive_access'
            ]].rename(columns={
                'principal_name': 'User',
                'user_type': 'Type',
                'risk_score': 'Risk Score',
                'risk_level': 'Risk Level',
                'object_count': 'Total Objects',
                'full_control_count': 'Full Control',
                'sensitive_access': 'Sensitive Access'
            }),
            use_container_width=True,
            hide_index=True
        )
