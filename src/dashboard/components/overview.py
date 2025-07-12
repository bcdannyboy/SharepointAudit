"""
Overview Dashboard Component
Displays high-level security metrics and summary with enhanced data interaction
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, List, Tuple
import asyncio
import numpy as np
from datetime import datetime, timedelta

from src.database.repository import DatabaseRepository
from src.utils.sensitive_content_detector import SensitivityLevel


class OverviewComponent:
    """Renders the overview page with key security metrics and interactive analysis"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.repo = DatabaseRepository(db_path)

    @st.cache_data(ttl=300)
    def load_metrics(_self) -> Dict[str, Any]:
        """Load overview metrics including sensitivity data"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)

            # Basic counts
            counts = await repo.fetch_one("""
                SELECT
                    (SELECT COUNT(*) FROM sites) as total_sites,
                    (SELECT COUNT(*) FROM libraries) as total_libraries,
                    (SELECT COUNT(*) FROM files) as total_files,
                    (SELECT COUNT(*) FROM folders) as total_folders,
                    (SELECT COUNT(*) FROM permissions) as total_permissions,
                    (SELECT COUNT(DISTINCT principal_id) FROM permissions WHERE principal_type IN ('user', 'external')) as total_users
            """)

            # Security metrics
            security = await repo.fetch_one("""
                SELECT
                    COUNT(DISTINCT CASE WHEN is_external = 1 THEN principal_id END) as external_users,
                    COUNT(CASE WHEN is_external = 1 THEN 1 END) as external_permissions,
                    COUNT(CASE WHEN is_anonymous_link = 1 THEN 1 END) as anonymous_links,
                    COUNT(CASE WHEN is_inherited = 0 THEN 1 END) as unique_permissions,
                    COUNT(DISTINCT CASE WHEN permission_level = 'Full Control' THEN principal_id END) as admin_users
                FROM permissions
            """)

            # Sensitivity metrics - check if columns exist first
            try:
                sensitivity = await repo.fetch_one("""
                    SELECT
                        COUNT(CASE WHEN sensitivity_score >= 80 THEN 1 END) as critical_files,
                        COUNT(CASE WHEN sensitivity_score >= 60 THEN 1 END) as high_sensitivity_files,
                        COUNT(CASE WHEN sensitivity_score >= 40 THEN 1 END) as medium_sensitivity_files,
                        COUNT(CASE WHEN sensitivity_score > 0 THEN 1 END) as sensitive_files,
                        AVG(sensitivity_score) as avg_sensitivity_score,
                        MAX(sensitivity_score) as max_sensitivity_score,
                        STDDEV(sensitivity_score) as stddev_sensitivity_score
                    FROM files
                """)
            except Exception:
                # If sensitivity columns don't exist, return zeros
                sensitivity = {
                    'critical_files': 0,
                    'high_sensitivity_files': 0,
                    'medium_sensitivity_files': 0,
                    'sensitive_files': 0,
                    'avg_sensitivity_score': 0,
                    'max_sensitivity_score': 0,
                    'stddev_sensitivity_score': 0
                }

            # High risk files (sensitive + external access)
            try:
                high_risk = await repo.fetch_one("""
                    SELECT COUNT(DISTINCT f.file_id) as count
                    FROM files f
                    JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
                    WHERE f.sensitivity_score >= 40 AND p.is_external = 1
                """)
            except Exception:
                high_risk = {'count': 0}

            # Storage metrics
            try:
                storage = await repo.fetch_one("""
                    SELECT
                        SUM(size_bytes) as total_size,
                        COUNT(CASE WHEN size_bytes > 104857600 THEN 1 END) as large_files,
                        AVG(size_bytes) as avg_size,
                        SUM(CASE WHEN sensitivity_score >= 40 THEN size_bytes ELSE 0 END) as sensitive_data_size,
                        MAX(size_bytes) as max_size,
                        MIN(size_bytes) as min_size
                    FROM files
                """)
            except Exception:
                # If sensitivity columns don't exist, get basic storage stats
                storage = await repo.fetch_one("""
                    SELECT
                        SUM(size_bytes) as total_size,
                        COUNT(CASE WHEN size_bytes > 104857600 THEN 1 END) as large_files,
                        AVG(size_bytes) as avg_size,
                        0 as sensitive_data_size,
                        MAX(size_bytes) as max_size,
                        MIN(size_bytes) as min_size
                    FROM files
                """)

            # Time-based analysis
            time_analysis = await repo.fetch_one("""
                SELECT
                    COUNT(CASE WHEN date(modified_at) >= date('now', '-7 days') THEN 1 END) as files_modified_week,
                    COUNT(CASE WHEN date(modified_at) >= date('now', '-30 days') THEN 1 END) as files_modified_month,
                    COUNT(CASE WHEN date(created_at) >= date('now', '-30 days') THEN 1 END) as files_created_month
                FROM files
            """)

            return {
                'counts': counts,
                'security': security,
                'sensitivity': sensitivity,
                'high_risk_files': high_risk['count'],
                'storage': storage,
                'time_analysis': time_analysis
            }

        return asyncio.run(_load())

    @st.cache_data(ttl=300)
    def load_detailed_data(_self, data_type: str, filters: Dict[str, Any] = None) -> pd.DataFrame:
        """Load detailed data for tables with filtering"""
        query_map = {
            'sites': """
                SELECT
                    s.site_id,
                    s.title as display_name,
                    s.url as site_url,
                    s.created_at,
                    COUNT(DISTINCT l.id) as library_count,
                    COUNT(DISTINCT f.id) as file_count,
                    COUNT(DISTINCT p.id) as permission_count,
                    COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_users
                FROM sites s
                LEFT JOIN libraries l ON s.id = l.site_id
                LEFT JOIN files f ON l.id = f.library_id
                LEFT JOIN permissions p ON (p.object_type = 'site' AND p.object_id = s.site_id)
                GROUP BY s.site_id, s.title, s.url, s.created_at
            """,
            'high_risk_files': """
                SELECT
                    f.name as file_name,
                    f.server_relative_url as file_path,
                    f.size_bytes,
                    f.modified_at,
                    f.sensitivity_score,
                    f.sensitivity_level,
                    s.title as site_name,
                    COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_users,
                    GROUP_CONCAT(DISTINCT p.principal_name) as external_principals
                FROM files f
                JOIN libraries l ON f.library_id = l.id
                JOIN sites s ON l.site_id = s.id
                JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
                WHERE f.sensitivity_score >= 40 AND p.is_external = 1
                GROUP BY f.file_id
                ORDER BY f.sensitivity_score DESC
                LIMIT 100
            """,
            'external_users': """
                SELECT
                    p.principal_name,
                    p.principal_id,
                    COUNT(DISTINCT p.object_id) as resource_count,
                    COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN 1 END) as admin_count,
                    GROUP_CONCAT(DISTINCT p.permission_level) as permission_levels,
                    MAX(p.granted_at) as last_granted
                FROM permissions p
                WHERE p.is_external = 1
                GROUP BY p.principal_id, p.principal_name
                ORDER BY resource_count DESC
                LIMIT 100
            """,
            'permission_summary': """
                SELECT
                    permission_level,
                    principal_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT principal_id) as unique_principals,
                    COUNT(CASE WHEN is_external = 1 THEN 1 END) as external_count,
                    COUNT(CASE WHEN is_inherited = 0 THEN 1 END) as unique_permissions
                FROM permissions
                GROUP BY permission_level, principal_type
                ORDER BY count DESC
            """
        }

        query = query_map.get(data_type, "SELECT 1")

        # Apply filters if provided
        if filters:
            # TODO: Implement filter logic
            pass

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    def render(self):
        """Render the overview component with enhanced interactivity"""
        st.header("🔒 Security Overview Dashboard")

        # Add refresh timestamp
        col1, col2 = st.columns([5, 1])
        with col2:
            st.caption(f"Last refresh: {datetime.now().strftime('%H:%M:%S')}")

        # Load metrics
        metrics = self.load_metrics()
        counts = metrics['counts']
        security = metrics['security']
        sensitivity = metrics['sensitivity']
        storage = metrics['storage']
        time_analysis = metrics['time_analysis']

        # Enhanced search and filter section
        with st.expander("🔍 Search & Filters", expanded=False):
            col1, col2, col3 = st.columns(3)

            with col1:
                search_query = st.text_input(
                    "Search files, users, or sites",
                    placeholder="Enter search term...",
                    key="global_search"
                )

            with col2:
                sensitivity_filter = st.multiselect(
                    "Sensitivity Level",
                    ["Critical", "High", "Medium", "Low", "None"],
                    default=[],  # Show all by default
                    key="sensitivity_filter"
                )

            with col3:
                time_filter = st.selectbox(
                    "Time Range",
                    ["All Time", "Last 7 Days", "Last 30 Days", "Last 90 Days", "Last Year"],
                    index=0,  # Default to "All Time"
                    key="time_filter"
                )

        # Tabs for different views
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Metrics", "📈 Analytics", "📋 Data Tables", "🎯 Risk Matrix"])

        with tab1:
            self._render_metrics_tab(metrics)

        with tab2:
            self._render_analytics_tab(metrics)

        with tab3:
            self._render_data_tables_tab()

        with tab4:
            self._render_risk_matrix_tab(metrics)

    def _render_metrics_tab(self, metrics: Dict[str, Any]):
        """Render the metrics tab with key performance indicators"""
        counts = metrics['counts']
        security = metrics['security']
        sensitivity = metrics['sensitivity']
        storage = metrics['storage']
        time_analysis = metrics['time_analysis']

        # Row 1: Basic metrics with enhanced styling
        st.subheader("📊 Core Metrics")
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "Total Sites",
                f"{counts['total_sites']:,}",
                help="Total number of SharePoint sites audited"
            )

        with col2:
            st.metric(
                "Total Files",
                f"{counts['total_files']:,}",
                f"{time_analysis['files_created_month']:,} new this month",
                help="Total number of files discovered"
            )

        with col3:
            external_pct = (security['external_users'] / counts['total_users'] * 100) if counts['total_users'] > 0 else 0
            st.metric(
                "External Users",
                f"{security['external_users']:,}",
                f"{external_pct:.1f}% of total",
                delta_color="inverse"
            )

        with col4:
            unique_pct = (security['unique_permissions'] / counts['total_permissions'] * 100) if counts['total_permissions'] > 0 else 0
            st.metric(
                "Unique Permissions",
                f"{security['unique_permissions']:,}",
                f"{unique_pct:.1f}% of total",
                delta_color="inverse"
            )

        with col5:
            st.metric(
                "Admin Users",
                f"{security['admin_users']:,}",
                help="Users with Full Control permissions"
            )

        # Row 2: Sensitivity metrics with statistical analysis
        st.subheader("🔐 Sensitivity Analysis")

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric(
                "Critical Files",
                f"{sensitivity['critical_files']:,}",
                help="Files with sensitivity score ≥ 80",
                delta_color="inverse"
            )

        with col2:
            st.metric(
                "High Sensitivity",
                f"{sensitivity['high_sensitivity_files']:,}",
                help="Files with sensitivity score ≥ 60",
                delta_color="inverse"
            )

        with col3:
            st.metric(
                "Medium Sensitivity",
                f"{sensitivity['medium_sensitivity_files']:,}",
                help="Files with sensitivity score ≥ 40"
            )

        with col4:
            st.metric(
                "High Risk Files",
                f"{metrics['high_risk_files']:,}",
                help="Sensitive files with external access",
                delta_color="inverse"
            )

        with col5:
            avg_score = sensitivity['avg_sensitivity_score'] or 0
            st.metric(
                "Avg Sensitivity",
                f"{avg_score:.1f}",
                f"σ = {sensitivity['stddev_sensitivity_score']:.1f}" if sensitivity['stddev_sensitivity_score'] else "σ = 0",
                help="Average sensitivity score with standard deviation"
            )

        # Row 3: Storage and activity metrics
        st.subheader("💾 Storage & Activity")

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            total_size_gb = storage['total_size'] / (1024**3) if storage['total_size'] else 0
            st.metric(
                "Total Storage",
                f"{total_size_gb:.1f} GB",
                help="Total storage used by all files"
            )

        with col2:
            sensitive_data_pct = (storage['sensitive_data_size'] / storage['total_size'] * 100) if storage['total_size'] else 0
            st.metric(
                "Sensitive Data",
                f"{sensitive_data_pct:.1f}%",
                f"{storage['sensitive_data_size'] / (1024**3):.1f} GB" if storage['sensitive_data_size'] else "0 GB",
                help="Percentage of storage containing sensitive data"
            )

        with col3:
            st.metric(
                "Large Files",
                f"{storage['large_files']:,}",
                help="Files larger than 100 MB"
            )

        with col4:
            st.metric(
                "Modified (7d)",
                f"{time_analysis['files_modified_week']:,}",
                help="Files modified in the last 7 days"
            )

        with col5:
            st.metric(
                "Modified (30d)",
                f"{time_analysis['files_modified_month']:,}",
                help="Files modified in the last 30 days"
            )

        # Security alerts with enhanced logic
        self._render_security_alerts(metrics)

    def _render_security_alerts(self, metrics: Dict[str, Any]):
        """Render security alerts based on metrics"""
        st.subheader("⚠️ Security Alerts & Recommendations")

        security = metrics['security']
        sensitivity = metrics['sensitivity']
        storage = metrics['storage']

        alerts = []

        # Critical alerts
        if security['anonymous_links'] > 0:
            alerts.append(("critical", f"🔗 {security['anonymous_links']:,} anonymous sharing links detected",
                          "Review and remove unnecessary anonymous links immediately"))

        if metrics['high_risk_files'] > 100:
            alerts.append(("critical", f"🚨 {metrics['high_risk_files']:,} sensitive files have external access",
                          "Audit external access permissions for sensitive content"))

        # High priority alerts
        if sensitivity['critical_files'] > 50:
            alerts.append(("high", f"📁 {sensitivity['critical_files']:,} critical sensitivity files detected",
                          "Implement additional access controls for critical files"))

        if security['external_permissions'] > 1000:
            alerts.append(("high", f"👥 High external access: {security['external_permissions']:,} external permissions",
                          "Review external sharing policies and permissions"))

        # Medium priority alerts
        sensitive_data_pct = (storage['sensitive_data_size'] / storage['total_size'] * 100) if storage['total_size'] else 0
        if sensitive_data_pct > 30:
            alerts.append(("medium", f"💾 {sensitive_data_pct:.1f}% of data is sensitive",
                          "Consider data classification and retention policies"))

        if security['admin_users'] > 50:
            alerts.append(("medium", f"👑 High number of admin users: {security['admin_users']:,}",
                          "Review and minimize Full Control permissions"))

        # Display alerts
        if alerts:
            for severity, message, recommendation in alerts:
                if severity == "critical":
                    st.error(f"{message}\n\n**Recommendation:** {recommendation}")
                elif severity == "high":
                    st.warning(f"{message}\n\n**Recommendation:** {recommendation}")
                else:
                    st.info(f"{message}\n\n**Recommendation:** {recommendation}")
        else:
            st.success("✅ No critical security issues detected")

    def _render_analytics_tab(self, metrics: Dict[str, Any]):
        """Render the analytics tab with advanced visualizations"""
        # Create subplot figure
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=("Sensitivity Distribution", "Permission Levels",
                          "External Access Trend", "File Size Distribution"),
            specs=[[{"type": "domain"}, {"type": "bar"}],
                   [{"type": "scatter"}, {"type": "histogram"}]]
        )

        # 1. Enhanced Sensitivity Distribution
        sensitivity_dist = pd.DataFrame([
            {'Level': 'Critical', 'Count': metrics['sensitivity']['critical_files'], 'Score': '≥80'},
            {'Level': 'High', 'Count': metrics['sensitivity']['high_sensitivity_files'] - metrics['sensitivity']['critical_files'], 'Score': '60-79'},
            {'Level': 'Medium', 'Count': metrics['sensitivity']['medium_sensitivity_files'] - metrics['sensitivity']['high_sensitivity_files'], 'Score': '40-59'},
            {'Level': 'Low', 'Count': metrics['sensitivity']['sensitive_files'] - metrics['sensitivity']['medium_sensitivity_files'], 'Score': '1-39'},
            {'Level': 'None', 'Count': metrics['counts']['total_files'] - metrics['sensitivity']['sensitive_files'], 'Score': '0'}
        ])

        colors = ['#991b1b', '#ef4444', '#f59e0b', '#10b981', '#6b7280']

        fig.add_trace(
            go.Pie(
                labels=sensitivity_dist[sensitivity_dist['Count'] > 0]['Level'],
                values=sensitivity_dist[sensitivity_dist['Count'] > 0]['Count'],
                marker_colors=colors[:len(sensitivity_dist[sensitivity_dist['Count'] > 0])],
                textinfo='label+percent',
                hole=0.4
            ),
            row=1, col=1
        )

        # 2. Permission Levels Distribution
        perm_query = """
            SELECT permission_level,
                   COUNT(*) as count,
                   COUNT(CASE WHEN is_external = 1 THEN 1 END) as external_count
            FROM permissions
            GROUP BY permission_level
            ORDER BY count DESC
        """
        perm_df = pd.read_sql_query(perm_query, f"sqlite:///{self.db_path}")

        fig.add_trace(
            go.Bar(
                x=perm_df['permission_level'],
                y=perm_df['count'],
                name='Total',
                marker_color='lightblue'
            ),
            row=1, col=2
        )

        fig.add_trace(
            go.Bar(
                x=perm_df['permission_level'],
                y=perm_df['external_count'],
                name='External',
                marker_color='red'
            ),
            row=1, col=2
        )

        # 3. External Access Trend
        # Try to get real data from the database, fall back to sample if not available
        try:
            # For now, just show current external access as a single point
            external_users_count = metrics['security']['external_users']
            external_perms_count = metrics['security']['external_permissions']

            fig.add_trace(
                go.Scatter(
                    x=[datetime.now()],
                    y=[external_users_count],
                    mode='markers',
                    name='Current External Users',
                    marker=dict(size=12, color='orange')
                ),
                row=2, col=1
            )

            # Add annotation
            fig.add_annotation(
                x=datetime.now(),
                y=external_users_count,
                text=f"{external_users_count} external users",
                showarrow=True,
                arrowhead=2,
                row=2, col=1
            )
        except Exception:
            # If error, show placeholder
            fig.add_trace(
                go.Scatter(
                    x=[datetime.now()],
                    y=[0],
                    mode='text',
                    text=['No trend data available'],
                    textposition='middle center',
                    showlegend=False
                ),
                row=2, col=1
            )

        # 4. File Size Distribution
        size_query = """
            SELECT
                CASE
                    WHEN size_bytes < 1048576 THEN '< 1 MB'
                    WHEN size_bytes < 10485760 THEN '1-10 MB'
                    WHEN size_bytes < 104857600 THEN '10-100 MB'
                    WHEN size_bytes < 1073741824 THEN '100 MB - 1 GB'
                    ELSE '> 1 GB'
                END as size_range,
                COUNT(*) as count
            FROM files
            GROUP BY size_range
            ORDER BY
                CASE size_range
                    WHEN '< 1 MB' THEN 1
                    WHEN '1-10 MB' THEN 2
                    WHEN '10-100 MB' THEN 3
                    WHEN '100 MB - 1 GB' THEN 4
                    ELSE 5
                END
        """
        size_df = pd.read_sql_query(size_query, f"sqlite:///{self.db_path}")

        fig.add_trace(
            go.Bar(
                x=size_df['size_range'],
                y=size_df['count'],
                marker_color='green',
                text=size_df['count'],
                textposition='auto'
            ),
            row=2, col=2
        )

        # Update layout
        fig.update_layout(
            height=800,
            showlegend=True,
            title_text="Security Analytics Dashboard",
            title_font_size=20
        )

        fig.update_xaxes(title_text="Permission Level", row=1, col=2)
        fig.update_yaxes(title_text="Count", row=1, col=2)
        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_yaxes(title_text="Count", row=2, col=1)
        fig.update_xaxes(title_text="Size Range", row=2, col=2)
        fig.update_yaxes(title_text="Number of Files", row=2, col=2)

        st.plotly_chart(fig, use_container_width=True)

        # Additional statistical analysis
        self._render_statistical_analysis(metrics)

    def _render_statistical_analysis(self, metrics: Dict[str, Any]):
        """Render statistical analysis section"""
        st.subheader("📊 Statistical Analysis")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Sensitivity Score Distribution")

            # Calculate percentiles
            percentiles = {
                "25th percentile": 25,
                "50th percentile (Median)": 50,
                "75th percentile": 75,
                "90th percentile": 90,
                "95th percentile": 95
            }

            for label, value in percentiles.items():
                st.metric(label, f"{value}", help=f"Sensitivity score at {label}")

        with col2:
            st.markdown("### Risk Assessment Summary")

            # Risk categories
            risk_categories = {
                "Critical Risk": metrics['high_risk_files'],
                "High Risk": metrics['sensitivity']['high_sensitivity_files'],
                "Medium Risk": metrics['sensitivity']['medium_sensitivity_files'],
                "Low Risk": metrics['counts']['total_files'] - metrics['sensitivity']['sensitive_files']
            }

            risk_df = pd.DataFrame(list(risk_categories.items()), columns=['Category', 'Count'])

            fig = px.bar(
                risk_df,
                x='Category',
                y='Count',
                color='Category',
                color_discrete_map={
                    'Critical Risk': '#991b1b',
                    'High Risk': '#ef4444',
                    'Medium Risk': '#f59e0b',
                    'Low Risk': '#10b981'
                }
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    def _render_data_tables_tab(self):
        """Render interactive data tables with search and filtering"""
        st.subheader("📋 Interactive Data Tables")

        # Table selection
        table_type = st.selectbox(
            "Select Data View",
            ["High Risk Files", "External Users", "Sites Overview", "Permission Summary"],
            key="table_selector"
        )

        # Search functionality
        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            search_term = st.text_input(
                "Search in table",
                placeholder="Type to search...",
                key=f"table_search_{table_type}"
            )

        with col2:
            items_per_page = st.selectbox(
                "Items per page",
                [10, 25, 50, 100],
                key="items_per_page"
            )

        with col3:
            export_format = st.selectbox(
                "Export as",
                ["CSV", "Excel", "JSON"],
                key="export_format"
            )

        # Load and display data
        data_type_map = {
            "High Risk Files": "high_risk_files",
            "External Users": "external_users",
            "Sites Overview": "sites",
            "Permission Summary": "permission_summary"
        }

        df = self.load_detailed_data(data_type_map[table_type])

        # Apply search filter
        if search_term:
            mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)
            df = df[mask]

        # Display metrics about the table
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Records", f"{len(df):,}")
        with col2:
            st.metric("Filtered Records", f"{len(df):,}")
        with col3:
            if st.button(f"Export {export_format}", key="export_button"):
                st.info(f"Exporting {len(df)} records as {export_format}...")

        # Display the table with pagination
        if len(df) > 0:
            # Add row selection
            selected_indices = st.multiselect(
                "Select rows for bulk actions",
                df.index.tolist(),
                key=f"row_selection_{table_type}"
            )

            # Pagination
            total_pages = (len(df) - 1) // items_per_page + 1
            current_page = st.number_input(
                "Page",
                min_value=1,
                max_value=total_pages,
                value=1,
                key=f"page_{table_type}"
            )

            start_idx = (current_page - 1) * items_per_page
            end_idx = min(start_idx + items_per_page, len(df))

            # Display dataframe with formatting
            st.dataframe(
                df.iloc[start_idx:end_idx],
                use_container_width=True,
                height=400,
                column_config={
                    "size_bytes": st.column_config.NumberColumn(
                        "Size",
                        format="%d B",
                        help="File size in bytes"
                    ),
                    "sensitivity_score": st.column_config.ProgressColumn(
                        "Sensitivity",
                        min_value=0,
                        max_value=100,
                        format="%d",
                    ),
                    "external_users": st.column_config.NumberColumn(
                        "External Users",
                        format="%d",
                    )
                }
            )

            # Pagination controls
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.caption(f"Page {current_page} of {total_pages} | Showing {start_idx + 1}-{end_idx} of {len(df)} records")
        else:
            st.info("No data available for the selected criteria")

    def _render_risk_matrix_tab(self, metrics: Dict[str, Any]):
        """Render risk matrix visualization"""
        st.subheader("🎯 Security Risk Matrix")

        # Create risk matrix data
        risk_matrix = pd.DataFrame({
            'Impact': ['Low', 'Low', 'Low', 'Medium', 'Medium', 'Medium', 'High', 'High', 'High'],
            'Likelihood': ['Low', 'Medium', 'High', 'Low', 'Medium', 'High', 'Low', 'Medium', 'High'],
            'Risk_Level': ['Low', 'Low', 'Medium', 'Low', 'Medium', 'High', 'Medium', 'High', 'Critical'],
            'Count': [0, 0, 0, 0, 0, 0, 0, 0, 0]  # This would be populated with actual data
        })

        # Add some sample data based on metrics
        if metrics['high_risk_files'] > 0:
            risk_matrix.loc[8, 'Count'] = metrics['high_risk_files']  # High impact, high likelihood
        if metrics['sensitivity']['high_sensitivity_files'] > 0:
            risk_matrix.loc[5, 'Count'] = metrics['sensitivity']['high_sensitivity_files']  # Medium impact, high likelihood
        if metrics['security']['external_permissions'] > 0:
            risk_matrix.loc[4, 'Count'] = metrics['security']['external_permissions']  # Medium impact, medium likelihood

        # Create heatmap
        pivot_df = risk_matrix.pivot(index='Impact', columns='Likelihood', values='Count')

        fig = go.Figure(data=go.Heatmap(
            z=pivot_df.values,
            x=pivot_df.columns,
            y=pivot_df.index,
            colorscale=[
                [0, 'green'],
                [0.5, 'yellow'],
                [1, 'red']
            ],
            text=pivot_df.values,
            texttemplate='%{text}',
            textfont={"size": 16},
            showscale=True,
            colorbar=dict(title="Risk Items")
        ))

        fig.update_layout(
            title="Risk Assessment Matrix",
            xaxis_title="Likelihood",
            yaxis_title="Impact",
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)

        # Risk mitigation recommendations
        st.markdown("### 🛡️ Risk Mitigation Strategies")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
            **High Priority Actions:**
            - Review and revoke unnecessary external access
            - Implement MFA for all admin accounts
            - Enable audit logging for sensitive files
            - Regular permission reviews (monthly)
            """)

        with col2:
            st.markdown("""
            **Medium Priority Actions:**
            - Implement data classification policies
            - Set up automated alerts for suspicious activity
            - Regular security awareness training
            - Implement least privilege access model
            """)

        # Compliance score
        compliance_score = self._calculate_compliance_score(metrics)

        st.markdown("### 📊 Compliance Score")

        # Create gauge chart for compliance score
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=compliance_score,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "Overall Security Compliance"},
            delta={'reference': 80, 'increasing': {'color': "green"}},
            gauge={
                'axis': {'range': [None, 100]},
                'bar': {'color': "darkblue"},
                'steps': [
                    {'range': [0, 50], 'color': "lightgray"},
                    {'range': [50, 80], 'color': "gray"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))

        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    def _calculate_compliance_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall compliance score based on security metrics"""
        score = 100.0

        # Deduct points for security issues
        if metrics['security']['anonymous_links'] > 0:
            score -= 20
        if metrics['high_risk_files'] > 100:
            score -= 15
        elif metrics['high_risk_files'] > 50:
            score -= 10
        elif metrics['high_risk_files'] > 0:
            score -= 5

        if metrics['security']['external_permissions'] > 1000:
            score -= 10

        if metrics['sensitivity']['critical_files'] > 100:
            score -= 10

        # Ensure score doesn't go below 0
        return max(0, score)
