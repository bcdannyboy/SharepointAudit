"""
Files Dashboard Component
Comprehensive file analysis, exploration, and management
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
import humanize

from src.database.repository import DatabaseRepository


class FilesComponent:
    """Comprehensive files analysis component"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.repo = DatabaseRepository(db_path)

    @st.cache_data(ttl=300)
    def load_files_data(_self, limit: int = 10000) -> pd.DataFrame:
        """Load comprehensive files data with limit for performance"""
        query = f"""
            SELECT
                f.id,
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
                f.folder_path,
                f.sensitivity_score,
                f.sensitivity_level,
                f.sensitivity_categories,
                s.title as site_name,
                s.url as site_url,
                l.name as library_name,
                fo.name as folder_name,
                COUNT(DISTINCT p.principal_id) as user_count,
                COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_user_count,
                COUNT(DISTINCT CASE WHEN p.permission_level IN ('Full Control', 'Edit') THEN p.principal_id END) as write_user_count
            FROM files f
            JOIN sites s ON f.site_id = s.id
            LEFT JOIN libraries l ON f.library_id = l.id
            LEFT JOIN folders fo ON f.folder_id = fo.id
            LEFT JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
            GROUP BY f.id
            ORDER BY f.size_bytes DESC
            LIMIT {limit}
        """

        df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

        # Calculate derived metrics
        df['size_mb'] = df['size_bytes'] / (1024 * 1024)
        df['size_gb'] = df['size_bytes'] / (1024 * 1024 * 1024)
        df['extension'] = df['name'].str.extract(r'\.([^.]+)$')[0].str.lower()
        df['is_sensitive'] = df['sensitivity_score'] >= 40
        df['has_external_access'] = df['external_user_count'] > 0
        df['risk_score'] = (
            (df['sensitivity_score'] * 0.4) +
            (df['external_user_count'] * 5).clip(0, 30) +
            (df['has_unique_permissions'].astype(int) * 10)
        ).clip(0, 100)

        # Parse dates
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['modified_at'] = pd.to_datetime(df['modified_at'])
        df['days_since_modified'] = (datetime.now() - df['modified_at']).dt.days
        df['days_since_created'] = (datetime.now() - df['created_at']).dt.days

        return df

    @st.cache_data(ttl=300)
    def load_file_type_stats(_self) -> pd.DataFrame:
        """Load file type statistics"""
        query = """
            SELECT
                LOWER(
                    CASE
                        WHEN name LIKE '%.%' THEN SUBSTR(name, INSTR(name, '.') + 1)
                        ELSE 'no_extension'
                    END
                ) as extension,
                COUNT(*) as file_count,
                SUM(size_bytes) as total_size,
                AVG(size_bytes) as avg_size,
                COUNT(CASE WHEN sensitivity_score >= 40 THEN 1 END) as sensitive_count
            FROM files
            GROUP BY extension
            ORDER BY file_count DESC
            LIMIT 50
        """

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    @st.cache_data(ttl=300)
    def load_folder_hierarchy(_self, site_id: Optional[str] = None) -> pd.DataFrame:
        """Load folder hierarchy for navigation"""
        query = """
            SELECT
                fo.id,
                fo.folder_id,
                fo.name,
                fo.server_relative_url,
                fo.parent_folder_id,
                fo.item_count,
                fo.has_unique_permissions,
                s.title as site_name,
                COUNT(DISTINCT f.id) as file_count,
                SUM(f.size_bytes) as total_size
            FROM folders fo
            JOIN sites s ON fo.site_id = s.id
            LEFT JOIN files f ON fo.id = f.folder_id
        """

        if site_id:
            query += " WHERE s.site_id = ?"
            df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}", params=(site_id,))
        else:
            query += " GROUP BY fo.id ORDER BY fo.server_relative_url"
            df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

        return df

    @st.cache_data(ttl=300)
    def find_duplicate_files(_self) -> pd.DataFrame:
        """Find potential duplicate files based on name and size"""
        query = """
            WITH duplicate_candidates AS (
                SELECT
                    name,
                    size_bytes,
                    COUNT(*) as duplicate_count
                FROM files
                WHERE size_bytes > 0
                GROUP BY name, size_bytes
                HAVING COUNT(*) > 1
            )
            SELECT
                f.file_id,
                f.name,
                f.server_relative_url,
                f.size_bytes,
                f.modified_at,
                s.title as site_name,
                dc.duplicate_count
            FROM files f
            JOIN duplicate_candidates dc ON f.name = dc.name AND f.size_bytes = dc.size_bytes
            JOIN sites s ON f.site_id = s.id
            ORDER BY dc.duplicate_count DESC, f.size_bytes DESC
        """

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    def render(self):
        """Render the files component"""
        st.header("📁 Files Analysis Dashboard")

        # Load data
        files_df = self.load_files_data()

        if files_df.empty:
            st.warning("No files data available. Please run an audit first.")
            return

        # Top metrics
        self._render_file_metrics(files_df)

        # Main content tabs
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "🔍 File Explorer",
            "📊 File Analytics",
            "📈 Size Analysis",
            "🔒 Security & Permissions",
            "📝 Version Control",
            "🔄 Duplicate Files",
            "📋 Reports"
        ])

        with tab1:
            self._render_file_explorer(files_df)

        with tab2:
            self._render_file_analytics(files_df)

        with tab3:
            self._render_size_analysis(files_df)

        with tab4:
            self._render_security_permissions(files_df)

        with tab5:
            self._render_version_control(files_df)

        with tab6:
            self._render_duplicate_files()

        with tab7:
            self._render_reports(files_df)

    def _render_file_metrics(self, df: pd.DataFrame):
        """Render top-level file metrics"""
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        total_files = len(df)
        total_size_tb = df['size_bytes'].sum() / (1024**4)

        with col1:
            st.metric(
                "Total Files",
                f"{total_files:,}",
                help="Total number of files analyzed"
            )

        with col2:
            st.metric(
                "Total Size",
                f"{total_size_tb:.2f} TB",
                f"Avg: {humanize.naturalsize(df['size_bytes'].mean())}"
            )

        with col3:
            sensitive_files = len(df[df['is_sensitive']])
            st.metric(
                "Sensitive Files",
                f"{sensitive_files:,}",
                f"{sensitive_files/total_files*100:.1f}% of total",
                delta_color="inverse"
            )

        with col4:
            external_files = len(df[df['has_external_access']])
            st.metric(
                "External Access",
                f"{external_files:,}",
                f"{external_files/total_files*100:.1f}% of files",
                delta_color="inverse"
            )

        with col5:
            checked_out = len(df[df['is_checked_out'] == True])
            st.metric(
                "Checked Out",
                f"{checked_out:,}",
                help="Files currently checked out"
            )

        with col6:
            unique_perms = len(df[df['has_unique_permissions'] == True])
            st.metric(
                "Unique Permissions",
                f"{unique_perms:,}",
                f"{unique_perms/total_files*100:.1f}% of files"
            )

    def _render_file_explorer(self, df: pd.DataFrame):
        """Render interactive file explorer"""
        st.subheader("🔍 File Explorer")

        # Search and filters
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            search_term = st.text_input(
                "Search files",
                placeholder="Enter file name or path...",
                key="file_search"
            )

        with col2:
            site_filter = st.selectbox(
                "Site",
                ["All Sites"] + sorted(df['site_name'].unique().tolist()),
                key="site_filter"
            )

        with col3:
            extension_filter = st.multiselect(
                "File Types",
                sorted(df['extension'].dropna().unique().tolist()),
                key="extension_filter"
            )

        with col4:
            size_filter = st.select_slider(
                "File Size",
                options=["All", "< 1 MB", "1-10 MB", "10-100 MB", "100 MB - 1 GB", "> 1 GB"],
                value="All",
                key="size_filter"
            )

        # Additional filters in expander
        with st.expander("Advanced Filters"):
            col1, col2, col3 = st.columns(3)

            with col1:
                sensitivity_filter = st.checkbox("Sensitive files only", key="sensitive_only")
                external_filter = st.checkbox("External access only", key="external_only")
                unique_perm_filter = st.checkbox("Unique permissions only", key="unique_perm_only")

            with col2:
                date_filter = st.date_input(
                    "Modified after",
                    value=None,
                    key="date_filter"
                )

                days_old_filter = st.number_input(
                    "Not modified for (days)",
                    min_value=0,
                    value=0,
                    key="days_old_filter"
                )

            with col3:
                user_filter = st.text_input(
                    "Modified by user",
                    placeholder="Enter username...",
                    key="user_filter"
                )

        # Apply filters
        filtered_df = df.copy()

        if search_term:
            mask = (
                filtered_df['name'].str.contains(search_term, case=False, na=False) |
                filtered_df['server_relative_url'].str.contains(search_term, case=False, na=False)
            )
            filtered_df = filtered_df[mask]

        if site_filter != "All Sites":
            filtered_df = filtered_df[filtered_df['site_name'] == site_filter]

        if extension_filter:
            filtered_df = filtered_df[filtered_df['extension'].isin(extension_filter)]

        # Size filters
        if size_filter == "< 1 MB":
            filtered_df = filtered_df[filtered_df['size_mb'] < 1]
        elif size_filter == "1-10 MB":
            filtered_df = filtered_df[(filtered_df['size_mb'] >= 1) & (filtered_df['size_mb'] < 10)]
        elif size_filter == "10-100 MB":
            filtered_df = filtered_df[(filtered_df['size_mb'] >= 10) & (filtered_df['size_mb'] < 100)]
        elif size_filter == "100 MB - 1 GB":
            filtered_df = filtered_df[(filtered_df['size_mb'] >= 100) & (filtered_df['size_gb'] < 1)]
        elif size_filter == "> 1 GB":
            filtered_df = filtered_df[filtered_df['size_gb'] >= 1]

        # Advanced filters
        if sensitivity_filter:
            filtered_df = filtered_df[filtered_df['is_sensitive'] == True]

        if external_filter:
            filtered_df = filtered_df[filtered_df['has_external_access'] == True]

        if unique_perm_filter:
            filtered_df = filtered_df[filtered_df['has_unique_permissions'] == True]

        if date_filter:
            filtered_df = filtered_df[filtered_df['modified_at'] >= pd.Timestamp(date_filter)]

        if days_old_filter > 0:
            filtered_df = filtered_df[filtered_df['days_since_modified'] >= days_old_filter]

        if user_filter:
            filtered_df = filtered_df[
                filtered_df['modified_by'].str.contains(user_filter, case=False, na=False)
            ]

        # Display results
        st.info(f"Showing {len(filtered_df):,} of {len(df):,} files")

        if not filtered_df.empty:
            # Prepare display dataframe
            display_df = filtered_df[[
                'name', 'site_name', 'library_name', 'size_mb', 'extension',
                'modified_at', 'modified_by', 'sensitivity_score', 'external_user_count'
            ]].copy()

            display_df.columns = [
                'File Name', 'Site', 'Library', 'Size (MB)', 'Type',
                'Modified', 'Modified By', 'Sensitivity', 'External Users'
            ]

            display_df['Size (MB)'] = display_df['Size (MB)'].round(2)
            display_df['Modified'] = display_df['Modified'].dt.strftime('%Y-%m-%d')

            # Interactive table with selection
            selected_indices = st.multiselect(
                "Select files for bulk actions",
                display_df.index.tolist(),
                key="file_selection"
            )

            st.dataframe(
                display_df,
                column_config={
                    "Sensitivity": st.column_config.ProgressColumn(
                        "Sensitivity",
                        min_value=0,
                        max_value=100,
                        format="%d",
                    ),
                },
                hide_index=True,
                use_container_width=True
            )

            # Bulk actions
            if selected_indices:
                st.markdown("### 🔧 Bulk Actions")
                col1, col2, col3 = st.columns(3)

                with col1:
                    if st.button("📊 Analyze Selected"):
                        selected_files = filtered_df.loc[selected_indices]
                        st.info(f"Analyzing {len(selected_files)} files...")
                        # Add analysis logic here

                with col2:
                    if st.button("📥 Export Selected"):
                        st.info("Exporting selected files data...")
                        # Add export logic here

                with col3:
                    if st.button("🔍 Deep Scan"):
                        st.info("Performing deep scan on selected files...")
                        # Add deep scan logic here
        else:
            st.info("No files match the selected filters")

    def _render_file_analytics(self, df: pd.DataFrame):
        """Render file analytics"""
        st.subheader("📊 File Analytics")

        # File type distribution
        col1, col2 = st.columns(2)

        with col1:
            # File types by count
            type_stats = self.load_file_type_stats()
            top_types = type_stats.head(15)

            fig = px.bar(
                top_types,
                x='file_count',
                y='extension',
                orientation='h',
                title="Top 15 File Types by Count",
                labels={'file_count': 'Number of Files', 'extension': 'File Type'},
                color='file_count',
                color_continuous_scale='Blues'
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # File types by size
            type_stats['total_size_gb'] = type_stats['total_size'] / (1024**3)
            top_size_types = type_stats.nlargest(15, 'total_size_gb')

            fig = px.bar(
                top_size_types,
                x='total_size_gb',
                y='extension',
                orientation='h',
                title="Top 15 File Types by Size",
                labels={'total_size_gb': 'Total Size (GB)', 'extension': 'File Type'},
                color='total_size_gb',
                color_continuous_scale='Reds'
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

        # Activity analysis
        st.markdown("### 📅 File Activity Analysis")

        col1, col2 = st.columns(2)

        with col1:
            # Files by age
            age_bins = pd.cut(
                df['days_since_modified'],
                bins=[0, 7, 30, 90, 365, float('inf')],
                labels=['< 1 week', '1-4 weeks', '1-3 months', '3-12 months', '> 1 year']
            )
            age_dist = age_bins.value_counts()

            fig = px.pie(
                values=age_dist.values,
                names=age_dist.index,
                title="Files by Last Modified Date",
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Creation vs modification timeline
            timeline_df = pd.DataFrame({
                'Created': df['created_at'].dt.to_period('M').value_counts().sort_index(),
                'Modified': df['modified_at'].dt.to_period('M').value_counts().sort_index()
            }).fillna(0)

            if not timeline_df.empty:
                fig = px.line(
                    timeline_df.reset_index(),
                    x='index',
                    y=['Created', 'Modified'],
                    title="File Creation vs Modification Timeline",
                    labels={'index': 'Month', 'value': 'File Count'}
                )
                fig.update_xaxes(tickformat='%Y-%m')
                st.plotly_chart(fig, use_container_width=True)

        # User activity
        st.markdown("### 👤 User Activity")

        # Most active users
        user_activity = df.groupby('modified_by').agg({
            'file_id': 'count',
            'size_bytes': 'sum',
            'sensitivity_score': 'mean'
        }).reset_index()

        user_activity.columns = ['User', 'Files Modified', 'Total Size', 'Avg Sensitivity']
        user_activity['Total Size GB'] = user_activity['Total Size'] / (1024**3)
        user_activity = user_activity.nlargest(20, 'Files Modified')

        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                user_activity.head(10),
                x='Files Modified',
                y='User',
                orientation='h',
                title="Top 10 Most Active Users",
                color='Avg Sensitivity',
                color_continuous_scale='YlOrRd'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(
                user_activity,
                x='Files Modified',
                y='Total Size GB',
                size='Avg Sensitivity',
                hover_data=['User'],
                title="User Activity: Files vs Storage",
                labels={'Total Size GB': 'Total Storage (GB)'}
            )
            st.plotly_chart(fig, use_container_width=True)

    def _render_size_analysis(self, df: pd.DataFrame):
        """Render file size analysis"""
        st.subheader("📈 Size Analysis")

        # Size distribution
        col1, col2 = st.columns(2)

        with col1:
            # Size histogram
            fig = px.histogram(
                df[df['size_mb'] < 1000],  # Focus on files < 1GB for clarity
                x='size_mb',
                nbins=50,
                title="File Size Distribution (< 1 GB)",
                labels={'size_mb': 'Size (MB)', 'count': 'Number of Files'}
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Size categories
            size_categories = pd.cut(
                df['size_bytes'],
                bins=[0, 1024*1024, 10*1024*1024, 100*1024*1024, 1024*1024*1024, float('inf')],
                labels=['< 1 MB', '1-10 MB', '10-100 MB', '100 MB - 1 GB', '> 1 GB']
            )
            size_cat_dist = size_categories.value_counts()

            fig = px.pie(
                values=size_cat_dist.values,
                names=size_cat_dist.index,
                title="Files by Size Category",
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)

        # Large files analysis
        st.markdown("### 🐘 Large Files Analysis")

        large_files = df[df['size_gb'] > 0.5].nlargest(20, 'size_bytes')

        if not large_files.empty:
            col1, col2 = st.columns([2, 1])

            with col1:
                fig = px.bar(
                    large_files,
                    x='size_gb',
                    y='name',
                    orientation='h',
                    title="Top 20 Largest Files",
                    labels={'size_gb': 'Size (GB)', 'name': 'File Name'},
                    hover_data=['site_name', 'modified_by'],
                    color='sensitivity_score',
                    color_continuous_scale='YlOrRd'
                )
                fig.update_layout(height=600)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.markdown("#### Large File Statistics")

                total_large_size = large_files['size_gb'].sum()
                avg_large_size = large_files['size_gb'].mean()
                sensitive_large = len(large_files[large_files['is_sensitive']])

                st.metric("Total Size", f"{total_large_size:.2f} GB")
                st.metric("Average Size", f"{avg_large_size:.2f} GB")
                st.metric("Sensitive Files", f"{sensitive_large} / {len(large_files)}")

                # Recommendations
                st.info("""
                **💡 Recommendations:**
                - Archive files not modified in >1 year
                - Compress large text/log files
                - Move to cheaper storage tier
                - Review necessity of duplicates
                """)

        # Storage efficiency
        st.markdown("### 💾 Storage Efficiency")

        # Group by site and analyze
        site_storage = df.groupby('site_name').agg({
            'size_bytes': ['sum', 'mean', 'count'],
            'sensitivity_score': 'mean'
        }).reset_index()

        site_storage.columns = ['Site', 'Total Size', 'Avg File Size', 'File Count', 'Avg Sensitivity']
        site_storage['Total Size GB'] = site_storage['Total Size'] / (1024**3)
        site_storage['Avg File Size MB'] = site_storage['Avg File Size'] / (1024**2)
        site_storage = site_storage.nlargest(15, 'Total Size GB')

        fig = px.scatter(
            site_storage,
            x='File Count',
            y='Total Size GB',
            size='Avg File Size MB',
            color='Avg Sensitivity',
            hover_data=['Site'],
            title="Storage Efficiency by Site",
            labels={'Total Size GB': 'Total Storage (GB)'},
            color_continuous_scale='YlOrRd'
        )
        st.plotly_chart(fig, use_container_width=True)

    def _render_security_permissions(self, df: pd.DataFrame):
        """Render security and permissions analysis"""
        st.subheader("🔒 Security & Permissions Analysis")

        # Security overview
        col1, col2, col3, col4 = st.columns(4)

        sensitive_external = len(df[(df['is_sensitive']) & (df['has_external_access'])])
        unique_perm_sensitive = len(df[(df['is_sensitive']) & (df['has_unique_permissions'])])

        with col1:
            st.metric(
                "High Risk Files",
                f"{sensitive_external:,}",
                "Sensitive + External Access",
                delta_color="inverse"
            )

        with col2:
            st.metric(
                "Complex Permissions",
                f"{unique_perm_sensitive:,}",
                "Sensitive + Unique Perms",
                delta_color="inverse"
            )

        with col3:
            avg_external_per_file = df[df['has_external_access']]['external_user_count'].mean()
            st.metric(
                "Avg External Users",
                f"{avg_external_per_file:.1f}",
                "Per file with external access"
            )

        with col4:
            write_access_pct = (df['write_user_count'] / df['user_count']).mean() * 100
            st.metric(
                "Write Access %",
                f"{write_access_pct:.1f}%",
                "Average across all files"
            )

        # Risk matrix
        col1, col2 = st.columns(2)

        with col1:
            # Sensitivity vs External Access
            fig = px.scatter(
                df.sample(min(1000, len(df))),  # Sample for performance
                x='external_user_count',
                y='sensitivity_score',
                size='size_mb',
                color='risk_score',
                title="File Risk Matrix: Sensitivity vs External Access",
                labels={
                    'external_user_count': 'External Users',
                    'sensitivity_score': 'Sensitivity Score'
                },
                hover_data=['name', 'site_name'],
                color_continuous_scale='YlOrRd'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Permission inheritance
            inheritance_data = pd.DataFrame({
                'Type': ['Inherited Permissions', 'Unique Permissions'],
                'Count': [
                    len(df[df['has_unique_permissions'] == False]),
                    len(df[df['has_unique_permissions'] == True])
                ]
            })

            fig = px.pie(
                inheritance_data,
                values='Count',
                names='Type',
                title="Permission Inheritance",
                color_discrete_map={
                    'Inherited Permissions': '#10b981',
                    'Unique Permissions': '#ef4444'
                }
            )
            st.plotly_chart(fig, use_container_width=True)

        # High risk files table
        st.markdown("### 🚨 High Risk Files")

        high_risk_files = df[df['risk_score'] >= 70].nlargest(50, 'risk_score')

        if not high_risk_files.empty:
            risk_display = high_risk_files[[
                'name', 'site_name', 'sensitivity_score', 'external_user_count',
                'write_user_count', 'size_mb', 'risk_score'
            ]].copy()

            risk_display.columns = [
                'File Name', 'Site', 'Sensitivity', 'External Users',
                'Write Users', 'Size (MB)', 'Risk Score'
            ]

            risk_display['Size (MB)'] = risk_display['Size (MB)'].round(2)
            risk_display['Risk Score'] = risk_display['Risk Score'].round(1)

            st.dataframe(
                risk_display,
                column_config={
                    "Sensitivity": st.column_config.ProgressColumn(
                        "Sensitivity",
                        min_value=0,
                        max_value=100,
                        format="%d",
                    ),
                    "Risk Score": st.column_config.ProgressColumn(
                        "Risk Score",
                        min_value=0,
                        max_value=100,
                        format="%.1f",
                    ),
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success("✅ No high risk files detected")

        # Security recommendations
        st.markdown("### 💡 Security Recommendations")

        recommendations = []

        if sensitive_external > 0:
            recommendations.append(
                f"🔴 **Critical**: {sensitive_external:,} sensitive files have external access. "
                "Review and restrict access immediately."
            )

        if unique_perm_sensitive > 100:
            recommendations.append(
                f"🟡 **Warning**: {unique_perm_sensitive:,} sensitive files have unique permissions. "
                "Consider standardizing permission inheritance."
            )

        old_sensitive = len(df[(df['is_sensitive']) & (df['days_since_modified'] > 365)])
        if old_sensitive > 0:
            recommendations.append(
                f"🟡 **Maintenance**: {old_sensitive:,} sensitive files haven't been modified in >1 year. "
                "Consider archiving or reviewing necessity."
            )

        if recommendations:
            for rec in recommendations:
                st.warning(rec)
        else:
            st.success("✅ No critical security issues detected")

    def _render_version_control(self, df: pd.DataFrame):
        """Render version control analysis"""
        st.subheader("📝 Version Control Analysis")

        # Checked out files
        checked_out_df = df[df['is_checked_out'] == True]

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Checked Out Files",
                f"{len(checked_out_df):,}",
                f"{len(checked_out_df)/len(df)*100:.1f}% of total"
            )

        with col2:
            if not checked_out_df.empty:
                avg_checkout_days = checked_out_df['days_since_modified'].mean()
                st.metric(
                    "Avg Checkout Duration",
                    f"{avg_checkout_days:.0f} days",
                    help="Average days since last modification"
                )

        with col3:
            if not checked_out_df.empty:
                unique_users = checked_out_df['checked_out_by'].nunique()
                st.metric(
                    "Users with Checkouts",
                    f"{unique_users}",
                    help="Number of unique users with checked out files"
                )

        if not checked_out_df.empty:
            # Checked out files by user
            checkout_by_user = checked_out_df.groupby('checked_out_by').agg({
                'file_id': 'count',
                'days_since_modified': 'mean'
            }).reset_index()

            checkout_by_user.columns = ['User', 'Files Checked Out', 'Avg Days Since Modified']
            checkout_by_user = checkout_by_user.nlargest(20, 'Files Checked Out')

            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    checkout_by_user.head(10),
                    x='Files Checked Out',
                    y='User',
                    orientation='h',
                    title="Top 10 Users with Most Checkouts",
                    color='Avg Days Since Modified',
                    color_continuous_scale='YlOrRd'
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Long-term checkouts
                long_checkouts = checked_out_df[checked_out_df['days_since_modified'] > 30]

                if not long_checkouts.empty:
                    st.markdown("#### ⚠️ Long-term Checkouts (>30 days)")

                    long_display = long_checkouts[[
                        'name', 'checked_out_by', 'days_since_modified', 'site_name'
                    ]].head(10)

                    long_display.columns = ['File', 'Checked Out By', 'Days', 'Site']
                    st.dataframe(long_display, hide_index=True)

                    st.warning(f"""
                    **Action Required**: {len(long_checkouts)} files have been checked out for >30 days.
                    Consider contacting users to check in files or force check-in if necessary.
                    """)

        # Version statistics
        st.markdown("### 📊 Version Statistics")

        # Parse version numbers (simplified - assumes format like "1.0", "2.3", etc.)
        df['version_major'] = df['version'].str.extract(r'^(\d+)')[0].astype(float)
        version_stats = df.groupby('version_major').size().reset_index(name='count')

        col1, col2 = st.columns(2)

        with col1:
            if not version_stats.empty:
                fig = px.bar(
                    version_stats.head(10),
                    x='version_major',
                    y='count',
                    title="Files by Major Version",
                    labels={'version_major': 'Major Version', 'count': 'Number of Files'}
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Files with many versions (high version numbers)
            high_version_files = df[df['version_major'] >= 10].nlargest(20, 'version_major')

            if not high_version_files.empty:
                st.markdown("#### 📈 Frequently Updated Files")

                freq_display = high_version_files[[
                    'name', 'version', 'modified_by', 'site_name'
                ]].head(10)

                freq_display.columns = ['File', 'Version', 'Last Modified By', 'Site']
                st.dataframe(freq_display, hide_index=True)

    def _render_duplicate_files(self):
        """Render duplicate files analysis"""
        st.subheader("🔄 Duplicate Files Analysis")

        duplicates_df = self.find_duplicate_files()

        if duplicates_df.empty:
            st.success("✅ No duplicate files detected")
            return

        # Duplicate statistics
        total_duplicates = len(duplicates_df)
        unique_duplicates = duplicates_df[['name', 'size_bytes']].drop_duplicates().shape[0]
        wasted_space = duplicates_df.groupby(['name', 'size_bytes'])['size_bytes'].sum().sum() - \
                       duplicates_df.groupby(['name', 'size_bytes'])['size_bytes'].first().sum()
        wasted_space_gb = wasted_space / (1024**3)

        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(
                "Duplicate Files",
                f"{total_duplicates:,}",
                help="Total number of duplicate files"
            )

        with col2:
            st.metric(
                "Unique Duplicates",
                f"{unique_duplicates:,}",
                help="Number of unique files with duplicates"
            )

        with col3:
            st.metric(
                "Wasted Space",
                f"{wasted_space_gb:.2f} GB",
                help="Potential space savings from deduplication"
            )

        # Duplicate groups
        st.markdown("### 📋 Duplicate File Groups")

        # Group duplicates
        dup_groups = duplicates_df.groupby(['name', 'size_bytes']).agg({
            'file_id': 'count',
            'site_name': lambda x: ', '.join(x.unique()[:3]) + ('...' if len(x.unique()) > 3 else '')
        }).reset_index()

        dup_groups.columns = ['File Name', 'Size', 'Count', 'Sites']
        dup_groups['Size MB'] = dup_groups['Size'] / (1024**2)
        dup_groups['Wasted Space MB'] = (dup_groups['Count'] - 1) * dup_groups['Size MB']
        dup_groups = dup_groups.nlargest(20, 'Wasted Space MB')

        display_cols = ['File Name', 'Size MB', 'Count', 'Wasted Space MB', 'Sites']
        display_df = dup_groups[display_cols].copy()
        display_df['Size MB'] = display_df['Size MB'].round(2)
        display_df['Wasted Space MB'] = display_df['Wasted Space MB'].round(2)

        st.dataframe(
            display_df,
            hide_index=True,
            use_container_width=True
        )

        # Visualization
        col1, col2 = st.columns(2)

        with col1:
            # Top duplicated files by count
            fig = px.bar(
                dup_groups.head(10),
                x='Count',
                y='File Name',
                orientation='h',
                title="Top 10 Most Duplicated Files",
                labels={'Count': 'Number of Copies'},
                color='Wasted Space MB',
                color_continuous_scale='Reds'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Duplicate distribution by site
            site_duplicates = duplicates_df.groupby('site_name').agg({
                'file_id': 'count',
                'size_bytes': 'sum'
            }).reset_index()

            site_duplicates.columns = ['Site', 'Duplicate Count', 'Total Size']
            site_duplicates['Total Size GB'] = site_duplicates['Total Size'] / (1024**3)
            site_duplicates = site_duplicates.nlargest(10, 'Duplicate Count')

            fig = px.scatter(
                site_duplicates,
                x='Duplicate Count',
                y='Total Size GB',
                size='Total Size GB',
                hover_data=['Site'],
                title="Duplicates by Site",
                labels={'Total Size GB': 'Total Duplicate Size (GB)'}
            )
            st.plotly_chart(fig, use_container_width=True)

        # Deduplication recommendations
        st.markdown("### 💡 Deduplication Strategy")

        st.info(f"""
        **Potential Savings: {wasted_space_gb:.2f} GB**

        **Recommended Actions:**
        1. **Immediate**: Review and remove obvious duplicates in the same site
        2. **Short-term**: Implement file deduplication policies
        3. **Long-term**: Consider document management system with built-in deduplication

        **Priority Targets:**
        - Large media files (videos, images) with multiple copies
        - Old backup copies of documents
        - Template files copied across multiple sites
        """)

    def _render_reports(self, df: pd.DataFrame):
        """Render file reports section"""
        st.subheader("📋 File Reports")

        report_type = st.selectbox(
            "Select Report Type",
            [
                "Executive Summary",
                "Large Files Report",
                "Sensitive Files Report",
                "Stale Files Report",
                "Permission Anomalies",
                "Storage Optimization"
            ],
            key="report_type"
        )

        if report_type == "Executive Summary":
            self._render_executive_summary(df)
        elif report_type == "Large Files Report":
            self._render_large_files_report(df)
        elif report_type == "Sensitive Files Report":
            self._render_sensitive_files_report(df)
        elif report_type == "Stale Files Report":
            self._render_stale_files_report(df)
        elif report_type == "Permission Anomalies":
            self._render_permission_anomalies_report(df)
        elif report_type == "Storage Optimization":
            self._render_storage_optimization_report(df)

        # Export options
        st.markdown("### 📥 Export Report")

        col1, col2, col3 = st.columns(3)

        with col1:
            export_format = st.selectbox(
                "Format",
                ["PDF", "Excel", "CSV", "JSON"],
                key="export_format_report"
            )

        with col2:
            include_charts = st.checkbox("Include visualizations", value=True)

        with col3:
            if st.button("Generate Report", type="primary"):
                st.info(f"Generating {report_type} in {export_format} format...")
                # Add export logic here

    def _render_executive_summary(self, df: pd.DataFrame):
        """Render executive summary report"""
        st.markdown("### 📊 Executive Summary")

        # Key metrics summary
        total_files = len(df)
        total_size_tb = df['size_bytes'].sum() / (1024**4)
        sensitive_files = len(df[df['is_sensitive']])
        external_files = len(df[df['has_external_access']])

        col1, col2 = st.columns([2, 1])

        with col1:
            st.markdown(f"""
            **File Repository Overview**

            - **Total Files**: {total_files:,}
            - **Total Storage**: {total_size_tb:.2f} TB
            - **Average File Size**: {humanize.naturalsize(df['size_bytes'].mean())}
            - **Sensitive Files**: {sensitive_files:,} ({sensitive_files/total_files*100:.1f}%)
            - **External Access**: {external_files:,} files
            - **Unique Permissions**: {len(df[df['has_unique_permissions']]):,} files

            **Key Findings**

            1. **Storage Distribution**: {df['extension'].value_counts().index[0].upper()} files dominate with {df[df['extension'] == df['extension'].value_counts().index[0]]['size_bytes'].sum() / (1024**3):.1f} GB
            2. **Security Posture**: {len(df[(df['is_sensitive']) & (df['has_external_access'])]):,} high-risk files require immediate attention
            3. **Version Control**: {len(df[df['is_checked_out']]):,} files currently checked out
            4. **File Age**: {len(df[df['days_since_modified'] > 365]):,} files not modified in over a year
            """)

        with col2:
            # Risk summary pie chart
            risk_categories = {
                'Low Risk': len(df[df['risk_score'] < 30]),
                'Medium Risk': len(df[(df['risk_score'] >= 30) & (df['risk_score'] < 70)]),
                'High Risk': len(df[df['risk_score'] >= 70])
            }

            fig = px.pie(
                values=list(risk_categories.values()),
                names=list(risk_categories.keys()),
                title="Risk Distribution",
                color_discrete_map={
                    'Low Risk': '#10b981',
                    'Medium Risk': '#f59e0b',
                    'High Risk': '#ef4444'
                }
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

        # Recommendations
        st.markdown("### 💡 Strategic Recommendations")

        recommendations = [
            "🔴 **Immediate Actions**",
            f"- Review and secure {len(df[(df['is_sensitive']) & (df['has_external_access'])]):,} sensitive files with external access",
            f"- Address {len(df[df['is_checked_out']]):,} long-term file checkouts",
            "",
            "🟡 **Short-term Improvements**",
            f"- Implement retention policy for {len(df[df['days_since_modified'] > 365]):,} stale files",
            f"- Optimize storage by deduplicating files (potential {self.find_duplicate_files()['size_bytes'].sum() / (1024**3):.1f} GB savings)",
            "",
            "🟢 **Long-term Strategy**",
            "- Standardize permission inheritance model",
            "- Implement automated sensitivity classification",
            "- Deploy file lifecycle management policies"
        ]

        for rec in recommendations:
            st.markdown(rec)

    def _render_large_files_report(self, df: pd.DataFrame):
        """Render large files report"""
        st.markdown("### 🐘 Large Files Report")

        # Define large file threshold
        threshold_gb = st.slider(
            "Large file threshold (GB)",
            min_value=0.1,
            max_value=10.0,
            value=1.0,
            step=0.1
        )

        large_files = df[df['size_gb'] >= threshold_gb].copy()

        if large_files.empty:
            st.info(f"No files larger than {threshold_gb} GB found")
            return

        # Statistics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Large Files", f"{len(large_files):,}")

        with col2:
            st.metric("Total Size", f"{large_files['size_gb'].sum():.2f} GB")

        with col3:
            st.metric("Avg Size", f"{large_files['size_gb'].mean():.2f} GB")

        with col4:
            pct_of_total = large_files['size_bytes'].sum() / df['size_bytes'].sum() * 100
            st.metric("% of Total Storage", f"{pct_of_total:.1f}%")

        # Large files by type
        large_by_type = large_files.groupby('extension').agg({
            'file_id': 'count',
            'size_gb': 'sum'
        }).reset_index()

        large_by_type.columns = ['Type', 'Count', 'Total GB']
        large_by_type = large_by_type.nlargest(10, 'Total GB')

        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                large_by_type,
                x='Total GB',
                y='Type',
                orientation='h',
                title="Large Files by Type",
                color='Count',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Age distribution of large files
            age_bins = pd.cut(
                large_files['days_since_modified'],
                bins=[0, 30, 90, 365, float('inf')],
                labels=['< 1 month', '1-3 months', '3-12 months', '> 1 year']
            )
            age_dist = age_bins.value_counts()

            fig = px.pie(
                values=age_dist.values,
                names=age_dist.index,
                title="Large Files by Age"
            )
            st.plotly_chart(fig, use_container_width=True)

        # Detailed table
        st.markdown("### 📋 Large Files Details")

        detail_df = large_files.nlargest(50, 'size_bytes')[[
            'name', 'site_name', 'size_gb', 'extension',
            'modified_at', 'modified_by', 'external_user_count'
        ]].copy()

        detail_df.columns = [
            'File Name', 'Site', 'Size (GB)', 'Type',
            'Last Modified', 'Modified By', 'External Users'
        ]

        detail_df['Size (GB)'] = detail_df['Size (GB)'].round(2)
        detail_df['Last Modified'] = detail_df['Last Modified'].dt.strftime('%Y-%m-%d')

        st.dataframe(detail_df, hide_index=True, use_container_width=True)

    def _render_sensitive_files_report(self, df: pd.DataFrame):
        """Render sensitive files report"""
        st.markdown("### 🔐 Sensitive Files Report")

        sensitive_df = df[df['is_sensitive']].copy()

        if sensitive_df.empty:
            st.info("No sensitive files detected")
            return

        # Risk analysis
        high_risk = sensitive_df[sensitive_df['has_external_access']]
        unique_perm = sensitive_df[sensitive_df['has_unique_permissions']]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Total Sensitive",
                f"{len(sensitive_df):,}",
                f"{len(sensitive_df)/len(df)*100:.1f}% of all files"
            )

        with col2:
            st.metric(
                "With External Access",
                f"{len(high_risk):,}",
                "High risk",
                delta_color="inverse"
            )

        with col3:
            st.metric(
                "Unique Permissions",
                f"{len(unique_perm):,}",
                "Complex access control"
            )

        with col4:
            total_sensitive_gb = sensitive_df['size_gb'].sum()
            st.metric(
                "Sensitive Data Size",
                f"{total_sensitive_gb:.1f} GB"
            )

        # Sensitivity distribution
        col1, col2 = st.columns(2)

        with col1:
            # By sensitivity level
            level_dist = sensitive_df['sensitivity_level'].value_counts()

            fig = px.pie(
                values=level_dist.values,
                names=level_dist.index,
                title="Distribution by Sensitivity Level",
                color_discrete_map={
                    'CRITICAL': '#991b1b',
                    'HIGH': '#ef4444',
                    'MEDIUM': '#f59e0b',
                    'LOW': '#10b981'
                }
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # By site
            site_sensitive = sensitive_df.groupby('site_name').agg({
                'file_id': 'count',
                'sensitivity_score': 'mean'
            }).reset_index()

            site_sensitive.columns = ['Site', 'Count', 'Avg Score']
            site_sensitive = site_sensitive.nlargest(10, 'Count')

            fig = px.bar(
                site_sensitive,
                x='Count',
                y='Site',
                orientation='h',
                title="Top 10 Sites with Sensitive Files",
                color='Avg Score',
                color_continuous_scale='YlOrRd'
            )
            st.plotly_chart(fig, use_container_width=True)

        # High risk sensitive files
        st.markdown("### 🚨 High Risk Sensitive Files")

        high_risk_display = high_risk.nlargest(50, 'sensitivity_score')[[
            'name', 'site_name', 'sensitivity_score', 'sensitivity_level',
            'external_user_count', 'size_mb'
        ]].copy()

        high_risk_display.columns = [
            'File Name', 'Site', 'Sensitivity Score', 'Level',
            'External Users', 'Size (MB)'
        ]

        high_risk_display['Size (MB)'] = high_risk_display['Size (MB)'].round(2)

        st.dataframe(
            high_risk_display,
            column_config={
                "Sensitivity Score": st.column_config.ProgressColumn(
                    "Sensitivity Score",
                    min_value=0,
                    max_value=100,
                    format="%d",
                ),
            },
            hide_index=True,
            use_container_width=True
        )

    def _render_stale_files_report(self, df: pd.DataFrame):
        """Render stale files report"""
        st.markdown("### 📅 Stale Files Report")

        # Stale threshold
        days_threshold = st.slider(
            "Days since last modification",
            min_value=90,
            max_value=1095,
            value=365,
            step=30
        )

        stale_df = df[df['days_since_modified'] >= days_threshold].copy()

        if stale_df.empty:
            st.info(f"No files older than {days_threshold} days found")
            return

        # Statistics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Stale Files",
                f"{len(stale_df):,}",
                f"{len(stale_df)/len(df)*100:.1f}% of total"
            )

        with col2:
            stale_size_gb = stale_df['size_gb'].sum()
            st.metric(
                "Stale Data Size",
                f"{stale_size_gb:.1f} GB",
                "Potential archive/delete"
            )

        with col3:
            stale_sensitive = len(stale_df[stale_df['is_sensitive']])
            st.metric(
                "Stale Sensitive Files",
                f"{stale_sensitive:,}",
                "Require review"
            )

        with col4:
            avg_age = stale_df['days_since_modified'].mean()
            st.metric(
                "Average Age",
                f"{avg_age:.0f} days",
                f"~{avg_age/365:.1f} years"
            )

        # Stale files by site
        stale_by_site = stale_df.groupby('site_name').agg({
            'file_id': 'count',
            'size_gb': 'sum'
        }).reset_index()

        stale_by_site.columns = ['Site', 'File Count', 'Total GB']
        stale_by_site = stale_by_site.nlargest(15, 'Total GB')

        fig = px.scatter(
            stale_by_site,
            x='File Count',
            y='Total GB',
            size='Total GB',
            hover_data=['Site'],
            title="Stale Files by Site",
            labels={'Total GB': 'Total Size (GB)'}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Recommendations
        st.markdown("### 💡 Archival Strategy")

        # Calculate potential savings
        very_old = stale_df[stale_df['days_since_modified'] > 730]  # 2+ years
        archive_candidates = very_old[~very_old['is_sensitive']]

        st.info(f"""
        **Archival Recommendations**

        **Immediate Actions:**
        - Archive {len(archive_candidates):,} non-sensitive files older than 2 years
        - Potential immediate savings: {archive_candidates['size_gb'].sum():.1f} GB

        **Review Required:**
        - {len(very_old[very_old['is_sensitive']]):,} sensitive files older than 2 years
        - {len(stale_df[stale_df['has_external_access']]):,} stale files with external access

        **Long-term Strategy:**
        - Implement automated archival policy
        - Set up lifecycle management rules
        - Regular quarterly reviews of stale content
        """)

    def _render_permission_anomalies_report(self, df: pd.DataFrame):
        """Render permission anomalies report"""
        st.markdown("### 🔍 Permission Anomalies Report")

        # Define anomalies
        anomalies = {
            'sensitive_external': df[(df['is_sensitive']) & (df['has_external_access'])],
            'high_write_access': df[df['write_user_count'] > df['user_count'] * 0.8],
            'orphaned_unique': df[(df['has_unique_permissions']) & (df['user_count'] < 2)],
            'overexposed': df[df['external_user_count'] > 10]
        }

        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "Sensitive + External",
                f"{len(anomalies['sensitive_external']):,}",
                "Critical risk"
            )

        with col2:
            st.metric(
                "High Write Access",
                f"{len(anomalies['high_write_access']):,}",
                ">80% users can edit"
            )

        with col3:
            st.metric(
                "Orphaned Permissions",
                f"{len(anomalies['orphaned_unique']):,}",
                "Unique perms, <2 users"
            )

        with col4:
            st.metric(
                "Overexposed Files",
                f"{len(anomalies['overexposed']):,}",
                ">10 external users"
            )

        # Detailed analysis by anomaly type
        anomaly_type = st.selectbox(
            "Select Anomaly Type",
            [
                "Sensitive Files with External Access",
                "Excessive Write Permissions",
                "Orphaned Unique Permissions",
                "Overexposed Files"
            ]
        )

        # Map selection to data
        anomaly_map = {
            "Sensitive Files with External Access": anomalies['sensitive_external'],
            "Excessive Write Permissions": anomalies['high_write_access'],
            "Orphaned Unique Permissions": anomalies['orphaned_unique'],
            "Overexposed Files": anomalies['overexposed']
        }

        selected_anomaly = anomaly_map[anomaly_type]

        if not selected_anomaly.empty:
            # Display details
            display_df = selected_anomaly.head(50)[[
                'name', 'site_name', 'sensitivity_score',
                'user_count', 'external_user_count', 'write_user_count'
            ]].copy()

            display_df.columns = [
                'File Name', 'Site', 'Sensitivity',
                'Total Users', 'External Users', 'Write Users'
            ]

            st.dataframe(
                display_df,
                column_config={
                    "Sensitivity": st.column_config.ProgressColumn(
                        "Sensitivity",
                        min_value=0,
                        max_value=100,
                        format="%d",
                    ),
                },
                hide_index=True,
                use_container_width=True
            )
        else:
            st.success(f"✅ No {anomaly_type.lower()} detected")

    def _render_storage_optimization_report(self, df: pd.DataFrame):
        """Render storage optimization report"""
        st.markdown("### 💾 Storage Optimization Report")

        # Calculate optimization opportunities
        duplicates = self.find_duplicate_files()
        stale_files = df[df['days_since_modified'] > 365]
        large_old_files = df[(df['size_gb'] > 0.5) & (df['days_since_modified'] > 180)]

        # Optimization summary
        col1, col2, col3 = st.columns(3)

        dup_savings = 0
        if not duplicates.empty:
            dup_savings = duplicates.groupby(['name', 'size_bytes'])['size_bytes'].sum().sum() - \
                         duplicates.groupby(['name', 'size_bytes'])['size_bytes'].first().sum()
            dup_savings = dup_savings / (1024**3)

        with col1:
            st.metric(
                "Duplicate Savings",
                f"{dup_savings:.1f} GB",
                "From deduplication"
            )

        with col2:
            stale_savings = stale_files['size_gb'].sum()
            st.metric(
                "Archival Potential",
                f"{stale_savings:.1f} GB",
                "Files >1 year old"
            )

        with col3:
            total_savings = dup_savings + (stale_savings * 0.5)  # Assume 50% can be archived
            st.metric(
                "Total Potential Savings",
                f"{total_savings:.1f} GB",
                f"{total_savings / df['size_gb'].sum() * 100:.1f}% of total"
            )

        # Optimization breakdown
        st.markdown("### 📊 Optimization Opportunities")

        # Create optimization categories
        categories = []

        if not duplicates.empty:
            categories.append({
                'Category': 'Duplicate Files',
                'File Count': len(duplicates),
                'Size (GB)': dup_savings,
                'Action': 'Deduplicate',
                'Priority': 'High'
            })

        categories.append({
            'Category': 'Stale Files (>1 year)',
            'File Count': len(stale_files),
            'Size (GB)': stale_files['size_gb'].sum(),
            'Action': 'Archive/Delete',
            'Priority': 'Medium'
        })

        categories.append({
            'Category': 'Large Old Files',
            'File Count': len(large_old_files),
            'Size (GB)': large_old_files['size_gb'].sum(),
            'Action': 'Compress/Archive',
            'Priority': 'Medium'
        })

        opt_df = pd.DataFrame(categories)

        # Visualization
        fig = px.bar(
            opt_df,
            x='Size (GB)',
            y='Category',
            orientation='h',
            title="Storage Optimization by Category",
            color='Priority',
            color_discrete_map={
                'High': '#ef4444',
                'Medium': '#f59e0b',
                'Low': '#10b981'
            }
        )
        st.plotly_chart(fig, use_container_width=True)

        # Detailed recommendations
        st.markdown("### 💡 Implementation Plan")

        st.markdown("""
        **Phase 1: Quick Wins (1-2 weeks)**
        - Deduplicate obvious duplicate files within same sites
        - Delete temporary and cache files older than 6 months
        - Compress large log and text files

        **Phase 2: Archival (1 month)**
        - Move files >2 years old to archive storage
        - Implement automated archival policies
        - Review and clean up orphaned content

        **Phase 3: Long-term Optimization (3 months)**
        - Implement storage quotas per site
        - Deploy intelligent tiering based on access patterns
        - Set up regular storage optimization reviews

        **Expected Outcomes:**
        - Immediate storage reduction: 10-15%
        - Long-term savings: 25-30%
        - Improved performance and reduced costs
        """)

        # Top candidates table
        st.markdown("### 📋 Top Optimization Candidates")

        # Combine different optimization candidates
        candidates = []

        # Add duplicates
        if not duplicates.empty:
            dup_summary = duplicates.groupby(['name', 'size_bytes']).agg({
                'file_id': 'count',
                'site_name': lambda x: x.iloc[0]
            }).reset_index()

            for _, row in dup_summary.head(10).iterrows():
                candidates.append({
                    'File': row['name'],
                    'Type': 'Duplicate',
                    'Size (MB)': row['size_bytes'] / (1024**2),
                    'Instances': row['file_id'],
                    'Potential Savings (MB)': (row['file_id'] - 1) * row['size_bytes'] / (1024**2)
                })

        # Add large stale files
        for _, row in stale_files.nlargest(10, 'size_bytes').iterrows():
            candidates.append({
                'File': row['name'],
                'Type': 'Stale',
                'Size (MB)': row['size_mb'],
                'Instances': 1,
                'Potential Savings (MB)': row['size_mb']
            })

        if candidates:
            cand_df = pd.DataFrame(candidates).head(20)
            cand_df = cand_df.round(2)
            st.dataframe(cand_df, hide_index=True, use_container_width=True)
