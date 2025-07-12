"""
Sensitivity Analysis Dashboard Component
Analyzes and displays file sensitivity patterns
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json
import asyncio
from typing import Dict, List, Any, Optional
import humanize

from src.database.repository import DatabaseRepository
from src.utils.sensitive_content_detector import SensitivityLevel, SensitiveContentDetector


class SensitivityComponent:
    """Handles sensitivity analysis and visualization"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.repo = DatabaseRepository(db_path)
        self.detector = SensitiveContentDetector()

    @st.cache_data(ttl=300)
    def load_sensitive_files(_self, min_score: int = 0, limit: int = 1000) -> pd.DataFrame:
        """Load files with sensitivity scores"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)
            query = f"""
                SELECT
                    f.file_id,
                    f.name as file_name,
                    f.server_relative_url as file_path,
                    f.size_bytes,
                    f.content_type,
                    f.sensitivity_score,
                    f.sensitivity_level,
                    f.sensitivity_categories,
                    f.sensitivity_factors,
                    f.modified_at,
                    f.modified_by,
                    s.title as site_name,
                    s.url as site_url,
                    l.name as library_name,
                    COUNT(DISTINCT p.principal_id) as total_users,
                    COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_users,
                    COUNT(DISTINCT CASE WHEN p.permission_level IN ('Full Control', 'Edit') THEN p.principal_id END) as write_users,
                    MAX(CASE WHEN p.is_anonymous_link = 1 THEN 1 ELSE 0 END) as has_anonymous_link
                FROM files f
                JOIN sites s ON f.site_id = s.id
                LEFT JOIN libraries l ON f.library_id = l.id
                LEFT JOIN permissions p ON p.object_type = 'file' AND p.object_id = f.file_id
                WHERE f.sensitivity_score >= {min_score}
                GROUP BY f.file_id
                ORDER BY f.sensitivity_score DESC, external_users DESC
                LIMIT {limit}
            """
            results = await repo.fetch_all(query)
            return pd.DataFrame(results) if results else pd.DataFrame()

        return asyncio.run(_load())

    @st.cache_data(ttl=300)
    def load_sensitivity_by_category(_self) -> pd.DataFrame:
        """Load sensitivity statistics by category"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)
            query = """
                SELECT
                    sensitivity_categories,
                    COUNT(*) as file_count,
                    AVG(sensitivity_score) as avg_score,
                    SUM(size_bytes) as total_size,
                    COUNT(DISTINCT site_id) as site_count
                FROM files
                WHERE sensitivity_score > 0
                GROUP BY sensitivity_categories
            """
            results = await repo.fetch_all(query)
            return pd.DataFrame(results) if results else pd.DataFrame()

        return asyncio.run(_load())

    @st.cache_data(ttl=300)
    def load_sensitivity_trends(_self) -> pd.DataFrame:
        """Load sensitivity trends over time"""
        async def _load():
            repo = DatabaseRepository(_self.db_path)
            query = """
                SELECT
                    DATE(created_at) as date,
                    COUNT(CASE WHEN sensitivity_score >= 80 THEN 1 END) as critical,
                    COUNT(CASE WHEN sensitivity_score >= 60 AND sensitivity_score < 80 THEN 1 END) as high,
                    COUNT(CASE WHEN sensitivity_score >= 40 AND sensitivity_score < 60 THEN 1 END) as medium,
                    COUNT(CASE WHEN sensitivity_score > 0 AND sensitivity_score < 40 THEN 1 END) as low
                FROM files
                WHERE created_at IS NOT NULL
                GROUP BY DATE(created_at)
                ORDER BY date DESC
                LIMIT 90
            """
            results = await repo.fetch_all(query)
            return pd.DataFrame(results) if results else pd.DataFrame()

        return asyncio.run(_load())

    def render(self):
        """Render the sensitivity analysis component"""
        st.header("🔍 Sensitivity Analysis")

        # Filters
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            min_score = st.slider(
                "Minimum Sensitivity Score",
                min_value=0,
                max_value=100,
                value=40,
                step=10,
                help="Filter files by minimum sensitivity score"
            )

        with col2:
            sensitivity_level = st.selectbox(
                "Sensitivity Level",
                options=["All", "Critical", "High", "Medium", "Low"],
                help="Filter by sensitivity level"
            )

        with col3:
            show_external_only = st.checkbox(
                "External Access Only",
                help="Show only files with external access"
            )

        with col4:
            show_anonymous_only = st.checkbox(
                "Anonymous Links Only",
                help="Show only files with anonymous links"
            )

        # Load data
        sensitive_files = self.load_sensitive_files(min_score)

        # Apply filters
        if sensitivity_level != "All":
            sensitive_files = sensitive_files[
                sensitive_files['sensitivity_level'] == sensitivity_level.upper()
            ]

        if show_external_only:
            sensitive_files = sensitive_files[sensitive_files['external_users'] > 0]

        if show_anonymous_only:
            sensitive_files = sensitive_files[sensitive_files['has_anonymous_link'] == 1]

        # Summary metrics
        if not sensitive_files.empty:
            self._render_summary_metrics(sensitive_files)

            # Tabs for different views
            tab1, tab2, tab3, tab4 = st.tabs([
                "📋 Sensitive Files",
                "📊 Category Analysis",
                "📈 Risk Matrix",
                "📅 Trends"
            ])

            with tab1:
                self._render_sensitive_files_table(sensitive_files)

            with tab2:
                self._render_category_analysis()

            with tab3:
                self._render_risk_matrix(sensitive_files)

            with tab4:
                self._render_trends()
        else:
            st.info("No files found matching the selected criteria.")

    def _render_summary_metrics(self, df: pd.DataFrame):
        """Render summary metrics for sensitive files"""
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            st.metric("Sensitive Files", f"{len(df):,}")

        with col2:
            critical_count = len(df[df['sensitivity_level'] == 'CRITICAL'])
            st.metric("Critical Files", f"{critical_count:,}")

        with col3:
            external_count = len(df[df['external_users'] > 0])
            st.metric("With External Access", f"{external_count:,}")

        with col4:
            total_size = df['size_bytes'].sum()
            st.metric("Total Size", humanize.naturalsize(total_size, binary=True))

        with col5:
            avg_score = df['sensitivity_score'].mean()
            st.metric("Avg Score", f"{avg_score:.1f}")

    def _render_sensitive_files_table(self, df: pd.DataFrame):
        """Render the sensitive files table"""
        st.subheader("🔐 Sensitive Files Details")

        # Process data for display
        display_df = df.copy()

        # Parse JSON fields
        display_df['categories'] = display_df['sensitivity_categories'].apply(
            lambda x: ', '.join(json.loads(x)) if x and x != 'null' else 'N/A'
        )
        display_df['risk_factors'] = display_df['sensitivity_factors'].apply(
            lambda x: len(json.loads(x)) if x and x != 'null' else 0
        )

        # Calculate risk score
        display_df['risk_score'] = (
            display_df['sensitivity_score'] * 0.5 +
            (display_df['external_users'] > 0).astype(int) * 20 +
            (display_df['has_anonymous_link'] == 1).astype(int) * 30
        ).clip(upper=100)

        # Format size
        display_df['size'] = display_df['size_bytes'].apply(
            lambda x: humanize.naturalsize(x, binary=True)
        )

        # Color coding function
        def highlight_sensitivity(val):
            if val >= 80:
                return 'background-color: #fee2e2'  # light red
            elif val >= 60:
                return 'background-color: #fef3c7'  # light yellow
            elif val >= 40:
                return 'background-color: #fde68a'  # lighter yellow
            else:
                return ''

        # Display columns
        display_cols = [
            'file_name', 'sensitivity_score', 'sensitivity_level', 'categories',
            'site_name', 'external_users', 'risk_score', 'size'
        ]

        st.dataframe(
            display_df[display_cols].rename(columns={
                'file_name': 'File Name',
                'sensitivity_score': 'Sensitivity',
                'sensitivity_level': 'Level',
                'categories': 'Categories',
                'site_name': 'Site',
                'external_users': 'External Users',
                'risk_score': 'Risk Score',
                'size': 'Size'
            }).style.applymap(highlight_sensitivity, subset=['Sensitivity', 'Risk Score']),
            use_container_width=True,
            hide_index=True
        )

        # Download option
        if st.button("📥 Export Sensitive Files Report"):
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name="sensitive_files_report.csv",
                mime="text/csv"
            )

    def _render_category_analysis(self):
        """Render analysis by sensitivity category"""
        st.subheader("📊 Sensitivity Categories")

        category_df = self.load_sensitivity_by_category()

        if category_df.empty:
            st.info("No sensitivity category data available.")
            return

        # Process categories
        all_categories = {}
        for idx, row in category_df.iterrows():
            if row['sensitivity_categories'] and row['sensitivity_categories'] != 'null':
                categories = json.loads(row['sensitivity_categories'])
                for cat in categories:
                    if cat not in all_categories:
                        all_categories[cat] = {
                            'file_count': 0,
                            'total_size': 0,
                            'avg_score': []
                        }
                    all_categories[cat]['file_count'] += row['file_count']
                    all_categories[cat]['total_size'] += row['total_size']
                    all_categories[cat]['avg_score'].append(row['avg_score'])

        # Create summary dataframe
        category_summary = []
        for cat, data in all_categories.items():
            category_summary.append({
                'Category': cat.title(),
                'Files': data['file_count'],
                'Total Size': humanize.naturalsize(data['total_size'], binary=True),
                'Avg Score': sum(data['avg_score']) / len(data['avg_score'])
            })

        summary_df = pd.DataFrame(category_summary)

        if not summary_df.empty:
            # Visualizations
            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    summary_df.sort_values('Files', ascending=True),
                    x='Files',
                    y='Category',
                    orientation='h',
                    title='Files by Sensitivity Category',
                    color='Avg Score',
                    color_continuous_scale='Reds'
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.pie(
                    summary_df,
                    values='Files',
                    names='Category',
                    title='Distribution of Sensitivity Categories'
                )
                st.plotly_chart(fig, use_container_width=True)

            # Table
            st.dataframe(
                summary_df.sort_values('Files', ascending=False),
                use_container_width=True,
                hide_index=True
            )

    def _render_risk_matrix(self, df: pd.DataFrame):
        """Render sensitivity vs access risk matrix"""
        st.subheader("📈 Risk Matrix: Sensitivity vs Access")

        # Create risk matrix data
        df['access_level'] = pd.cut(
            df['total_users'],
            bins=[0, 5, 20, 50, float('inf')],
            labels=['Limited', 'Moderate', 'Wide', 'Very Wide']
        )

        # Create heatmap data
        matrix_data = df.groupby(['sensitivity_level', 'access_level']).size().reset_index(name='count')

        # Pivot for heatmap
        pivot_data = matrix_data.pivot(
            index='sensitivity_level',
            columns='access_level',
            values='count'
        ).fillna(0)

        # Create heatmap
        fig = px.imshow(
            pivot_data,
            labels=dict(x="Access Level", y="Sensitivity Level", color="File Count"),
            x=pivot_data.columns,
            y=pivot_data.index,
            color_continuous_scale="Reds",
            title="Risk Heatmap: Sensitivity vs Access Level"
        )

        st.plotly_chart(fig, use_container_width=True)

        # Scatter plot
        fig = px.scatter(
            df,
            x='total_users',
            y='sensitivity_score',
            size='size_bytes',
            color='external_users',
            hover_data=['file_name', 'site_name'],
            title='Files: Users vs Sensitivity Score',
            labels={
                'total_users': 'Total Users with Access',
                'sensitivity_score': 'Sensitivity Score',
                'external_users': 'External Users'
            },
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

        fig.add_annotation(
            x=75,
            y=80,
            text="High Risk Zone",
            showarrow=False,
            font=dict(color="red", size=12)
        )

        st.plotly_chart(fig, use_container_width=True)

    def _render_trends(self):
        """Render sensitivity trends over time"""
        st.subheader("📅 Sensitivity Trends")

        trends_df = self.load_sensitivity_trends()

        if trends_df.empty:
            st.info("No trend data available.")
            return

        # Convert to datetime
        trends_df['date'] = pd.to_datetime(trends_df['date'])

        # Create stacked area chart
        fig = go.Figure()

        for level, color in [
            ('critical', '#991b1b'),
            ('high', '#ef4444'),
            ('medium', '#f59e0b'),
            ('low', '#10b981')
        ]:
            fig.add_trace(go.Scatter(
                x=trends_df['date'],
                y=trends_df[level],
                mode='lines',
                name=level.title(),
                stackgroup='one',
                fillcolor=color
            ))

        fig.update_layout(
            title="Sensitive Files Created Over Time",
            xaxis_title="Date",
            yaxis_title="Number of Files",
            hovermode='x unified'
        )

        st.plotly_chart(fig, use_container_width=True)
