"""
Sites Dashboard Component
Comprehensive SharePoint sites analysis and monitoring
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import Dict, Any, List, Optional
import asyncio
from datetime import datetime, timedelta
import numpy as np

from src.database.repository import DatabaseRepository


class SitesComponent:
    """Comprehensive sites analysis component"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.repo = DatabaseRepository(db_path)

    @st.cache_data(ttl=300)
    def load_sites_data(_self) -> pd.DataFrame:
        """Load comprehensive sites data"""
        query = """
            SELECT
                s.id,
                s.site_id,
                s.title,
                s.url,
                s.description,
                s.created_at,
                s.last_modified,
                s.storage_used,
                s.storage_quota,
                s.is_hub_site,
                s.hub_site_id,
                COUNT(DISTINCT l.id) as library_count,
                COUNT(DISTINCT fi.id) as file_count,
                COUNT(DISTINCT fo.id) as folder_count,
                COUNT(DISTINCT p.id) as permission_count,
                COUNT(DISTINCT CASE WHEN p.is_inherited = 0 THEN p.id END) as unique_permission_count,
                COUNT(DISTINCT p.principal_id) as user_count,
                COUNT(DISTINCT CASE WHEN p.is_external = 1 THEN p.principal_id END) as external_user_count,
                COUNT(DISTINCT CASE WHEN p.permission_level = 'Full Control' THEN p.principal_id END) as admin_count,
                SUM(fi.size_bytes) as total_file_size,
                AVG(fi.size_bytes) as avg_file_size,
                MAX(fi.size_bytes) as max_file_size,
                COUNT(DISTINCT CASE WHEN fi.sensitivity_score >= 40 THEN fi.id END) as sensitive_file_count,
                COUNT(DISTINCT CASE WHEN fi.has_unique_permissions = 1 THEN fi.id END) as files_unique_perms,
                MAX(fi.modified_at) as last_file_modified
            FROM sites s
            LEFT JOIN libraries l ON s.id = l.site_id
            LEFT JOIN files fi ON s.id = fi.site_id
            LEFT JOIN folders fo ON s.id = fo.site_id
            LEFT JOIN permissions p ON p.object_type = 'site' AND p.object_id = s.site_id
            GROUP BY s.id
        """

        df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

        # Calculate derived metrics
        if 'storage_used' in df.columns:
            df['storage_used_gb'] = df['storage_used'] / (1024**3)
        else:
            df['storage_used_gb'] = 0.0

        if 'total_file_size' in df.columns:
            df['total_file_size_gb'] = df['total_file_size'] / (1024**3)
        else:
            df['total_file_size_gb'] = 0.0
        storage_util = df['storage_used'] / df['storage_quota'] * 100
        df['storage_utilization'] = storage_util.where(storage_util.notna(), 0)

        # Calculate permission_complexity safely
        if 'permission_count' in df.columns and 'unique_permission_count' in df.columns:
            perm_complex = df['unique_permission_count'] / df['permission_count'] * 100
            df['permission_complexity'] = perm_complex.where(perm_complex.notna(), 0)
        else:
            df['permission_complexity'] = 0.0

        ext_exp = df['external_user_count'] / df['user_count'] * 100
        df['external_exposure'] = ext_exp.where(ext_exp.notna(), 0)

        sens_ratio = df['sensitive_file_count'] / df['file_count'] * 100
        df['sensitivity_ratio'] = sens_ratio.where(sens_ratio.notna(), 0)

        # Calculate health score (0-100)
        df['health_score'] = 100
        df.loc[df['storage_utilization'] > 90, 'health_score'] -= 20
        df.loc[df['permission_complexity'] > 50, 'health_score'] -= 15
        df.loc[df['external_exposure'] > 30, 'health_score'] -= 25
        df.loc[df['sensitivity_ratio'] > 20, 'health_score'] -= 20
        df.loc[df['admin_count'] > 10, 'health_score'] -= 10
        df['health_score'] = df['health_score'].clip(0, 100)

        return df

    @st.cache_data(ttl=300)
    def load_library_details(_self, site_id: Optional[str] = None) -> pd.DataFrame:
        """Load library details for a specific site or all sites"""
        query = """
            SELECT
                l.*,
                s.title as site_title,
                COUNT(DISTINCT f.id) as file_count,
                SUM(f.size_bytes) as total_size,
                AVG(f.size_bytes) as avg_file_size,
                COUNT(DISTINCT CASE WHEN f.sensitivity_score >= 40 THEN f.id END) as sensitive_files
            FROM libraries l
            JOIN sites s ON l.site_id = s.id
            LEFT JOIN files f ON l.id = f.library_id
        """

        if site_id:
            query += " WHERE s.site_id = ?"
            df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}", params=(site_id,))
        else:
            query += " GROUP BY l.id"
            df = pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

        return df

    @st.cache_data(ttl=300)
    def load_hub_site_relationships(_self) -> pd.DataFrame:
        """Load hub site relationships"""
        query = """
            SELECT
                h.site_id as hub_id,
                h.title as hub_title,
                h.url as hub_url,
                s.site_id as associated_site_id,
                s.title as associated_site_title,
                s.url as associated_site_url,
                COUNT(DISTINCT f.id) as shared_files,
                COUNT(DISTINCT p.principal_id) as shared_users
            FROM sites h
            JOIN sites s ON h.site_id = s.hub_site_id
            LEFT JOIN files f ON s.id = f.site_id
            LEFT JOIN permissions p ON p.object_type = 'site' AND p.object_id = s.site_id
            WHERE h.is_hub_site = 1
            GROUP BY h.site_id, s.site_id
        """

        return pd.read_sql_query(query, f"sqlite:///{_self.db_path}")

    def render(self):
        """Render the sites component"""
        st.header("🏢 Sites Analysis Dashboard")

        # Load data
        sites_df = self.load_sites_data()

        if sites_df.empty:
            st.warning("No sites data available. Please run an audit first.")
            return

        # Top metrics
        self._render_site_metrics(sites_df)

        # Main content tabs
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📊 Sites Overview",
            "💾 Storage Analytics",
            "🔐 Security Analysis",
            "🌐 Hub Sites",
            "📚 Libraries",
            "📈 Trends & Insights"
        ])

        with tab1:
            self._render_sites_overview(sites_df)

        with tab2:
            self._render_storage_analytics(sites_df)

        with tab3:
            self._render_security_analysis(sites_df)

        with tab4:
            self._render_hub_sites(sites_df)

        with tab5:
            self._render_libraries_view(sites_df)

        with tab6:
            self._render_trends_insights(sites_df)

    def _render_site_metrics(self, df: pd.DataFrame):
        """Render top-level site metrics"""
        col1, col2, col3, col4, col5, col6 = st.columns(6)

        with col1:
            st.metric(
                "Total Sites",
                f"{len(df):,}",
                help="Total number of SharePoint sites"
            )

        with col2:
            total_storage_tb = df['storage_used'].sum() / (1024**4) if 'storage_used' in df else 0
            st.metric(
                "Total Storage",
                f"{total_storage_tb:.2f} TB",
                help="Total storage used across all sites"
            )

        with col3:
            avg_health = df['health_score'].mean()
            delta_color = "normal" if avg_health >= 70 else "inverse"
            st.metric(
                "Avg Health Score",
                f"{avg_health:.0f}/100",
                delta=f"{len(df[df['health_score'] < 50])} sites at risk",
                delta_color=delta_color
            )

        with col4:
            hub_count = len(df[df['is_hub_site'] == True])
            st.metric(
                "Hub Sites",
                f"{hub_count}",
                f"{len(df[df['hub_site_id'].notna()]) - hub_count} associated",
                help="Hub sites and their associations"
            )

        with col5:
            high_exposure = len(df[df['external_exposure'] > 50])
            st.metric(
                "High Exposure",
                f"{high_exposure}",
                delta_color="inverse" if high_exposure > 0 else "off",
                help="Sites with >50% external users"
            )

        with col6:
            total_files = df['file_count'].sum()
            st.metric(
                "Total Files",
                f"{total_files:,}",
                f"{df['sensitive_file_count'].sum():,} sensitive",
                help="Total files across all sites"
            )

    def _render_sites_overview(self, df: pd.DataFrame):
        """Render sites overview with interactive table"""
        st.subheader("📊 Sites Inventory")

        # Search and filters
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            search_term = st.text_input(
                "Search sites",
                placeholder="Enter site name or URL...",
                key="site_search"
            )

        with col2:
            health_filter = st.select_slider(
                "Health Score",
                options=["All", "Critical (<50)", "Warning (50-70)", "Good (>70)"],
                value="All",
                key="health_filter"
            )

        with col3:
            storage_filter = st.select_slider(
                "Storage Utilization",
                options=["All", "Low (<50%)", "Medium (50-80%)", "High (>80%)"],
                value="All",
                key="storage_filter"
            )

        with col4:
            hub_filter = st.selectbox(
                "Site Type",
                ["All Sites", "Hub Sites Only", "Associated Sites", "Standalone Sites"],
                key="hub_filter"
            )

        # Apply filters
        filtered_df = df.copy()

        if search_term:
            mask = (
                filtered_df['title'].str.contains(search_term, case=False, na=False) |
                filtered_df['url'].str.contains(search_term, case=False, na=False)
            )
            filtered_df = filtered_df[mask]

        if health_filter == "Critical (<50)":
            filtered_df = filtered_df[filtered_df['health_score'] < 50]
        elif health_filter == "Warning (50-70)":
            filtered_df = filtered_df[(filtered_df['health_score'] >= 50) & (filtered_df['health_score'] <= 70)]
        elif health_filter == "Good (>70)":
            filtered_df = filtered_df[filtered_df['health_score'] > 70]

        if storage_filter == "Low (<50%)":
            filtered_df = filtered_df[filtered_df['storage_utilization'] < 50]
        elif storage_filter == "Medium (50-80%)":
            filtered_df = filtered_df[(filtered_df['storage_utilization'] >= 50) & (filtered_df['storage_utilization'] <= 80)]
        elif storage_filter == "High (>80%)":
            filtered_df = filtered_df[filtered_df['storage_utilization'] > 80]

        if hub_filter == "Hub Sites Only":
            filtered_df = filtered_df[filtered_df['is_hub_site'] == True]
        elif hub_filter == "Associated Sites":
            filtered_df = filtered_df[filtered_df['hub_site_id'].notna() & (filtered_df['is_hub_site'] == False)]
        elif hub_filter == "Standalone Sites":
            filtered_df = filtered_df[filtered_df['hub_site_id'].isna() & (filtered_df['is_hub_site'] == False)]

        # Display results count
        st.info(f"Showing {len(filtered_df)} of {len(df)} sites")

        # Interactive data table
        if not filtered_df.empty:
            # Prepare display dataframe
            display_df = filtered_df[[
                'title', 'url', 'health_score', 'storage_used_gb', 'storage_utilization',
                'file_count', 'sensitive_file_count', 'external_user_count', 'permission_complexity'
            ]].copy()

            display_df.columns = [
                'Site Name', 'URL', 'Health Score', 'Storage (GB)', 'Storage %',
                'Files', 'Sensitive Files', 'External Users', 'Permission Complexity %'
            ]

            # Format columns - ensure numeric types before rounding
            display_df['Storage %'] = pd.to_numeric(display_df['Storage %'], errors='coerce').fillna(0).round(1)
            display_df['Permission Complexity %'] = pd.to_numeric(display_df['Permission Complexity %'], errors='coerce').fillna(0).round(1)
            display_df['Storage (GB)'] = pd.to_numeric(display_df['Storage (GB)'], errors='coerce').fillna(0).round(2)

            # Display with conditional formatting
            st.dataframe(
                display_df,
                column_config={
                    "Health Score": st.column_config.ProgressColumn(
                        "Health Score",
                        min_value=0,
                        max_value=100,
                        format="%d",
                    ),
                    "Storage %": st.column_config.ProgressColumn(
                        "Storage %",
                        min_value=0,
                        max_value=100,
                        format="%.1f%%",
                    ),
                    "URL": st.column_config.LinkColumn("URL"),
                },
                hide_index=True,
                use_container_width=True
            )

            # Site details expander
            if st.checkbox("Show detailed site analysis"):
                selected_site = st.selectbox(
                    "Select a site for detailed analysis",
                    filtered_df['title'].tolist(),
                    key="site_detail_select"
                )

                if selected_site:
                    site_data = filtered_df[filtered_df['title'] == selected_site].iloc[0]
                    self._render_site_details(site_data)
        else:
            st.info("No sites match the selected filters")

    def _render_site_details(self, site_data: pd.Series):
        """Render detailed analysis for a specific site"""
        st.markdown(f"### 🔍 Detailed Analysis: {site_data['title']}")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📊 Site Metrics")
            st.metric("Health Score", f"{site_data['health_score']:.0f}/100")
            st.metric("Total Libraries", f"{site_data['library_count']:,}")
            st.metric("Total Files", f"{site_data['file_count']:,}")
            avg_size = site_data.get('avg_file_size', 0) or 0
            st.metric("Average File Size", f"{avg_size / 1024 / 1024:.2f} MB")

            # Permission breakdown
            st.markdown("#### 🔐 Permission Analysis")
            perm_data = {
                "Total Permissions": site_data['permission_count'],
                "Unique Permissions": site_data['unique_permission_count'],
                "Total Users": site_data['user_count'],
                "External Users": site_data['external_user_count'],
                "Administrators": site_data['admin_count']
            }

            perm_df = pd.DataFrame(list(perm_data.items()), columns=['Metric', 'Count'])
            st.dataframe(perm_df, hide_index=True)

        with col2:
            # Health score breakdown
            st.markdown("#### 🏥 Health Score Breakdown")

            health_factors = {
                'Storage Utilization': min(20, site_data['storage_utilization'] / 4.5) if site_data['storage_utilization'] > 90 else 0,
                'Permission Complexity': min(15, site_data['permission_complexity'] / 3.33) if site_data['permission_complexity'] > 50 else 0,
                'External Exposure': min(25, site_data['external_exposure'] / 1.2) if site_data['external_exposure'] > 30 else 0,
                'Sensitive Content': min(20, site_data['sensitivity_ratio']) if site_data['sensitivity_ratio'] > 20 else 0,
                'Admin Count': 10 if site_data['admin_count'] > 10 else 0
            }

            # Create gauge chart for health score
            fig = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = site_data['health_score'],
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "Overall Health Score"},
                gauge = {
                    'axis': {'range': [None, 100]},
                    'bar': {'color': "darkblue"},
                    'steps': [
                        {'range': [0, 50], 'color': "lightgray"},
                        {'range': [50, 70], 'color': "gray"}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 90
                    }
                }
            ))

            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

            # Risk factors
            st.markdown("#### ⚠️ Risk Factors")
            for factor, penalty in health_factors.items():
                if penalty > 0:
                    st.warning(f"{factor}: -{penalty:.0f} points")

    def _render_storage_analytics(self, df: pd.DataFrame):
        """Render storage analytics"""
        st.subheader("💾 Storage Analytics")

        # Storage overview metrics
        col1, col2, col3, col4 = st.columns(4)

        total_storage = df['storage_used'].sum() / (1024**4) if 'storage_used' in df else 0
        total_quota = df['storage_quota'].sum() / (1024**4) if 'storage_quota' in df else 0

        with col1:
            st.metric(
                "Total Storage Used",
                f"{total_storage:.2f} TB",
                f"{(total_storage / total_quota * 100):.1f}% of quota" if total_quota > 0 else "No quota set"
            )

        with col2:
            st.metric(
                "Average Site Storage",
                f"{df['storage_used_gb'].mean():.2f} GB",
                help="Average storage per site"
            )

        with col3:
            large_sites = len(df[df['storage_used_gb'] > 100])
            st.metric(
                "Large Sites (>100GB)",
                f"{large_sites}",
                f"{large_sites / len(df) * 100:.1f}% of sites" if len(df) > 0 else "0%"
            )

        with col4:
            st.metric(
                "Storage Efficiency",
                f"{df['total_file_size'].sum() / df['storage_used'].sum() * 100:.1f}%" if df['storage_used'].sum() > 0 else "N/A",
                help="Actual file size vs allocated storage"
            )

        # Storage distribution charts
        col1, col2 = st.columns(2)

        with col1:
            # Storage by site - top 20
            # Ensure storage_used_gb is numeric
            df['storage_used_gb'] = pd.to_numeric(df['storage_used_gb'], errors='coerce').fillna(0)
            top_storage_df = df.nlargest(20, 'storage_used_gb')[['title', 'storage_used_gb']]

            fig = px.bar(
                top_storage_df,
                x='storage_used_gb',
                y='title',
                orientation='h',
                title="Top 20 Sites by Storage Usage",
                labels={'storage_used_gb': 'Storage (GB)', 'title': 'Site'},
                color='storage_used_gb',
                color_continuous_scale='Reds'
            )
            fig.update_layout(height=600, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Storage utilization distribution
            utilization_bins = pd.cut(
                df['storage_utilization'],
                bins=[0, 25, 50, 75, 90, 100],
                labels=['0-25%', '25-50%', '50-75%', '75-90%', '90-100%']
            )
            utilization_counts = utilization_bins.value_counts().sort_index()

            fig = px.pie(
                values=utilization_counts.values,
                names=utilization_counts.index,
                title="Storage Utilization Distribution",
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)

        # File size analysis
        st.markdown("### 📁 File Size Analysis")

        col1, col2 = st.columns(2)

        with col1:
            # Average file size by site
            # Ensure avg_file_size is numeric
            df['avg_file_size'] = pd.to_numeric(df['avg_file_size'], errors='coerce').fillna(0)
            avg_size_df = df.nlargest(15, 'avg_file_size')[['title', 'avg_file_size']]
            avg_size_df['avg_file_size_mb'] = avg_size_df['avg_file_size'] / (1024 * 1024)

            fig = px.bar(
                avg_size_df,
                x='title',
                y='avg_file_size_mb',
                title="Sites with Largest Average File Size",
                labels={'avg_file_size_mb': 'Average File Size (MB)', 'title': 'Site'}
            )
            fig.update_xaxes(tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Storage growth projection (mock data for demonstration)
            st.markdown("#### 📈 Storage Growth Projection")

            # Calculate monthly growth rate (simplified)
            monthly_growth = total_storage * 0.05  # Assume 5% monthly growth

            months = pd.date_range(start=datetime.now(), periods=12, freq='ME')
            projected_storage = [total_storage]

            for i in range(1, 12):
                projected_storage.append(projected_storage[-1] + monthly_growth)

            projection_df = pd.DataFrame({
                'Month': months,
                'Storage (TB)': projected_storage
            })

            fig = px.line(
                projection_df,
                x='Month',
                y='Storage (TB)',
                title="12-Month Storage Projection",
                markers=True
            )

            # Add quota line if available
            if total_quota > 0:
                fig.add_hline(
                    y=total_quota,
                    line_dash="dash",
                    line_color="red",
                    annotation_text="Storage Quota"
                )

            st.plotly_chart(fig, use_container_width=True)

            # Storage recommendations
            if monthly_growth > 0 and total_quota > total_storage:
                months_to_quota = int((total_quota - total_storage) / monthly_growth)
                projection_text = f"- Projected to reach quota in: {months_to_quota} months"
            elif monthly_growth <= 0:
                projection_text = "- No storage growth detected"
            else:
                projection_text = "- Storage quota already exceeded"

            st.info(f"""
            **💡 Storage Recommendations:**
            - Current growth rate: ~{monthly_growth:.2f} TB/month
            {projection_text}
            - Consider archiving sites with low activity
            - Review large files in sites with >90% utilization
            """)

    def _render_security_analysis(self, df: pd.DataFrame):
        """Render security analysis for sites"""
        st.subheader("🔐 Security Analysis")

        # Security metrics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            high_exposure_sites = len(df[df['external_exposure'] > 30])
            st.metric(
                "High External Exposure",
                f"{high_exposure_sites}",
                f"{high_exposure_sites / len(df) * 100:.1f}% of sites",
                delta_color="inverse"
            )

        with col2:
            complex_perms = len(df[df['permission_complexity'] > 50])
            st.metric(
                "Complex Permissions",
                f"{complex_perms}",
                "Sites with >50% unique permissions",
                delta_color="inverse"
            )

        with col3:
            over_admined = len(df[df['admin_count'] > 5])
            st.metric(
                "Over-Administered",
                f"{over_admined}",
                "Sites with >5 admins",
                delta_color="inverse"
            )

        with col4:
            sensitive_heavy = len(df[df['sensitivity_ratio'] > 30])
            st.metric(
                "Sensitive Content Heavy",
                f"{sensitive_heavy}",
                "Sites with >30% sensitive files",
                delta_color="inverse"
            )

        # Security visualizations
        col1, col2 = st.columns(2)

        with col1:
            # External exposure heatmap
            fig = px.scatter(
                df,
                x='external_user_count',
                y='sensitive_file_count',
                size='file_count',
                color='health_score',
                title="External Exposure vs Sensitive Content",
                labels={
                    'external_user_count': 'External Users',
                    'sensitive_file_count': 'Sensitive Files'
                },
                hover_data=['title'],
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Permission complexity by site type
            complexity_by_type = df.groupby('is_hub_site').agg({
                'permission_complexity': 'mean',
                'unique_permission_count': 'sum',
                'title': 'count'
            }).reset_index()

            complexity_by_type['Site Type'] = complexity_by_type['is_hub_site'].map({
                True: 'Hub Sites',
                False: 'Regular Sites'
            })

            fig = px.bar(
                complexity_by_type,
                x='Site Type',
                y='permission_complexity',
                title="Average Permission Complexity by Site Type",
                labels={'permission_complexity': 'Complexity %'},
                text='permission_complexity'
            )
            fig.update_traces(texttemplate='%{text:.1f}%', textposition='outside')
            st.plotly_chart(fig, use_container_width=True)

        # Security risk matrix
        st.markdown("### 🎯 Security Risk Matrix")

        # Calculate risk scores
        df['security_risk_score'] = (
            (df['external_exposure'] * 0.3) +
            (df['permission_complexity'] * 0.2) +
            (df['sensitivity_ratio'] * 0.3) +
            (np.where(df['admin_count'] > 10, 20, df['admin_count'] * 2))
        ).clip(0, 100)

        # Create risk categories
        df['risk_category'] = pd.cut(
            df['security_risk_score'],
            bins=[0, 25, 50, 75, 100],
            labels=['Low', 'Medium', 'High', 'Critical']
        )

        # Risk distribution
        risk_dist = df['risk_category'].value_counts()

        col1, col2 = st.columns([1, 2])

        with col1:
            # Risk summary
            st.markdown("#### Risk Distribution")
            for category in ['Critical', 'High', 'Medium', 'Low']:
                count = risk_dist.get(category, 0)
                if category == 'Critical':
                    st.error(f"{category}: {count} sites")
                elif category == 'High':
                    st.warning(f"{category}: {count} sites")
                elif category == 'Medium':
                    st.info(f"{category}: {count} sites")
                else:
                    st.success(f"{category}: {count} sites")

        with col2:
            # Top risk sites
            # Ensure security_risk_score is numeric
            df['security_risk_score'] = pd.to_numeric(df['security_risk_score'], errors='coerce').fillna(0)
            high_risk_sites = df[df['risk_category'].isin(['Critical', 'High'])].nlargest(10, 'security_risk_score')

            if not high_risk_sites.empty:
                st.markdown("#### 🚨 Highest Risk Sites")

                risk_display = high_risk_sites[['title', 'security_risk_score', 'external_user_count',
                                               'sensitive_file_count', 'admin_count']].copy()
                risk_display.columns = ['Site', 'Risk Score', 'External Users', 'Sensitive Files', 'Admins']
                risk_display['Risk Score'] = risk_display['Risk Score'].round(1)

                st.dataframe(
                    risk_display,
                    column_config={
                        "Risk Score": st.column_config.ProgressColumn(
                            "Risk Score",
                            min_value=0,
                            max_value=100,
                            format="%.1f",
                        ),
                    },
                    hide_index=True
                )

        # Security recommendations
        st.markdown("### 💡 Security Recommendations")

        recommendations = []

        if high_exposure_sites > 0:
            recommendations.append(f"• Review external access for {high_exposure_sites} sites with high external exposure")

        if complex_perms > 0:
            recommendations.append(f"• Simplify permissions for {complex_perms} sites with complex permission structures")

        if over_admined > 0:
            recommendations.append(f"• Reduce admin count for {over_admined} over-administered sites")

        if sensitive_heavy > 0:
            recommendations.append(f"• Implement additional controls for {sensitive_heavy} sites with high sensitive content")

        if recommendations:
            for rec in recommendations:
                st.warning(rec)
        else:
            st.success("✅ No critical security issues detected across sites")

    def _render_hub_sites(self, df: pd.DataFrame):
        """Render hub sites analysis"""
        st.subheader("🌐 Hub Sites Network")

        hub_relationships = self.load_hub_site_relationships()

        if hub_relationships.empty:
            st.info("No hub site relationships found in this tenant")
            return

        # Hub sites overview
        hub_sites = df[df['is_hub_site'] == True]
        associated_sites = df[df['hub_site_id'].notna() & (df['is_hub_site'] == False)]

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Hub Sites", f"{len(hub_sites)}")

        with col2:
            st.metric("Associated Sites", f"{len(associated_sites)}")

        with col3:
            avg_associations = len(associated_sites) / len(hub_sites) if len(hub_sites) > 0 else 0
            st.metric("Avg Associations per Hub", f"{avg_associations:.1f}")

        with col4:
            total_hub_storage = hub_sites['storage_used_gb'].sum() + associated_sites['storage_used_gb'].sum()
            st.metric("Total Hub Network Storage", f"{total_hub_storage:.2f} GB")

        # Hub site network visualization
        if not hub_relationships.empty:
            # Create network graph
            st.markdown("### 🔗 Hub Site Network Visualization")

            # Prepare data for network graph
            nodes = []
            edges = []

            # Add hub nodes
            for _, hub in hub_relationships[['hub_id', 'hub_title']].drop_duplicates().iterrows():
                nodes.append({
                    'id': hub['hub_id'],
                    'label': hub['hub_title'],
                    'group': 'hub',
                    'size': 30
                })

            # Add associated site nodes and edges
            for _, rel in hub_relationships.iterrows():
                if rel['associated_site_id'] not in [n['id'] for n in nodes]:
                    nodes.append({
                        'id': rel['associated_site_id'],
                        'label': rel['associated_site_title'],
                        'group': 'associated',
                        'size': 20
                    })

                edges.append({
                    'from': rel['hub_id'],
                    'to': rel['associated_site_id'],
                    'value': rel['shared_users']
                })

            # Create Plotly network graph
            edge_x = []
            edge_y = []

            # Simple circular layout
            num_nodes = len(nodes)
            for i, node in enumerate(nodes):
                angle = 2 * np.pi * i / num_nodes
                if node['group'] == 'hub':
                    radius = 0.5
                else:
                    radius = 1.0
                node['x'] = radius * np.cos(angle)
                node['y'] = radius * np.sin(angle)

            for edge in edges:
                x0 = next(n['x'] for n in nodes if n['id'] == edge['from'])
                y0 = next(n['y'] for n in nodes if n['id'] == edge['from'])
                x1 = next(n['x'] for n in nodes if n['id'] == edge['to'])
                y1 = next(n['y'] for n in nodes if n['id'] == edge['to'])
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])

            edge_trace = go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=0.5, color='#888'),
                hoverinfo='none',
                mode='lines'
            )

            node_x = [n['x'] for n in nodes]
            node_y = [n['y'] for n in nodes]
            node_text = [n['label'] for n in nodes]
            node_color = ['red' if n['group'] == 'hub' else 'lightblue' for n in nodes]

            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text',
                text=node_text,
                textposition="top center",
                hoverinfo='text',
                marker=dict(
                    color=node_color,
                    size=[n['size'] for n in nodes],
                    line_width=2
                )
            )

            fig = go.Figure(data=[edge_trace, node_trace],
                          layout=go.Layout(
                              title='Hub Site Network',
                              showlegend=False,
                              hovermode='closest',
                              margin=dict(b=0,l=0,r=0,t=40),
                              xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                              yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                              height=600
                          ))

            st.plotly_chart(fig, use_container_width=True)

        # Hub site details table
        st.markdown("### 📋 Hub Site Details")

        hub_summary = hub_relationships.groupby(['hub_id', 'hub_title']).agg({
            'associated_site_id': 'count',
            'shared_files': 'sum',
            'shared_users': 'sum'
        }).reset_index()

        hub_summary.columns = ['Hub ID', 'Hub Site', 'Associated Sites', 'Shared Files', 'Shared Users']

        st.dataframe(
            hub_summary.sort_values('Associated Sites', ascending=False),
            hide_index=True,
            use_container_width=True
        )

    def _render_libraries_view(self, df: pd.DataFrame):
        """Render libraries analysis"""
        st.subheader("📚 Libraries Analysis")

        # Library overview metrics
        total_libraries = df['library_count'].sum()
        avg_libraries_per_site = df['library_count'].mean()

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Libraries", f"{total_libraries:,}")

        with col2:
            st.metric("Avg Libraries per Site", f"{avg_libraries_per_site:.1f}")

        with col3:
            sites_many_libs = len(df[df['library_count'] > 10])
            st.metric("Sites with >10 Libraries", f"{sites_many_libs}")

        with col4:
            sites_no_libs = len(df[df['library_count'] == 0])
            st.metric("Sites with No Libraries", f"{sites_no_libs}")

        # Library distribution
        col1, col2 = st.columns(2)

        with col1:
            # Distribution of libraries per site
            fig = px.histogram(
                df,
                x='library_count',
                nbins=30,
                title="Distribution of Libraries per Site",
                labels={'library_count': 'Number of Libraries', 'count': 'Number of Sites'}
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Top sites by library count
            # Ensure library_count is numeric
            df['library_count'] = pd.to_numeric(df['library_count'], errors='coerce').fillna(0)
            top_lib_sites = df.nlargest(10, 'library_count')[['title', 'library_count']]

            fig = px.bar(
                top_lib_sites,
                x='library_count',
                y='title',
                orientation='h',
                title="Top 10 Sites by Library Count",
                labels={'library_count': 'Libraries', 'title': 'Site'}
            )
            st.plotly_chart(fig, use_container_width=True)

        # Library details
        st.markdown("### 📂 Library Details")

        # Site selector for library details
        selected_site = st.selectbox(
            "Select a site to view its libraries",
            df.sort_values('title')['title'].tolist(),
            key="library_site_select"
        )

        if selected_site:
            site_data = df[df['title'] == selected_site].iloc[0]
            site_libraries = self.load_library_details(site_data['site_id'])

            if not site_libraries.empty:
                st.info(f"Showing {len(site_libraries)} libraries for {selected_site}")

                # Prepare display data
                lib_display = site_libraries[[
                    'name', 'item_count', 'total_size', 'sensitive_files',
                    'is_hidden', 'enable_versioning'
                ]].copy()

                lib_display['total_size_mb'] = (pd.to_numeric(lib_display['total_size'], errors='coerce').fillna(0) / (1024 * 1024)).round(2)
                lib_display = lib_display.drop('total_size', axis=1)

                lib_display.columns = [
                    'Library Name', 'Item Count', 'Sensitive Files',
                    'Hidden', 'Versioning', 'Size (MB)'
                ]

                st.dataframe(
                    lib_display,
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("No libraries found for this site")

    def _render_trends_insights(self, df: pd.DataFrame):
        """Render trends and insights"""
        st.subheader("📈 Trends & Insights")

        # Activity insights
        st.markdown("### 📊 Activity Insights")

        # Calculate days since last modification
        if 'last_file_modified' in df.columns:
            df['last_file_modified'] = pd.to_datetime(df['last_file_modified'])
            df['days_since_modified'] = (datetime.now() - df['last_file_modified']).dt.days

            # Categorize activity
            df['activity_status'] = pd.cut(
                df['days_since_modified'],
                bins=[0, 7, 30, 90, 365, float('inf')],
                labels=['Very Active', 'Active', 'Moderate', 'Low', 'Dormant']
            )

            # Activity distribution
            activity_dist = df['activity_status'].value_counts()

            col1, col2 = st.columns(2)

            with col1:
                fig = px.pie(
                    values=activity_dist.values,
                    names=activity_dist.index,
                    title="Site Activity Distribution",
                    color_discrete_map={
                        'Very Active': '#10b981',
                        'Active': '#3b82f6',
                        'Moderate': '#f59e0b',
                        'Low': '#ef4444',
                        'Dormant': '#6b7280'
                    }
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Dormant sites with high storage
                dormant_sites = df[
                    (df['activity_status'] == 'Dormant') &
                    (df['storage_used_gb'] > 10)
                ][['title', 'storage_used_gb', 'days_since_modified']].head(10)

                if not dormant_sites.empty:
                    st.markdown("#### 💤 Dormant Sites with High Storage")
                    st.dataframe(dormant_sites, hide_index=True)

                    total_dormant_storage = dormant_sites['storage_used_gb'].sum()
                    st.info(f"💡 Potential storage savings: {total_dormant_storage:.2f} GB from archiving dormant sites")

        # Growth insights
        st.markdown("### 📈 Growth Patterns")

        col1, col2 = st.columns(2)

        with col1:
            # Sites by creation date (if available)
            if 'created_at' in df.columns:
                df['created_at'] = pd.to_datetime(df['created_at'])
                # Remove timezone info before converting to period
                df['creation_month'] = df['created_at'].dt.tz_localize(None).dt.to_period('M')

                monthly_creation = df.groupby('creation_month').size()

                if not monthly_creation.empty:
                    fig = px.line(
                        x=monthly_creation.index.astype(str),
                        y=monthly_creation.values,
                        title="Site Creation Trend",
                        labels={'x': 'Month', 'y': 'Sites Created'}
                    )
                    st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Correlation analysis
            st.markdown("#### 🔍 Correlation Insights")

            correlations = []

            # Storage vs Files correlation
            storage_file_corr = df['storage_used_gb'].corr(df['file_count'])
            correlations.append(("Storage vs File Count", f"{storage_file_corr:.2f}"))

            # External users vs sensitive files
            external_sensitive_corr = df['external_user_count'].corr(df['sensitive_file_count'])
            correlations.append(("External Users vs Sensitive Files", f"{external_sensitive_corr:.2f}"))

            # Permission complexity vs site size
            perm_size_corr = df['permission_complexity'].corr(df['file_count'])
            correlations.append(("Permission Complexity vs Site Size", f"{perm_size_corr:.2f}"))

            corr_df = pd.DataFrame(correlations, columns=['Metric', 'Correlation'])
            st.dataframe(corr_df, hide_index=True)

        # Key insights summary
        st.markdown("### 💡 Key Insights Summary")

        insights = []

        # Storage insights
        if df['storage_utilization'].max() > 90:
            critical_storage = len(df[df['storage_utilization'] > 90])
            insights.append(f"🚨 {critical_storage} sites are above 90% storage capacity")

        # Security insights
        external_heavy = len(df[df['external_exposure'] > 50])
        if external_heavy > 0:
            insights.append(f"⚠️ {external_heavy} sites have more external users than internal")

        # Activity insights
        if 'activity_status' in df.columns:
            dormant_count = len(df[df['activity_status'] == 'Dormant'])
            if dormant_count > len(df) * 0.2:
                insights.append(f"💤 {dormant_count} sites ({dormant_count/len(df)*100:.1f}%) appear dormant")

        # Permission insights
        complex_sites = len(df[df['permission_complexity'] > 70])
        if complex_sites > 0:
            insights.append(f"🔐 {complex_sites} sites have highly complex permission structures")

        if insights:
            for insight in insights:
                st.warning(insight)
        else:
            st.success("✅ No critical insights requiring immediate attention")
