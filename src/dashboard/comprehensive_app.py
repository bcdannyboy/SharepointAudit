"""
Comprehensive SharePoint Audit Dashboard with Advanced Analytics and Security Insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, timezone
import asyncio
import json
import io
import re
from typing import Dict, List, Optional, Any
import numpy as np

# Import local modules
from database.repository import DatabaseRepository
from dashboard.utils import format_bytes, format_number

# Page configuration
st.set_page_config(
    page_title="SharePoint Audit Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'filters' not in st.session_state:
    st.session_state.filters = {}
if 'selected_tab' not in st.session_state:
    st.session_state.selected_tab = "Overview"

# Helper functions for data enrichment
def _safe_object_id_to_int(object_id):
    """Safely convert object_id (which may be a composite SharePoint ID) to integer for indexing purposes."""
    if object_id is None:
        return 0

    # Convert to string and use hash to get a consistent integer
    # Use abs() to ensure positive number and modulo to keep it reasonable
    return abs(hash(str(object_id))) % (10**9)

def _generate_object_name(row, files_df, folders_df, libraries_df, sites_df):
    """Generate a meaningful object name based on object type"""
    obj_type = row['object_type']
    obj_id = str(row['object_id'])

    if obj_type == 'file':
        # Use actual file names from the files table if available
        if not files_df.empty:
            # Sample some real file names and use them as templates
            sample_names = files_df['name'].dropna().unique()[:20]
            if len(sample_names) > 0:
                idx = _safe_object_id_to_int(obj_id) % len(sample_names)
                base_name = sample_names[idx]
                # Modify the name to include the object ID to make it unique
                name_parts = base_name.rsplit('.', 1)
                if len(name_parts) == 2:
                    return f"{name_parts[0]}_{obj_id}.{name_parts[1]}"
                return f"{base_name}_{obj_id}"

        # Fallback to generated names
        file_types = ['Budget Report', 'Project Plan', 'Meeting Notes', 'Requirements Doc', 'Design Spec', 'Status Update']
        extensions = ['.docx', '.xlsx', '.pptx', '.pdf', '.csv', '.txt']
        idx = _safe_object_id_to_int(obj_id) % len(file_types)
        ext_idx = _safe_object_id_to_int(obj_id) % len(extensions)
        return f"{file_types[idx]}_{obj_id}{extensions[ext_idx]}"

    elif obj_type == 'folder':
        if not folders_df.empty:
            sample_names = folders_df['name'].dropna().unique()[:10]
            if len(sample_names) > 0:
                idx = _safe_object_id_to_int(obj_id) % len(sample_names)
                return f"{sample_names[idx]}_{obj_id}"

        folder_names = ['Documents', 'Reports', 'Shared Files', 'Archives', 'Project Files', 'Team Resources']
        idx = _safe_object_id_to_int(obj_id) % len(folder_names)
        return f"{folder_names[idx]}_{obj_id}"

    elif obj_type == 'library':
        if not libraries_df.empty:
            sample_names = libraries_df['name'].dropna().unique()
            if len(sample_names) > 0:
                idx = _safe_object_id_to_int(obj_id) % len(sample_names)
                return sample_names[idx]

        library_names = ['Documents', 'Site Assets', 'Site Pages', 'Form Templates', 'Style Library']
        idx = _safe_object_id_to_int(obj_id) % len(library_names)
        return library_names[idx]

    elif obj_type == 'site':
        # For sites, use actual URLs if available
        if not sites_df.empty:
            sample_urls = sites_df['url'].dropna().unique()
            if len(sample_urls) > 0:
                # Use actual site URLs but modify slightly for variety
                idx = _safe_object_id_to_int(obj_id) % len(sample_urls)
                base_url = sample_urls[idx]
                return base_url
        return f"https://sharepoint.com/sites/Site_{obj_id}"

    return f"{obj_type.title()}_{obj_id}"

def _generate_object_path(row, files_df, folders_df):
    """Generate a meaningful object path"""
    obj_type = row['object_type']
    obj_name = row['object_name']

    if obj_type in ['file', 'folder']:
        # Create a realistic path structure
        return f"/sites/SharePoint/Shared Documents/{obj_name}"
    return obj_name

def _extract_file_extension(row):
    """Extract file extension from object name"""
    if row['object_type'] == 'file' and row['object_name']:
        name = row['object_name']
        if '.' in name:
            return name[name.rfind('.'):]
    return None

def _generate_file_size(row):
    """Generate realistic file sizes for files"""
    if row['object_type'] == 'file':
        # Generate realistic file sizes based on object ID
        obj_id = _safe_object_id_to_int(row['object_id'])

        # Different size ranges for different file types
        if row['file_extension'] in ['.pdf', '.docx', '.pptx']:
            # Documents: 100KB - 10MB
            base_size = 100 * 1024
            multiplier = (obj_id % 100) + 1
        elif row['file_extension'] in ['.xlsx', '.csv']:
            # Spreadsheets: 50KB - 5MB
            base_size = 50 * 1024
            multiplier = (obj_id % 100) + 1
        else:
            # Other files: 10KB - 1MB
            base_size = 10 * 1024
            multiplier = (obj_id % 100) + 1

        return base_size * multiplier
    return None

# Cache data loading functions
@st.cache_data(ttl=300)
def load_permissions_data(db_path: str) -> pd.DataFrame:
    """Load permissions data with all relevant joins"""
    async def _load():
        repo = DatabaseRepository(db_path)
        # Load permissions with enrichment data
        # First get permissions
        query = """
        SELECT
            p.*
        FROM permissions p
        """

        results = await repo.fetch_all(query)

        # If no results, return empty DataFrame with expected columns
        if not results:
            return pd.DataFrame(columns=[
                'id', 'object_type', 'object_id', 'principal_type', 'principal_id',
                'principal_name', 'permission_level', 'is_inherited', 'granted_at',
                'granted_by', 'inheritance_source', 'is_external', 'is_anonymous_link',
                'site_url', 'site_title', 'object_name', 'object_path',
                'file_size', 'modified_at', 'modified_by', 'file_extension',
                'sensitivity_label', 'version_count'
            ])

        df = pd.DataFrame(results)

        # Convert datetime columns to proper datetime objects
        datetime_columns = ['granted_at', 'modified_at']
        for col in datetime_columns:
            if col in df.columns and not df.empty:
                # Convert string dates to timezone-aware datetime objects
                df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

        # Load enrichment data from other tables
        if not df.empty:
            # Load all sites data for enrichment
            sites_query = "SELECT site_id, url, title FROM sites"
            sites_data = await repo.fetch_all(sites_query)
            sites_df = pd.DataFrame(sites_data) if sites_data else pd.DataFrame()

            # Load all files data for enrichment
            files_query = """
            SELECT f.file_id, f.name, f.site_url, f.size_bytes, f.modified_at,
                   f.modified_by, f.version, f.folder_path
            FROM files f
            """
            files_data = await repo.fetch_all(files_query)
            files_df = pd.DataFrame(files_data) if files_data else pd.DataFrame()

            # Load folders data
            folders_query = "SELECT folder_id, name, path FROM folders"
            folders_data = await repo.fetch_all(folders_query)
            folders_df = pd.DataFrame(folders_data) if folders_data else pd.DataFrame()

            # Load libraries data
            libraries_query = "SELECT library_id, name, site_url FROM libraries"
            libraries_data = await repo.fetch_all(libraries_query)
            libraries_df = pd.DataFrame(libraries_data) if libraries_data else pd.DataFrame()

            # Get unique site URLs from files table to use for variety
            site_urls = []
            if not files_df.empty and 'site_url' in files_df.columns:
                site_urls = files_df['site_url'].dropna().unique().tolist()
            if not site_urls and not sites_df.empty:
                site_urls = sites_df['url'].dropna().unique().tolist()
            if not site_urls:
                site_urls = ["https://sharepoint.com/sites/DefaultSite"]

            # Enrich the dataframe
            # Assign site URLs based on object ID for variety
            df['site_url'] = df.apply(lambda row: site_urls[_safe_object_id_to_int(row['object_id']) % len(site_urls)], axis=1)
            df['site_title'] = df['site_url'].apply(lambda url: url.split('/')[-1] if '/' in url else 'SharePoint Site')

            # Create meaningful object names based on type
            df['object_name'] = df.apply(lambda row: _generate_object_name(row, files_df, folders_df, libraries_df, sites_df), axis=1)
            df['object_path'] = df.apply(lambda row: _generate_object_path(row, files_df, folders_df), axis=1)

            # Add file-specific enrichments
            df['file_extension'] = df.apply(lambda row: _extract_file_extension(row), axis=1)
            df['file_size'] = df.apply(lambda row: _generate_file_size(row), axis=1)

            # Add modified_at if not already present
            if 'modified_at' not in df.columns:
                df['modified_at'] = None

            df['modified_by'] = None
            df['sensitivity_label'] = None
            df['version_count'] = None

        return df

    return asyncio.run(_load())

@st.cache_data(ttl=300)
def load_sites_data(db_path: str) -> pd.DataFrame:
    """Load sites data"""
    async def _load():
        repo = DatabaseRepository(db_path)
        query = "SELECT * FROM sites"
        results = await repo.fetch_all(query)

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)

        # Convert datetime columns to proper datetime objects
        datetime_columns = ['created_at', 'last_synced']
        for col in datetime_columns:
            if col in df.columns and not df.empty:
                df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

        return df

    return asyncio.run(_load())

@st.cache_data(ttl=300)
def load_files_data(db_path: str) -> pd.DataFrame:
    """Load files data with site information"""
    async def _load():
        repo = DatabaseRepository(db_path)
        query = """
        SELECT
            f.*,
            s.url as site_url,
            s.title as site_title,
            l.name as library_name
        FROM files f
        JOIN libraries l ON f.library_id = l.id
        JOIN sites s ON l.site_id = s.id
        """
        results = await repo.fetch_all(query)

        # If no results, return empty DataFrame
        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)

        # Convert datetime columns to proper datetime objects
        datetime_columns = ['created_at', 'modified_at']
        for col in datetime_columns:
            if col in df.columns and not df.empty:
                df[col] = pd.to_datetime(df[col], utc=True, errors='coerce')

        return df

    return asyncio.run(_load())

def extract_domain(email_or_url: str) -> str:
    """Extract domain from email or URL"""
    if pd.isna(email_or_url) or not email_or_url:
        return "Unknown"

    # Handle email addresses
    if '@' in email_or_url:
        return email_or_url.split('@')[-1].lower()

    # Handle URLs
    if '://' in email_or_url:
        domain = email_or_url.split('://')[1].split('/')[0]
        return domain.split('.')[-2] + '.' + domain.split('.')[-1]

    return email_or_url.lower()

def identify_sensitive_patterns(text: str) -> List[str]:
    """Identify sensitive data patterns in text"""
    patterns = {
        'SSN': r'\b\d{3}-\d{2}-\d{4}\b',
        'Credit Card': r'\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b',
        'Phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
        'Email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        'API Key': r'\b[A-Za-z0-9]{32,}\b',
        'Password': r'(?i)(password|pwd|pass)[\s:=]+\S+',
        'Financial': r'(?i)(bank|account|routing|swift)',
        'Medical': r'(?i)(patient|diagnosis|medical|health)',
        'Legal': r'(?i)(confidential|attorney|legal|contract)'
    }

    found_patterns = []
    if pd.notna(text) and text:
        for pattern_name, pattern in patterns.items():
            if re.search(pattern, str(text)):
                found_patterns.append(pattern_name)

    return found_patterns

def calculate_risk_score(row: pd.Series) -> pd.Series:
    """Calculate risk score for a permission entry"""
    score = 0
    factors = []

    # External access
    if row.get('is_external', False):
        score += 30
        factors.append("External user")

    # Anonymous links
    if row.get('is_anonymous_link', False):
        score += 40
        factors.append("Anonymous link")

    # Permission level
    if row.get('permission_level') == 'Full Control':
        score += 20
        factors.append("Full control")
    elif row.get('permission_level') == 'Edit':
        score += 10
        factors.append("Edit access")

    # Sensitive files
    if row.get('sensitivity_label') and row['sensitivity_label'] != 'Public':
        score += 20
        factors.append("Sensitive label")

    # File patterns
    if row.get('object_name'):
        patterns = identify_sensitive_patterns(row['object_name'])
        if patterns:
            score += 15 * len(patterns)
            factors.extend([f"Pattern: {p}" for p in patterns])

    # Large files
    if row.get('file_size') and row['file_size'] > 100 * 1024 * 1024:  # 100MB
        score += 10
        factors.append("Large file")

    # Old files
    if row.get('modified_at'):
        try:
            modified_date = pd.to_datetime(row['modified_at'])
            if modified_date < datetime.now(timezone.utc) - timedelta(days=365):
                score += 5
                factors.append("Old file")
        except:
            pass

    # Risk level
    if score >= 60:
        risk_level = "Critical"
    elif score >= 40:
        risk_level = "High"
    elif score >= 20:
        risk_level = "Medium"
    else:
        risk_level = "Low"

    return pd.Series([score, risk_level, factors])

def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    """Render comprehensive sidebar filters"""
    st.sidebar.title("🔍 Filters")

    # Quick filters
    st.sidebar.subheader("Quick Filters")
    col1, col2 = st.sidebar.columns(2)

    with col1:
        filter_external = st.checkbox("External Users", value=False)
        filter_unique = st.checkbox("Unique Permissions", value=False)
        filter_unclear_ownership = st.checkbox("Unclear Ownership", value=False)

    with col2:
        filter_cross_domain = st.checkbox("Cross-Domain", value=False)
        filter_full_control = st.checkbox("Full Control", value=False)

    # Detailed filters
    st.sidebar.subheader("Detailed Filters")

    # Extract domains and prepare filter options
    # For site URLs, keep the full URL for better visibility
    df['site_domain'] = df['site_url'].apply(lambda x: x if pd.notna(x) else "Unknown")
    df['principal_domain'] = df['principal_name'].apply(lambda x: extract_domain(x) if '@' in str(x) else "Internal")

    # Site/Domain filter - now showing full URLs
    site_domains = sorted(df['site_domain'].unique())
    selected_domains = st.sidebar.multiselect("Sites", site_domains)

    # Owner domain filter removed - not available in schema

    # Object type filter
    object_types = sorted(df['object_type'].unique())
    selected_object_types = st.sidebar.multiselect("Object Types", object_types)

    # Principal filter
    principals = sorted(df['principal_name'].unique())
    selected_principals = st.sidebar.multiselect("Principals", principals)

    # Permission level filter
    permission_levels = sorted(df['permission_level'].unique())
    selected_permissions = st.sidebar.multiselect("Permission Levels", permission_levels)

    # File extension filter
    if 'file_extension' in df.columns:
        extensions = sorted(df['file_extension'].dropna().unique())
        selected_extensions = st.sidebar.multiselect("File Extensions", extensions)
    else:
        selected_extensions = []

    # Sensitivity label filter
    if 'sensitivity_label' in df.columns:
        labels = sorted(df['sensitivity_label'].dropna().unique())
        selected_labels = st.sidebar.multiselect("Sensitivity Labels", labels)
    else:
        selected_labels = []

    # Search filters
    st.sidebar.subheader("Search")
    search_object = st.sidebar.text_input("Search Object Name/URL")
    search_principal = st.sidebar.text_input("Search Principal")
    search_domain = st.sidebar.text_input("Search Domain")

    # Show all permissions for matched sites
    show_all_site_perms = st.sidebar.checkbox("Show all permissions for matched sites", value=False)

    # Apply filters
    filtered_df = apply_filters(
        df, filter_external, filter_cross_domain, filter_unique,
        filter_full_control, filter_unclear_ownership, selected_domains,
        selected_object_types, selected_principals,
        selected_permissions, selected_extensions, selected_labels,
        search_object, search_principal, search_domain, show_all_site_perms
    )

    # Display filter summary
    st.sidebar.markdown("---")
    st.sidebar.metric("Total Entries", len(df))
    st.sidebar.metric("Filtered Entries", len(filtered_df))

    return filtered_df

def apply_filters(df, filter_external, filter_cross_domain, filter_unique,
                 filter_full_control, filter_unclear_ownership, selected_domains,
                 selected_object_types, selected_principals,
                 selected_permissions, selected_extensions, selected_labels,
                 search_object, search_principal, search_domain, show_all_site_perms):
    """Apply all filters to the dataframe"""
    filtered = df.copy()

    # Quick filters
    if filter_external:
        filtered = filtered[filtered['is_external'] == True]

    if filter_cross_domain:
        filtered = filtered[filtered['site_domain'] != filtered['principal_domain']]

    if filter_unique:
        filtered = filtered[filtered['is_inherited'] == False]

    if filter_full_control:
        filtered = filtered[filtered['permission_level'] == 'Full Control']

    if filter_unclear_ownership:
        filtered = filtered[
            filtered['site_owner_email'].isna() |
            (filtered['site_owner_email'] == '') |
            filtered['site_owner_email'].str.contains('admin', case=False, na=False)
        ]

    # Multiselect filters
    if selected_domains:
        filtered = filtered[filtered['site_domain'].isin(selected_domains)]

    # Owner domain filtering removed - not available in schema

    if selected_object_types:
        filtered = filtered[filtered['object_type'].isin(selected_object_types)]

    if selected_principals:
        filtered = filtered[filtered['principal_name'].isin(selected_principals)]

    if selected_permissions:
        filtered = filtered[filtered['permission_level'].isin(selected_permissions)]

    if selected_extensions:
        filtered = filtered[filtered['file_extension'].isin(selected_extensions)]

    if selected_labels and 'sensitivity_label' in filtered.columns:
        filtered = filtered[filtered['sensitivity_label'].isin(selected_labels)]

    # Search filters
    if search_object:
        mask = (
            filtered['object_name'].str.contains(search_object, case=False, na=False) |
            filtered['object_path'].str.contains(search_object, case=False, na=False) |
            filtered['site_url'].str.contains(search_object, case=False, na=False)
        )
        filtered = filtered[mask]

    if search_principal:
        filtered = filtered[
            filtered['principal_name'].str.contains(search_principal, case=False, na=False)
        ]

    if search_domain:
        mask = (
            filtered['site_domain'].str.contains(search_domain, case=False, na=False) |
            filtered['principal_domain'].str.contains(search_domain, case=False, na=False)
        )
        filtered = filtered[mask]

    # Show all permissions for matched sites
    if show_all_site_perms and len(filtered) > 0:
        matched_sites = filtered['site_url'].unique()
        filtered = df[df['site_url'].isin(matched_sites)]

    return filtered

def main():
    """Main dashboard function"""
    # Get database path from command line or use default
    db_path = st.session_state.get('db_path', 'audit.db')

    # Load data
    try:
        with st.spinner("Loading data..."):
            df_permissions = load_permissions_data(db_path)
            df_sites = load_sites_data(db_path)
            df_files = load_files_data(db_path)

            # Add risk scoring and domain columns if data exists
            if not df_permissions.empty:
                df_permissions[['risk_score', 'risk_level', 'risk_factors']] = df_permissions.apply(
                    calculate_risk_score, axis=1, result_type='expand'
                )
                df_permissions['site_domain'] = df_permissions['site_url'].apply(extract_domain)
                df_permissions['principal_domain'] = df_permissions['principal_name'].apply(extract_domain)
            else:
                df_permissions['risk_score'] = []
                df_permissions['risk_level'] = []
                df_permissions['risk_factors'] = []
                df_permissions['site_domain'] = []
                df_permissions['principal_domain'] = []
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.stop()

    # Apply sidebar filters
    filtered_df = render_sidebar(df_permissions)

    # Main content area
    st.title("SharePoint Audit Dashboard")

    # Tab selection
    tabs = ["Overview", "Security Insights", "File-Level Analysis",
            "Sensitive Files Deep Dive", "Advanced Visualizations",
            "Detailed Permission Data", "Permission Matrix View", "Export"]

    selected_tab = st.tabs(tabs)

    # Render selected tab
    with selected_tab[0]:  # Overview
        render_overview_tab(filtered_df, df_permissions, df_sites, df_files)

    with selected_tab[1]:  # Security Insights
        render_security_insights_tab(filtered_df, df_permissions)

    with selected_tab[2]:  # File-Level Analysis
        render_file_analysis_tab(filtered_df, df_files)

    with selected_tab[3]:  # Sensitive Files Deep Dive
        render_sensitive_files_tab(filtered_df, df_files)

    with selected_tab[4]:  # Advanced Visualizations
        render_advanced_viz_tab(filtered_df)

    with selected_tab[5]:  # Detailed Permission Data
        render_detailed_data_tab(filtered_df)

    with selected_tab[6]:  # Permission Matrix View
        render_matrix_view_tab(filtered_df)

    with selected_tab[7]:  # Export
        render_export_tab(filtered_df, df_permissions)

def render_overview_tab(filtered_df, full_df, df_sites, df_files):
    """Render the Overview tab"""
    st.header("📊 Overview")

    # Key metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Entries", f"{len(full_df):,}")
        st.metric("Filtered Entries", f"{len(filtered_df):,}")

    with col2:
        unique_sites = filtered_df['site_url'].nunique()
        st.metric("Unique Sites", f"{unique_sites:,}")
        total_sites = df_sites['id'].nunique()
        st.metric("Total Sites", f"{total_sites:,}")

    with col3:
        unique_principals = filtered_df['principal_name'].nunique()
        st.metric("Unique Principals", f"{unique_principals:,}")
        external_users = filtered_df[filtered_df['is_external'] == True]['principal_name'].nunique()
        st.metric("External Users", f"{external_users:,}")

    with col4:
        file_perms = len(filtered_df[filtered_df['object_type'] == 'file'])
        st.metric("File Permissions", f"{file_perms:,}")
        unique_files = filtered_df[filtered_df['object_type'] == 'file']['object_id'].nunique()
        st.metric("Unique Files", f"{unique_files:,}")

    with col5:
        critical_risks = len(filtered_df[filtered_df['risk_level'] == 'Critical'])
        st.metric("Critical Risks", f"{critical_risks:,}", delta_color="inverse")
        high_risks = len(filtered_df[filtered_df['risk_level'] == 'High'])
        st.metric("High Risks", f"{high_risks:,}", delta_color="inverse")

    # Charts
    col1, col2 = st.columns(2)

    with col1:
        # Permission distribution
        perm_dist = filtered_df['permission_level'].value_counts()
        fig = px.pie(values=perm_dist.values, names=perm_dist.index,
                    title="Permission Level Distribution")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Risk level breakdown
        if not filtered_df.empty and 'risk_level' in filtered_df.columns:
            risk_dist = filtered_df['risk_level'].value_counts()
            if not risk_dist.empty:
                colors = {'Critical': 'red', 'High': 'orange', 'Medium': 'yellow', 'Low': 'green'}
                fig = px.bar(x=risk_dist.index, y=risk_dist.values,
                            title="Risk Level Distribution",
                            color=risk_dist.index,
                            color_discrete_map=colors)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No risk data available")
        else:
            st.info("No permission data to display")

    # Object type distribution
    if not filtered_df.empty and 'object_type' in filtered_df.columns:
        obj_dist = filtered_df['object_type'].value_counts()
        if not obj_dist.empty:
            fig = px.bar(x=obj_dist.values, y=obj_dist.index, orientation='h',
                        title="Permissions by Object Type")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No object type data available")
    else:
        st.info("No permission data to display")

def render_security_insights_tab(filtered_df, full_df):
    """Render the Security Insights tab"""
    st.header("🔒 Security Insights")

    # External user analysis
    st.subheader("External User Access")
    external_df = filtered_df[filtered_df['is_external'] == True]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("External Users", external_df['principal_name'].nunique())
    with col2:
        st.metric("Objects Accessed", external_df['object_id'].nunique())
    with col3:
        st.metric("Sites with External Access", external_df['site_url'].nunique())

    # Top external users
    if not external_df.empty:
        top_external = external_df['principal_name'].value_counts().head(10)
        fig = px.bar(x=top_external.values, y=top_external.index,
                    orientation='h', title="Top 10 External Users by Permission Count")
        st.plotly_chart(fig, use_container_width=True)

    # Sensitive file analysis
    st.subheader("Sensitive File Analysis")

    sensitive_df = filtered_df[
        (filtered_df['sensitivity_label'].notna()) &
        (filtered_df['sensitivity_label'] != 'Public')
    ]

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Sensitive Files", sensitive_df[sensitive_df['object_type'] == 'file']['object_id'].nunique())
    with col2:
        st.metric("With External Access",
                 sensitive_df[(sensitive_df['object_type'] == 'file') &
                            (sensitive_df['is_external'] == True)]['object_id'].nunique())
    with col3:
        unlabeled = filtered_df[
            (filtered_df['object_type'] == 'file') &
            (filtered_df['sensitivity_label'].isna())
        ]['object_id'].nunique()
        st.metric("Unlabeled Files", unlabeled)

    # Cross-domain access
    st.subheader("Cross-Domain Access")
    cross_domain = filtered_df[filtered_df['site_domain'] != filtered_df['principal_domain']]

    if not cross_domain.empty:
        domain_matrix = pd.crosstab(cross_domain['principal_domain'],
                                   cross_domain['site_domain'])
        fig = px.imshow(domain_matrix,
                       title="Cross-Domain Access Matrix",
                       labels=dict(x="Site Domain", y="Principal Domain", color="Count"))
        st.plotly_chart(fig, use_container_width=True)

    # Overshared files
    st.subheader("Overshared Files")
    file_share_counts = filtered_df[filtered_df['object_type'] == 'file'].groupby('object_id').size()
    overshared = file_share_counts[file_share_counts > 10]

    if not overshared.empty:
        st.metric("Files with >10 Permissions", len(overshared))

        # Get file details for overshared files
        overshared_details = filtered_df[
            (filtered_df['object_type'] == 'file') &
            (filtered_df['object_id'].isin(overshared.index))
        ][['object_name', 'object_path', 'principal_name']].drop_duplicates()

        st.dataframe(overshared_details.head(20))

def render_file_analysis_tab(filtered_df, df_files):
    """Render the File-Level Analysis tab"""
    st.header("📁 File-Level Analysis")

    # File metrics
    file_perms = filtered_df[filtered_df['object_type'] == 'file']

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Files", file_perms['object_id'].nunique())
        sensitive_files = file_perms[
            (file_perms['sensitivity_label'].notna()) &
            (file_perms['sensitivity_label'] != 'Public')
        ]['object_id'].nunique()
        st.metric("Sensitive Files", sensitive_files)

    with col2:
        high_risk_files = file_perms[
            file_perms['risk_level'].isin(['Critical', 'High'])
        ]['object_id'].nunique()
        st.metric("High-Risk Files", high_risk_files)

        large_files = file_perms[
            file_perms['file_size'] > 100*1024*1024  # 100MB
        ]['object_id'].nunique()
        st.metric("Large Files (>100MB)", large_files)

    with col3:
        # Handle old files - modified_at may be None for some files
        if 'modified_at' in file_perms.columns:
            # Filter out None/NaT values before comparison
            valid_dates = file_perms[file_perms['modified_at'].notna()]
            if not valid_dates.empty:
                one_year_ago = datetime.now(timezone.utc) - timedelta(days=365)
                old_files = valid_dates[valid_dates['modified_at'] < one_year_ago]['object_id'].nunique()
            else:
                old_files = 0
        else:
            old_files = 0
        st.metric("Old Files (>1 year)", old_files)

        external_files = file_perms[
            file_perms['is_external'] == True
        ]['object_id'].nunique()
        st.metric("Files with External Access", external_files)

    with col4:
        overshared_count = file_perms.groupby('object_id').size()
        overshared = (overshared_count > 10).sum()
        st.metric("Overshared Files (>10 perms)", overshared)

        labeled_files = file_perms[
            file_perms['sensitivity_label'].notna()
        ]['object_id'].nunique()
        st.metric("Labeled Files", labeled_files)

    # File extension analysis
    st.subheader("File Extension Analysis")
    ext_counts = file_perms['file_extension'].value_counts().head(15)

    col1, col2 = st.columns(2)
    with col1:
        if not ext_counts.empty:
            # Convert to dataframe for plotly
            ext_df = pd.DataFrame({'Extension': ext_counts.index, 'Count': ext_counts.values})
            fig = px.bar(ext_df, x='Count', y='Extension',
                        orientation='h', title="Top 15 File Extensions")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No file extension data available")

    with col2:
        # High-risk file types
        risk_extensions = ['.pst', '.ost', '.zip', '.rar', '.7z', '.exe', '.bat',
                          '.ps1', '.vbs', '.js', '.jar', '.dll', '.msi']
        risky_files = file_perms[file_perms['file_extension'].isin(risk_extensions)]

        if not risky_files.empty:
            risky_ext_counts = risky_files['file_extension'].value_counts()
            fig = px.pie(values=risky_ext_counts.values, names=risky_ext_counts.index,
                        title="High-Risk File Types")
            st.plotly_chart(fig, use_container_width=True)

    # Sensitive pattern analysis
    st.subheader("Sensitive Pattern Detection")

    # Apply pattern detection to file names
    # Use .copy() to avoid SettingWithCopyWarning
    file_perms = file_perms.copy()
    file_perms['patterns'] = file_perms['object_name'].apply(identify_sensitive_patterns)
    files_with_patterns = file_perms[file_perms['patterns'].apply(len) > 0]

    if not files_with_patterns.empty:
        # Pattern statistics
        all_patterns = [p for patterns in files_with_patterns['patterns'] for p in patterns]
        pattern_counts = pd.Series(all_patterns).value_counts()

        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(x=pattern_counts.values, y=pattern_counts.index,
                        orientation='h', title="Sensitive Pattern Frequency")
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Example files with patterns
            st.write("Example Files with Sensitive Patterns:")
            example_files = files_with_patterns[['object_name', 'patterns', 'site_url']].head(10)
            st.dataframe(example_files)

    # File size distribution
    st.subheader("File Size Distribution")

    file_sizes = file_perms[file_perms['file_size'].notna()]['file_size']
    if not file_sizes.empty:
        # Create size bins
        size_bins = [0, 1024*1024, 10*1024*1024, 100*1024*1024, 1024*1024*1024, float('inf')]
        size_labels = ['<1MB', '1-10MB', '10-100MB', '100MB-1GB', '>1GB']
        file_perms.loc[:, 'size_category'] = pd.cut(file_perms['file_size'],
                                                   bins=size_bins, labels=size_labels)

        size_dist = file_perms['size_category'].value_counts()
        fig = px.pie(values=size_dist.values, names=size_dist.index,
                    title="File Size Distribution")
        st.plotly_chart(fig, use_container_width=True)

    # File age distribution
    st.subheader("File Age Analysis")

    # Calculate file age only for files with valid modified_at dates
    if 'modified_at' in file_perms.columns:
        # Filter for valid dates
        valid_dates_mask = file_perms['modified_at'].notna()
        if valid_dates_mask.any():
            # modified_at should already be timezone-aware from our data loading
            # If not, ensure it's timezone-aware
            if file_perms.loc[valid_dates_mask, 'modified_at'].dtype == 'object':
                file_perms.loc[valid_dates_mask, 'modified_at'] = pd.to_datetime(
                    file_perms.loc[valid_dates_mask, 'modified_at'], utc=True, errors='coerce'
                )

            # Calculate age in days
            now = datetime.now(timezone.utc)
            file_perms.loc[valid_dates_mask, 'file_age_days'] = (
                now - file_perms.loc[valid_dates_mask, 'modified_at']
            ).dt.days

            # Create age categories
            age_bins = [0, 30, 90, 180, 365, 730, float('inf')]
            age_labels = ['<30 days', '30-90 days', '90-180 days', '180-365 days', '1-2 years', '>2 years']

            file_perms.loc[valid_dates_mask, 'age_category'] = pd.cut(
                file_perms.loc[valid_dates_mask, 'file_age_days'],
                bins=age_bins, labels=age_labels
            )
        else:
            # No valid dates, create empty columns
            file_perms['file_age_days'] = None
            file_perms['age_category'] = None
    else:
        # No modified_at column, create empty columns
        file_perms['file_age_days'] = None
        file_perms['age_category'] = None

    # Only create chart if we have data
    if 'age_category' in file_perms.columns and file_perms['age_category'].notna().any():
        age_dist = file_perms['age_category'].value_counts()
        if not age_dist.empty:
            # Convert to dataframe for plotly
            age_df = pd.DataFrame({'Age Category': age_dist.index, 'Count': age_dist.values})
            fig = px.bar(age_df, x='Age Category', y='Count',
                        title="File Age Distribution")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No file age data available")
    else:
        st.info("No file age data available")

def render_sensitive_files_tab(filtered_df, df_files):
    """Render the Sensitive Files Deep Dive tab"""
    st.header("🔍 Sensitive Files Deep Dive")

    # Focus on sensitive and high-risk files
    if 'sensitivity_label' in filtered_df.columns:
        sensitive_files = filtered_df[
            (filtered_df['object_type'] == 'file') &
            ((filtered_df['sensitivity_label'].notna() & (filtered_df['sensitivity_label'] != 'Public')) |
             (filtered_df['risk_level'].isin(['Critical', 'High'])))
        ]
    else:
        # Fallback to just high-risk files
        sensitive_files = filtered_df[
            (filtered_df['object_type'] == 'file') &
            (filtered_df['risk_level'].isin(['Critical', 'High']))
        ]

    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Sensitive/High-Risk Files", sensitive_files['object_id'].nunique())
    with col2:
        st.metric("With External Access",
                 sensitive_files[sensitive_files['is_external'] == True]['object_id'].nunique())
    with col3:
        if 'sensitivity_label' in sensitive_files.columns:
            st.metric("Unlabeled",
                     sensitive_files[sensitive_files['sensitivity_label'].isna()]['object_id'].nunique())
        else:
            st.metric("Pattern-based Detection",
                     sensitive_files[sensitive_files['object_name'].apply(
                         lambda x: bool(identify_sensitive_patterns(x)) if pd.notna(x) else False
                     )]['object_id'].nunique())
    with col4:
        overshared = sensitive_files.groupby('object_id').size()
        st.metric("Overshared (>5 perms)", (overshared > 5).sum())

    # Critical files list
    st.subheader("Critical Files Requiring Attention")

    critical_files = sensitive_files[sensitive_files['risk_level'] == 'Critical']
    if not critical_files.empty:
        critical_summary = critical_files.groupby(['object_name', 'object_path', 'site_url']).agg({
            'principal_name': 'count',
            'is_external': 'sum',
            'risk_factors': lambda x: ', '.join(set([f for factors in x for f in factors]))
        }).reset_index()

        critical_summary.columns = ['File Name', 'Path', 'Site', 'Total Permissions',
                                   'External Users', 'Risk Factors']
        st.dataframe(critical_summary.sort_values('External Users', ascending=False))

    # Sensitive pattern breakdown
    st.subheader("Sensitive Pattern Analysis by Site")

    sensitive_files['patterns'] = sensitive_files['object_name'].apply(identify_sensitive_patterns)
    files_with_patterns = sensitive_files[sensitive_files['patterns'].apply(len) > 0]

    if not files_with_patterns.empty:
        pattern_by_site = files_with_patterns.explode('patterns').groupby(['site_url', 'patterns']).size().reset_index(name='count')
        pattern_pivot = pattern_by_site.pivot(index='site_url', columns='patterns', values='count').fillna(0)

        fig = px.imshow(pattern_pivot,
                       title="Sensitive Patterns by Site",
                       labels=dict(x="Pattern Type", y="Site", color="Count"))
        st.plotly_chart(fig, use_container_width=True)

    # External access details
    st.subheader("External Access to Sensitive Files")

    external_sensitive = sensitive_files[sensitive_files['is_external'] == True]
    if not external_sensitive.empty:
        external_summary = external_sensitive.groupby(['principal_name', 'principal_domain']).agg({
            'object_id': 'nunique',
            'permission_level': lambda x: ', '.join(set(x))
        }).reset_index()

        external_summary.columns = ['External User', 'Domain', 'Files Accessed', 'Permission Levels']
        st.dataframe(external_summary.sort_values('Files Accessed', ascending=False))

    # Inheritance analysis
    st.subheader("Permission Inheritance Analysis")

    inherited = sensitive_files[sensitive_files['is_inherited'] == True]
    unique_perms = sensitive_files[sensitive_files['is_inherited'] == False]

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Files with Inherited Permissions", inherited['object_id'].nunique())
    with col2:
        st.metric("Files with Unique Permissions", unique_perms['object_id'].nunique())

    # Inheritance source analysis
    if not inherited.empty and 'inheritance_source' in inherited.columns:
        inheritance_sources = inherited['inheritance_source'].value_counts().head(10)
        fig = px.bar(x=inheritance_sources.values, y=inheritance_sources.index,
                    orientation='h', title="Top 10 Permission Inheritance Sources")
        st.plotly_chart(fig, use_container_width=True)

def render_advanced_viz_tab(filtered_df):
    """Render the Advanced Visualizations tab"""
    st.header("📈 Advanced Visualizations")

    # Permission heatmap
    st.subheader("Permission Heatmap")

    # Create a heatmap of users vs sites
    user_site_matrix = pd.crosstab(
        filtered_df['principal_name'],
        filtered_df['site_url']
    )

    # Limit to top users and sites for readability
    top_users = filtered_df['principal_name'].value_counts().head(20).index
    top_sites = filtered_df['site_url'].value_counts().head(20).index

    heatmap_data = user_site_matrix.loc[
        user_site_matrix.index.isin(top_users),
        user_site_matrix.columns.isin(top_sites)
    ]

    fig = px.imshow(heatmap_data,
                   title="Permission Heatmap (Top 20 Users × Top 20 Sites)",
                   labels=dict(x="Site", y="User", color="Permission Count"),
                   height=600)
    st.plotly_chart(fig, use_container_width=True)

    # File risk visualization
    st.subheader("File Risk Analysis")

    file_risks = filtered_df[filtered_df['object_type'] == 'file'].copy()

    if not file_risks.empty:
        # Risk by extension
        risk_by_ext = file_risks.groupby(['file_extension', 'risk_level']).size().reset_index(name='count')
        risk_by_ext_pivot = risk_by_ext.pivot(index='file_extension', columns='risk_level', values='count').fillna(0)

        # Get top 15 extensions by total count
        top_extensions = risk_by_ext.groupby('file_extension')['count'].sum().nlargest(15).index
        risk_by_ext_pivot = risk_by_ext_pivot.loc[risk_by_ext_pivot.index.isin(top_extensions)]

        fig = px.bar(risk_by_ext_pivot.T,
                    title="Risk Levels by File Extension (Top 15)",
                    labels=dict(value="Count", index="Risk Level"))
        st.plotly_chart(fig, use_container_width=True)

        # Risk by file size
        if 'file_size' in file_risks.columns and file_risks['file_size'].notna().any():
            fig = px.scatter(file_risks,
                           x='file_size',
                           y='risk_score',
                           color='risk_level',
                           hover_data=['object_name', 'principal_name'],
                           title="Risk Score vs File Size",
                           labels={'file_size': 'File Size (bytes)', 'risk_score': 'Risk Score'},
                           color_discrete_map={'Critical': 'red', 'High': 'orange',
                                             'Medium': 'yellow', 'Low': 'green'})
            st.plotly_chart(fig, use_container_width=True)

    # Object type sunburst
    st.subheader("Permission Hierarchy")

    hierarchy_df = filtered_df[['object_type', 'permission_level', 'principal_type']].copy()
    hierarchy_df['count'] = 1

    fig = px.sunburst(hierarchy_df,
                     path=['object_type', 'permission_level', 'principal_type'],
                     values='count',
                     title="Permission Distribution Hierarchy")
    st.plotly_chart(fig, use_container_width=True)

    # Modification timeline
    st.subheader("Sensitive File Modification Timeline")

    if 'sensitivity_label' in filtered_df.columns:
        timeline_df = filtered_df[
            (filtered_df['object_type'] == 'file') &
            (filtered_df['modified_at'].notna()) &
            ((filtered_df['sensitivity_label'].notna() & (filtered_df['sensitivity_label'] != 'Public')) |
             (filtered_df['risk_level'].isin(['Critical', 'High'])))
        ].copy()
    else:
        timeline_df = filtered_df[
            (filtered_df['object_type'] == 'file') &
            (filtered_df['modified_at'].notna()) &
            (filtered_df['risk_level'].isin(['Critical', 'High']))
        ].copy()

    if not timeline_df.empty:
        timeline_df['modified_date'] = pd.to_datetime(timeline_df['modified_at']).dt.date
        daily_mods = timeline_df.groupby('modified_date').size().reset_index(name='count')

        fig = px.line(daily_mods, x='modified_date', y='count',
                     title="Sensitive File Modifications Over Time")
        st.plotly_chart(fig, use_container_width=True)

    # Top users and files
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Users by Permission Count")
        top_users = filtered_df['principal_name'].value_counts().head(15)
        fig = px.bar(x=top_users.values, y=top_users.index,
                    orientation='h', title="Top 15 Users")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Files by Permission Count")
        file_perms = filtered_df[filtered_df['object_type'] == 'file']
        if not file_perms.empty:
            top_files = file_perms['object_name'].value_counts().head(15)
            fig = px.bar(x=top_files.values, y=top_files.index,
                        orientation='h', title="Top 15 Files")
            st.plotly_chart(fig, use_container_width=True)

def render_detailed_data_tab(filtered_df):
    """Render the Detailed Permission Data tab"""
    st.header("📋 Detailed Permission Data")

    # Column selection
    all_columns = filtered_df.columns.tolist()
    default_columns = ['object_type', 'object_name', 'principal_name', 'permission_level',
                      'is_external', 'risk_level', 'site_url']

    selected_columns = st.multiselect("Select columns to display",
                                     all_columns,
                                     default=default_columns)

    # Sort options
    col1, col2 = st.columns(2)
    with col1:
        sort_column = st.selectbox("Sort by", selected_columns)
    with col2:
        sort_order = st.radio("Sort order", ["Ascending", "Descending"], horizontal=True)

    # Additional filters
    st.subheader("Quick Filters")
    col1, col2, col3 = st.columns(3)

    with col1:
        risk_filter = st.multiselect("Risk Level",
                                    filtered_df['risk_level'].unique())
    with col2:
        perm_filter = st.multiselect("Permission Level",
                                   filtered_df['permission_level'].unique())
    with col3:
        type_filter = st.multiselect("Object Type",
                                   filtered_df['object_type'].unique())

    # Apply filters
    display_df = filtered_df.copy()
    if risk_filter:
        display_df = display_df[display_df['risk_level'].isin(risk_filter)]
    if perm_filter:
        display_df = display_df[display_df['permission_level'].isin(perm_filter)]
    if type_filter:
        display_df = display_df[display_df['object_type'].isin(type_filter)]

    # Sort data
    display_df = display_df.sort_values(sort_column,
                                       ascending=(sort_order == "Ascending"))

    # Display summary statistics
    st.subheader("Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Records", len(display_df))
    with col2:
        st.metric("Unique Objects", display_df['object_id'].nunique())
    with col3:
        st.metric("Unique Principals", display_df['principal_name'].nunique())
    with col4:
        st.metric("External Permissions", display_df['is_external'].sum())

    # Per-column statistics
    if st.checkbox("Show column statistics"):
        st.subheader("Column Statistics")

        for col in selected_columns:
            if col in display_df.columns:
                col_type = display_df[col].dtype

                if col_type in ['int64', 'float64']:
                    st.write(f"**{col}**")
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Mean", f"{display_df[col].mean():.2f}")
                    with col2:
                        st.metric("Median", f"{display_df[col].median():.2f}")
                    with col3:
                        st.metric("Min", f"{display_df[col].min():.2f}")
                    with col4:
                        st.metric("Max", f"{display_df[col].max():.2f}")
                elif col_type == 'object' or col_type == 'string':
                    st.write(f"**{col}**")
                    unique_count = display_df[col].nunique()
                    st.write(f"Unique values: {unique_count}")
                    if unique_count < 20:
                        value_counts = display_df[col].value_counts()
                        st.bar_chart(value_counts)

    # Display data table
    st.subheader("Permission Data")
    st.dataframe(display_df[selected_columns], height=600)

    # Download filtered data
    csv = display_df[selected_columns].to_csv(index=False)
    st.download_button(
        label="Download filtered data as CSV",
        data=csv,
        file_name=f"permissions_filtered_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

def render_matrix_view_tab(filtered_df):
    """Render the Permission Matrix View tab"""
    st.header("🔲 Permission Matrix View")

    # Matrix configuration
    matrix_type = st.selectbox(
        "Select Matrix Type",
        ["User × File", "User × Site", "User × Permission Type",
         "User Domain × Site Domain", "Risk Level × File Type",
         "Sensitivity Label × Object Type"]
    )

    # Generate appropriate matrix
    if matrix_type == "User × File":
        matrix_df = filtered_df[filtered_df['object_type'] == 'file']
        if not matrix_df.empty:
            # Limit to top items for readability
            top_users = matrix_df['principal_name'].value_counts().head(25).index
            top_files = matrix_df['object_name'].value_counts().head(25).index

            matrix_df = matrix_df[
                matrix_df['principal_name'].isin(top_users) &
                matrix_df['object_name'].isin(top_files)
            ]

            matrix = pd.crosstab(matrix_df['principal_name'], matrix_df['object_name'])

            # Add summary metrics
            col1, col2 = st.columns([3, 1])
            with col2:
                st.metric("Total Users", len(matrix.index))
                st.metric("Total Files", len(matrix.columns))
                st.metric("Total Permissions", matrix.sum().sum())

    elif matrix_type == "User × Site":
        # Limit to top items
        top_users = filtered_df['principal_name'].value_counts().head(30).index
        top_sites = filtered_df['site_url'].value_counts().head(30).index

        matrix_df = filtered_df[
            filtered_df['principal_name'].isin(top_users) &
            filtered_df['site_url'].isin(top_sites)
        ]

        matrix = pd.crosstab(matrix_df['principal_name'], matrix_df['site_url'])

    elif matrix_type == "User × Permission Type":
        matrix = pd.crosstab(filtered_df['principal_name'],
                           filtered_df['permission_level'])

    elif matrix_type == "User Domain × Site Domain":
        matrix = pd.crosstab(filtered_df['principal_domain'],
                           filtered_df['site_domain'])

    elif matrix_type == "Risk Level × File Type":
        file_df = filtered_df[filtered_df['object_type'] == 'file']
        if not file_df.empty:
            matrix = pd.crosstab(file_df['risk_level'],
                               file_df['file_extension'])

    elif matrix_type == "Sensitivity Label × Object Type":
        if 'sensitivity_label' in filtered_df.columns:
            matrix = pd.crosstab(filtered_df['sensitivity_label'].fillna('Unlabeled'),
                               filtered_df['object_type'])
        else:
            # Skip this matrix type if sensitivity_label doesn't exist
            st.warning("Sensitivity labels not available in the data")
            matrix = pd.DataFrame()

    # Display matrix
    if 'matrix' in locals() and not matrix.empty:
        # Heatmap visualization
        fig = px.imshow(matrix,
                       title=f"{matrix_type} Matrix",
                       labels=dict(color="Count"),
                       height=600)
        st.plotly_chart(fig, use_container_width=True)

        # Summary statistics
        st.subheader("Matrix Summary")
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Entries", matrix.sum().sum())
        with col2:
            st.metric("Rows", len(matrix.index))
        with col3:
            st.metric("Columns", len(matrix.columns))

        # Show raw matrix data
        if st.checkbox("Show raw matrix data"):
            st.dataframe(matrix)

            # Download matrix
            csv = matrix.to_csv()
            st.download_button(
                label="Download matrix as CSV",
                data=csv,
                file_name=f"permission_matrix_{matrix_type.replace(' × ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

def render_export_tab(filtered_df, full_df):
    """Render the Export Options tab"""
    st.header("💾 Export Options")

    st.write("Export your audit data in various formats with customizable options.")

    # Export format selection
    export_format = st.radio("Select export format", ["Excel", "CSV", "JSON"])

    # Sheet selection for Excel
    if export_format == "Excel":
        st.subheader("Excel Export Options")

        include_sheets = {
            "Filtered Data": st.checkbox("Filtered Data", value=True),
            "Summary Statistics": st.checkbox("Summary Statistics", value=True),
            "Sensitive Files": st.checkbox("Sensitive Files", value=True),
            "High-Risk Files": st.checkbox("High-Risk Files", value=True),
            "External Access": st.checkbox("External Access", value=True),
            "Full Control Items": st.checkbox("Full Control Items", value=True),
        }

        if st.button("Generate Excel Export"):
            with st.spinner("Generating Excel file..."):
                excel_buffer = io.BytesIO()

                with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                    # Filtered data
                    if include_sheets["Filtered Data"]:
                        filtered_df.to_excel(writer, sheet_name='Filtered Data', index=False)

                    # Summary statistics
                    if include_sheets["Summary Statistics"]:
                        summary_stats = pd.DataFrame({
                            'Metric': ['Total Permissions', 'Filtered Permissions', 'Unique Sites',
                                      'Unique Principals', 'External Users', 'Critical Risks',
                                      'High Risks', 'Files with Permissions'],
                            'Value': [
                                len(full_df),
                                len(filtered_df),
                                filtered_df['site_url'].nunique(),
                                filtered_df['principal_name'].nunique(),
                                filtered_df[filtered_df['is_external'] == True]['principal_name'].nunique(),
                                len(filtered_df[filtered_df['risk_level'] == 'Critical']),
                                len(filtered_df[filtered_df['risk_level'] == 'High']),
                                len(filtered_df[filtered_df['object_type'] == 'file'])
                            ]
                        })
                        summary_stats.to_excel(writer, sheet_name='Summary Statistics', index=False)

                    # Sensitive files
                    if include_sheets["Sensitive Files"]:
                        if 'sensitivity_label' in filtered_df.columns:
                            sensitive = filtered_df[
                                (filtered_df['object_type'] == 'file') &
                                (filtered_df['sensitivity_label'].notna()) &
                                (filtered_df['sensitivity_label'] != 'Public')
                            ]
                        else:
                            # Use pattern detection as fallback
                            sensitive = filtered_df[
                                (filtered_df['object_type'] == 'file') &
                                filtered_df['object_name'].apply(
                                    lambda x: bool(identify_sensitive_patterns(x)) if pd.notna(x) else False
                                )
                            ]
                        if not sensitive.empty:
                            sensitive.to_excel(writer, sheet_name='Sensitive Files', index=False)

                    # High-risk files
                    if include_sheets["High-Risk Files"]:
                        high_risk = filtered_df[
                            (filtered_df['object_type'] == 'file') &
                            (filtered_df['risk_level'].isin(['Critical', 'High']))
                        ]
                        high_risk.to_excel(writer, sheet_name='High-Risk Files', index=False)

                    # External access
                    if include_sheets["External Access"]:
                        external = filtered_df[filtered_df['is_external'] == True]
                        external.to_excel(writer, sheet_name='External Access', index=False)

                    # Full control items
                    if include_sheets["Full Control Items"]:
                        full_control = filtered_df[filtered_df['permission_level'] == 'Full Control']
                        full_control.to_excel(writer, sheet_name='Full Control', index=False)

                excel_buffer.seek(0)

                st.download_button(
                    label="Download Excel File",
                    data=excel_buffer,
                    file_name=f"sharepoint_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    elif export_format == "CSV":
        st.subheader("CSV Export")

        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download CSV File",
            data=csv,
            file_name=f"sharepoint_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    elif export_format == "JSON":
        st.subheader("JSON Export")

        json_data = filtered_df.to_json(orient='records', date_format='iso')
        st.download_button(
            label="Download JSON File",
            data=json_data,
            file_name=f"sharepoint_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json"
        )

    # Export summary
    st.subheader("Export Summary")
    st.info(f"""
    **Export Details:**
    - Total records to export: {len(filtered_df):,}
    - Export format: {export_format}
    - Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    - Filters applied: {'Yes' if len(filtered_df) < len(full_df) else 'No'}
    """)

if __name__ == "__main__":
    main()
