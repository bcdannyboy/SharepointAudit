import asyncio
import io
from datetime import datetime
import streamlit as st
import pandas as pd
from ...database.repository import DatabaseRepository


@st.cache_data(ttl=60)
def load_export_data(db_path: str, export_type: str) -> pd.DataFrame:
    """Load data for export based on type."""

    async def _load():
        repo = DatabaseRepository(db_path)

        if export_type == "Sites":
            query = """
            SELECT
                s.site_id,
                s.title,
                s.url,
                s.created_at,
                s.last_modified,
                s.storage_used,
                s.storage_quota,
                s.is_hub_site,
                COUNT(DISTINCT l.id) as library_count,
                COUNT(DISTINCT f.id) as file_count
            FROM sites s
            LEFT JOIN libraries l ON s.id = l.site_id
            LEFT JOIN files f ON l.id = f.library_id
            GROUP BY s.id
            """

        elif export_type == "Libraries":
            query = """
            SELECT
                l.library_id,
                l.name,
                l.description,
                l.created_at,
                l.item_count,
                l.is_hidden,
                l.enable_versioning,
                s.title as site_title,
                s.url as site_url
            FROM libraries l
            JOIN sites s ON l.site_id = s.id
            """

        elif export_type == "Files":
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
                s.title as site_title
            FROM files f
            JOIN libraries l ON f.library_id = l.id
            JOIN sites s ON l.site_id = s.id
            """

        elif export_type == "Permissions":
            query = """
            SELECT
                p.object_type,
                p.object_id,
                p.principal_type,
                p.principal_name,
                p.permission_level,
                p.is_inherited,
                p.granted_at,
                p.granted_by,
                CASE
                    WHEN p.object_type = 'site' THEN s.title
                    WHEN p.object_type = 'library' THEN l.name
                    WHEN p.object_type = 'folder' THEN fo.name
                    WHEN p.object_type = 'file' THEN fi.name
                END as object_name,
                CASE
                    WHEN p.object_type = 'site' THEN s.url
                    WHEN p.object_type = 'library' THEN (SELECT url FROM sites WHERE id = l.site_id)
                    WHEN p.object_type = 'folder' THEN fo.server_relative_url
                    WHEN p.object_type = 'file' THEN fi.server_relative_url
                END as object_path
            FROM permissions p
            LEFT JOIN sites s ON p.object_type = 'site' AND p.object_id = s.site_id
            LEFT JOIN libraries l ON p.object_type = 'library' AND p.object_id = l.library_id
            LEFT JOIN folders fo ON p.object_type = 'folder' AND p.object_id = fo.folder_id
            LEFT JOIN files fi ON p.object_type = 'file' AND p.object_id = fi.file_id
            """

        elif export_type == "External Users":
            query = """
            SELECT
                p.principal_name,
                p.principal_type,
                p.permission_level,
                p.object_type,
                CASE
                    WHEN p.object_type = 'site' THEN s.title
                    WHEN p.object_type = 'library' THEN l.name
                    WHEN p.object_type = 'folder' THEN fo.name
                    WHEN p.object_type = 'file' THEN fi.name
                END as object_name,
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
            WHERE p.principal_name LIKE '%#ext#%' OR p.principal_name LIKE '%(External)%'
            """

        elif export_type == "Summary Report":
            # Return multiple datasets for summary
            sites_count = await repo.count_rows("sites")
            files_count = await repo.count_rows("files")
            permissions_count = await repo.count_rows("permissions")

            summary_data = {
                "Metric": [
                    "Total Sites",
                    "Total Libraries",
                    "Total Files",
                    "Total Permissions",
                    "Unique Permissions",
                    "External Users",
                ],
                "Value": [
                    sites_count,
                    await repo.count_rows("libraries"),
                    files_count,
                    permissions_count,
                    await repo.count_rows("permissions", "is_inherited = 0"),
                    await repo.count_rows(
                        "permissions",
                        "principal_name LIKE '%#ext#%' OR principal_name LIKE '%(External)%'",
                    ),
                ],
            }
            return pd.DataFrame(summary_data)

        else:
            return pd.DataFrame()

        result = await repo.fetch_all(query)
        return pd.DataFrame(result)

    return asyncio.run(_load())


def create_excel_download(dataframes: dict, filename: str):
    """Create Excel file with multiple sheets and return download button."""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(
                writer, sheet_name=sheet_name[:31], index=False
            )  # Excel sheet names limited to 31 chars

            # Auto-adjust column widths
            worksheet = writer.sheets[sheet_name[:31]]
            for idx, col in enumerate(df.columns):
                max_len = max(df[col].astype(str).str.len().max(), len(col)) + 2
                worksheet.set_column(idx, idx, min(max_len, 50))

    output.seek(0)

    st.download_button(
        label="📥 Download Excel Report",
        data=output.getvalue(),
        file_name=f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def create_csv_download(df: pd.DataFrame, filename: str):
    """Create CSV download button."""
    csv = df.to_csv(index=False)

    st.download_button(
        label="📥 Download CSV",
        data=csv,
        file_name=f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
    )


def render(db_path: str) -> None:
    """Render export page."""
    st.title("📤 Export Data")

    st.markdown(
        """
    Export your SharePoint audit data in various formats. You can export specific data types
    or generate a comprehensive report with all audit findings.
    """
    )

    # Export options
    export_type = st.selectbox(
        "Select Export Type",
        [
            "Summary Report",
            "Sites",
            "Libraries",
            "Files",
            "Permissions",
            "External Users",
        ],
    )

    export_format = st.radio("Export Format", ["Excel", "CSV"], horizontal=True)

    # Preview section
    st.subheader("Data Preview")

    with st.spinner("Loading data..."):
        try:
            preview_df = load_export_data(db_path, export_type)

            if preview_df.empty:
                st.warning("No data available for the selected export type.")
                return

            # Show preview (first 100 rows)
            st.info(
                f"Showing preview of {min(100, len(preview_df))} out of {len(preview_df):,} total records"
            )
            st.dataframe(
                preview_df.head(100), use_container_width=True, hide_index=True
            )

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return

    # Export section
    st.subheader("Export Options")

    col1, col2 = st.columns(2)

    with col1:
        include_timestamp = st.checkbox("Include export timestamp", value=True)
        include_filters = st.checkbox("Include applied filters", value=True)

    with col2:
        if export_format == "Excel" and export_type == "Summary Report":
            include_all_sheets = st.checkbox(
                "Include all data types in Excel", value=True
            )

    # Generate export button
    if st.button("Generate Export", type="primary"):
        with st.spinner("Generating export..."):
            try:
                if export_format == "Excel":
                    if (
                        export_type == "Summary Report"
                        and "include_all_sheets" in locals()
                        and include_all_sheets
                    ):
                        # Load all data types for comprehensive report
                        dataframes = {}

                        # Add summary sheet
                        dataframes["Summary"] = preview_df

                        # Add metadata sheet
                        metadata_df = pd.DataFrame(
                            {
                                "Property": [
                                    "Export Date",
                                    "Database Path",
                                    "Export Type",
                                    "Total Records",
                                ],
                                "Value": [
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    db_path,
                                    export_type,
                                    len(preview_df),
                                ],
                            }
                        )
                        dataframes["Metadata"] = metadata_df

                        # Load other data types
                        for data_type in [
                            "Sites",
                            "Libraries",
                            "Files",
                            "Permissions",
                            "External Users",
                        ]:
                            if data_type != export_type:  # Don't reload the same data
                                df = load_export_data(db_path, data_type)
                                if not df.empty:
                                    dataframes[data_type] = df

                        create_excel_download(dataframes, "sharepoint_audit_complete")
                    else:
                        # Single sheet export
                        dataframes = {export_type: preview_df}

                        if include_timestamp or include_filters:
                            metadata_df = pd.DataFrame(
                                {
                                    "Property": [
                                        "Export Date",
                                        "Database Path",
                                        "Export Type",
                                        "Total Records",
                                    ],
                                    "Value": [
                                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                        db_path,
                                        export_type,
                                        len(preview_df),
                                    ],
                                }
                            )
                            dataframes["Metadata"] = metadata_df

                        create_excel_download(
                            dataframes,
                            f"sharepoint_audit_{export_type.lower().replace(' ', '_')}",
                        )

                else:  # CSV format
                    create_csv_download(
                        preview_df,
                        f"sharepoint_audit_{export_type.lower().replace(' ', '_')}",
                    )

                st.success("Export generated successfully!")

            except Exception as e:
                st.error(f"Error generating export: {str(e)}")

    # Additional export information
    with st.expander("Export Information"):
        st.markdown(
            """
        ### Available Export Types:

        - **Summary Report**: High-level overview of the audit findings
        - **Sites**: All SharePoint sites with storage and metadata
        - **Libraries**: Document libraries and their properties
        - **Files**: Detailed file information including size, type, and ownership
        - **Permissions**: Complete permission assignments across all objects
        - **External Users**: List of external users and their access rights

        ### Export Formats:

        - **Excel**: Multi-sheet workbook with formatted data and auto-sized columns
        - **CSV**: Single file suitable for further processing or importing into other tools

        ### Tips:

        - For comprehensive analysis, use the Excel format with "Include all data types" option
        - Large exports may take some time to generate
        - The export includes all data matching your current filters
        """
        )
