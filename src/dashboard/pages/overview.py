import streamlit as st


def render(db_path: str) -> None:
    """Render the dashboard overview page."""
    st.title("SharePoint Audit Dashboard")
    st.write(f"Database: {db_path}")
