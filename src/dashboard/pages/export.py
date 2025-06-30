import streamlit as st
from ..components import export as export_component


def render(db_path: str) -> None:
    """Render export page."""
    st.title("Export Data")
    if st.button("Generate Export"):
        export_component.create_download_button(b"data", "audit_data.txt")
