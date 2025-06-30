import streamlit as st

def create_download_button(data: bytes, filename: str) -> None:
    """Provide a simple download button."""
    st.download_button(
        label="Download",
        data=data,
        file_name=filename,
        mime="application/octet-stream",
    )
