import streamlit as st


def _sample_data(site: str) -> list[dict]:
    return [
        {"object_name": "FileA.docx", "principal_name": "User1", "permission_level": "Edit", "site": site},
        {"object_name": "FolderB", "principal_name": "GroupA", "permission_level": "Read", "site": site},
    ]


@st.cache_data(ttl=300)
def load_permission_data(db_path: str, site_filter: str) -> list[dict]:
    # In the real app this would query the database
    return _sample_data(site_filter)


def render(db_path: str) -> None:
    """Render permissions analysis page."""
    st.title("Permission Analysis")
    site_filter = st.selectbox("Filter by Site", ["Site A", "Site B"], key="site_filter")
    data = load_permission_data(db_path, site_filter)
    st.dataframe(data)
    st.bar_chart([1, 2])
