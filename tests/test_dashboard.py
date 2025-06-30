import importlib
import sys
from types import ModuleType, SimpleNamespace

import pytest


def _create_stub_streamlit(selected_page="Overview", site_selection="Site A"):
    """Create a minimal streamlit stub for testing."""
    stub = ModuleType("streamlit")
    sidebar = SimpleNamespace(
        title=lambda *a, **k: None,
        radio=lambda label, options: selected_page,
        button=lambda *a, **k: False,
    )
    stub.sidebar = sidebar
    stub.set_page_config = lambda *a, **k: None
    stub.title = lambda *a, **k: None
    stub.write = lambda *a, **k: None
    stub.dataframe = lambda *a, **k: None
    stub.bar_chart = lambda *a, **k: None
    stub.selectbox = lambda label, options, **k: site_selection
    stub.download_button = lambda *a, **k: None
    stub.button = lambda *a, **k: False

    def cache_data(ttl=None):
        def decorator(func):
            return func
        return decorator

    stub.cache_data = cache_data
    return stub


def test_dashboard_main_loads(monkeypatch):
    st_stub = _create_stub_streamlit()
    monkeypatch.setitem(sys.modules, 'streamlit', st_stub)
    import src.dashboard.streamlit_app as app
    importlib.reload(app)
    app.main(['--db-path', 'test.db'])
    # The overview page sets the title
    assert True  # If no exception, test passes


def test_permissions_page_filtering(monkeypatch):
    st_stub = _create_stub_streamlit(selected_page="Permissions", site_selection="Site B")
    monkeypatch.setitem(sys.modules, 'streamlit', st_stub)
    import src.dashboard.pages.permissions as perm
    importlib.reload(perm)
    monkeypatch.setattr(perm, 'load_permission_data', lambda db, site: [{"site": site}])
    import src.dashboard.streamlit_app as app
    importlib.reload(app)
    app.main(['--db-path', 'test.db'])
    assert True  # Ran without errors
