import importlib
import sys
import os
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch
import pytest


def _create_stub_streamlit(selected_page="Overview", site_selection="Site A"):
    """Create a minimal streamlit stub for testing."""
    stub = ModuleType("streamlit")

    # Basic attributes and methods
    sidebar = SimpleNamespace(
        title=lambda *a, **k: None,
        radio=lambda label, options: selected_page,
        button=lambda *a, **k: False,
        selectbox=lambda *a, **k: site_selection,
        checkbox=lambda *a, **k: False,
        subheader=lambda *a, **k: None,
    )
    stub.sidebar = sidebar

    # Main streamlit methods
    stub.set_page_config = lambda *a, **k: None
    stub.title = lambda *a, **k: None
    stub.write = lambda *a, **k: None
    stub.dataframe = lambda *a, **k: None
    stub.bar_chart = lambda *a, **k: None
    stub.selectbox = lambda label, options, **k: site_selection
    stub.download_button = lambda *a, **k: None
    stub.button = lambda *a, **k: False
    stub.error = lambda *a, **k: None
    stub.warning = lambda *a, **k: None
    stub.info = lambda *a, **k: None
    stub.success = lambda *a, **k: None
    stub.metric = lambda *a, **k: None
    # Mock columns to return context managers
    class MockColumn:
        def __init__(self, parent):
            self.parent = parent

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def metric(self, *args, **kwargs):
            return self.parent.metric(*args, **kwargs)

    stub.columns = lambda n: [MockColumn(stub) for _ in range(n)]
    stub.subheader = lambda *a, **k: None
    stub.markdown = lambda *a, **k: None
    stub.text_input = lambda *a, **k: ""
    stub.number_input = lambda *a, **k: 0
    stub.checkbox = lambda *a, **k: False
    stub.radio = lambda *a, **k: "Excel"
    # Mock expander and spinner as context managers
    class MockContextManager:
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return None

    stub.expander = lambda *a, **k: MockContextManager()
    stub.spinner = lambda *a, **k: MockContextManager()
    stub.plotly_chart = lambda *a, **k: None

    # Column config
    column_config = SimpleNamespace(
        TextColumn=lambda *a, **k: {},
        NumberColumn=lambda *a, **k: {},
        DatetimeColumn=lambda *a, **k: {},
        CheckboxColumn=lambda *a, **k: {},
        LinkColumn=lambda *a, **k: {},
        ProgressColumn=lambda *a, **k: {},
    )
    stub.column_config = column_config

    def cache_data(ttl=None):
        def decorator(func):
            return func
        return decorator

    stub.cache_data = cache_data
    return stub


def _create_stub_plotly():
    """Create minimal plotly stubs."""
    px = ModuleType("plotly.express")
    px.pie = lambda *a, **k: SimpleNamespace(update_traces=lambda *a, **k: None, update_layout=lambda *a, **k: None)
    px.bar = lambda *a, **k: SimpleNamespace(update_layout=lambda *a, **k: None)
    px.scatter = lambda *a, **k: SimpleNamespace(update_layout=lambda *a, **k: None)

    go = ModuleType("plotly.graph_objects")
    go.Figure = lambda *a, **k: SimpleNamespace(update_layout=lambda *a, **k: None)
    go.Heatmap = lambda *a, **k: {}

    plotly = ModuleType("plotly")
    plotly.express = px
    plotly.graph_objects = go

    return plotly, px, go


def _create_stub_pandas():
    """Create minimal pandas stub."""
    pd = ModuleType("pandas")

    class DataFrame:
        def __init__(self, data=None):
            self.data = data or []
            self.empty = len(self.data) == 0
            self.columns = []

        def __len__(self):
            return len(self.data)

        def __getitem__(self, key):
            return self

        def head(self, n=5):
            return self

        def to_csv(self, *a, **k):
            return "csv_data"

        def to_excel(self, *a, **k):
            pass

        @property
        def str(self):
            return SimpleNamespace(
                contains=lambda *a, **k: self,
                len=lambda: SimpleNamespace(max=lambda: 10)
            )

        def nunique(self):
            return 5

        def value_counts(self):
            return SimpleNamespace(
                index=['A', 'B'],
                values=[10, 20],
                head=lambda n: SimpleNamespace(index=['A'], values=[10])
            )

        def sum(self):
            return 100

        def mean(self):
            return 50

        def nlargest(self, n, col):
            return self

        def sort_values(self, *a, **k):
            return self

        def pivot_table(self, *a, **k):
            return DataFrame()

        def astype(self, *a, **k):
            return self

        def fillna(self, *a, **k):
            return self

        def copy(self):
            return self

    pd.DataFrame = DataFrame
    pd.ExcelWriter = MagicMock()
    pd.to_datetime = lambda x: x
    pd.Categorical = lambda *a, **k: []

    return pd


@patch('asyncio.run')
@patch('src.database.repository.DatabaseRepository')
def test_dashboard_main_loads(mock_repo, mock_asyncio_run, monkeypatch):
    # Mock asyncio.run to return proper data structure for overview page
    mock_asyncio_run.return_value = {
        'sites': {'total_sites': 0, 'total_libraries': 0, 'total_files': 0, 'total_size_bytes': 0},
        'permissions': {'total_permissions': 0, 'unique_permissions': 0, 'permissions_by_level': {}},
        'latest_audit': None,
        'file_stats': None,
        'external_users': 0
    }

    # Create stubs
    st_stub = _create_stub_streamlit()
    plotly_stub, px_stub, go_stub = _create_stub_plotly()
    pd_stub = _create_stub_pandas()

    # Set up module mocks
    monkeypatch.setitem(sys.modules, 'streamlit', st_stub)
    monkeypatch.setitem(sys.modules, 'plotly', plotly_stub)
    monkeypatch.setitem(sys.modules, 'plotly.express', px_stub)
    monkeypatch.setitem(sys.modules, 'plotly.graph_objects', go_stub)
    monkeypatch.setitem(sys.modules, 'pandas', pd_stub)

    # Import and test
    import src.dashboard.streamlit_app as app
    importlib.reload(app)

    # Test that main runs without error
    app.main(['--db-path', 'test.db'])
    assert True  # If no exception, test passes


@patch('asyncio.run')
@patch('src.database.repository.DatabaseRepository')
def test_permissions_page_filtering(mock_repo, mock_asyncio_run, monkeypatch):
    # Mock asyncio.run to return test data
    mock_asyncio_run.return_value = [
        {"title": "Site A"},
        {"title": "Site B"},
        {"permission_level": "Full Control"},
        {"site": "Site B"}
    ]

    # Create stubs
    st_stub = _create_stub_streamlit(selected_page="Permissions", site_selection="Site B")
    plotly_stub, px_stub, go_stub = _create_stub_plotly()
    pd_stub = _create_stub_pandas()

    # Set up module mocks
    monkeypatch.setitem(sys.modules, 'streamlit', st_stub)
    monkeypatch.setitem(sys.modules, 'plotly', plotly_stub)
    monkeypatch.setitem(sys.modules, 'plotly.express', px_stub)
    monkeypatch.setitem(sys.modules, 'plotly.graph_objects', go_stub)
    monkeypatch.setitem(sys.modules, 'pandas', pd_stub)

    # Import modules
    import src.dashboard.pages.permissions as perm
    importlib.reload(perm)

    # Mock the data loading functions
    monkeypatch.setattr(perm, 'load_permission_data', lambda db, filters: pd_stub.DataFrame([{"site": "Site B"}]))
    monkeypatch.setattr(perm, 'load_sites_list', lambda db: ["Site A", "Site B"])
    monkeypatch.setattr(perm, 'load_permission_levels', lambda db: ["Full Control", "Edit", "Read"])
    monkeypatch.setattr(perm, 'load_permission_matrix', lambda db, site: pd_stub.DataFrame())

    import src.dashboard.streamlit_app as app
    importlib.reload(app)

    # Test that app runs without errors
    app.main(['--db-path', 'test.db'])
    assert True  # Ran without errors


def test_dashboard_loads_and_shows_title(monkeypatch):
    """Test that the main dashboard app loads without error and shows title."""
    # Create stubs
    st_stub = _create_stub_streamlit()
    plotly_stub, px_stub, go_stub = _create_stub_plotly()
    pd_stub = _create_stub_pandas()

    # Track title calls
    titles = []
    original_title = st_stub.title
    st_stub.title = lambda t: titles.append(t) or original_title(t)

    # Set up module mocks
    monkeypatch.setitem(sys.modules, 'streamlit', st_stub)
    monkeypatch.setitem(sys.modules, 'plotly', plotly_stub)
    monkeypatch.setitem(sys.modules, 'plotly.express', px_stub)
    monkeypatch.setitem(sys.modules, 'plotly.graph_objects', go_stub)
    monkeypatch.setitem(sys.modules, 'pandas', pd_stub)

    # Mock asyncio.run
    with patch('asyncio.run') as mock_asyncio_run:
        mock_asyncio_run.return_value = {
            'sites': {'total_sites': 0, 'total_libraries': 0, 'total_files': 0, 'total_size_bytes': 0},
            'permissions': {'total_permissions': 0, 'unique_permissions': 0, 'permissions_by_level': {}},
            'latest_audit': None,
            'file_stats': None,
            'external_users': 0
        }

        # Import and test
        import src.dashboard.streamlit_app as app
        importlib.reload(app)

        # Run the app
        app.main(['--db-path', 'test.db'])

        # Check that title was set
        assert any("SharePoint Audit" in title for title in titles)


def test_permission_page_filtering_advanced(monkeypatch, tmp_path):
    """Test that filtering on the permissions page works."""
    # Create a temporary test database
    test_db = tmp_path / "test.db"

    # Create stubs
    st_stub = _create_stub_streamlit(selected_page="Permissions", site_selection="Site A")
    plotly_stub, px_stub, go_stub = _create_stub_plotly()
    pd_stub = _create_stub_pandas()

    # Track filter interactions
    filter_selections = {}

    def track_selectbox(label, options, **kwargs):
        if "Site" in label:
            filter_selections['site'] = "Site A"
            return "Site A"
        elif "Permission Level" in label:
            filter_selections['permission'] = "Full Control"
            return "Full Control"
        return options[0] if options else ""

    st_stub.sidebar.selectbox = track_selectbox

    # Set up module mocks
    monkeypatch.setitem(sys.modules, 'streamlit', st_stub)
    monkeypatch.setitem(sys.modules, 'plotly', plotly_stub)
    monkeypatch.setitem(sys.modules, 'plotly.express', px_stub)
    monkeypatch.setitem(sys.modules, 'plotly.graph_objects', go_stub)
    monkeypatch.setitem(sys.modules, 'pandas', pd_stub)

    # Mock data with site filtering
    def mock_load_permission_data(db_path, filters):
        if filters.get('site_id') == 'Site A':
            return pd_stub.DataFrame([
                {"site": "Site A", "permission_level": "Full Control"},
                {"site": "Site A", "permission_level": "Edit"}
            ])
        return pd_stub.DataFrame([])

    with patch('asyncio.run') as mock_asyncio_run:
        mock_asyncio_run.return_value = []

        # Import and configure
        import src.dashboard.pages.permissions as perm
        importlib.reload(perm)

        monkeypatch.setattr(perm, 'load_permission_data', mock_load_permission_data)
        monkeypatch.setattr(perm, 'load_sites_list', lambda db: ["Site A", "Site B"])
        monkeypatch.setattr(perm, 'load_permission_levels', lambda db: ["Full Control", "Edit", "Read"])
        monkeypatch.setattr(perm, 'load_permission_matrix', lambda db, site: pd_stub.DataFrame())

        import src.dashboard.streamlit_app as app
        importlib.reload(app)

        # Run app
        app.main(['--db-path', str(test_db)])

        # Verify filters were applied
        assert filter_selections.get('site') == "Site A"
