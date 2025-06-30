from types import ModuleType
import sys
from click.testing import CliRunner
from unittest.mock import patch


def _ensure_rich_modules():
    """Create minimal rich stubs if rich is not installed."""
    if 'rich' in sys.modules:
        return
    rich = ModuleType('rich')
    console_mod = ModuleType('rich.console')
    class DummyConsole:
        def print(self, *args, **kwargs):
            pass
        class _Status:
            def __init__(self, msg):
                self.msg = msg
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc, tb):
                pass
        def status(self, message, *a, **k):
            return DummyConsole._Status(message)
    console_mod.Console = DummyConsole
    progress_mod = ModuleType('rich.progress')
    class DummyProgress:
        def __init__(self, *a, **k):
            pass
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            pass
        def add_task(self, *a, **k):
            return 0
        def update(self, *a, **k):
            pass
        def stop(self):
            pass
    progress_mod.Progress = DummyProgress
    progress_mod.SpinnerColumn = progress_mod.TextColumn = progress_mod.BarColumn = progress_mod.TaskProgressColumn = progress_mod.TimeRemainingColumn = object
    table_mod = ModuleType('rich.table')
    class DummyTable:
        def __init__(self, *a, **k):
            pass
        def add_column(self, *a, **k):
            pass
        def add_row(self, *a, **k):
            pass
    table_mod.Table = DummyTable
    logging_mod = ModuleType('rich.logging')
    class DummyHandler:
        def __init__(self, *a, **k):
            pass
        def emit(self, *a, **k):
            pass
    logging_mod.RichHandler = DummyHandler
    panel_mod = ModuleType('rich.panel')
    class DummyPanel:
        def __init__(self, *a, **k):
            pass
    panel_mod.Panel = DummyPanel
    text_mod = ModuleType('rich.text')
    class DummyText:
        def __init__(self, *a, **k):
            pass
        def append(self, *a, **k):
            pass
    text_mod.Text = DummyText
    for mod in [rich, console_mod, progress_mod, table_mod, logging_mod, panel_mod, text_mod]:
        sys.modules[mod.__name__] = mod


def test_audit_command_with_config(tmp_path):
    _ensure_rich_modules()
    from src.cli.main import cli

    runner = CliRunner()
    with runner.isolated_filesystem():
        config_path = tmp_path / "config.json"
        config_path.write_text('{"auth": {"tenant_id": "tid", "client_id": "cid", "certificate_path": "cert.pem"}}')
        dummy_config = {
            'auth': {
                'tenant_id': 'tid',
                'client_id': 'cid',
                'certificate_path': 'cert.pem'
            },
            'db': {'path': 'audit.db'}
        }
        with patch('src.cli.commands.setup_logging'), \
             patch('src.cli.commands.load_and_merge_config', return_value=dummy_config), \
             patch('src.cli.commands._run_audit', return_value=None):
            result = runner.invoke(cli, ['audit', '--config', str(config_path)])
            assert result.exit_code == 0


def test_dashboard_command():
    _ensure_rich_modules()
    from src.cli.main import cli

    runner = CliRunner()
    with patch('subprocess.run') as mock_run, \
         patch('pathlib.Path.exists', return_value=True):
        result = runner.invoke(cli, ['dashboard', '--db-path', 'test.db'])
        assert result.exit_code == 0
        assert 'streamlit' in mock_run.call_args[0][0]
        assert '--db-path' in mock_run.call_args[0][0]
        assert 'test.db' in mock_run.call_args[0][0]
