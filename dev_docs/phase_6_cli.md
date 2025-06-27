# Phase 6: CLI Interface

## Overview

Implement a user-friendly and powerful command-line interface (CLI) using the `click` library. This will be the primary way users interact with the audit utility, providing commands to run audits, launch the dashboard, manage backups, and check system health.

## Architectural Alignment

The CLI is the main entry point for the user and is a critical component of the application's design. This phase is guided by:

- **[CLI Interface Design](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#cli-interface-design)**: This section in `ARCHITECTURE.md` provides the complete specification for the CLI, including the command structure, options, and arguments.
- **[Component Architecture: CLI Tool Interface](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#system-components)**: Outlines how the CLI interacts with the core audit engine and other components.
- **[Configuration Management](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#configuration-management)**: The CLI must implement the logic for parsing the JSON configuration file and correctly merging it with command-line arguments, with CLI arguments taking precedence.

## Prerequisites

- [Phase 2: Database Layer & Models](./phase_2_database.md)
- [Phase 5: Permission Analysis](./phase_5_permissions.md) (for the full `audit` command)

## Deliverables

1.  **CLI Commands**: A main entry point (`sharepoint-audit`) and subcommands defined in `src/cli/main.py` and `src/cli/commands.py`.
2.  **Configuration Parser**: Logic in `src/cli/config_parser.py` to load, validate, and merge configurations from a file and CLI options.
3.  **CLI Output**: Rich terminal output using the `rich` library for progress bars, tables, and formatted text, implemented in `src/cli/output.py`.

## Detailed Implementation Guide

### 1. Set Up the Main CLI Entry Point (`src/cli/main.py`)

Use `click` to create the main command group. This file will serve as the central hub for all CLI commands.

```python
# src/cli/main.py
import click
# from .commands import audit, dashboard, backup, restore, health

@click.group()
@click.version_option()
def cli():
    """SharePoint Audit Utility CLI."""
    pass

# Add commands to the main group
# cli.add_command(audit)
# cli.add_command(dashboard)
# ... and so on
```

### 2. Implement CLI Commands (`src/cli/commands.py`)

Create the logic for each command. The `audit` command will be the most complex, as it needs to initialize and run the `AuditPipeline`.

```python
# src/cli/commands.py
import click
# from src.core.pipeline import AuditPipeline, PipelineContext
# from .config_parser import load_and_merge_config
# from .output import RichProgress

@click.command()
@click.option('--config', default='config.json', help='Path to configuration file.')
@click.option('--sites', help='Comma-separated list of specific site URLs to audit.')
@click.option('--dry-run', is_flag=True, help='Show what would be done without making API calls.')
@click.option('-v', '--verbose', count=True, help='Increase verbosity level.')
def audit(config, sites, dry_run, verbose):
    """Run a comprehensive SharePoint audit."""
    click.echo("Starting SharePoint audit...")

    # 1. Load configuration
    # final_config = load_and_merge_config(config_path=config, cli_args={'sites': sites})

    # 2. Initialize components (pipeline, context, etc.)
    # context = PipelineContext(run_id='some_run_id', config=final_config)
    # pipeline = AuditPipeline(context)
    # ... add stages ...

    # 3. Set up progress display
    # progress = RichProgress()
    # context.progress_tracker = progress

    # 4. Run the pipeline
    # if not dry_run:
    #     asyncio.run(pipeline.run())

    click.echo("Audit complete.")

@click.command()
@click.option('--db-path', required=True, help='Path to the audit database file.')
def dashboard(db_path):
    """Launch the Streamlit dashboard."""
    import subprocess
    import sys

    streamlit_script_path = "src/dashboard/streamlit_app.py"
    command = [
        sys.executable, "-m", "streamlit", "run", streamlit_script_path,
        "--", "--db-path", db_path
    ]

    click.echo(f"Launching dashboard for {db_path}...")
    subprocess.run(command)

```

### 3. Implement Configuration Parsing (`src/cli/config_parser.py`)

Create a function that can load the base configuration from a JSON file and intelligently override its values with any options provided via the command line.

## Implementation Task Checklist

- [ ] Set up the main Click group and entry point in `setup.py`.
- [ ] Implement the `audit` command, which orchestrates the `AuditPipeline`.
- [ ] Implement the `dashboard` command, which launches the Streamlit app as a subprocess.
- [ ] Implement `backup` and `restore` commands.
- [ ] Implement the configuration parser that merges file and CLI options.
- [ ] Integrate the `ProgressTracker` with `rich` to display live progress in the terminal.
- [ ] Implement a `--dry-run` option for the audit command.
- [ ] Add verbosity levels (`-v`, `-vv`) to control log output.

## Test Plan & Cases

Testing the CLI involves using `click.testing.CliRunner` to invoke commands programmatically and assert their output and behavior.

```python
# tests/test_cli.py
from click.testing import CliRunner
# from src.cli.main import cli
from unittest.mock import patch

def test_audit_command_with_config():
    """Test the audit command with a config file."""
    runner = CliRunner()
    with runner.isolated_filesystem():
        with open("config.json", "w") as f:
            f.write('{"auth": {"tenant_id": "test"}}')

        # Mock the pipeline itself to avoid running a full audit
        with patch('src.cli.commands.AuditPipeline') as MockPipeline:
            result = runner.invoke(cli, ['audit', '--config', 'config.json'])
            assert result.exit_code == 0
            assert "Starting SharePoint audit..." in result.output
            MockPipeline.assert_called_once()

def test_dashboard_command():
    """Test that the dashboard command tries to launch streamlit."""
    runner = CliRunner()
    with patch('subprocess.run') as mock_run:
        result = runner.invoke(cli, ['dashboard', '--db-path', 'test.db'])
        assert result.exit_code == 0
        # Check that streamlit was called with the correct arguments
        assert 'streamlit' in mock_run.call_args[0][0]
        assert '--db-path' in mock_run.call_args[0][0]
        assert 'test.db' in mock_run.call_args[0][0]
```

## Verification & Validation

After installing the package in editable mode, the commands should be directly available in the terminal.

```bash
# 1. Install the package and verify the command is available
pip install -e .
sharepoint-audit --help

# 2. Run a dry-run audit to test configuration parsing
sharepoint-audit audit --config config/config.json --dry-run

# 3. Run a real audit on a small site and observe the progress bar
sharepoint-audit audit --config config/config.json --sites https://tenant.sharepoint.com/sites/small_test

# 4. Launch the dashboard
sharepoint-audit dashboard --db-path audit.db
```

## Done Criteria

- [ ] The `sharepoint-audit` command is available in the shell after installation.
- [ ] The `audit` command successfully initiates and runs the audit pipeline.
- [ ] The `dashboard` command successfully launches the Streamlit application.
- [ ] Command-line options correctly override settings from the configuration file.
- [ ] The progress display is visible and updates during an audit.
- [ ] The `--help` text is comprehensive for all commands.
