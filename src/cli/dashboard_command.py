"""Dashboard command implementation - isolated to avoid import issues."""

import subprocess
import sys
from pathlib import Path

import click

from cli.output import RichOutput


@click.command()
@click.option('--db-path', required=True, help='Path to the audit database file.')
@click.option('--port', default=9999, help='Port to run the dashboard on.')
@click.option('--no-browser', is_flag=True, help='Do not open browser automatically.')
@click.pass_context
def dashboard(ctx, db_path, port, no_browser):
    """Launch the Streamlit dashboard for viewing audit results.

    This command starts a web-based dashboard that allows you to explore
    audit results, view analytics, and generate reports.
    """
    output = RichOutput()
    output.show_banner("SharePoint Audit Dashboard")

    # Check if database exists
    if not Path(db_path).exists():
        output.error(f"Database file not found: {db_path}")
        ctx.exit(1)

    # Check if streamlit app exists
    streamlit_app_path = Path(__file__).parent.parent / "dashboard" / "streamlit_app.py"
    if not streamlit_app_path.exists():
        output.error(f"Dashboard app not found: {streamlit_app_path}")
        ctx.exit(1)

    # Build streamlit command
    command = [
        sys.executable, "-m", "streamlit", "run",
        str(streamlit_app_path),
        "--server.port", str(port),
        "--", "--db-path", db_path
    ]

    if no_browser:
        command.insert(-2, "--server.headless")
        command.insert(-2, "true")

    output.info(f"Launching dashboard on port {port}...")
    output.info(f"Database: {db_path}")

    try:
        # Run streamlit
        subprocess.run(command)
    except KeyboardInterrupt:
        output.info("Dashboard stopped")
    except Exception as e:
        output.error(f"Failed to launch dashboard: {e}")
        ctx.exit(1)
