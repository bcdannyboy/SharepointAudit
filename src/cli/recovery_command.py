"""Command to check crash recovery status and resume interrupted audits."""

import asyncio
import logging
from typing import Optional
from datetime import datetime, timezone

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from database.repository import DatabaseRepository
from utils.live_checkpoint_manager import LiveCheckpointManager
from utils.logger import LoggingConfiguration
from core.discovery_enhanced import EnhancedDiscoveryModule

logger = logging.getLogger(__name__)
console = Console()


@click.command()
@click.option(
    "--db-path",
    default="audit.db",
    help="Path to the audit database",
)
@click.option(
    "--run-id",
    help="Specific run ID to check (defaults to most recent)",
)
def recovery_status(db_path: str, run_id: Optional[str]) -> None:
    """Check crash recovery status and show resumable audits."""
    LoggingConfiguration.setup_logging()

    asyncio.run(_recovery_status(db_path, run_id))


async def _recovery_status(db_path: str, run_id: Optional[str]) -> None:
    """Async implementation of recovery status check."""
    try:
        # Initialize database
        db_repo = DatabaseRepository(db_path)
        await db_repo.initialize()

        # Get audit runs
        if run_id:
            runs = [await db_repo.get_audit_run(run_id)]
            if not runs[0]:
                console.print(f"[red]Run ID {run_id} not found[/red]")
                return
        else:
            # Get recent runs
            runs = await db_repo.get_recent_audit_runs(limit=10)
            if not runs:
                console.print("[yellow]No audit runs found[/yellow]")
                return

        # Display runs table
        table = Table(title="Audit Runs")
        table.add_column("Run ID", style="cyan")
        table.add_column("Started", style="green")
        table.add_column("Status", style="yellow")
        table.add_column("Sites", style="blue")
        table.add_column("Files", style="blue")
        table.add_column("Errors", style="red")

        for run in runs:
            # Get checkpoint status
            checkpoint_manager = LiveCheckpointManager(db_repo)

            # Check if discovery completed
            discovery_complete = await checkpoint_manager.restore_checkpoint(
                run['run_id'],
                'discovery_complete'
            )

            # Get last pipeline stage
            last_stage = None
            for stage in ['discovery', 'validation', 'transformation',
                         'enrichment', 'permission_analysis', 'storage']:
                stage_status = await checkpoint_manager.restore_checkpoint(
                    run['run_id'],
                    f'pipeline_stage_{stage}'
                )
                if stage_status and stage_status.get('status') == 'completed':
                    last_stage = stage

            status = "Completed" if discovery_complete else f"In Progress ({last_stage or 'starting'})"

            # Get counts from database
            site_count = await db_repo.get_site_count(run['run_id'])
            file_count = await db_repo.get_file_count(run['run_id'])
            error_count = len(run.get('errors', [])) if isinstance(run, dict) else 0

            table.add_row(
                run['run_id'],
                run['created_at'].strftime("%Y-%m-%d %H:%M:%S"),
                status,
                str(site_count),
                str(file_count),
                str(error_count)
            )

        console.print(table)

        # If specific run requested, show detailed progress
        if run_id and not discovery_complete:
            console.print("\n[bold]Detailed Progress:[/bold]")

            # Create discovery module to get progress summary
            discovery = EnhancedDiscoveryModule(
                None, None, db_repo, checkpoints=checkpoint_manager
            )

            summary = await discovery.get_crash_recovery_summary(run_id)

            progress_panel = Panel(
                f"""[green]Completed Sites:[/green] {len(summary['completed_sites'])}
[yellow]Pending Sites:[/yellow] {len(summary['pending_sites'])}
[blue]Total Folders:[/blue] {summary['total_folders_discovered']:,}
[blue]Total Files:[/blue] {summary['total_files_discovered']:,}
[dim]Last Update:[/dim] {summary.get('last_update', 'Unknown')}

[bold]Resume Command:[/bold]
sharepoint-audit audit --config config/config.json --resume {run_id}""",
                title="Recovery Information",
                border_style="blue"
            )

            console.print(progress_panel)

            # Show sample of pending sites
            if summary['pending_sites']:
                console.print("\n[bold]Sample Pending Sites:[/bold]")
                for site in summary['pending_sites'][:5]:
                    console.print(f"  • {site}")
                if len(summary['pending_sites']) > 5:
                    console.print(f"  ... and {len(summary['pending_sites']) - 5} more")

    except Exception as e:
        console.print(f"[red]Error checking recovery status: {e}[/red]")
        logger.error("Recovery status check failed", exc_info=True)
    finally:
        if 'db_repo' in locals():
            await db_repo.close()


if __name__ == "__main__":
    recovery_status()
