"""Command to show current and recent run information."""

import asyncio
import json
from pathlib import Path
from datetime import datetime

import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from utils.run_id_manager import RunIDManager
from utils.logger import LoggingConfiguration

console = Console()


@click.command()
@click.option(
    "--current",
    is_flag=True,
    help="Show only the current running audit"
)
@click.option(
    "--last",
    is_flag=True,
    help="Show only the last completed audit"
)
@click.option(
    "--history",
    type=int,
    default=0,
    help="Show recent run history (default: 5 if no other option specified)"
)
def run_info(current: bool, last: bool, history: int) -> None:
    """Show information about audit runs."""
    LoggingConfiguration.setup_logging()

    run_id_manager = RunIDManager()

    # If no specific option, show current + last 5
    if not current and not last and history == 0:
        current = True
        last = True
        history = 5

    # Show current run
    if current:
        current_run = run_id_manager.get_current_run()
        if current_run:
            panel = Panel(
                f"""[bold green]Run ID:[/bold green] {current_run['run_id']}
[bold]Status:[/bold] {current_run.get('status', 'running')}
[bold]Started:[/bold] {current_run['started_at']}
[bold]PID:[/bold] {current_run.get('pid', 'unknown')}
[bold]Config:[/bold] {current_run.get('config_path', 'unknown')}

[dim]Run ID file:[/dim] .current_run_id
[dim]Details file:[/dim] .runs/current_run.json""",
                title="[bold cyan]Current Running Audit[/bold cyan]",
                border_style="green"
            )
            console.print(panel)
        else:
            console.print("[yellow]No audit currently running[/yellow]\n")

    # Show last completed run
    if last:
        last_run = run_id_manager.get_last_run()
        if last_run:
            duration = None
            if 'started_at' in last_run and 'completed_at' in last_run:
                start = datetime.fromisoformat(last_run['started_at'])
                end = datetime.fromisoformat(last_run['completed_at'])
                duration = (end - start).total_seconds()

            panel = Panel(
                f"""[bold blue]Run ID:[/bold blue] {last_run['run_id']}
[bold]Status:[/bold] {last_run.get('status', 'unknown')}
[bold]Started:[/bold] {last_run['started_at']}
[bold]Completed:[/bold] {last_run.get('completed_at', 'unknown')}
[bold]Duration:[/bold] {f'{duration:.1f}s' if duration else 'unknown'}
[bold]Config:[/bold] {last_run.get('config_path', 'unknown')}
{f"[bold red]Error:[/bold red] {last_run['error']}" if 'error' in last_run else ""}

[dim]Details file:[/dim] .runs/last_run.json""",
                title="[bold cyan]Last Completed Audit[/bold cyan]",
                border_style="blue"
            )
            console.print(panel)
        else:
            console.print("[yellow]No completed audits found[/yellow]\n")

    # Show history
    if history > 0:
        runs = run_id_manager.get_run_history(history)
        if runs:
            table = Table(title=f"Recent Audit Runs (Last {history})")
            table.add_column("Run ID", style="cyan")
            table.add_column("Started", style="green")
            table.add_column("Status", style="yellow")
            table.add_column("Duration", style="blue")
            table.add_column("Config", style="dim")

            for run in runs:
                duration = None
                if run.get('status') != 'running' and 'started_at' in run and 'completed_at' in run:
                    start = datetime.fromisoformat(run['started_at'])
                    end = datetime.fromisoformat(run['completed_at'])
                    duration = (end - start).total_seconds()

                table.add_row(
                    run['run_id'],
                    run['started_at'][:19],  # Trim to just date/time
                    run.get('status', 'unknown'),
                    f"{duration:.1f}s" if duration else "-",
                    Path(run.get('config_path', 'unknown')).name
                )

            console.print(table)
        else:
            console.print("[yellow]No run history found[/yellow]")

    # Show quick access info
    console.print("\n[bold]Quick Access:[/bold]")
    console.print("• Current run ID: [cyan]cat .current_run_id[/cyan]")
    console.print("• Resume command: [cyan]sharepoint-audit audit --resume $(cat .current_run_id)[/cyan]")
    console.print("• Run details: [cyan]cat .runs/current_run.json | jq[/cyan]")


if __name__ == "__main__":
    run_info()
