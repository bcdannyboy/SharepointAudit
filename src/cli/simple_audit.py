"""Simple audit command for testing."""

import click
import subprocess
import sys
from pathlib import Path

@click.command()
@click.option('--config', default='config/config.json', help='Path to configuration file.')
@click.option('--sites', help='Comma-separated list of specific site URLs to audit.')
@click.option('--dry-run', is_flag=True, help='Show what would be done without making API calls.')
@click.option('-v', '--verbose', count=True, help='Increase verbosity level (-v, -vv, -vvv).')
@click.option('--resume', help='Resume a previous run by providing its run ID.')
@click.option('--active-only', is_flag=True, help='Only scan active SharePoint sites (exclude inactive/archived sites).')
@click.option('--output-format', type=click.Choice(['table', 'json', 'csv']), default='table',
              help='Format for summary output.')
@click.option('--batch-size', type=int, default=100, help='Batch size for processing items.')
@click.option('--max-concurrent', type=int, default=50, help='Maximum concurrent operations.')
@click.pass_context
def audit(ctx, config, sites, dry_run, verbose, resume, active_only, output_format, batch_size, max_concurrent):
    """Run a comprehensive SharePoint audit.

    This command discovers and audits all SharePoint sites, libraries, folders,
    files, and permissions in your tenant.
    """
    # For now, run the pipeline script with the provided config
    print(f"Running audit with config: {config}")

    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent
    pipeline_script = project_root / "scripts" / "run_pipeline.py"

    if not pipeline_script.exists():
        click.echo(f"Error: Pipeline script not found at {pipeline_script}")
        ctx.exit(1)

    # Build the command
    cmd = [sys.executable, str(pipeline_script), "--config", config]

    # Only pass arguments that run_pipeline.py accepts
    if dry_run:
        cmd.append("--dry-run")

    if resume:
        cmd.extend(["--resume", resume])

    if active_only:
        cmd.append("--active-only")

    if sites:
        cmd.extend(["--sites", sites])

    # Log the options that aren't supported by the pipeline script yet
    if verbose:
        click.echo(f"Note: Verbose level {verbose} requested but not yet supported by pipeline")

    if output_format != 'table':
        click.echo(f"Note: Output format '{output_format}' not yet supported by pipeline")

    if batch_size != 100:
        click.echo(f"Note: Batch size {batch_size} not yet supported by pipeline")

    if max_concurrent != 50:
        click.echo(f"Note: Max concurrent {max_concurrent} not yet supported by pipeline")

    # Run the command
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        click.echo(f"Error: Audit failed with exit code {e.returncode}")
        ctx.exit(e.returncode)
    except KeyboardInterrupt:
        click.echo("Audit interrupted by user")
        ctx.exit(1)
