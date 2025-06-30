"""CLI commands for the SharePoint Audit Utility."""

import asyncio
import datetime
import logging
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Optional, List, Dict, Any

import click
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
from rich.table import Table

from ..core.pipeline import AuditPipeline, PipelineContext
from ..api.auth_manager import AuthenticationManager
from ..api.graph_client import GraphAPIClient
from ..api.sharepoint_client import SharePointAPIClient
from ..cache.cache_manager import CacheManager
from ..core.discovery import DiscoveryModule
from ..core.permissions import PermissionAnalyzer
from ..core.processors import (
    DiscoveryStage,
    ValidationStage,
    TransformationStage,
    EnrichmentStage,
    StorageStage,
    PermissionAnalysisStage
)
from ..core.pipeline_metrics import PipelineMetrics
from ..database.repository import DatabaseRepository
from ..utils.checkpoint_manager import CheckpointManager
from ..utils.rate_limiter import RateLimiter
from ..utils.retry_handler import RetryStrategy, RetryConfig
from .config_parser import load_and_merge_config
from .output import RichOutput, setup_logging

logger = logging.getLogger(__name__)
console = Console()
output = RichOutput()


@click.command()
@click.option('--config', default='config/config.json', help='Path to configuration file.')
@click.option('--sites', help='Comma-separated list of specific site URLs to audit.')
@click.option('--dry-run', is_flag=True, help='Show what would be done without making API calls.')
@click.option('-v', '--verbose', count=True, help='Increase verbosity level (-v, -vv, -vvv).')
@click.option('--resume', help='Resume a previous run by providing its run ID.')
@click.option('--analyze-permissions', is_flag=True, help='Include permission analysis in the audit.')
@click.option('--output-format', type=click.Choice(['table', 'json', 'csv']), default='table',
              help='Format for summary output.')
@click.option('--batch-size', type=int, default=100, help='Batch size for processing items.')
@click.option('--max-concurrent', type=int, default=50, help='Maximum concurrent operations.')
@click.pass_context
def audit(ctx, config, sites, dry_run, verbose, resume, analyze_permissions, output_format, batch_size, max_concurrent):
    """Run a comprehensive SharePoint audit.

    This command discovers and audits all SharePoint sites, libraries, folders,
    files, and optionally permissions in your tenant.
    """
    # Setup logging based on verbosity
    setup_logging(verbose)

    # Show banner
    output.show_banner("SharePoint Audit")

    # Parse CLI arguments
    cli_args = {
        'target_sites': sites.split(',') if sites else None,
        'batch_size': batch_size,
        'max_concurrent': max_concurrent,
        'analyze_permissions': analyze_permissions
    }

    try:
        # Load and merge configuration
        with output.status("Loading configuration..."):
            final_config = load_and_merge_config(config_path=config, cli_args=cli_args)

        output.success("Configuration loaded successfully")

        if dry_run:
            output.info("Running in DRY-RUN mode - no API calls will be made")
            _show_dry_run_plan(final_config)
            return

        # Run the audit
        asyncio.run(_run_audit(ctx, final_config, resume, analyze_permissions, output_format))

    except FileNotFoundError as e:
        output.error(f"Configuration file not found: {e}")
        ctx.exit(1)
    except ValueError as e:
        output.error(f"Configuration error: {e}")
        ctx.exit(1)
    except KeyboardInterrupt:
        output.warning("Audit interrupted by user")
        ctx.exit(1)
    except Exception as e:
        output.error(f"Audit failed: {e}")
        logger.exception("Detailed error:")
        ctx.exit(1)


async def _run_audit(ctx, config: Dict[str, Any], resume_id: Optional[str],
                    analyze_permissions: bool, output_format: str):
    """Run the actual audit pipeline."""
    # Generate or use existing run ID
    run_id = resume_id or f"audit_run_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    if resume_id:
        output.info(f"Resuming audit run: {run_id}")
    else:
        output.info(f"Starting new audit run: {run_id}")

    # Initialize components
    output.info("Initializing audit components...")

    # Create database repository
    db_path = config.get('db', {}).get('path', 'audit.db')
    db_repo = DatabaseRepository(db_path)
    await db_repo.initialize_database()

    # Create authentication manager
    auth_config = config['auth']
    auth_manager = AuthenticationManager(auth_config)

    # Create API clients
    rate_limiter = RateLimiter()
    retry_strategy = RetryStrategy(RetryConfig(
        max_attempts=3,
        base_delay=0.5,
        max_delay=30,
        circuit_breaker_threshold=5,
        circuit_breaker_timeout=60
    ))

    graph_client = GraphAPIClient(auth_manager, retry_strategy, rate_limiter)
    sp_client = SharePointAPIClient(auth_manager, retry_strategy, rate_limiter)

    # Create checkpoint manager
    checkpoint_manager = CheckpointManager(db_repo)

    # Create discovery module
    discovery_module = DiscoveryModule(
        graph_client,
        sp_client,
        db_repo,
        checkpoint_manager,
        max_concurrent_sites=config.get('max_concurrent', 50),
        max_concurrent_operations=config.get('max_concurrent', 50)
    )

    # Create pipeline context
    context = PipelineContext(
        run_id=run_id,
        config=config,
        metrics=PipelineMetrics(),
        checkpoint_manager=checkpoint_manager,
        db_repository=db_repo
    )

    # Create audit run record
    if not resume_id:
        await db_repo.create_audit_run(run_id)

    # Create pipeline
    pipeline = AuditPipeline(context)

    # Add stages
    pipeline.add_stage(DiscoveryStage(discovery_module))
    pipeline.add_stage(ValidationStage())
    pipeline.add_stage(TransformationStage())
    pipeline.add_stage(EnrichmentStage())
    pipeline.add_stage(StorageStage(db_repo))

    if analyze_permissions:
        output.info("Permission analysis enabled")
        cache_manager = CacheManager(db_repo)
        permission_analyzer = PermissionAnalyzer(
            graph_client=graph_client,
            sp_client=sp_client,
            db_repo=db_repo,
            cache_manager=cache_manager
        )
        pipeline.add_stage(PermissionAnalysisStage(permission_analyzer))

    # Run pipeline with progress tracking
    output.info("Starting audit pipeline...")

    # Create progress display
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        # Add main task
        main_task = progress.add_task("Auditing SharePoint", total=None)

        # Run pipeline
        try:
            result = await pipeline.run()
            progress.update(main_task, completed=100)

            # Show results
            _show_audit_results(result, output_format)

            # Update audit run status
            status = "completed" if not result.errors else "completed_with_errors"
            await result.db_repository.update_audit_run(
                result.run_id,
                {
                    "completed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "status": status,
                    "total_sites_processed": len(result.sites),
                    "total_items_processed": result.total_items,
                    "total_errors": len(result.errors),
                    "error_details": "\n".join(result.errors[:10]) if result.errors else None
                }
            )

            if result.errors:
                output.warning(f"Audit completed with {len(result.errors)} errors")
                for i, error in enumerate(result.errors[:5]):
                    output.error(f"  {i+1}. {error}")
                if len(result.errors) > 5:
                    output.info(f"  ... and {len(result.errors) - 5} more errors")
            else:
                output.success("Audit completed successfully!")

        except Exception as e:
            progress.stop()
            raise


def _show_dry_run_plan(config: Dict[str, Any]):
    """Show what would be done in a dry run."""
    table = Table(title="Dry Run Plan", show_header=True, header_style="bold magenta")
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Action", style="white")

    table.add_row("Authentication", f"Using tenant: {config['auth']['tenant_id']}")
    table.add_row("Database", f"Using database: {config.get('db', {}).get('path', 'audit.db')}")

    if config.get('target_sites'):
        table.add_row("Sites", f"Auditing specific sites: {', '.join(config['target_sites'])}")
    else:
        table.add_row("Sites", "Discovering all sites in tenant")

    table.add_row("Stages", "Discovery → Validation → Transformation → Enrichment → Storage")

    if config.get('analyze_permissions'):
        table.add_row("Permissions", "Permission analysis enabled")

    console.print(table)


def _show_audit_results(result: PipelineContext, output_format: str):
    """Display audit results in the requested format."""
    if output_format == "json":
        import json
        summary = {
            "run_id": result.run_id,
            "duration_seconds": result.metrics.total_duration,
            "sites_processed": len(result.sites),
            "total_items": result.total_items,
            "errors": len(result.errors),
            "metrics": result.metrics.custom_metrics
        }
        console.print_json(json.dumps(summary, indent=2))

    elif output_format == "csv":
        # Simple CSV output
        console.print("metric,value")
        console.print(f"run_id,{result.run_id}")
        console.print(f"duration_seconds,{result.metrics.total_duration:.2f}")
        console.print(f"sites_processed,{len(result.sites)}")
        console.print(f"total_items,{result.total_items}")
        console.print(f"errors,{len(result.errors)}")

    else:  # table format
        table = Table(title="Audit Summary", show_header=True, header_style="bold green")
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="white")

        table.add_row("Run ID", result.run_id)
        table.add_row("Duration", f"{result.metrics.total_duration:.2f} seconds")
        table.add_row("Sites Processed", str(len(result.sites)))
        table.add_row("Total Items", f"{result.total_items:,}")
        table.add_row("Errors", str(len(result.errors)))

        if result.metrics.custom_metrics:
            if "total_files" in result.metrics.custom_metrics:
                table.add_row("Total Files", f"{result.metrics.custom_metrics['total_files']:,}")
            if "total_storage_gb" in result.metrics.custom_metrics:
                table.add_row("Total Storage", f"{result.metrics.custom_metrics['total_storage_gb']:.2f} GB")
            if "average_file_size_mb" in result.metrics.custom_metrics:
                table.add_row("Average File Size", f"{result.metrics.custom_metrics['average_file_size_mb']:.2f} MB")

        console.print(table)


@click.command()
@click.option('--db-path', required=True, help='Path to the audit database file.')
@click.option('--port', default=8501, help='Port to run the dashboard on.')
@click.option('--no-browser', is_flag=True, help='Do not open browser automatically.')
@click.pass_context
def dashboard(ctx, db_path, port, no_browser):
    """Launch the Streamlit dashboard for viewing audit results.

    This command starts a web-based dashboard that allows you to explore
    audit results, view analytics, and generate reports.
    """
    output.show_banner("SharePoint Audit Dashboard")

    # Check if database exists
    if not Path(db_path).exists():
        output.error(f"Database file not found: {db_path}")
        ctx.exit(1)

    # Check if streamlit app exists
    streamlit_app_path = Path(__file__).parent.parent / "dashboard" / "streamlit_app.py"
    if not streamlit_app_path.exists():
        output.warning("Dashboard not yet implemented (Phase 8)")
        output.info("Creating placeholder dashboard...")
        # Create a minimal placeholder
        streamlit_app_path.parent.mkdir(parents=True, exist_ok=True)
        streamlit_app_path.write_text(_get_placeholder_dashboard())

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


@click.command()
@click.option('--db-path', required=True, help='Path to the audit database to backup.')
@click.option('--output', required=True, help='Path for the backup file.')
@click.option('--compress', is_flag=True, help='Compress the backup file.')
@click.pass_context
def backup(ctx, db_path, output, compress):
    """Create a backup of the audit database.

    This command creates a backup copy of your audit database, optionally
    with compression.
    """
    output_handler = RichOutput()
    output_handler.show_banner("Database Backup")

    # Check if database exists
    if not Path(db_path).exists():
        output_handler.error(f"Database file not found: {db_path}")
        ctx.exit(1)

    try:
        with output_handler.status("Creating backup..."):
            if compress:
                import gzip
                import shutil

                # Create compressed backup
                with open(db_path, 'rb') as f_in:
                    with gzip.open(f"{output}.gz", 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                output_handler.success(f"Backup created: {output}.gz")
            else:
                # Simple file copy
                import shutil
                shutil.copy2(db_path, output)
                output_handler.success(f"Backup created: {output}")

    except Exception as e:
        output_handler.error(f"Backup failed: {e}")
        ctx.exit(1)


@click.command()
@click.option('--backup-path', required=True, help='Path to the backup file.')
@click.option('--db-path', required=True, help='Path where database should be restored.')
@click.option('--force', is_flag=True, help='Overwrite existing database.')
@click.pass_context
def restore(ctx, backup_path, db_path, force):
    """Restore an audit database from a backup.

    This command restores a previously created backup of the audit database.
    """
    output = RichOutput()
    output.show_banner("Database Restore")

    # Check if backup exists
    if not Path(backup_path).exists():
        output.error(f"Backup file not found: {backup_path}")
        ctx.exit(1)

    # Check if target exists
    if Path(db_path).exists() and not force:
        output.error(f"Database already exists: {db_path}")
        output.info("Use --force to overwrite")
        ctx.exit(1)

    try:
        with output.status("Restoring backup..."):
            if backup_path.endswith('.gz'):
                import gzip
                import shutil

                # Decompress backup
                with gzip.open(backup_path, 'rb') as f_in:
                    with open(db_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            else:
                # Simple file copy
                import shutil
                shutil.copy2(backup_path, db_path)

        output.success(f"Database restored: {db_path}")

    except Exception as e:
        output.error(f"Restore failed: {e}")
        ctx.exit(1)


@click.command()
@click.option('--config', default='config/config.json', help='Path to configuration file.')
@click.option('--check-auth', is_flag=True, help='Test authentication.')
@click.option('--check-api', is_flag=True, help='Test API connectivity.')
@click.option('--check-db', help='Test database connectivity (provide path).')
@click.pass_context
def health(ctx, config, check_auth, check_api, check_db):
    """Check system health and connectivity.

    This command runs various health checks to ensure your environment
    is properly configured and all components are working correctly.
    """
    output = RichOutput()
    output.show_banner("Health Check")

    all_checks_passed = True

    # Load configuration if needed
    if check_auth or check_api:
        try:
            with output.status("Loading configuration..."):
                from ..utils.config_parser import load_config
                app_config = load_config(config)
            output.success("Configuration loaded")
        except Exception as e:
            output.error(f"Failed to load configuration: {e}")
            all_checks_passed = False

    # Check authentication
    if check_auth and all_checks_passed:
        try:
            with output.status("Testing authentication..."):
                auth_manager = AuthenticationManager(app_config.auth)
                # This will attempt to get a token
                asyncio.run(auth_manager.get_token())
            output.success("Authentication successful")
        except Exception as e:
            output.error(f"Authentication failed: {e}")
            all_checks_passed = False

    # Check API connectivity
    if check_api and all_checks_passed:
        try:
            with output.status("Testing API connectivity..."):
                async def test_api():
                    auth_manager = AuthenticationManager(app_config.auth)
                    rate_limiter = RateLimiter()
                    retry_strategy = RetryStrategy(RetryConfig())

                    graph_client = GraphAPIClient(auth_manager, retry_strategy, rate_limiter)
                    # Try to get tenant info
                    await graph_client.get_tenant_info()

                asyncio.run(test_api())
            output.success("API connectivity verified")
        except Exception as e:
            output.error(f"API test failed: {e}")
            all_checks_passed = False

    # Check database
    if check_db:
        try:
            with output.status("Testing database connectivity..."):
                async def test_db():
                    db_repo = DatabaseRepository(check_db)
                    await db_repo.initialize_database()
                    # Try a simple query
                    await db_repo.get_audit_runs()

                asyncio.run(test_db())
            output.success("Database connectivity verified")
        except Exception as e:
            output.error(f"Database test failed: {e}")
            all_checks_passed = False

    # Show summary
    if all_checks_passed:
        output.success("\nAll health checks passed!")
    else:
        output.error("\nSome health checks failed. Please check the errors above.")
        ctx.exit(1)


def _get_placeholder_dashboard():
    """Get placeholder dashboard code."""
    return '''"""Placeholder dashboard for SharePoint Audit (Phase 8 - To be implemented)."""

import streamlit as st
import sys
from pathlib import Path

st.set_page_config(
    page_title="SharePoint Audit Dashboard",
    page_icon="📊",
    layout="wide"
)

st.title("SharePoint Audit Dashboard")
st.info("This dashboard will be implemented in Phase 8 of the development plan.")

# Parse command line arguments
db_path = None
if "--db-path" in sys.argv:
    idx = sys.argv.index("--db-path")
    if idx + 1 < len(sys.argv):
        db_path = sys.argv[idx + 1]

if db_path and Path(db_path).exists():
    st.success(f"Database found: {db_path}")

    st.subheader("Planned Features")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("""
        **Analytics & Visualizations**
        - Site storage usage charts
        - File type distribution
        - Permission complexity metrics
        - User access patterns
        """)

    with col2:
        st.markdown("""
        **Data Exploration**
        - Browse sites and libraries
        - Search files and folders
        - View permission reports
        - Export audit data
        """)
else:
    st.error("No database path provided or database not found.")
    st.info("Usage: streamlit run streamlit_app.py -- --db-path /path/to/audit.db")
'''
