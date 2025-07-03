#!/usr/bin/env python3
"""Script to run the audit pipeline."""

import asyncio
import logging
import sys
import uuid
import threading
import signal
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.auth_manager import AuthenticationManager
from src.api.graph_client import GraphAPIClient
from src.api.sharepoint_client import SharePointAPIClient
from src.cache.cache_manager import CacheManager
from src.core.discovery import DiscoveryModule
from src.core.permissions import PermissionAnalyzer
from src.core.pipeline import AuditPipeline, PipelineContext, PipelineStage
from src.core.processors import (
    DiscoveryStage,
    ValidationStage,
    TransformationStage,
    EnrichmentStage,
    StorageStage,
    PermissionAnalysisStage
)
from src.core.pipeline_metrics import PipelineMetrics
from src.database.repository import DatabaseRepository
from src.utils.checkpoint_manager import CheckpointManager
from src.utils.config_parser import load_config
from src.utils.logger import LoggingConfiguration
from src.utils.rate_limiter import RateLimiter
from src.utils.retry_handler import RetryStrategy, RetryConfig

# Configure logging
LoggingConfiguration.setup_logging()
logger = logging.getLogger(__name__)


class MockDiscoveryStage(PipelineStage):
    """Mock discovery stage for dry-run mode."""

    def __init__(self):
        super().__init__("mock_discovery")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Provide mock data instead of making API calls."""
        self.logger.info("Running mock discovery stage (dry-run mode)")

        # Add mock sites
        mock_sites = [
            {
                "site_id": "mock_site_1",
                "url": "https://test.sharepoint.com/sites/MockSite1",
                "title": "Mock Test Site 1",
                "created_at": datetime(2023, 1, 1, tzinfo=timezone.utc),
                "created_by": "mock_user@test.com",
                "storage_used": 1024 * 1024 * 100,  # 100 MB
                "storage_quota": 1024 * 1024 * 1024,  # 1 GB
                "web_template": "STS#3",
                "locale_id": 1033
            },
            {
                "site_id": "mock_site_2",
                "url": "https://test.sharepoint.com/sites/MockSite2",
                "title": "Mock Test Site 2",
                "created_at": datetime(2023, 6, 1, tzinfo=timezone.utc),
                "created_by": "mock_admin@test.com",
                "storage_used": 1024 * 1024 * 500,  # 500 MB
                "storage_quota": 1024 * 1024 * 1024 * 5,  # 5 GB
                "web_template": "TEAMCHANNEL#0",
                "locale_id": 1033
            }
        ]

        # Add mock libraries
        mock_libraries = [
            {
                "library_id": "mock_lib_1",
                "site_id": "mock_site_1",
                "name": "Documents",
                "server_relative_url": "/sites/MockSite1/Shared Documents",
                "created_at": datetime(2023, 1, 15, tzinfo=timezone.utc),
                "item_count": 150,
                "is_catalog": False,
                "is_private_library": False
            },
            {
                "library_id": "mock_lib_2",
                "site_id": "mock_site_2",
                "name": "Site Assets",
                "server_relative_url": "/sites/MockSite2/SiteAssets",
                "created_at": datetime(2023, 6, 15, tzinfo=timezone.utc),
                "item_count": 50,
                "is_catalog": False,
                "is_private_library": False
            }
        ]

        # Add mock files
        mock_files = [
            {
                "file_id": "mock_file_1",
                "library_id": "mock_lib_1",
                "site_id": "mock_site_1",
                "name": "ProjectPlan.docx",
                "server_relative_url": "/sites/MockSite1/Shared Documents/ProjectPlan.docx",
                "size_bytes": 1024 * 500,  # 500 KB
                "created_at": datetime(2023, 2, 1, tzinfo=timezone.utc),
                "modified_at": datetime(2023, 11, 1, tzinfo=timezone.utc),
                "created_by": "mock_user@test.com",
                "modified_by": "mock_user@test.com",
                "version": "2.0"
            },
            {
                "file_id": "mock_file_2",
                "library_id": "mock_lib_1",
                "site_id": "mock_site_1",
                "name": "Budget2023.xlsx",
                "server_relative_url": "/sites/MockSite1/Shared Documents/Finance/Budget2023.xlsx",
                "size_bytes": 1024 * 1024 * 2,  # 2 MB
                "created_at": datetime(2023, 3, 1, tzinfo=timezone.utc),
                "modified_at": datetime(2023, 12, 15, tzinfo=timezone.utc),
                "created_by": "mock_finance@test.com",
                "modified_by": "mock_finance@test.com",
                "version": "5.0"
            }
        ]

        # Add mock folders
        mock_folders = [
            {
                "folder_id": "mock_folder_1",
                "library_id": "mock_lib_1",
                "site_id": "mock_site_1",
                "name": "Finance",
                "server_relative_url": "/sites/MockSite1/Shared Documents/Finance",
                "created_at": datetime(2023, 1, 20, tzinfo=timezone.utc),
                "item_count": 25,
                "is_root": False
            }
        ]

        # Set context data
        context.sites = mock_sites
        context.libraries = mock_libraries
        context.files = mock_files
        context.folders = mock_folders
        context.raw_data = mock_sites
        context.total_items = len(mock_sites) + len(mock_libraries) + len(mock_files) + len(mock_folders)

        # Record metrics
        if context.metrics:
            context.metrics.record_stage_items(self.name, len(mock_sites))

        self.logger.info(f"Mock discovery complete: {len(mock_sites)} sites, "
                        f"{len(mock_libraries)} libraries, {len(mock_files)} files, "
                        f"{len(mock_folders)} folders")

        return context


async def create_pipeline(config_path: str = "config/config.json",
                        run_id: Optional[str] = None,
                        dry_run: bool = False,
                        active_only: bool = False,
                        sites_to_process: Optional[List[str]] = None,
                        limit: Optional[int] = None) -> AuditPipeline:
    """Create and configure the audit pipeline."""
    # Load configuration
    logger.info(f"Loading configuration from {config_path}")
    try:
        config = load_config(config_path)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        logger.info("Creating a basic configuration for testing...")
        # Create a minimal config for testing
        from src.utils.config_parser import AppConfig, AuthConfig, DbConfig
        config = AppConfig(
            auth=AuthConfig(
                tenant_id="test_tenant",
                client_id="test_client",
                certificate_path="test_cert.pem"
            ),
            db=DbConfig(path="test_audit.db")
        )

    # Initialize components
    logger.info("Initializing components...")

    # Authentication
    auth_manager = AuthenticationManager(config.auth)

    # API clients with rate limiting and retry
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

    # Database
    db_path = getattr(config.db, 'path', 'audit.db')
    logger.info(f"Using database: {db_path}")
    db_repo = DatabaseRepository(db_path)

    # Initialize database
    logger.info("Initializing database...")
    await db_repo.initialize_database()

    # Checkpoint manager
    checkpoint_manager = CheckpointManager(db_repo)

    # Discovery module
    discovery_module = DiscoveryModule(
        graph_client,
        sp_client,
        db_repo,
        cache=None,
        checkpoints=checkpoint_manager,
        max_concurrent_operations=3,  # Further reduced to prevent deadlock
        active_only=active_only
    )

    # Set limit if provided
    if limit:
        discovery_module.site_limit = limit
        logger.info(f"Limiting discovery to {limit} sites")

    # Create pipeline context
    if not run_id:
        run_id = f"audit_run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    context = PipelineContext(
        run_id=run_id,
        config=config.__dict__ if hasattr(config, '__dict__') else {},
        metrics=PipelineMetrics(),
        checkpoint_manager=checkpoint_manager,
        db_repository=db_repo
    )

    # Add sites to filter if provided
    if sites_to_process:
        context.sites_to_process = sites_to_process

    # Create audit run record in database
    await db_repo.create_audit_run(run_id)

    # Create pipeline
    pipeline = AuditPipeline(context)

    # Add stages
    logger.info("Adding pipeline stages...")

    if dry_run:
        # Use mock discovery stage for dry-run mode
        pipeline.add_stage(MockDiscoveryStage())
    else:
        pipeline.add_stage(DiscoveryStage(discovery_module))

    pipeline.add_stage(ValidationStage())
    pipeline.add_stage(TransformationStage())
    pipeline.add_stage(EnrichmentStage())

    # Always add permission analysis stage for comprehensive auditing
    logger.info("Adding permission analysis stage...")

    # Create cache manager
    cache_manager = CacheManager(db_repo)

    # Create permission analyzer
    permission_analyzer = PermissionAnalyzer(
        graph_client=graph_client,
        sp_client=sp_client,
        db_repo=db_repo,
        cache=cache_manager
    )

    pipeline.add_stage(PermissionAnalysisStage(permission_analyzer))

    # Add storage stage AFTER permission analysis so permissions are saved
    pipeline.add_stage(StorageStage(db_repo))

    return pipeline


async def monitor_tasks():
    """Monitor active asyncio tasks and log their status."""
    while True:
        try:
            all_tasks = asyncio.all_tasks()
            logger.debug(f"[DEBUG MONITOR] Active tasks: {len(all_tasks)}")

            # Show details of long-running tasks
            for task in all_tasks:
                if not task.done():
                    coro = task.get_coro()
                    logger.debug(f"[DEBUG MONITOR] Active task: {coro}")

            await asyncio.sleep(30)  # Check every 30 seconds
        except Exception as e:
            logger.error(f"[DEBUG MONITOR] Error in task monitor: {e}")
            await asyncio.sleep(30)


async def main_with_timeout():
    """Main function with timeout and monitoring."""
    # Start the task monitor
    monitor_task = asyncio.create_task(monitor_tasks())

    try:
        # Run main with a timeout
        await asyncio.wait_for(main(), timeout=3600)  # 1 hour timeout
    except asyncio.TimeoutError:
        logger.error("[ERROR] Pipeline execution timed out after 1 hour")
        # Log all active tasks when timeout occurs
        all_tasks = asyncio.all_tasks()
        logger.error(f"[ERROR] {len(all_tasks)} tasks were still active at timeout:")
        for task in all_tasks:
            if not task.done():
                logger.error(f"[ERROR] Active task: {task.get_coro()}")
        raise
    finally:
        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass


async def main():
    """Main entry point for the pipeline runner."""
    import argparse

    parser = argparse.ArgumentParser(description="Run the SharePoint audit pipeline")
    parser.add_argument(
        "--config",
        default="config/config.json",
        help="Path to configuration file"
    )
    parser.add_argument(
        "--run-id",
        help="Unique run ID (auto-generated if not provided)"
    )
    # Remove the --analyze-permissions flag since permissions should always be collected
    # parser.add_argument(
    #     "--analyze-permissions",
    #     action="store_true",
    #     default=True,
    #     help="Include permission analysis stage (enabled by default for comprehensive auditing)"
    # )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Only scan active SharePoint sites (exclude inactive/archived sites)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no API calls)"
    )
    parser.add_argument(
        "--resume",
        help="Resume a previous run by providing its run ID"
    )
    parser.add_argument(
        "--sites",
        help="Comma-separated list of specific site URLs to audit"
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Limit the number of sites to process (for testing)"
    )

    args = parser.parse_args()

    try:
        # Use resume ID if provided
        run_id = args.resume or args.run_id

        # Parse sites if provided
        sites_to_process = None
        if args.sites:
            sites_to_process = [s.strip() for s in args.sites.split(',')]
            logger.info(f"Will filter to specific sites: {sites_to_process}")

        # Create and configure pipeline
        logger.info("Creating audit pipeline...")
        pipeline = await create_pipeline(
            args.config,
            run_id,
            dry_run=args.dry_run,
            active_only=args.active_only,
            sites_to_process=sites_to_process,
            limit=args.limit
        )

        if args.resume:
            logger.info(f"Resuming pipeline run: {run_id}")
        else:
            logger.info(f"Starting new pipeline run: {pipeline.context.run_id}")

        logger.info("=" * 60)

        # Run the pipeline
        result = await pipeline.run()

        # Log metrics
        result.metrics.log_summary()

        # Update audit run status
        status = "completed" if not result.errors else "completed_with_errors"
        await result.db_repository.update_audit_run(
            result.run_id,
            {
                "end_time": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "total_sites": len(result.sites),
                "total_files": getattr(result, 'total_files', 0),
                "error_count": len(result.errors)
            }
        )

        # Check for errors
        if result.errors:
            logger.error(f"Pipeline completed with {len(result.errors)} errors:")
            for i, error in enumerate(result.errors[:10]):
                logger.error(f"  {i+1}. {error}")
            if len(result.errors) > 10:
                logger.error(f"  ... and {len(result.errors) - 10} more errors")
            sys.exit(1)
        else:
            logger.info("Pipeline completed successfully!")

            # Show summary
            logger.info("\n" + "=" * 60)
            logger.info("AUDIT SUMMARY")
            logger.info("=" * 60)
            logger.info(f"Run ID: {result.run_id}")
            logger.info(f"Duration: {result.metrics.total_duration:.2f} seconds")
            logger.info(f"Sites Processed: {len(result.sites)}")
            logger.info(f"Total Items: {result.total_items}")

            if result.metrics.custom_metrics:
                logger.info("\nStorage Metrics:")
                if "total_files" in result.metrics.custom_metrics:
                    logger.info(f"  Total Files: {result.metrics.custom_metrics['total_files']:,}")
                if "total_storage_gb" in result.metrics.custom_metrics:
                    logger.info(f"  Total Storage: {result.metrics.custom_metrics['total_storage_gb']:.2f} GB")
                if "average_file_size_mb" in result.metrics.custom_metrics:
                    logger.info(f"  Average File Size: {result.metrics.custom_metrics['average_file_size_mb']:.2f} MB")

            logger.info("=" * 60)

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Check Python version
    if sys.version_info < (3, 11):
        print("Error: Python 3.11 or higher is required")
        sys.exit(1)

    # Set up signal handlers for debugging
    def dump_threads(signum, frame):
        logger.info("[DEBUG] Thread dump requested")
        for thread in threading.enumerate():
            logger.info(f"[DEBUG] Thread: {thread.name} (alive: {thread.is_alive()})")

    signal.signal(signal.SIGUSR1, dump_threads)

    # Run the main function with monitoring
    asyncio.run(main_with_timeout())
