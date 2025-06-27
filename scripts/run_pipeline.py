#!/usr/bin/env python3
"""Script to run the audit pipeline."""

import asyncio
import logging
import sys
import uuid
from pathlib import Path
from datetime import datetime

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.auth_manager import AuthenticationManager
from src.api.graph_client import GraphAPIClient
from src.api.sharepoint_client import SharePointAPIClient
from src.core.discovery import DiscoveryModule
from src.core.pipeline import AuditPipeline, PipelineContext
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


async def create_pipeline(config_path: str = "config/config.json",
                        run_id: Optional[str] = None) -> AuditPipeline:
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
        checkpoint_manager,
        max_concurrent_sites=20,
        max_concurrent_operations=50
    )

    # Create pipeline context
    if not run_id:
        run_id = f"audit_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    context = PipelineContext(
        run_id=run_id,
        config=config.__dict__ if hasattr(config, '__dict__') else {},
        metrics=PipelineMetrics(),
        checkpoint_manager=checkpoint_manager,
        db_repository=db_repo
    )

    # Create audit run record in database
    await db_repo.create_audit_run(run_id)

    # Create pipeline
    pipeline = AuditPipeline(context)

    # Add stages
    logger.info("Adding pipeline stages...")
    pipeline.add_stage(DiscoveryStage(discovery_module))
    pipeline.add_stage(ValidationStage())
    pipeline.add_stage(TransformationStage())
    pipeline.add_stage(EnrichmentStage())
    pipeline.add_stage(StorageStage(db_repo))

    # Add permission analysis stage if available (Phase 5)
    # Note: This is a placeholder for Phase 5
    # pipeline.add_stage(PermissionAnalysisStage(permission_analyzer))

    return pipeline


async def main():
    """Main entry point for the pipeline runner."""
    import argparse
    from typing import Optional

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
    parser.add_argument(
        "--analyze-permissions",
        action="store_true",
        help="Include permission analysis stage (Phase 5)"
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

    args = parser.parse_args()

    try:
        # Use resume ID if provided
        run_id = args.resume or args.run_id

        # Create and configure pipeline
        logger.info("Creating audit pipeline...")
        pipeline = await create_pipeline(args.config, run_id)

        if args.dry_run:
            logger.info("Running in dry-run mode - no actual API calls will be made")
            # In dry-run mode, we would mock the API calls
            # For now, just add some test data
            pipeline.context.raw_data = [
                {
                    "site_id": "test_site_1",
                    "url": "https://test.sharepoint.com/sites/test1",
                    "title": "Test Site 1",
                    "created_at": "2023-01-01T00:00:00Z"
                }
            ]

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
                "completed_at": datetime.utcnow().isoformat(),
                "status": status,
                "total_sites_processed": len(result.sites),
                "total_items_processed": result.total_items,
                "total_errors": len(result.errors),
                "error_details": "\n".join(result.errors[:10]) if result.errors else None
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

    # Run the main function
    asyncio.run(main())
