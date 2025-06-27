#!/usr/bin/env python3
"""Script to run the audit pipeline."""

import asyncio
import logging
import sys
from pathlib import Path

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


async def create_pipeline(config_path: str = "config/config.json") -> AuditPipeline:
    """Create and configure the audit pipeline."""
    # Load configuration
    logger.info(f"Loading configuration from {config_path}")
    config = load_config(config_path)

    # Initialize components
    logger.info("Initializing components...")

    # Authentication
    auth_manager = AuthenticationManager(config.auth)

    # API clients with rate limiting and retry
    rate_limiter = RateLimiter()
    retry_strategy = RetryStrategy(RetryConfig())

    graph_client = GraphAPIClient(auth_manager, retry_strategy, rate_limiter)
    sp_client = SharePointAPIClient(auth_manager, retry_strategy, rate_limiter)

    # Database
    db_path = config.db.path
    db_repo = DatabaseRepository(db_path)
    await db_repo.initialize_database()

    # Checkpoint manager
    checkpoint_manager = CheckpointManager(db_repo)

    # Discovery module
    discovery_module = DiscoveryModule(
        graph_client,
        sp_client,
        db_repo,
        checkpoint_manager
    )

    # Create pipeline context
    run_id = f"audit_run_{int(asyncio.get_event_loop().time())}"
    context = PipelineContext(
        run_id=run_id,
        config=config.__dict__,
        metrics=PipelineMetrics(),
        checkpoint_manager=checkpoint_manager,
        db_repository=db_repo
    )

    # Create pipeline
    pipeline = AuditPipeline(context)

    # Add stages
    logger.info("Adding pipeline stages...")
    pipeline.add_stage(DiscoveryStage(discovery_module))
    pipeline.add_stage(ValidationStage())
    pipeline.add_stage(TransformationStage())
    pipeline.add_stage(EnrichmentStage())
    pipeline.add_stage(StorageStage(db_repo))

    # Add permission analysis stage if available
    # Note: This would be added in Phase 5
    # pipeline.add_stage(PermissionAnalysisStage(permission_analyzer))

    return pipeline


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
        "--analyze-permissions",
        action="store_true",
        help="Include permission analysis stage"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no API calls)"
    )

    args = parser.parse_args()

    try:
        # Create and run pipeline
        logger.info("Creating audit pipeline...")
        pipeline = await create_pipeline(args.config)

        if args.dry_run:
            logger.info("Running in dry-run mode - no actual API calls will be made")
            # In dry-run mode, we would mock the API calls

        logger.info(f"Starting pipeline run: {pipeline.context.run_id}")
        logger.info("=" * 60)

        # Run the pipeline
        result = await pipeline.run()

        # Log metrics
        result.metrics.log_summary()

        # Check for errors
        if result.errors:
            logger.error(f"Pipeline completed with {len(result.errors)} errors:")
            for error in result.errors:
                logger.error(f"  - {error}")
            sys.exit(1)
        else:
            logger.info("Pipeline completed successfully!")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
