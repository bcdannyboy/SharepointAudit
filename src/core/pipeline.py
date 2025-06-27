"""Data processing pipeline framework for orchestrating audit stages."""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from ..database.repository import DatabaseRepository
from ..utils.checkpoint_manager import CheckpointManager
from .pipeline_metrics import PipelineMetrics

logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    """Holds state that is passed between pipeline stages."""
    run_id: str
    config: Optional[Dict[str, Any]] = None
    raw_data: List[Dict[str, Any]] = field(default_factory=list)
    processed_data: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Optional[PipelineMetrics] = None
    checkpoint_manager: Optional[CheckpointManager] = None
    db_repository: Optional[DatabaseRepository] = None
    total_items: int = 0
    errors: List[str] = field(default_factory=list)

    # Stage-specific data
    sites: List[Dict[str, Any]] = field(default_factory=list)
    libraries: List[Dict[str, Any]] = field(default_factory=list)
    folders: List[Dict[str, Any]] = field(default_factory=list)
    files: List[Dict[str, Any]] = field(default_factory=list)
    permissions: List[Dict[str, Any]] = field(default_factory=list)


class PipelineStage(ABC):
    """Abstract base class for a single stage in the pipeline."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")

    @abstractmethod
    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Executes the logic for this stage."""
        pass

    async def checkpoint(self, context: PipelineContext, state: Dict[str, Any]) -> None:
        """Save checkpoint for this stage."""
        if context.checkpoint_manager:
            await context.checkpoint_manager.save_checkpoint(
                context.run_id,
                f"stage_{self.name}",
                state
            )

    async def restore_checkpoint(self, context: PipelineContext) -> Optional[Dict[str, Any]]:
        """Restore checkpoint for this stage."""
        if context.checkpoint_manager:
            return await context.checkpoint_manager.restore_checkpoint(
                context.run_id,
                f"stage_{self.name}"
            )
        return None


class AuditPipeline:
    """Manages and executes a sequence of pipeline stages."""

    def __init__(self, context: PipelineContext):
        self.context = context
        self._stages: List[PipelineStage] = []
        self.logger = logging.getLogger(__name__)

        # Initialize metrics if not provided
        if not self.context.metrics:
            self.context.metrics = PipelineMetrics()

    def add_stage(self, stage: PipelineStage) -> None:
        """Add a stage to the pipeline."""
        self._stages.append(stage)
        self.logger.debug(f"Added stage: {stage.name}")

    async def run(self) -> PipelineContext:
        """Executes all stages in the pipeline sequentially."""
        self.logger.info(f"Starting pipeline run: {self.context.run_id}")
        self.context.metrics.start_timer()

        try:
            # Check if we're resuming from a checkpoint
            last_completed_stage = await self._get_last_completed_stage()
            start_index = 0

            if last_completed_stage:
                # Find the index to resume from
                for i, stage in enumerate(self._stages):
                    if stage.name == last_completed_stage:
                        start_index = i + 1
                        self.logger.info(f"Resuming from stage: {self._stages[start_index].name if start_index < len(self._stages) else 'END'}")
                        break

            # Execute stages
            for i in range(start_index, len(self._stages)):
                stage = self._stages[i]

                # Check if this stage was already completed
                stage_status = await self._get_stage_status(stage.name)
                if stage_status == "completed":
                    self.logger.info(f"Stage {stage.name} already completed, skipping")
                    continue

                self.logger.info(f"Executing stage: {stage.name}")

                try:
                    # Measure stage duration
                    with self.context.metrics.measure_stage(stage.name):
                        self.context = await stage.execute(self.context)

                    # Mark stage as completed
                    await self._mark_stage_completed(stage.name)

                except Exception as e:
                    self.logger.error(f"Stage {stage.name} failed: {str(e)}")
                    self.context.errors.append(f"Stage {stage.name}: {str(e)}")

                    # Save error state
                    await self._save_pipeline_error(stage.name, str(e))
                    raise

            # Pipeline completed successfully
            self.context.metrics.stop_timer()
            await self._mark_pipeline_completed()

            self.logger.info(
                f"Pipeline completed successfully in {self.context.metrics.total_duration:.2f}s"
            )

        except Exception as e:
            self.context.metrics.stop_timer()
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

        return self.context

    async def _get_last_completed_stage(self) -> Optional[str]:
        """Get the last successfully completed stage."""
        if self.context.checkpoint_manager:
            checkpoint = await self.context.checkpoint_manager.restore_checkpoint(
                self.context.run_id,
                "pipeline_last_completed_stage"
            )
            return checkpoint
        return None

    async def _get_stage_status(self, stage_name: str) -> Optional[str]:
        """Get the status of a specific stage."""
        if self.context.checkpoint_manager:
            return await self.context.checkpoint_manager.restore_checkpoint(
                self.context.run_id,
                f"stage_{stage_name}_status"
            )
        return None

    async def _mark_stage_completed(self, stage_name: str) -> None:
        """Mark a stage as completed."""
        if self.context.checkpoint_manager:
            await self.context.checkpoint_manager.save_checkpoint(
                self.context.run_id,
                f"stage_{stage_name}_status",
                "completed"
            )
            await self.context.checkpoint_manager.save_checkpoint(
                self.context.run_id,
                "pipeline_last_completed_stage",
                stage_name
            )

    async def _save_pipeline_error(self, stage_name: str, error: str) -> None:
        """Save pipeline error state."""
        if self.context.checkpoint_manager:
            await self.context.checkpoint_manager.save_checkpoint(
                self.context.run_id,
                "pipeline_error",
                {
                    "stage": stage_name,
                    "error": error,
                    "timestamp": time.time()
                }
            )

    async def _mark_pipeline_completed(self) -> None:
        """Mark the entire pipeline as completed."""
        if self.context.checkpoint_manager:
            await self.context.checkpoint_manager.save_checkpoint(
                self.context.run_id,
                "pipeline_status",
                {
                    "status": "completed",
                    "timestamp": time.time(),
                    "duration": self.context.metrics.total_duration,
                    "total_items": self.context.total_items
                }
            )


class ParallelProcessor:
    """Handles parallel processing of large datasets."""

    def __init__(self, max_workers: int = 50):
        self.max_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)
        self.logger = logging.getLogger(__name__)

    async def process_items_parallel(
        self,
        items: List[Any],
        processor_func: Any,
        batch_size: int = 100,
        progress_callback: Optional[Any] = None
    ) -> List[Any]:
        """Process items in parallel batches."""
        results = []
        total_items = len(items)

        # Create batches
        batches = [items[i:i + batch_size] for i in range(0, total_items, batch_size)]

        self.logger.info(f"Processing {total_items} items in {len(batches)} batches")

        # Process batches
        for batch_idx, batch in enumerate(batches):
            # Create tasks for batch
            tasks = []
            for item in batch:
                task = self._process_with_semaphore(processor_func, item)
                tasks.append(task)

            # Execute batch
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Handle results
            for idx, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    self.logger.error(f"Failed to process item: {str(result)}")
                else:
                    results.append(result)

            # Update progress
            if progress_callback:
                processed = (batch_idx + 1) * batch_size
                progress_callback(min(processed, total_items), total_items)

        return results

    async def _process_with_semaphore(self, func: Any, item: Any) -> Any:
        """Process item with semaphore control."""
        async with self.semaphore:
            return await func(item)
