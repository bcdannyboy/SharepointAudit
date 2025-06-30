"""Metrics collection and tracking for the audit pipeline."""

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class PipelineMetrics:
    """Collects metrics for each pipeline stage."""

    # Stage timing metrics
    stage_durations: Dict[str, float] = field(default_factory=dict)
    stage_start_times: Dict[str, float] = field(default_factory=dict)
    stage_item_counts: Dict[str, int] = field(default_factory=dict)
    stage_error_counts: Dict[str, int] = field(default_factory=dict)

    # Overall pipeline metrics
    total_duration: float = 0.0
    total_start_time: Optional[float] = None
    items_processed: int = 0
    items_failed: int = 0

    # Custom metrics that stages can set
    custom_metrics: Dict[str, Any] = field(default_factory=dict)

    # Performance metrics
    throughput_items_per_second: float = 0.0
    average_item_processing_time: float = 0.0

    def start_timer(self) -> None:
        """Start the overall pipeline timer."""
        self.total_start_time = time.time()
        logger.debug("Pipeline timer started")

    def stop_timer(self) -> None:
        """Stop the overall pipeline timer and calculate duration."""
        if self.total_start_time:
            self.total_duration = time.time() - self.total_start_time

            # Calculate throughput
            if self.total_duration > 0 and self.items_processed > 0:
                self.throughput_items_per_second = self.items_processed / self.total_duration
                self.average_item_processing_time = self.total_duration / self.items_processed

            logger.debug(f"Pipeline timer stopped. Duration: {self.total_duration:.2f}s")

    @contextmanager
    def measure_stage(self, stage_name: str):
        """Context manager to measure the duration of a stage."""
        start_time = time.time()
        self.stage_start_times[stage_name] = start_time

        logger.debug(f"Stage '{stage_name}' started")

        try:
            yield
        finally:
            duration = time.time() - start_time
            self.stage_durations[stage_name] = duration

            logger.debug(f"Stage '{stage_name}' completed in {duration:.2f}s")

    def record_stage_items(self, stage_name: str, count: int) -> None:
        """Record the number of items processed by a stage."""
        self.stage_item_counts[stage_name] = count
        self.items_processed += count

    def record_stage_error(self, stage_name: str) -> None:
        """Record an error in a stage."""
        if stage_name not in self.stage_error_counts:
            self.stage_error_counts[stage_name] = 0
        self.stage_error_counts[stage_name] += 1
        self.items_failed += 1

    def set_custom_metric(self, name: str, value: Any) -> None:
        """Set a custom metric value."""
        self.custom_metrics[name] = value

    def get_stage_metrics(self, stage_name: str) -> Dict[str, Any]:
        """Get all metrics for a specific stage."""
        return {
            "duration": self.stage_durations.get(stage_name, 0.0),
            "items_processed": self.stage_item_counts.get(stage_name, 0),
            "errors": self.stage_error_counts.get(stage_name, 0),
            "throughput": self._calculate_stage_throughput(stage_name)
        }

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all pipeline metrics."""
        summary = {
            "total_duration_seconds": self.total_duration,
            "total_items_processed": self.items_processed,
            "total_items_failed": self.items_failed,
            "success_rate": self._calculate_success_rate(),
            "throughput_items_per_second": self.throughput_items_per_second,
            "average_item_processing_time_seconds": self.average_item_processing_time,
            "stages": {}
        }

        # Add per-stage metrics
        for stage_name in self.stage_durations:
            summary["stages"][stage_name] = self.get_stage_metrics(stage_name)

        # Add custom metrics
        if self.custom_metrics:
            summary["custom_metrics"] = self.custom_metrics

        return summary

    def log_summary(self) -> None:
        """Log a summary of the pipeline metrics."""
        summary = self.get_summary()

        logger.info("=" * 60)
        logger.info("Pipeline Execution Summary")
        logger.info("=" * 60)
        logger.info(f"Total Duration: {summary['total_duration_seconds']:.2f} seconds")
        logger.info(f"Items Processed: {summary['total_items_processed']:,}")
        logger.info(f"Items Failed: {summary['total_items_failed']:,}")
        logger.info(f"Success Rate: {summary['success_rate']:.1f}%")
        logger.info(f"Throughput: {summary['throughput_items_per_second']:.2f} items/second")

        logger.info("\nStage Performance:")
        logger.info("-" * 40)

        for stage_name, metrics in summary["stages"].items():
            logger.info(
                f"  {stage_name}: {metrics['duration']:.2f}s, "
                f"{metrics['items_processed']:,} items, "
                f"{metrics['errors']} errors"
            )

        if summary.get("custom_metrics"):
            logger.info("\nCustom Metrics:")
            logger.info("-" * 40)
            for name, value in summary["custom_metrics"].items():
                if isinstance(value, float):
                    logger.info(f"  {name}: {value:.2f}")
                else:
                    logger.info(f"  {name}: {value}")

        logger.info("=" * 60)

    def _calculate_success_rate(self) -> float:
        """Calculate the overall success rate."""
        total = self.items_processed + self.items_failed
        if total == 0:
            return 100.0
        return (self.items_processed / total) * 100

    def _calculate_stage_throughput(self, stage_name: str) -> float:
        """Calculate throughput for a specific stage."""
        duration = self.stage_durations.get(stage_name, 0)
        items = self.stage_item_counts.get(stage_name, 0)

        if duration > 0 and items > 0:
            return items / duration
        return 0.0
