import logging

logger = logging.getLogger(__name__)


class ProgressTracker:
    """Provides simple progress logging."""

    def start(self, task_name: str) -> None:
        logger.info("[PROGRESS] Starting: %s", task_name)

    def finish(self, task_name: str, message: str = "Done") -> None:
        logger.info("[PROGRESS] Finished: %s - %s", task_name, message)

    def skip(self, task_name: str, reason: str) -> None:
        logger.info("[PROGRESS] Skipping: %s - %s", task_name, reason)

    def update(self, task_name: str, current: int, total: int) -> None:
        logger.info("[PROGRESS] %s: %d/%d (%.1f%%)", task_name, current, total, (current / total * 100) if total else 0)
