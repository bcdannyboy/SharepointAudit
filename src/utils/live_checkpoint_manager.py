"""Enhanced checkpoint manager with live progress saving for crash recovery."""

import json
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set
from collections import defaultdict

from database.repository import DatabaseRepository

logger = logging.getLogger(__name__)


class LiveCheckpointManager:
    """
    Enhanced checkpoint manager that saves progress in real-time.

    Features:
    - Automatic periodic saving
    - Batch checkpoint updates for efficiency
    - Progress tracking for discovery operations
    - Crash recovery support
    """

    def __init__(
        self,
        db: DatabaseRepository,
        save_interval: int = 30,  # Save every 30 seconds
        batch_size: int = 50      # Batch up to 50 updates
    ) -> None:
        self.db = db
        self.save_interval = save_interval
        self.batch_size = batch_size
        self._cache: Dict[str, Any] = {}
        self._pending_updates: Dict[str, Any] = {}
        self._save_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

        # Track discovery progress
        self._discovery_progress: Dict[str, Dict[str, Any]] = defaultdict(dict)

    async def start(self) -> None:
        """Start the periodic save task."""
        if self._save_task is None:
            self._save_task = asyncio.create_task(self._periodic_save())
            logger.info(f"Started live checkpoint manager with {self.save_interval}s save interval")

    async def stop(self) -> None:
        """Stop the periodic save task and save any pending updates."""
        if self._save_task:
            self._save_task.cancel()
            try:
                await self._save_task
            except asyncio.CancelledError:
                pass
            self._save_task = None

        # Save any remaining updates
        await self._flush_pending_updates()
        logger.info("Stopped live checkpoint manager")

    async def _periodic_save(self) -> None:
        """Periodically save pending checkpoint updates."""
        while True:
            try:
                await asyncio.sleep(self.save_interval)
                await self._flush_pending_updates()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic save: {e}", exc_info=True)

    async def _flush_pending_updates(self) -> None:
        """Save all pending checkpoint updates."""
        async with self._lock:
            if not self._pending_updates:
                return

            updates = list(self._pending_updates.items())
            self._pending_updates.clear()

        # Save updates in batches
        for i in range(0, len(updates), self.batch_size):
            batch = updates[i:i + self.batch_size]
            try:
                for key, data in batch:
                    run_id, checkpoint_type = key.split(':', 1)
                    await self.db.save_checkpoint(run_id, checkpoint_type, data)
                    self._cache[key] = data

                logger.debug(f"Saved batch of {len(batch)} checkpoints")
            except Exception as e:
                logger.error(f"Error saving checkpoint batch: {e}", exc_info=True)

    async def save_checkpoint(
        self,
        run_id: str,
        checkpoint_type: str,
        state: Any,
        immediate: bool = False
    ) -> None:
        """
        Save a checkpoint (queued for batch save unless immediate=True).

        Args:
            run_id: The audit run ID
            checkpoint_type: Type of checkpoint (e.g., 'pipeline_stage', 'site_progress')
            state: The state to save
            immediate: If True, save immediately instead of batching
        """
        key = f"{run_id}:{checkpoint_type}"

        if immediate:
            await self.db.save_checkpoint(run_id, checkpoint_type, state)
            self._cache[key] = state
            logger.debug(f"Saved immediate checkpoint: {checkpoint_type}")
        else:
            async with self._lock:
                self._pending_updates[key] = state

                # Force save if batch size reached
                if len(self._pending_updates) >= self.batch_size:
                    await self._flush_pending_updates()

    async def save_discovery_progress(
        self,
        run_id: str,
        site_id: str,
        library_id: Optional[str] = None,
        progress_data: Dict[str, Any] = None
    ) -> None:
        """
        Save discovery progress for a site/library.

        Args:
            run_id: The audit run ID
            site_id: Site being processed
            library_id: Library being processed (optional)
            progress_data: Progress information (folders/files discovered, etc.)
        """
        checkpoint_type = f"discovery_progress_{site_id}"
        if library_id:
            checkpoint_type += f"_{library_id}"

        progress = self._discovery_progress[checkpoint_type]
        progress.update({
            'site_id': site_id,
            'library_id': library_id,
            'last_updated': datetime.now(timezone.utc).isoformat(),
            **(progress_data or {})
        })

        await self.save_checkpoint(run_id, checkpoint_type, progress)

    async def restore_checkpoint(
        self,
        run_id: str,
        checkpoint_type: str
    ) -> Optional[Any]:
        """Restore a checkpoint from cache or database."""
        key = f"{run_id}:{checkpoint_type}"

        # Check cache first
        if key in self._cache:
            return self._cache[key]

        # Check pending updates
        async with self._lock:
            if key in self._pending_updates:
                return self._pending_updates[key]

        # Load from database
        checkpoint = await self.db.get_latest_checkpoint(run_id, checkpoint_type)
        if checkpoint is not None:
            state = json.loads(checkpoint["checkpoint_data"])
            self._cache[key] = state
            return state

        return None

    async def get_discovery_progress_summary(self, run_id: str) -> Dict[str, Any]:
        """Get a summary of discovery progress for crash recovery."""
        # Get all discovery progress checkpoints
        progress_checkpoints = {}

        # This would need a new repository method to get all checkpoints by pattern
        # For now, we'll reconstruct from cache
        for key in list(self._cache.keys()) + list(self._pending_updates.keys()):
            if key.startswith(f"{run_id}:discovery_progress_"):
                checkpoint_type = key.split(':', 1)[1]
                data = self._cache.get(key) or self._pending_updates.get(key)
                if data:
                    progress_checkpoints[checkpoint_type] = data

        return {
            'total_sites': len(set(cp.get('site_id') for cp in progress_checkpoints.values())),
            'checkpoints': progress_checkpoints,
            'last_update': max(
                (cp.get('last_updated') for cp in progress_checkpoints.values()),
                default=None
            )
        }

    async def cleanup_old_checkpoints(self, days: int = 7) -> None:
        """Clean up checkpoints older than specified days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        await self.db.delete_checkpoints_before(cutoff)

        # Clear cache entries for old checkpoints
        self._cache.clear()
        logger.info(f"Cleaned up checkpoints older than {days} days")
