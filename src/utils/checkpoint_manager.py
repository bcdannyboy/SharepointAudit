import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from database.repository import DatabaseRepository


class CheckpointManager:
    """Manages checkpoints for resumable operations."""

    def __init__(self, db: DatabaseRepository) -> None:
        self.db = db
        self._cache: dict[str, Any] = {}

    async def save_checkpoint(
        self, run_id: str, checkpoint_type: str, state: Any
    ) -> None:
        await self.db.save_checkpoint(run_id, checkpoint_type, state)
        self._cache[f"{run_id}:{checkpoint_type}"] = state

    async def restore_checkpoint(
        self, run_id: str, checkpoint_type: str
    ) -> Optional[Any]:
        key = f"{run_id}:{checkpoint_type}"
        if key in self._cache:
            return self._cache[key]
        checkpoint = await self.db.get_latest_checkpoint(run_id, checkpoint_type)
        if checkpoint is not None:
            state = json.loads(checkpoint["checkpoint_data"])
            self._cache[key] = state
            return state
        return None

    async def cleanup_old_checkpoints(self, days: int = 7) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        await self.db.delete_checkpoints_before(cutoff)
