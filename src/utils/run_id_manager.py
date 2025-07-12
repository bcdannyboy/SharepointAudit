"""Run ID management utilities for easy tracking and recovery."""

import os
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class RunIDManager:
    """Manages run IDs and makes them easily accessible."""

    def __init__(self, base_dir: str = "."):
        self.base_dir = Path(base_dir)
        self.run_dir = self.base_dir / ".runs"
        self.run_dir.mkdir(exist_ok=True)

        # Files for tracking runs
        self.current_run_file = self.run_dir / "current_run.json"
        self.last_run_file = self.run_dir / "last_run.json"
        self.run_history_file = self.run_dir / "run_history.json"

    def save_current_run(self, run_id: str, config_path: Optional[str] = None) -> None:
        """Save the current run ID and metadata."""
        run_info = {
            "run_id": run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "config_path": config_path,
            "pid": os.getpid(),
            "status": "running"
        }

        # Save current run
        with open(self.current_run_file, 'w') as f:
            json.dump(run_info, f, indent=2)

        # Also save to history
        self._append_to_history(run_info)

        # Create a simple text file with just the run ID for easy access
        run_id_file = self.base_dir / f".current_run_id"
        with open(run_id_file, 'w') as f:
            f.write(run_id)

        logger.info(f"Run ID saved to {run_id_file}")

    def complete_current_run(self, status: str = "completed", error: Optional[str] = None) -> None:
        """Mark the current run as completed."""
        if not self.current_run_file.exists():
            return

        with open(self.current_run_file, 'r') as f:
            run_info = json.load(f)

        run_info["completed_at"] = datetime.now(timezone.utc).isoformat()
        run_info["status"] = status
        if error:
            run_info["error"] = error

        # Move to last run
        with open(self.last_run_file, 'w') as f:
            json.dump(run_info, f, indent=2)

        # Update history
        self._update_history(run_info["run_id"], run_info)

        # Remove current run file
        self.current_run_file.unlink()

        # Remove simple run ID file
        run_id_file = self.base_dir / f".current_run_id"
        if run_id_file.exists():
            run_id_file.unlink()

    def get_current_run(self) -> Optional[Dict[str, Any]]:
        """Get the current run information."""
        if not self.current_run_file.exists():
            return None

        with open(self.current_run_file, 'r') as f:
            return json.load(f)

    def get_last_run(self) -> Optional[Dict[str, Any]]:
        """Get the last completed run information."""
        if not self.last_run_file.exists():
            return None

        with open(self.last_run_file, 'r') as f:
            return json.load(f)

    def get_run_history(self, limit: int = 10) -> list[Dict[str, Any]]:
        """Get recent run history."""
        if not self.run_history_file.exists():
            return []

        with open(self.run_history_file, 'r') as f:
            history = json.load(f)

        # Sort by started_at descending
        sorted_runs = sorted(
            history.values(),
            key=lambda x: x.get("started_at", ""),
            reverse=True
        )

        return sorted_runs[:limit]

    def _append_to_history(self, run_info: Dict[str, Any]) -> None:
        """Append run to history file."""
        history = {}
        if self.run_history_file.exists():
            with open(self.run_history_file, 'r') as f:
                history = json.load(f)

        history[run_info["run_id"]] = run_info

        # Keep only last 100 runs
        if len(history) > 100:
            # Sort by started_at and keep most recent
            sorted_runs = sorted(
                history.items(),
                key=lambda x: x[1].get("started_at", ""),
                reverse=True
            )
            history = dict(sorted_runs[:100])

        with open(self.run_history_file, 'w') as f:
            json.dump(history, f, indent=2)

    def _update_history(self, run_id: str, run_info: Dict[str, Any]) -> None:
        """Update run in history file."""
        history = {}
        if self.run_history_file.exists():
            with open(self.run_history_file, 'r') as f:
                history = json.load(f)

        history[run_id] = run_info

        with open(self.run_history_file, 'w') as f:
            json.dump(history, f, indent=2)

    @staticmethod
    def format_run_id_banner(run_id: str, width: int = 80) -> str:
        """Format a prominent banner for the run ID."""
        border = "=" * width
        padding = "=" * 3

        lines = [
            border,
            f"{padding} RUN ID: {run_id} {padding}",
            f"{padding} Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} {padding}",
            border
        ]

        # Center each line
        centered_lines = []
        for line in lines:
            if len(line) < width:
                pad = (width - len(line)) // 2
                line = " " * pad + line + " " * (width - len(line) - pad)
            centered_lines.append(line)

        return "\n".join(centered_lines)
