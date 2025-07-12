"""Enhanced discovery module with live checkpoint saving."""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from core.discovery import DiscoveryModule
from utils.live_checkpoint_manager import LiveCheckpointManager
from api.graph_client import GraphAPIClient
from api.sharepoint_client import SharePointAPIClient
from database.repository import DatabaseRepository
from cache.cache_manager import CacheManager

logger = logging.getLogger(__name__)


class EnhancedDiscoveryModule(DiscoveryModule):
    """Enhanced discovery with live progress tracking and checkpoint saving."""

    def __init__(
        self,
        graph_client: GraphAPIClient,
        sp_client: SharePointAPIClient,
        db_repo: DatabaseRepository,
        cache: Optional[CacheManager] = None,
        checkpoints: Optional[LiveCheckpointManager] = None,
        max_concurrent_operations: int = 50,
        active_only: bool = False,
    ):
        # Use live checkpoint manager if provided
        if checkpoints and isinstance(checkpoints, LiveCheckpointManager):
            self.live_checkpoints = checkpoints
        else:
            # Create a new live checkpoint manager
            self.live_checkpoints = LiveCheckpointManager(db_repo)

        # Initialize parent with the live checkpoint manager
        super().__init__(
            graph_client=graph_client,
            sp_client=sp_client,
            db_repo=db_repo,
            cache=cache,
            checkpoints=self.live_checkpoints,
            max_concurrent_operations=max_concurrent_operations,
            active_only=active_only,
        )

        # Track progress counters for live updates
        self._site_progress_counters = {}
        self._last_progress_update = {}

    async def run_discovery(
        self, run_id: str, sites_to_process: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Run discovery with live checkpoint saving."""
        # Start live checkpoint manager
        await self.live_checkpoints.start()

        try:
            # Run parent discovery
            result = await super().run_discovery(run_id, sites_to_process)

            # Save final discovery state
            await self.live_checkpoints.save_checkpoint(
                run_id,
                "discovery_complete",
                {
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "discovered_counts": self.discovered_counts,
                    "sites_with_errors": list(self.sites_with_errors),
                },
                immediate=True
            )

            return result
        finally:
            # Stop live checkpoint manager
            await self.live_checkpoints.stop()

    async def _discover_library_contents_queue(self, site: Dict[str, Any], library: Dict[str, Any]) -> None:
        """Override to add live progress tracking."""
        # Handle both database record format and raw API format
        library_id = library.get('library_id') or library.get('id')
        site_id = site.get('site_id') or site.get('id')

        # Initialize progress counters
        progress_key = f"{site_id}_{library_id}"
        self._site_progress_counters[progress_key] = {
            'folders': 0,
            'files': 0,
            'started_at': datetime.now(timezone.utc).isoformat()
        }

        # Run parent implementation
        await super()._discover_library_contents_queue(site, library)

        # Save final progress
        run_id = getattr(self, '_current_run_id', 'unknown')
        await self.live_checkpoints.save_discovery_progress(
            run_id=run_id,
            site_id=site_id,
            library_id=library_id,
            progress_data={
                **self._site_progress_counters[progress_key],
                'completed_at': datetime.now(timezone.utc).isoformat(),
                'status': 'completed'
            }
        )

    async def _save_folders_batch(self, folders: List[Dict[str, Any]]) -> None:
        """Override to track folder discovery progress."""
        if not folders:
            return

        # Run parent implementation
        await super()._save_folders_batch(folders)

        # Update progress counters and save checkpoint periodically
        for folder in folders:
            site_id = folder.get('site_id')
            library_id = folder.get('library_id')
            if site_id and library_id:
                progress_key = f"{site_id}_{library_id}"
                if progress_key in self._site_progress_counters:
                    self._site_progress_counters[progress_key]['folders'] += 1

                    # Save progress every 100 folders or 60 seconds
                    last_update = self._last_progress_update.get(progress_key, 0)
                    current_time = datetime.now(timezone.utc).timestamp()
                    folder_count = self._site_progress_counters[progress_key]['folders']

                    if folder_count % 100 == 0 or (current_time - last_update) > 60:
                        run_id = getattr(self, '_current_run_id', 'unknown')
                        await self.live_checkpoints.save_discovery_progress(
                            run_id=run_id,
                            site_id=site_id,
                            library_id=library_id,
                            progress_data=self._site_progress_counters[progress_key]
                        )
                        self._last_progress_update[progress_key] = current_time
                        logger.debug(f"Saved progress for {progress_key}: {folder_count} folders")

    async def _save_files_batch(self, files: List[Dict[str, Any]]) -> None:
        """Override to track file discovery progress."""
        if not files:
            return

        # Run parent implementation
        await super()._save_files_batch(files)

        # Update progress counters and save checkpoint periodically
        for file in files:
            site_id = file.get('site_id')
            library_id = file.get('library_id')
            if site_id and library_id:
                progress_key = f"{site_id}_{library_id}"
                if progress_key in self._site_progress_counters:
                    self._site_progress_counters[progress_key]['files'] += 1

                    # Save progress every 100 files or 60 seconds
                    last_update = self._last_progress_update.get(progress_key, 0)
                    current_time = datetime.now(timezone.utc).timestamp()
                    file_count = self._site_progress_counters[progress_key]['files']

                    if file_count % 100 == 0 or (current_time - last_update) > 60:
                        run_id = getattr(self, '_current_run_id', 'unknown')
                        await self.live_checkpoints.save_discovery_progress(
                            run_id=run_id,
                            site_id=site_id,
                            library_id=library_id,
                            progress_data=self._site_progress_counters[progress_key]
                        )
                        self._last_progress_update[progress_key] = current_time
                        logger.debug(f"Saved progress for {progress_key}: {file_count} files")

    async def discover_all_sites(self, run_id: str) -> List[Dict[str, Any]]:
        """Override to store run_id for progress tracking."""
        self._current_run_id = run_id
        return await super().discover_all_sites(run_id)

    async def get_crash_recovery_summary(self, run_id: str) -> Dict[str, Any]:
        """Get a summary of where discovery left off for crash recovery."""
        summary = await self.live_checkpoints.get_discovery_progress_summary(run_id)

        # Get completed sites
        completed_sites = set()
        pending_sites = set()

        for checkpoint_type, data in summary.get('checkpoints', {}).items():
            if data.get('status') == 'completed':
                completed_sites.add(data.get('site_id'))
            else:
                pending_sites.add(data.get('site_id'))

        return {
            'run_id': run_id,
            'completed_sites': list(completed_sites),
            'pending_sites': list(pending_sites),
            'total_folders_discovered': sum(
                cp.get('folders', 0)
                for cp in summary.get('checkpoints', {}).values()
            ),
            'total_files_discovered': sum(
                cp.get('files', 0)
                for cp in summary.get('checkpoints', {}).values()
            ),
            'last_update': summary.get('last_update'),
            'raw_summary': summary
        }
