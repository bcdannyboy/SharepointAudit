from __future__ import annotations

import asyncio
from typing import Any, Iterable

from ..api.graph_client import GraphAPIClient
from ..api.sharepoint_client import SharePointAPIClient
from ..database.repository import DatabaseRepository
from .progress_tracker import ProgressTracker
from ..utils.checkpoint_manager import CheckpointManager


class DiscoveryModule:
    """Discovers SharePoint sites and their contents."""

    def __init__(
        self,
        graph_client: GraphAPIClient,
        sp_client: SharePointAPIClient,
        db_repo: DatabaseRepository,
        checkpoint_manager: CheckpointManager,
    ) -> None:
        self.graph_client = graph_client
        self.sp_client = sp_client
        self.db_repo = db_repo
        self.checkpoints = checkpoint_manager
        self.progress_tracker = ProgressTracker()

    async def run_discovery(self, run_id: str) -> None:
        sites_state = await self.checkpoints.restore_checkpoint(run_id, "sites_delta_token")
        result = await self.graph_client.get_all_sites_delta(sites_state)
        if getattr(result, "delta_token", None):
            await self.checkpoints.save_checkpoint(run_id, "sites_delta_token", result.delta_token)
        for site in result.items:
            key = f"site_{getattr(site, 'id', '')}_status"
            status = await self.checkpoints.restore_checkpoint(run_id, key)
            if status == "completed":
                self.progress_tracker.skip(key, "Already processed")
                continue
            await self.discover_site_content(run_id, site)
            await self.checkpoints.save_checkpoint(run_id, key, "completed")

    async def discover_site_content(self, run_id: str, site: Any) -> None:
        """Discover lists, libraries and subsites for a single site."""
        tasks = [
            self._discover_libraries(site),
            self._discover_lists(site),
            self._discover_subsites(run_id, site),
        ]
        await asyncio.gather(*tasks)

    async def _discover_libraries(self, site: Any) -> Iterable[Any]:
        """Enumerate document libraries for the given site."""
        site_id = getattr(site, "id", None)
        if site_id is None:
            return []

        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
        data = await self.graph_client.get_with_retry(url)
        libraries = data.get("value", [])
        records = [
            {
                "library_id": lib.get("id"),
                "site_id": site_id,
                "name": lib.get("name"),
            }
            for lib in libraries
        ]
        if records:
            await self.db_repo.bulk_insert("libraries", records)
        return libraries

    async def _discover_lists(self, site: Any) -> Iterable[Any]:
        """Enumerate lists for the given site."""
        site_id = getattr(site, "id", None)
        if site_id is None:
            return []

        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        data = await self.graph_client.get_with_retry(url)
        return data.get("value", [])

    async def _discover_subsites(self, run_id: str, site: Any) -> Iterable[Any]:
        """Discover subsites and recursively process their contents."""
        site_id = getattr(site, "id", None)
        if site_id is None:
            return []

        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/sites"
        data = await self.graph_client.get_with_retry(url)
        subsites = data.get("value", [])
        for sub in subsites:
            await self.discover_site_content(run_id, sub)
        return subsites

