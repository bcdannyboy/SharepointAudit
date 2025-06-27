import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.core.discovery import DiscoveryModule
from src.utils.checkpoint_manager import CheckpointManager
from src.core.progress_tracker import ProgressTracker


@pytest.fixture
def mock_graph_client():
    return AsyncMock()


@pytest.fixture
def mock_sharepoint_client():
    return AsyncMock()


@pytest.fixture
def mock_checkpoint_manager():
    manager = AsyncMock(spec=CheckpointManager)
    return manager


@pytest.fixture
def discovery_module(mock_graph_client, mock_sharepoint_client, mock_checkpoint_manager, tmp_path):
    from src.database.repository import DatabaseRepository

    db_repo = DatabaseRepository(str(tmp_path / "audit.db"))
    asyncio.run(db_repo.initialize_database())
    module = DiscoveryModule(
        mock_graph_client,
        mock_sharepoint_client,
        db_repo,
        mock_checkpoint_manager,
    )
    # Speed up by disabling progress logging
    module.progress_tracker = ProgressTracker()
    return module


def test_discover_all_sites_uses_delta(discovery_module, mock_graph_client, mock_checkpoint_manager):
    async def run():
        result = SimpleNamespace(items=[], delta_token="TEST_TOKEN")
        mock_graph_client.get_all_sites_delta.return_value = result
        discovery_module.discover_site_content = AsyncMock()

        await discovery_module.run_discovery("test_run")

        mock_checkpoint_manager.save_checkpoint.assert_any_call("test_run", "sites_delta_token", "TEST_TOKEN")

    asyncio.run(run())


def test_discover_site_content_in_parallel(discovery_module):
    async def run():
        test_site = SimpleNamespace(id="1")
        async def fake_gather(*args, **kwargs):
            fake_gather.recorded = args
            results = []
            for coro in args:
                results.append(await coro)
            return results

        with patch("asyncio.gather", side_effect=fake_gather) as mock_gather:
            discovery_module._discover_libraries = AsyncMock(return_value=[])
            discovery_module._discover_lists = AsyncMock(return_value=[])
            discovery_module._discover_subsites = AsyncMock(return_value=[])
            await discovery_module.discover_site_content("run", test_site)
            assert mock_gather.call_count == 1
            assert len(fake_gather.recorded) > 1

    asyncio.run(run())


def test_discovery_resumes_from_checkpoint(discovery_module, mock_graph_client, mock_checkpoint_manager):
    async def run():
        async def restore_side_effect(run_id, key):
            if key == "site_site1_status":
                return "completed"
            return None

        mock_checkpoint_manager.restore_checkpoint.side_effect = restore_side_effect

        site1 = SimpleNamespace(id="site1")
        site2 = SimpleNamespace(id="site2")
        mock_graph_client.get_all_sites_delta.return_value = SimpleNamespace(items=[site1, site2], delta_token=None)

        discovery_module.discover_site_content = AsyncMock()

        await discovery_module.run_discovery("test_run")

        discovery_module.discover_site_content.assert_called_once_with("test_run", site2)

    asyncio.run(run())

