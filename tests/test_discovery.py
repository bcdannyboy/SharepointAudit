import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
    manager.restore_checkpoint.return_value = None
    manager.save_checkpoint.return_value = None
    return manager


@pytest.fixture
def mock_db_repo():
    repo = AsyncMock()
    repo.bulk_insert.return_value = 1
    return repo


@pytest.fixture
def discovery_module(
    mock_graph_client, mock_sharepoint_client, mock_checkpoint_manager, mock_db_repo
):
    module = DiscoveryModule(
        mock_graph_client,
        mock_sharepoint_client,
        mock_db_repo,
        cache=None,
        checkpoints=mock_checkpoint_manager,
        active_only=False,
    )
    # Speed up by disabling progress logging
    module.progress_tracker = ProgressTracker()
    return module


def test_discover_all_sites_uses_delta(
    discovery_module, mock_graph_client, mock_checkpoint_manager
):
    async def run():
        # Create a result object with sites
        sites = [
            SimpleNamespace(
                id="site1",
                displayName="Test Site 1",
                webUrl="https://tenant.sharepoint.com/sites/site1",
                description="Test site 1",
                createdDateTime="2023-01-01T00:00:00Z",
                lastModifiedDateTime="2023-01-02T00:00:00Z",
            )
        ]
        result = SimpleNamespace(items=sites, delta_token="TEST_TOKEN")

        mock_graph_client.get_all_sites_delta.return_value = result
        discovery_module.discover_site_content = AsyncMock()

        await discovery_module.run_discovery("test_run")

        # Verify delta token was saved
        mock_checkpoint_manager.save_checkpoint.assert_any_call(
            "test_run", "sites_delta_token", "TEST_TOKEN"
        )

        # Verify sites were saved to database
        assert discovery_module.db_repo.bulk_insert.called
        call_args = discovery_module.db_repo.bulk_insert.call_args_list[0]
        assert call_args[0][0] == "sites"  # Table name
        assert len(call_args[0][1]) == 1  # One site record

    asyncio.run(run())


def test_discover_site_content_in_parallel(discovery_module):
    async def run():
        test_site = SimpleNamespace(id="1", title="Test Site")

        # Track which methods were called
        called_methods = []

        async def track_libraries(*args):
            called_methods.append("libraries")
            return []

        async def track_lists(*args):
            called_methods.append("lists")
            return []

        async def track_subsites(*args):
            called_methods.append("subsites")
            return []

        discovery_module._discover_libraries = track_libraries
        discovery_module._discover_lists = track_lists
        discovery_module._discover_subsites = track_subsites

        await discovery_module.discover_site_content("run", test_site)

        # All three methods should have been called
        assert "libraries" in called_methods
        assert "lists" in called_methods
        assert "subsites" in called_methods

    asyncio.run(run())


def test_discovery_resumes_from_checkpoint(
    discovery_module, mock_graph_client, mock_checkpoint_manager
):
    async def run():
        async def restore_side_effect(run_id, key):
            if key == "site_site1_status":
                return "completed"
            return None

        mock_checkpoint_manager.restore_checkpoint.side_effect = restore_side_effect

        site1 = SimpleNamespace(id="site1", title="Site 1", displayName="Site 1")
        site2 = SimpleNamespace(id="site2", title="Site 2", displayName="Site 2")
        result = SimpleNamespace(items=[site1, site2], delta_token=None)

        mock_graph_client.get_all_sites_delta.return_value = result

        # Track which sites actually get their content discovered
        sites_processed = []

        # Mock the internal discovery methods instead of the main method
        async def mock_discover_libraries(site):
            sites_processed.append(site.id)
            return []

        discovery_module._discover_libraries = mock_discover_libraries
        discovery_module._discover_lists = AsyncMock(return_value=[])
        discovery_module._discover_subsites = AsyncMock(return_value=[])

        await discovery_module.run_discovery("test_run")

        # Should only process site2 since site1 is marked as completed
        assert len(sites_processed) == 1
        assert sites_processed[0] == "site2"

    asyncio.run(run())


def test_run_discovery_filters_sites(discovery_module):
    async def run():
        site1 = SimpleNamespace(
            id="site1", webUrl="https://contoso.sharepoint.com/sites/one"
        )
        site2 = SimpleNamespace(
            id="site2", webUrl="https://contoso.sharepoint.com/sites/two"
        )

        discovery_module.discover_all_sites = AsyncMock(return_value=[site1, site2])
        discovery_module._discover_site_with_semaphore = AsyncMock()

        await discovery_module.run_discovery(
            "run", ["https://contoso.sharepoint.com/sites/two/"]
        )

        # Should only process the matching site
        discovery_module._discover_site_with_semaphore.assert_called_once()
        processed_site = discovery_module._discover_site_with_semaphore.call_args[0][1]
        assert processed_site == site2

    asyncio.run(run())


def test_discover_libraries_saves_to_database(discovery_module, mock_graph_client):
    async def run():
        test_site = SimpleNamespace(id="site123")

        # Mock Graph API response
        mock_graph_client.get_with_retry.return_value = {
            "value": [
                {
                    "id": "lib1",
                    "name": "Documents",
                    "description": "Document library",
                    "createdDateTime": "2023-01-01T00:00:00Z",
                },
                {
                    "id": "lib2",
                    "name": "Site Assets",
                    "description": "Site assets library",
                    "createdDateTime": "2023-01-02T00:00:00Z",
                },
            ]
        }

        libraries = await discovery_module._discover_libraries(test_site)

        # Verify Graph API was called correctly
        mock_graph_client.get_with_retry.assert_called_once_with(
            "https://graph.microsoft.com/v1.0/sites/site123/drives"
        )

        # Verify libraries were saved to database
        discovery_module.db_repo.bulk_insert.assert_called_once()
        call_args = discovery_module.db_repo.bulk_insert.call_args
        assert call_args[0][0] == "libraries"  # Table name
        assert len(call_args[0][1]) == 2  # Two libraries

        # Verify library data structure
        saved_libraries = call_args[0][1]
        assert saved_libraries[0]["library_id"] == "lib1"
        assert saved_libraries[0]["name"] == "Documents"
        assert saved_libraries[0]["site_id"] == "site123"

        # Verify return value
        assert len(libraries) == 2

    asyncio.run(run())


def test_discover_folder_contents_with_pagination(discovery_module, mock_graph_client):
    async def run():
        test_site = SimpleNamespace(id="site123")
        test_library = {"id": "lib123", "name": "Documents"}

        # Mock paginated responses
        page1_response = {
            "value": [
                {"id": "file1", "name": "doc1.pdf", "size": 1000},
                {"id": "folder1", "name": "Subfolder", "folder": {}},
            ],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/sites/site123/drives/lib123/root/children?$skiptoken=abc",
        }

        page2_response = {
            "value": [
                {"id": "file2", "name": "doc2.docx", "size": 2000},
            ]
        }

        mock_graph_client.get_with_retry.side_effect = [page1_response, page2_response]

        # Mock recursive folder discovery to avoid infinite recursion
        discovery_module._discover_folder_contents = AsyncMock(
            side_effect=[None]  # Stop recursion
        )

        # Manually call the real method for the root folder
        real_method = DiscoveryModule._discover_folder_contents
        await real_method(discovery_module, test_site, test_library, None, "/")

        # Verify API calls
        assert mock_graph_client.get_with_retry.call_count == 2

        # Verify database inserts
        assert (
            discovery_module.db_repo.bulk_insert.call_count == 2
        )  # One for folders, one for files

        # Check folder insert
        folder_call = [
            call
            for call in discovery_module.db_repo.bulk_insert.call_args_list
            if call[0][0] == "folders"
        ][0]
        assert len(folder_call[0][1]) == 1  # One folder

        # Check file insert
        file_call = [
            call
            for call in discovery_module.db_repo.bulk_insert.call_args_list
            if call[0][0] == "files"
        ][0]
        assert len(file_call[0][1]) == 2  # Two files

    asyncio.run(run())


def test_error_handling_continues_discovery(discovery_module, mock_graph_client):
    async def run():
        # Create sites where one will fail
        sites = [
            SimpleNamespace(id="site1", title="Site 1", displayName="Site 1"),
            SimpleNamespace(id="site2", title="Site 2", displayName="Site 2"),
            SimpleNamespace(id="site3", title="Site 3", displayName="Site 3"),
        ]
        result = SimpleNamespace(items=sites, delta_token=None)

        mock_graph_client.get_all_sites_delta.return_value = result

        # Make site2 fail
        async def discover_site_content_side_effect(run_id, site):
            if site.id == "site2":
                raise Exception("Simulated error")

        discovery_module.discover_site_content = AsyncMock(
            side_effect=discover_site_content_side_effect
        )

        # Should not raise exception
        await discovery_module.run_discovery("test_run")

        # All sites should have been attempted
        assert discovery_module.discover_site_content.call_count == 3

    asyncio.run(run())


def test_semaphore_limits_concurrent_operations(discovery_module, mock_graph_client):
    async def run():
        # Create many sites to test semaphore limiting
        sites = [
            SimpleNamespace(id=f"site{i}", title=f"Site {i}", displayName=f"Site {i}")
            for i in range(50)
        ]
        result = SimpleNamespace(items=sites, delta_token=None)

        mock_graph_client.get_all_sites_delta.return_value = result

        # Track concurrent executions
        max_concurrent = 0
        current_concurrent = 0

        async def track_concurrency(run_id, site):
            nonlocal max_concurrent, current_concurrent
            current_concurrent += 1
            max_concurrent = max(max_concurrent, current_concurrent)
            await asyncio.sleep(0.01)  # Simulate work
            current_concurrent -= 1

        discovery_module.discover_site_content = track_concurrency

        await discovery_module.run_discovery("test_run")

        # Verify semaphore limited concurrency
        assert (
            max_concurrent <= discovery_module.site_semaphore._value + 1
        )  # Allow for small timing variance

    asyncio.run(run())
