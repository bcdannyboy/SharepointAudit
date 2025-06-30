import asyncio
import pytest
from unittest.mock import AsyncMock

from src.core.permissions import PermissionAnalyzer
from src.core.concurrency import ConcurrencyManager
from src.cache.cache_manager import CacheManager
from src.api.graph_client import GraphAPIClient
from src.api.sharepoint_client import SharePointAPIClient
from src.database.repository import DatabaseRepository


@pytest.fixture
def permission_analyzer_with_cache():
    graph_client = AsyncMock(spec=GraphAPIClient)
    sp_client = AsyncMock(spec=SharePointAPIClient)
    db_repo = AsyncMock(spec=DatabaseRepository)
    db_repo.get_cache_entry.return_value = None
    cache = CacheManager(db_repo)
    analyzer = PermissionAnalyzer(
        graph_client=graph_client,
        sp_client=sp_client,
        db_repo=db_repo,
        cache_manager=cache,
    )
    return analyzer


@pytest.fixture
def concurrency_manager():
    return ConcurrencyManager(max_api_calls=5, max_db_connections=5)


@pytest.mark.asyncio
async def test_cache_reduces_api_calls(permission_analyzer_with_cache):
    analyzer = permission_analyzer_with_cache
    analyzer.graph_client.expand_group_members_transitive.return_value = []
    analyzer.graph_client.get_group_info.return_value = {"displayName": "Test"}

    await analyzer.expand_group_permissions("group1")
    analyzer.graph_client.expand_group_members_transitive.assert_called_once()

    await analyzer.expand_group_permissions("group1")
    analyzer.graph_client.expand_group_members_transitive.assert_called_once()


@pytest.mark.asyncio
async def test_concurrency_manager_limits_tasks(concurrency_manager):
    concurrency_manager.api_semaphore = asyncio.Semaphore(5)

    tasks = [
        concurrency_manager.run_api_task(asyncio.sleep(0.01))
        for _ in range(10)
    ]

    start = asyncio.get_event_loop().time()
    await asyncio.gather(*tasks)
    duration = asyncio.get_event_loop().time() - start

    assert duration >= 0.02
