"""Tests for cache functionality."""

import pytest
import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.cache.cache_manager import CacheManager, InMemoryCache
from src.database.repository import DatabaseRepository


class TestInMemoryCache:
    """Test cases for InMemoryCache class."""

    def test_basic_get_set(self):
        """Test basic get and set operations."""
        cache = InMemoryCache(max_size=10)

        # Test set and get
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        # Test missing key
        assert cache.get("missing") is None

    def test_ttl_expiration(self):
        """Test TTL expiration."""
        cache = InMemoryCache(max_size=10)

        # Set with short TTL
        cache.set("key1", "value1", ttl=1)
        assert cache.get("key1") == "value1"

        # Wait for expiration
        time.sleep(1.1)
        assert cache.get("key1") is None

    def test_lru_eviction(self):
        """Test LRU eviction when cache is full."""
        cache = InMemoryCache(max_size=3)

        # Fill cache
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # Access key1 to make it recently used
        cache.get("key1")

        # Add new item, should evict key2 (least recently used)
        cache.set("key4", "value4")

        assert cache.get("key1") == "value1"  # Still present
        assert cache.get("key2") is None      # Evicted
        assert cache.get("key3") == "value3"  # Still present
        assert cache.get("key4") == "value4"  # New item

    def test_delete_operation(self):
        """Test delete operation."""
        cache = InMemoryCache()

        cache.set("key1", "value1")
        assert cache.delete("key1") is True
        assert cache.get("key1") is None

        # Delete non-existent key
        assert cache.delete("missing") is False

    def test_clear_operation(self):
        """Test clear operation."""
        cache = InMemoryCache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        cache.clear()

        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.stats()["size"] == 0

    def test_cache_stats(self):
        """Test cache statistics."""
        cache = InMemoryCache(max_size=10)

        # Initial stats
        stats = cache.stats()
        assert stats["size"] == 0
        assert stats["hits"] == 0
        assert stats["misses"] == 0

        # Add items and test
        cache.set("key1", "value1")
        cache.get("key1")  # Hit
        cache.get("key2")  # Miss

        stats = cache.stats()
        assert stats["size"] == 1
        assert stats["hits"] == 1
        assert stats["misses"] == 1
        assert stats["hit_rate"] == 0.5

    def test_thread_safety(self):
        """Test thread safety of cache operations."""
        cache = InMemoryCache(max_size=100)

        def worker(start, end):
            for i in range(start, end):
                cache.set(f"key{i}", f"value{i}")
                cache.get(f"key{i}")

        import threading
        threads = []
        for i in range(4):
            t = threading.Thread(target=worker, args=(i*25, (i+1)*25))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Verify all items were set
        assert cache.stats()["size"] == 100


@pytest.fixture
def mock_db_repo():
    """Create a mock database repository."""
    repo = AsyncMock(spec=DatabaseRepository)
    return repo


@pytest.fixture
def cache_manager(mock_db_repo):
    """Create a cache manager with mocked database."""
    return CacheManager(mock_db_repo, memory_cache_size=100)


class TestCacheManager:
    """Test cases for CacheManager class."""

    @pytest.mark.asyncio
    async def test_memory_cache_hit(self, cache_manager):
        """Test getting value from memory cache."""
        # Set in memory cache
        await cache_manager.set("test_key", {"data": "value"})

        # Get should hit memory cache
        result = await cache_manager.get("test_key")
        assert result == {"data": "value"}

        # Database should not be called
        cache_manager.db_repo.get_cache_entry.assert_not_called()

    @pytest.mark.asyncio
    async def test_database_cache_fallback(self, cache_manager, mock_db_repo):
        """Test falling back to database cache on memory miss."""
        # Mock database response
        mock_db_repo.get_cache_entry.return_value = {
            "key": "sp_audit:test_key",
            "value": json.dumps({"data": "db_value"}),
            "expires_at": datetime.now(timezone.utc) + timedelta(hours=1)
        }

        # Get should check database
        result = await cache_manager.get("test_key")
        assert result == {"data": "db_value"}

        # Verify database was called
        mock_db_repo.get_cache_entry.assert_called_once_with("sp_audit:test_key")

    @pytest.mark.asyncio
    async def test_expired_database_entry(self, cache_manager, mock_db_repo):
        """Test handling of expired database cache entries."""
        # Mock expired database response
        mock_db_repo.get_cache_entry.return_value = {
            "key": "sp_audit:test_key",
            "value": json.dumps({"data": "expired"}),
            "expires_at": datetime.now(timezone.utc) - timedelta(hours=1)  # Expired
        }

        # Get should return None and delete expired entry
        result = await cache_manager.get("test_key")
        assert result is None

        # Verify expired entry was deleted
        mock_db_repo.delete_cache_entry.assert_called_once_with("sp_audit:test_key")

    @pytest.mark.asyncio
    async def test_set_stores_in_both_caches(self, cache_manager, mock_db_repo):
        """Test that set stores in both memory and database cache."""
        # Set value with TTL
        await cache_manager.set("test_key", {"data": "value"}, ttl=3600)

        # Verify memory cache has it
        assert cache_manager.memory_cache.get("sp_audit:test_key") == {"data": "value"}

        # Verify database was called
        mock_db_repo.set_cache_entry.assert_called_once()
        call_args = mock_db_repo.set_cache_entry.call_args
        assert call_args[1]["key"] == "sp_audit:test_key"
        assert json.loads(call_args[1]["value"]) == {"data": "value"}
        assert call_args[1]["expires_at"] is not None

    @pytest.mark.asyncio
    async def test_delete_from_both_caches(self, cache_manager, mock_db_repo):
        """Test that delete removes from both caches."""
        # Set value first
        await cache_manager.set("test_key", {"data": "value"})

        # Delete
        mock_db_repo.delete_cache_entry.return_value = True
        result = await cache_manager.delete("test_key")
        assert result is True

        # Verify deleted from memory cache
        assert cache_manager.memory_cache.get("sp_audit:test_key") is None

        # Verify database delete was called
        mock_db_repo.delete_cache_entry.assert_called_once_with("sp_audit:test_key")

    @pytest.mark.asyncio
    async def test_clear_all_caches(self, cache_manager, mock_db_repo):
        """Test clearing all cache entries."""
        # Add some items
        await cache_manager.set("key1", "value1")
        await cache_manager.set("key2", "value2")

        # Clear
        await cache_manager.clear()

        # Verify memory cache is empty
        assert cache_manager.memory_cache.stats()["size"] == 0

        # Verify database clear was called
        mock_db_repo.clear_cache.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_operations(self, cache_manager, mock_db_repo):
        """Test batch get and set operations."""
        # Batch set
        items = {
            "key1": {"data": "value1"},
            "key2": {"data": "value2"},
            "key3": {"data": "value3"}
        }
        await cache_manager.batch_set(items, ttl=3600)

        # Verify all items were set
        for key, value in items.items():
            result = await cache_manager.get(key)
            assert result == value

        # Batch get
        results = await cache_manager.batch_get(["key1", "key2", "missing"])
        assert len(results) == 2
        assert results["key1"] == {"data": "value1"}
        assert results["key2"] == {"data": "value2"}
        assert "missing" not in results

    @pytest.mark.asyncio
    async def test_cache_key_namespacing(self, cache_manager):
        """Test that cache keys are properly namespaced."""
        await cache_manager.set("test_key", "value")

        # Internal key should be namespaced
        assert "sp_audit:test_key" in cache_manager.memory_cache.cache

    @pytest.mark.asyncio
    async def test_error_handling(self, cache_manager, mock_db_repo):
        """Test error handling in cache operations."""
        # Mock database error
        mock_db_repo.get_cache_entry.side_effect = Exception("Database error")

        # Should return None on error, not raise
        result = await cache_manager.get("test_key")
        assert result is None

        # Mock set error
        mock_db_repo.set_cache_entry.side_effect = Exception("Database error")

        # Should still set in memory cache
        await cache_manager.set("test_key2", "value2")
        assert cache_manager.memory_cache.get("sp_audit:test_key2") == "value2"

    @pytest.mark.asyncio
    async def test_cleanup_expired_entries(self, cache_manager, mock_db_repo):
        """Test cleanup of expired cache entries."""
        mock_db_repo.cleanup_expired_cache_entries.return_value = 5

        count = await cache_manager.cleanup_expired()
        assert count == 5

        mock_db_repo.cleanup_expired_cache_entries.assert_called_once()

    def test_cache_stats(self, cache_manager):
        """Test getting cache statistics."""
        stats = cache_manager.stats()

        assert "memory_cache" in stats
        assert "cache_prefix" in stats
        assert stats["cache_prefix"] == "sp_audit"


@pytest.mark.asyncio
async def test_cache_integration_with_permission_analyzer():
    """Test cache integration with permission analyzer."""
    from src.core.permissions import PermissionAnalyzer

    # Create real cache manager with mock DB
    mock_db = AsyncMock(spec=DatabaseRepository)
    mock_db.get_cache_entry.return_value = None
    cache_manager = CacheManager(mock_db)

    # Create permission analyzer with real cache
    mock_graph = AsyncMock()
    mock_sp = AsyncMock()

    analyzer = PermissionAnalyzer(
        graph_client=mock_graph,
        sp_client=mock_sp,
        db_repo=mock_db,
        cache_manager=cache_manager
    )

    # Mock group expansion
    mock_graph.expand_group_members_transitive.return_value = [
        {"@odata.type": "#microsoft.graph.user", "id": "user_1"}
    ]
    mock_graph.get_group_info.return_value = {"displayName": "Test Group"}

    # First call should hit the API
    result1 = await analyzer.expand_group_permissions("group_1")
    assert mock_graph.expand_group_members_transitive.call_count == 1

    # Second call should hit cache
    result2 = await analyzer.expand_group_permissions("group_1")
    assert mock_graph.expand_group_members_transitive.call_count == 1  # Not called again

    # Results should be the same
    assert result1.group_id == result2.group_id
    assert len(result1.members) == len(result2.members)
