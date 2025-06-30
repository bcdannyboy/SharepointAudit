"""Cache manager for SharePoint audit system."""

import asyncio
import json
import time
from typing import Any, Dict, Optional, Union
from datetime import datetime, timedelta, timezone
import logging
from collections import OrderedDict
from threading import Lock

from src.database.repository import DatabaseRepository
from src.redis.asyncio import from_url as redis_from_url
from src.cachetools import TTLCache

logger = logging.getLogger(__name__)


class InMemoryCache:
    """Thread-safe in-memory LRU cache with TTL support."""

    def __init__(self, max_size: int = 10000):
        self.cache: OrderedDict[str, Dict[str, Any]] = OrderedDict()
        self.max_size = max_size
        self.lock = Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if it exists and hasn't expired."""
        with self.lock:
            if key not in self.cache:
                self.misses += 1
                return None

            entry = self.cache[key]
            if entry['expires_at'] and time.time() > entry['expires_at']:
                # Entry has expired
                del self.cache[key]
                self.misses += 1
                return None

            # Move to end (most recently used)
            self.cache.move_to_end(key)
            self.hits += 1
            return entry['value']

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache with optional TTL in seconds."""
        with self.lock:
            expires_at = None
            if ttl:
                expires_at = time.time() + ttl

            self.cache[key] = {
                'value': value,
                'expires_at': expires_at,
                'created_at': time.time()
            }

            # Move to end
            self.cache.move_to_end(key)

            # Evict oldest if over max size
            while len(self.cache) > self.max_size:
                oldest_key = next(iter(self.cache))
                del self.cache[oldest_key]

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                return True
            return False

    def clear(self):
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self.hits = 0
            self.misses = 0

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self.lock:
            total_requests = self.hits + self.misses
            hit_rate = self.hits / total_requests if total_requests > 0 else 0

            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'hits': self.hits,
                'misses': self.misses,
                'hit_rate': hit_rate,
                'total_requests': total_requests
            }


class CacheStatistics:
    """Track overall cache performance metrics."""

    def __init__(self) -> None:
        self.l1_hits = 0
        self.l2_hits = 0
        self.misses = 0

    @property
    def hit_rate(self) -> float:
        total = self.l1_hits + self.l2_hits + self.misses
        if total == 0:
            return 0.0
        return (self.l1_hits + self.l2_hits) / total

    def as_dict(self) -> Dict[str, Any]:
        return {
            'l1_hits': self.l1_hits,
            'l2_hits': self.l2_hits,
            'misses': self.misses,
            'hit_rate': self.hit_rate,
        }


class CacheManager:
    """
    Manages caching for the SharePoint audit system.

    Provides a two-tier cache:
    1. In-memory cache for fast access
    2. Database cache for persistence across runs
    """

    def __init__(
        self,
        db_repo: DatabaseRepository | None,
        redis_url: str | None = None,
        memory_cache_size: int = 10000,
    ):
        self.db_repo = db_repo
        self.memory_cache = InMemoryCache(max_size=memory_cache_size)
        self.redis = redis_from_url(redis_url) if redis_url else None
        self._cache_prefix = "sp_audit"
        self.cache_stats = CacheStatistics()

    def _make_key(self, key: str) -> str:
        """Create a namespaced cache key."""
        return f"{self._cache_prefix}:{key}"

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache.

        First checks memory cache, then database cache.
        """
        full_key = self._make_key(key)

        # Check memory cache first
        value = self.memory_cache.get(full_key)
        if value is not None:
            self.cache_stats.l1_hits += 1
            return value

        # Check redis cache
        if self.redis:
            try:
                value = await self.redis.get(full_key)
                if value is not None:
                    self.cache_stats.l2_hits += 1
                    deserialized = json.loads(value)
                    self.memory_cache.set(full_key, deserialized)
                    return deserialized
            except Exception as e:
                logger.error(f"Error accessing redis cache: {e}")

        # Check database cache
        if self.db_repo:
            try:
                cache_entry = await self.db_repo.get_cache_entry(full_key)
                if cache_entry:
                    # Check if expired
                    expires_at = cache_entry.get("expires_at") if isinstance(cache_entry, dict) else cache_entry.expires_at
                    if expires_at:
                        # Handle both string and datetime formats
                        if isinstance(expires_at, str):
                            expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        if expires_at < datetime.now(timezone.utc):
                            await self.db_repo.delete_cache_entry(full_key)
                            return None

                    # Deserialize value
                    cache_value = cache_entry.get("value") if isinstance(cache_entry, dict) else cache_entry.value
                    value = json.loads(cache_value)

                    # Store in memory cache for faster access
                    ttl = None
                    if expires_at:
                        ttl = int((expires_at - datetime.now(timezone.utc)).total_seconds())
                        if ttl > 0:
                            self.memory_cache.set(full_key, value, ttl)
                    else:
                        self.memory_cache.set(full_key, value)

                    return value
            except Exception as e:
                logger.error(f"Error accessing database cache: {e}")

        self.cache_stats.misses += 1
        return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Set value in cache with optional TTL in seconds.

        Stores in both memory and database cache.
        """
        full_key = self._make_key(key)

        # Store in memory cache
        self.memory_cache.set(full_key, value, ttl)

        # Store in redis if available
        if self.redis:
            try:
                serialized = json.dumps(value)
                await self.redis.setex(full_key, ttl or 3600, serialized)
            except Exception as e:
                logger.error(f"Error storing in redis cache: {e}")

        # Store in database cache if configured
        if self.db_repo:
            try:
                expires_at = None
                if ttl:
                    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)

                await self.db_repo.set_cache_entry(
                    key=full_key,
                    value=json.dumps(value),
                    expires_at=expires_at
                )
            except Exception as e:
                logger.error(f"Error storing in database cache: {e}")

    async def delete(self, key: str) -> bool:
        """Delete key from both caches."""
        full_key = self._make_key(key)

        # Delete from memory cache
        memory_deleted = self.memory_cache.delete(full_key)

        # Delete from redis
        if self.redis:
            try:
                await self.redis.delete(full_key)
            except Exception as e:
                logger.error(f"Error deleting from redis cache: {e}")

        # Delete from database cache
        if self.db_repo:
            try:
                db_deleted = await self.db_repo.delete_cache_entry(full_key)
                return memory_deleted or db_deleted
            except Exception as e:
                logger.error(f"Error deleting from database cache: {e}")
                return memory_deleted
        return memory_deleted

    async def clear(self):
        """Clear all cache entries."""
        # Clear memory cache
        self.memory_cache.clear()

        if self.redis:
            try:
                await self.redis.flushdb()
            except Exception as e:
                logger.error(f"Error clearing redis cache: {e}")

        # Clear database cache
        if self.db_repo:
            try:
                await self.db_repo.clear_cache()
            except Exception as e:
                logger.error(f"Error clearing database cache: {e}")

    async def batch_get(self, keys: list[str]) -> Dict[str, Any]:
        """Get multiple values from cache."""
        results = {}

        for key in keys:
            value = await self.get(key)
            if value is not None:
                results[key] = value

        return results

    async def batch_set(self, items: Dict[str, Any], ttl: Optional[int] = None):
        """Set multiple values in cache."""
        tasks = []
        for key, value in items.items():
            tasks.append(self.set(key, value, ttl))

        await asyncio.gather(*tasks)

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return {
            'memory_cache': self.memory_cache.stats(),
            'cache_prefix': self._cache_prefix,
            'cache_stats': self.cache_stats.as_dict()
        }

    async def cleanup_expired(self):
        """Remove expired entries from database cache."""
        if self.db_repo:
            try:
                deleted_count = await self.db_repo.cleanup_expired_cache_entries()
                logger.info(f"Cleaned up {deleted_count} expired cache entries")
                return deleted_count
            except Exception as e:
                logger.error(f"Error cleaning up expired cache entries: {e}")
                return 0
        return 0
