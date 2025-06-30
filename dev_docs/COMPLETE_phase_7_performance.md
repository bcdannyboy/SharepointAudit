# Phase 7: Performance & Caching

## Overview

Implement and integrate advanced performance optimizations. This phase focuses on reducing latency and resource consumption through a multi-level caching strategy and efficient parallel processing. The goal is to ensure the application can handle enterprise-scale tenants without overwhelming the system or the target APIs.

## Architectural Alignment

This phase is crucial for fulfilling the "Performance at Scale" core objective of the project. It directly implements several advanced concepts from the `ARCHITECTURE.md`:

- **[Performance and Scalability Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#performance-and-scalability-architecture)**: This is the primary reference, providing the complete design for the caching and concurrency systems.
- **[Caching Strategy](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#caching-strategy)**: Guides the implementation of the `CacheManager` with its two-level (in-memory and Redis) approach.
- **[Concurrency Management](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#concurrency-management)**: Details the `ConcurrencyManager` responsible for managing `asyncio.Semaphore` to control concurrent tasks and prevent resource exhaustion.

## Prerequisites

- [Phase 3: Basic Discovery Module](./phase_3_discovery.md)
- [Phase 5: Permission Analysis](./phase_5_permissions.md)

## Deliverables

1.  **Cache Manager**: A `CacheManager` class in `src/cache/cache_manager.py` implementing a two-level cache (in-memory `TTLCache` and optional Redis).
2.  **Concurrency Manager**: A `ConcurrencyManager` class in `src/core/concurrency.py` to manage and limit concurrent tasks using `asyncio.Semaphore`.
3.  **Integration**: The `DiscoveryModule` and `PermissionAnalyzer` will be refactored to use the `CacheManager` and `ConcurrencyManager`.

## Detailed Implementation Guide

### 1. Implement the Cache Manager (`src/cache/cache_manager.py`)

This class will provide a simple `get`/`set` interface that abstracts away the underlying cache implementation. It should prioritize the fast in-memory cache (L1) and fall back to a distributed Redis cache (L2) if configured.

```python
# src/cache/cache_manager.py
from cachetools import TTLCache
import redis.asyncio as redis
import json
from typing import Optional, Any

class CacheManager:
    """Multi-level caching system (L1: in-memory, L2: Redis)."""

    def __init__(self, redis_url: Optional[str] = None, local_cache_size: int = 10000, ttl: int = 300):
        self.local_cache = TTLCache(maxsize=local_cache_size, ttl=ttl)
        self.redis = redis.from_url(redis_url) if redis_url else None
        # self.stats = CacheStatistics() # To be implemented

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache, checking L1 then L2."""
        if key in self.local_cache:
            # self.stats.l1_hit()
            return self.local_cache[key]

        if self.redis:
            value = await self.redis.get(key)
            if value:
                # self.stats.l2_hit()
                deserialized = json.loads(value)
                self.local_cache[key] = deserialized # Populate L1
                return deserialized

        # self.stats.miss()
        return None

    async def set(self, key: str, value: Any, ttl: int = 3600):
        """Set value in both cache levels."""
        self.local_cache[key] = value
        if self.redis:
            serialized = json.dumps(value) # Add custom encoder for complex types if needed
            await self.redis.setex(key, ttl, serialized)
```

### 2. Implement the Concurrency Manager (`src/core/concurrency.py`)

This class will provide semaphores to limit different types of concurrent operations, such as API calls, database connections, and CPU-bound tasks. This prevents the application from overwhelming system resources.

```python
# src/core/concurrency.py
import asyncio

class ConcurrencyManager:
    """Manages concurrent operations with resource limits."""
    def __init__(self, max_api_calls: int = 20, max_db_connections: int = 10):
        self.api_semaphore = asyncio.Semaphore(max_api_calls)
        self.db_semaphore = asyncio.Semaphore(max_db_connections)

    async def run_api_task(self, coro):
        """Runs an API-related coroutine under the API semaphore."""
        async with self.api_semaphore:
            return await coro

    async def run_db_task(self, coro):
        """Runs a database-related coroutine under the DB semaphore."""
        async with self.db_semaphore:
            return await coro
```

### 3. Integrate into Core Modules

Refactor the `DiscoveryModule` and `PermissionAnalyzer` to use these new managers. For example, all API calls should be wrapped by the `ConcurrencyManager`, and expensive lookups (like group expansion) must be cached.

```python
# Example in PermissionAnalyzer
async def expand_group_permissions(self, group_id: str):
    cache_key = f"group_members:{group_id}"
    cached_members = await self.cache.get(cache_key)
    if cached_members:
        return cached_members

    # Wrap the API call with the concurrency manager
    members = await self.concurrency_manager.run_api_task(
        self.graph_client.expand_group_members_transitive(group_id)
    )

    await self.cache.set(cache_key, members, ttl=21600)
    return members
```

## Implementation Task Checklist

- [ ] Implement the `CacheManager` with in-memory `TTLCache`.
- [ ] Add optional Redis support to the `CacheManager`.
- [ ] Implement `CacheStatistics` to monitor cache performance.
- [ ] Implement the `ConcurrencyManager` with semaphores for different resource types.
- [ ] Integrate the `CacheManager` into the `DiscoveryModule` to cache site and content lookups.
- [ ] Integrate the `CacheManager` into the `PermissionAnalyzer` to cache group memberships and permission sets.
- [ ] Refactor the `AuditPipeline` to use the `ConcurrencyManager` for running site audits in parallel.
- [ ] Establish performance benchmarks for key operations before and after optimizations.

## Test Plan & Cases

Testing performance features involves verifying that the cache is being used correctly and that concurrency limits are respected.

```python
# tests/test_performance.py
import pytest
import asyncio
from unittest.mock import AsyncMock

@pytest.mark.asyncio
async def test_cache_reduces_api_calls(permission_analyzer_with_cache):
    """Verify that caching group lookups avoids repeated API calls."""
    # The first call should trigger an API call (mocked)
    await permission_analyzer_with_cache.expand_group_permissions("group1")
    permission_analyzer_with_cache._graph_client.get.assert_called_once()

    # The second call for the same group should hit the cache
    await permission_analyzer_with_cache.expand_group_permissions("group1")
    # The call count should still be 1
    permission_analyzer_with_cache._graph_client.get.assert_called_once()

@pytest.mark.asyncio
async def test_concurrency_manager_limits_tasks(concurrency_manager):
    """Test that the semaphore correctly limits concurrent operations."""
    concurrency_manager.api_semaphore = asyncio.Semaphore(5)

    # Try to run 10 tasks concurrently
    tasks = [concurrency_manager.run_api_task(asyncio.sleep(0.01)) for _ in range(10)]

    start_time = asyncio.get_event_loop().time()
    await asyncio.gather(*tasks)
    duration = asyncio.get_event_loop().time() - start_time

    # Since only 5 can run at once, the total time should be at least two "sleeps"
    assert duration >= 0.02
```

## Verification & Validation

Performance verification is done by comparing audit runs before and after the changes.

```bash
# 1. Run a large audit without caching and record the time and API calls from logs.
sharepoint-audit audit --config config/config.json --no-cache

# 2. Run the same audit with caching enabled.
sharepoint-audit audit --config config/config.json

# 3. Compare the total time, API calls, and cache hit rate from the logs/metrics.
#    Expect a significant reduction in time and API calls.

# 4. If Redis is available, configure it and verify the cache is populated.
redis-cli KEYS "sharepoint_audit:*"
```

## Done Criteria

- [ ] The cache hit rate for repeated operations (like group expansion) is above 90%.
- [ ] Parallel processing of sites results in a significant, measurable reduction in total audit time compared to sequential processing.
- [ ] Memory and CPU usage remain within acceptable limits during a large-scale audit.
- [ ] All performance-related tests pass.
- [ ] The application can be configured to use Redis as a cache backend.
