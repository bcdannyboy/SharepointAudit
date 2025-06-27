import asyncio
import time
import logging

logger = logging.getLogger(__name__)


class RateLimiter:
    """Simple rate limiter based on resource units per time window."""

    def __init__(self, tenant_size: str = "large") -> None:
        self.resource_units = self._get_resource_units(tenant_size)
        self.window_size = 300  # seconds
        self.current_usage = 0
        self.window_start = time.time()
        self._lock = asyncio.Lock()
        self.operation_costs = {
            "simple_get": 2,
            "complex_get": 3,
            "get_with_expand": 4,
            "batch_request": 5,
            "delta_query": 1,
        }

    async def acquire(self, operation_type: str = "simple_get") -> None:
        cost = self.operation_costs.get(operation_type, 2)
        async with self._lock:
            now = time.time()
            if now - self.window_start >= self.window_size:
                self.current_usage = 0
                self.window_start = now

            if self.current_usage + cost > self.resource_units:
                wait_time = self.window_size - (now - self.window_start)
                logger.warning("Rate limit reached. Waiting %.2f seconds", wait_time)
                await asyncio.sleep(max(wait_time, 0))
                self.current_usage = 0
                self.window_start = time.time()

            self.current_usage += cost

    def _get_resource_units(self, tenant_size: str) -> int:
        limits = {"small": 6000, "medium": 9000, "large": 12000}
        return limits.get(tenant_size.lower(), 12000)
