import asyncio
from typing import Callable, Coroutine, Any


class ConcurrencyManager:
    """Manage concurrent operations using semaphores."""

    def __init__(self, max_api_calls: int = 20, max_db_connections: int = 10):
        self.api_semaphore = asyncio.Semaphore(max_api_calls)
        self.db_semaphore = asyncio.Semaphore(max_db_connections)

    async def run_api_task(self, coro: Coroutine[Any, Any, Any]) -> Any:
        async with self.api_semaphore:
            return await coro

    async def run_db_task(self, coro: Coroutine[Any, Any, Any]) -> Any:
        async with self.db_semaphore:
            return await coro
