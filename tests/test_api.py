import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from src.utils.exceptions import CircuitBreakerOpenError


def test_sharepoint_auth_success(auth_manager):
    async def run():
        with patch("src.api.auth_manager.ClientContext") as MockContext:
            MockContext.connect_with_certificate.return_value = MockContext
            ctx = await auth_manager.get_sharepoint_context(
                "https://tenant.sharepoint.com/sites/test"
            )
            assert ctx is not None
            MockContext.connect_with_certificate.assert_called_once()

    asyncio.run(run())


def test_retry_on_429(api_client):
    async def run():
        with patch("aiohttp.ClientSession.get") as mock_get:
            mock_get.side_effect = [
                AsyncMock(status=429, headers={"Retry-After": "0"}),
                AsyncMock(status=200, json=AsyncMock(return_value={"ok": True})),
            ]
            response = await api_client.get_with_retry("https://api.test.com/endpoint")
            assert response["ok"] is True
            assert mock_get.call_count == 2

    asyncio.run(run())


def test_post_with_retry(api_client):
    async def run():
        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.side_effect = [
                AsyncMock(status=429, headers={"Retry-After": "0"}),
                AsyncMock(status=200, json=AsyncMock(return_value={"ok": True})),
            ]
            response = await api_client.post_with_retry(
                "https://api.test.com/endpoint",
                json={"data": 1},
            )
            assert response["ok"] is True
            assert mock_post.call_count == 2

    asyncio.run(run())


def test_batch_request(api_client):
    async def run():
        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.return_value = AsyncMock(
                status=200,
                json=AsyncMock(return_value={"responses": [1, 2, 3]}),
            )
            result = await api_client.batch_request(
                "https://api.test.com/$batch",
                [{"id": "1"}, {"id": "2"}]
            )
            assert result["responses"] == [1, 2, 3]
            mock_post.assert_called_once()

    asyncio.run(run())


def test_circuit_breaker_opens(retry_strategy):
    async def run():
        failing_func = AsyncMock(side_effect=Exception("API Down"))

        for _ in range(retry_strategy.circuit_breakers["test"].failure_threshold):
            with pytest.raises(Exception):
                await retry_strategy.execute_with_retry("test", failing_func)

        with pytest.raises(CircuitBreakerOpenError):
            await retry_strategy.execute_with_retry("test", failing_func)

    asyncio.run(run())
