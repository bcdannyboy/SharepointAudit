import asyncio
from unittest.mock import AsyncMock, patch

from src.utils.exceptions import GraphAPIError


def test_graph_auth_success(auth_manager):
    async def run():
        with patch("src.api.auth_manager.GraphServiceClient") as MockClient, patch(
            "src.api.auth_manager.ClientCertificateCredential"
        ) as MockCred:
            instance = MockClient.return_value
            client = await auth_manager.get_graph_client()
            assert client is instance
            MockCred.assert_called_once()
            MockClient.assert_called_once()

    asyncio.run(run())


def test_graph_get_with_retry(graph_client):
    async def run():
        # Mock the auth manager to return a valid token
        mock_credential = AsyncMock()
        mock_credential.get_token = AsyncMock(return_value=AsyncMock(token="test_token"))

        mock_graph_service_client = AsyncMock()
        mock_graph_service_client.credentials = mock_credential

        graph_client.auth_manager.get_graph_client = AsyncMock(return_value=mock_graph_service_client)

        with patch("aiohttp.ClientSession.get") as mock_get:
            mock_get.side_effect = [
                AsyncMock(status=429, headers={"Retry-After": "0"}),
                AsyncMock(status=200, json=AsyncMock(return_value={"ok": True})),
            ]
            result = await graph_client.get_with_retry("https://graph.test.com/endpoint")
            assert result["ok"] is True
            assert mock_get.call_count == 2

    asyncio.run(run())


def test_graph_post_with_retry(graph_client):
    async def run():
        # Mock the auth manager to return a valid token
        mock_credential = AsyncMock()
        mock_credential.get_token = AsyncMock(return_value=AsyncMock(token="test_token"))

        mock_graph_service_client = AsyncMock()
        mock_graph_service_client.credentials = mock_credential

        graph_client.auth_manager.get_graph_client = AsyncMock(return_value=mock_graph_service_client)

        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.side_effect = [
                AsyncMock(status=429, headers={"Retry-After": "0"}),
                AsyncMock(status=200, json=AsyncMock(return_value={"ok": True})),
            ]
            result = await graph_client.post_with_retry(
                "https://graph.test.com/endpoint", json={"data": 1}
            )
            assert result["ok"] is True
            assert mock_post.call_count == 2

    asyncio.run(run())


def test_graph_batch_request(graph_client):
    async def run():
        # Mock the auth manager to return a valid token
        mock_credential = AsyncMock()
        mock_credential.get_token = AsyncMock(return_value=AsyncMock(token="test_token"))

        mock_graph_service_client = AsyncMock()
        mock_graph_service_client.credentials = mock_credential

        graph_client.auth_manager.get_graph_client = AsyncMock(return_value=mock_graph_service_client)

        with patch("aiohttp.ClientSession.post") as mock_post:
            mock_post.return_value = AsyncMock(
                status=200, json=AsyncMock(return_value={"responses": [1, 2]})
            )
            result = await graph_client.batch_request(
                "https://graph.test.com/$batch", [{"id": "1"}, {"id": "2"}]
            )
            assert result["responses"] == [1, 2]
            mock_post.assert_called_once()

    asyncio.run(run())
