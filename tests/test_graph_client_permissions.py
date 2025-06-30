"""Tests for Graph API client permission-related functionality."""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import aiohttp

from src.api.graph_client import GraphAPIClient
from src.api.auth_manager import AuthenticationManager
from src.utils.exceptions import GraphAPIError


@pytest.fixture
def mock_auth_manager():
    """Create a mock authentication manager."""
    auth = AsyncMock(spec=AuthenticationManager)

    # Mock the graph client and credential
    mock_credential = MagicMock()
    mock_credential.get_token = AsyncMock(return_value=MagicMock(token="test_token"))

    mock_graph_client = MagicMock()
    mock_graph_client.credentials = mock_credential

    auth.get_graph_client = AsyncMock(return_value=mock_graph_client)

    return auth


@pytest.fixture
def graph_client(mock_auth_manager):
    """Create a Graph API client with mocked auth."""
    return GraphAPIClient(mock_auth_manager)


class TestGraphAPIClientPermissions:
    """Test permission-related Graph API client functionality."""

    @pytest.mark.asyncio
    async def test_get_auth_headers(self, graph_client):
        """Test getting authentication headers."""
        headers = await graph_client._get_auth_headers()

        assert "Authorization" in headers
        assert headers["Authorization"] == "Bearer test_token"

    @pytest.mark.asyncio
    async def test_auth_token_caching(self, graph_client):
        """Test that auth tokens are cached."""
        # First call
        headers1 = await graph_client._get_auth_headers()

        # Second call should use cached token
        headers2 = await graph_client._get_auth_headers()

        # Verify token was only fetched once
        graph_client.auth_manager.get_graph_client.assert_called_once()

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_expand_group_members_transitive(self, mock_session_class, graph_client):
        """Test expanding group members transitively."""
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "value": [
                {
                    "@odata.type": "#microsoft.graph.user",
                    "id": "user1",
                    "userPrincipalName": "user1@test.com"
                },
                {
                    "@odata.type": "#microsoft.graph.user",
                    "id": "user2",
                    "userPrincipalName": "user2@test.com"
                }
            ]
        })

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Call method
        members = await graph_client.expand_group_members_transitive("group123")

        # Verify
        assert len(members) == 2
        assert members[0]["id"] == "user1"
        assert members[1]["id"] == "user2"

        # Verify correct URL was called
        mock_session.get.assert_called()
        call_args = mock_session.get.call_args
        assert "groups/group123/transitiveMembers" in call_args[0][0]

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_expand_group_members_with_pagination(self, mock_session_class, graph_client):
        """Test expanding group members with pagination."""
        # Mock paginated responses
        page1_response = AsyncMock()
        page1_response.status = 200
        page1_response.json = AsyncMock(return_value={
            "value": [{"id": "user1"}, {"id": "user2"}],
            "@odata.nextLink": "https://graph.microsoft.com/v1.0/groups/123/transitiveMembers?$skip=2"
        })

        page2_response = AsyncMock()
        page2_response.status = 200
        page2_response.json = AsyncMock(return_value={
            "value": [{"id": "user3"}]
        })

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(side_effect=[page1_response, page2_response])
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Call method
        members = await graph_client.expand_group_members_transitive("group123")

        # Verify all members were collected
        assert len(members) == 3
        assert members[0]["id"] == "user1"
        assert members[2]["id"] == "user3"

        # Verify pagination was followed
        assert mock_session.get.call_count == 2

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_get_group_info(self, mock_session_class, graph_client):
        """Test getting group information."""
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "id": "group123",
            "displayName": "Test Group",
            "mail": "testgroup@test.com"
        })

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Call method
        group_info = await graph_client.get_group_info("group123")

        # Verify
        assert group_info["id"] == "group123"
        assert group_info["displayName"] == "Test Group"

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_get_user_info(self, mock_session_class, graph_client):
        """Test getting user information."""
        # Mock response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "id": "user123",
            "userPrincipalName": "user@test.com",
            "displayName": "Test User",
            "userType": "Member"
        })

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Call method
        user_info = await graph_client.get_user_info("user123")

        # Verify
        assert user_info["id"] == "user123"
        assert user_info["userType"] == "Member"

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_batch_get_users(self, mock_session_class, graph_client):
        """Test batch getting user information."""
        # Mock batch response
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "responses": [
                {
                    "id": "user1",
                    "status": 200,
                    "body": {
                        "id": "user1",
                        "userPrincipalName": "user1@test.com"
                    }
                },
                {
                    "id": "user2",
                    "status": 200,
                    "body": {
                        "id": "user2",
                        "userPrincipalName": "user2@test.com"
                    }
                }
            ]
        })

        mock_session = AsyncMock()
        mock_session.post = AsyncMock(return_value=mock_response)
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Call method
        users = await graph_client.batch_get_users(["user1", "user2"])

        # Verify
        assert len(users) == 2
        assert users["user1"]["userPrincipalName"] == "user1@test.com"
        assert users["user2"]["userPrincipalName"] == "user2@test.com"

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_batch_get_users_handles_large_batches(self, mock_session_class, graph_client):
        """Test batch getting users handles more than 20 users."""
        # Create 25 user IDs
        user_ids = [f"user{i}" for i in range(25)]

        # Mock responses for two batches
        batch1_users = [{"id": f"user{i}", "status": 200, "body": {"id": f"user{i}"}}
                       for i in range(20)]
        batch2_users = [{"id": f"user{i}", "status": 200, "body": {"id": f"user{i}"}}
                       for i in range(20, 25)]

        mock_response1 = AsyncMock()
        mock_response1.status = 200
        mock_response1.json = AsyncMock(return_value={"responses": batch1_users})

        mock_response2 = AsyncMock()
        mock_response2.status = 200
        mock_response2.json = AsyncMock(return_value={"responses": batch2_users})

        mock_session = AsyncMock()
        mock_session.post = AsyncMock(side_effect=[mock_response1, mock_response2])
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Call method
        users = await graph_client.batch_get_users(user_ids)

        # Verify
        assert len(users) == 25
        assert mock_session.post.call_count == 2

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_check_external_user(self, mock_session_class, graph_client):
        """Test checking if a user is external."""
        # Test #EXT# pattern
        is_external = await graph_client.check_external_user("user#EXT#@test.onmicrosoft.com")
        assert is_external is True

        # Test guest user type
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json = AsyncMock(return_value={
            "id": "user123",
            "userType": "Guest"
        })

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value.__aenter__.return_value = mock_session

        is_external = await graph_client.check_external_user("guest@external.com")
        assert is_external is True

        # Test member user type
        mock_response.json = AsyncMock(return_value={
            "id": "user456",
            "userType": "Member"
        })

        is_external = await graph_client.check_external_user("member@test.com")
        assert is_external is False

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_check_external_user_error_handling(self, mock_session_class, graph_client):
        """Test external user check handles errors gracefully."""
        # Mock API error
        mock_response = AsyncMock()
        mock_response.status = 404

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Should fall back to pattern matching
        is_external = await graph_client.check_external_user("user_external@test.com")
        assert is_external is True  # Has underscore in username part

        is_external = await graph_client.check_external_user("normaluser@test.com")
        assert is_external is False

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_authentication_error(self, mock_session_class, graph_client):
        """Test handling of authentication errors."""
        # Mock auth failure
        graph_client.auth_manager.get_graph_client.side_effect = Exception("Auth failed")

        # Should raise GraphAPIError
        with pytest.raises(GraphAPIError) as exc_info:
            await graph_client._get_auth_headers()

        assert "Authentication failed" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch('aiohttp.ClientSession')
    async def test_rate_limit_handling(self, mock_session_class, graph_client):
        """Test handling of rate limit (429) responses."""
        # Mock 429 response
        mock_response = AsyncMock()
        mock_response.status = 429
        mock_response.headers = {"Retry-After": "60"}

        mock_session = AsyncMock()
        mock_session.get = AsyncMock(return_value=mock_response)
        mock_session_class.return_value.__aenter__.return_value = mock_session

        # Should raise with retry info
        with pytest.raises(GraphAPIError) as exc_info:
            await graph_client.get_group_info("group123")

        assert exc_info.value.status_code == 429
        assert exc_info.value.retry_after == 60
