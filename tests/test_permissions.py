"""Tests for permission analysis functionality."""

import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.core.permissions import (
    PermissionAnalyzer,
    PermissionEntry,
    PermissionSet,
    PrincipalType,
    PermissionLevel,
    GroupMembership
)
from src.api.graph_client import GraphAPIClient
from src.api.sharepoint_client import SharePointAPIClient
from src.database.repository import DatabaseRepository
from src.cache.cache_manager import CacheManager


@pytest.fixture
def mock_graph_client():
    """Create a mock Graph API client."""
    client = AsyncMock(spec=GraphAPIClient)
    return client


@pytest.fixture
def mock_sp_client():
    """Create a mock SharePoint API client."""
    client = AsyncMock(spec=SharePointAPIClient)
    return client


@pytest.fixture
def mock_db_repo():
    """Create a mock database repository."""
    repo = AsyncMock(spec=DatabaseRepository)
    return repo


@pytest.fixture
def mock_cache_manager():
    """Create a mock cache manager."""
    cache = AsyncMock(spec=CacheManager)
    cache.get.return_value = None  # Default to no cache
    return cache


@pytest.fixture
def permission_analyzer(mock_graph_client, mock_sp_client, mock_db_repo, mock_cache_manager):
    """Create a permission analyzer with mocked dependencies."""
    return PermissionAnalyzer(
        graph_client=mock_graph_client,
        sp_client=mock_sp_client,
        db_repo=mock_db_repo,
        cache_manager=mock_cache_manager
    )


class TestPermissionAnalyzer:
    """Test cases for PermissionAnalyzer class."""

    @pytest.mark.asyncio
    async def test_analyze_item_with_unique_permissions(self, permission_analyzer, mock_sp_client):
        """Test analyzing an item with unique permissions."""
        # Arrange
        item = {
            "id": "test_file_1",
            "library_id": "lib_1",
            "site_url": "https://test.sharepoint.com/sites/TestSite",
            "has_unique_permissions": True
        }

        mock_sp_client.get_item_permissions.return_value = [
            {
                "Member": {
                    "Id": "user_1",
                    "Title": "Test User",
                    "PrincipalType": 1
                },
                "RoleDefinitionBindings": [
                    {"Name": "Read"}
                ]
            }
        ]

        # Act
        result = await permission_analyzer.analyze_item_permissions(item, "file")

        # Assert
        assert isinstance(result, PermissionSet)
        assert result.has_unique_permissions is True
        assert len(result.permissions) == 1
        assert result.permissions[0].principal_name == "Test User"
        assert result.permissions[0].permission_level == "Read"
        assert result.permissions[0].is_inherited is False

    @pytest.mark.asyncio
    async def test_analyze_item_with_inherited_permissions(self, permission_analyzer, mock_db_repo):
        """Test analyzing an item with inherited permissions."""
        # Arrange
        item = {
            "id": "test_file_2",
            "library_id": "lib_1",
            "has_unique_permissions": False
        }

        parent_library = {
            "id": "lib_1",
            "site_id": "site_1",
            "has_unique_permissions": False
        }

        parent_site = {
            "id": "site_1",
            "site_url": "https://test.sharepoint.com/sites/TestSite",
            "has_unique_permissions": True
        }

        mock_db_repo.fetch_one.side_effect = [parent_library, parent_site]

        # Mock site permissions
        permission_analyzer.sp_client.get_site_permissions.return_value = [
            {
                "Member": {
                    "Id": "group_1",
                    "Title": "Site Members",
                    "PrincipalType": 8
                },
                "RoleDefinitionBindings": [
                    {"Name": "Contribute"}
                ]
            }
        ]

        # Act
        result = await permission_analyzer.analyze_item_permissions(item, "file")

        # Assert
        assert result.has_unique_permissions is False
        assert result.inheritance_source_id == "site_1"
        assert len(result.permissions) == 1
        assert result.permissions[0].is_inherited is True
        assert result.permissions[0].permission_level == "Contribute"

    @pytest.mark.asyncio
    async def test_group_expansion_is_transitive(self, permission_analyzer, mock_graph_client):
        """Verify that group expansion correctly fetches members of nested groups."""
        # Arrange
        group_id = "group_with_nested"
        mock_graph_client.expand_group_members_transitive.return_value = [
            {
                "@odata.type": "#microsoft.graph.user",
                "id": "user_1",
                "userPrincipalName": "user1@test.com"
            },
            {
                "@odata.type": "#microsoft.graph.user",
                "id": "user_2",
                "userPrincipalName": "user2@test.com"
            },
            {
                "@odata.type": "#microsoft.graph.group",
                "id": "nested_group_1"
            }
        ]

        mock_graph_client.get_group_info.return_value = {
            "id": group_id,
            "displayName": "Test Group"
        }

        # Act
        result = await permission_analyzer.expand_group_permissions(group_id)

        # Assert
        mock_graph_client.expand_group_members_transitive.assert_called_once_with(group_id)
        assert isinstance(result, GroupMembership)
        assert len(result.members) == 2  # Only users
        assert len(result.nested_groups) == 1  # Only groups
        assert result.total_member_count == 2

    @pytest.mark.asyncio
    async def test_permission_inheritance_logic(self, permission_analyzer):
        """Test that the analyzer correctly fetches permissions from the parent object."""
        # Arrange
        item_with_inherited_perms = {
            "id": "test_folder_1",
            "library_id": "lib_1",
            "has_unique_role_assignments": False
        }

        # Mock the internal methods
        permission_analyzer._get_unique_permissions = AsyncMock()
        permission_analyzer._get_inherited_permissions = AsyncMock()

        # Act
        await permission_analyzer.analyze_item_permissions(item_with_inherited_perms, "folder")

        # Assert
        permission_analyzer._get_unique_permissions.assert_not_called()
        permission_analyzer._get_inherited_permissions.assert_called_once()

    @pytest.mark.asyncio
    async def test_external_user_detection(self, permission_analyzer, mock_graph_client):
        """Test detection of external users."""
        # Arrange
        mock_graph_client.check_external_user.return_value = True

        # Test various external user patterns
        assert await permission_analyzer._check_if_external(
            "user_external#EXT#@test.onmicrosoft.com",
            PrincipalType.USER
        ) is True

        assert await permission_analyzer._check_if_external(
            "guest_user@external.com",
            PrincipalType.USER
        ) is True

        # Non-external user
        mock_graph_client.check_external_user.return_value = False
        assert await permission_analyzer._check_if_external(
            "internal_user@test.com",
            PrincipalType.USER
        ) is False

    @pytest.mark.asyncio
    async def test_anonymous_link_detection(self, permission_analyzer):
        """Test detection of anonymous sharing links."""
        # Anonymous link
        principal = {"IsAnonymousGuestUser": True}
        assignment = {}
        assert permission_analyzer._check_if_anonymous_link(principal, assignment) is True

        # Anonymous link via assignment
        principal = {}
        assignment = {"IsAnonymousLink": True}
        assert permission_analyzer._check_if_anonymous_link(principal, assignment) is True

        # Normal user
        principal = {"Title": "Normal User"}
        assignment = {}
        assert permission_analyzer._check_if_anonymous_link(principal, assignment) is False

    @pytest.mark.asyncio
    async def test_permission_caching(self, permission_analyzer, mock_cache_manager):
        """Test that permissions are cached and retrieved from cache."""
        # Arrange
        cached_permission = {
            "object_type": "file",
            "object_id": "cached_file_1",
            "object_path": "/sites/Test/file.docx",
            "has_unique_permissions": True,
            "permissions": []
        }

        mock_cache_manager.get.return_value = cached_permission

        item = {
            "id": "cached_file_1",
            "has_unique_permissions": True
        }

        # Act
        result = await permission_analyzer.analyze_item_permissions(item, "file")

        # Assert
        mock_cache_manager.get.assert_called_once()
        assert result.object_id == "cached_file_1"
        # Should not call SharePoint API when cache hit
        permission_analyzer.sp_client.get_item_permissions.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_permission_analysis(self, permission_analyzer):
        """Test analyzing permissions for multiple items concurrently."""
        # Arrange
        items = [
            {"id": f"file_{i}", "has_unique_permissions": False}
            for i in range(5)
        ]

        # Mock analyze_item_permissions to return simple results
        async def mock_analyze(item, item_type):
            return PermissionSet(
                object_type=item_type,
                object_id=item["id"],
                object_path=f"/path/{item['id']}",
                has_unique_permissions=False
            )

        permission_analyzer.analyze_item_permissions = mock_analyze

        # Act
        results = await permission_analyzer.analyze_permissions_batch(items, "file", max_concurrent=3)

        # Assert
        assert len(results) == 5
        assert all(isinstance(r, PermissionSet) for r in results)

    def test_principal_type_mapping(self, permission_analyzer):
        """Test mapping of SharePoint principal type numbers to enums."""
        assert permission_analyzer._get_principal_type(1) == PrincipalType.USER
        assert permission_analyzer._get_principal_type(2) == PrincipalType.GROUP
        assert permission_analyzer._get_principal_type(4) == PrincipalType.GROUP
        assert permission_analyzer._get_principal_type(8) == PrincipalType.SHAREPOINT_GROUP
        assert permission_analyzer._get_principal_type(16) == PrincipalType.APPLICATION
        assert permission_analyzer._get_principal_type(999) == PrincipalType.USER  # Default

    def test_risk_level_calculation(self, permission_analyzer):
        """Test risk level calculation based on external sharing."""
        assert permission_analyzer._calculate_risk_level(0, 0) == "NONE"
        assert permission_analyzer._calculate_risk_level(5, 0) == "LOW"
        assert permission_analyzer._calculate_risk_level(15, 0) == "MEDIUM"
        assert permission_analyzer._calculate_risk_level(0, 1) == "HIGH"
        assert permission_analyzer._calculate_risk_level(20, 5) == "HIGH"

    @pytest.mark.asyncio
    async def test_permission_report_generation(self, permission_analyzer):
        """Test generation of permission analysis report."""
        # Arrange
        permission_sets = [
            PermissionSet(
                object_type="file",
                object_id="file_1",
                object_path="/file1.docx",
                has_unique_permissions=True,
                external_users_count=2,
                anonymous_links_count=0
            ),
            PermissionSet(
                object_type="file",
                object_id="file_2",
                object_path="/file2.xlsx",
                has_unique_permissions=False,
                external_users_count=0,
                anonymous_links_count=1
            )
        ]

        # Add some permissions to the sets
        permission_sets[0].add_permission(PermissionEntry(
            principal_id="user_1",
            principal_name="user1@test.com",
            principal_type=PrincipalType.USER,
            permission_level="Read",
            is_inherited=False,
            is_external=True
        ))

        # Act
        report = await permission_analyzer.generate_permission_report(permission_sets)

        # Assert
        assert report["summary"]["total_items_analyzed"] == 2
        assert report["summary"]["items_with_unique_permissions"] == 1
        assert report["summary"]["items_with_external_sharing"] == 2
        assert report["summary"]["total_external_users"] == 3  # 2 from first set + 1 added
        assert report["summary"]["total_anonymous_links"] == 1
        assert report["risk_summary"]["high_risk_items"] == 1  # Has anonymous link
        assert report["risk_summary"]["low_risk_items"] == 1  # Has external users


class TestPermissionEntry:
    """Test cases for PermissionEntry dataclass."""

    def test_permission_entry_creation(self):
        """Test creating a permission entry."""
        entry = PermissionEntry(
            principal_id="user_1",
            principal_name="test@example.com",
            principal_type=PrincipalType.USER,
            permission_level="Read",
            is_inherited=False,
            granted_at=datetime.now(timezone.utc),
            granted_by="admin@example.com",
            is_external=False,
            is_anonymous_link=False
        )

        assert entry.principal_id == "user_1"
        assert entry.principal_type == PrincipalType.USER
        assert entry.permission_level == "Read"
        assert not entry.is_inherited
        assert not entry.is_external


class TestPermissionSet:
    """Test cases for PermissionSet dataclass."""

    def test_permission_set_add_permission(self):
        """Test adding permissions to a permission set."""
        perm_set = PermissionSet(
            object_type="file",
            object_id="file_1",
            object_path="/test/file.docx",
            has_unique_permissions=True
        )

        # Add regular permission
        entry1 = PermissionEntry(
            principal_id="user_1",
            principal_name="user@test.com",
            principal_type=PrincipalType.USER,
            permission_level="Read",
            is_inherited=False
        )
        perm_set.add_permission(entry1)

        assert len(perm_set.permissions) == 1
        assert perm_set.external_users_count == 0
        assert perm_set.anonymous_links_count == 0

        # Add external user
        entry2 = PermissionEntry(
            principal_id="user_2",
            principal_name="external@guest.com",
            principal_type=PrincipalType.USER,
            permission_level="Read",
            is_inherited=False,
            is_external=True
        )
        perm_set.add_permission(entry2)

        assert len(perm_set.permissions) == 2
        assert perm_set.external_users_count == 1

        # Add anonymous link
        entry3 = PermissionEntry(
            principal_id="link_1",
            principal_name="Anonymous Link",
            principal_type=PrincipalType.ANONYMOUS,
            permission_level="View",
            is_inherited=False,
            is_anonymous_link=True
        )
        perm_set.add_permission(entry3)

        assert len(perm_set.permissions) == 3
        assert perm_set.anonymous_links_count == 1


class TestCacheIntegration:
    """Test cache integration with permission analyzer."""

    @pytest.mark.asyncio
    async def test_group_membership_caching(self, permission_analyzer, mock_cache_manager, mock_graph_client):
        """Test that group memberships are cached properly."""
        # Arrange
        group_id = "test_group_1"
        mock_cache_manager.get.return_value = None  # Cache miss

        mock_graph_client.expand_group_members_transitive.return_value = [
            {"@odata.type": "#microsoft.graph.user", "id": "user_1"}
        ]
        mock_graph_client.get_group_info.return_value = {
            "id": group_id,
            "displayName": "Test Group"
        }

        # Act
        result = await permission_analyzer.expand_group_permissions(group_id)

        # Assert
        mock_cache_manager.set.assert_called_once()
        cache_call_args = mock_cache_manager.set.call_args
        assert cache_call_args[0][0] == f"group_members:{group_id}"
        assert cache_call_args[1]["ttl"] == 21600  # 6 hours


@pytest.mark.asyncio
async def test_permission_stage_integration():
    """Test integration of permission analysis in the pipeline."""
    from src.core.processors import PermissionAnalysisStage
    from src.core.pipeline import PipelineContext

    # Create mock analyzer
    mock_analyzer = AsyncMock(spec=PermissionAnalyzer)
    mock_analyzer.analyze_item_permissions.return_value = PermissionSet(
        object_type="file",
        object_id="test_file",
        object_path="/test.docx",
        has_unique_permissions=True,
        permissions=[
            PermissionEntry(
                principal_id="user_1",
                principal_name="test@example.com",
                principal_type=PrincipalType.USER,
                permission_level="Read",
                is_inherited=False
            )
        ]
    )

    # Create stage
    stage = PermissionAnalysisStage(mock_analyzer)

    # Create context with test data
    context = PipelineContext(
        run_id="test_run",
        files=[{"id": "test_file", "has_unique_permissions": True}],
        sites=[],
        libraries=[],
        folders=[]
    )

    # Execute stage
    result = await stage.execute(context)

    # Verify permissions were analyzed and stored
    assert len(result.permissions) == 1
    assert result.permissions[0]["object_id"] == "test_file"
    assert result.permissions[0]["principal_name"] == "test@example.com"
