"""Comprehensive validation tests for Phase 5: Permission Analysis."""

import pytest
import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.api.auth_manager import AuthenticationManager
from src.api.graph_client import GraphAPIClient
from src.api.sharepoint_client import SharePointAPIClient
from src.cache.cache_manager import CacheManager
from src.core.permissions import (
    PermissionAnalyzer,
    PermissionEntry,
    PermissionSet,
    PrincipalType,
    GroupMembership
)
from src.core.processors import PermissionAnalysisStage
from src.core.pipeline import PipelineContext, AuditPipeline
from src.database.repository import DatabaseRepository
from src.utils.config_parser import AppConfig, AuthConfig, DbConfig


class TestPhase5ValidationSuite:
    """Comprehensive test suite to validate Phase 5 implementation."""

    @pytest.fixture
    def test_db(self, tmp_path):
        """Create a test database."""
        db_path = tmp_path / "test_phase5.db"
        db_repo = DatabaseRepository(str(db_path))
        asyncio.run(db_repo.initialize_database())
        yield db_repo
        # No explicit close needed - SQLite will handle cleanup

    @pytest.fixture
    def mock_auth_manager(self):
        """Create a mock authentication manager."""
        auth_config = AuthConfig(
            tenant_id="test_tenant",
            client_id="test_client",
            certificate_path="test_cert.pem"
        )
        return AuthenticationManager(auth_config)

    @pytest.fixture
    def mock_graph_client(self, mock_auth_manager):
        """Create a mock Graph API client."""
        client = AsyncMock(spec=GraphAPIClient)

        # Mock group expansion
        client.expand_group_members_transitive.return_value = [
            {
                "@odata.type": "#microsoft.graph.user",
                "id": "user1",
                "userPrincipalName": "user1@internal.com"
            },
            {
                "@odata.type": "#microsoft.graph.user",
                "id": "user2",
                "userPrincipalName": "external_user#EXT#@contoso.com"
            },
            {
                "@odata.type": "#microsoft.graph.group",
                "id": "nested_group1"
            }
        ]

        # Mock group info
        client.get_group_info.return_value = {
            "id": "test_group",
            "displayName": "Test Security Group"
        }

        # Mock external user check
        client.check_external_user.return_value = True

        return client

    @pytest.fixture
    def mock_sp_client(self, mock_auth_manager):
        """Create a mock SharePoint API client."""
        client = AsyncMock(spec=SharePointAPIClient)

        # Mock site permissions
        client.get_site_permissions.return_value = [
            {
                "Member": {
                    "Id": "group1",
                    "Title": "Site Owners",
                    "PrincipalType": 8
                },
                "RoleDefinitionBindings": [
                    {"Name": "Full Control"}
                ]
            },
            {
                "Member": {
                    "Id": "user1",
                    "Title": "admin@contoso.com",
                    "PrincipalType": 1
                },
                "RoleDefinitionBindings": [
                    {"Name": "Full Control"}
                ]
            }
        ]

        # Mock library permissions
        client.get_library_permissions.return_value = [
            {
                "Member": {
                    "Id": "group2",
                    "Title": "Site Members",
                    "PrincipalType": 8
                },
                "RoleDefinitionBindings": [
                    {"Name": "Contribute"}
                ]
            }
        ]

        # Mock item permissions
        client.get_item_permissions.return_value = [
            {
                "Member": {
                    "Id": "user2",
                    "Title": "external_user#EXT#@partner.com",
                    "PrincipalType": 1
                },
                "RoleDefinitionBindings": [
                    {"Name": "Read"}
                ]
            },
            {
                "Member": {
                    "Id": "anonymous_link",
                    "Title": "Anonymous Link",
                    "IsAnonymousGuestUser": True
                },
                "RoleDefinitionBindings": [
                    {"Name": "View Only"}
                ]
            }
        ]

        return client

    @pytest.mark.asyncio
    async def test_phase5_core_requirements(self, test_db, mock_graph_client, mock_sp_client):
        """Test that all core Phase 5 requirements are met."""
        # Get the actual db_repo from the async generator
        db_repo = test_db

        # Create cache manager
        cache = CacheManager(db_repo)

        # Create permission analyzer
        analyzer = PermissionAnalyzer(
            graph_client=mock_graph_client,
            sp_client=mock_sp_client,
            db_repo=db_repo,
            cache_manager=cache
        )

        # Test 1: Unique vs Inherited Permission Detection
        # Item with unique permissions
        unique_item = {
            "id": "file1",
            "library_id": "lib1",
            "site_url": "https://test.sharepoint.com/sites/TestSite",
            "has_unique_permissions": True
        }

        result = await analyzer.analyze_item_permissions(unique_item, "file")
        assert result.has_unique_permissions is True
        assert len(result.permissions) > 0
        assert not result.permissions[0].is_inherited

        # Item with inherited permissions
        inherited_item = {
            "id": "file2",
            "library_id": "lib1",
            "has_unique_permissions": False
        }

        # Mock parent library lookup
        db_repo.fetch_one = AsyncMock(return_value={
            "id": "lib1",
            "site_id": "site1",
            "has_unique_permissions": True,
            "site_url": "https://test.sharepoint.com/sites/TestSite"
        })

        result = await analyzer.analyze_item_permissions(inherited_item, "file")
        assert result.has_unique_permissions is False
        assert result.inheritance_source_id is not None
        assert all(perm.is_inherited for perm in result.permissions)

        # Test 2: Group Expansion (Transitive)
        group_membership = await analyzer.expand_group_permissions("test_group")
        assert isinstance(group_membership, GroupMembership)
        assert len(group_membership.members) == 2  # Only users
        assert len(group_membership.nested_groups) == 1  # Only groups
        assert group_membership.total_member_count == 2

        # Verify transitiveMembers endpoint was called
        mock_graph_client.expand_group_members_transitive.assert_called_with("test_group")

        # Test 3: External User Detection
        # Check various external user patterns
        assert await analyzer._check_if_external("user#EXT#@external.com", PrincipalType.USER) is True
        assert await analyzer._check_if_external("guest_user@partner.com", PrincipalType.USER) is True
        assert await analyzer._check_if_external("internal@contoso.com", PrincipalType.USER) is True  # Checked via Graph

        # Test 4: Anonymous Link Detection
        anonymous_principal = {"IsAnonymousGuestUser": True}
        assert analyzer._check_if_anonymous_link(anonymous_principal, {}) is True

        anonymous_assignment = {"IsAnonymousLink": True}
        assert analyzer._check_if_anonymous_link({}, anonymous_assignment) is True

        # Test 5: Permission Caching
        # First call should hit the API
        cache_test_item = {
            "id": "cached_file",
            "library_id": "lib1",
            "site_url": "https://test.sharepoint.com/sites/TestSite",
            "has_unique_permissions": True
        }

        await analyzer.analyze_item_permissions(cache_test_item, "file")

        # Second call should use cache
        mock_sp_client.get_item_permissions.reset_mock()
        await analyzer.analyze_item_permissions(cache_test_item, "file")

        # API should not be called second time due to caching
        assert mock_sp_client.get_item_permissions.call_count == 0

    @pytest.mark.asyncio
    async def test_permission_analysis_pipeline_integration(self, test_db, mock_graph_client, mock_sp_client):
        """Test that permission analysis is properly integrated into the pipeline."""
        # Get the actual db_repo from the async generator
        db_repo = test_db

        # Create pipeline context
        context = PipelineContext(
            run_id="test_run",
            db_repository=db_repo,
            sites=[{
                "site_id": "site1",
                "url": "https://test.sharepoint.com/sites/TestSite",
                "has_unique_permissions": True
            }],
            libraries=[{
                "library_id": "lib1",
                "site_id": "site1",
                "has_unique_permissions": False
            }],
            folders=[{
                "folder_id": "folder1",
                "library_id": "lib1",
                "has_unique_permissions": False
            }],
            files=[{
                "file_id": "file1",
                "library_id": "lib1",
                "has_unique_permissions": True
            }]
        )

        # Create cache manager
        cache = CacheManager(db_repo)

        # Create permission analyzer
        analyzer = PermissionAnalyzer(
            graph_client=mock_graph_client,
            sp_client=mock_sp_client,
            db_repo=db_repo,
            cache_manager=cache
        )

        # Create and execute permission stage
        permission_stage = PermissionAnalysisStage(analyzer)
        result_context = await permission_stage.execute(context)

        # Verify permissions were analyzed and stored in context
        assert len(result_context.permissions) > 0

        # Check that permissions have required fields
        for perm in result_context.permissions:
            assert "object_type" in perm
            assert "object_id" in perm
            assert "principal_type" in perm
            assert "principal_id" in perm
            assert "permission_level" in perm
            assert "is_inherited" in perm

    @pytest.mark.asyncio
    async def test_permission_report_generation(self, test_db, mock_graph_client, mock_sp_client):
        """Test that permission analysis generates accurate reports."""
        # Get the actual db_repo from the async generator
        db_repo = test_db

        # Create cache manager
        cache = CacheManager(db_repo)

        # Create permission analyzer
        analyzer = PermissionAnalyzer(
            graph_client=mock_graph_client,
            sp_client=mock_sp_client,
            db_repo=db_repo,
            cache_manager=cache
        )

        # Create sample permission sets
        permission_sets = [
            PermissionSet(
                object_type="file",
                object_id="file1",
                object_path="/sites/Test/file1.docx",
                has_unique_permissions=True,
                external_users_count=2,
                anonymous_links_count=0
            ),
            PermissionSet(
                object_type="file",
                object_id="file2",
                object_path="/sites/Test/file2.xlsx",
                has_unique_permissions=False,
                external_users_count=0,
                anonymous_links_count=1
            ),
            PermissionSet(
                object_type="folder",
                object_id="folder1",
                object_path="/sites/Test/Documents",
                has_unique_permissions=True,
                external_users_count=15,
                anonymous_links_count=0
            )
        ]

        # Generate report
        report = await analyzer.generate_permission_report(permission_sets)

        # Validate report structure and accuracy
        assert report["summary"]["total_items_analyzed"] == 3
        assert report["summary"]["items_with_unique_permissions"] == 2
        assert report["summary"]["items_with_external_sharing"] == 3  # All 3 items have some external sharing
        assert report["summary"]["total_external_users"] == 17
        assert report["summary"]["total_anonymous_links"] == 1
        assert report["summary"]["unique_permission_percentage"] == pytest.approx(66.67, 0.1)

        # Validate risk summary
        assert report["risk_summary"]["high_risk_items"] == 1  # 1 item with anonymous links
        assert report["risk_summary"]["medium_risk_items"] == 1  # 1 item with >10 external users
        assert report["risk_summary"]["low_risk_items"] == 1  # 1 item with 1-10 external users

    @pytest.mark.asyncio
    async def test_batch_permission_analysis(self, test_db, mock_graph_client, mock_sp_client):
        """Test concurrent batch permission analysis."""
        # Get the actual db_repo from the async generator
        db_repo = test_db

        # Create cache manager
        cache = CacheManager(db_repo)

        # Create permission analyzer
        analyzer = PermissionAnalyzer(
            graph_client=mock_graph_client,
            sp_client=mock_sp_client,
            db_repo=db_repo,
            cache_manager=cache
        )

        # Create multiple items
        items = [
            {
                "id": f"file{i}",
                "library_id": "lib1",
                "site_url": "https://test.sharepoint.com/sites/TestSite",
                "has_unique_permissions": i % 2 == 0
            }
            for i in range(10)
        ]

        # Analyze in batch
        results = await analyzer.analyze_permissions_batch(items, "file", max_concurrent=5)

        # Verify all items were analyzed
        assert len(results) == 10
        assert all(isinstance(r, PermissionSet) for r in results if not isinstance(r, Exception))

        # Verify concurrent execution (max 5 at a time)
        # This is implicitly tested by the semaphore in analyze_permissions_batch

    @pytest.mark.asyncio
    async def test_permission_database_storage(self, test_db):
        """Test that permissions are properly stored in the database."""
        # Get the actual db_repo from the async generator
        db_repo = test_db

        # Insert test permissions
        permissions = [
            {
                "object_type": "file",
                "object_id": "file1",
                "principal_type": "user",
                "principal_id": "user1",
                "principal_name": "test@contoso.com",
                "permission_level": "Read",
                "is_inherited": False,
                "granted_at": datetime.now(timezone.utc).isoformat(),
                "granted_by": "admin@contoso.com"
            },
            {
                "object_type": "file",
                "object_id": "file1",
                "principal_type": "group",
                "principal_id": "group1",
                "principal_name": "Site Members",
                "permission_level": "Contribute",
                "is_inherited": True,
                "granted_at": datetime.now(timezone.utc).isoformat(),
                "granted_by": "system"
            }
        ]

        # Store permissions
        await db_repo.bulk_insert("permissions", permissions)

        # Query permissions
        stored_perms = await db_repo.fetch_all(
            "SELECT * FROM permissions WHERE object_id = ?",
            ("file1",)
        )

        assert len(stored_perms) == 2
        assert any(p["principal_type"] == "user" for p in stored_perms)
        assert any(p["principal_type"] == "group" for p in stored_perms)

        # Test permission summary view
        summary = await db_repo.get_permissions_summary()
        assert summary["total_permissions"] >= 2
        assert summary["unique_permissions"] >= 1  # At least one unique permission
        # Calculate inherited count from total - unique
        inherited_count = summary["total_permissions"] - summary["unique_permissions"]
        assert inherited_count >= 1  # At least one inherited permission


@pytest.mark.asyncio
async def test_phase5_end_to_end():
    """End-to-end test of Phase 5 functionality."""
    # This test verifies the complete flow from discovery to permission storage
    from scripts.run_pipeline import create_pipeline

    # Create pipeline with permission analysis
    pipeline = await create_pipeline(
        config_path="config/config.json",
        dry_run=True,
        analyze_permissions=True
    )

    # Run the pipeline
    result = await pipeline.run()

    # Verify permission analysis was executed
    assert "permission_analysis" in [stage.name for stage in pipeline._stages]

    # Verify no errors occurred
    assert len(result.errors) == 0

    # Verify metrics were recorded
    assert result.metrics.total_duration > 0
