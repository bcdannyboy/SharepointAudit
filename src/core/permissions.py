"""Permission analysis module for SharePoint audit system."""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from src.api.graph_client import GraphAPIClient
from src.api.sharepoint_client import SharePointAPIClient
from src.database.repository import DatabaseRepository
from src.cache.cache_manager import CacheManager
from .concurrency import ConcurrencyManager
from src.utils.exceptions import SharePointAPIError
from src.utils.retry_handler import RetryStrategy, RetryConfig

logger = logging.getLogger(__name__)


class PrincipalType(Enum):
    """Types of security principals in SharePoint."""
    USER = "user"
    GROUP = "group"
    APPLICATION = "application"
    SHAREPOINT_GROUP = "sharepoint_group"
    ANONYMOUS = "anonymous"
    EXTERNAL = "external"


class PermissionLevel(Enum):
    """Standard SharePoint permission levels."""
    FULL_CONTROL = "Full Control"
    DESIGN = "Design"
    EDIT = "Edit"
    CONTRIBUTE = "Contribute"
    READ = "Read"
    LIMITED_ACCESS = "Limited Access"
    VIEW_ONLY = "View Only"
    CUSTOM = "Custom"


@dataclass
class PermissionEntry:
    """Represents a single permission entry."""
    principal_id: str
    principal_name: str
    principal_type: PrincipalType
    permission_level: str
    is_inherited: bool
    granted_at: Optional[datetime] = None
    granted_by: Optional[str] = None
    inheritance_source: Optional[str] = None
    is_external: bool = False
    is_anonymous_link: bool = False


@dataclass
class PermissionSet:
    """Collection of permissions for a SharePoint item."""
    object_type: str
    object_id: str
    object_path: str
    has_unique_permissions: bool
    permissions: List[PermissionEntry] = field(default_factory=list)
    inheritance_source_id: Optional[str] = None
    inheritance_source_path: Optional[str] = None
    external_users_count: int = 0
    anonymous_links_count: int = 0

    def add_permission(self, entry: PermissionEntry):
        """Add a permission entry and update counters."""
        self.permissions.append(entry)
        if entry.is_external:
            self.external_users_count += 1
        if entry.is_anonymous_link:
            self.anonymous_links_count += 1


@dataclass
class GroupMembership:
    """Represents expanded group membership."""
    group_id: str
    group_name: str
    members: List[Dict[str, Any]]
    nested_groups: List[str]
    total_member_count: int
    last_expanded: datetime


class PermissionAnalyzer:
    """Analyzes and maps all permissions across SharePoint."""

    def __init__(
        self,
        graph_client: GraphAPIClient,
        sp_client: SharePointAPIClient,
        db_repo: DatabaseRepository,
        cache: CacheManager,
        max_concurrent_operations: int = 20
    ):
        self.graph_client = graph_client
        self.sp_client = sp_client
        self.db_repo = db_repo
        self.cache = cache

        # Concurrency control
        self.concurrency_manager = ConcurrencyManager(max_concurrent_operations)
        self.operation_semaphore = asyncio.Semaphore(max_concurrent_operations)

        # Retry strategy with backoff
        self.retry_strategy = RetryStrategy(
            RetryConfig(
                max_attempts=3,
                base_delay=1.0,
                max_delay=30.0
            )
        )

        # Statistics
        self.stats = {
            "total_analyzed": 0,
            "unique_permissions": 0,
            "inherited_permissions": 0,
            "errors": 0,
            "external_shares": 0,
            "anonymous_links": 0
        }

    async def analyze_item_permissions(
        self,
        item: Dict[str, Any],
        item_type: str
    ) -> PermissionSet:
        """Analyze permissions for a specific SharePoint item.

        Args:
            item: The SharePoint item (site, library, folder, or file)
            item_type: Type of the item (site, library, folder, file)

        Returns:
            PermissionSet containing all resolved permissions
        """
        item_id = item.get("id") or item.get("site_id") or item.get("library_id") or item.get("file_id")
        item_path = item.get("url") or item.get("web_url") or item.get("server_relative_url") or item.get("path") or ""

        # Check cache first
        cache_key = f"permissions:{item_type}:{item_id}"
        cached_permissions = await self.cache.get(cache_key)
        if cached_permissions:
            return self._reconstruct_permission_set(cached_permissions)

        # Determine if item has unique permissions
        has_unique = await self._check_has_unique_permissions(item, item_type)

        permission_set = PermissionSet(
            object_type=item_type,
            object_id=item_id,
            object_path=item_path,
            has_unique_permissions=has_unique
        )

        if has_unique:
            # Get unique permissions directly
            await self._get_unique_permissions(item, item_type, permission_set)
            self.stats["unique_permissions"] += 1
        else:
            # Get inherited permissions from parent
            await self._get_inherited_permissions(item, item_type, permission_set)
            self.stats["inherited_permissions"] += 1

        # If no permissions were found (e.g., API failure), add defaults
        if not permission_set.permissions:
            logger.warning(f"No permissions found for {item_type} {item_id}, adding defaults")
            self._add_default_permission(permission_set, item, item_type)

        # Check for external sharing
        if has_unique:
            await self._check_external_sharing(item, item_type, permission_set)

        # Cache the result
        await self._cache_permission_set(cache_key, permission_set)

        self.stats["total_analyzed"] += 1
        return permission_set

    async def _check_has_unique_permissions(
        self,
        item: Dict[str, Any],
        item_type: str
    ) -> bool:
        """Check if an item has unique role assignments."""
        # Sites always have unique permissions (they're the root)
        if item_type == "site":
            return True

        # Check the has_unique_permissions flag first
        has_unique = (
            item.get("has_unique_role_assignments", False) or
            item.get("has_unique_permissions", False) or
            item.get("HasUniqueRoleAssignments", False)
        )

        # For items other than sites, we might need to check via API
        if not has_unique:
            try:
                site_url = await self._get_site_url_for_item(item, item_type)
                if site_url:
                    library_id = item.get("library_id")

                    if item_type == "library":
                        # For libraries, we need to check if they have unique permissions
                        # This would require a different API call, so for now we'll assume they inherit
                        # unless the flag is already set
                        pass
                    elif item_type in ["folder", "file"]:
                        item_id = item.get("id") or item.get("file_id") or item.get("folder_id")
                        if library_id and item_id:
                            has_unique = await self.sp_client.check_unique_permissions(
                                site_url, library_id, int(item_id)
                            )
            except Exception as e:
                logger.debug(f"Could not check unique permissions for {item_type} {item.get('id')}: {e}")

        return has_unique

    async def _get_unique_permissions(
        self,
        item: Dict[str, Any],
        item_type: str,
        permission_set: PermissionSet
    ):
        """Fetches and processes unique role assignments for an item."""
        try:
            # Get site URL for the item
            site_url = await self._get_site_url_for_item(item, item_type)
            if not site_url:
                raise ValueError(f"No site URL found for {item_type}: {item}")

            # Get role assignments from SharePoint
            logger.debug(f"Getting permissions for {item_type} with site_url: {site_url}")

            if item_type == "site":
                role_assignments = await self._run_api_task(
                    self.sp_client.get_site_permissions(site_url)
                )
                logger.debug(f"Site permissions response: {len(role_assignments)} role assignments")
            elif item_type == "library":
                library_id = item.get("library_id") or item.get("id")
                if library_id:
                    logger.debug(f"Getting library permissions for library_id: {library_id}")
                    role_assignments = await self._run_api_task(
                        self.sp_client.get_library_permissions(site_url, library_id)
                    )
                    logger.debug(f"Library permissions response: {len(role_assignments)} role assignments")
                else:
                    raise ValueError(f"Missing library_id for library: {item}")
            elif item_type in ["folder", "file"]:
                library_id = item.get("library_id")
                item_id = item.get("id") or item.get("file_id") or item.get("folder_id")
                if library_id and item_id:
                    # Convert item_id to int if it's numeric
                    try:
                        item_id_int = int(item_id)
                        logger.debug(f"Getting {item_type} permissions for library_id: {library_id}, item_id: {item_id_int}")
                        role_assignments = await self._run_api_task(
                            self.sp_client.get_item_permissions(site_url, library_id, item_id_int)
                        )
                        logger.debug(f"{item_type} permissions response: {len(role_assignments)} role assignments")
                    except (ValueError, TypeError):
                        # If item_id is not numeric, try as string
                        logger.warning(f"Item ID {item_id} is not numeric, skipping permissions")
                        role_assignments = []
                else:
                    raise ValueError(f"Missing required fields for {item_type}: {item}")
            else:
                logger.warning(f"Unknown item type: {item_type}")
                return

            # Process each role assignment
            logger.debug(f"Processing {len(role_assignments)} role assignments for {item_type}")
            for assignment in role_assignments:
                await self._process_role_assignment(assignment, permission_set)

            if not role_assignments:
                logger.warning(f"No role assignments returned for {item_type} {item.get('id')}")

        except SharePointAPIError as e:
            logger.error(f"Failed to get permissions for {item_type} {item.get('id')}: {e}")
            self.stats["errors"] += 1
            # Add default permission entry when API fails
            self._add_default_permission(permission_set, item, item_type)
        except Exception as e:
            logger.error(f"Unexpected error getting permissions for {item_type} {item.get('id')}: {e}")
            self.stats["errors"] += 1
            # Add default permission entry when API fails
            self._add_default_permission(permission_set, item, item_type)

    async def _get_inherited_permissions(
        self,
        item: Dict[str, Any],
        item_type: str,
        permission_set: PermissionSet
    ):
        """Traverses up the hierarchy to find the source of inherited permissions."""
        # Get parent item based on type
        parent_item = None
        parent_type = None

        if item_type == "file":
            # Parent is the folder or library
            if item.get("folder_id"):
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM folders WHERE folder_id = ?",
                    (item.get("folder_id"),)
                )
                parent_type = "folder"
            else:
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM libraries WHERE library_id = ?",
                    (item.get("library_id"),)
                )
                parent_type = "library"
        elif item_type == "folder":
            # Parent is the parent folder or library
            if item.get("parent_folder_id"):
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM folders WHERE folder_id = ?",
                    (item.get("parent_folder_id"),)
                )
                parent_type = "folder"
            else:
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM libraries WHERE library_id = ?",
                    (item.get("library_id"),)
                )
                parent_type = "library"
        elif item_type == "library":
            # Parent is the site
            site_id = item.get("site_id")
            if site_id:
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM sites WHERE site_id = ?",
                    (site_id,)
                )
                parent_type = "site"

        if parent_item:
            # Recursively get parent's permissions
            parent_permissions = await self.analyze_item_permissions(
                parent_item,
                parent_type
            )

            # Copy permissions from parent, marking them as inherited
            if parent_permissions.inheritance_source_id:
                permission_set.inheritance_source_id = parent_permissions.inheritance_source_id
                permission_set.inheritance_source_path = parent_permissions.inheritance_source_path
            else:
                permission_set.inheritance_source_id = parent_permissions.object_id
                permission_set.inheritance_source_path = parent_permissions.object_path

            for perm in parent_permissions.permissions:
                inherited_perm = PermissionEntry(
                    principal_id=perm.principal_id,
                    principal_name=perm.principal_name,
                    principal_type=perm.principal_type,
                    permission_level=perm.permission_level,
                    is_inherited=True,
                    granted_at=perm.granted_at,
                    granted_by=perm.granted_by,
                    inheritance_source=permission_set.inheritance_source_path,
                    is_external=perm.is_external,
                    is_anonymous_link=perm.is_anonymous_link
                )
                permission_set.add_permission(inherited_perm)

    async def _process_role_assignment(
        self,
        assignment: Dict[str, Any],
        permission_set: PermissionSet
    ):
        """Process a single role assignment."""
        logger.debug(f"Processing role assignment: {assignment}")
        member = assignment.get("Member", {})

        # Handle different response formats for role bindings
        role_bindings = assignment.get("RoleDefinitionBindings", [])
        if isinstance(role_bindings, dict) and "results" in role_bindings:
            role_bindings = role_bindings["results"]
        elif not isinstance(role_bindings, list):
            role_bindings = []

        principal_id = member.get("Id", "")
        principal_name = member.get("Title", member.get("LoginName", "Unknown"))
        principal_type_value = member.get("PrincipalType", 1)

        # Determine principal type
        principal_type = self._get_principal_type(principal_type_value, member)

        # Check if external
        is_external = self._is_external_user(member)
        is_anonymous = principal_type == PrincipalType.ANONYMOUS

        # Process each role binding
        for role in role_bindings:
            permission_level = role.get("Name", "Unknown")

            if principal_type == PrincipalType.GROUP:
                # Expand group membership
                await self._expand_and_add_group_permissions(
                    principal_id,
                    principal_name,
                    permission_level,
                    permission_set
                )
            else:
                # Add individual permission
                entry = PermissionEntry(
                    principal_id=principal_id,
                    principal_name=principal_name,
                    principal_type=principal_type,
                    permission_level=permission_level,
                    is_inherited=False,
                    granted_at=datetime.now(timezone.utc),
                    is_external=is_external,
                    is_anonymous_link=is_anonymous
                )
                permission_set.add_permission(entry)

    async def _expand_and_add_group_permissions(
        self,
        group_id: str,
        group_name: str,
        permission_level: str,
        permission_set: PermissionSet
    ):
        """Expand group membership and add permissions for all members."""
        try:
            # Get expanded group membership
            members = await self.expand_group_permissions(group_id)

            # Add permission for the group itself
            group_entry = PermissionEntry(
                principal_id=group_id,
                principal_name=group_name,
                principal_type=PrincipalType.GROUP,
                permission_level=permission_level,
                is_inherited=False,
                granted_at=datetime.now(timezone.utc)
            )
            permission_set.add_permission(group_entry)

            # Add permissions for each member
            for member in members:
                is_external = self._is_external_user(member)
                member_entry = PermissionEntry(
                    principal_id=member.get("id", ""),
                    principal_name=member.get("displayName", member.get("userPrincipalName", "Unknown")),
                    principal_type=PrincipalType.USER,
                    permission_level=permission_level,
                    is_inherited=False,
                    granted_at=datetime.now(timezone.utc),
                    granted_by=f"Group: {group_name}",
                    is_external=is_external
                )
                permission_set.add_permission(member_entry)

        except Exception as e:
            logger.error(f"Failed to expand group {group_id}: {e}")

    async def expand_group_permissions(self, group_id: str) -> List[Dict[str, Any]]:
        """Expands a group to get all its members, including nested groups."""
        cache_key = f"group_members:{group_id}"
        cached_members = await self.cache.get(cache_key)
        if cached_members:
            return cached_members

        try:
            # Use Graph API's transitiveMembers endpoint
            url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/transitiveMembers"
            members = []

            while url:
                response = await self._run_api_task(
                    self.graph_client.get_with_retry(url)
                )

                for member in response.get('value', []):
                    if member.get('@odata.type') == '#microsoft.graph.user':
                        members.append({
                            'id': member['id'],
                            'displayName': member.get('displayName', ''),
                            'userPrincipalName': member.get('userPrincipalName', ''),
                            'mail': member.get('mail', '')
                        })

                # Handle pagination
                url = response.get('@odata.nextLink')

            # Cache for 6 hours
            await self.cache.set(cache_key, members, ttl=21600)
            return members

        except Exception as e:
            logger.error(f"Failed to expand group {group_id}: {e}")
            return []

    async def _check_external_sharing(
        self,
        item: Dict[str, Any],
        item_type: str,
        permission_set: PermissionSet
    ):
        """Check for external sharing links and anonymous access."""
        try:
            site_url = await self._get_site_url_for_item(item, item_type)
            if not site_url:
                return

            item_url = item.get("server_relative_url", "")
            if not item_url:
                return

            # Get sharing links
            sharing_links = await self._run_api_task(
                self.sp_client.get_sharing_links(site_url, item_url)
            )

            for link in sharing_links:
                link_type = link.get("linkKind", "")
                if link_type in ["AnonymousAccess", "AnonymousEdit", "AnonymousView"]:
                    # Add anonymous link permission
                    anon_entry = PermissionEntry(
                        principal_id=link.get("shareId", "anonymous"),
                        principal_name="Anonymous Link",
                        principal_type=PrincipalType.ANONYMOUS,
                        permission_level="Read" if "View" in link_type else "Edit",
                        is_inherited=False,
                        granted_at=datetime.fromisoformat(link.get("createdDateTime", "").replace("Z", "+00:00")) if link.get("createdDateTime") else None,
                        is_anonymous_link=True
                    )
                    permission_set.add_permission(anon_entry)
                    self.stats["anonymous_links"] += 1

        except Exception as e:
            logger.debug(f"Could not check external sharing for {item_type} {item.get('id')}: {e}")

    async def _get_site_url_for_item(
        self,
        item: Dict[str, Any],
        item_type: str
    ) -> Optional[str]:
        """Get the site URL for an item."""
        # First check if site_url is already in the item
        site_url = item.get("site_url") or item.get("url") or item.get("web_url")
        if site_url:
            return site_url

        # For non-site items, fetch from database
        if item_type != "site":
            site_id = item.get("site_id")
            if site_id:
                site = await self.db_repo.fetch_one(
                    "SELECT url FROM sites WHERE site_id = ?",
                    (site_id,)
                )
                if site:
                    return site.get("url")

        return None

    def _get_principal_type(self, principal_type_value: int, member: Dict[str, Any]) -> PrincipalType:
        """Determine the principal type from SharePoint values."""
        # SharePoint principal type values:
        # 1 = User, 2 = DL, 4 = Security Group, 8 = SharePoint Group
        if principal_type_value == 1:
            # Check if external user
            login_name = member.get("LoginName", "")
            email = member.get("Email", "")
            if "#ext#" in login_name or "#EXT#" in login_name:
                return PrincipalType.EXTERNAL
            elif "urn:spo:guest" in login_name:
                return PrincipalType.EXTERNAL
            return PrincipalType.USER
        elif principal_type_value in [2, 4]:
            return PrincipalType.GROUP
        elif principal_type_value == 8:
            return PrincipalType.SHAREPOINT_GROUP
        else:
            return PrincipalType.USER

    def _is_external_user(self, member: Dict[str, Any]) -> bool:
        """Check if a user is external."""
        login_name = member.get("LoginName", "") or member.get("userPrincipalName", "")
        email = member.get("Email", "") or member.get("mail", "")

        external_indicators = [
            "#ext#", "#EXT#", "urn:spo:guest", "_external",
            "#guest#", "Guest User"
        ]

        for indicator in external_indicators:
            if indicator in login_name or indicator in email:
                return True

        # Check user type
        if member.get("userType") == "Guest":
            return True

        return False

    def _add_default_permission(
        self,
        permission_set: PermissionSet,
        item: Dict[str, Any],
        item_type: str
    ):
        """Add default permission entry when actual permissions cannot be retrieved."""
        entry = PermissionEntry(
            principal_id="system",
            principal_name="System Default",
            principal_type=PrincipalType.APPLICATION,
            permission_level="Unknown",
            is_inherited=True,
            granted_at=datetime.now(timezone.utc),
            inheritance_source="Unable to retrieve permissions",
            is_external=False,
            is_anonymous_link=False
        )
        permission_set.add_permission(entry)

    async def _cache_permission_set(self, cache_key: str, permission_set: PermissionSet):
        """Cache a permission set."""
        cache_data = {
            "object_type": permission_set.object_type,
            "object_id": permission_set.object_id,
            "object_path": permission_set.object_path,
            "has_unique_permissions": permission_set.has_unique_permissions,
            "inheritance_source_id": permission_set.inheritance_source_id,
            "inheritance_source_path": permission_set.inheritance_source_path,
            "external_users_count": permission_set.external_users_count,
            "anonymous_links_count": permission_set.anonymous_links_count,
            "permissions": [
                {
                    "principal_id": p.principal_id,
                    "principal_name": p.principal_name,
                    "principal_type": p.principal_type.value,
                    "permission_level": p.permission_level,
                    "is_inherited": p.is_inherited,
                    "granted_at": p.granted_at.isoformat() if p.granted_at else None,
                    "granted_by": p.granted_by,
                    "inheritance_source": p.inheritance_source,
                    "is_external": p.is_external,
                    "is_anonymous_link": p.is_anonymous_link
                }
                for p in permission_set.permissions
            ]
        }
        await self.cache.set(cache_key, cache_data, ttl=3600)

    def _reconstruct_permission_set(self, cached_data: Dict[str, Any]) -> PermissionSet:
        """Reconstruct a PermissionSet from cached data."""
        permission_set = PermissionSet(
            object_type=cached_data["object_type"],
            object_id=cached_data["object_id"],
            object_path=cached_data["object_path"],
            has_unique_permissions=cached_data["has_unique_permissions"],
            inheritance_source_id=cached_data.get("inheritance_source_id"),
            inheritance_source_path=cached_data.get("inheritance_source_path"),
            external_users_count=cached_data.get("external_users_count", 0),
            anonymous_links_count=cached_data.get("anonymous_links_count", 0)
        )

        # Reconstruct permissions
        for perm_data in cached_data.get("permissions", []):
            perm = PermissionEntry(
                principal_id=perm_data["principal_id"],
                principal_name=perm_data["principal_name"],
                principal_type=PrincipalType(perm_data["principal_type"]),
                permission_level=perm_data["permission_level"],
                is_inherited=perm_data["is_inherited"],
                granted_at=datetime.fromisoformat(perm_data["granted_at"]) if perm_data.get("granted_at") else None,
                granted_by=perm_data.get("granted_by"),
                inheritance_source=perm_data.get("inheritance_source"),
                is_external=perm_data.get("is_external", False),
                is_anonymous_link=perm_data.get("is_anonymous_link", False)
            )
            permission_set.permissions.append(perm)

        return permission_set

    async def _run_api_task(self, coro):
        """Run an API task with the concurrency manager."""
        return await self.concurrency_manager.run_api_task(coro)

    def get_statistics(self) -> Dict[str, int]:
        """Get current statistics."""
        return self.stats.copy()
