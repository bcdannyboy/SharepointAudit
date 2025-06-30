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
        cache_manager: CacheManager,
        concurrency_manager: "ConcurrencyManager | None" = None,
    ):
        self.graph_client = graph_client
        self.sp_client = sp_client
        self.db_repo = db_repo
        self.cache = cache_manager
        self.concurrency_manager = concurrency_manager
        self._permission_cache: Dict[str, PermissionSet] = {}

    async def _run_api_task(self, coro):
        if self.concurrency_manager:
            return await self.concurrency_manager.run_api_task(coro)
        return await coro

    async def analyze_item_permissions(
        self,
        item: Dict[str, Any],
        item_type: str
    ) -> PermissionSet:
        """
        Analyzes permissions for a specific SharePoint item.

        Args:
            item: The SharePoint item (site, library, folder, or file)
            item_type: Type of the item (site, library, folder, file)

        Returns:
            PermissionSet containing all resolved permissions
        """
        item_id = item.get("id") or item.get("site_id") or item.get("library_id")
        item_path = item.get("web_url") or item.get("path") or ""

        # Check cache first
        cache_key = f"permissions:{item_type}:{item_id}"
        cached_permissions = await self.cache.get(cache_key)
        if cached_permissions:
            # Reconstruct PermissionSet from cached data
            permission_set = PermissionSet(
                object_type=cached_permissions["object_type"],
                object_id=cached_permissions["object_id"],
                object_path=cached_permissions["object_path"],
                has_unique_permissions=cached_permissions["has_unique_permissions"],
                inheritance_source_id=cached_permissions.get("inheritance_source_id"),
                inheritance_source_path=cached_permissions.get("inheritance_source_path"),
                external_users_count=cached_permissions.get("external_users_count", 0),
                anonymous_links_count=cached_permissions.get("anonymous_links_count", 0)
            )

            # Reconstruct permissions
            for perm_data in cached_permissions.get("permissions", []):
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
        else:
            # Get inherited permissions from parent
            await self._get_inherited_permissions(item, item_type, permission_set)

        # Cache the result - convert to serializable format
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

        return permission_set

    async def _check_has_unique_permissions(
        self,
        item: Dict[str, Any],
        item_type: str
    ) -> bool:
        """Check if an item has unique role assignments."""
        # This is typically indicated by the has_unique_role_assignments property
        # or has_unique_permissions flag in the item data
        return (
            item.get("has_unique_role_assignments", False) or
            item.get("has_unique_permissions", False) or
            item.get("HasUniqueRoleAssignments", False)
        )

    async def _get_unique_permissions(
        self,
        item: Dict[str, Any],
        item_type: str,
        permission_set: PermissionSet
    ):
        """Fetches and processes unique role assignments for an item."""
        try:
            # Get role assignments from SharePoint
            if item_type == "site":
                role_assignments = await self._run_api_task(
                    self.sp_client.get_site_permissions(
                        item.get("site_url")
                    )
                )
            elif item_type == "library":
                role_assignments = await self._run_api_task(
                    self.sp_client.get_library_permissions(
                        item.get("site_url"),
                        item.get("library_id")
                    )
                )
            elif item_type in ["folder", "file"]:
                role_assignments = await self._run_api_task(
                    self.sp_client.get_item_permissions(
                        item.get("site_url"),
                        item.get("library_id"),
                        item.get("id")
                    )
                )
            else:
                logger.warning(f"Unknown item type: {item_type}")
                return

            # Process each role assignment
            for assignment in role_assignments:
                await self._process_role_assignment(assignment, permission_set)

        except SharePointAPIError as e:
            logger.error(f"Failed to get permissions for {item_type} {item.get('id')}: {e}")

    async def _get_inherited_permissions(
        self,
        item: Dict[str, Any],
        item_type: str,
        permission_set: PermissionSet
    ):
        """Traverses up the hierarchy to find the source of inherited permissions."""
        # Determine parent based on item type
        parent_item = None
        parent_type = None

        if item_type == "file":
            # Parent is the folder or library
            if item.get("parent_folder_id"):
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM folders WHERE id = ?",
                    (item.get("parent_folder_id"),)
                )
                parent_type = "folder"
            else:
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM libraries WHERE id = ?",
                    (item.get("library_id"),)
                )
                parent_type = "library"
        elif item_type == "folder":
            # Parent is the parent folder or library
            if item.get("parent_folder_id"):
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM folders WHERE id = ?",
                    (item.get("parent_folder_id"),)
                )
                parent_type = "folder"
            else:
                parent_item = await self.db_repo.fetch_one(
                    "SELECT * FROM libraries WHERE id = ?",
                    (item.get("library_id"),)
                )
                parent_type = "library"
        elif item_type == "library":
            # Parent is the site
            parent_item = await self.db_repo.fetch_one(
                "SELECT * FROM sites WHERE id = ?",
                (item.get("site_id"),)
            )
            parent_type = "site"

        if parent_item:
            # Recursively get parent's permissions
            parent_permissions = await self.analyze_item_permissions(
                parent_item,
                parent_type
            )

            # Copy permissions from parent, marking them as inherited
            # If parent also has inherited permissions, use its source
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
                    inheritance_source=parent_permissions.object_path,
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
        principal = assignment.get("Member") or assignment.get("principal") or {}
        principal_id = principal.get("Id") or principal.get("id")
        principal_name = principal.get("Title") or principal.get("title") or principal.get("LoginName")
        principal_type_value = principal.get("PrincipalType") or principal.get("principalType") or 1

        # Determine principal type
        principal_type = self._get_principal_type(principal_type_value)

        # Get permission levels from role definitions
        role_bindings = assignment.get("RoleDefinitionBindings") or []
        for role in role_bindings:
            permission_level = role.get("Name") or role.get("name") or "Unknown"

            # Check if external user
            is_external = await self._check_if_external(principal_name, principal_type)

            # Check if anonymous link
            is_anonymous = self._check_if_anonymous_link(principal, assignment)

            permission_entry = PermissionEntry(
                principal_id=str(principal_id),
                principal_name=principal_name,
                principal_type=principal_type,
                permission_level=permission_level,
                is_inherited=False,
                granted_at=datetime.now(timezone.utc),
                is_external=is_external,
                is_anonymous_link=is_anonymous
            )

            # If it's a group, expand members
            if principal_type == PrincipalType.GROUP:
                await self._expand_group_permissions(
                    principal_id,
                    principal_name,
                    permission_level,
                    permission_set
                )

            permission_set.add_permission(permission_entry)

    def _get_principal_type(self, type_value: int) -> PrincipalType:
        """Map SharePoint principal type number to enum."""
        # SharePoint principal types:
        # 1 = User, 2 = DL, 4 = SecurityGroup, 8 = SharePointGroup
        type_map = {
            1: PrincipalType.USER,
            2: PrincipalType.GROUP,
            4: PrincipalType.GROUP,
            8: PrincipalType.SHAREPOINT_GROUP,
            16: PrincipalType.APPLICATION
        }
        return type_map.get(type_value, PrincipalType.USER)

    async def _check_if_external(
        self,
        principal_name: str,
        principal_type: PrincipalType
    ) -> bool:
        """Check if a principal is an external user."""
        if principal_type != PrincipalType.USER:
            return False

        # Common patterns for external users
        if any(pattern in principal_name.lower() for pattern in ["#ext#", "_external_", "guest"]):
            return True

        # Check via Graph API if it looks like an email
        if "@" in principal_name:
            try:
                return await self._run_api_task(
                    self.graph_client.check_external_user(principal_name)
                )
            except Exception as e:
                logger.debug(f"Could not check external status for {principal_name}: {e}")

        return False

    def _check_if_anonymous_link(
        self,
        principal: Dict[str, Any],
        assignment: Dict[str, Any]
    ) -> bool:
        """Check if this is an anonymous sharing link."""
        # Anonymous links typically have specific properties
        return (
            principal.get("IsAnonymousGuestUser", False) or
            assignment.get("IsAnonymousLink", False) or
            "anonymous" in principal.get("Title", "").lower()
        )

    async def expand_group_permissions(
        self,
        group_id: str
    ) -> GroupMembership:
        """
        Expands a group to get all its members, including nested groups.

        Uses caching to avoid repeated API calls for the same group.
        """
        cache_key = f"group_members:{group_id}"
        cached_members = await self.cache.get(cache_key)
        if cached_members:
            # Reconstruct GroupMembership from cached data
            return GroupMembership(
                group_id=cached_members["group_id"],
                group_name=cached_members["group_name"],
                members=cached_members["members"],
                nested_groups=cached_members["nested_groups"],
                total_member_count=cached_members["total_member_count"],
                last_expanded=datetime.fromisoformat(cached_members["last_expanded"])
            )

        try:
            # Use Graph API's transitiveMembers endpoint
            members = await self._run_api_task(
                self.graph_client.expand_group_members_transitive(group_id)
            )

            # Separate users and nested groups
            users = []
            nested_groups = []

            for member in members:
                member_type = member.get("@odata.type", "")
                if "user" in member_type.lower():
                    users.append(member)
                elif "group" in member_type.lower():
                    nested_groups.append(member.get("id"))

            # Get group info
            group_info = await self._run_api_task(
                self.graph_client.get_group_info(group_id)
            )

            membership = GroupMembership(
                group_id=group_id,
                group_name=group_info.get("displayName", "Unknown Group"),
                members=users,
                nested_groups=nested_groups,
                total_member_count=len(users),
                last_expanded=datetime.now(timezone.utc)
            )

            # Cache for 6 hours - convert to serializable format
            cache_data = {
                "group_id": membership.group_id,
                "group_name": membership.group_name,
                "members": membership.members,
                "nested_groups": membership.nested_groups,
                "total_member_count": membership.total_member_count,
                "last_expanded": membership.last_expanded.isoformat()
            }
            await self.cache.set(cache_key, cache_data, ttl=21600)

            return membership

        except Exception as e:
            logger.error(f"Failed to expand group {group_id}: {e}")
            # Return empty membership on error
            return GroupMembership(
                group_id=group_id,
                group_name="Unknown Group",
                members=[],
                nested_groups=[],
                total_member_count=0,
                last_expanded=datetime.now(timezone.utc)
            )

    async def _expand_group_permissions(
        self,
        group_id: str,
        group_name: str,
        permission_level: str,
        permission_set: PermissionSet
    ):
        """Expand group members and add them as individual permissions."""
        try:
            membership = await self.expand_group_permissions(group_id)

            # Add each member as an individual permission
            for member in membership.members:
                user_principal_name = member.get("userPrincipalName", "")
                is_external = await self._check_if_external(
                    user_principal_name,
                    PrincipalType.USER
                )

                member_entry = PermissionEntry(
                    principal_id=member.get("id", ""),
                    principal_name=user_principal_name,
                    principal_type=PrincipalType.USER,
                    permission_level=permission_level,
                    is_inherited=permission_set.inheritance_source_id is not None,
                    granted_at=datetime.now(timezone.utc),
                    granted_by=f"Via group: {group_name}",
                    is_external=is_external,
                    is_anonymous_link=False
                )

                permission_set.add_permission(member_entry)

        except Exception as e:
            logger.error(f"Failed to expand group {group_id}: {e}")

    def detect_external_sharing(self, permission_set: PermissionSet) -> Dict[str, Any]:
        """
        Analyzes a permission set to identify external sharing patterns.

        Returns:
            Dictionary with external sharing statistics and details
        """
        external_users = []
        anonymous_links = []
        external_domains = set()

        for perm in permission_set.permissions:
            if perm.is_external:
                external_users.append({
                    "name": perm.principal_name,
                    "permission": perm.permission_level,
                    "granted_by": perm.granted_by
                })

                # Extract domain from email
                if "@" in perm.principal_name:
                    domain = perm.principal_name.split("@")[1]
                    external_domains.add(domain)

            if perm.is_anonymous_link:
                anonymous_links.append({
                    "permission": perm.permission_level,
                    "granted_at": perm.granted_at.isoformat() if perm.granted_at else None
                })

        return {
            "has_external_sharing": len(external_users) > 0 or len(anonymous_links) > 0,
            "external_users_count": len(external_users),
            "external_users": external_users,
            "anonymous_links_count": len(anonymous_links),
            "anonymous_links": anonymous_links,
            "external_domains": list(external_domains),
            "risk_level": self._calculate_risk_level(
                len(external_users),
                len(anonymous_links)
            )
        }

    def _calculate_risk_level(
        self,
        external_users_count: int,
        anonymous_links_count: int
    ) -> str:
        """Calculate risk level based on external sharing."""
        if anonymous_links_count > 0:
            return "HIGH"
        elif external_users_count > 10:
            return "MEDIUM"
        elif external_users_count > 0:
            return "LOW"
        else:
            return "NONE"

    async def analyze_permissions_batch(
        self,
        items: List[Dict[str, Any]],
        item_type: str,
        max_concurrent: int = 10
    ) -> List[PermissionSet]:
        """
        Analyze permissions for multiple items concurrently.

        Args:
            items: List of items to analyze
            item_type: Type of the items
            max_concurrent: Maximum concurrent analyses

        Returns:
            List of PermissionSet objects
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def analyze_with_semaphore(item):
            async with semaphore:
                return await self.analyze_item_permissions(item, item_type)

        tasks = [analyze_with_semaphore(item) for item in items]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def generate_permission_report(
        self,
        permission_sets: List[PermissionSet]
    ) -> Dict[str, Any]:
        """Generate a summary report of permissions."""
        total_items = len(permission_sets)
        unique_permission_items = sum(
            1 for ps in permission_sets if ps.has_unique_permissions
        )

        external_sharing_items = sum(
            1 for ps in permission_sets
            if ps.external_users_count > 0 or ps.anonymous_links_count > 0
        )

        total_external_users = sum(ps.external_users_count for ps in permission_sets)
        total_anonymous_links = sum(ps.anonymous_links_count for ps in permission_sets)

        # Group by permission levels
        permission_level_counts = {}
        for ps in permission_sets:
            for perm in ps.permissions:
                level = perm.permission_level
                permission_level_counts[level] = permission_level_counts.get(level, 0) + 1

        return {
            "summary": {
                "total_items_analyzed": total_items,
                "items_with_unique_permissions": unique_permission_items,
                "items_with_external_sharing": external_sharing_items,
                "total_external_users": total_external_users,
                "total_anonymous_links": total_anonymous_links,
                "unique_permission_percentage": (
                    unique_permission_items / total_items * 100
                    if total_items > 0 else 0
                )
            },
            "permission_levels": permission_level_counts,
            "risk_summary": {
                "high_risk_items": sum(
                    1 for ps in permission_sets
                    if ps.anonymous_links_count > 0
                ),
                "medium_risk_items": sum(
                    1 for ps in permission_sets
                    if ps.external_users_count > 10 and ps.anonymous_links_count == 0
                ),
                "low_risk_items": sum(
                    1 for ps in permission_sets
                    if 0 < ps.external_users_count <= 10 and ps.anonymous_links_count == 0
                )
            }
        }
