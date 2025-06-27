"""Data models for the audit pipeline."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class AuditItem:
    """Base class for audit items."""
    id: str
    type: str
    name: Optional[str] = None
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "modified_at": self.modified_at.isoformat() if self.modified_at else None,
        }


@dataclass
class Site(AuditItem):
    """Represents a SharePoint site."""
    url: str
    title: Optional[str] = None
    description: Optional[str] = None
    storage_used: Optional[int] = None
    storage_quota: Optional[int] = None
    is_hub_site: bool = False
    hub_site_id: Optional[str] = None

    def __post_init__(self):
        self.type = "Site"

    @classmethod
    def from_graph_response(cls, data: Dict[str, Any]) -> "Site":
        """Create a Site from Graph API response."""
        return cls(
            id=data.get("id"),
            url=data.get("webUrl", ""),
            title=data.get("displayName") or data.get("name"),
            description=data.get("description"),
            created_at=cls._parse_datetime(data.get("createdDateTime")),
            modified_at=cls._parse_datetime(data.get("lastModifiedDateTime")),
        )

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO datetime string."""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        except ValueError:
            return None


@dataclass
class Library(AuditItem):
    """Represents a document library."""
    site_id: str
    description: Optional[str] = None
    item_count: int = 0
    is_hidden: bool = False
    enable_versioning: bool = True
    enable_minor_versions: bool = False

    def __post_init__(self):
        self.type = "Library"


@dataclass
class Folder(AuditItem):
    """Represents a folder."""
    library_id: str
    parent_folder_id: Optional[str] = None
    server_relative_url: str = ""
    item_count: int = 0
    has_unique_permissions: bool = False
    created_by: Optional[str] = None
    modified_by: Optional[str] = None

    def __post_init__(self):
        self.type = "Folder"


@dataclass
class File(AuditItem):
    """Represents a file."""
    library_id: str
    folder_id: Optional[str] = None
    server_relative_url: str = ""
    size_bytes: int = 0
    content_type: Optional[str] = None
    created_by: Optional[str] = None
    modified_by: Optional[str] = None
    version: str = "1.0"
    is_checked_out: bool = False
    checked_out_by: Optional[str] = None
    has_unique_permissions: bool = False

    def __post_init__(self):
        self.type = "File"


@dataclass
class SiteContent:
    """Container for all content within a site."""
    site_id: str
    libraries: List[Library] = field(default_factory=list)
    lists: List[Dict[str, Any]] = field(default_factory=list)
    subsites: List[Site] = field(default_factory=list)
    folders: List[Folder] = field(default_factory=list)
    files: List[File] = field(default_factory=list)


@dataclass
class DeltaResult:
    """Result of a delta query operation."""
    items: List[Any]
    delta_token: Optional[str] = None

    def __len__(self) -> int:
        return len(self.items)


@dataclass
class SharePointItem:
    """Generic SharePoint item for permission analysis."""
    id: str
    type: str  # 'site', 'library', 'folder', 'file'
    site_url: str
    has_unique_permissions: bool = False
    parent_id: Optional[str] = None
