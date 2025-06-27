"""Core module containing the main audit engine components."""

from .pipeline import AuditPipeline, PipelineContext, PipelineStage, ParallelProcessor
from .pipeline_metrics import PipelineMetrics
from .processors import (
    DiscoveryStage,
    ValidationStage,
    TransformationStage,
    EnrichmentStage,
    StorageStage,
    DataProcessor,
    ProcessingResult,
    PermissionAnalysisStage
)
from .progress_tracker import ProgressTracker
from .discovery import DiscoveryModule
from .models import (
    AuditItem,
    Site,
    Library,
    Folder,
    File,
    SiteContent,
    DeltaResult,
    SharePointItem
)

__all__ = [
    # Pipeline framework
    "AuditPipeline",
    "PipelineContext",
    "PipelineStage",
    "ParallelProcessor",

    # Pipeline stages
    "DiscoveryStage",
    "ValidationStage",
    "TransformationStage",
    "EnrichmentStage",
    "StorageStage",
    "PermissionAnalysisStage",

    # Data processing
    "DataProcessor",
    "ProcessingResult",

    # Discovery
    "DiscoveryModule",

    # Models
    "AuditItem",
    "Site",
    "Library",
    "Folder",
    "File",
    "SiteContent",
    "DeltaResult",
    "SharePointItem",

    # Metrics and tracking
    "PipelineMetrics",
    "ProgressTracker",
]
