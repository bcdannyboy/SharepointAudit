"""Tests for the data processing pipeline."""

import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from src.core.pipeline import AuditPipeline, PipelineContext, PipelineStage, ParallelProcessor
from src.core.processors import (
    DiscoveryStage,
    ValidationStage,
    TransformationStage,
    EnrichmentStage,
    StorageStage,
    DataProcessor,
    ProcessingResult,
    PermissionAnalysisStage
)
from src.core.pipeline_metrics import PipelineMetrics
from src.database.repository import DatabaseRepository
from src.utils.checkpoint_manager import CheckpointManager


class MockStage(PipelineStage):
    """Mock stage for testing."""

    def __init__(self, name: str, side_effect=None):
        super().__init__(name)
        self.executed = False
        self.side_effect = side_effect

    async def execute(self, context: PipelineContext) -> PipelineContext:
        self.executed = True
        if self.side_effect:
            self.side_effect(context)
        return context


@pytest.fixture
def pipeline_context():
    """Create a test pipeline context."""
    return PipelineContext(
        run_id="test_run_123",
        config={"test": True},
        metrics=PipelineMetrics(),
        checkpoint_manager=AsyncMock(spec=CheckpointManager),
        db_repository=AsyncMock(spec=DatabaseRepository)
    )


@pytest.fixture
def mock_pipeline_stages():
    """Create mock pipeline stages."""
    return [
        MockStage("stage1"),
        MockStage("stage2"),
        MockStage("stage3")
    ]


@pytest.fixture
def audit_pipeline(pipeline_context):
    """Create an audit pipeline instance."""
    return AuditPipeline(pipeline_context)


@pytest.fixture
def mock_discovery_module():
    """Create a mock discovery module."""
    module = AsyncMock()
    module.run_discovery = AsyncMock()
    return module


@pytest.fixture
def mock_db_repo():
    """Create a mock database repository."""
    repo = AsyncMock(spec=DatabaseRepository)
    repo.bulk_insert = AsyncMock(return_value=100)
    repo.fetch_all = AsyncMock(return_value=[])
    return repo


@pytest.mark.asyncio
async def test_pipeline_executes_stages_in_order(audit_pipeline, mock_pipeline_stages):
    """Verify the pipeline executes its stages sequentially."""
    # Add stages to pipeline
    for stage in mock_pipeline_stages:
        audit_pipeline.add_stage(stage)

    # Run pipeline
    result = await audit_pipeline.run()

    # Check that all stages were executed
    for stage in mock_pipeline_stages:
        assert stage.executed is True

    # Verify context was returned
    assert result == audit_pipeline.context


@pytest.mark.asyncio
async def test_pipeline_handles_stage_failure(audit_pipeline, pipeline_context):
    """Test that pipeline handles stage failures gracefully."""
    # Create a failing stage
    failing_stage = MockStage("failing_stage")
    failing_stage.execute = AsyncMock(side_effect=Exception("Stage failed"))

    audit_pipeline.add_stage(failing_stage)

    # Run pipeline - should raise exception
    with pytest.raises(Exception, match="Stage failed"):
        await audit_pipeline.run()

    # Check that error was recorded
    assert len(pipeline_context.errors) == 1
    assert "Stage failing_stage: Stage failed" in pipeline_context.errors[0]


@pytest.mark.asyncio
async def test_pipeline_checkpoint_resume(audit_pipeline, pipeline_context):
    """Test that pipeline can resume from checkpoint."""
    # Mock checkpoint manager to indicate stage1 was completed
    async def restore_checkpoint_side_effect(run_id, key):
        if key == "pipeline_last_completed_stage":
            return "stage1"
        elif key == "stage_stage1_status":
            return "completed"
        return None

    pipeline_context.checkpoint_manager.restore_checkpoint.side_effect = restore_checkpoint_side_effect

    # Create stages
    stage1 = MockStage("stage1")
    stage2 = MockStage("stage2")
    stage3 = MockStage("stage3")

    audit_pipeline.add_stage(stage1)
    audit_pipeline.add_stage(stage2)
    audit_pipeline.add_stage(stage3)

    # Run pipeline
    await audit_pipeline.run()

    # Stage1 should not have been executed (already completed)
    assert stage1.executed is False
    assert stage2.executed is True
    assert stage3.executed is True


@pytest.mark.asyncio
async def test_pipeline_metrics_collection(audit_pipeline, pipeline_context):
    """Test that pipeline collects metrics correctly."""
    # Create a stage that modifies metrics
    def update_metrics(context):
        context.metrics.record_stage_items("test_stage", 100)
        context.metrics.set_custom_metric("test_metric", 42)

    stage = MockStage("test_stage", side_effect=update_metrics)
    audit_pipeline.add_stage(stage)

    # Run pipeline
    await audit_pipeline.run()

    # Check metrics
    assert pipeline_context.metrics.stage_item_counts["test_stage"] == 100
    assert pipeline_context.metrics.custom_metrics["test_metric"] == 42
    assert "test_stage" in pipeline_context.metrics.stage_durations


@pytest.mark.asyncio
async def test_discovery_stage(mock_discovery_module, mock_db_repo):
    """Test the discovery stage."""
    # Setup mock data
    mock_db_repo.fetch_all.side_effect = [
        [{"site_id": "1", "url": "https://test.com"}],  # sites
        [{"library_id": "1", "name": "Docs"}],  # libraries
        [{"file_id": "1", "name": "test.txt"}],  # files
        [{"folder_id": "1", "name": "Folder1"}],  # folders
    ]

    context = PipelineContext(
        run_id="test_run",
        db_repository=mock_db_repo,
        metrics=PipelineMetrics()
    )

    stage = DiscoveryStage(mock_discovery_module)

    # Execute stage
    result = await stage.execute(context)

    # Verify discovery was called
    mock_discovery_module.run_discovery.assert_called_once_with("test_run")

    # Verify data was fetched
    assert len(result.sites) == 1
    assert len(result.libraries) == 1
    assert len(result.files) == 1
    assert len(result.folders) == 1
    assert result.total_items == 4


def test_validation_stage():
    """Test the validation stage."""
    async def run():
        context = PipelineContext(run_id="test_run", metrics=PipelineMetrics())
        context.sites = [
            {"site_id": "1", "url": "https://test.sharepoint.com/sites/site1"},
            {"site_id": "2", "url": "invalid-url"},  # Invalid URL
            {"url": "https://test.sharepoint.com/sites/site3"}  # Missing site_id
        ]
        context.files = [
            {"file_id": "1", "name": "test.txt", "server_relative_url": "/test.txt"},
            {"name": "missing_id.txt", "server_relative_url": "/missing.txt"}  # Missing file_id
        ]

        stage = ValidationStage()
        result = await stage.execute(context)

        # Should have validation errors
        assert len(context.errors) > 0
        assert any("Invalid site URL" in error for error in context.errors)
        assert any("missing required field" in error for error in context.errors)

    asyncio.run(run())


def test_transformation_stage():
    """Test the transformation stage."""
    async def run():
        context = PipelineContext(run_id="test_run", metrics=PipelineMetrics())
        context.raw_data = [
            {
                "name": "test.docx",
                "createdDateTime": "2023-01-01T00:00:00Z",
                "url": "https://tenant.sharepoint.com/sites/test"
            }
        ]
        context.files = [
            {
                "file_id": "1",
                "name": "document.pdf",
                "size_bytes": "12345",
                "created_at": "2023-01-01T00:00:00Z"
            }
        ]

        stage = TransformationStage()
        result = await stage.execute(context)

        # Check transformations
        assert len(result.processed_data) == 1
        item = result.processed_data[0]
        assert item["name_normalized"] == "TEST.DOCX"
        assert item["file_extension"] == ".docx"
        assert isinstance(item["createdDateTime"], datetime)

        # Check file transformations
        file = result.files[0]
        assert file["size_bytes"] == 12345  # Converted to int
        assert isinstance(file["created_at"], datetime)

    asyncio.run(run())


def test_enrichment_stage():
    """Test the enrichment stage."""
    async def run():
        context = PipelineContext(run_id="test_run", metrics=PipelineMetrics())
        context.processed_data = [
            {
                "name": "old_file.pdf",
                "created_at": datetime(2020, 1, 1),
                "server_relative_url": "/sites/test/docs/folder1/folder2/file.pdf"
            }
        ]
        context.files = [
            {
                "file_id": "1",
                "name": "large_file.zip",
                "size_bytes": 5 * 1024 * 1024,  # 5 MB
                "created_at": datetime(2023, 1, 1),
                "server_relative_url": "/sites/test/file.zip"
            }
        ]

        stage = EnrichmentStage()
        result = await stage.execute(context)

        # Check enrichments
        item = result.processed_data[0]
        assert "age_days" in item
        assert item["age_category"] == "Archived"  # Over 2 years old
        assert item["path_depth"] == 5  # Number of path segments

        # Check file enrichments
        file = result.files[0]
        assert file["size_category"] == "Small"  # 5 MB
        assert file["file_type"] == "Archive"  # .zip file
        assert "age_days" in file

    asyncio.run(run())


@pytest.mark.asyncio
async def test_storage_stage(mock_db_repo):
    """Test the storage stage."""
    context = PipelineContext(
        run_id="test_run",
        db_repository=mock_db_repo,
        metrics=PipelineMetrics()
    )
    context.permissions = [{"object_id": "1", "permission_level": "Read"}] * 10

    stage = StorageStage(mock_db_repo)
    await stage.execute(context)

    # Verify bulk insert was called for permissions
    mock_db_repo.bulk_insert.assert_called_with("permissions", context.permissions[:1000])


def test_data_processor_transforms_file_data():
    """Verify the data processor correctly transforms raw file data."""
    async def run():
        db_repo = AsyncMock(spec=DatabaseRepository)
        processor = DataProcessor(db_repo)

        raw_files = [
            {
                "type": "File",
                "id": "file1",
                "name": "MyReport.docx",
                "size": 12345,
                "createdDateTime": "2023-01-01T00:00:00Z",
                "webUrl": "/sites/test/MyReport.docx",
                "hasUniquePermissions": True,
                "permissions": [
                    {
                        "role": "Edit",
                        "grantedTo": {
                            "user": {
                                "id": "user1",
                                "email": "user@example.com"
                            }
                        }
                    }
                ]
            }
        ]

        # Process files
        processor_result = processor._process_files(raw_files)

        assert processor_result.file_count == 1
        assert processor_result.permission_count == 1

        file_record = processor_result.file_records[0]
        assert file_record['name'] == 'MyReport.docx'
        assert file_record['size_bytes'] == 12345
        assert file_record['has_unique_permissions'] is True

        perm_record = processor_result.permission_records[0]
        assert perm_record['object_type'] == 'file'
        assert perm_record['principal_type'] == 'user'
        assert perm_record['principal_name'] == 'user@example.com'

    asyncio.run(run())


@pytest.mark.asyncio
async def test_parallel_processor():
    """Test the parallel processor."""
    processor = ParallelProcessor(max_workers=5)

    # Create test items
    items = list(range(20))

    # Define a simple processor function
    async def process_item(item):
        await asyncio.sleep(0.01)  # Simulate work
        return item * 2

    # Process items
    results = await processor.process_items_parallel(
        items,
        process_item,
        batch_size=5
    )

    # Verify results
    assert len(results) == 20
    assert sorted(results) == [i * 2 for i in range(20)]


@pytest.mark.asyncio
async def test_parallel_processor_error_handling():
    """Test that parallel processor handles errors gracefully."""
    processor = ParallelProcessor(max_workers=5)

    # Create test items
    items = list(range(10))

    # Define a processor function that fails for some items
    async def process_item(item):
        if item == 5:
            raise ValueError("Item 5 failed")
        return item * 2

    # Process items
    results = await processor.process_items_parallel(
        items,
        process_item,
        batch_size=3
    )

    # Should have 9 results (10 - 1 failed)
    assert len(results) == 9
    assert 10 not in results  # 5 * 2 should be missing


def test_processing_result_merge():
    """Test that ProcessingResult can merge correctly."""
    result1 = ProcessingResult()
    result1.file_count = 10
    result1.file_records = [{"id": "1"}, {"id": "2"}]
    result1.errors = ["Error 1"]

    result2 = ProcessingResult()
    result2.file_count = 5
    result2.file_records = [{"id": "3"}]
    result2.permission_count = 3
    result2.errors = ["Error 2"]

    # Merge result2 into result1
    result1.merge(result2)

    assert result1.file_count == 15
    assert len(result1.file_records) == 3
    assert result1.permission_count == 3
    assert len(result1.errors) == 2


@pytest.mark.asyncio
async def test_permission_analysis_stage_without_analyzer():
    """Test permission analysis stage when no analyzer is configured."""
    context = PipelineContext(run_id="test_run")
    context.processed_data = [
        {"id": "1", "has_unique_permissions": True},
        {"id": "2", "has_unique_permissions": False}
    ]

    stage = PermissionAnalysisStage(permission_analyzer=None)
    result = await stage.execute(context)

    # Should complete without error but not analyze permissions
    assert result == context
    assert len(context.permissions) == 0


@pytest.mark.asyncio
async def test_full_pipeline_integration():
    """Test a full pipeline with multiple stages."""
    # Create context with mocks
    mock_db_repo = AsyncMock(spec=DatabaseRepository)
    mock_db_repo.bulk_insert = AsyncMock(return_value=10)
    mock_db_repo.fetch_all = AsyncMock(return_value=[])

    context = PipelineContext(
        run_id="integration_test",
        metrics=PipelineMetrics(),
        checkpoint_manager=AsyncMock(spec=CheckpointManager),
        db_repository=mock_db_repo
    )

    # Mock checkpoint manager to return None for all checkpoints
    context.checkpoint_manager.restore_checkpoint.return_value = None

    # Create pipeline
    pipeline = AuditPipeline(context)

    # Add stages
    discovery_module = AsyncMock()
    discovery_module.run_discovery = AsyncMock()

    pipeline.add_stage(DiscoveryStage(discovery_module))
    pipeline.add_stage(ValidationStage())
    pipeline.add_stage(TransformationStage())
    pipeline.add_stage(EnrichmentStage())
    pipeline.add_stage(StorageStage(context.db_repository))

    # Add some test data to process
    context.raw_data = [
        {
            "name": "test.docx",
            "createdDateTime": "2023-01-01T00:00:00Z",
            "size": 1000
        }
    ]

    # Run pipeline
    result = await pipeline.run()

    # Verify pipeline completed
    assert result.metrics.total_duration > 0
    assert len(result.metrics.stage_durations) == 5

    # Verify checkpoint saves
    assert context.checkpoint_manager.save_checkpoint.called


@pytest.mark.asyncio
async def test_pipeline_metrics_summary():
    """Test pipeline metrics summary generation."""
    metrics = PipelineMetrics()

    # Simulate pipeline execution
    metrics.start_timer()

    with metrics.measure_stage("discovery"):
        metrics.record_stage_items("discovery", 100)
        await asyncio.sleep(0.01)

    with metrics.measure_stage("processing"):
        metrics.record_stage_items("processing", 90)
        metrics.record_stage_error("processing")
        await asyncio.sleep(0.01)

    metrics.set_custom_metric("total_size_gb", 50.5)
    metrics.stop_timer()

    # Get summary
    summary = metrics.get_summary()

    assert summary["total_items_processed"] == 190
    assert summary["total_items_failed"] == 1
    assert summary["success_rate"] > 99
    assert "discovery" in summary["stages"]
    assert "processing" in summary["stages"]
    assert summary["stages"]["discovery"]["items_processed"] == 100
    assert summary["stages"]["processing"]["errors"] == 1
    assert summary["custom_metrics"]["total_size_gb"] == 50.5


def test_transformation_date_parsing():
    """Test various date format parsing in transformation stage."""
    async def run():
        stage = TransformationStage()

        # Test different date formats
        test_dates = [
            "2023-01-01T00:00:00Z",
            "2023-01-01T00:00:00.123Z",
            "2023-01-01 00:00:00",
            datetime(2023, 1, 1),
            None,
            ""
        ]

        for date_str in test_dates:
            result = stage._parse_date(date_str)
            if date_str and date_str != "":
                if isinstance(date_str, datetime):
                    assert result == date_str
                else:
                    assert isinstance(result, datetime) or result is None
            else:
                assert result is None

    asyncio.run(run())


def test_enrichment_categorization():
    """Test various categorization functions in enrichment stage."""
    stage = EnrichmentStage()

    # Test age categorization
    assert stage._categorize_age(15) == "Recent"
    assert stage._categorize_age(60) == "Current"
    assert stage._categorize_age(200) == "Aging"
    assert stage._categorize_age(500) == "Old"
    assert stage._categorize_age(1000) == "Archived"

    # Test size categorization
    assert stage._categorize_size(0) == "Empty"
    assert stage._categorize_size(500 * 1024) == "Tiny"  # 500 KB
    assert stage._categorize_size(5 * 1024 * 1024) == "Small"  # 5 MB
    assert stage._categorize_size(50 * 1024 * 1024) == "Medium"  # 50 MB
    assert stage._categorize_size(500 * 1024 * 1024) == "Large"  # 500 MB
    assert stage._categorize_size(2 * 1024 * 1024 * 1024) == "Huge"  # 2 GB

    # Test external user detection
    assert stage._is_external_user("user#ext#@company.com") is True
    assert stage._is_external_user("user@gmail.com") is True
    assert stage._is_external_user("internal.user@company.com") is False


@pytest.mark.asyncio
async def test_discovery_stage_error_handling(mock_discovery_module, mock_db_repo):
    """Test discovery stage handles errors gracefully."""
    # Make discovery fail
    mock_discovery_module.run_discovery.side_effect = Exception("Discovery failed")

    context = PipelineContext(
        run_id="test_run",
        db_repository=mock_db_repo,
        metrics=PipelineMetrics()
    )

    stage = DiscoveryStage(mock_discovery_module)

    # Should raise the exception
    with pytest.raises(Exception, match="Discovery failed"):
        await stage.execute(context)

    # Should record error metric
    assert context.metrics.stage_error_counts.get("discovery", 0) == 1
