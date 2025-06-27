# Phase 4: Data Processing Pipeline

## Overview

Implement a formal, multi-stage data processing pipeline to orchestrate the audit. This pipeline will manage the flow of data from discovery through validation, transformation, and storage, ensuring a clear, maintainable, and extensible process. This moves beyond simple scripting to a more robust, software-engineered approach.

## Architectural Alignment

This phase introduces a core architectural pattern for the application, abstracting the audit process into a series of composable stages. It is primarily guided by:

- **[Data Processing Pipeline](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#data-processing-pipeline)**: This section is the blueprint, defining the `AuditPipeline`, `PipelineStage`, and `PipelineContext` classes that form the framework.
- **[Component Architecture: Data Processor](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#4-data-processor)**: The logic for transforming raw API data into structured database records will be encapsulated within a `DataProcessor` class, which will be executed as part of a pipeline stage.
- **[Monitoring and Observability: Metrics Collection](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#metrics-collection)**: The pipeline architecture provides the perfect hooks for collecting metrics on the performance of each stage (e.g., duration, items processed).

## Prerequisites

- [Phase 3: Basic Discovery Module](./phase_3_discovery.md)

## Deliverables

1.  **Pipeline Framework**: Core classes `AuditPipeline`, `PipelineStage`, and `PipelineContext` in `src/core/pipeline.py`.
2.  **Processing Stages**: Concrete stage implementations in `src/core/processors.py`, such as `DiscoveryStage`, `ValidationStage`, `TransformationStage`, and `StorageStage`.
3.  **Pipeline Monitoring**: A `PipelineMetrics` class in `src/core/pipeline_metrics.py` to collect performance data for each stage.

## Detailed Implementation Guide

### 1. Implement the Pipeline Framework (`src/core/pipeline.py`)

Define the abstract base classes and context that will govern the pipeline's execution.

```python
# src/core/pipeline.py
import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any

@dataclass
class PipelineContext:
    """Holds state that is passed between pipeline stages."""
    run_id: str
    raw_data: List[Dict[str, Any]] = field(default_factory=list)
    processed_data: List[Dict[str, Any]] = field(default_factory=list)
    metrics: 'PipelineMetrics' = None # Forward reference
    # Add other shared state as needed

class PipelineStage(ABC):
    """Abstract base class for a single stage in the pipeline."""
    @abstractmethod
    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Executes the logic for this stage."""
        pass

class AuditPipeline:
    """Manages and executes a sequence of pipeline stages."""
    def __init__(self, context: PipelineContext):
        self._stages: List[PipelineStage] = []
        self.context = context

    def add_stage(self, stage: PipelineStage):
        self._stages.append(stage)

    async def run(self):
        """Executes all stages in the pipeline sequentially."""
        for stage in self._stages:
            self.context = await stage.execute(self.context)
        return self.context
```

### 2. Implement Concrete Pipeline Stages (`src/core/processors.py`)

Create concrete implementations of `PipelineStage`. Each stage should have a single, well-defined responsibility.

```python
# src/core/processors.py
# from src.core.pipeline import PipelineStage, PipelineContext
# from src.core.discovery import DiscoveryModule
# from src.database.repository import DatabaseRepository

class DiscoveryStage(PipelineStage):
    """Pipeline stage for discovering raw data from APIs."""
    def __init__(self, discovery_module: 'DiscoveryModule'):
        self.discovery_module = discovery_module

    async def execute(self, context: PipelineContext) -> PipelineContext:
        # This is a simplified example. In reality, you'd stream data.
        raw_sites = await self.discovery_module.discover_all_sites()
        context.raw_data.extend([site.to_dict() for site in raw_sites])
        return context

class TransformationStage(PipelineStage):
    """Transforms raw API data into structured database records."""
    async def execute(self, context: PipelineContext) -> PipelineContext:
        for raw_item in context.raw_data:
            processed_item = self._transform(raw_item)
            context.processed_data.append(processed_item)
        return context

    def _transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Normalize date formats, resolve user IDs, enrich data, etc.
        # Example:
        item['name_upper'] = item.get('name', '').upper()
        return item

class StorageStage(PipelineStage):
    """Saves processed data to the database."""
    def __init__(self, db_repo: 'DatabaseRepository'):
        self.db_repo = db_repo

    async def execute(self, context: PipelineContext) -> PipelineContext:
        await self.db_repo.bulk_insert('sites', context.processed_data) # Assuming 'sites' table
        return context
```

### 3. Implement Pipeline Metrics (`src/core/pipeline_metrics.py`)

Create a class to hold metrics about the pipeline's execution, such as the time taken for each stage.

```python
# src/core/pipeline_metrics.py
import time
from dataclasses import dataclass, field
from typing import Dict

@dataclass
class PipelineMetrics:
    """Collects metrics for each pipeline stage."""
    stage_durations: Dict[str, float] = field(default_factory=dict)
    total_duration: float = 0.0
    items_processed: int = 0

    def start_timer(self):
        self.total_start_time = time.time()

    def stop_timer(self):
        self.total_duration = time.time() - self.total_start_time

    def measure_stage(self, stage_name: str):
        """A context manager to measure the duration of a stage."""
        start_time = time.time()
        yield
        duration = time.time() - start_time
        self.stage_durations[stage_name] = duration
```

## Implementation Task Checklist

- [ ] Design and implement the `AuditPipeline`, `PipelineStage`, and `PipelineContext` classes.
- [ ] Implement the `DiscoveryStage` which runs the `DiscoveryModule`.
- [ ] Implement a `ValidationStage` to perform basic schema validation on the data received from APIs.
- [ ] Implement a `TransformationStage` where a `DataProcessor` normalizes data (e.g., standardizing date formats, resolving user IDs).
- [ ] Implement an `EnrichmentStage` to add calculated fields (e.g., file extension, path depth).
- [ ] Implement the `StorageStage` which uses the `DatabaseRepository` to save the processed data.
- [ ] Integrate `PipelineMetrics` to track the performance of each stage.
- [ ] Ensure the pipeline supports checkpointing between stages.

## Test Plan & Cases

Testing the pipeline involves creating mock stages and a mock context to ensure they are executed in the correct order and that the context is passed correctly.

```python
# tests/test_pipeline.py
import pytest
from unittest.mock import AsyncMock
# from src.core.pipeline import AuditPipeline, PipelineContext

@pytest.mark.asyncio
async def test_pipeline_executes_stages_in_order():
    """Verify the pipeline executes its stages sequentially."""
    # Create mock stages
    stage1 = AsyncMock()
    stage2 = AsyncMock()

    # Configure mocks to return the context they receive
    stage1.execute.side_effect = lambda ctx: ctx
    stage2.execute.side_effect = lambda ctx: ctx

    context = PipelineContext(run_id='test_run')
    pipeline = AuditPipeline(context)
    pipeline.add_stage(stage1)
    pipeline.add_stage(stage2)

    await pipeline.run()

    # Check that execute was called on each stage in order
    stage1.execute.assert_called_once()
    stage2.execute.assert_called_once()
```

## Verification & Validation

Create a script to run the full pipeline and check the logs for output from each stage.

```bash
# 1. Run the full pipeline via a script
python scripts/run_pipeline.py --config config/config.json

# 2. Check the logs for output from each pipeline stage
#    Look for "Starting stage: Discovery", "Starting stage: Transformation", etc.

# 3. Check the final database to ensure data is fully processed and enriched.
sqlite3 audit.db "SELECT name, name_upper FROM sites LIMIT 10;"
```

## Done Criteria

- [ ] The pipeline can execute a full audit end-to-end, from discovery to storage.
- [ ] Data is correctly transformed and enriched as it passes through the pipeline.
- [ ] Metrics for each stage are collected and can be reported.
- [ ] The pipeline can recover from a failure in one stage and resume from the last successful checkpoint.
- [ ] The code is cleanly separated into distinct, testable pipeline stages.
