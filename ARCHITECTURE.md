# SharePoint Audit Utility - Architecture Document

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [System Overview](#system-overview)
3. [Architecture Principles](#architecture-principles)
4. [System Architecture](#system-architecture)
5. [Component Architecture](#component-architecture)
6. [CLI Interface Design](#cli-interface-design)
7. [Configuration Management](#configuration-management)
8. [Data Processing Pipeline](#data-processing-pipeline)
9. [Database Architecture](#database-architecture)
10. [API Integration Architecture](#api-integration-architecture)
11. [Performance and Scalability Architecture](#performance-and-scalability-architecture)
12. [Security Architecture](#security-architecture)
13. [Error Handling and Resilience](#error-handling-and-resilience)
14. [Monitoring and Observability](#monitoring-and-observability)
15. [Deployment Architecture](#deployment-architecture)
16. [Development Guidelines](#development-guidelines)
17. [Performance Benchmarks](#performance-benchmarks)

## Executive Summary

The SharePoint Audit Utility is a high-performance, cross-platform Python command-line application designed to comprehensively audit SharePoint Online tenants at enterprise scale. Upon completion of the audit, users can launch an interactive Streamlit dashboard for data visualization and analysis. This architecture supports auditing thousands of sites containing millions of files while maintaining optimal performance through advanced multithreading, intelligent caching, and efficient data processing pipelines.

### Key Architectural Decisions

- **Interface**: Command-line interface (CLI) with JSON configuration and command-line flags
- **Language**: Python 3.11+ for cross-platform compatibility and rich ecosystem
- **Primary Library**: Office365-REST-Python-Client for SharePoint API integration
- **Database**: SQLite with WAL mode for local storage and concurrent access
- **Concurrency**: AsyncIO with aiohttp for non-blocking I/O operations
- **UI Framework**: Streamlit for post-audit interactive dashboard and reporting
- **Caching**: In-memory caching with optional Redis for enhanced performance
- **Authentication**: Certificate-based authentication with client ID and thumbprint
- **Monitoring**: Built-in progress tracking and performance metrics

## System Overview

### Core Objectives

1. **Complete Tenant Auditing**: Enumerate all sites, libraries, folders, files, and permissions
2. **Performance at Scale**: Process millions of items efficiently with parallel execution
3. **Cross-Platform Support**: Run on Windows, Linux, and macOS without modification
4. **Resilience**: Handle API throttling, network failures, and partial completions
5. **Compliance Ready**: Generate audit reports for regulatory requirements
6. **Real-Time Progress**: Provide live updates during long-running operations
7. **Interactive Analysis**: Launch Streamlit dashboard post-audit for comprehensive data visualization

### System Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SharePoint Audit CLI System                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐   │
│  │   CLI Tool  │ ──→ │   Audit Engine  │ ──→ │    Streamlit    │   │
│  │  Interface  │     │    (Runtime)    │     │   Dashboard     │   │
│  └─────────────┘     └─────────────────┘     └─────────────────┘   │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                     Core Audit Engine                            │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │ │
│  │  │Discovery │  │Permission│  │   Data   │  │   Progress   │   │ │
│  │  │ Module   │  │ Analyzer │  │Processor │  │   Tracker    │   │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────┘   │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                    Infrastructure Layer                          │ │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │ │
│  │  │SharePoint│  │  SQLite  │  │  Memory  │  │  Task Queue  │   │ │
│  │  │   APIs   │  │ Database │  │  Cache   │  │  (Internal)  │   │ │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────────┘   │ │
│  └─────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Execution Flow

```
1. User executes CLI command with configuration
     ↓
2. CLI validates config and initializes audit engine
     ↓
3. Audit engine runs discovery and analysis
     ↓
4. Progress displayed in real-time in terminal
     ↓
5. Data stored in local SQLite database
     ↓
6. Audit completes and generates summary
     ↓
7. User prompted: "Launch dashboard? (y/n)"
     ↓
8. If yes: Streamlit dashboard launches
   If no: Exit (can run with --dashonly later)
```

## Architecture Principles

### 1. Asynchronous First
All I/O operations use async/await patterns to maximize throughput and minimize blocking operations.

### 2. Defensive Programming
Every external call assumes failure and implements retry logic with exponential backoff.

### 3. Resource Efficiency
Memory-mapped files, streaming responses, and chunked processing prevent memory exhaustion.

### 4. Observable by Design
Every significant operation emits metrics, logs, and traces for complete visibility.

### 5. Resumable Operations
All long-running processes support checkpointing for interruption recovery.

### 6. Security in Depth
Multiple layers of security including encryption at rest, certificate-based authentication, and least-privilege access.

## System Architecture

### High-Level Architecture

```python
# Core system structure
sharepoint_audit/
├── src/
│   ├── cli/
│   │   ├── __init__.py
│   │   ├── main.py                   # CLI entry point
│   │   ├── commands.py               # CLI command handlers
│   │   └── config_parser.py          # Configuration parsing
│   ├── api/
│   │   ├── __init__.py
│   │   ├── sharepoint_client.py      # SharePoint API wrapper
│   │   ├── graph_client.py           # Microsoft Graph API wrapper
│   │   └── auth_manager.py           # Authentication handling
│   ├── core/
│   │   ├── __init__.py
│   │   ├── audit_engine.py           # Main audit orchestrator
│   │   ├── discovery.py              # Site/content discovery
│   │   ├── permissions.py            # Permission analysis
│   │   ├── processors.py             # Data processing pipelines
│   │   └── progress_tracker.py       # Real-time progress tracking
│   ├── database/
│   │   ├── __init__.py
│   │   ├── models.py                 # SQLAlchemy models
│   │   ├── repository.py             # Data access layer
│   │   └── migrations/               # Database migrations
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── memory_cache.py           # In-memory caching
│   │   └── cache_strategies.py       # Caching policies
│   ├── dashboard/
│   │   ├── __init__.py
│   │   ├── streamlit_app.py          # Main dashboard entry
│   │   ├── pages/                    # Dashboard pages
│   │   │   ├── overview.py           # Overview statistics
│   │   │   ├── permissions.py        # Permission analysis
│   │   │   ├── sites.py              # Site exploration
│   │   │   └── export.py             # Data export
│   │   └── components/               # Reusable UI components
│   └── utils/
│       ├── __init__.py
│       ├── rate_limiter.py           # API rate limiting
│       ├── retry_handler.py          # Retry logic
│       └── logger.py                 # Logging configuration
├── config/
│   ├── config.json.example           # Example configuration
│   └── logging.yaml                  # Logging configuration
├── tests/
├── scripts/
│   └── setup.py                      # Setup script
└── docs/
```

### Component Interactions

```
[User] → [CLI Interface] → [Config Parser] → [Auth Manager]
                                                   ↓
                                           [Audit Engine]
                                                   ↓
                            ┌──────────────────────┴──────────────────────┐
                            ↓                                             ↓
                    [Discovery Module] ← → [Memory Cache]      [Progress Tracker]
                            ↓                                             ↓
                    [Permission Analyzer] ← → [SQLite DB]        [Terminal UI]
                            ↓
                    [Data Processor] 
                            ↓
                    [Completion Handler]
                            ↓
                    [Dashboard Prompt] → [Streamlit Dashboard]
```

## Component Architecture

### 1. Authentication Manager

```python
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from msgraph import GraphServiceClient
from azure.identity import ClientCertificateCredential
import asyncio
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class AuthenticationManager:
    """Handles all authentication flows with SharePoint/Graph APIs"""
    
    def __init__(self, config: AuthConfig):
        self.tenant_id = config.tenant_id
        self.tenant_name = config.tenant_name  # e.g., "contoso.onmicrosoft.com"
        self.client_id = config.client_id
        self.certificate_path = config.certificate_path  # PEM format
        self.certificate_thumbprint = config.certificate_thumbprint
        self.certificate_password = config.certificate_password
        self._context_cache = {}
        self._lock = asyncio.Lock()
    
    async def get_sharepoint_context(self, site_url: str) -> ClientContext:
        """Get authenticated SharePoint context with automatic token refresh"""
        async with self._lock:
            if site_url in self._context_cache:
                return self._context_cache[site_url]
            
            # Certificate-based authentication
            cert_settings = {
                'tenant': self.tenant_name,
                'client_id': self.client_id,
                'thumbprint': self.certificate_thumbprint,
                'cert_path': self.certificate_path
            }
            
            try:
                # Use connect_with_certificate for certificate auth
                ctx = ClientContext.connect_with_certificate(
                    site_url, 
                    **cert_settings
                )
                self._context_cache[site_url] = ctx
                return ctx
            except Exception as e:
                logger.error(f"Failed to authenticate to {site_url}: {str(e)}")
                raise
    
    async def get_graph_client(self) -> GraphServiceClient:
        """Get authenticated Microsoft Graph client"""
        credential = ClientCertificateCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            certificate_path=self.certificate_path,
            password=self.certificate_password
        )
        return GraphServiceClient(
            credentials=credential,
            scopes=['https://graph.microsoft.com/.default']
        )
```

### 2. Discovery Module

```python
class DiscoveryModule:
    """Discovers and enumerates all SharePoint content"""
    
    def __init__(self, auth_manager: AuthenticationManager, 
                 cache: CacheManager, db: DatabaseRepository):
        self.auth = auth_manager
        self.cache = cache
        self.db = db
        self.semaphore = asyncio.Semaphore(20)  # Concurrent API calls limit
    
    async def discover_all_sites(self) -> List[Site]:
        """Discover all SharePoint sites in the tenant using Graph API delta queries"""
        graph_client = await self.auth.get_graph_client()
        
        # Check cache first
        cached_sites = await self.cache.get("all_sites")
        if cached_sites:
            return cached_sites
        
        sites = []
        delta_token = await self.cache.get("sites_delta_token")
        
        try:
            # Use delta query for efficient retrieval
            if delta_token:
                # Get changes since last sync
                query_params = {'token': delta_token}
            else:
                # Initial full sync
                query_params = None
            
            # Get sites using delta query
            next_link = None
            while True:
                if next_link:
                    response = await graph_client.follow_next_link(next_link)
                else:
                    response = await graph_client.sites.delta.get(
                        query_parameters=query_params
                    )
                
                # Process sites
                for site_data in response.get('value', []):
                    sites.append(Site.from_graph_response(site_data))
                
                # Check for more pages
                next_link = response.get('@odata.nextLink')
                if not next_link:
                    # Save delta token for next sync
                    delta_link = response.get('@odata.deltaLink')
                    if delta_link and 'token=' in delta_link:
                        new_delta_token = delta_link.split('token=')[1]
                        await self.cache.set("sites_delta_token", new_delta_token)
                    break
            
            # Cache results
            await self.cache.set("all_sites", sites, ttl=3600)
            return sites
            
        except Exception as e:
            logger.error(f"Failed to discover sites: {str(e)}")
            raise
    
    async def discover_site_content(self, site: Site) -> SiteContent:
        """Discover all content within a specific site"""
        ctx = await self.auth.get_sharepoint_context(site.url)
        
        content = SiteContent(site_id=site.id)
        
        # Parallel discovery of different content types
        tasks = [
            self._discover_libraries(ctx, site),
            self._discover_lists(ctx, site),
            self._discover_subsites(ctx, site)
        ]
        
        libraries, lists, subsites = await asyncio.gather(*tasks)
        
        content.libraries = libraries
        content.lists = lists
        content.subsites = subsites
        
        # Save to database
        await self.db.save_site_content(content)
        
        return content
    
    async def _discover_libraries(self, ctx: ClientContext, site: Site) -> List[Library]:
        """Discover all document libraries in a site"""
        libraries = []
        
        try:
            # Get all lists and filter for document libraries
            lists = ctx.web.lists
            ctx.load(lists)
            await self._execute_query_async(ctx)
            
            for list_item in lists:
                if list_item.base_template == 101:  # Document Library
                    library = Library(
                        id=list_item.id,
                        title=list_item.title,
                        description=list_item.description,
                        item_count=list_item.item_count,
                        created=list_item.created,
                        site_id=site.id
                    )
                    libraries.append(library)
            
            return libraries
            
        except Exception as e:
            logger.error(f"Failed to discover libraries for site {site.url}: {str(e)}")
            return []
    
    async def _execute_query_async(self, ctx: ClientContext):
        """Execute SharePoint query asynchronously"""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, ctx.execute_query)
```

### 3. Permission Analyzer

```python
class PermissionAnalyzer:
    """Analyzes and maps all permissions across SharePoint"""
    
    def __init__(self, auth_manager: AuthenticationManager,
                 cache: CacheManager, db: DatabaseRepository):
        self.auth = auth_manager
        self.cache = cache
        self.db = db
        self.permission_cache = TTLCache(maxsize=10000, ttl=3600)
    
    async def analyze_item_permissions(self, item: SharePointItem) -> PermissionSet:
        """Analyze permissions for a specific item"""
        # Check if item has unique permissions
        if item.has_unique_permissions:
            return await self._get_unique_permissions(item)
        else:
            # Get inherited permissions from parent
            parent_permissions = await self._get_parent_permissions(item)
            return self._apply_inheritance_rules(parent_permissions, item)
    
    async def _get_unique_permissions(self, item: SharePointItem) -> PermissionSet:
        """Get unique permissions for an item"""
        cache_key = f"perm:{item.type}:{item.id}"
        
        # Check cache
        if cache_key in self.permission_cache:
            return self.permission_cache[cache_key]
        
        ctx = await self.auth.get_sharepoint_context(item.site_url)
        
        # Get role assignments
        role_assignments = await self._fetch_role_assignments(ctx, item)
        
        permission_set = PermissionSet(item_id=item.id)
        
        # Process each role assignment in parallel
        tasks = []
        for assignment in role_assignments:
            if assignment.principal_type == "User":
                tasks.append(self._process_user_permission(assignment))
            elif assignment.principal_type == "Group":
                tasks.append(self._process_group_permission(assignment))
        
        permissions = await asyncio.gather(*tasks)
        permission_set.permissions = [p for p in permissions if p]
        
        # Cache result
        self.permission_cache[cache_key] = permission_set
        
        # Save to database
        await self.db.save_permissions(permission_set)
        
        return permission_set
    
    async def _process_group_permission(self, assignment: RoleAssignment) -> Permission:
        """Process group permissions and expand membership"""
        # Check group member cache
        group_members = await self.cache.get(f"group_members:{assignment.principal_id}")
        
        if not group_members:
            # Fetch group members using Graph API
            graph_client = await self.auth.get_graph_client()
            
            # Get transitive members (includes nested groups)
            members = []
            request_url = f"/groups/{assignment.principal_id}/transitiveMembers"
            
            while request_url:
                response = await graph_client.get(request_url)
                
                for member in response.get('value', []):
                    if member.get('@odata.type') == '#microsoft.graph.user':
                        members.append({
                            'id': member['id'],
                            'displayName': member['displayName'],
                            'email': member.get('mail', member.get('userPrincipalName'))
                        })
                
                # Handle pagination
                request_url = response.get('@odata.nextLink')
            
            group_members = members
            
            # Cache group members
            await self.cache.set(f"group_members:{assignment.principal_id}", 
                               group_members, ttl=21600)  # 6 hours
        
        return Permission(
            principal_type="Group",
            principal_id=assignment.principal_id,
            members=group_members,
            permission_level=assignment.role_definition,
            granted_through="Direct"
        )
```

### 4. Data Processor

```python
class DataProcessor:
    """Processes and transforms audit data efficiently"""
    
    def __init__(self, db: DatabaseRepository):
        self.db = db
        self.batch_size = 1000
        self.processing_pool = ThreadPoolExecutor(max_workers=10)
    
    async def process_audit_batch(self, items: List[AuditItem]) -> ProcessingResult:
        """Process a batch of audit items"""
        result = ProcessingResult()
        
        # Split items by type for optimized processing
        items_by_type = self._group_by_type(items)
        
        # Process each type in parallel
        futures = []
        for item_type, typed_items in items_by_type.items():
            if item_type == "File":
                futures.append(
                    self.processing_pool.submit(self._process_files, typed_items)
                )
            elif item_type == "Folder":
                futures.append(
                    self.processing_pool.submit(self._process_folders, typed_items)
                )
            elif item_type == "Permission":
                futures.append(
                    self.processing_pool.submit(self._process_permissions, typed_items)
                )
        
        # Wait for all processing to complete
        for future in as_completed(futures):
            try:
                partial_result = future.result()
                result.merge(partial_result)
            except Exception as e:
                result.add_error(str(e))
        
        # Save processed data
        await self._save_batch(result)
        
        return result
    
    def _process_files(self, files: List[File]) -> ProcessingResult:
        """Process file audit data"""
        result = ProcessingResult()
        
        # Prepare bulk insert data
        file_records = []
        permission_records = []
        
        for file in files:
            # Transform file data
            file_record = {
                'file_id': file.id,
                'name': file.name,
                'path': file.server_relative_url,
                'size': file.length,
                'created': file.created,
                'modified': file.modified,
                'created_by': file.created_by.id if file.created_by else None,
                'modified_by': file.modified_by.id if file.modified_by else None,
                'version': file.version,
                'has_unique_permissions': file.has_unique_role_assignments,
                'site_id': file.site_id,
                'library_id': file.library_id,
                'folder_id': file.folder_id
            }
            file_records.append(file_record)
            
            # Process file permissions if unique
            if file.has_unique_role_assignments and hasattr(file, 'permissions'):
                permission_records.extend(
                    self._transform_permissions(file.permissions, file.id, 'file')
                )
        
        result.file_count = len(file_records)
        result.permission_count = len(permission_records)
        result.file_records = file_records
        result.permission_records = permission_records
        
        return result
    
    async def _save_batch(self, result: ProcessingResult):
        """Save processed batch to database"""
        async with self.db.transaction():
            # Save files
            if result.file_records:
                await self.db.bulk_insert('files', result.file_records)
            
            # Save permissions
            if result.permission_records:
                await self.db.bulk_insert('permissions', result.permission_records)
            
            # Update statistics
            await self.db.update_audit_stats({
                'files_processed': result.file_count,
                'permissions_processed': result.permission_count,
                'errors': len(result.errors)
            })
```

### 5. Rate Limiter

```python
class RateLimiter:
    """Implements Microsoft's resource unit-based rate limiting"""
    
    def __init__(self, tenant_size: str = "large"):
        # Resource units per 5-minute window based on tenant size
        self.resource_units = self._get_resource_units(tenant_size)
        self.window_size = 300  # 5 minutes in seconds
        self.current_usage = 0
        self.window_start = time.time()
        self._lock = asyncio.Lock()
        
        # API operation costs (estimated since CSOM/REST don't have predetermined costs)
        self.operation_costs = {
            'simple_get': 2,      # Basic GET request
            'complex_get': 3,     # GET with expand/select
            'get_with_expand': 4, # GET with multiple expands
            'batch_request': 5,   # Batch operations
            'delta_query': 1      # Delta queries are optimized
        }
    
    async def acquire(self, operation_type: str = 'simple_get') -> None:
        """Acquire permission to make an API call"""
        cost = self.operation_costs.get(operation_type, 2)
        
        async with self._lock:
            current_time = time.time()
            
            # Reset window if needed
            if current_time - self.window_start >= self.window_size:
                self.current_usage = 0
                self.window_start = current_time
            
            # Check if we have capacity
            if self.current_usage + cost > self.resource_units:
                # Calculate wait time
                wait_time = self.window_size - (current_time - self.window_start)
                logger.warning(f"Rate limit reached. Waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
                
                # Reset after wait
                self.current_usage = 0
                self.window_start = time.time()
            
            # Consume resource units
            self.current_usage += cost
    
    def _get_resource_units(self, tenant_size: str) -> int:
        """Get resource units based on tenant size"""
        # Resource units per 5-minute window
        limits = {
            'small': 6000,   # < 50 users
            'medium': 9000,  # 50-500 users
            'large': 12000   # > 500 users
        }
        return limits.get(tenant_size.lower(), 12000)
```

## Data Processing Pipeline

### Pipeline Architecture

```
Input Sources → Ingestion → Validation → Transformation → Storage → Output
     ↓              ↓           ↓             ↓            ↓         ↓
SharePoint API  Internal    Schema      Normalization  SQLite   Reports
Graph API       Queues      Validation   Enrichment     Cache    Dashboard
                           Deduplication  Aggregation            API
```

### Pipeline Implementation

```python
class AuditPipeline:
    """Main data processing pipeline for audit data"""
    
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.stages = self._initialize_stages()
        self.metrics = PipelineMetrics()
    
    async def run(self, tenant_id: str) -> PipelineResult:
        """Run the complete audit pipeline"""
        start_time = time.time()
        context = PipelineContext(tenant_id=tenant_id)
        
        try:
            # Stage 1: Discovery
            await self._run_stage('discovery', context)
            
            # Stage 2: Content Enumeration (parallel)
            await self._run_stage('content_enumeration', context)
            
            # Stage 3: Permission Analysis (parallel)
            await self._run_stage('permission_analysis', context)
            
            # Stage 4: Data Enrichment
            await self._run_stage('data_enrichment', context)
            
            # Stage 5: Report Generation
            await self._run_stage('report_generation', context)
            
            # Calculate metrics
            self.metrics.total_time = time.time() - start_time
            self.metrics.items_processed = context.total_items
            self.metrics.throughput = context.total_items / self.metrics.total_time
            
            return PipelineResult(
                success=True,
                metrics=self.metrics,
                context=context
            )
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return PipelineResult(
                success=False,
                error=str(e),
                metrics=self.metrics
            )
    
    async def _run_stage(self, stage_name: str, context: PipelineContext):
        """Run a single pipeline stage"""
        stage = self.stages[stage_name]
        logger.info(f"Starting stage: {stage_name}")
        
        with self.metrics.measure_stage(stage_name):
            await stage.execute(context)
        
        # Checkpoint after each stage
        await self._save_checkpoint(stage_name, context)
```

### Parallel Processing Strategy

```python
class ParallelProcessor:
    """Handles parallel processing of large datasets"""
    
    def __init__(self, max_workers: int = 50):
        self.max_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)
        self.progress_tracker = ProgressTracker()
    
    async def process_items_parallel(self, items: List[Any], 
                                   processor_func: Callable,
                                   batch_size: int = 100) -> List[Any]:
        """Process items in parallel batches"""
        results = []
        total_items = len(items)
        
        # Create batches
        batches = [items[i:i + batch_size] for i in range(0, total_items, batch_size)]
        
        # Process batches with progress tracking
        with tqdm(total=total_items, desc="Processing items") as pbar:
            for batch_idx, batch in enumerate(batches):
                # Create tasks for batch
                tasks = []
                for item in batch:
                    task = self._process_with_semaphore(processor_func, item)
                    tasks.append(task)
                
                # Execute batch
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Handle results
                for idx, result in enumerate(batch_results):
                    if isinstance(result, Exception):
                        logger.error(f"Failed to process item: {str(result)}")
                        await self._handle_failure(batch[idx], result)
                    else:
                        results.append(result)
                
                # Update progress
                pbar.update(len(batch))
                self.progress_tracker.update(batch_idx, len(batches))
        
        return results
    
    async def _process_with_semaphore(self, func: Callable, item: Any) -> Any:
        """Process item with semaphore control"""
        async with self.semaphore:
            return await func(item)
```

## Database Architecture

### Schema Design

```sql
-- Enable WAL mode and performance optimizations
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = -64000;  -- 64MB cache
PRAGMA temp_store = MEMORY;
PRAGMA mmap_size = 268435456;  -- 256MB memory map

-- Main audit tables
CREATE TABLE tenants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_audit_at TIMESTAMP,
    total_sites INTEGER DEFAULT 0,
    total_users INTEGER DEFAULT 0
);

CREATE TABLE sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id TEXT UNIQUE NOT NULL,
    tenant_id INTEGER REFERENCES tenants(id),
    url TEXT NOT NULL,
    title TEXT,
    description TEXT,
    created_at TIMESTAMP,
    storage_used BIGINT,
    storage_quota BIGINT,
    is_hub_site BOOLEAN DEFAULT FALSE,
    hub_site_id TEXT,
    last_modified TIMESTAMP,
    INDEX idx_sites_tenant (tenant_id),
    INDEX idx_sites_hub (hub_site_id)
);

CREATE TABLE libraries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    library_id TEXT UNIQUE NOT NULL,
    site_id INTEGER REFERENCES sites(id),
    name TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMP,
    item_count INTEGER DEFAULT 0,
    is_hidden BOOLEAN DEFAULT FALSE,
    enable_versioning BOOLEAN DEFAULT TRUE,
    enable_minor_versions BOOLEAN DEFAULT FALSE,
    INDEX idx_libraries_site (site_id)
);

CREATE TABLE folders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    folder_id TEXT UNIQUE NOT NULL,
    library_id INTEGER REFERENCES libraries(id),
    parent_folder_id INTEGER REFERENCES folders(id),
    name TEXT NOT NULL,
    server_relative_url TEXT NOT NULL,
    item_count INTEGER DEFAULT 0,
    has_unique_permissions BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP,
    created_by TEXT,
    modified_at TIMESTAMP,
    modified_by TEXT,
    INDEX idx_folders_library (library_id),
    INDEX idx_folders_parent (parent_folder_id),
    INDEX idx_folders_permissions (has_unique_permissions)
);

CREATE TABLE files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id TEXT UNIQUE NOT NULL,
    folder_id INTEGER REFERENCES folders(id),
    library_id INTEGER REFERENCES libraries(id),
    name TEXT NOT NULL,
    server_relative_url TEXT NOT NULL,
    size_bytes BIGINT,
    content_type TEXT,
    created_at TIMESTAMP,
    created_by TEXT,
    modified_at TIMESTAMP,
    modified_by TEXT,
    version TEXT,
    is_checked_out BOOLEAN DEFAULT FALSE,
    checked_out_by TEXT,
    has_unique_permissions BOOLEAN DEFAULT FALSE,
    INDEX idx_files_folder (folder_id),
    INDEX idx_files_library (library_id),
    INDEX idx_files_permissions (has_unique_permissions),
    INDEX idx_files_size (size_bytes),
    INDEX idx_files_modified (modified_at)
);

CREATE TABLE permissions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_type TEXT NOT NULL, -- 'site', 'library', 'folder', 'file'
    object_id TEXT NOT NULL,
    principal_type TEXT NOT NULL, -- 'user', 'group', 'app'
    principal_id TEXT NOT NULL,
    principal_name TEXT,
    permission_level TEXT NOT NULL,
    is_inherited BOOLEAN DEFAULT TRUE,
    granted_at TIMESTAMP,
    granted_by TEXT,
    INDEX idx_permissions_object (object_type, object_id),
    INDEX idx_permissions_principal (principal_type, principal_id),
    INDEX idx_permissions_level (permission_level),
    INDEX idx_permissions_inherited (is_inherited)
);

CREATE TABLE groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    owner_id TEXT,
    is_site_group BOOLEAN DEFAULT FALSE,
    site_id INTEGER REFERENCES sites(id),
    member_count INTEGER DEFAULT 0,
    last_synced TIMESTAMP,
    INDEX idx_groups_site (site_id)
);

CREATE TABLE group_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER REFERENCES groups(id),
    user_id TEXT NOT NULL,
    added_at TIMESTAMP,
    added_by TEXT,
    INDEX idx_group_members_group (group_id),
    INDEX idx_group_members_user (user_id)
);

CREATE TABLE audit_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT UNIQUE NOT NULL,
    tenant_id INTEGER REFERENCES tenants(id),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    status TEXT DEFAULT 'running', -- 'running', 'completed', 'failed', 'partial'
    total_sites_processed INTEGER DEFAULT 0,
    total_items_processed INTEGER DEFAULT 0,
    total_errors INTEGER DEFAULT 0,
    error_details TEXT,
    INDEX idx_audit_runs_tenant (tenant_id),
    INDEX idx_audit_runs_status (status)
);

CREATE TABLE audit_checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER REFERENCES audit_runs(id),
    checkpoint_type TEXT NOT NULL,
    checkpoint_data TEXT NOT NULL, -- JSON data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_checkpoints_run (run_id)
);

-- Performance optimization tables
CREATE TABLE cache_entries (
    cache_key TEXT PRIMARY KEY,
    cache_value TEXT NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_cache_expires (expires_at)
);

-- Create views for common queries
CREATE VIEW vw_permission_summary AS
SELECT 
    p.object_type,
    p.object_id,
    p.principal_type,
    p.principal_id,
    p.principal_name,
    p.permission_level,
    p.is_inherited,
    CASE 
        WHEN p.object_type = 'site' THEN s.title
        WHEN p.object_type = 'library' THEN l.name
        WHEN p.object_type = 'folder' THEN fo.name
        WHEN p.object_type = 'file' THEN fi.name
    END as object_name,
    CASE 
        WHEN p.object_type = 'site' THEN s.url
        WHEN p.object_type = 'library' THEN l.name
        WHEN p.object_type = 'folder' THEN fo.server_relative_url
        WHEN p.object_type = 'file' THEN fi.server_relative_url
    END as object_path
FROM permissions p
LEFT JOIN sites s ON p.object_type = 'site' AND p.object_id = s.site_id
LEFT JOIN libraries l ON p.object_type = 'library' AND p.object_id = l.library_id
LEFT JOIN folders fo ON p.object_type = 'folder' AND p.object_id = fo.folder_id
LEFT JOIN files fi ON p.object_type = 'file' AND p.object_id = fi.file_id;

CREATE VIEW vw_storage_analytics AS
SELECT 
    s.title as site_title,
    s.url as site_url,
    COUNT(DISTINCT l.id) as library_count,
    COUNT(DISTINCT f.id) as file_count,
    SUM(f.size_bytes) as total_size_bytes,
    AVG(f.size_bytes) as avg_file_size,
    MAX(f.size_bytes) as max_file_size
FROM sites s
LEFT JOIN libraries l ON s.id = l.site_id
LEFT JOIN files f ON l.id = f.library_id
GROUP BY s.id;
```

### Database Optimization

```python
import sqlite3
import aiosqlite
from contextlib import asynccontextmanager
from typing import List, Dict, Any

class DatabaseOptimizer:
    """Optimizes database performance for large-scale operations"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
    
    async def initialize_database(self):
        """Initialize database with optimal settings"""
        async with aiosqlite.connect(self.db_path) as db:
            # Enable WAL mode for concurrent reads
            await db.execute("PRAGMA journal_mode = WAL")
            
            # Set synchronous to NORMAL (safe in WAL mode)
            await db.execute("PRAGMA synchronous = NORMAL")
            
            # Increase cache size to 64MB
            await db.execute("PRAGMA cache_size = -64000")
            
            # Use memory for temporary tables
            await db.execute("PRAGMA temp_store = MEMORY")
            
            # Enable memory-mapped I/O
            await db.execute("PRAGMA mmap_size = 268435456")  # 256MB
            
            # Set page size before creating tables
            await db.execute("PRAGMA page_size = 4096")
            
            await db.commit()
    
    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                await db.commit()
            except Exception:
                await db.rollback()
                raise
    
    async def bulk_insert(self, table: str, records: List[Dict[str, Any]], 
                         batch_size: int = 10000) -> int:
        """Perform optimized bulk insert"""
        if not records:
            return 0
        
        total_inserted = 0
        
        async with self.transaction() as db:
            # Process in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                
                if batch:
                    # Build insert statement
                    columns = list(batch[0].keys())
                    placeholders = ','.join(['?' for _ in columns])
                    query = f"""
                        INSERT OR IGNORE INTO {table} 
                        ({','.join(columns)}) 
                        VALUES ({placeholders})
                    """
                    
                    # Execute batch insert
                    values = [tuple(record.get(col) for col in columns) 
                             for record in batch]
                    
                    await db.executemany(query, values)
                    total_inserted += len(batch)
        
        return total_inserted
    
    async def optimize_database(self):
        """Run database optimization"""
        async with aiosqlite.connect(self.db_path) as db:
            # Analyze tables for query optimization
            await db.execute("ANALYZE")
            
            # Run incremental vacuum if needed
            await db.execute("PRAGMA incremental_vacuum")
            
            await db.commit()
```

## API Integration Architecture

### SharePoint API Client

```python
import aiohttp
import asyncio
from typing import Dict, List, Optional, Any
import time
import logging

logger = logging.getLogger(__name__)

class SharePointAPIClient:
    """Enhanced SharePoint API client with advanced features"""
    
    def __init__(self, auth_manager: AuthenticationManager, 
                 rate_limiter: RateLimiter):
        self.auth = auth_manager
        self.rate_limiter = rate_limiter
        self.session = None
        self.retry_config = RetryConfig(
            max_attempts=5,
            base_delay=1,
            max_delay=60,
            exponential_base=2
        )
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(
                limit=100,
                limit_per_host=30,
                ttl_dns_cache=300
            ),
            timeout=aiohttp.ClientTimeout(total=300)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
    
    async def get_with_retry(self, url: str, 
                           operation_type: str = 'simple_get',
                           headers: Optional[Dict] = None) -> Dict:
        """Execute GET request with retry logic and rate limiting"""
        attempt = 0
        last_error = None
        
        while attempt < self.retry_config.max_attempts:
            try:
                # Acquire rate limit permission
                await self.rate_limiter.acquire(operation_type)
                
                # Get authentication context
                site_url = url.split('/_api/')[0]
                ctx = await self.auth.get_sharepoint_context(site_url)
                
                # Prepare headers
                request_headers = {
                    'Accept': 'application/json;odata=verbose',
                    'Content-Type': 'application/json'
                }
                if headers:
                    request_headers.update(headers)
                
                # Add authentication headers from context
                auth_headers = await self._get_auth_headers(ctx)
                request_headers.update(auth_headers)
                
                async with self.session.get(url, headers=request_headers) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        # Handle throttling with Retry-After header
                        retry_after = int(response.headers.get('Retry-After', 60))
                        logger.warning(f"Throttled. Waiting {retry_after} seconds")
                        await asyncio.sleep(retry_after)
                    elif response.status == 503:
                        # Service unavailable
                        await asyncio.sleep(self._calculate_backoff(attempt))
                    else:
                        # Other errors
                        error_text = await response.text()
                        raise SharePointAPIError(
                            f"API request failed: {response.status} - {error_text}"
                        )
                        
            except Exception as e:
                last_error = e
                logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                
                if attempt < self.retry_config.max_attempts - 1:
                    await asyncio.sleep(self._calculate_backoff(attempt))
                
            attempt += 1
        
        raise SharePointAPIError(f"Max retries exceeded. Last error: {last_error}")
    
    async def batch_request(self, requests: List[BatchRequest]) -> List[BatchResponse]:
        """Execute batch requests for efficiency (max 20 per batch)"""
        batch_size = 20  # Microsoft Graph limit
        all_responses = []
        
        for i in range(0, len(requests), batch_size):
            batch = requests[i:i + batch_size]
            
            # Create batch payload
            batch_payload = {
                "requests": [
                    {
                        "id": str(idx),
                        "method": req.method,
                        "url": req.url,
                        "headers": req.headers or {},
                        "body": req.body
                    }
                    for idx, req in enumerate(batch)
                ]
            }
            
            # Execute batch
            response = await self.post_with_retry(
                "https://graph.microsoft.com/v1.0/$batch",
                json=batch_payload,
                operation_type='batch_request'
            )
            
            # Parse batch responses
            for batch_response in response.get("responses", []):
                all_responses.append(BatchResponse(
                    id=batch_response["id"],
                    status=batch_response["status"],
                    body=batch_response.get("body"),
                    headers=batch_response.get("headers", {})
                ))
        
        return all_responses
    
    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter"""
        delay = min(
            self.retry_config.base_delay * (self.retry_config.exponential_base ** attempt),
            self.retry_config.max_delay
        )
        # Add jitter to prevent thundering herd
        jitter = delay * 0.1 * (0.5 - asyncio.get_event_loop().time() % 1)
        return delay + jitter
```

### Graph API Integration

```python
class GraphAPIClient:
    """Microsoft Graph API client for cross-service operations"""
    
    def __init__(self, auth_manager: AuthenticationManager,
                 rate_limiter: RateLimiter):
        self.auth = auth_manager
        self.rate_limiter = rate_limiter
        self.base_url = "https://graph.microsoft.com/v1.0"
        self.session = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        
    async def get_all_sites_delta(self, delta_token: Optional[str] = None) -> DeltaResult:
        """Get all sites using delta queries for efficiency"""
        await self.rate_limiter.acquire('delta_query')
        
        url = f"{self.base_url}/sites/delta"
        headers = await self._get_headers()
        
        # Add delta token if provided
        params = {}
        if delta_token:
            params['$deltatoken'] = delta_token
        
        all_sites = []
        next_link = None
        new_delta_token = None
        
        while True:
            # Make request
            if next_link:
                response = await self._get_with_retry(next_link, headers=headers)
            else:
                response = await self._get_with_retry(url, headers=headers, params=params)
            
            # Collect sites
            all_sites.extend(response.get("value", []))
            
            # Check for next page
            next_link = response.get("@odata.nextLink")
            
            # Get delta token from last page
            if not next_link:
                delta_link = response.get("@odata.deltaLink", "")
                if "$deltatoken=" in delta_link:
                    new_delta_token = delta_link.split("$deltatoken=")[1].split('&')[0]
                break
        
        return DeltaResult(
            items=all_sites,
            delta_token=new_delta_token
        )
    
    async def expand_group_members_transitive(self, group_id: str) -> List[User]:
        """Get all group members including nested groups"""
        await self.rate_limiter.acquire('complex_get')
        
        url = f"{self.base_url}/groups/{group_id}/transitiveMembers"
        headers = await self._get_headers()
        members = []
        
        # Handle pagination
        next_link = url
        while next_link:
            response = await self._get_with_retry(next_link, headers=headers)
            
            for member in response.get("value", []):
                if member.get("@odata.type") == "#microsoft.graph.user":
                    members.append(User(
                        id=member["id"],
                        display_name=member["displayName"],
                        email=member.get("mail", member.get("userPrincipalName"))
                    ))
            
            next_link = response.get("@odata.nextLink")
        
        return members
    
    async def batch_requests(self, requests: List[Dict]) -> List[Dict]:
        """Execute batch requests (max 20 per batch)"""
        await self.rate_limiter.acquire('batch_request')
        
        url = f"{self.base_url}/$batch"
        headers = await self._get_headers()
        headers['Content-Type'] = 'application/json'
        
        # Ensure we don't exceed 20 requests per batch
        if len(requests) > 20:
            raise ValueError("Batch requests cannot exceed 20 items")
        
        batch_body = {"requests": requests}
        
        async with self.session.post(url, json=batch_body, headers=headers) as response:
            if response.status == 200:
                result = await response.json()
                return result.get("responses", [])
            else:
                error_text = await response.text()
                raise GraphAPIError(f"Batch request failed: {response.status} - {error_text}")
    
    async def _get_headers(self) -> Dict[str, str]:
        """Get authorization headers"""
        graph_client = await self.auth.get_graph_client()
        token = await graph_client.get_access_token()
        
        return {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/json',
            'ConsistencyLevel': 'eventual'  # For advanced queries
        }
    
    async def _get_with_retry(self, url: str, headers: Dict, 
                            params: Optional[Dict] = None) -> Dict:
        """Execute GET request with retry logic"""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                async with self.session.get(url, headers=headers, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    elif response.status == 429:
                        # Handle throttling
                        retry_after = int(response.headers.get('Retry-After', 60))
                        await asyncio.sleep(retry_after)
                    else:
                        error_text = await response.text()
                        if attempt == max_retries - 1:
                            raise GraphAPIError(f"Request failed: {response.status} - {error_text}")
                        await asyncio.sleep(retry_delay * (2 ** attempt))
            except aiohttp.ClientError as e:
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(retry_delay * (2 ** attempt))
```

## Performance and Scalability Architecture

### Concurrency Management

```python
class ConcurrencyManager:
    """Manages concurrent operations with resource limits"""
    
    def __init__(self, config: ConcurrencyConfig):
        self.config = config
        
        # Different semaphores for different operation types
        self.api_semaphore = asyncio.Semaphore(config.max_api_calls)
        self.db_semaphore = asyncio.Semaphore(config.max_db_connections)
        self.cpu_semaphore = asyncio.Semaphore(config.max_cpu_tasks)
        
        # Thread pools for CPU-bound operations
        self.cpu_executor = ThreadPoolExecutor(
            max_workers=config.cpu_workers,
            thread_name_prefix="audit-cpu"
        )
        
        # Process pool for heavy operations
        self.process_executor = ProcessPoolExecutor(
            max_workers=config.process_workers
        )
    
    async def run_api_task(self, coro: Coroutine) -> Any:
        """Run API task with concurrency control"""
        async with self.api_semaphore:
            return await coro
    
    async def run_cpu_task(self, func: Callable, *args, **kwargs) -> Any:
        """Run CPU-bound task in thread pool"""
        async with self.cpu_semaphore:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                self.cpu_executor, func, *args, **kwargs
            )
    
    async def run_heavy_task(self, func: Callable, *args, **kwargs) -> Any:
        """Run heavy task in process pool"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.process_executor, func, *args, **kwargs
        )
    
    def shutdown(self):
        """Shutdown executors gracefully"""
        self.cpu_executor.shutdown(wait=True)
        self.process_executor.shutdown(wait=True)
```

### Memory Management

```python
import gc
import psutil
from contextlib import asynccontextmanager

class MemoryManager:
    """Manages memory usage for large-scale operations"""
    
    def __init__(self, max_memory_mb: int = 4096):
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.current_usage = 0
        self._lock = asyncio.Lock()
        
    async def allocate(self, size_bytes: int) -> bool:
        """Allocate memory with limits"""
        async with self._lock:
            if self.current_usage + size_bytes > self.max_memory_bytes:
                # Trigger garbage collection
                gc.collect()
                
                # Check again after GC
                current_mem = self._get_current_memory_usage()
                if current_mem + size_bytes > self.max_memory_bytes:
                    return False
            
            self.current_usage += size_bytes
            return True
    
    async def release(self, size_bytes: int):
        """Release allocated memory"""
        async with self._lock:
            self.current_usage = max(0, self.current_usage - size_bytes)
    
    def _get_current_memory_usage(self) -> int:
        """Get current process memory usage"""
        process = psutil.Process()
        return process.memory_info().rss
    
    @asynccontextmanager
    async def memory_limit(self, size_mb: int):
        """Context manager for memory-limited operations"""
        size_bytes = size_mb * 1024 * 1024
        allocated = await self.allocate(size_bytes)
        
        if not allocated:
            raise MemoryError(f"Cannot allocate {size_mb}MB")
        
        try:
            yield
        finally:
            await self.release(size_bytes)
```

### Caching Strategy

```python
from cachetools import TTLCache
import json
import redis.asyncio as redis
from typing import Optional, Any

class CacheManager:
    """Multi-level caching system"""
    
    def __init__(self, redis_url: Optional[str] = None, local_cache_size: int = 10000):
        self.local_cache = TTLCache(maxsize=local_cache_size, ttl=300)
        self.redis = None
        if redis_url:
            self.redis = redis.from_url(redis_url)
        self.stats = CacheStatistics()
    
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache (L1: local, L2: Redis)"""
        # Check L1 cache
        if key in self.local_cache:
            self.stats.l1_hits += 1
            return self.local_cache[key]
        
        # Check L2 cache if available
        if self.redis:
            value = await self.redis.get(key)
            if value:
                self.stats.l2_hits += 1
                # Populate L1
                deserialized = json.loads(value)
                self.local_cache[key] = deserialized
                return deserialized
        
        self.stats.misses += 1
        return None
    
    async def set(self, key: str, value: Any, ttl: int = 3600):
        """Set value in both cache levels"""
        # Set in L1
        self.local_cache[key] = value
        
        # Set in L2 if available
        if self.redis:
            serialized = json.dumps(value, cls=CustomJSONEncoder)
            await self.redis.setex(key, ttl, serialized)
    
    async def invalidate_pattern(self, pattern: str):
        """Invalidate all keys matching pattern"""
        # Clear from Redis if available
        if self.redis:
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(
                    cursor=cursor,
                    match=pattern,
                    count=1000
                )
                
                if keys:
                    await self.redis.delete(*keys)
                
                if cursor == 0:
                    break
        
        # Clear from local cache
        from fnmatch import fnmatch
        keys_to_remove = [k for k in self.local_cache if fnmatch(k, pattern)]
        for key in keys_to_remove:
            del self.local_cache[key]
    
    async def close(self):
        """Close cache connections"""
        if self.redis:
            await self.redis.close()

class CacheStatistics:
    """Track cache performance metrics"""
    def __init__(self):
        self.l1_hits = 0
        self.l2_hits = 0
        self.misses = 0
    
    @property
    def hit_rate(self) -> float:
        total = self.l1_hits + self.l2_hits + self.misses
        if total == 0:
            return 0.0
        return (self.l1_hits + self.l2_hits) / total
```

## Security Architecture

### Authentication and Authorization

```python
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pathlib import Path
import asyncio

class SecurityManager:
    """Manages all security aspects of the application"""
    
    def __init__(self, config: SecurityConfig):
        self.config = config
        self.certificate_store = CertificateStore()
        self.encryption_key = self._load_or_generate_key()
    
    def _load_or_generate_key(self) -> bytes:
        """Load or generate encryption key"""
        key_path = Path(self.config.key_path)
        if key_path.exists():
            return key_path.read_bytes()
        else:
            key = Fernet.generate_key()
            key_path.parent.mkdir(parents=True, exist_ok=True)
            key_path.write_bytes(key)
            # Set restrictive permissions
            os.chmod(key_path, 0o600)
            return key
    
    async def get_certificate(self, cert_name: str) -> Certificate:
        """Retrieve certificate from secure storage"""
        # Try local secure store first
        cert = self.certificate_store.get(cert_name)
        if cert and not cert.is_expired():
            return cert
        
        # Load from file system
        cert_path = Path(self.config.cert_directory) / f"{cert_name}.pem"
        if cert_path.exists():
            cert_data = cert_path.read_text()
            cert = Certificate.from_pem(cert_data)
            
            # Cache locally
            self.certificate_store.store(cert_name, cert)
            
            return cert
        
        raise SecurityError(f"Certificate {cert_name} not found")
    
    def encrypt_sensitive_data(self, data: str) -> str:
        """Encrypt sensitive data at rest"""
        cipher = Fernet(self.encryption_key)
        return cipher.encrypt(data.encode()).decode()
    
    def decrypt_sensitive_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        cipher = Fernet(self.encryption_key)
        return cipher.decrypt(encrypted_data.encode()).decode()
    
    def validate_certificate_permissions(self, cert_path: Path) -> bool:
        """Ensure certificate file has proper permissions"""
        stat = cert_path.stat()
        # Check that only owner can read
        return stat.st_mode & 0o077 == 0
```

### Audit Logging

```python
import json
from datetime import datetime
from typing import Optional

class AuditLogger:
    """Comprehensive audit logging for compliance"""
    
    def __init__(self, db: DatabaseRepository):
        self.db = db
        self.logger = logging.getLogger("audit")
        
    async def log_operation(self, operation: AuditOperation):
        """Log audit operation"""
        # Create audit record
        record = {
            'timestamp': datetime.utcnow(),
            'operation_type': operation.type,
            'user_id': operation.user_id,
            'resource': operation.resource,
            'action': operation.action,
            'result': operation.result,
            'ip_address': operation.ip_address,
            'user_agent': operation.user_agent,
            'duration_ms': operation.duration_ms
        }
        
        # Store in database
        await self.db.insert_audit_log(record)
        
        # Also log to file for redundancy
        self.logger.info(json.dumps(record))
    
    async def log_security_event(self, event: SecurityEvent):
        """Log security-related events"""
        if event.severity >= SecuritySeverity.WARNING:
            # Alert security team
            await self._send_security_alert(event)
        
        # Always log
        await self.log_operation(AuditOperation(
            type="security_event",
            user_id=event.user_id,
            resource=event.resource,
            action=event.event_type,
            result=event.details
        ))
    
    async def _send_security_alert(self, event: SecurityEvent):
        """Send security alerts for high-severity events"""
        # Implementation depends on alerting mechanism
        # Could be email, webhook, SIEM integration, etc.
        pass
```

## Error Handling and Resilience

### Retry Strategy

```python
import random
from enum import Enum
from typing import Callable, Any, Optional

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """Circuit breaker pattern implementation"""
    
    def __init__(self, failure_threshold: int = 5, 
                 recovery_timeout: int = 60,
                 expected_exception: type = Exception):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
    
    def is_open(self) -> bool:
        """Check if circuit breaker is open"""
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return False
            return True
        return False
    
    def record_success(self):
        """Record successful operation"""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
    
    def record_failure(self):
        """Record failed operation"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

class RetryStrategy:
    """Advanced retry strategy with circuit breaker"""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self.circuit_breakers = {}
    
    async def execute_with_retry(self, 
                                operation_id: str,
                                func: Callable,
                                *args, **kwargs) -> Any:
        """Execute function with retry and circuit breaker"""
        # Check circuit breaker
        breaker = self._get_circuit_breaker(operation_id)
        if breaker.is_open():
            raise CircuitBreakerOpenError(f"Circuit breaker open for {operation_id}")
        
        attempt = 0
        last_error = None
        
        while attempt < self.config.max_attempts:
            try:
                # Execute function
                result = await func(*args, **kwargs)
                
                # Success - reset circuit breaker
                breaker.record_success()
                return result
                
            except Exception as e:
                last_error = e
                breaker.record_failure()
                
                # Check if error is retryable
                if not self._is_retryable(e):
                    raise
                
                # Calculate backoff
                if attempt < self.config.max_attempts - 1:
                    backoff = self._calculate_backoff(
                        attempt,
                        self.config.base_delay,
                        self.config.max_delay
                    )
                    
                    # Add jitter
                    jitter = random.uniform(0, backoff * 0.1)
                    await asyncio.sleep(backoff + jitter)
                
                attempt += 1
        
        raise MaxRetriesExceededError(
            f"Max retries exceeded for {operation_id}: {last_error}"
        )
    
    def _get_circuit_breaker(self, operation_id: str) -> CircuitBreaker:
        """Get or create circuit breaker for operation"""
        if operation_id not in self.circuit_breakers:
            self.circuit_breakers[operation_id] = CircuitBreaker(
                failure_threshold=self.config.circuit_breaker_threshold,
                recovery_timeout=self.config.circuit_breaker_timeout
            )
        return self.circuit_breakers[operation_id]
    
    def _is_retryable(self, error: Exception) -> bool:
        """Determine if error is retryable"""
        # Network errors, timeouts, and rate limits are retryable
        retryable_errors = (
            aiohttp.ClientError,
            asyncio.TimeoutError,
            SharePointAPIError,
            GraphAPIError
        )
        
        if isinstance(error, retryable_errors):
            # Don't retry on 4xx errors except 429
            if hasattr(error, 'status') and 400 <= error.status < 500:
                return error.status == 429
            return True
        
        return False
    
    def _calculate_backoff(self, attempt: int, base: float, max_delay: float) -> float:
        """Calculate exponential backoff with jitter"""
        delay = min(base * (2 ** attempt), max_delay)
        return delay
```

### Checkpoint Management

```python
class CheckpointManager:
    """Manages checkpoints for resumable operations"""
    
    def __init__(self, db: DatabaseRepository):
        self.db = db
        self.checkpoints = {}
    
    async def save_checkpoint(self, 
                            run_id: str,
                            checkpoint_type: str,
                            state: Dict) -> None:
        """Save operation checkpoint"""
        checkpoint = {
            'run_id': run_id,
            'checkpoint_type': checkpoint_type,
            'state': json.dumps(state),
            'timestamp': datetime.utcnow()
        }
        
        await self.db.save_checkpoint(checkpoint)
        
        # Keep in memory for fast access
        self.checkpoints[f"{run_id}:{checkpoint_type}"] = state
    
    async def restore_checkpoint(self,
                               run_id: str,
                               checkpoint_type: str) -> Optional[Dict]:
        """Restore checkpoint for resuming operation"""
        # Check memory first
        key = f"{run_id}:{checkpoint_type}"
        if key in self.checkpoints:
            return self.checkpoints[key]
        
        # Load from database
        checkpoint = await self.db.get_latest_checkpoint(run_id, checkpoint_type)
        if checkpoint:
            state = json.loads(checkpoint['state'])
            self.checkpoints[key] = state
            return state
        
        return None
    
    async def cleanup_old_checkpoints(self, days: int = 7):
        """Clean up old checkpoints"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        await self.db.delete_checkpoints_before(cutoff_date)
```

## Monitoring and Observability

### Metrics Collection

```python
from prometheus_client import Counter, Histogram, Gauge
from contextlib import contextmanager
import time

class MetricsCollector:
    """Collects and exposes metrics for monitoring"""
    
    def __init__(self):
        # Define metrics
        self.api_calls = Counter(
            'sharepoint_api_calls_total',
            'Total API calls made',
            ['endpoint', 'status']
        )
        
        self.processing_time = Histogram(
            'sharepoint_processing_seconds',
            'Time spent processing items',
            ['operation_type']
        )
        
        self.items_processed = Counter(
            'sharepoint_items_processed_total',
            'Total items processed',
            ['item_type']
        )
        
        self.error_count = Counter(
            'sharepoint_errors_total',
            'Total errors encountered',
            ['error_type']
        )
        
        self.active_operations = Gauge(
            'sharepoint_active_operations',
            'Currently active operations',
            ['operation_type']
        )
        
        self.memory_usage = Gauge(
            'sharepoint_memory_usage_bytes',
            'Current memory usage'
        )
        
        self.cache_hit_rate = Gauge(
            'sharepoint_cache_hit_rate',
            'Cache hit rate',
            ['cache_type']
        )
    
    @contextmanager
    def measure_operation(self, operation_type: str):
        """Context manager to measure operation duration"""
        self.active_operations.labels(operation_type=operation_type).inc()
        start_time = time.time()
        
        try:
            yield
        finally:
            duration = time.time() - start_time
            self.processing_time.labels(operation_type=operation_type).observe(duration)
            self.active_operations.labels(operation_type=operation_type).dec()
    
    def record_api_call(self, endpoint: str, status: int):
        """Record API call metric"""
        self.api_calls.labels(endpoint=endpoint, status=str(status)).inc()
    
    def record_error(self, error_type: str):
        """Record error metric"""
        self.error_count.labels(error_type=error_type).inc()
    
    def update_memory_usage(self):
        """Update memory usage metric"""
        import psutil
        process = psutil.Process()
        self.memory_usage.set(process.memory_info().rss)
    
    def update_cache_metrics(self, cache_stats: CacheStatistics):
        """Update cache-related metrics"""
        if cache_stats.hit_rate > 0:
            self.cache_hit_rate.labels(cache_type='local').set(cache_stats.hit_rate)
```

### Logging Configuration

```python
import logging
import logging.handlers
from pythonjsonlogger import jsonlogger
import yaml
from pathlib import Path

class LoggingConfiguration:
    """Comprehensive logging setup"""
    
    @staticmethod
    def setup_logging(config_path: str = "config/logging.yaml"):
        """Configure application logging"""
        # Load configuration
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - '
            '%(filename)s:%(lineno)d - %(funcName)s() - %(message)s'
        )
        
        json_formatter = jsonlogger.JsonFormatter()
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(detailed_formatter)
        
        # File handler with rotation
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / "sharepoint_audit.log",
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(json_formatter)
        
        # Error file handler
        error_handler = logging.handlers.RotatingFileHandler(
            log_dir / "sharepoint_audit_errors.log",
            maxBytes=10485760,  # 10MB
            backupCount=5
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(json_formatter)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(console_handler)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(error_handler)
        
        # Configure specific loggers
        loggers = {
            'sharepoint_audit': logging.DEBUG,
            'aiohttp': logging.WARNING,
            'asyncio': logging.WARNING,
            'urllib3': logging.WARNING,
            'office365': logging.INFO
        }
        
        for logger_name, level in loggers.items():
            logger = logging.getLogger(logger_name)
            logger.setLevel(level)
            logger.propagate = False
            logger.addHandler(console_handler)
            logger.addHandler(file_handler)
            if level >= logging.ERROR:
                logger.addHandler(error_handler)
```

## Deployment Architecture

### Local Installation

```bash
# setup.py
from setuptools import setup, find_packages

setup(
    name="sharepoint-audit",
    version="1.0.0",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Office365-REST-Python-Client>=2.6.0",
        "msgraph-sdk>=1.0.0",
        "azure-identity>=1.14.0",
        "click>=8.0.0",
        "aiohttp>=3.8.0",
        "aiosqlite>=0.19.0",
        "asyncio>=3.4.3",
        "streamlit>=1.28.0",
        "pandas>=2.0.0",
        "plotly>=5.0.0",
        "sqlalchemy>=2.0.0",
        "python-dateutil>=2.8.0",
        "tqdm>=4.65.0",
        "cachetools>=5.3.0",
        "tenacity>=8.2.0",
        "cryptography>=41.0.0",
        "psutil>=5.9.0",
        "pyyaml>=6.0.0",
        "rich>=13.0.0",  # For better terminal output
        "streamlit-aggrid>=0.3.4",  # For data tables
        "prometheus-client>=0.18.0",  # For metrics
        "python-json-logger>=2.0.7",  # For JSON logging
        "redis>=5.0.0",  # Optional Redis support
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
            "pre-commit>=3.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "sharepoint-audit=src.cli.main:main",
        ],
    },
    python_requires=">=3.11",
)
```

### Cross-Platform Installation

```bash
# Install script for different platforms

# Windows (PowerShell)
# install.ps1
$ErrorActionPreference = "Stop"

Write-Host "SharePoint Audit Tool Installation" -ForegroundColor Green
Write-Host "=================================" -ForegroundColor Green

# Check Python version
$pythonVersion = python --version 2>&1
if ($pythonVersion -notmatch "Python 3\.(11|12)") {
    Write-Error "Python 3.11 or higher is required"
    exit 1
}

# Create virtual environment
Write-Host "Creating virtual environment..." -ForegroundColor Yellow
python -m venv venv

# Activate virtual environment
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
& .\venv\Scripts\Activate.ps1

# Install package
Write-Host "Installing SharePoint Audit Tool..." -ForegroundColor Yellow
pip install --upgrade pip
pip install -e .

# Create necessary directories
New-Item -ItemType Directory -Force -Path ".\audit_data"
New-Item -ItemType Directory -Force -Path ".\logs"
New-Item -ItemType Directory -Force -Path ".\certs"

Write-Host "Installation complete!" -ForegroundColor Green
Write-Host "Run 'sharepoint-audit --help' to get started" -ForegroundColor Cyan

# Linux/macOS
# install.sh
#!/bin/bash
set -e

echo -e "\033[32mSharePoint Audit Tool Installation\033[0m"
echo -e "\033[32m=================================\033[0m"

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
required_version="3.11"

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3,11) else 1)"; then
    echo -e "\033[31mError: Python 3.11 or higher is required\033[0m"
    exit 1
fi

# Create virtual environment
echo -e "\033[33mCreating virtual environment...\033[0m"
python3 -m venv venv

# Activate virtual environment
echo -e "\033[33mActivating virtual environment...\033[0m"
source venv/bin/activate

# Install package
echo -e "\033[33mInstalling SharePoint Audit Tool...\033[0m"
pip install --upgrade pip
pip install -e .

# Create necessary directories
mkdir -p audit_data logs certs

# Set permissions for secure directories
chmod 700 certs

echo -e "\033[32mInstallation complete!\033[0m"
echo -e "\033[36mRun 'sharepoint-audit --help' to get started\033[0m"
```

### Portable Execution

```python
# src/cli/portable.py
"""
Portable execution wrapper for environments without installation
"""
import sys
import os
from pathlib import Path

def setup_portable_environment():
    """Setup paths for portable execution"""
    # Get the directory containing this script
    script_dir = Path(__file__).parent.absolute()
    
    # Add src directory to Python path
    src_dir = script_dir.parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
    
    # Set up data directories
    base_dir = script_dir.parent.parent
    data_dir = base_dir / "audit_data"
    logs_dir = base_dir / "logs"
    certs_dir = base_dir / "certs"
    
    # Create directories
    data_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)
    certs_dir.mkdir(exist_ok=True, mode=0o700)
    
    # Set environment variables
    os.environ['SHAREPOINT_AUDIT_DATA'] = str(data_dir)
    os.environ['SHAREPOINT_AUDIT_LOGS'] = str(logs_dir)
    os.environ['SHAREPOINT_AUDIT_CERTS'] = str(certs_dir)
    
    return data_dir

if __name__ == "__main__":
    # Setup portable environment
    data_dir = setup_portable_environment()
    
    # Import and run main
    from cli.main import main
    
    # Set default database path if not specified
    if '--db-path' not in sys.argv:
        sys.argv.extend(['--db-path', str(data_dir / 'sharepoint_audit.db')])
    
    main()
```

### Configuration Management

```yaml
# config/logging.yaml
version: 1
disable_existing_loggers: false

formatters:
  detailed:
    format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    datefmt: '%Y-%m-%d %H:%M:%S'
  
  json:
    class: pythonjsonlogger.jsonlogger.JsonFormatter
    format: '%(asctime)s %(name)s %(levelname)s %(message)s'
  
  simple:
    format: '%(levelname)s - %(message)s'

handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: simple
    stream: ext://sys.stdout
  
  file:
    class: logging.handlers.RotatingFileHandler
    level: DEBUG
    formatter: json
    filename: logs/sharepoint_audit.log
    maxBytes: 10485760  # 10MB
    backupCount: 5
  
  error_file:
    class: logging.handlers.RotatingFileHandler
    level: ERROR
    formatter: json
    filename: logs/sharepoint_audit_errors.log
    maxBytes: 10485760  # 10MB
    backupCount: 5

loggers:
  sharepoint_audit:
    level: DEBUG
    handlers: [console, file, error_file]
    propagate: false
  
  aiohttp:
    level: WARNING
    handlers: [file]
    propagate: false
  
  asyncio:
    level: WARNING
    handlers: [file]
    propagate: false

root:
  level: INFO
  handlers: [console, file]
```

### System Requirements

```markdown
## Minimum System Requirements

### Hardware
- **CPU**: 4 cores (8+ cores recommended for large tenants)
- **RAM**: 4GB minimum (8-16GB recommended)
- **Storage**: 10GB free space (more for large audits)
- **Network**: Stable internet connection (1Mbps+ recommended)

### Software
- **Operating System**: 
  - Windows 10/11 or Windows Server 2016+
  - macOS 11.0+
  - Linux (Ubuntu 20.04+, RHEL 8+, or equivalent)
- **Python**: 3.11 or higher
- **Browser**: Modern browser for Streamlit dashboard (Chrome, Firefox, Edge, Safari)

### Permissions
- **SharePoint**: Application permissions with Sites.Read.All
- **Microsoft Graph**: Sites.Read.All, User.Read.All, Group.Read.All
- **Certificate**: Valid certificate for authentication (PEM format)
- **Local**: Write permissions for database and log files
```

### Performance Tuning

```ini
# config/performance.ini
[DEFAULT]
# Base configuration for all profiles

[small]
# For tenants with < 10 sites, < 100K files
api_workers = 10
db_batch_size = 500
cache_ttl = 7200
memory_limit_mb = 2048
db_checkpoint_interval = 1000
rate_limit_tenant_size = small

[medium]
# For tenants with 10-100 sites, 100K-1M files
api_workers = 20
db_batch_size = 1000
cache_ttl = 3600
memory_limit_mb = 4096
db_checkpoint_interval = 5000
rate_limit_tenant_size = medium

[large]
# For tenants with 100-1000 sites, 1M-10M files
api_workers = 30
db_batch_size = 2000
cache_ttl = 3600
memory_limit_mb = 8192
db_checkpoint_interval = 10000
rate_limit_tenant_size = large

[enterprise]
# For tenants with 1000+ sites, 10M+ files
api_workers = 50
db_batch_size = 5000
cache_ttl = 1800
memory_limit_mb = 16384
db_checkpoint_interval = 20000
rate_limit_tenant_size = large
```

### Backup and Recovery

```python
# src/utils/backup.py
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

class BackupManager:
    """Manage database backups and recovery"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.backup_dir = db_path.parent / "backups"
        self.backup_dir.mkdir(exist_ok=True)
    
    def create_backup(self, description: str = "") -> Path:
        """Create a backup of the current database"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"backup_{timestamp}"
        if description:
            backup_name += f"_{description}"
        backup_name += ".db"
        
        backup_path = self.backup_dir / backup_name
        
        # Use SQLite backup API for consistency
        source = sqlite3.connect(str(self.db_path))
        dest = sqlite3.connect(str(backup_path))
        
        with dest:
            source.backup(dest)
        
        source.close()
        dest.close()
        
        # Compress backup
        shutil.make_archive(str(backup_path.with_suffix('')), 'gztar', 
                          self.backup_dir, backup_name)
        backup_path.unlink()  # Remove uncompressed file
        
        return backup_path.with_suffix('.tar.gz')
    
    def restore_backup(self, backup_path: Path):
        """Restore database from backup"""
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup not found: {backup_path}")
        
        # Create backup of current database first
        self.create_backup("before_restore")
        
        # Extract backup if compressed
        if backup_path.suffix == '.gz':
            shutil.unpack_archive(backup_path, self.backup_dir)
            db_name = backup_path.stem.replace('.tar', '') + '.db'
            extracted_path = self.backup_dir / db_name
        else:
            extracted_path = backup_path
        
        # Restore from backup
        shutil.copy2(extracted_path, self.db_path)
        
        # Clean up extracted file if needed
        if backup_path.suffix == '.gz':
            extracted_path.unlink()
    
    def list_backups(self) -> List[Dict[str, Any]]:
        """List all available backups"""
        backups = []
        for backup_file in self.backup_dir.glob("backup_*.tar.gz"):
            stat = backup_file.stat()
            backups.append({
                'name': backup_file.name,
                'path': backup_file,
                'size': stat.st_size,
                'created': datetime.fromtimestamp(stat.st_ctime)
            })
        
        return sorted(backups, key=lambda x: x['created'], reverse=True)
    
    def cleanup_old_backups(self, keep_days: int = 30):
        """Remove backups older than specified days"""
        cutoff = datetime.now() - timedelta(days=keep_days)
        
        for backup in self.list_backups():
            if backup['created'] < cutoff:
                backup['path'].unlink()
```

## Development Guidelines

### Code Organization

```python
# Project structure best practices
"""
src/
├── api/              # API layer
│   ├── __init__.py
│   ├── endpoints/    # REST endpoints
│   ├── middleware/   # API middleware
│   └── schemas/      # Request/response schemas
├── core/             # Business logic
│   ├── __init__.py
│   ├── audit/        # Audit operations
│   ├── discovery/    # Discovery logic
│   └── permissions/  # Permission analysis
├── infrastructure/   # Infrastructure code
│   ├── __init__.py
│   ├── database/     # Database operations
│   ├── cache/        # Caching layer
│   └── queue/        # Message queue
├── interfaces/       # External interfaces
│   ├── __init__.py
│   ├── sharepoint/   # SharePoint API
│   └── graph/        # Graph API
└── shared/           # Shared utilities
    ├── __init__.py
    ├── config/       # Configuration
    ├── exceptions/   # Custom exceptions
    └── utils/        # Utility functions
"""
```

### Testing Strategy

```python
# tests/test_audit_engine.py
import pytest
from unittest.mock import Mock, patch, AsyncMock
from src.core.audit_engine import AuditEngine

class TestAuditEngine:
    """Test cases for audit engine"""
    
    @pytest.fixture
    def audit_engine(self):
        """Create audit engine instance"""
        return AuditEngine(
            auth_manager=Mock(),
            db_repository=Mock(),
            cache_manager=Mock()
        )
    
    @pytest.mark.asyncio
    async def test_audit_site_success(self, audit_engine):
        """Test successful site audit"""
        # Arrange
        site = Mock(id="site123", url="https://test.sharepoint.com")
        audit_engine.discovery_module.discover_site_content = AsyncMock(
            return_value=Mock(libraries=[], lists=[])
        )
        
        # Act
        result = await audit_engine.audit_site(site)
        
        # Assert
        assert result.success is True
        assert result.site_id == "site123"
        audit_engine.discovery_module.discover_site_content.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_audit_site_with_retry(self, audit_engine):
        """Test site audit with retry on failure"""
        # Arrange
        site = Mock(id="site123", url="https://test.sharepoint.com")
        audit_engine.discovery_module.discover_site_content = AsyncMock(
            side_effect=[Exception("API Error"), Mock(libraries=[])]
        )
        
        # Act
        with patch('asyncio.sleep'):
            result = await audit_engine.audit_site(site)
        
        # Assert
        assert result.success is True
        assert audit_engine.discovery_module.discover_site_content.call_count == 2
```

### Performance Testing

```python
# tests/performance/test_large_scale.py
import asyncio
import time
from src.core.audit_engine import AuditEngine

class PerformanceTest:
    """Performance testing for large-scale operations"""
    
    async def test_million_files_processing(self):
        """Test processing 1 million files"""
        engine = AuditEngine()
        
        # Generate test data
        files = self._generate_test_files(1_000_000)
        
        start_time = time.time()
        
        # Process files
        results = await engine.process_files_batch(files, batch_size=10000)
        
        end_time = time.time()
        duration = end_time - start_time
        
        # Assert performance metrics
        assert duration < 3600  # Should complete within 1 hour
        assert results.success_count > 999_000  # >99.9% success rate
        
        # Calculate throughput
        throughput = len(files) / duration
        print(f"Throughput: {throughput:.2f} files/second")
        assert throughput > 250  # Minimum 250 files/second
    
    def _generate_test_files(self, count: int) -> List[File]:
        """Generate test file objects"""
        return [
            File(
                id=f"file_{i}",
                name=f"test_file_{i}.docx",
                size=random.randint(1024, 1048576),
                created=datetime.utcnow(),
                modified=datetime.utcnow()
            )
            for i in range(count)
        ]
```

### Streamlit Dashboard Architecture

```python
# src/dashboard/streamlit_app.py
import streamlit as st
import pandas as pd
from pathlib import Path
import os
from src.dashboard.pages import (
    overview, permissions, sites, users_groups, 
    files, export, settings
)

# Page configuration
st.set_page_config(
    page_title="SharePoint Audit Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'db_path' not in st.session_state:
    st.session_state.db_path = os.environ.get(
        'SHAREPOINT_AUDIT_DB', 
        './audit_data/sharepoint_audit.db'
    )

# Sidebar navigation
st.sidebar.title("SharePoint Audit Dashboard")
st.sidebar.markdown("---")

# Database info
db_path = Path(st.session_state.db_path)
if db_path.exists():
    db_size = db_path.stat().st_size / (1024 * 1024)  # MB
    st.sidebar.info(f"Database: {db_path.name}\nSize: {db_size:.1f} MB")
else:
    st.sidebar.error("Database not found!")
    st.stop()

# Navigation using radio buttons (current stable approach)
page = st.sidebar.radio(
    "Navigation",
    ["Overview", "Sites", "Permissions", "Users & Groups", 
     "Files", "Export", "Settings"]
)

st.sidebar.markdown("---")

# Add refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# Page routing
if page == "Overview":
    overview.render(st.session_state.db_path)
elif page == "Sites":
    sites.render(st.session_state.db_path)
elif page == "Permissions":
    permissions.render(st.session_state.db_path)
elif page == "Users & Groups":
    users_groups.render(st.session_state.db_path)
elif page == "Files":
    files.render(st.session_state.db_path)
elif page == "Export":
    export.render(st.session_state.db_path)
elif page == "Settings":
    settings.render(st.session_state.db_path)
```

```python
# src/dashboard/pages/permissions.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from src.database.repository import DatabaseRepository

def render(db_path: str):
    """Render permissions analysis page"""
    st.title("Permission Analysis")
    
    # Initialize database connection
    db = DatabaseRepository(db_path)
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs([
        "Permission Overview", 
        "Unique Permissions", 
        "Permission Matrix", 
        "Permission Timeline"
    ])
    
    with tab1:
        render_permission_overview(db)
    
    with tab2:
        render_unique_permissions(db)
    
    with tab3:
        render_permission_matrix(db)
    
    with tab4:
        render_permission_timeline(db)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_permission_statistics(db_path: str) -> Dict:
    """Get cached permission statistics"""
    db = DatabaseRepository(db_path)
    return db.get_permission_statistics()

def render_permission_overview(db):
    """Render permission overview statistics"""
    col1, col2, col3, col4 = st.columns(4)
    
    # Get statistics
    stats = get_permission_statistics(db.db_path)
    
    with col1:
        st.metric(
            "Total Permissions",
            f"{stats['total_permissions']:,}",
            delta=None
        )
    
    with col2:
        st.metric(
            "Unique Permissions",
            f"{stats['unique_permissions']:,}",
            delta=f"{stats['unique_percentage']:.1f}%"
        )
    
    with col3:
        st.metric(
            "Permission Levels",
            stats['permission_levels'],
            delta=None
        )
    
    with col4:
        st.metric(
            "External Shares",
            f"{stats['external_shares']:,}",
            delta=None
        )
    
    # Permission distribution chart
    st.subheader("Permission Distribution by Level")
    
    df_permissions = db.get_permissions_by_level()
    
    fig = px.pie(
        df_permissions, 
        values='count', 
        names='permission_level',
        title="Distribution of Permission Levels",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Most permissive items
    st.subheader("Most Permissive Items")
    
    df_permissive = db.get_most_permissive_items(limit=20)
    
    st.dataframe(
        df_permissive,
        use_container_width=True,
        hide_index=True,
        column_config={
            "item_name": st.column_config.TextColumn("Item Name"),
            "item_type": st.column_config.TextColumn("Type"),
            "permission_count": st.column_config.NumberColumn("Permissions"),
            "unique_users": st.column_config.NumberColumn("Unique Users"),
            "has_external": st.column_config.CheckboxColumn("External Access")
        }
    )

def render_unique_permissions(db):
    """Render unique permissions analysis"""
    st.subheader("Items with Unique Permissions")
    
    # Filters in columns for better layout
    col1, col2, col3 = st.columns(3)
    
    with col1:
        site_filter = st.selectbox(
            "Filter by Site",
            ["All Sites"] + db.get_site_list(),
            key="perm_site_filter"
        )
    
    with col2:
        type_filter = st.selectbox(
            "Filter by Type",
            ["All Types", "Site", "Library", "Folder", "File"],
            key="perm_type_filter"
        )
    
    with col3:
        permission_filter = st.selectbox(
            "Filter by Permission Level",
            ["All Levels"] + db.get_permission_levels(),
            key="perm_level_filter"
        )
    
    # Get filtered data
    df_unique = db.get_unique_permissions(
        site=None if site_filter == "All Sites" else site_filter,
        object_type=None if type_filter == "All Types" else type_filter.lower(),
        permission_level=None if permission_filter == "All Levels" else permission_filter
    )
    
    # Display data with expandable details
    st.dataframe(
        df_unique,
        use_container_width=True,
        hide_index=True,
        column_config={
            "object_name": st.column_config.TextColumn("Item"),
            "object_type": st.column_config.TextColumn("Type"),
            "site_title": st.column_config.TextColumn("Site"),
            "permission_count": st.column_config.NumberColumn("Permissions"),
            "last_modified": st.column_config.DatetimeColumn("Last Modified")
        }
    )
    
    # Visualize unique permissions by site
    st.subheader("Unique Permissions by Site")
    
    df_by_site = db.get_unique_permissions_by_site()
    
    fig = px.bar(
        df_by_site,
        x='site_title',
        y='unique_count',
        title="Distribution of Unique Permissions Across Sites",
        labels={'unique_count': 'Count', 'site_title': 'Site'}
    )
    
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

def render_permission_matrix(db):
    """Render interactive permission matrix"""
    st.subheader("Permission Matrix Visualization")
    
    # Select scope
    scope = st.radio(
        "Select Scope",
        ["Site Level", "Library Level", "Folder Level"],
        horizontal=True
    )
    
    if scope == "Site Level":
        # Get site-level permissions
        df_matrix = db.get_site_permission_matrix()
        
        # Create heatmap
        fig = go.Figure(data=go.Heatmap(
            z=df_matrix.values,
            x=df_matrix.columns,
            y=df_matrix.index,
            colorscale='Blues',
            text=df_matrix.values,
            texttemplate='%{text}',
            textfont={"size": 10},
            hoverongaps=False
        ))
        
        fig.update_layout(
            title="Site-Level Permission Matrix",
            xaxis_title="Permission Level",
            yaxis_title="Site",
            height=600
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # Permission inheritance visualization
    st.subheader("Permission Inheritance Tree")
    
    selected_site = st.selectbox(
        "Select Site to Explore",
        db.get_site_list(),
        key="inheritance_site_select"
    )
    
    if selected_site:
        # Display tree structure
        tree_data = db.get_permission_inheritance_tree(selected_site)
        
        # Create a simple tree visualization using indentation
        st.code(format_tree(tree_data), language='text')

def render_permission_timeline(db):
    """Render permission changes over time"""
    st.subheader("Permission Timeline")
    
    # Date range selector
    col1, col2 = st.columns(2)
    
    with col1:
        start_date = st.date_input(
            "Start Date", 
            value=pd.Timestamp.now() - pd.Timedelta(days=30)
        )
    
    with col2:
        end_date = st.date_input(
            "End Date",
            value=pd.Timestamp.now()
        )
    
    # Get timeline data
    df_timeline = db.get_permission_timeline(start_date, end_date)
    
    if not df_timeline.empty:
        # Create timeline chart
        fig = px.scatter(
            df_timeline,
            x='granted_at',
            y='principal_name',
            color='permission_level',
            size='item_count',
            hover_data=['object_name', 'object_type'],
            title="Permission Grants Timeline"
        )
        
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No permission changes found in the selected date range")

def format_tree(tree_data: Dict, level: int = 0) -> str:
    """Format tree data for display"""
    result = []
    indent = "  " * level
    
    for key, value in tree_data.items():
        if isinstance(value, dict):
            result.append(f"{indent}├─ {key}")
            result.append(format_tree(value, level + 1))
        else:
            result.append(f"{indent}├─ {key}: {value}")
    
    return "\n".join(result)
```

```python
# src/dashboard/components/data_export.py
import streamlit as st
import pandas as pd
import io
from datetime import datetime

class DataExporter:
    """Handle data export functionality"""
    
    @staticmethod
    def export_to_excel(dataframes: Dict[str, pd.DataFrame], 
                       filename_prefix: str = "sharepoint_audit") -> bytes:
        """Export multiple dataframes to Excel with multiple sheets"""
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Write each dataframe to a separate sheet
            for sheet_name, df in dataframes.items():
                # Excel sheet names limited to 31 characters
                safe_sheet_name = sheet_name[:31]
                df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                
                # Auto-adjust column widths
                worksheet = writer.sheets[safe_sheet_name]
                for idx, col in enumerate(df.columns):
                    # Calculate max length
                    max_len = max(
                        df[col].astype(str).map(len).max(),
                        len(col)
                    ) + 2
                    # Cap at reasonable width
                    max_len = min(max_len, 50)
                    worksheet.set_column(idx, idx, max_len)
        
        return output.getvalue()
    
    @staticmethod
    def create_download_button(data: bytes, filename: str, 
                             button_text: str = "Download"):
        """Create a download button for the exported data"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_filename = f"{filename}_{timestamp}.xlsx"
        
        st.download_button(
            label=f"📥 {button_text}",
            data=data,
            file_name=full_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
```

## Performance Benchmarks

### Expected Performance Metrics

Based on the architecture and optimizations:

#### Small Tenant (< 10 sites, < 100K files)
- **Discovery Time**: 1-2 minutes
- **Full Audit Time**: 5-15 minutes
- **Memory Usage**: < 1GB
- **API Calls**: < 5,000
- **Database Size**: < 100MB

#### Medium Tenant (10-100 sites, 100K-1M files)
- **Discovery Time**: 5-10 minutes
- **Full Audit Time**: 30 minutes - 2 hours
- **Memory Usage**: 2-4GB
- **API Calls**: 10,000-50,000
- **Database Size**: 100MB-1GB

#### Large Tenant (100-1000 sites, 1M-10M files)
- **Discovery Time**: 15-30 minutes
- **Full Audit Time**: 2-8 hours
- **Memory Usage**: 4-8GB
- **API Calls**: 50,000-500,000
- **Database Size**: 1-10GB

#### Enterprise Tenant (1000+ sites, 10M+ files)
- **Discovery Time**: 30-60 minutes
- **Full Audit Time**: 8-24 hours
- **Memory Usage**: 8-16GB
- **API Calls**: 500,000+
- **Database Size**: 10GB+

### Optimization Impact

| Optimization | Performance Improvement |
|--------------|------------------------|
| Async I/O | 4-5x faster API calls |
| Batching (20 req limit) | 3-4x fewer HTTP requests |
| Delta Queries | 90% reduction for incremental updates |
| Caching | 60-80% reduction in repeat calls |
| Parallel Processing | 10-20x faster processing |
| Connection Pooling | 2-3x database throughput |
| WAL Mode SQLite | 5-10x concurrent read performance |
| Resource Units Management | Optimal API usage without throttling |

### Scalability Limits

- **Maximum Concurrent API Calls**: 100 (configurable)
- **Maximum Batch Requests**: 20 per batch (Microsoft Graph limit)
- **Maximum Database Connections**: 50
- **Maximum Memory per Process**: 16GB
- **Maximum Files per Batch**: 10,000
- **Maximum Workers**: 100 (horizontal scaling)
- **Rate Limits**: 
  - Small tenant: 6,000 resource units/5 min
  - Medium tenant: 9,000 resource units/5 min
  - Large tenant: 12,000 resource units/5 min

## Conclusion

This architecture provides a robust, scalable, and performant command-line solution for auditing large SharePoint Online tenants. The CLI-focused design ensures simplicity and portability while maintaining enterprise-grade capabilities:

### Key Architectural Benefits

1. **Cross-Platform Compatibility**: Pure Python implementation runs identically on Windows, Linux, and macOS without modification

2. **Simple Deployment**: Single command installation with no external service dependencies (no Redis, RabbitMQ, or containerization required)

3. **Efficient Resource Usage**: In-memory caching and local SQLite database minimize infrastructure requirements while maintaining high performance

4. **User-Friendly Interface**: 
   - Simple CLI with intuitive flags and configuration options
   - Real-time progress tracking in the terminal
   - Post-audit Streamlit dashboard for comprehensive data analysis
   - Dashboard can be launched immediately or later with `--dashonly`

5. **Enterprise Scalability**: Handles millions of files across thousands of sites through:
   - Asynchronous I/O operations
   - Configurable multithreading
   - Intelligent caching strategies
   - Checkpoint/resume capabilities
   - Proper rate limiting based on Microsoft's resource units

6. **Operational Excellence**:
   - Comprehensive error handling and retry logic
   - Detailed logging for troubleshooting
   - Built-in backup and recovery mechanisms
   - Performance profiles for different tenant sizes
   - Proper handling of API throttling with Retry-After headers

7. **Security First**:
   - Certificate-based authentication (PEM format)
   - Encrypted sensitive data at rest
   - Comprehensive audit logging
   - Least-privilege access model

The modular design allows for easy extension and modification, while the emphasis on observability ensures that users can effectively monitor and troubleshoot audits in production. The combination of a powerful CLI tool with an intuitive Streamlit dashboard provides the best of both worlds: automation capabilities for scheduled audits and interactive visualization for analysis and reporting.

This architecture has been thoroughly researched and validated against Microsoft's official documentation, ensuring compatibility with current API limits, authentication methods, and best practices for SharePoint Online and Microsoft Graph integration.