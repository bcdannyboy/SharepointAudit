# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SharePoint Audit Utility is a high-performance Python CLI application for comprehensively auditing SharePoint Online tenants at enterprise scale. It discovers and audits all SharePoint sites, libraries, folders, files, and permissions, storing data in SQLite and providing a Streamlit dashboard for analysis.

## Key Development Commands

### Setup & Installation
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in editable mode with dev dependencies
pip install -e .[dev]
```

### Running the Application
```bash
# Main CLI entry point (not yet fully implemented)
sharepoint-audit audit --config config/config.json
sharepoint-audit dashboard --db-path audit.db

# Current way to run the pipeline
python scripts/run_pipeline.py --config config/config.json
```

### Testing
```bash
# Run all tests
pytest

# Run tests excluding slow ones
pytest -m "not slow"

# Run specific test categories
pytest -m asyncio      # Async tests only
pytest -m integration  # Integration tests
pytest -m e2e          # End-to-end tests

# Run with coverage
pytest --cov
```

### Code Quality
```bash
# Format code
black .

# Lint code
flake8

# Type checking
mypy src/

# Run pre-commit hooks
pre-commit run --all-files
```

## Architecture Overview

### Core Design Principles
- **Async-First**: Heavy use of asyncio for non-blocking I/O operations
- **Pipeline Architecture**: Multi-stage processing with checkpointing support
- **Repository Pattern**: Database access through abstracted repository classes
- **Resilience**: Circuit breaker pattern, retry strategies, and rate limiting
- **Caching**: In-memory caching layer with optional Redis support

### Key Components
- `src/api/` - SharePoint API client with authentication handling
- `src/core/` - Business logic (pipeline, discovery, processing)
- `src/database/` - SQLAlchemy models and repository layer
- `src/cache/` - Caching implementation
- `src/dashboard/` - Streamlit dashboard components
- `src/cli/` - Click-based CLI interface (in development)

### Authentication
Uses certificate-based authentication with Azure AD app registration:
- Requires tenant_id, client_id, and certificate_path
- Supports both .pem and .pfx certificate formats

### Database
SQLite with WAL mode for concurrent reads:
- Models defined in `src/database/models.py`
- Repository pattern in `src/database/repository.py`
- Automatic schema creation on first run

### Performance Considerations
- Concurrent API requests with configurable rate limiting
- Batch processing with checkpointing for resumability
- Progress tracking with tqdm for long-running operations
- Memory-efficient streaming for large datasets

## Development Workflow

### Current Development Phase
The project follows a 10-phase development plan (see `DEVELOPMENT_PHASES.md`):
- Phases 0-3: ✅ Complete (Setup, Auth, Database, Discovery)
- Phase 4: 🚧 In Progress (Pipeline Implementation)
- Phases 5-10: ⬜ Planned (Permissions, CLI, Performance, Dashboard, etc.)

### Adding New Features
1. Follow existing architectural patterns (async, repository, etc.)
2. Add comprehensive type hints for all new code
3. Write tests for new functionality (unit and integration)
4. Update relevant documentation in `docs/` or `dev_docs/`
5. Ensure code passes black, flake8, and mypy checks

### Testing Guidelines
- Use pytest fixtures for common test data
- Mock external API calls to ensure fast, reliable tests
- Use `pytest.mark.asyncio` for async test functions
- Mark slow tests with `@pytest.mark.slow`
- Aim for high test coverage but prioritize meaningful tests

## Common Development Tasks

### Working with the Pipeline
The pipeline is the core processing engine:
- Stages: Discovery → Item Processing → Permission Analysis → Database Storage
- Checkpointing allows resuming from failures
- Configuration in `config/config.json`
- Pipeline implementation in `src/core/pipeline.py`

### Adding API Endpoints
1. Extend `src/api/sharepoint_client.py` for new SharePoint operations
2. Follow async patterns and include retry logic
3. Add corresponding methods to discovery or processing modules
4. Update tests in `tests/test_api/`

### Database Schema Changes
1. Modify models in `src/database/models.py`
2. Consider migration strategy (project uses SQLAlchemy)
3. Update repository methods if needed
4. Test with existing data to ensure compatibility

## Important Notes

- Always use async/await patterns for I/O operations
- Follow the established error handling patterns with proper logging
- Respect rate limits and implement backoff strategies
- Keep security in mind - never commit credentials or sensitive data
- The project targets Python 3.11+ for optimal async performance
- When working on the CLI, follow Click conventions and patterns
- Dashboard development should follow Streamlit best practices
