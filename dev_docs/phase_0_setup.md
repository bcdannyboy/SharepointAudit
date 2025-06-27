# Phase 0: Project Setup & Infrastructure

## Overview

Establish the project foundation including directory structure, development environment, configuration management, and basic utilities. This phase ensures a solid and consistent starting point for all subsequent development.

## Architectural Alignment

This phase directly implements the foundational structure outlined in the [System Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#system-architecture) section of the `ARCHITECTURE.md` document. The core utilities created here are designed according to the principles in the [Component Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#component-architecture) and [Development Guidelines](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#development-guidelines) sections.

- **[System Architecture (`ARCHITECTURE.md`)]**
- **[Configuration Management (`ARCHITECTURE.md`)]**
- **[Development Guidelines (`ARCHITECTURE.md`)]**

## Prerequisites

- Python 3.11+ installed
- Git repository initialized

## Deliverables

1.  **Project Structure**: A complete directory structure as defined in `ARCHITECTURE.md`.
2.  **Core Utilities**: Foundational utilities for logging, configuration, and custom exceptions.
3.  **Development Environment**: Scripts and configurations for a consistent and automated development workflow.

## Detailed Implementation Guide

### 1. Create Project Directory Structure

Create the following directory and file structure. This structure separates concerns and provides a logical layout for the application components.

```
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
│   │   └── components/               # Reusable UI components
│   └── utils/
│       ├── __init__.py
│       ├── rate_limiter.py           # API rate limiting
│       ├── retry_handler.py          # Retry logic
│       ├── logger.py                 # Logging configuration
│       └── exceptions.py             # Custom exceptions
├── config/
│   ├── config.json.example           # Example configuration
│   └── logging.yaml                  # Logging configuration
├── tests/
├── scripts/
├── docs/
├── setup.py
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── .gitignore
└── README.md
```
*Touch `__init__.py` files in each directory to mark them as Python packages.*

### 2. Implement Core Utilities

#### `src/utils/logger.py`
Create a centralized logging configuration using the `logging` and `PyYAML` libraries. This ensures consistent, structured logging across the application. Refer to the `LoggingConfiguration` class design in `ARCHITECTURE.md`.

#### `src/utils/config_parser.py`
Implement typed configuration models using Pydantic or dataclasses. This will parse and validate the main `config.json` file, providing type hints and auto-completion for configuration settings like `AppConfig`, `AuthConfig`, etc.

#### `src/utils/exceptions.py`
Define a set of base custom exception classes that other modules can inherit from. This standardizes error handling. At a minimum, create `SharePointAuditError` as a base class, and more specific errors like `APIError`.

### 3. Configure the Development Environment

#### `setup.py`
Create the `setup.py` file to define project metadata, dependencies, and the CLI entry point. This makes the package installable via `pip`.

```python
from setuptools import setup, find_packages

setup(
    name="sharepoint-audit",
    version="1.0.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    install_requires=[
        "Office365-REST-Python-Client>=2.6.0",
        "msgraph-sdk>=1.0.0",
        "azure-identity>=1.14.0",
        "click>=8.0.0",
        "aiohttp>=3.8.0",
        "aiosqlite>=0.19.0",
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
        "rich>=13.0.0",
        "streamlit-aggrid>=0.3.4",
        "prometheus-client>=0.18.0",
        "python-json-logger>=2.0.7",
        "redis>=5.0.0",
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
            "sharepoint-audit=cli.main:main",
        ],
    },
    python_requires=">=3.11",
)
```

#### `requirements.txt` & `requirements-dev.txt`
Generate these files from `setup.py` to support environments that don't use `pip install -e .`.
- `pip-compile setup.py -o requirements.txt`
- `pip-compile setup.py --extra dev -o requirements-dev.txt`

#### Pre-commit Hooks
Configure `pre-commit` to automate code formatting and linting. Create a `.pre-commit-config.yaml` file.

```yaml
repos:
-   repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
    -   id: trailing-whitespace
    -   id: end-of-file-fixer
    -   id: check-yaml
    -   id: check-added-large-files
-   repo: https://github.com/psf/black
    rev: 23.3.0
    hooks:
    -   id: black
-   repo: https://github.com/PyCQA/flake8
    rev: 6.0.0
    hooks:
    -   id: flake8
```

#### Test Configuration (`pytest.ini`)
Configure `pytest` for test discovery, markers, and coverage.

```ini
[pytest]
testpaths = tests
addopts = -p no:warnings --strict-markers
markers =
    slow: marks tests as slow (deselect with '-m "not slow"')
    integration: marks integration tests
    e2e: marks end-to-end tests
```

## Implementation Task Checklist

- [ ] Create the full directory structure.
- [ ] Initialize Python package structure with `__init__.py` files.
- [ ] Create `setup.py` with all dependencies from `ARCHITECTURE.md`.
- [ ] Create `requirements.txt` and `requirements-dev.txt`.
- [ ] Implement `LoggingConfiguration` in `src/utils/logger.py` based on the architecture.
- [ ] Define data classes for configuration in `src/utils/config_parser.py`.
- [ ] Define base exception classes in `src/utils/exceptions.py`.
- [ ] Configure `pytest.ini` for test discovery and markers.
- [ ] Configure and install pre-commit hooks (`.pre-commit-config.yaml`).
- [ ] Create an initial `README.md` and `.gitignore`.

## Test Plan & Cases

The primary goal of testing in this phase is to ensure the project's structure and configuration are sound.

```python
# tests/test_setup.py
import logging
# from src.utils.logger import LoggingConfiguration
# from src.utils.config_parser import AppConfig, AuthConfig, DbConfig # Assuming these are defined

def test_logging_setup():
    """Verify that the logging configuration can be initialized."""
    # This test will need a mock logging.yaml or be adapted once implemented
    # LoggingConfiguration.setup_logging()
    # logger = logging.getLogger("sharepoint_audit")
    # assert logger.level == logging.DEBUG
    pass # Placeholder

def test_config_model():
    """Verify that the configuration model can be instantiated."""
    # This is a basic test; file loading will be tested later.
    # config = AppConfig(auth=AuthConfig(...), db=DbConfig(...))
    # assert config is not None
    pass # Placeholder

def test_installation():
    """Verify that the package can be installed in editable mode."""
    import subprocess
    result = subprocess.run(["pip", "install", "-e", "."], capture_output=True, text=True)
    assert result.returncode == 0, f"pip install -e . failed: {result.stderr}"
```

## Verification & Validation

Execute the following commands in your terminal to verify the setup.

```bash
# 1. Set up the environment
python3 -m venv venv
source venv/bin/activate
pip install -e .[dev]
pre-commit install

# 2. Run initial tests
pytest

# 3. Manually trigger a pre-commit run
git add .
# Make a small change to a file and commit to test hooks
# git commit -m "test: Initial project setup"
```

## Done Criteria

- [ ] `pip install -e .[dev]` completes successfully.
- [ ] `pre-commit install` completes successfully and hooks run on commit.
- [ ] `pytest tests/` runs and discovers initial tests (they can be placeholders).
- [ ] The directory structure matches the architecture document.
- [ ] Basic logging and configuration models are implemented and pass tests.
