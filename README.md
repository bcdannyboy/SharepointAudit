# SharePoint Audit Tool

## 🚀 Overview

The SharePoint Audit Tool is a comprehensive, enterprise-grade Python application designed to audit SharePoint Online tenants at scale. It discovers and analyzes all SharePoint sites, libraries, folders, files, and permissions, storing the data in a SQLite database and providing an interactive Streamlit dashboard for analysis and reporting.

## 🌟 Key Features

### Core Functionality
- **Comprehensive Discovery**: Automatically discovers all SharePoint sites, document libraries, folders, and files in your tenant
- **Permission Analysis**: Deep analysis of permissions including unique permissions, external sharing, and permission inheritance
- **High Performance**: Asynchronous operations with concurrent request handling, rate limiting, and intelligent caching
- **Resilient Architecture**: Built-in retry logic, circuit breaker pattern, and checkpoint/resume capability
- **Interactive Dashboard**: Beautiful Streamlit-based web interface for data exploration and analysis
- **Export Capabilities**: Export audit results to Excel (multi-sheet) or CSV formats

### Technical Features
- **Async-First Design**: Built on asyncio for maximum performance
- **Pipeline Architecture**: Multi-stage processing with checkpoint support
- **Repository Pattern**: Clean database abstraction layer
- **Comprehensive Logging**: Structured logging with multiple output formats
- **Health Monitoring**: Built-in health checks and system diagnostics
- **Security-First**: Certificate-based authentication, no passwords stored

## 📋 Prerequisites

- Python 3.11 or higher
- Azure AD App Registration with SharePoint API permissions
- Certificate for authentication (.pfx or .pem format)
- SharePoint Online tenant

### Required Azure AD Permissions
Your Azure AD app registration needs the following API permissions:
- `Sites.Read.All` - Read all site collections
- `Files.Read.All` - Read files in all site collections
- `User.Read.All` - Read user profiles (for permission analysis)
- `Group.Read.All` - Read group information (for permission analysis)

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone https://github.com/yourusername/SharepointAudit.git
cd SharepointAudit
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install the Package
```bash
pip install -e .
```

This installs the tool in development mode with all dependencies.

## ⚙️ Configuration

### 1. Certificate Setup
Place your authentication certificate in a secure location (e.g., `.secrets/` directory):
```
.secrets/
├── SharePoint Audit Tool.pfx
├── SharePoint Audit Tool.cer
└── details.txt
```

### 2. Create Configuration File
Create `config/config.json` based on the example:

```json
{
  "auth": {
    "tenant_id": "your-tenant-id",
    "client_id": "your-client-id",
    "certificate_path": ".secrets/your-certificate.pfx",
    "certificate_thumbprint": "your-thumbprint",
    "certificate_password": null
  },
  "tenant_url": "https://yourtenant.sharepoint.com",
  "db": {
    "path": "audit.db"
  },
  "pipeline": {
    "concurrent_requests": 10,
    "batch_size": 100,
    "checkpoint_interval": 300,
    "rate_limit": {
      "requests_per_minute": 600,
      "burst_size": 20
    }
  },
  "target_sites": [],
  "exclude_patterns": [],
  "cache": {
    "enabled": true,
    "ttl": 3600,
    "backend": "memory"
  },
  "logging": {
    "level": "INFO",
    "file": "sharepoint_audit.log"
  }
}
```

### Configuration Options

#### Authentication (`auth`)
- `tenant_id`: Azure AD tenant ID
- `client_id`: Azure AD application (client) ID
- `certificate_path`: Path to your certificate file
- `certificate_thumbprint`: Certificate thumbprint (optional, for validation)
- `certificate_password`: Password for .pfx files (null if none)

#### Pipeline (`pipeline`)
- `concurrent_requests`: Number of concurrent API requests (default: 10)
- `batch_size`: Number of items to process in each batch (default: 100)
- `checkpoint_interval`: Seconds between checkpoint saves (default: 300)
- `rate_limit`: API rate limiting configuration
  - `requests_per_minute`: Maximum requests per minute (default: 600)
  - `burst_size`: Burst capacity for rate limiter (default: 20)

#### Targeting
- `target_sites`: Array of specific site URLs to audit (empty = all sites)
- `exclude_patterns`: Array of regex patterns to exclude sites/files

#### Cache
- `enabled`: Enable/disable caching (default: true)
- `ttl`: Cache time-to-live in seconds (default: 3600)
- `backend`: Cache backend - "memory" or "redis" (default: "memory")

## 🚀 Usage

### Command Line Interface

The tool provides a comprehensive CLI with multiple commands:

#### 1. Run a Full Audit
```bash
# Basic audit (discovers all sites, includes permissions)
sharepoint-audit audit --config config/config.json

# Verbose output with progress
sharepoint-audit audit --config config/config.json --verbose

# Dry run (test without saving)
sharepoint-audit audit --config config/config.json --dry-run
```

#### 2. Launch the Dashboard
```bash
# Start the Streamlit dashboard
sharepoint-audit dashboard --db-path audit.db

# Custom port
sharepoint-audit dashboard --db-path audit.db --port 8502
```

#### 3. Health Check
```bash
# Test authentication and connectivity
sharepoint-audit health --config config/config.json
```

#### 4. Database Management
```bash
# Backup database
sharepoint-audit backup --db-path audit.db --output backup.db

# Restore database
sharepoint-audit restore --backup-path backup.db --output restored.db
```

### Python Module Usage

You can also use the tool programmatically:

```python
import asyncio
from src.api.auth import CertificateAuthProvider
from src.api.sharepoint_client import SharePointClient
from src.core.pipeline import AuditPipeline

async def run_audit():
    # Initialize authentication
    auth = CertificateAuthProvider(
        tenant_id="your-tenant-id",
        client_id="your-client-id",
        certificate_path="path/to/cert.pfx"
    )

    # Create client
    client = SharePointClient(auth)

    # Run pipeline
    pipeline = AuditPipeline(client, "audit.db")
    await pipeline.run()

# Run the audit
asyncio.run(run_audit())
```

## 📊 Dashboard Features

The Streamlit dashboard provides comprehensive analysis capabilities:

### Overview Page
- **Key Metrics**: Total sites, libraries, files, storage usage, external users
- **Permission Insights**: Unique vs inherited permissions, permission distribution
- **File Statistics**: Average file size, largest files, file types
- **Recent Activity**: Latest file modifications
- **Quick Insights**: Actionable recommendations

### Sites Page
- **Site Metrics**: Storage usage, file counts, library counts
- **Visualizations**: Storage distribution charts, top sites by size
- **Search & Filter**: Find sites by name, URL, storage, or file count
- **Hub Site Analysis**: Identify and analyze hub sites
- **Storage Analytics**: Detailed storage usage patterns

### Permissions Page
- **Permission Analysis**: Distribution by level and type
- **External Sharing**: Identify external users and their access
- **Permission Matrix**: Visual heatmap of user/group permissions
- **Advanced Filtering**: Filter by site, permission level, principal type
- **Unique Permissions**: Identify objects with broken inheritance

### Files Page
- **File Analytics**: Distribution by type, size, and age
- **Large File Detection**: Identify storage-consuming files
- **Search Capabilities**: Find files by name, type, size, age
- **Special Filters**: Checked-out files, files with unique permissions
- **Storage Optimization**: Identify optimization opportunities

### Export Page
- **Multiple Formats**: Excel (multi-sheet) and CSV exports
- **Comprehensive Reports**: Full audit data with metadata
- **Filtered Exports**: Export only what you need
- **Batch Operations**: Export multiple data types at once

## 🏗️ Architecture

### System Architecture
```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Azure AD      │────▶│  SharePoint API  │────▶│  Audit Tool     │
│   App Reg       │     │    (Graph/REST)  │     │   (Python)      │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                           │
                                                           ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│   Streamlit     │◀────│     SQLite       │◀────│    Pipeline     │
│   Dashboard     │     │    Database      │     │   Processing    │
└─────────────────┘     └──────────────────┘     └─────────────────┘
```

### Component Structure
```
src/
├── api/                 # SharePoint API clients
│   ├── auth.py         # Authentication providers
│   ├── sharepoint_client.py  # Main API client
│   └── graph_client.py # Microsoft Graph client
├── core/               # Business logic
│   ├── pipeline.py     # Multi-stage pipeline
│   ├── discovery.py    # Site/file discovery
│   ├── processor.py    # Data processing
│   └── analyzer.py     # Permission analysis
├── database/           # Data layer
│   ├── models.py       # SQLAlchemy models
│   ├── repository.py   # Repository pattern
│   └── optimizer.py    # DB optimization
├── cache/              # Caching layer
│   ├── base.py        # Cache interface
│   ├── memory.py      # In-memory cache
│   └── redis.py       # Redis cache
├── dashboard/          # Streamlit UI
│   ├── streamlit_app.py  # Main app
│   └── pages/         # Dashboard pages
└── cli/               # Command line interface
    ├── main.py        # CLI entry point
    └── commands.py    # CLI commands
```

### Database Schema

The tool uses SQLite with the following main tables:

#### Core Tables
- `tenants`: SharePoint tenant information
- `sites`: Site collections with metadata
- `libraries`: Document libraries
- `folders`: Folder hierarchy
- `files`: File metadata and properties
- `permissions`: Permission assignments
- `groups`: SharePoint groups
- `group_members`: Group membership

#### Support Tables
- `audit_runs`: Audit execution history
- `audit_checkpoints`: Resume capability
- `cache_entries`: Cached API responses

#### Views
- `vw_permission_summary`: Aggregated permission data
- `vw_storage_analytics`: Storage usage analysis

## 🔧 Development

### Development Setup
```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run specific test suites
pytest -m asyncio      # Async tests
pytest -m integration  # Integration tests
pytest -m e2e         # End-to-end tests

# Code quality
black .               # Format code
flake8               # Lint code
mypy src/            # Type checking

# Pre-commit hooks
pre-commit install
pre-commit run --all-files
```

### Project Structure
```
SharepointAudit/
├── src/                 # Source code
├── tests/              # Test suites
├── config/             # Configuration files
├── scripts/            # Utility scripts
├── docs/               # Documentation
├── dev_docs/           # Development documentation
├── .secrets/           # Certificates (git-ignored)
├── requirements.txt    # Python dependencies
├── setup.py           # Package setup
├── pytest.ini         # Test configuration
├── .pre-commit-config.yaml  # Code quality
└── README.md          # This file
```

## 📈 Performance Considerations

### Optimization Features
- **Concurrent Processing**: Configurable concurrent API requests
- **Batch Operations**: Process items in configurable batches
- **Rate Limiting**: Respect API limits with intelligent throttling
- **Caching**: In-memory or Redis caching for repeated operations
- **Streaming**: Memory-efficient processing of large datasets
- **Database Optimization**: WAL mode, indexes, and connection pooling

### Performance Tuning
- Adjust `concurrent_requests` based on your network capacity
- Increase `batch_size` for faster processing (watch memory usage)
- Use Redis cache backend for multi-instance deployments
- Enable `--analyze-permissions` only when needed (slower)

## 🔒 Security

### Security Features
- **Certificate Authentication**: No passwords in configuration
- **Secure Storage**: Credentials never logged or stored
- **Least Privilege**: Request only required permissions
- **Audit Trail**: Complete logging of all operations
- **Data Privacy**: All data stored locally

### Best Practices
1. Store certificates in a secure location with restricted access
2. Use certificate passwords for .pfx files
3. Regularly rotate certificates
4. Review audit logs for suspicious activity
5. Limit Azure AD app permissions to minimum required

## 🐛 Troubleshooting

### Common Issues

#### Authentication Errors
```
Error: Authentication failed: Invalid client certificate
```
**Solution**: Verify certificate path and thumbprint in config.json

#### Rate Limiting
```
Error: 429 Too Many Requests
```
**Solution**: Reduce `concurrent_requests` and `requests_per_minute`

#### Memory Issues
```
Error: Out of memory
```
**Solution**: Reduce `batch_size` or process specific sites

#### Permission Errors
```
Error: Access denied to site
```
**Solution**: Verify Azure AD app has required permissions

### Debug Mode
```bash
# Enable debug logging
sharepoint-audit audit --config config/config.json --log-level DEBUG
```

## 📝 Current Development Status

### Completed Phases (✅)
- **Phase 0**: Project setup and structure
- **Phase 1**: Authentication and API clients
- **Phase 2**: Database layer and models
- **Phase 3**: Site discovery engine
- **Phase 4**: Pipeline implementation
- **Phase 5**: Permission analysis
- **Phase 6**: CLI interface
- **Phase 7**: Performance optimizations
- **Phase 8**: Streamlit dashboard

### In Progress (🚧)
- **Phase 9**: Notification system
- **Phase 10**: Multi-tenant support

## 🤝 Contributing

Contributions are welcome! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Code Style
- Follow PEP 8
- Use type hints
- Add docstrings to all functions
- Write tests for new features
- Run pre-commit hooks

## 📞 Support

For issues, questions, or contributions:
- Open an issue on GitHub
- Check existing documentation in `/docs` and `/dev_docs`
- Review test cases for usage examples
