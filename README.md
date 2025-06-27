# SharePoint Audit Utility

A high-performance, cross-platform Python command-line application designed to comprehensively audit SharePoint Online tenants at enterprise scale.

## Overview

This utility provides a suite of tools to discover, analyze, and report on SharePoint Online environments. It is designed for performance, scalability, and security.

## Features

- **Comprehensive Auditing**: Enumerate sites, libraries, files, and permissions.
- **Performance**: Asynchronous processing and intelligent caching for speed.
- **Reporting**: Interactive dashboard for data visualization and analysis.
- **Security**: Certificate-based authentication and secure handling of credentials.

## Getting Started

### Prerequisites

- Python 3.11+
- Access to a SharePoint Online tenant
- An Azure App Registration with appropriate permissions

### Installation

1.  Clone the repository:
    ```bash
    git clone https://github.com/danielbloom/SharepointAudit.git
    cd SharepointAudit
    ```

2.  Create and activate a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  Install the package in editable mode with development dependencies:
    ```bash
    pip install -e .[dev]
    ```

### Configuration

1.  Copy `config/config.json.example` to `config/config.json`.
2.  Update `config/config.json` with your tenant details and certificate path.

### Usage

```bash
# Run an audit
sharepoint-audit audit --config config/config.json

# Launch the dashboard
sharepoint-audit dashboard --db-path audit.db
```
