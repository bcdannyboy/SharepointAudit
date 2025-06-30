"""Configuration parser for CLI that merges file and command-line options."""

import json
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

from ..utils.config_parser import load_config as load_base_config, AppConfig

logger = logging.getLogger(__name__)


def load_and_merge_config(config_path: str = "config/config.json",
                         cli_args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load configuration from file and merge with CLI arguments.

    CLI arguments take precedence over configuration file values.

    Args:
        config_path: Path to the configuration JSON file
        cli_args: Dictionary of CLI arguments to override config values

    Returns:
        Merged configuration dictionary

    Raises:
        FileNotFoundError: If configuration file doesn't exist
        ValueError: If configuration is invalid
    """
    # Load base configuration
    try:
        app_config = load_base_config(config_path)
        logger.debug(f"Loaded configuration from {config_path}")
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise ValueError(f"Invalid configuration: {e}")

    # Convert to dictionary for easier manipulation
    config_dict = {
        "auth": {
            "tenant_id": app_config.auth.tenant_id,
            "client_id": app_config.auth.client_id,
            "certificate_path": app_config.auth.certificate_path,
        },
        "db": {
            "path": app_config.db.path
        }
    }

    # Add certificate thumbprint if present
    if app_config.auth.certificate_thumbprint:
        config_dict["auth"]["certificate_thumbprint"] = app_config.auth.certificate_thumbprint

    # Add certificate password if present
    if app_config.auth.certificate_password:
        config_dict["auth"]["certificate_password"] = app_config.auth.certificate_password

    # Add target sites if present
    if app_config.target_sites:
        config_dict["target_sites"] = app_config.target_sites

    # Merge with CLI arguments
    if cli_args:
        config_dict = merge_cli_args(config_dict, cli_args)

    # Validate final configuration
    validate_config(config_dict)

    return config_dict


def merge_cli_args(config: Dict[str, Any], cli_args: Dict[str, Any]) -> Dict[str, Any]:
    """Merge CLI arguments into configuration dictionary.

    CLI arguments take precedence over configuration file values.

    Args:
        config: Base configuration dictionary
        cli_args: CLI arguments to merge

    Returns:
        Merged configuration
    """
    # Make a copy to avoid modifying the original
    merged = config.copy()

    # Override target sites if provided
    if cli_args.get('target_sites') is not None:
        merged['target_sites'] = cli_args['target_sites']
        logger.debug(f"Overriding target_sites with CLI value: {cli_args['target_sites']}")

    # Override batch size if provided
    if cli_args.get('batch_size') is not None:
        merged['batch_size'] = cli_args['batch_size']
        logger.debug(f"Setting batch_size from CLI: {cli_args['batch_size']}")

    # Override max concurrent operations if provided
    if cli_args.get('max_concurrent') is not None:
        merged['max_concurrent'] = cli_args['max_concurrent']
        logger.debug(f"Setting max_concurrent from CLI: {cli_args['max_concurrent']}")

    # Add analyze permissions flag
    if cli_args.get('analyze_permissions') is not None:
        merged['analyze_permissions'] = cli_args['analyze_permissions']
        logger.debug(f"Setting analyze_permissions from CLI: {cli_args['analyze_permissions']}")

    # Override database path if provided
    if cli_args.get('db_path') is not None:
        merged['db']['path'] = cli_args['db_path']
        logger.debug(f"Overriding database path with CLI value: {cli_args['db_path']}")

    return merged


def validate_config(config: Dict[str, Any]) -> None:
    """Validate the configuration dictionary.

    Args:
        config: Configuration dictionary to validate

    Raises:
        ValueError: If configuration is invalid
    """
    # Check required auth fields
    if 'auth' not in config:
        raise ValueError("Missing 'auth' section in configuration")

    auth = config['auth']
    required_auth_fields = ['tenant_id', 'client_id', 'certificate_path']

    for field in required_auth_fields:
        if field not in auth or not auth[field]:
            raise ValueError(f"Missing required auth field: {field}")

    # Validate certificate path exists
    cert_path = Path(auth['certificate_path'])
    if not cert_path.exists():
        raise ValueError(f"Certificate file not found: {cert_path}")

    # Check certificate file extension
    valid_extensions = ['.pem', '.pfx', '.p12']
    if cert_path.suffix.lower() not in valid_extensions:
        raise ValueError(f"Invalid certificate file type: {cert_path.suffix}. "
                        f"Supported types: {', '.join(valid_extensions)}")

    # Validate database configuration
    if 'db' not in config:
        config['db'] = {'path': 'audit.db'}

    # Validate target sites if provided
    if 'target_sites' in config and config['target_sites']:
        if not isinstance(config['target_sites'], list):
            raise ValueError("target_sites must be a list")

        # Validate each site URL
        for site in config['target_sites']:
            if not site.startswith('https://'):
                raise ValueError(f"Invalid site URL (must start with https://): {site}")

    # Validate numeric parameters
    if 'batch_size' in config:
        if not isinstance(config['batch_size'], int) or config['batch_size'] < 1:
            raise ValueError("batch_size must be a positive integer")

    if 'max_concurrent' in config:
        if not isinstance(config['max_concurrent'], int) or config['max_concurrent'] < 1:
            raise ValueError("max_concurrent must be a positive integer")

    logger.debug("Configuration validation passed")


def get_config_template() -> Dict[str, Any]:
    """Get a configuration template with all available options.

    Returns:
        Configuration template dictionary
    """
    return {
        "auth": {
            "tenant_id": "your-tenant-id",
            "client_id": "your-client-id",
            "certificate_path": "/path/to/certificate.pem",
            "certificate_password": None  # Optional, for .pfx files
        },
        "db": {
            "path": "audit.db"
        },
        "target_sites": None,  # Optional: ["https://tenant.sharepoint.com/sites/site1"]
        "batch_size": 100,
        "max_concurrent": 50,
        "analyze_permissions": False
    }


def create_config_file(path: str = "config/config.json") -> None:
    """Create a template configuration file.

    Args:
        path: Path where to create the configuration file
    """
    config_path = Path(path)
    config_path.parent.mkdir(parents=True, exist_ok=True)

    template = get_config_template()

    with open(config_path, 'w') as f:
        json.dump(template, f, indent=2)

    logger.info(f"Created configuration template at: {config_path}")
