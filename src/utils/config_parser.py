import json
from pydantic import BaseModel, Field, SecretStr
from typing import Optional, List


class AuthConfig(BaseModel):
    """Authentication configuration model."""

    tenant_id: str
    client_id: str
    certificate_path: str
    certificate_password: Optional[SecretStr] = None


class DbConfig(BaseModel):
    """Database configuration model."""

    path: str = "audit.db"


class AppConfig(BaseModel):
    """Top-level application configuration model."""

    auth: AuthConfig
    db: DbConfig = Field(default_factory=DbConfig)
    target_sites: Optional[List[str]] = None


def load_config(config_path: str = "config/config.json") -> AppConfig:
    """
    Loads, validates, and returns the application configuration.

    Args:
        config_path: The path to the JSON configuration file.

    Returns:
        An AppConfig instance with the validated configuration.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If the config file is not valid JSON or fails validation.
    """
    try:
        with open(config_path, "r") as f:
            config_data = json.load(f)
        return AppConfig(**config_data)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in configuration file: {config_path}")
    except Exception as e:
        # Pydantic's ValidationError will be caught here
        raise ValueError(f"Configuration validation error: {e}")
