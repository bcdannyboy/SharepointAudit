import json
from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class AuthConfig:
    tenant_id: str
    client_id: str
    certificate_path: str
    certificate_thumbprint: Optional[str] = None
    certificate_password: Optional[str] = None


@dataclass
class DbConfig:
    path: str = "audit.db"


@dataclass
class AppConfig:
    auth: AuthConfig
    db: DbConfig = field(default_factory=DbConfig)
    target_sites: Optional[List[str]] = None


def load_config(config_path: str = "config/config.json") -> AppConfig:
    """Load application configuration from a JSON file."""
    try:
        with open(config_path, "r") as f:
            data = json.load(f)
        auth = AuthConfig(**data["auth"])
        db = DbConfig(**data.get("db", {}))
        target_sites = data.get("target_sites")
        return AppConfig(auth=auth, db=db, target_sites=target_sites)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in configuration file: {config_path}")
    except Exception as e:
        raise ValueError(f"Configuration validation error: {e}")
