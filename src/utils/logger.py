import logging
import logging.handlers
import logging.config
from pathlib import Path

try:
    from pythonjsonlogger import jsonlogger
except ImportError:
    # Fallback if pythonjsonlogger is not available
    jsonlogger = None

try:
    import yaml
except ImportError:
    # Fallback if yaml is not available
    yaml = None


class LoggingConfiguration:
    """Comprehensive logging setup from a YAML configuration file."""

    @staticmethod
    def setup_logging(
        config_path: str = "config/logging.yaml", default_level=logging.INFO
    ):
        """Configure application logging from a YAML file."""
        if yaml is None:
            logging.basicConfig(level=default_level)
            logging.warning("YAML module not available. Using basic configuration.")
            return

        path = Path(config_path)
        if path.exists():
            with open(path, "rt") as f:
                try:
                    config = yaml.safe_load(f.read())
                    logging.config.dictConfig(config)
                except Exception as e:
                    logging.basicConfig(level=default_level)
                    logging.warning(
                        f"Failed to load logging config from {config_path}. Error: {e}"
                    )
                    logging.warning("Falling back to basic configuration.")
        else:
            logging.basicConfig(level=default_level)
            logging.warning(
                f"Logging config file not found at {config_path}. Falling back to basic configuration."
            )
