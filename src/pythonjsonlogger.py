# Mock pythonjsonlogger module
import logging

class JsonFormatter(logging.Formatter):
    """Mock JSON formatter that just uses regular formatting."""
    pass

jsonlogger = type('Module', (), {'JsonFormatter': JsonFormatter})()
