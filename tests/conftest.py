import pytest
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

from src.api.auth_manager import AuthenticationManager
from src.api.sharepoint_client import SharePointAPIClient
from src.api.graph_client import GraphAPIClient
from src.utils.retry_handler import RetryStrategy, RetryConfig
from src.utils.rate_limiter import RateLimiter
from src.utils.config_parser import AuthConfig


@pytest.fixture
def auth_manager():
    config = AuthConfig(
        tenant_id="tid",
        client_id="cid",
        certificate_path="path/to/cert.pem",
    )
    return AuthenticationManager(config)


@pytest.fixture
def retry_strategy():
    cfg = RetryConfig(
        max_attempts=3,
        base_delay=0.01,
        max_delay=0.02,
        circuit_breaker_threshold=3,
        circuit_breaker_timeout=1,
    )
    strategy = RetryStrategy(cfg)
    # Pre-create circuit breaker for tests
    strategy.circuit_breakers["test"] = strategy._get_circuit_breaker("test")
    return strategy


@pytest.fixture
def api_client(auth_manager, retry_strategy):
    return SharePointAPIClient(auth_manager, retry_strategy, RateLimiter())


@pytest.fixture
def graph_client(auth_manager, retry_strategy):
    return GraphAPIClient(auth_manager, retry_strategy, RateLimiter())
