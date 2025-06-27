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


# ---------------------------------------------------------------------------
# Minimal asyncio support for environments without pytest-asyncio
# ---------------------------------------------------------------------------
import asyncio
import inspect


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "asyncio: mark test to run with asyncio event loop"
    )


@pytest.fixture
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


def pytest_pyfunc_call(pyfuncitem):
    marker = pyfuncitem.get_closest_marker("asyncio")
    if marker is not None and inspect.iscoroutinefunction(pyfuncitem.obj):
        loop = pyfuncitem.funcargs.get("event_loop")
        if loop is None:
            loop = asyncio.new_event_loop()
            pyfuncitem.funcargs["event_loop"] = loop
        # Only pass arguments expected by the test function
        funcargs = {
            name: pyfuncitem.funcargs[name]
            for name in pyfuncitem._fixtureinfo.argnames
        }
        loop.run_until_complete(pyfuncitem.obj(**funcargs))
        return True


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
