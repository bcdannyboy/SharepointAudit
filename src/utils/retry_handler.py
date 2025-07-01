import asyncio
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict

from .exceptions import (
    SharePointAPIError,
    GraphAPIError,
    CircuitBreakerOpenError,
    MaxRetriesExceededError,
)

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker implementation."""

    def __init__(self, failure_threshold: int = 5, recovery_timeout: int = 60) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time: float | None = None
        self.state = CircuitState.CLOSED

    def is_open(self) -> bool:
        if self.state == CircuitState.OPEN:
            if self.last_failure_time and time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return False
            return True
        return False

    def record_success(self) -> None:
        self.failure_count = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 0.5
    max_delay: float = 30.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: int = 60
    request_timeout: float = 120.0  # Timeout for individual requests


class RetryStrategy:
    """Advanced retry strategy with circuit breakers."""

    def __init__(self, config: RetryConfig) -> None:
        self.config = config
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}

    async def execute_with_retry(self, operation_id: str, func: Callable, *args, **kwargs) -> Any:
        breaker = self._get_circuit_breaker(operation_id)
        if breaker.is_open():
            raise CircuitBreakerOpenError(f"Circuit breaker open for {operation_id}")

        attempt = 0
        last_error: Exception | None = None

        while attempt < self.config.max_attempts:
            try:
                logger.debug(f"[RETRY] Attempt {attempt + 1}/{self.config.max_attempts} for {operation_id}")

                # Execute with timeout
                try:
                    result = await asyncio.wait_for(
                        func(*args, **kwargs),
                        timeout=self.config.request_timeout
                    )
                except asyncio.TimeoutError:
                    logger.error(f"[RETRY] Request timeout after {self.config.request_timeout}s for {operation_id}")
                    raise TimeoutError(f"Request timed out after {self.config.request_timeout}s")

                breaker.record_success()
                return result
            except Exception as exc:  # pragma: no cover - simple fallback
                last_error = exc
                breaker.record_failure()

                # Log the error with context
                logger.warning(
                    f"[RETRY] Attempt {attempt + 1} failed for {operation_id}: {type(exc).__name__}: {str(exc)}"
                )

                if not self._is_retryable(exc) or attempt >= self.config.max_attempts - 1:
                    logger.error(f"[RETRY] Giving up on {operation_id} after {attempt + 1} attempts")
                    raise

                backoff = self._calculate_backoff(attempt)
                jitter = random.uniform(0, backoff * 0.1)
                delay = backoff + jitter
                logger.debug(f"[RETRY] Waiting {delay:.2f}s before retry")
                await asyncio.sleep(delay)
                attempt += 1

        raise MaxRetriesExceededError(f"Max retries exceeded for {operation_id}: {last_error}")

    def _get_circuit_breaker(self, operation_id: str) -> CircuitBreaker:
        if operation_id not in self.circuit_breakers:
            self.circuit_breakers[operation_id] = CircuitBreaker(
                failure_threshold=self.config.circuit_breaker_threshold,
                recovery_timeout=self.config.circuit_breaker_timeout,
            )
        return self.circuit_breakers[operation_id]

    def _is_retryable(self, error: Exception) -> bool:
        retryable_errors = (
            asyncio.TimeoutError,
            SharePointAPIError,
            GraphAPIError,
        )
        if isinstance(error, retryable_errors):
            status = getattr(error, "status_code", None)
            if status is not None and 400 <= status < 500 and status != 429:
                return False
            return True
        return False

    def _calculate_backoff(self, attempt: int) -> float:
        delay = min(self.config.base_delay * (2**attempt), self.config.max_delay)
        return delay
