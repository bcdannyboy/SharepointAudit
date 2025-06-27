# Phase 1: Core Authentication & API Client

## Overview

Implement a robust and resilient client for interacting with SharePoint and Microsoft Graph APIs. This includes certificate-based authentication, automatic token management, rate limiting, and a retry strategy with a circuit breaker.

## Architectural Alignment

This phase is critical and implements several key components from the `ARCHITECTURE.md`. The work directly corresponds to the following architectural sections:

- **[Component Architecture: Authentication Manager](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#1-authentication-manager)**: Defines the `AuthenticationManager` class responsible for handling secure, certificate-based authentication.
- **[API Integration Architecture](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#api-integration-architecture)**: Details the design of the `SharePointAPIClient` and `GraphAPIClient`, including retry logic and batching.
- **[Error Handling and Resilience](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#error-handling-and-resilience)**: Specifies the implementation of the `RetryStrategy` and `CircuitBreaker` patterns.
- **[Performance and Scalability: Rate Limiter](https://github.com/danielbloom/SharepointAudit/blob/main/ARCHITECTURE.md#5-rate-limiter)**: Outlines the strategy for managing API consumption to avoid throttling.

## Prerequisites

- [Phase 0: Project Setup & Infrastructure](./phase_0_setup.md) must be complete.
- Access to a SharePoint test tenant with an App Registration (providing a Client ID and Tenant ID).
- A valid certificate in PEM format, along with its private key and thumbprint, for authentication.

## Deliverables

1.  **Authentication Manager**: A centralized `AuthenticationManager` class in `src/api/auth_manager.py` that handles all authentication flows.
2.  **Resilient API Clients**: `SharePointAPIClient` and `GraphAPIClient` classes in `src/api/sharepoint_client.py` and `src/api/graph_client.py` that encapsulate all API interactions with built-in resilience.
3.  **Resilience Handlers**: A suite of utilities in `src/utils/` for rate limiting, retries (`retry_handler.py`), and circuit breaking to ensure application stability.

## Detailed Implementation Guide

### 1. Implement the Authentication Manager

In `src/api/auth_manager.py`, create the `AuthenticationManager` class. This class will be responsible for obtaining authenticated contexts for both SharePoint and Microsoft Graph using certificate credentials.

```python
# src/api/auth_manager.py
from office365.sharepoint.client_context import ClientContext
from msgraph import GraphServiceClient
from azure.identity.aio import ClientCertificateCredential
import asyncio
from typing import Dict
import logging

logger = logging.getLogger(__name__)

class AuthenticationManager:
    """Handles all authentication flows with SharePoint/Graph APIs."""

    def __init__(self, config: 'AuthConfig'): # Use forward reference for AuthConfig
        self.tenant_id = config.tenant_id
        self.client_id = config.client_id
        self.certificate_path = config.certificate_path
        self.certificate_password = config.certificate_password # If the cert is password-protected
        self._context_cache: Dict[str, ClientContext] = {}
        self._graph_client_cache: GraphServiceClient | None = None
        self._lock = asyncio.Lock()

    async def get_sharepoint_context(self, site_url: str) -> ClientContext:
        """Get authenticated SharePoint context with certificate-based auth."""
        async with self._lock:
            if site_url in self._context_cache:
                return self._context_cache[site_url]

            try:
                # The Office365-REST-Python-Client library handles token acquisition
                # and refresh automatically when using certificate credentials.
                ctx = ClientContext(site_url).with_client_certificate(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    cert_path=self.certificate_path,
                    cert_password=self.certificate_password
                )
                self._context_cache[site_url] = ctx
                return ctx
            except Exception as e:
                logger.error(f"Failed to authenticate to SharePoint site {site_url}: {e}")
                raise

    async def get_graph_client(self) -> GraphServiceClient:
        """Get an authenticated Microsoft Graph client."""
        async with self._lock:
            if self._graph_client_cache:
                return self._graph_client_cache

            try:
                credential = ClientCertificateCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    certificate_path=self.certificate_path,
                    password=self.certificate_password
                )
                client = GraphServiceClient(credentials=credential)
                self._graph_client_cache = client
                return client
            except Exception as e:
                logger.error(f"Failed to create GraphServiceClient: {e}")
                raise
```

### 2. Implement Resilience Handlers

#### Rate Limiter (`src/utils/rate_limiter.py`)
Implement the `RateLimiter` class to manage API calls based on SharePoint's resource unit model and to handle `Retry-After` headers gracefully.

#### Retry Strategy & Circuit Breaker (`src/utils/retry_handler.py`)
Create a `RetryStrategy` class that incorporates exponential backoff with jitter. This class should also manage a `CircuitBreaker` to prevent the application from repeatedly calling a failing service.

### 3. Implement the API Clients

Create `SharePointAPIClient` and `GraphAPIClient` classes. These clients will use the `AuthenticationManager` and integrate the resilience handlers for all `get`, `post`, and `batch` operations. They should abstract away the complexities of authentication, rate limiting, and retries from the rest of the application.

## Implementation Task Checklist

- [ ] Implement `AuthenticationManager` to get authenticated `ClientContext` and `GraphServiceClient`.
- [ ] Implement `RateLimiter` based on Microsoft's resource unit model.
- [ ] Implement `RetryStrategy` with exponential backoff and jitter.
- [ ] Implement the `CircuitBreaker` pattern within the retry strategy.
- [ ] Create the `SharePointAPIClient` and `GraphAPIClient`, integrating the auth manager, rate limiter, and retry strategy.
- [ ] Implement batch request processing for both SharePoint and Graph APIs to bundle multiple requests.
- [ ] Add structured logging to all API interactions, including request details, response status, and latency.
- [ ] Define specific API exception classes (`SharePointAPIError`, `GraphAPIError`) in `src/utils/exceptions.py`.

## Test Plan & Cases

Testing this phase requires extensive mocking of API responses to simulate various scenarios like success, throttling, and failures.

```python
# tests/test_api.py
import pytest
from unittest.mock import AsyncMock, patch
# from src.api.auth_manager import AuthenticationManager
# from src.api.sharepoint_client import SharePointAPIClient
# from src.utils.retry_handler import CircuitBreakerOpenError

@pytest.mark.asyncio
async def test_sharepoint_auth_success(auth_manager):
    """Test successful SharePoint authentication against a mock context."""
    with patch('office365.sharepoint.client_context.ClientContext') as MockContext:
        # Configure the mock to behave like the real object
        instance = MockContext.return_value
        instance.with_client_certificate.return_value = instance

        ctx = await auth_manager.get_sharepoint_context("https://tenant.sharepoint.com/sites/test")
        assert ctx is not None
        instance.with_client_certificate.assert_called_once()

@pytest.mark.asyncio
async def test_retry_on_429(api_client):
    """Test that the client automatically retries on a 429 (throttling) response."""
    with patch('aiohttp.ClientSession.get') as mock_get:
        # Simulate one 429 response, then a 200 response
        mock_get.side_effect = [
            AsyncMock(status=429, headers={'Retry-After': '1'}),
            AsyncMock(status=200, json=AsyncMock(return_value={'ok': True}))
        ]
        response = await api_client.get_with_retry("https://api.test.com/endpoint")
        assert response['ok'] is True
        assert mock_get.call_count == 2

@pytest.mark.asyncio
async def test_circuit_breaker_opens(retry_strategy):
    """Test that the circuit breaker opens after repeated failures."""
    # Simulate a function that always fails
    failing_func = AsyncMock(side_effect=Exception("API Down"))

    # Assuming the failure threshold is 3 for this test
    for _ in range(3):
        with pytest.raises(Exception):
            await retry_strategy.execute_with_retry("test_op", failing_func)

    # The circuit should now be open
    with pytest.raises(Exception): # Replace with CircuitBreakerOpenError
        await retry_strategy.execute_with_retry("test_op", failing_func)
```

## Verification & Validation

Create a verification script to test real-world connectivity against a test SharePoint tenant.

```python
# scripts/verify_api.py
import asyncio
# from src.utils.config_parser import load_config
# from src.api.auth_manager import AuthenticationManager
# from src.api.graph_client import GraphAPIClient

async def main():
    # config = load_config("config/config.json")
    # auth_manager = AuthenticationManager(config.auth)
    # graph_client = GraphAPIClient(auth_manager) # Simplified for verification

    # print("Attempting to get user profile via Graph API...")
    # try:
    #     me = await graph_client.get_me()
    #     print(f"Successfully authenticated as: {me['displayName']}")
    # except Exception as e:
    #     print(f"Verification failed: {e}")
    pass # Placeholder

if __name__ == "__main__":
    asyncio.run(main())
```
*Run this script with `python scripts/verify_api.py` after configuring `config/config.json`.*

## Done Criteria

- [ ] Authentication to both SharePoint and Graph APIs is successful using a real test tenant.
- [ ] API calls are automatically retried on transient errors (e.g., 503) and throttling (429).
- [ ] The circuit breaker opens after a configurable number of consecutive failures.
- [ ] The rate limiter correctly throttles requests to stay within defined resource unit limits.
- [ ] All unit tests for authentication, retries, and rate limiting pass.
