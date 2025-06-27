import asyncio
import logging
from typing import Dict, Optional

try:
    from office365.sharepoint.client_context import ClientContext
except Exception:  # pragma: no cover - fallback stub
    class ClientContext:
        """Fallback stub for testing when Office365 library is unavailable."""

        def __init__(self, site_url: str = "") -> None:
            self.site_url = site_url

        @classmethod
        def connect_with_certificate(cls, site_url: str, **_kwargs):
            return cls(site_url)

try:
    from msgraph import GraphServiceClient
except Exception:  # pragma: no cover - fallback stub
    class GraphServiceClient:  # type: ignore
        def __init__(self, credentials=None):
            self.credentials = credentials

try:
    from azure.identity.aio import ClientCertificateCredential
except Exception:  # pragma: no cover - fallback stub
    class ClientCertificateCredential:  # type: ignore
        def __init__(self, **_kwargs):
            pass

from ..utils.config_parser import AuthConfig

logger = logging.getLogger(__name__)


class AuthenticationManager:
    """Handles certificate-based authentication for SharePoint and Graph APIs."""

    def __init__(self, config: AuthConfig) -> None:
        self.tenant_id = config.tenant_id
        self.client_id = config.client_id
        self.certificate_path = config.certificate_path
        self.certificate_password = getattr(config, "certificate_password", None)
        self._context_cache: Dict[str, ClientContext] = {}
        self._graph_client_cache: Optional[GraphServiceClient] = None
        self._lock = asyncio.Lock()

    async def get_sharepoint_context(self, site_url: str) -> ClientContext:
        """Return an authenticated SharePoint ClientContext."""
        async with self._lock:
            if site_url in self._context_cache:
                return self._context_cache[site_url]

            try:
                ctx = ClientContext.connect_with_certificate(
                    site_url,
                    tenant=self.tenant_id,
                    client_id=self.client_id,
                    cert_path=self.certificate_path,
                    thumbprint=None,
                    cert_password=self.certificate_password,
                )
                self._context_cache[site_url] = ctx
                return ctx
            except Exception as exc:  # pragma: no cover - real error logging
                logger.error("Failed to authenticate to %s: %s", site_url, exc)
                raise

    async def get_graph_client(self) -> GraphServiceClient:
        """Return an authenticated Microsoft Graph client."""
        async with self._lock:
            if self._graph_client_cache is not None:
                return self._graph_client_cache

            try:
                credential = ClientCertificateCredential(
                    tenant_id=self.tenant_id,
                    client_id=self.client_id,
                    certificate_path=self.certificate_path,
                    password=self.certificate_password,
                )
                client = GraphServiceClient(credentials=credential)
                self._graph_client_cache = client
                return client
            except Exception as exc:  # pragma: no cover - real error logging
                logger.error("Failed to create GraphServiceClient: %s", exc)
                raise
