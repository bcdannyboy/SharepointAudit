import asyncio
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

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
    MSGRAPH_AVAILABLE = True
except Exception as e:  # pragma: no cover - fallback stub
    logger.warning(f"Could not import msgraph: {e}")
    MSGRAPH_AVAILABLE = False
    class GraphServiceClient:  # type: ignore
        def __init__(self, credentials=None, **kwargs):
            self.credentials = credentials

try:
    from azure.identity import CertificateCredential as ClientCertificateCredential
except Exception:  # pragma: no cover - fallback stub
    class ClientCertificateCredential:  # type: ignore
        def __init__(self, **_kwargs):
            pass

        def get_token(self, *scopes, **kwargs):
            """Stub get_token method for testing."""
            from collections import namedtuple
            Token = namedtuple('Token', ['token', 'expires_on'])
            return Token(token="stub_token", expires_on=0)

from utils.config_parser import AuthConfig


class AuthenticationManager:
    """Handles certificate-based authentication for SharePoint and Graph APIs."""

    def __init__(self, config: AuthConfig) -> None:
        self.tenant_id = config.tenant_id
        self.client_id = config.client_id
        self.certificate_path = config.certificate_path
        self.certificate_password = getattr(config, "certificate_password", None)
        self._context_cache: Dict[str, ClientContext] = {}
        self._graph_client_cache: Optional[GraphServiceClient] = None
        self._credential_cache: Optional[ClientCertificateCredential] = None
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

    async def get_credential(self) -> ClientCertificateCredential:
        """Return the certificate credential for authentication."""
        async with self._lock:
            if self._credential_cache is not None:
                return self._credential_cache

            try:
                kwargs = {
                    "tenant_id": self.tenant_id,
                    "client_id": self.client_id,
                    "certificate_path": self.certificate_path,
                }
                if self.certificate_password:
                    kwargs["password"] = self.certificate_password

                credential = ClientCertificateCredential(**kwargs)
                self._credential_cache = credential
                return credential
            except Exception as exc:
                logger.error("Failed to create credential: %s", exc)
                raise

    async def get_graph_client(self) -> GraphServiceClient:
        """Return an authenticated Microsoft Graph client."""
        async with self._lock:
            if self._graph_client_cache is not None:
                return self._graph_client_cache

            try:
                # Get or create credential
                if self._credential_cache is None:
                    self._credential_cache = await self.get_credential()

                credential = self._credential_cache
                logger.info(f"Using credential of type: {type(credential).__name__} from module: {type(credential).__module__}")
                logger.info(f"Credential has get_token method: {hasattr(credential, 'get_token')}")
                logger.info(f"MSGRAPH_AVAILABLE: {MSGRAPH_AVAILABLE}")

                # For app-only authentication, use the .default scope
                scopes = ['https://graph.microsoft.com/.default']

                # Create client based on whether msgraph is available
                if MSGRAPH_AVAILABLE:
                    client = GraphServiceClient(
                        credentials=credential,
                        scopes=scopes
                    )
                else:
                    # Fallback mode - just store the credential
                    client = GraphServiceClient(
                        credentials=credential
                    )
                self._graph_client_cache = client
                return client
            except Exception as exc:  # pragma: no cover - real error logging
                logger.error("Failed to create GraphServiceClient: %s", exc)
                raise
