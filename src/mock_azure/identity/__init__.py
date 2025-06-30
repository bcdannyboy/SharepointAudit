# Mock azure.identity module

class AccessToken:
    """Mock AccessToken class."""
    def __init__(self, token: str, expires_on: int):
        self.token = token
        self.expires_on = expires_on

class ClientCertificateCredential:
    """Stub ClientCertificateCredential for testing."""
    def __init__(self, tenant_id: str, client_id: str, certificate_path: str, password: str = None, **kwargs):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.certificate_path = certificate_path
        self.password = password

    def get_token(self, *scopes, **kwargs):
        """Mock get_token method that returns a dummy token."""
        # Return a mock token that expires in 1 hour
        import time
        return AccessToken(
            token="mock_access_token_for_testing",
            expires_on=int(time.time()) + 3600
        )
