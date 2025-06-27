# Mock ClientContext for testing
class ClientContext:
    """Stub ClientContext for testing when Office365 library is unavailable."""

    def __init__(self, site_url: str = "") -> None:
        self.site_url = site_url

    @classmethod
    def connect_with_certificate(cls, site_url: str, **_kwargs):
        return cls(site_url)
