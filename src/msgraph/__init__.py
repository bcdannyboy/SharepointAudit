# Mock msgraph module for when real SDK is not available
class GraphServiceClient:
    """Stub GraphServiceClient for testing."""
    def __init__(self, credentials=None):
        self.credentials = credentials
