# Mock cachetools module for testing
class TTLCache(dict):
    """Simple TTL cache mock that behaves like a dict."""
    def __init__(self, maxsize=128, ttl=600):
        super().__init__()
        self.maxsize = maxsize
        self.ttl = ttl
