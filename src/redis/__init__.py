# Mock redis module

class Redis:
    """Mock Redis client."""
    async def get(self, key):
        return None

    async def setex(self, key, ttl, value):
        pass

def from_url(url):
    """Return mock Redis instance."""
    return Redis()
