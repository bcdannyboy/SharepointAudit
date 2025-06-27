# Mock aiohttp for testing when the real package is not available
try:
    import aiohttp
except ImportError:
    # Create a simple mock
    import sys
    from . import aiohttp as mock_aiohttp
    sys.modules['aiohttp'] = mock_aiohttp

__all__ = []
