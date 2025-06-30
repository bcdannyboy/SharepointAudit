# Minimal TTLCache implementation for testing purposes
from collections import OrderedDict
import time

class TTLCache:
    """Simple TTL-based cache with maxsize and LRU eviction."""

    def __init__(self, maxsize=128, ttl=600):
        self.maxsize = maxsize
        self.ttl = ttl
        self._store = OrderedDict()

    def _expire(self, key):
        value, expires_at = self._store.get(key, (None, 0))
        if expires_at and expires_at < time.time():
            self._store.pop(key, None)
            return True
        return False

    def __contains__(self, key):
        if key in self._store and not self._expire(key):
            return True
        return False

    def __getitem__(self, key):
        if key in self and not self._expire(key):
            value, _ = self._store[key]
            self._store.move_to_end(key)
            return value
        raise KeyError(key)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __setitem__(self, key, value):
        expires_at = time.time() + self.ttl if self.ttl else 0
        self._store[key] = (value, expires_at)
        self._store.move_to_end(key)
        while len(self._store) > self.maxsize:
            self._store.popitem(last=False)

    def pop(self, key, default=None):
        if key in self._store:
            value, _ = self._store.pop(key)
            return value
        return default

    def clear(self):
        self._store.clear()
