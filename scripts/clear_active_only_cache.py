#!/usr/bin/env python3
"""
Script to clear the active-only cache to test the Search API filtering
"""

import asyncio
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from database.repository import DatabaseRepository
from cache.cache_manager import CacheManager
from utils.config_parser import load_config


async def clear_cache():
    """Clear the active-only cache to force Search API execution."""

    # Load config
    config = load_config("config/config.json")

    # Initialize database repository
    db_path = config.db.path
    db_repo = DatabaseRepository(db_path)
    await db_repo.initialize_database()

    # Initialize cache manager
    cache_manager = CacheManager(db_repo)

    # Cache keys to clear
    cache_keys_to_clear = [
        "all_sites_active_True",    # active_only=True cache
        "all_sites_active_False",   # active_only=False cache (for comparison)
        "sites_delta_token",        # Delta token cache
    ]

    print("Clearing active-only related cache entries...")

    for key in cache_keys_to_clear:
        deleted = await cache_manager.delete(key)
        print(f"  - {key}: {'Deleted' if deleted else 'Not found'}")

    # Show cache stats
    stats = cache_manager.stats()
    print(f"\nCache stats after clearing:")
    print(f"  Memory cache size: {stats['memory_cache']['size']}")
    print(f"  Total hits: {stats['cache_stats']['l1_hits'] + stats['cache_stats']['l2_hits']}")
    print(f"  Total misses: {stats['cache_stats']['misses']}")
    print(f"  Hit rate: {stats['cache_stats']['hit_rate']:.2%}")

    print("\n✅ Cache cleared successfully!")
    print("Now run the CLI with --active-only to test if Search API is executed.")


if __name__ == "__main__":
    asyncio.run(clear_cache())
