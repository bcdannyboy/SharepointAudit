#!/usr/bin/env python3
"""
Primary Debug Script for --active-only Flag Bug Investigation

This script compares current vs proposed filtering approaches for the SharePoint
audit tool's --active-only flag. It logs all API calls, displays site properties,
and generates comprehensive reports with recommendations.

Usage:
    python3 scripts/debug_active_only_flag.py [--config config/config.json] [--limit 50] [--verbose]
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import argparse
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from api.auth_manager import AuthenticationManager
from api.graph_client import GraphAPIClient
from utils.config_parser import load_config
from utils.rate_limiter import RateLimiter
from utils.retry_handler import RetryStrategy, RetryConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ActiveOnlyDebugger:
    """Debug the --active-only flag implementation."""

    def __init__(self, config_path: str, limit: int = 50, verbose: bool = False):
        self.config_path = config_path
        self.limit = limit
        self.verbose = verbose
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'api_calls': [],
            'current_approach': {},
            'proposed_approaches': {},
            'site_samples': [],
            'performance_metrics': {},
            'recommendations': []
        }

        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
            logging.getLogger('api.graph_client').setLevel(logging.DEBUG)

    async def run_debug_analysis(self):
        """Run the complete debug analysis."""
        logger.info("Starting --active-only flag debug analysis")

        # Load configuration and initialize clients
        config = load_config(self.config_path)
        auth_manager = AuthenticationManager(config.auth)
        rate_limiter = RateLimiter()
        retry_strategy = RetryStrategy(RetryConfig())

        # Create custom graph client with API call logging
        graph_client = LoggingGraphAPIClient(
            auth_manager, retry_strategy, rate_limiter, self.results['api_calls']
        )

        try:
            logger.info("=== PHASE 1: Current Implementation Analysis ===")
            await self._analyze_current_implementation(graph_client)

            logger.info("=== PHASE 2: Proposed Approach Testing ===")
            await self._test_proposed_approaches(graph_client)

            logger.info("=== PHASE 3: Site Property Analysis ===")
            await self._analyze_site_properties(graph_client)

            logger.info("=== PHASE 4: Performance Comparison ===")
            await self._compare_performance(graph_client)

            logger.info("=== PHASE 5: Generate Recommendations ===")
            self._generate_recommendations()

            # Save results
            await self._save_results()

        finally:
            await graph_client.close()

    async def _analyze_current_implementation(self, graph_client):
        """Analyze the current --active-only implementation."""
        logger.info("Testing current implementation (active_only=True)")

        start_time = time.time()

        # Call current implementation with active_only=True
        current_result = await graph_client.get_all_sites_delta(
            delta_token=None, active_only=True
        )

        elapsed = time.time() - start_time

        self.results['current_approach'] = {
            'method': 'Delta API + Search API fallback + client-side filtering',
            'sites_returned': len(current_result.get('value', [])),
            'elapsed_time': elapsed,
            'api_calls_made': len([call for call in self.results['api_calls']
                                 if call['timestamp'] >= start_time]),
            'sample_sites': current_result.get('value', [])[:10]  # First 10 for analysis
        }

        logger.info(f"Current approach returned {len(current_result.get('value', []))} sites in {elapsed:.2f}s")

        # Test current implementation with active_only=False for comparison
        start_time = time.time()
        all_sites_result = await graph_client.get_all_sites_delta(
            delta_token=None, active_only=False
        )
        elapsed = time.time() - start_time

        self.results['current_approach']['all_sites_count'] = len(all_sites_result.get('value', []))
        self.results['current_approach']['all_sites_time'] = elapsed

        logger.info(f"Current approach (active_only=False) returned {len(all_sites_result.get('value', []))} sites")

    async def _test_proposed_approaches(self, graph_client):
        """Test various proposed filtering approaches."""

        # Approach 1: Pure Search API with enhanced filtering
        await self._test_enhanced_search_api(graph_client)

        # Approach 2: Sites endpoint with OData filters
        await self._test_sites_endpoint_filtering(graph_client)

        # Approach 3: Combined approach
        await self._test_combined_approach(graph_client)

    async def _test_enhanced_search_api(self, graph_client):
        """Test enhanced Search API approach."""
        logger.info("Testing Enhanced Search API approach")

        start_time = time.time()
        api_calls_before = len(self.results['api_calls'])

        # Enhanced search query to exclude archived/inactive sites
        search_queries = [
            # Basic query excluding personal sites and common archived patterns
            "NOT path:*/personal/* AND NOT displayName:*archived* AND NOT displayName:*test* AND NOT displayName:*old*",

            # More comprehensive exclusions
            "NOT path:*/personal/* AND NOT path:*/appcatalog/* AND NOT displayName:*archived* AND NOT displayName:*test* AND NOT displayName:*demo* AND NOT displayName:*old* AND NOT displayName:*backup*",

            # Include only active content types
            "contentClass:STS_Site AND NOT path:*/personal/* AND NOT displayName:*archived*"
        ]

        results = {}
        for i, query in enumerate(search_queries):
            search_start = time.time()

            search_url = "https://graph.microsoft.com/v1.0/search/query"
            search_body = {
                "requests": [{
                    "entityTypes": ["site"],
                    "query": {
                        "queryString": query
                    },
                    "from": 0,
                    "size": min(500, self.limit),  # API maximum is 500
                    "fields": ["id", "webUrl", "displayName", "description", "createdDateTime", "lastModifiedDateTime"]
                }]
            }

            try:
                search_result = await graph_client.post_with_retry(search_url, json=search_body)

                sites = []
                if search_result and 'value' in search_result:
                    for response in search_result['value']:
                        for container in response.get('hitsContainers', []):
                            for hit in container.get('hits', []):
                                sites.append(hit.get('resource', {}))

                search_elapsed = time.time() - search_start

                results[f'query_{i+1}'] = {
                    'query': query,
                    'sites_found': len(sites),
                    'elapsed_time': search_elapsed,
                    'sample_sites': sites[:5]
                }

                logger.info(f"Search query {i+1} found {len(sites)} sites in {search_elapsed:.2f}s")

            except Exception as e:
                logger.error(f"Search query {i+1} failed: {e}")
                results[f'query_{i+1}'] = {
                    'query': query,
                    'error': str(e)
                }

        total_elapsed = time.time() - start_time
        api_calls_made = len(self.results['api_calls']) - api_calls_before

        self.results['proposed_approaches']['enhanced_search_api'] = {
            'method': 'Enhanced Search API with OData filtering',
            'queries_tested': len(search_queries),
            'results': results,
            'total_elapsed_time': total_elapsed,
            'api_calls_made': api_calls_made
        }

    async def _test_sites_endpoint_filtering(self, graph_client):
        """Test direct sites endpoint with OData filtering."""
        logger.info("Testing Sites endpoint with OData filtering")

        start_time = time.time()
        api_calls_before = len(self.results['api_calls'])

        # Test various OData filter combinations
        filters = [
            # Basic filters (may not work with sites endpoint)
            "$filter=isArchived eq false",
            "$filter=deleted eq null",
            "$filter=not(startswith(displayName,'archived'))",
            "$filter=not(startswith(displayName,'test'))",
            # Combined filters
            "$filter=not(startswith(displayName,'archived')) and not(startswith(displayName,'test'))"
        ]

        results = {}
        for i, filter_param in enumerate(filters):
            filter_start = time.time()

            # Try with the sites endpoint
            url = f"https://graph.microsoft.com/v1.0/sites?{filter_param}&$top={min(50, self.limit)}"

            try:
                filter_result = await graph_client.get_with_retry(url)
                filter_elapsed = time.time() - filter_start

                sites = filter_result.get('value', [])

                results[f'filter_{i+1}'] = {
                    'filter': filter_param,
                    'sites_found': len(sites),
                    'elapsed_time': filter_elapsed,
                    'sample_sites': sites[:3]
                }

                logger.info(f"OData filter {i+1} found {len(sites)} sites in {filter_elapsed:.2f}s")

            except Exception as e:
                logger.warning(f"OData filter {i+1} failed: {e}")
                results[f'filter_{i+1}'] = {
                    'filter': filter_param,
                    'error': str(e)
                }

        total_elapsed = time.time() - start_time
        api_calls_made = len(self.results['api_calls']) - api_calls_before

        self.results['proposed_approaches']['sites_endpoint_filtering'] = {
            'method': 'Sites endpoint with OData filters',
            'filters_tested': len(filters),
            'results': results,
            'total_elapsed_time': total_elapsed,
            'api_calls_made': api_calls_made
        }

    async def _test_combined_approach(self, graph_client):
        """Test combined approach: Search API + property validation."""
        logger.info("Testing Combined approach")

        start_time = time.time()
        api_calls_before = len(self.results['api_calls'])

        # Step 1: Use Search API to get candidate sites
        search_url = "https://graph.microsoft.com/v1.0/search/query"
        search_body = {
            "requests": [{
                "entityTypes": ["site"],
                "query": {
                    "queryString": "NOT path:*/personal/*"
                },
                "from": 0,
                "size": min(100, self.limit),
                "fields": ["id", "webUrl", "displayName", "description", "createdDateTime", "lastModifiedDateTime"]
            }]
        }

        try:
            search_result = await graph_client.post_with_retry(search_url, json=search_body)

            candidate_sites = []
            if search_result and 'value' in search_result:
                for response in search_result['value']:
                    for container in response.get('hitsContainers', []):
                        for hit in container.get('hits', []):
                            candidate_sites.append(hit.get('resource', {}))

            logger.info(f"Found {len(candidate_sites)} candidate sites from search")

            # Step 2: Get detailed properties for first few sites
            detailed_sites = []
            for site in candidate_sites[:10]:  # Limit to 10 for testing
                site_id = site.get('id', '')
                if site_id:
                    try:
                        # Get detailed site properties
                        detailed_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}?$select=*"
                        detailed_result = await graph_client.get_with_retry(detailed_url)

                        # Analyze properties for active status indicators
                        is_active = self._analyze_site_active_status(detailed_result)
                        detailed_result['_computed_is_active'] = is_active
                        detailed_sites.append(detailed_result)

                    except Exception as e:
                        logger.warning(f"Failed to get detailed info for site {site_id}: {e}")

            combined_elapsed = time.time() - start_time
            api_calls_made = len(self.results['api_calls']) - api_calls_before

            self.results['proposed_approaches']['combined_approach'] = {
                'method': 'Search API + detailed property validation',
                'candidate_sites_found': len(candidate_sites),
                'detailed_sites_analyzed': len(detailed_sites),
                'estimated_active_sites': len([s for s in detailed_sites if s.get('_computed_is_active', True)]),
                'elapsed_time': combined_elapsed,
                'api_calls_made': api_calls_made,
                'sample_detailed_sites': detailed_sites[:5]
            }

            logger.info(f"Combined approach: {len(detailed_sites)} sites analyzed, "
                       f"{len([s for s in detailed_sites if s.get('_computed_is_active', True)])} estimated active")

        except Exception as e:
            logger.error(f"Combined approach failed: {e}")
            self.results['proposed_approaches']['combined_approach'] = {
                'method': 'Search API + detailed property validation',
                'error': str(e)
            }

    def _analyze_site_active_status(self, site_data: Dict[str, Any]) -> bool:
        """Analyze site properties to determine if it's active."""

        # Check display name for archived indicators
        display_name = site_data.get('displayName', '').lower()
        if any(pattern in display_name for pattern in ['archived', 'old', 'test', 'demo', 'backup', 'delete']):
            return False

        # Check URL for personal sites
        web_url = site_data.get('webUrl', '').lower()
        if '/personal/' in web_url or '-my.sharepoint.com' in web_url:
            return False

        # Check for last modified date (sites not modified in 6+ months might be inactive)
        last_modified = site_data.get('lastModifiedDateTime')
        if last_modified:
            try:
                from datetime import datetime, timezone, timedelta
                last_mod_date = datetime.fromisoformat(last_modified.replace('Z', '+00:00'))
                six_months_ago = datetime.now(timezone.utc) - timedelta(days=180)
                if last_mod_date < six_months_ago:
                    # Note: This is heuristic, not definitive
                    pass
            except:
                pass

        # Default to active if no clear indicators of inactivity
        return True

    async def _analyze_site_properties(self, graph_client):
        """Analyze available site properties to understand status indicators."""
        logger.info("Analyzing site properties for status indicators")

        # Get a sample of sites with all available properties
        try:
            # Get sites via delta API to see all available properties
            delta_url = "https://graph.microsoft.com/v1.0/sites/delta?$top=10&$select=*"
            delta_result = await graph_client.get_with_retry(delta_url)

            sample_sites = delta_result.get('value', [])[:5]  # Analyze first 5

            # Get detailed properties for each site
            detailed_properties = []
            for site in sample_sites:
                site_id = site.get('id', '')
                if site_id:
                    try:
                        # Get ALL available properties
                        detailed_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}?$select=*"
                        detailed_site = await graph_client.get_with_retry(detailed_url)
                        detailed_properties.append(detailed_site)
                    except Exception as e:
                        logger.warning(f"Failed to get detailed properties for site {site_id}: {e}")

            self.results['site_samples'] = detailed_properties

            # Analyze property patterns
            all_properties = set()
            for site in detailed_properties:
                all_properties.update(site.keys())

            logger.info(f"Found {len(all_properties)} unique properties across sample sites")
            logger.info(f"Sample properties: {sorted(list(all_properties))[:20]}")

        except Exception as e:
            logger.error(f"Site property analysis failed: {e}")
            self.results['site_samples'] = []

    async def _compare_performance(self, graph_client):
        """Compare performance of different approaches."""
        logger.info("Comparing performance metrics")

        # Summarize API call counts and timing
        current_calls = self.results['current_approach'].get('api_calls_made', 0)
        current_time = self.results['current_approach'].get('elapsed_time', 0)

        search_calls = self.results['proposed_approaches'].get('enhanced_search_api', {}).get('api_calls_made', 0)
        search_time = self.results['proposed_approaches'].get('enhanced_search_api', {}).get('total_elapsed_time', 0)

        combined_calls = self.results['proposed_approaches'].get('combined_approach', {}).get('api_calls_made', 0)
        combined_time = self.results['proposed_approaches'].get('combined_approach', {}).get('elapsed_time', 0)

        self.results['performance_metrics'] = {
            'current_implementation': {
                'api_calls': current_calls,
                'elapsed_time': current_time,
                'sites_per_second': (self.results['current_approach'].get('sites_returned', 0) / current_time) if current_time > 0 else 0
            },
            'enhanced_search_api': {
                'api_calls': search_calls,
                'elapsed_time': search_time,
                'estimated_efficiency_gain': (current_time - search_time) / current_time * 100 if current_time > 0 else 0
            },
            'combined_approach': {
                'api_calls': combined_calls,
                'elapsed_time': combined_time,
                'estimated_efficiency_gain': (current_time - combined_time) / current_time * 100 if current_time > 0 else 0
            }
        }

    def _generate_recommendations(self):
        """Generate recommendations based on analysis."""
        recommendations = []

        # Analyze current implementation issues
        current_sites = self.results['current_approach'].get('sites_returned', 0)
        all_sites = self.results['current_approach'].get('all_sites_count', 0)

        if all_sites > 0:
            filter_effectiveness = (all_sites - current_sites) / all_sites * 100
            recommendations.append(f"Current filtering effectiveness: {filter_effectiveness:.1f}% of sites filtered out")

        # API efficiency recommendations
        current_calls = self.results['current_approach'].get('api_calls_made', 0)
        if current_calls > 5:
            recommendations.append(f"Current implementation makes {current_calls} API calls - optimization needed")

        # Search API recommendations
        search_results = self.results['proposed_approaches'].get('enhanced_search_api', {}).get('results', {})
        if search_results:
            best_query = None
            best_count = 0
            for query_key, result in search_results.items():
                if 'sites_found' in result and result['sites_found'] > best_count:
                    best_count = result['sites_found']
                    best_query = result.get('query')

            if best_query:
                recommendations.append(f"Best search query found {best_count} sites: {best_query[:100]}...")

        # Performance recommendations
        perf = self.results.get('performance_metrics', {})
        search_gain = perf.get('enhanced_search_api', {}).get('estimated_efficiency_gain', 0)
        if search_gain > 0:
            recommendations.append(f"Enhanced Search API could improve performance by {search_gain:.1f}%")

        # Property analysis recommendations
        if self.results.get('site_samples'):
            recommendations.append("Site property analysis reveals additional filtering opportunities")
            recommendations.append("Consider implementing server-side filtering using available site properties")

        self.results['recommendations'] = recommendations

    async def _save_results(self):
        """Save debug results to file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"debug_active_only_results_{timestamp}.json"

        with open(output_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)

        logger.info(f"Debug results saved to: {output_file}")

        # Print summary
        print("\n" + "="*80)
        print("ACTIVE-ONLY FLAG DEBUG ANALYSIS SUMMARY")
        print("="*80)

        current = self.results['current_approach']
        print(f"Current Implementation:")
        print(f"  - Sites returned: {current.get('sites_returned', 0)}")
        print(f"  - All sites available: {current.get('all_sites_count', 0)}")
        print(f"  - Time elapsed: {current.get('elapsed_time', 0):.2f}s")
        print(f"  - API calls made: {current.get('api_calls_made', 0)}")

        print(f"\nAPI Calls Logged: {len(self.results['api_calls'])}")
        print(f"Site Samples Analyzed: {len(self.results['site_samples'])}")

        print(f"\nRecommendations:")
        for i, rec in enumerate(self.results['recommendations'], 1):
            print(f"  {i}. {rec}")

        print(f"\nDetailed results saved to: {output_file}")
        print("="*80)


class LoggingGraphAPIClient(GraphAPIClient):
    """GraphAPIClient that logs all API calls for debugging."""

    def __init__(self, auth_manager, retry_strategy, rate_limiter, api_calls_log):
        super().__init__(auth_manager, retry_strategy, rate_limiter)
        self.api_calls_log = api_calls_log

    async def get_with_retry(self, url: str, **kwargs) -> Any:
        """Override to log GET requests."""
        start_time = time.time()

        try:
            result = await super().get_with_retry(url, **kwargs)
            elapsed = time.time() - start_time

            self.api_calls_log.append({
                'timestamp': start_time,
                'method': 'GET',
                'url': url,
                'elapsed_time': elapsed,
                'success': True,
                'response_size': len(result.get('value', [])) if isinstance(result, dict) else 'unknown'
            })

            return result

        except Exception as e:
            elapsed = time.time() - start_time
            self.api_calls_log.append({
                'timestamp': start_time,
                'method': 'GET',
                'url': url,
                'elapsed_time': elapsed,
                'success': False,
                'error': str(e)
            })
            raise

    async def post_with_retry(self, url: str, **kwargs) -> Any:
        """Override to log POST requests."""
        start_time = time.time()

        try:
            result = await super().post_with_retry(url, **kwargs)
            elapsed = time.time() - start_time

            self.api_calls_log.append({
                'timestamp': start_time,
                'method': 'POST',
                'url': url,
                'elapsed_time': elapsed,
                'success': True,
                'request_body': kwargs.get('json', {}) if 'json' in kwargs else 'unknown'
            })

            return result

        except Exception as e:
            elapsed = time.time() - start_time
            self.api_calls_log.append({
                'timestamp': start_time,
                'method': 'POST',
                'url': url,
                'elapsed_time': elapsed,
                'success': False,
                'error': str(e),
                'request_body': kwargs.get('json', {}) if 'json' in kwargs else 'unknown'
            })
            raise


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Debug --active-only flag implementation")
    parser.add_argument("--config", default="config/config.json", help="Configuration file path")
    parser.add_argument("--limit", type=int, default=50, help="Limit number of sites to test")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    debugger = ActiveOnlyDebugger(args.config, args.limit, args.verbose)
    await debugger.run_debug_analysis()


if __name__ == "__main__":
    asyncio.run(main())
