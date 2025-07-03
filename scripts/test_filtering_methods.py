#!/usr/bin/env python3
"""
Filter Testing Script for --active-only Flag Debug

This script tests the proposed enhanced Search API queries, validates OData filter
syntax for excluding non-active sites, compares results between different filtering
approaches, measures API response times and pagination handling, and generates
comparison reports.

Usage:
    python3 scripts/test_filtering_methods.py [--config config/config.json] [--comprehensive] [--benchmark]
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import argparse
import sys
import statistics

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


class FilterTestingFramework:
    """Framework for testing different SharePoint site filtering methods."""

    def __init__(self, config_path: str, comprehensive: bool = False, benchmark: bool = False):
        self.config_path = config_path
        self.comprehensive = comprehensive
        self.benchmark = benchmark
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'test_configuration': {
                'comprehensive': comprehensive,
                'benchmark': benchmark
            },
            'baseline_results': {},
            'filter_tests': [],
            'performance_comparison': {},
            'effectiveness_analysis': {},
            'recommendations': []
        }

    async def run_filter_testing(self):
        """Run comprehensive filter testing."""
        logger.info("Starting comprehensive filter testing framework")

        # Load configuration and initialize clients
        config = load_config(self.config_path)
        auth_manager = AuthenticationManager(config.auth)
        rate_limiter = RateLimiter()
        retry_strategy = RetryStrategy(RetryConfig())

        graph_client = GraphAPIClient(auth_manager, retry_strategy, rate_limiter)

        try:
            logger.info("=== PHASE 1: Establish Baseline (Current Implementation) ===")
            await self._establish_baseline(graph_client)

            logger.info("=== PHASE 2: Test Enhanced Search API Queries ===")
            await self._test_enhanced_search_queries(graph_client)

            logger.info("=== PHASE 3: Test OData Filter Combinations ===")
            await self._test_odata_filter_combinations(graph_client)

            logger.info("=== PHASE 4: Test Pagination Handling ===")
            await self._test_pagination_handling(graph_client)

            if self.benchmark:
                logger.info("=== PHASE 5: Performance Benchmarking ===")
                await self._run_performance_benchmarks(graph_client)

            logger.info("=== PHASE 6: Effectiveness Analysis ===")
            await self._analyze_filtering_effectiveness(graph_client)

            logger.info("=== PHASE 7: Generate Comparison Report ===")
            await self._generate_comparison_report()

        finally:
            await graph_client.close()

    async def _establish_baseline(self, graph_client):
        """Establish baseline using current implementation."""
        logger.info("Establishing baseline with current implementation")

        # Test current active_only=False (all sites)
        start_time = time.time()
        all_sites_result = await graph_client.get_all_sites_delta(delta_token=None, active_only=False)
        all_sites_time = time.time() - start_time
        all_sites = all_sites_result.get('value', [])

        # Test current active_only=True (filtered sites)
        start_time = time.time()
        active_sites_result = await graph_client.get_all_sites_delta(delta_token=None, active_only=True)
        active_sites_time = time.time() - start_time
        active_sites = active_sites_result.get('value', [])

        self.results['baseline_results'] = {
            'all_sites': {
                'count': len(all_sites),
                'elapsed_time': all_sites_time,
                'sites_per_second': len(all_sites) / all_sites_time if all_sites_time > 0 else 0,
                'sample_sites': all_sites[:10]
            },
            'current_active_filtering': {
                'count': len(active_sites),
                'elapsed_time': active_sites_time,
                'sites_per_second': len(active_sites) / active_sites_time if active_sites_time > 0 else 0,
                'filter_effectiveness': (len(all_sites) - len(active_sites)) / len(all_sites) * 100 if len(all_sites) > 0 else 0,
                'sample_sites': active_sites[:10]
            }
        }

        logger.info(f"Baseline: {len(all_sites)} total sites, {len(active_sites)} active sites")
        logger.info(f"Current filtering removes {len(all_sites) - len(active_sites)} sites "
                   f"({self.results['baseline_results']['current_active_filtering']['filter_effectiveness']:.1f}%)")

    async def _test_enhanced_search_queries(self, graph_client):
        """Test enhanced Search API queries with various filtering strategies."""
        logger.info("Testing enhanced Search API queries")

        # Define comprehensive search query strategies
        search_strategies = [
            {
                'name': 'Basic Personal Site Exclusion',
                'query': 'NOT path:*/personal/*',
                'description': 'Exclude only personal OneDrive sites'
            },
            {
                'name': 'Pattern-based Archived Exclusion',
                'query': 'NOT path:*/personal/* AND NOT displayName:*archived* AND NOT displayName:*test* AND NOT displayName:*old*',
                'description': 'Exclude personal sites and common archived/test patterns'
            },
            {
                'name': 'Comprehensive Pattern Exclusion',
                'query': 'NOT path:*/personal/* AND NOT path:*/appcatalog/* AND NOT displayName:*archived* AND NOT displayName:*test* AND NOT displayName:*demo* AND NOT displayName:*old* AND NOT displayName:*backup* AND NOT displayName:*temp*',
                'description': 'Comprehensive exclusion of system, archived, and test sites'
            },
            {
                'name': 'Content Class Filtering',
                'query': 'contentClass:STS_Site AND NOT path:*/personal/*',
                'description': 'Include only SharePoint sites, exclude personal sites'
            },
            {
                'name': 'Active Content with Time Filter',
                'query': 'contentClass:STS_Site AND NOT path:*/personal/* AND lastModifiedTime>=2023-01-01',
                'description': 'Sites modified within recent timeframe'
            },
            {
                'name': 'URL Pattern Exclusion',
                'query': 'NOT path:*/personal/* AND NOT path:*-my.sharepoint.com* AND NOT path:*/appcatalog/*',
                'description': 'Exclude based on URL patterns'
            }
        ]

        if self.comprehensive:
            # Add more complex queries for comprehensive testing
            search_strategies.extend([
                {
                    'name': 'Multi-field Pattern Matching',
                    'query': '(displayName:"*" AND NOT displayName:*archived* AND NOT displayName:*test*) AND (path:"*" AND NOT path:*/personal/*)',
                    'description': 'Multi-field pattern matching approach'
                },
                {
                    'name': 'Boolean Logic Complex',
                    'query': '(contentClass:STS_Site OR contentClass:STS_Web) AND NOT (path:*/personal/* OR displayName:*archived* OR displayName:*test* OR displayName:*demo*)',
                    'description': 'Complex boolean logic with OR conditions'
                }
            ])

        search_test_results = []

        for strategy in search_strategies:
            await self._test_single_search_strategy(graph_client, strategy, search_test_results)

        self.results['filter_tests'].extend([{
            'method': 'Enhanced Search API',
            'strategies_tested': len(search_strategies),
            'results': search_test_results
        }])

    async def _test_single_search_strategy(self, graph_client, strategy: Dict[str, str], results: List[Dict]):
        """Test a single search strategy."""
        logger.info(f"Testing search strategy: {strategy['name']}")

        start_time = time.time()
        all_sites = []
        api_calls = 0
        errors = []

        try:
            # Implement pagination for comprehensive results
            from_index = 0
            page_size = 500  # Maximum allowed by Search API

            while True:
                search_url = "https://graph.microsoft.com/v1.0/search/query"
                search_body = {
                    "requests": [{
                        "entityTypes": ["site"],
                        "query": {
                            "queryString": strategy['query']
                        },
                        "from": from_index,
                        "size": page_size,
                        "fields": ["id", "webUrl", "displayName", "description", "createdDateTime", "lastModifiedDateTime"]
                    }]
                }

                try:
                    search_result = await graph_client.post_with_retry(search_url, json=search_body)
                    api_calls += 1

                    page_sites = []
                    has_more = False

                    if search_result and 'value' in search_result:
                        for response in search_result['value']:
                            for container in response.get('hitsContainers', []):
                                hits = container.get('hits', [])
                                for hit in hits:
                                    resource = hit.get('resource', {})
                                    if resource:
                                        page_sites.append(resource)

                                # Check if there are more results
                                has_more = container.get('moreResultsAvailable', False)

                    all_sites.extend(page_sites)

                    # Break if no more results or if we're testing pagination limits
                    if not has_more or len(page_sites) == 0:
                        break

                    # For comprehensive testing, continue pagination
                    if not self.comprehensive and len(all_sites) >= 1000:
                        break

                    from_index += page_size

                    # Rate limiting between pages
                    await asyncio.sleep(0.1)

                except Exception as page_error:
                    errors.append(f"Page {from_index//page_size + 1}: {str(page_error)}")
                    break

            elapsed_time = time.time() - start_time

            # Analyze the results
            personal_sites = len([s for s in all_sites if '/personal/' in s.get('webUrl', '')])
            archived_sites = len([s for s in all_sites if 'archived' in s.get('displayName', '').lower()])
            test_sites = len([s for s in all_sites if any(pattern in s.get('displayName', '').lower()
                                                         for pattern in ['test', 'demo', 'old'])])

            result = {
                'strategy_name': strategy['name'],
                'query': strategy['query'],
                'description': strategy['description'],
                'success': True,
                'sites_found': len(all_sites),
                'elapsed_time': elapsed_time,
                'api_calls': api_calls,
                'sites_per_second': len(all_sites) / elapsed_time if elapsed_time > 0 else 0,
                'personal_sites_included': personal_sites,
                'archived_sites_included': archived_sites,
                'test_sites_included': test_sites,
                'filtering_effectiveness': {
                    'personal_sites_filtered': personal_sites == 0,
                    'archived_sites_filtered': archived_sites == 0,
                    'test_sites_filtered': test_sites == 0
                },
                'sample_sites': all_sites[:5],
                'errors': errors
            }

            results.append(result)

            logger.info(f"Strategy '{strategy['name']}': {len(all_sites)} sites found, "
                       f"{api_calls} API calls, {elapsed_time:.2f}s")

        except Exception as e:
            error_result = {
                'strategy_name': strategy['name'],
                'query': strategy['query'],
                'success': False,
                'error': str(e),
                'elapsed_time': time.time() - start_time
            }
            results.append(error_result)
            logger.error(f"Strategy '{strategy['name']}' failed: {e}")

    async def _test_odata_filter_combinations(self, graph_client):
        """Test OData filter combinations with sites endpoint."""
        logger.info("Testing OData filter combinations")

        # Define OData filter strategies
        odata_strategies = [
            {
                'name': 'Basic Display Name Filtering',
                'filter': "$filter=not(contains(tolower(displayName), 'archived'))",
                'description': 'Exclude sites with "archived" in display name'
            },
            {
                'name': 'Multiple Pattern Exclusion',
                'filter': "$filter=not(contains(tolower(displayName), 'archived')) and not(contains(tolower(displayName), 'test'))",
                'description': 'Exclude archived and test sites'
            },
            {
                'name': 'URL-based Filtering',
                'filter': "$filter=not(contains(tolower(webUrl), '/personal/'))",
                'description': 'Exclude personal sites by URL pattern'
            },
            {
                'name': 'Time-based Activity Filter',
                'filter': "$filter=lastModifiedDateTime gt 2023-01-01T00:00:00Z",
                'description': 'Include only recently modified sites'
            },
            {
                'name': 'Combined Pattern and Time',
                'filter': "$filter=not(contains(tolower(displayName), 'archived')) and lastModifiedDateTime gt 2023-01-01T00:00:00Z",
                'description': 'Combine pattern exclusion with activity filter'
            }
        ]

        odata_test_results = []

        for strategy in odata_strategies:
            await self._test_single_odata_strategy(graph_client, strategy, odata_test_results)

        self.results['filter_tests'].append({
            'method': 'OData Filtering',
            'strategies_tested': len(odata_strategies),
            'results': odata_test_results
        })

    async def _test_single_odata_strategy(self, graph_client, strategy: Dict[str, str], results: List[Dict]):
        """Test a single OData filtering strategy."""
        logger.info(f"Testing OData strategy: {strategy['name']}")

        start_time = time.time()
        all_sites = []
        api_calls = 0
        errors = []

        try:
            # Test with pagination
            page_size = 100  # Reasonable page size for OData
            next_url = f"https://graph.microsoft.com/v1.0/sites?$top={page_size}&{strategy['filter']}"

            while next_url:
                try:
                    result = await graph_client.get_with_retry(next_url)
                    api_calls += 1

                    page_sites = result.get('value', [])
                    all_sites.extend(page_sites)

                    # Get next page URL
                    next_url = result.get('@odata.nextLink')

                    # For non-comprehensive testing, limit results
                    if not self.comprehensive and len(all_sites) >= 500:
                        break

                    # Rate limiting
                    await asyncio.sleep(0.1)

                except Exception as page_error:
                    errors.append(f"Page {api_calls}: {str(page_error)}")
                    break

            elapsed_time = time.time() - start_time

            # Analyze results
            personal_sites = len([s for s in all_sites if '/personal/' in s.get('webUrl', '')])
            archived_sites = len([s for s in all_sites if 'archived' in s.get('displayName', '').lower()])

            result = {
                'strategy_name': strategy['name'],
                'filter': strategy['filter'],
                'description': strategy['description'],
                'success': True,
                'sites_found': len(all_sites),
                'elapsed_time': elapsed_time,
                'api_calls': api_calls,
                'sites_per_second': len(all_sites) / elapsed_time if elapsed_time > 0 else 0,
                'personal_sites_included': personal_sites,
                'archived_sites_included': archived_sites,
                'sample_sites': all_sites[:5],
                'errors': errors
            }

            results.append(result)

            logger.info(f"OData strategy '{strategy['name']}': {len(all_sites)} sites found, "
                       f"{api_calls} API calls, {elapsed_time:.2f}s")

        except Exception as e:
            error_result = {
                'strategy_name': strategy['name'],
                'filter': strategy['filter'],
                'success': False,
                'error': str(e),
                'elapsed_time': time.time() - start_time
            }
            results.append(error_result)
            logger.error(f"OData strategy '{strategy['name']}' failed: {e}")

    async def _test_pagination_handling(self, graph_client):
        """Test pagination handling across different methods."""
        logger.info("Testing pagination handling")

        pagination_tests = []

        # Test Delta API pagination
        await self._test_delta_pagination(graph_client, pagination_tests)

        # Test Search API pagination
        await self._test_search_pagination(graph_client, pagination_tests)

        # Test Sites endpoint pagination
        await self._test_sites_pagination(graph_client, pagination_tests)

        self.results['pagination_tests'] = pagination_tests

    async def _test_delta_pagination(self, graph_client, results: List[Dict]):
        """Test Delta API pagination."""
        try:
            start_time = time.time()
            pages = 0
            total_sites = 0

            url = "https://graph.microsoft.com/v1.0/sites/delta?$top=50"

            while url and pages < 5:  # Limit to 5 pages for testing
                result = await graph_client.get_with_retry(url)
                pages += 1
                page_sites = len(result.get('value', []))
                total_sites += page_sites

                url = result.get('@odata.nextLink')
                if not url and '@odata.deltaLink' in result:
                    break

            elapsed_time = time.time() - start_time

            results.append({
                'method': 'Delta API',
                'success': True,
                'pages_fetched': pages,
                'total_sites': total_sites,
                'elapsed_time': elapsed_time,
                'avg_time_per_page': elapsed_time / pages if pages > 0 else 0
            })

        except Exception as e:
            results.append({
                'method': 'Delta API',
                'success': False,
                'error': str(e)
            })

    async def _test_search_pagination(self, graph_client, results: List[Dict]):
        """Test Search API pagination."""
        try:
            start_time = time.time()
            pages = 0
            total_sites = 0

            page_size = 500
            from_index = 0

            while pages < 3:  # Limit to 3 pages for testing
                search_url = "https://graph.microsoft.com/v1.0/search/query"
                search_body = {
                    "requests": [{
                        "entityTypes": ["site"],
                        "query": {"queryString": "*"},
                        "from": from_index,
                        "size": page_size
                    }]
                }

                result = await graph_client.post_with_retry(search_url, json=search_body)
                pages += 1

                page_sites = 0
                has_more = False

                if result and 'value' in result:
                    for response in result['value']:
                        for container in response.get('hitsContainers', []):
                            page_sites += len(container.get('hits', []))
                            has_more = container.get('moreResultsAvailable', False)

                total_sites += page_sites

                if not has_more or page_sites == 0:
                    break

                from_index += page_size

            elapsed_time = time.time() - start_time

            results.append({
                'method': 'Search API',
                'success': True,
                'pages_fetched': pages,
                'total_sites': total_sites,
                'elapsed_time': elapsed_time,
                'avg_time_per_page': elapsed_time / pages if pages > 0 else 0
            })

        except Exception as e:
            results.append({
                'method': 'Search API',
                'success': False,
                'error': str(e)
            })

    async def _test_sites_pagination(self, graph_client, results: List[Dict]):
        """Test Sites endpoint pagination."""
        try:
            start_time = time.time()
            pages = 0
            total_sites = 0

            url = "https://graph.microsoft.com/v1.0/sites?$top=50"

            while url and pages < 5:  # Limit to 5 pages for testing
                result = await graph_client.get_with_retry(url)
                pages += 1
                page_sites = len(result.get('value', []))
                total_sites += page_sites

                url = result.get('@odata.nextLink')

            elapsed_time = time.time() - start_time

            results.append({
                'method': 'Sites Endpoint',
                'success': True,
                'pages_fetched': pages,
                'total_sites': total_sites,
                'elapsed_time': elapsed_time,
                'avg_time_per_page': elapsed_time / pages if pages > 0 else 0
            })

        except Exception as e:
            results.append({
                'method': 'Sites Endpoint',
                'success': False,
                'error': str(e)
            })

    async def _run_performance_benchmarks(self, graph_client):
        """Run performance benchmarks if benchmark mode is enabled."""
        logger.info("Running performance benchmarks")

        benchmarks = []

        # Benchmark current implementation
        times = []
        for i in range(3):  # Run 3 times for average
            start_time = time.time()
            result = await graph_client.get_all_sites_delta(delta_token=None, active_only=True)
            elapsed = time.time() - start_time
            times.append(elapsed)
            await asyncio.sleep(1)  # Cool down between tests

        benchmarks.append({
            'method': 'Current Implementation (active_only=True)',
            'runs': 3,
            'times': times,
            'avg_time': statistics.mean(times),
            'min_time': min(times),
            'max_time': max(times),
            'std_dev': statistics.stdev(times) if len(times) > 1 else 0
        })

        # Benchmark best search query (if any successful ones found)
        search_tests = []
        for test_group in self.results['filter_tests']:
            if test_group.get('method') == 'Enhanced Search API':
                search_tests.extend([r for r in test_group['results'] if r.get('success', False)])

        if search_tests:
            # Find the fastest successful search strategy
            fastest_search = min(search_tests, key=lambda x: x.get('elapsed_time', float('inf')))

            # Benchmark it
            times = []
            for i in range(3):
                start_time = time.time()
                # Simulate the search query
                search_url = "https://graph.microsoft.com/v1.0/search/query"
                search_body = {
                    "requests": [{
                        "entityTypes": ["site"],
                        "query": {"queryString": fastest_search['query']},
                        "from": 0,
                        "size": 100
                    }]
                }
                await graph_client.post_with_retry(search_url, json=search_body)
                elapsed = time.time() - start_time
                times.append(elapsed)
                await asyncio.sleep(1)

            benchmarks.append({
                'method': f"Best Search Strategy: {fastest_search['strategy_name']}",
                'runs': 3,
                'times': times,
                'avg_time': statistics.mean(times),
                'min_time': min(times),
                'max_time': max(times),
                'std_dev': statistics.stdev(times) if len(times) > 1 else 0
            })

        self.results['performance_benchmarks'] = benchmarks

    async def _analyze_filtering_effectiveness(self, graph_client):
        """Analyze the effectiveness of different filtering approaches."""
        logger.info("Analyzing filtering effectiveness")

        # Compare all successful filtering strategies
        baseline_active_count = self.results['baseline_results']['current_active_filtering']['count']
        baseline_total_count = self.results['baseline_results']['all_sites']['count']

        effectiveness_analysis = {
            'baseline_filtering_rate': (baseline_total_count - baseline_active_count) / baseline_total_count * 100 if baseline_total_count > 0 else 0,
            'strategy_comparison': []
        }

        # Analyze search strategies
        for test_group in self.results['filter_tests']:
            if test_group.get('method') == 'Enhanced Search API':
                for result in test_group['results']:
                    if result.get('success', False):
                        personal_filtered = result['personal_sites_included'] == 0
                        archived_filtered = result['archived_sites_included'] == 0
                        test_filtered = result['test_sites_included'] == 0

                        effectiveness_score = sum([personal_filtered, archived_filtered, test_filtered]) / 3 * 100

                        effectiveness_analysis['strategy_comparison'].append({
                            'method': 'Search API',
                            'strategy': result['strategy_name'],
                            'sites_returned': result['sites_found'],
                            'effectiveness_score': effectiveness_score,
                            'personal_sites_filtered': personal_filtered,
                            'archived_sites_filtered': archived_filtered,
                            'test_sites_filtered': test_filtered,
                            'performance_score': result['sites_per_second']
                        })

        # Analyze OData strategies
        for test_group in self.results['filter_tests']:
            if test_group.get('method') == 'OData Filtering':
                for result in test_group['results']:
                    if result.get('success', False):
                        personal_filtered = result['personal_sites_included'] == 0
                        archived_filtered = result['archived_sites_included'] == 0

                        effectiveness_score = sum([personal_filtered, archived_filtered]) / 2 * 100

                        effectiveness_analysis['strategy_comparison'].append({
                            'method': 'OData Filtering',
                            'strategy': result['strategy_name'],
                            'sites_returned': result['sites_found'],
                            'effectiveness_score': effectiveness_score,
                            'personal_sites_filtered': personal_filtered,
                            'archived_sites_filtered': archived_filtered,
                            'performance_score': result['sites_per_second']
                        })

        # Find best strategies
        if effectiveness_analysis['strategy_comparison']:
            best_effectiveness = max(effectiveness_analysis['strategy_comparison'],
                                   key=lambda x: x['effectiveness_score'])
            best_performance = max(effectiveness_analysis['strategy_comparison'],
                                 key=lambda x: x['performance_score'])

            effectiveness_analysis['recommendations'] = {
                'best_effectiveness': best_effectiveness,
                'best_performance': best_performance
            }

        self.results['effectiveness_analysis'] = effectiveness_analysis

    async def _generate_comparison_report(self):
        """Generate comprehensive comparison report."""
        logger.info("Generating comparison report")

        # Generate recommendations based on all test results
        recommendations = []

        # Performance recommendations
        baseline_time = self.results['baseline_results']['current_active_filtering']['elapsed_time']

        if 'performance_benchmarks' in self.results:
            current_avg = None
            for benchmark in self.results['performance_benchmarks']:
                if 'Current Implementation' in benchmark['method']:
                    current_avg = benchmark['avg_time']
                    break

            if current_avg:
                recommendations.append(f"Current implementation average response time: {current_avg:.2f}s")

        # Effectiveness recommendations
        effectiveness = self.results.get('effectiveness_analysis', {})
        if 'recommendations' in effectiveness:
            best_eff = effectiveness['recommendations'].get('best_effectiveness')
            best_perf = effectiveness['recommendations'].get('best_performance')

            if best_eff:
                recommendations.append(f"Most effective filtering: {best_eff['method']} - {best_eff['strategy']} "
                                     f"(effectiveness: {best_eff['effectiveness_score']:.1f}%)")

            if best_perf:
                recommendations.append(f"Best performance: {best_perf['method']} - {best_perf['strategy']} "
                                     f"({best_perf['performance_score']:.1f} sites/second)")

        # API method recommendations
        search_successful = any(r.get('success', False) for test_group in self.results['filter_tests']
                               if test_group.get('method') == 'Enhanced Search API'
                               for r in test_group.get('results', []))

        odata_successful = any(r.get('success', False) for test_group in self.results['filter_tests']
                              if test_group.get('method') == 'OData Filtering'
                              for r in test_group.get('results', []))

        if search_successful and odata_successful:
            recommendations.append("Both Search API and OData filtering are viable - recommend hybrid approach")
        elif search_successful:
            recommendations.append("Search API filtering is most reliable - recommend as primary method")
        elif odata_successful:
            recommendations.append("OData filtering is available - recommend for server-side filtering")
        else:
            recommendations.append("Consider improving client-side filtering with better heuristics")

        # Pagination recommendations
        if 'pagination_tests' in self.results:
            fastest_pagination = min(self.results['pagination_tests'],
                                   key=lambda x: x.get('avg_time_per_page', float('inf'))
                                   if x.get('success', False) else float('inf'))

            if fastest_pagination.get('success', False):
                recommendations.append(f"Most efficient pagination: {fastest_pagination['method']} "
                                     f"({fastest_pagination['avg_time_per_page']:.2f}s per page)")

        self.results['recommendations'] = recommendations

        # Save comprehensive report
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = f"filtering_methods_comparison_{timestamp}.json"

        with open(report_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)

        # Create summary report
        summary_file = f"filtering_summary_{timestamp}.txt"
        self._create_summary_report(summary_file)

        logger.info(f"Comprehensive report saved to: {report_file}")
        logger.info(f"Summary report saved to: {summary_file}")

        # Print summary to console
        self._print_console_summary()

    def _create_summary_report(self, filename: str):
        """Create a human-readable summary report."""
        with open(filename, 'w') as f:
            f.write("SharePoint Site Filtering Methods Comparison Report\n")
            f.write("=" * 60 + "\n\n")

            f.write(f"Test Date: {self.results['timestamp']}\n")
            f.write(f"Test Configuration: {'Comprehensive' if self.comprehensive else 'Standard'}\n")
            f.write(f"Benchmark Mode: {'Enabled' if self.benchmark else 'Disabled'}\n\n")

            # Baseline results
            baseline = self.results['baseline_results']
            f.write("BASELINE RESULTS:\n")
            f.write(f"  Total Sites: {baseline['all_sites']['count']}\n")
            f.write(f"  Current Active Filter: {baseline['current_active_filtering']['count']} sites\n")
            f.write(f"  Filter Effectiveness: {baseline['current_active_filtering']['filter_effectiveness']:.1f}%\n")
            f.write(f"  Current Response Time: {baseline['current_active_filtering']['elapsed_time']:.2f}s\n\n")

            # Test results summary
            f.write("FILTERING METHODS TESTED:\n")
            for test_group in self.results['filter_tests']:
                successful = len([r for r in test_group['results'] if r.get('success', False)])
                total = len(test_group['results'])
                f.write(f"  {test_group['method']}: {successful}/{total} strategies successful\n")
            f.write("\n")

            # Effectiveness analysis
            if 'effectiveness_analysis' in self.results:
                eff = self.results['effectiveness_analysis']
                f.write("EFFECTIVENESS ANALYSIS:\n")
                if 'recommendations' in eff:
                    best_eff = eff['recommendations'].get('best_effectiveness')
                    best_perf = eff['recommendations'].get('best_performance')

                    if best_eff:
                        f.write(f"  Best Effectiveness: {best_eff['strategy']} ({best_eff['effectiveness_score']:.1f}%)\n")
                    if best_perf:
                        f.write(f"  Best Performance: {best_perf['strategy']} ({best_perf['performance_score']:.1f} sites/sec)\n")
                f.write("\n")

            # Recommendations
            f.write("RECOMMENDATIONS:\n")
            for i, rec in enumerate(self.results['recommendations'], 1):
                f.write(f"  {i}. {rec}\n")

    def _print_console_summary(self):
        """Print summary to console."""
        print("\n" + "="*70)
        print("SHAREPOINT FILTERING METHODS COMPARISON SUMMARY")
        print("="*70)

        baseline = self.results['baseline_results']
        print(f"Baseline: {baseline['all_sites']['count']} total sites, "
              f"{baseline['current_active_filtering']['count']} after current filtering")
        print(f"Current filter effectiveness: {baseline['current_active_filtering']['filter_effectiveness']:.1f}%")

        # Count successful strategies
        total_strategies = 0
        successful_strategies = 0

        for test_group in self.results['filter_tests']:
            for result in test_group['results']:
                total_strategies += 1
                if result.get('success', False):
                    successful_strategies += 1

        print(f"Filtering strategies tested: {successful_strategies}/{total_strategies} successful")

        # Best recommendations
        if self.results['recommendations']:
            print("\nTop Recommendations:")
            for i, rec in enumerate(self.results['recommendations'][:3], 1):
                print(f"  {i}. {rec}")

        print("="*70)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Test SharePoint site filtering methods")
    parser.add_argument("--config", default="config/config.json", help="Configuration file path")
    parser.add_argument("--comprehensive", action="store_true", help="Run comprehensive tests (more queries, longer execution)")
    parser.add_argument("--benchmark", action="store_true", help="Run performance benchmarks")

    args = parser.parse_args()

    framework = FilterTestingFramework(args.config, args.comprehensive, args.benchmark)
    await framework.run_filter_testing()


if __name__ == "__main__":
    asyncio.run(main())
