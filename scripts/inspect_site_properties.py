#!/usr/bin/env python3
"""
Site Property Inspector for --active-only Flag Debug

This script fetches sample sites using different API methods and displays all
available properties to identify which properties indicate archived/deleted/
personal status. Exports findings to JSON/CSV for analysis.

Usage:
    python3 scripts/inspect_site_properties.py [--config config/config.json] [--samples 20] [--export-format json]
"""

import asyncio
import json
import csv
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Set
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


class SitePropertyInspector:
    """Inspect SharePoint site properties to understand status indicators."""

    def __init__(self, config_path: str, samples: int = 20, export_format: str = "json"):
        self.config_path = config_path
        self.samples = samples
        self.export_format = export_format.lower()
        self.findings = {
            'timestamp': datetime.now().isoformat(),
            'api_methods_tested': [],
            'site_samples': [],
            'property_analysis': {},
            'filtering_opportunities': [],
            'recommendations': []
        }

    async def run_inspection(self):
        """Run the complete site property inspection."""
        logger.info("Starting SharePoint site property inspection")

        # Load configuration and initialize clients
        config = load_config(self.config_path)
        auth_manager = AuthenticationManager(config.auth)
        rate_limiter = RateLimiter()
        retry_strategy = RetryStrategy(RetryConfig())

        graph_client = GraphAPIClient(auth_manager, retry_strategy, rate_limiter)

        try:
            logger.info("=== PHASE 1: Fetch Sites via Different API Methods ===")
            await self._fetch_sites_via_different_methods(graph_client)

            logger.info("=== PHASE 2: Analyze Property Patterns ===")
            self._analyze_property_patterns()

            logger.info("=== PHASE 3: Test Various OData Filter Combinations ===")
            await self._test_odata_filters(graph_client)

            logger.info("=== PHASE 4: Identify Filtering Opportunities ===")
            self._identify_filtering_opportunities()

            logger.info("=== PHASE 5: Export Findings ===")
            await self._export_findings()

        finally:
            await graph_client.close()

    async def _fetch_sites_via_different_methods(self, graph_client):
        """Fetch sites using different API methods to compare available properties."""

        # Method 1: Delta API
        await self._fetch_via_delta_api(graph_client)

        # Method 2: Search API
        await self._fetch_via_search_api(graph_client)

        # Method 3: Sites endpoint
        await self._fetch_via_sites_endpoint(graph_client)

        # Method 4: Detailed individual site queries
        await self._fetch_detailed_site_properties(graph_client)

    async def _fetch_via_delta_api(self, graph_client):
        """Fetch sites using Delta API."""
        logger.info("Fetching sites via Delta API")

        start_time = time.time()

        try:
            # Get all available properties via delta
            url = f"https://graph.microsoft.com/v1.0/sites/delta?$top={min(self.samples, 25)}&$select=*"
            result = await graph_client.get_with_retry(url)

            sites = result.get('value', [])
            elapsed = time.time() - start_time

            method_info = {
                'method': 'Delta API',
                'url_template': 'https://graph.microsoft.com/v1.0/sites/delta',
                'sites_fetched': len(sites),
                'elapsed_time': elapsed,
                'supports_odata_filters': 'Limited',
                'sample_properties': list(sites[0].keys()) if sites else [],
                'sites': sites
            }

            self.findings['api_methods_tested'].append(method_info)
            self.findings['site_samples'].extend([
                {**site, '_api_method': 'Delta API'} for site in sites
            ])

            logger.info(f"Delta API: {len(sites)} sites, {len(method_info['sample_properties'])} properties")

        except Exception as e:
            logger.error(f"Delta API fetch failed: {e}")
            self.findings['api_methods_tested'].append({
                'method': 'Delta API',
                'error': str(e)
            })

    async def _fetch_via_search_api(self, graph_client):
        """Fetch sites using Search API."""
        logger.info("Fetching sites via Search API")

        start_time = time.time()

        try:
            search_url = "https://graph.microsoft.com/v1.0/search/query"
            search_body = {
                "requests": [{
                    "entityTypes": ["site"],
                    "query": {
                        "queryString": "*"  # Get all sites
                    },
                    "from": 0,
                    "size": min(self.samples, 50),
                    "fields": ["*"]  # Request all available fields
                }]
            }

            result = await graph_client.post_with_retry(search_url, json=search_body)

            sites = []
            if result and 'value' in result:
                for response in result['value']:
                    for container in response.get('hitsContainers', []):
                        for hit in container.get('hits', []):
                            sites.append(hit.get('resource', {}))

            elapsed = time.time() - start_time

            method_info = {
                'method': 'Search API',
                'url_template': 'https://graph.microsoft.com/v1.0/search/query',
                'sites_fetched': len(sites),
                'elapsed_time': elapsed,
                'supports_odata_filters': 'Query String Only',
                'sample_properties': list(sites[0].keys()) if sites else [],
                'sites': sites
            }

            self.findings['api_methods_tested'].append(method_info)
            self.findings['site_samples'].extend([
                {**site, '_api_method': 'Search API'} for site in sites
            ])

            logger.info(f"Search API: {len(sites)} sites, {len(method_info['sample_properties'])} properties")

        except Exception as e:
            logger.error(f"Search API fetch failed: {e}")
            self.findings['api_methods_tested'].append({
                'method': 'Search API',
                'error': str(e)
            })

    async def _fetch_via_sites_endpoint(self, graph_client):
        """Fetch sites using direct sites endpoint."""
        logger.info("Fetching sites via Sites endpoint")

        start_time = time.time()

        try:
            # Try with all properties
            url = f"https://graph.microsoft.com/v1.0/sites?$top={min(self.samples, 25)}&$select=*"
            result = await graph_client.get_with_retry(url)

            sites = result.get('value', [])
            elapsed = time.time() - start_time

            method_info = {
                'method': 'Sites Endpoint',
                'url_template': 'https://graph.microsoft.com/v1.0/sites',
                'sites_fetched': len(sites),
                'elapsed_time': elapsed,
                'supports_odata_filters': 'Full OData Support',
                'sample_properties': list(sites[0].keys()) if sites else [],
                'sites': sites
            }

            self.findings['api_methods_tested'].append(method_info)
            self.findings['site_samples'].extend([
                {**site, '_api_method': 'Sites Endpoint'} for site in sites
            ])

            logger.info(f"Sites Endpoint: {len(sites)} sites, {len(method_info['sample_properties'])} properties")

        except Exception as e:
            logger.error(f"Sites endpoint fetch failed: {e}")
            self.findings['api_methods_tested'].append({
                'method': 'Sites Endpoint',
                'error': str(e)
            })

    async def _fetch_detailed_site_properties(self, graph_client):
        """Fetch detailed properties for individual sites."""
        logger.info("Fetching detailed individual site properties")

        # Get a few site IDs from previously fetched sites
        site_ids = []
        for sample in self.findings['site_samples'][:5]:  # Limit to 5 for detailed analysis
            site_id = sample.get('id')
            if site_id and site_id not in site_ids:
                site_ids.append(site_id)

        detailed_sites = []

        for site_id in site_ids:
            try:
                start_time = time.time()

                # Get full site details
                url = f"https://graph.microsoft.com/v1.0/sites/{site_id}?$select=*"
                site_detail = await graph_client.get_with_retry(url)

                # Also try to get root site properties that might indicate status
                root_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/root?$select=*"
                try:
                    root_detail = await graph_client.get_with_retry(root_url)
                    site_detail['_root_properties'] = root_detail
                except:
                    pass

                # Try to get site collection properties
                try:
                    collection_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}?$expand=*"
                    collection_detail = await graph_client.get_with_retry(collection_url)
                    site_detail['_collection_properties'] = collection_detail
                except:
                    pass

                elapsed = time.time() - start_time
                site_detail['_fetch_time'] = elapsed
                site_detail['_api_method'] = 'Detailed Individual'

                detailed_sites.append(site_detail)

                logger.info(f"Detailed site {site_id}: {len(site_detail.keys())} properties")

            except Exception as e:
                logger.warning(f"Failed to get detailed properties for site {site_id}: {e}")

        if detailed_sites:
            method_info = {
                'method': 'Detailed Individual Queries',
                'url_template': 'https://graph.microsoft.com/v1.0/sites/{id}',
                'sites_fetched': len(detailed_sites),
                'average_properties': sum(len(s.keys()) for s in detailed_sites) / len(detailed_sites),
                'supports_odata_filters': 'Full OData Support',
                'sites': detailed_sites
            }

            self.findings['api_methods_tested'].append(method_info)
            self.findings['site_samples'].extend(detailed_sites)

    def _analyze_property_patterns(self):
        """Analyze patterns in site properties across different API methods."""
        logger.info("Analyzing property patterns")

        # Collect all unique properties across all sites and methods
        all_properties: Set[str] = set()
        property_frequency: Dict[str, int] = {}
        api_method_properties: Dict[str, Set[str]] = {}

        for site in self.findings['site_samples']:
            api_method = site.get('_api_method', 'Unknown')

            if api_method not in api_method_properties:
                api_method_properties[api_method] = set()

            for prop in site.keys():
                if not prop.startswith('_'):  # Skip our internal metadata
                    all_properties.add(prop)
                    property_frequency[prop] = property_frequency.get(prop, 0) + 1
                    api_method_properties[api_method].add(prop)

        # Identify potentially useful properties for filtering
        status_related_properties = []
        time_related_properties = []
        metadata_properties = []

        for prop in all_properties:
            prop_lower = prop.lower()

            # Status-related properties
            if any(keyword in prop_lower for keyword in [
                'status', 'state', 'active', 'archive', 'delete', 'disable',
                'lock', 'block', 'suspend', 'hidden', 'visible'
            ]):
                status_related_properties.append(prop)

            # Time-related properties (useful for activity heuristics)
            elif any(keyword in prop_lower for keyword in [
                'modified', 'created', 'last', 'updated', 'access', 'activity'
            ]):
                time_related_properties.append(prop)

            # Metadata properties
            elif any(keyword in prop_lower for keyword in [
                'type', 'template', 'category', 'classification', 'owner'
            ]):
                metadata_properties.append(prop)

        # Analyze sample values for status-related properties
        property_value_analysis = {}
        for prop in status_related_properties + time_related_properties:
            values = []
            for site in self.findings['site_samples']:
                if prop in site and site[prop] is not None:
                    values.append(site[prop])

            if values:
                property_value_analysis[prop] = {
                    'sample_values': values[:10],  # First 10 values
                    'unique_values': len(set(str(v) for v in values)),
                    'null_count': sum(1 for site in self.findings['site_samples'] if site.get(prop) is None)
                }

        self.findings['property_analysis'] = {
            'total_unique_properties': len(all_properties),
            'property_frequency': dict(sorted(property_frequency.items(), key=lambda x: x[1], reverse=True)),
            'api_method_properties': {k: list(v) for k, v in api_method_properties.items()},
            'status_related_properties': status_related_properties,
            'time_related_properties': time_related_properties,
            'metadata_properties': metadata_properties,
            'property_value_analysis': property_value_analysis
        }

        logger.info(f"Found {len(all_properties)} unique properties")
        logger.info(f"Status-related: {len(status_related_properties)}")
        logger.info(f"Time-related: {len(time_related_properties)}")

    async def _test_odata_filters(self, graph_client):
        """Test various OData filter combinations."""
        logger.info("Testing OData filter combinations")

        # Based on property analysis, test potential filters
        filter_tests = []

        # Standard filters to test
        standard_filters = [
            "$filter=webUrl ne null",
            "$filter=displayName ne null",
            "$filter=createdDateTime gt 2020-01-01T00:00:00Z",
            "$filter=lastModifiedDateTime gt 2023-01-01T00:00:00Z",
            "$filter=not(contains(tolower(displayName), 'archived'))",
            "$filter=not(contains(tolower(displayName), 'test'))",
            "$filter=not(contains(tolower(displayName), 'old'))",
            "$filter=not(contains(tolower(webUrl), '/personal/'))",
            "$filter=not(contains(tolower(webUrl), '-my.sharepoint.com'))"
        ]

        # Test each filter
        for filter_expr in standard_filters:
            await self._test_single_odata_filter(graph_client, filter_expr, filter_tests)

        # Test combined filters
        combined_filters = [
            "$filter=not(contains(tolower(displayName), 'archived')) and not(contains(tolower(displayName), 'test'))",
            "$filter=not(contains(tolower(webUrl), '/personal/')) and lastModifiedDateTime gt 2023-01-01T00:00:00Z",
            "$filter=displayName ne null and not(contains(tolower(displayName), 'archived')) and not(contains(tolower(displayName), 'test'))"
        ]

        for filter_expr in combined_filters:
            await self._test_single_odata_filter(graph_client, filter_expr, filter_tests)

        self.findings['odata_filter_tests'] = filter_tests

    async def _test_single_odata_filter(self, graph_client, filter_expr: str, filter_tests: List[Dict]):
        """Test a single OData filter."""
        try:
            start_time = time.time()

            # Test on sites endpoint
            url = f"https://graph.microsoft.com/v1.0/sites?$top=10&{filter_expr}"
            result = await graph_client.get_with_retry(url)

            elapsed = time.time() - start_time
            sites_returned = len(result.get('value', []))

            filter_test = {
                'filter_expression': filter_expr,
                'success': True,
                'sites_returned': sites_returned,
                'elapsed_time': elapsed,
                'sample_sites': result.get('value', [])[:3]  # First 3 for analysis
            }

            filter_tests.append(filter_test)
            logger.info(f"Filter test successful: {filter_expr} -> {sites_returned} sites")

        except Exception as e:
            filter_test = {
                'filter_expression': filter_expr,
                'success': False,
                'error': str(e)
            }
            filter_tests.append(filter_test)
            logger.warning(f"Filter test failed: {filter_expr} -> {e}")

    def _identify_filtering_opportunities(self):
        """Identify specific filtering opportunities based on analysis."""
        opportunities = []

        # Analyze property patterns
        property_analysis = self.findings.get('property_analysis', {})

        # Status-related opportunities
        status_props = property_analysis.get('status_related_properties', [])
        if status_props:
            opportunities.append({
                'type': 'Status Properties',
                'description': f"Found {len(status_props)} status-related properties that could indicate site state",
                'properties': status_props,
                'implementation': 'Use OData filters on these properties to exclude inactive sites'
            })

        # Time-based opportunities
        time_props = property_analysis.get('time_related_properties', [])
        if time_props:
            opportunities.append({
                'type': 'Time-based Filtering',
                'description': f"Found {len(time_props)} time-related properties for activity heuristics",
                'properties': time_props,
                'implementation': 'Filter sites based on lastModifiedDateTime or similar activity indicators'
            })

        # URL pattern opportunities
        sample_urls = [site.get('webUrl', '') for site in self.findings['site_samples'] if site.get('webUrl')]
        personal_sites = [url for url in sample_urls if '/personal/' in url.lower()]

        if personal_sites:
            opportunities.append({
                'type': 'URL Pattern Filtering',
                'description': f"Found {len(personal_sites)} personal sites that should be excluded",
                'sample_patterns': list(set(personal_sites))[:5],
                'implementation': 'Use OData contains() function to exclude URLs with /personal/ pattern'
            })

        # Name pattern opportunities
        archived_sites = []
        test_sites = []
        for site in self.findings['site_samples']:
            display_name = site.get('displayName', '').lower()
            if 'archived' in display_name:
                archived_sites.append(site.get('displayName'))
            elif any(pattern in display_name for pattern in ['test', 'demo', 'old']):
                test_sites.append(site.get('displayName'))

        if archived_sites or test_sites:
            opportunities.append({
                'type': 'Name Pattern Filtering',
                'description': f"Found {len(archived_sites)} archived and {len(test_sites)} test/demo sites",
                'archived_examples': archived_sites[:5],
                'test_examples': test_sites[:5],
                'implementation': 'Use OData contains() function with displayName to exclude pattern matches'
            })

        # OData filter success analysis
        filter_tests = self.findings.get('odata_filter_tests', [])
        successful_filters = [test for test in filter_tests if test.get('success', False)]

        if successful_filters:
            opportunities.append({
                'type': 'Proven OData Filters',
                'description': f"Tested {len(successful_filters)} successful OData filters",
                'successful_filters': [test['filter_expression'] for test in successful_filters],
                'implementation': 'Implement these tested filters directly in the API queries'
            })

        self.findings['filtering_opportunities'] = opportunities

        # Generate recommendations
        recommendations = []

        if successful_filters:
            recommendations.append("Replace client-side filtering with server-side OData filters")
            recommendations.append("Use proven filter combinations to reduce API response size")

        if status_props:
            recommendations.append("Investigate status properties for definitive active/inactive indicators")

        if '/personal/' in str(sample_urls):
            recommendations.append("Implement URL-based filtering to exclude personal sites at API level")

        recommendations.append("Consider combining Search API queries with OData filtering for optimal performance")

        self.findings['recommendations'] = recommendations

    async def _export_findings(self):
        """Export findings to specified format."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if self.export_format == 'json':
            output_file = f"site_property_inspection_{timestamp}.json"
            with open(output_file, 'w') as f:
                json.dump(self.findings, f, indent=2, default=str)
            logger.info(f"Findings exported to JSON: {output_file}")

        elif self.export_format == 'csv':
            # Export site samples to CSV
            output_file = f"site_samples_{timestamp}.csv"

            if self.findings['site_samples']:
                # Get all unique property names
                all_properties = set()
                for site in self.findings['site_samples']:
                    all_properties.update(site.keys())

                with open(output_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=sorted(all_properties))
                    writer.writeheader()

                    for site in self.findings['site_samples']:
                        # Convert all values to strings for CSV compatibility
                        row = {}
                        for prop in sorted(all_properties):
                            value = site.get(prop, '')
                            if isinstance(value, (dict, list)):
                                row[prop] = json.dumps(value)
                            else:
                                row[prop] = str(value) if value is not None else ''
                        writer.writerow(row)

                # Also export property analysis summary
                summary_file = f"property_analysis_{timestamp}.json"
                with open(summary_file, 'w') as f:
                    json.dump({
                        'property_analysis': self.findings['property_analysis'],
                        'filtering_opportunities': self.findings['filtering_opportunities'],
                        'recommendations': self.findings['recommendations']
                    }, f, indent=2, default=str)

                logger.info(f"Site samples exported to CSV: {output_file}")
                logger.info(f"Analysis summary exported to JSON: {summary_file}")

        # Always create a summary report
        self._create_summary_report(timestamp)

    def _create_summary_report(self, timestamp: str):
        """Create a human-readable summary report."""
        report_file = f"site_property_summary_{timestamp}.txt"

        with open(report_file, 'w') as f:
            f.write("SharePoint Site Property Inspection Summary\n")
            f.write("=" * 50 + "\n\n")

            f.write(f"Timestamp: {self.findings['timestamp']}\n")
            f.write(f"Samples Analyzed: {len(self.findings['site_samples'])}\n\n")

            # API Methods Summary
            f.write("API Methods Tested:\n")
            for method in self.findings['api_methods_tested']:
                if 'error' in method:
                    f.write(f"  - {method['method']}: FAILED - {method['error']}\n")
                else:
                    f.write(f"  - {method['method']}: {method['sites_fetched']} sites, "
                           f"{len(method.get('sample_properties', []))} properties\n")
            f.write("\n")

            # Property Analysis Summary
            prop_analysis = self.findings.get('property_analysis', {})
            f.write(f"Properties Found: {prop_analysis.get('total_unique_properties', 0)}\n")
            f.write(f"  - Status-related: {len(prop_analysis.get('status_related_properties', []))}\n")
            f.write(f"  - Time-related: {len(prop_analysis.get('time_related_properties', []))}\n")
            f.write(f"  - Metadata: {len(prop_analysis.get('metadata_properties', []))}\n\n")

            # Filtering Opportunities
            f.write("Filtering Opportunities:\n")
            for i, opp in enumerate(self.findings.get('filtering_opportunities', []), 1):
                f.write(f"  {i}. {opp['type']}: {opp['description']}\n")
            f.write("\n")

            # Recommendations
            f.write("Recommendations:\n")
            for i, rec in enumerate(self.findings.get('recommendations', []), 1):
                f.write(f"  {i}. {rec}\n")

            # OData Filter Test Summary
            filter_tests = self.findings.get('odata_filter_tests', [])
            if filter_tests:
                successful = len([t for t in filter_tests if t.get('success', False)])
                f.write(f"\nOData Filter Tests: {successful}/{len(filter_tests)} successful\n")

        logger.info(f"Summary report created: {report_file}")

        # Print summary to console
        print("\n" + "="*60)
        print("SITE PROPERTY INSPECTION SUMMARY")
        print("="*60)
        print(f"Samples analyzed: {len(self.findings['site_samples'])}")
        print(f"Properties found: {prop_analysis.get('total_unique_properties', 0)}")
        print(f"Filtering opportunities: {len(self.findings.get('filtering_opportunities', []))}")
        print(f"Recommendations: {len(self.findings.get('recommendations', []))}")
        print(f"Summary report: {report_file}")
        print("="*60)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Inspect SharePoint site properties for filtering opportunities")
    parser.add_argument("--config", default="config/config.json", help="Configuration file path")
    parser.add_argument("--samples", type=int, default=20, help="Number of site samples to analyze")
    parser.add_argument("--export-format", choices=['json', 'csv'], default='json', help="Export format")

    args = parser.parse_args()

    inspector = SitePropertyInspector(args.config, args.samples, args.export_format)
    await inspector.run_inspection()


if __name__ == "__main__":
    asyncio.run(main())
