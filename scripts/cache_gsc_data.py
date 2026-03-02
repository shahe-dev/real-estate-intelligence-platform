#!/usr/bin/env python3
"""
Cache GSC Site Data

Exports all site-wide GSC data to a JSON file for use by the content optimizer.
Run this periodically (e.g., weekly) to refresh the cached data.

Includes:
- Page-level data (clicks, impressions, CTR, position per URL)
- Query-level data (clicks, impressions, CTR, position per query)
- Query-to-Page mapping (which queries rank for which pages)

Usage:
    python scripts/cache_gsc_data.py
    python scripts/cache_gsc_data.py --gsc-property "https://your-site.com/"
    python scripts/cache_gsc_data.py --days 90
"""

import argparse
import os
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.gsc_client import GSCClient

# Default paths
GSC_CREDENTIALS_PATH = Path(os.getenv("GSC_CREDENTIALS_PATH", "credentials/gsc-service-account.json"))
CACHE_DIR = Path(os.getenv("GSC_CACHE_DIR", "data/generated_content/gsc_cache"))


def cache_site_data(
    gsc_property: str = os.getenv("GSC_SITE_URL", "https://your-site.com/"),
    days: int = 90,
    output_path: str = None
) -> Path:
    """
    Cache all site-wide GSC data to a JSON file.

    Includes page_queries: which queries each URL is ranking for.

    Returns path to the cache file.
    """
    print(f"Connecting to Google Search Console...")
    print(f"  Property: {gsc_property}")
    print(f"  Days: {days}")

    client = GSCClient(str(GSC_CREDENTIALS_PATH), gsc_property)

    # Calculate date range
    end_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=days + 3)).strftime('%Y-%m-%d')

    print(f"\nFetching site overview...")
    overview = client.get_site_overview(days=days)

    # Fetch page-to-query mapping (which queries each URL ranks for)
    print(f"Fetching page-to-query data...")
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['page', 'query'],
        'rowLimit': 5000
    }

    response = client.service.searchanalytics().query(
        siteUrl=gsc_property,
        body=request
    ).execute()

    # Build page_queries: { "url": [ {query, clicks, impressions, ctr, position}, ... ] }
    page_queries = {}
    for row in response.get('rows', []):
        page = row['keys'][0]
        query = row['keys'][1]

        if page not in page_queries:
            page_queries[page] = []

        page_queries[page].append({
            'query': query,
            'clicks': row['clicks'],
            'impressions': row['impressions'],
            'ctr': row['ctr'],
            'position': row['position']
        })

    print(f"  Found {len(response.get('rows', []))} page-query combinations")
    print(f"  URLs with query data: {len(page_queries)}")

    # Build cache structure
    cache_data = {
        "metadata": {
            "gsc_property": gsc_property,
            "cached_at": datetime.now().isoformat(),
            "date_range": overview.date_range,
            "days": days
        },
        "summary": {
            "total_clicks": overview.total_clicks,
            "total_impressions": overview.total_impressions,
            "avg_ctr": overview.avg_ctr,
            "avg_position": overview.avg_position
        },
        "pages": overview.top_pages,
        "queries": [
            {
                "query": q.query,
                "clicks": q.clicks,
                "impressions": q.impressions,
                "ctr": q.ctr,
                "position": q.position,
                "opportunity_score": q.opportunity_score
            }
            for q in overview.top_queries
        ],
        "page_queries": page_queries
    }

    # Determine output path
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Extract host for filename
    host = gsc_property.replace('https://', '').replace('http://', '').rstrip('/')

    if output_path:
        cache_file = Path(output_path)
    else:
        cache_file = CACHE_DIR / f"gsc_cache_{host}_{datetime.now().strftime('%Y%m%d')}.json"

    # Also save a "latest" version for easy access
    latest_file = CACHE_DIR / f"gsc_cache_{host}_latest.json"

    # Save cache
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)

    # Save as latest
    with open(latest_file, 'w', encoding='utf-8') as f:
        json.dump(cache_data, f, indent=2, ensure_ascii=False)

    print(f"\nCache saved:")
    print(f"  {cache_file}")
    print(f"  {latest_file} (latest)")

    # Print summary
    print(f"\nData Summary:")
    print(f"  Pages: {len(cache_data['pages'])}")
    print(f"  Queries: {len(cache_data['queries'])}")
    print(f"  Total Clicks: {cache_data['summary']['total_clicks']:,}")
    print(f"  Total Impressions: {cache_data['summary']['total_impressions']:,}")

    # Show top pages
    print(f"\nTop 10 Pages:")
    for p in cache_data['pages'][:10]:
        path = p['page'].replace(gsc_property.rstrip('/'), '')
        print(f"  {path} ({p['clicks']:,} clicks)")

    # Show top opportunity queries
    print(f"\nTop 10 Opportunity Queries:")
    sorted_queries = sorted(cache_data['queries'], key=lambda x: x['opportunity_score'], reverse=True)[:10]
    for q in sorted_queries:
        print(f"  {q['query']} (pos: {q['position']:.1f}, imp: {q['impressions']:,})")

    return cache_file


def main():
    parser = argparse.ArgumentParser(
        description="Cache GSC site data for content optimization"
    )

    parser.add_argument(
        "--gsc-property",
        type=str,
        default=os.getenv("GSC_SITE_URL", "https://your-site.com/"),
        help="GSC property URL (default: from GSC_SITE_URL env var)"
    )

    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Days of data to fetch (default: 90)"
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Custom output path for cache file"
    )

    parser.add_argument(
        "--list-sites",
        action="store_true",
        help="List available GSC properties"
    )

    args = parser.parse_args()

    if not GSC_CREDENTIALS_PATH.exists():
        print(f"Error: GSC credentials not found at {GSC_CREDENTIALS_PATH}")
        sys.exit(1)

    if args.list_sites:
        client = GSCClient(str(GSC_CREDENTIALS_PATH), "")
        print("Available GSC properties:")
        for site in client.list_sites():
            print(f"  - {site}")
        return

    try:
        cache_site_data(
            gsc_property=args.gsc_property,
            days=args.days,
            output_path=args.output
        )
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
