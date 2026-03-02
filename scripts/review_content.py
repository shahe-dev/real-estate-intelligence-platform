#!/usr/bin/env python3
"""
Content Review Workflow Script

Generates data-driven content reviews using GSC data.
Outputs comprehensive markdown reports with actionable recommendations.

Usage:
    python scripts/review_content.py --url "https://your-site.com/off-plan-projects-new-developments-dubai/"
    python scripts/review_content.py --url "https://your-site.com/page/" --content-file page_content.txt
    python scripts/review_content.py --site-overview
    python scripts/review_content.py --list-sites
"""

import argparse
import os
import sys
import re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.gsc_client import GSCClient, create_client
from src.analytics.content_reviewer import ContentReviewer, ContentReviewFormatter


# Default paths
GSC_CREDENTIALS_PATH = Path(os.getenv("GSC_CREDENTIALS_PATH", "credentials/gsc-service-account.json"))
OUTPUT_DIR = Path(os.getenv("CONTENT_OUTPUT_DIR", "data/generated_content/content-reviews"))

# Site URL to GSC property mapping
# Configure your GSC properties in environment or config file
# Example: GSC_PROPERTY_MAP = {"your-site.com": "https://your-site.com/"}
GSC_PROPERTY_MAP = {}


def parse_page_url(page_url: str) -> tuple:
    """
    Parse a full page URL into site URL and path.

    Args:
        page_url: Full URL like 'https://your-site.com/off-plan-projects/'

    Returns:
        Tuple of (gsc_property, page_path, site_domain)
    """
    parsed = urlparse(page_url)
    domain = parsed.netloc

    # Get GSC property for this domain
    gsc_property = GSC_PROPERTY_MAP.get(domain)
    if not gsc_property:
        # Try to find partial match
        for key, value in GSC_PROPERTY_MAP.items():
            if key in domain:
                gsc_property = value
                break

    if not gsc_property:
        raise ValueError(f"Unknown domain: {domain}. Available domains: {list(GSC_PROPERTY_MAP.keys())}")

    page_path = parsed.path or '/'

    return gsc_property, page_path, domain


def review_page(
    page_url: str,
    content: str = "",
    title: str = "",
    meta_description: str = "",
    days: int = 90
) -> tuple:
    """
    Generate a content review for a specific page.

    Args:
        page_url: Full URL (e.g., 'https://your-site.com/off-plan-projects/')
        content: Page content text (optional - provides better recommendations)
        title: Page title (optional)
        meta_description: Meta description (optional)
        days: Days of GSC data to analyze

    Returns:
        Tuple of (markdown_report, page_path_for_filename)
    """
    gsc_property, page_path, domain = parse_page_url(page_url)

    print(f"Connecting to Google Search Console...")
    print(f"  GSC Property: {gsc_property}")
    print(f"  Page Path: {page_path}")

    client = GSCClient(str(GSC_CREDENTIALS_PATH), gsc_property)

    print(f"Fetching performance data...")
    reviewer = ContentReviewer(client)

    review = reviewer.review_page(
        page_path=page_path,
        page_content=content,
        page_title=title or f"Page: {page_path}",
        meta_description=meta_description,
        days=days
    )

    return review.to_markdown(), page_path


def get_site_overview(
    site_url: str = "https://your-site.com",
    days: int = 90
) -> str:
    """Generate site-wide overview report."""
    print(f"Connecting to Google Search Console...")
    client = GSCClient(str(GSC_CREDENTIALS_PATH), site_url)

    print(f"Fetching site overview...")
    overview = client.get_site_overview(days=days)

    lines = []
    lines.append(f"# Site Overview: {site_url}")
    lines.append(f"\n**Report Date:** {datetime.now().strftime('%Y-%m-%d')}")
    lines.append(f"**Data Period:** {overview.date_range['start']} to {overview.date_range['end']}")

    lines.append("\n## Performance Summary\n")
    lines.append(f"| Metric | Value |")
    lines.append(f"|--------|-------|")
    lines.append(f"| Total Clicks | {overview.total_clicks:,} |")
    lines.append(f"| Total Impressions | {overview.total_impressions:,} |")
    lines.append(f"| Average CTR | {overview.avg_ctr*100:.2f}% |")
    lines.append(f"| Average Position | {overview.avg_position:.1f} |")

    lines.append("\n## Top Pages\n")
    lines.append("| Page | Clicks | Impressions | CTR | Position |")
    lines.append("|------|--------|-------------|-----|----------|")
    for page in overview.top_pages[:20]:
        path = page['page'].replace(site_url, '')
        lines.append(f"| {path} | {page['clicks']:,} | {page['impressions']:,} | {page['ctr']*100:.1f}% | {page['position']:.1f} |")

    lines.append("\n## Top Queries\n")
    lines.append("| Query | Clicks | Impressions | CTR | Position |")
    lines.append("|-------|--------|-------------|-----|----------|")
    for q in overview.top_queries[:30]:
        lines.append(f"| {q.query} | {q.clicks:,} | {q.impressions:,} | {q.ctr*100:.1f}% | {q.position:.1f} |")

    # High opportunity queries
    opp_queries = sorted(overview.top_queries, key=lambda x: x.opportunity_score, reverse=True)[:20]
    lines.append("\n## Highest Opportunity Queries\n")
    lines.append("| Query | Position | Impressions | Opportunity Score |")
    lines.append("|-------|----------|-------------|-------------------|")
    for q in opp_queries:
        lines.append(f"| {q.query} | {q.position:.1f} | {q.impressions:,} | {q.opportunity_score:.1f} |")

    return '\n'.join(lines)


def list_available_sites():
    """List all sites available in the GSC account."""
    print(f"Connecting to Google Search Console...")
    client = GSCClient(str(GSC_CREDENTIALS_PATH), "")

    print("Available sites:")
    for site in client.list_sites():
        print(f"  - {site}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate data-driven content reviews using GSC data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Review a specific page:
    python scripts/review_content.py --url "https://your-site.com/off-plan-projects-new-developments-dubai/"

  Review with page content for better analysis:
    python scripts/review_content.py --url "https://your-site.com/off-plan-projects-new-developments-dubai/" --content-file page_content.txt

  Get site-wide overview:
    python scripts/review_content.py --site-overview --gsc-property "sc-domain:your-site.com"

  List available sites:
    python scripts/review_content.py --list-sites
        """
    )

    parser.add_argument(
        "--url",
        type=str,
        help="Full URL of the page to review (e.g., 'https://your-site.com/off-plan-projects/')"
    )

    parser.add_argument(
        "--content-file",
        type=str,
        help="Path to file containing page content text"
    )

    parser.add_argument(
        "--title",
        type=str,
        default="",
        help="Page title for analysis"
    )

    parser.add_argument(
        "--meta",
        type=str,
        default="",
        help="Meta description for analysis"
    )

    parser.add_argument(
        "--gsc-property",
        type=str,
        default="sc-domain:your-site.com",
        help="GSC property for site overview (e.g., 'sc-domain:your-site.com')"
    )

    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Days of data to analyze (default: 90)"
    )

    parser.add_argument(
        "--site-overview",
        action="store_true",
        help="Generate site-wide overview instead of page review"
    )

    parser.add_argument(
        "--list-sites",
        action="store_true",
        help="List available sites in GSC account"
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (default: auto-generated in reviews folder)"
    )

    args = parser.parse_args()

    # Check credentials exist
    if not GSC_CREDENTIALS_PATH.exists():
        print(f"Error: GSC credentials not found at {GSC_CREDENTIALS_PATH}")
        print("Please place your service account JSON file in the expected location.")
        sys.exit(1)

    try:
        if args.list_sites:
            list_available_sites()
            return

        if args.site_overview:
            report = get_site_overview(args.gsc_property, args.days)
            output_file = args.output or OUTPUT_DIR / f"site-overview-{datetime.now().strftime('%Y%m%d')}.md"
        elif args.url:
            # Load content if file provided
            content = ""
            if args.content_file:
                content = Path(args.content_file).read_text(encoding='utf-8')

            report, page_path = review_page(
                page_url=args.url,
                content=content,
                title=args.title,
                meta_description=args.meta,
                days=args.days
            )

            # Generate output filename from page path
            safe_name = page_path.strip('/').replace('/', '-') or 'homepage'
            output_file = args.output or OUTPUT_DIR / f"review-{safe_name}-{datetime.now().strftime('%Y%m%d')}.md"
        else:
            parser.print_help()
            print("\nError: Either --url or --site-overview is required")
            sys.exit(1)

        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save report
        output_path.write_text(report, encoding='utf-8')
        print(f"\nReport saved to: {output_path}")

        # Also print summary to console
        print("\n" + "="*60)
        print("REPORT PREVIEW")
        print("="*60)
        preview_lines = report.split('\n')[:40]
        print('\n'.join(preview_lines))
        if len(report.split('\n')) > 40:
            print(f"\n... ({len(report.split(chr(10))) - 40} more lines in full report)")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
