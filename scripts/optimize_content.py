#!/usr/bin/env python3
"""
Content Optimization Workflow

Supercharged content optimization combining:
- Google Search Console data (queries, CTR, positions)
- Property Monitor market intelligence (trends, pricing, opportunities)
- Screenshot analysis (visual structure, UX issues)
- Current content analysis

Usage:
    # Single page optimization
    python scripts/optimize_content.py --url "https://your-site.com/page/"

    # With all inputs for best recommendations
    python scripts/optimize_content.py \\
        --url "https://your-site.com/off-plan-projects/" \\
        --content-file content.txt \\
        --screenshot screenshot.png \\
        --title "Off-Plan Projects in Dubai"

    # Batch processing from JSON config
    python scripts/optimize_content.py --batch pages_to_optimize.json

    # Interactive mode
    python scripts/optimize_content.py --interactive
"""

import argparse
import sys
import json
import os
import shutil
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from typing import Optional, Dict, Any, List

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))


# Default paths
GSC_CREDENTIALS_PATH = Path(os.getenv("GSC_CREDENTIALS_PATH", "credentials/gsc-service-account.json"))
OUTPUT_DIR = Path(os.getenv("CONTENT_OUTPUT_DIR", "data/generated_content/content-reviews"))
DB_PATH = Path("data/dubai_transactions.duckdb")

# Site URL to GSC property mapping
# Configure your GSC properties in environment or config file
# Example: GSC_PROPERTY_MAP = {"your-site.com": "https://your-site.com/"}
GSC_PROPERTY_MAP = {}


def get_gsc_property(page_url: str) -> str:
    """Get GSC property for a URL."""
    parsed = urlparse(page_url)
    domain = parsed.netloc

    gsc_property = GSC_PROPERTY_MAP.get(domain)
    if not gsc_property:
        for key, value in GSC_PROPERTY_MAP.items():
            if key in domain:
                return value

    if not gsc_property:
        raise ValueError(f"Unknown domain: {domain}")

    return gsc_property


def setup_optimizer(page_url: str, use_market_data: bool = True, use_anthropic: bool = False):
    """Set up the content optimizer with required clients."""
    from src.analytics.gsc_client import GSCClient
    from src.analytics.content_optimizer import ContentOptimizer

    # GSC client
    gsc_property = get_gsc_property(page_url)
    gsc_client = GSCClient(str(GSC_CREDENTIALS_PATH), gsc_property)

    # Database connection for market data
    db_connection = None
    if use_market_data and DB_PATH.exists():
        try:
            import duckdb
            db_connection = duckdb.connect(str(DB_PATH), read_only=True)
            print(f"  Connected to Property Monitor database")
        except Exception as e:
            print(f"  Warning: Could not connect to database: {e}")

    # Anthropic client for screenshot analysis
    anthropic_client = None
    if use_anthropic:
        try:
            import anthropic
            anthropic_client = anthropic.Anthropic()
            print(f"  Anthropic client initialized for screenshot analysis")
        except Exception as e:
            print(f"  Warning: Could not initialize Anthropic client: {e}")

    return ContentOptimizer(
        gsc_client=gsc_client,
        db_connection=db_connection,
        anthropic_client=anthropic_client
    )


def optimize_single_page(
    page_url: str,
    content: str = "",
    title: str = "",
    meta_description: str = "",
    screenshot_path: Optional[str] = None,
    days: int = 90,
    use_market_data: bool = True,
    use_anthropic: bool = False
):
    """
    Optimize a single page and return the optimization plan.
    """
    print(f"\nOptimizing: {page_url}")
    print(f"  Setting up optimizer...")

    optimizer = setup_optimizer(page_url, use_market_data, use_anthropic)

    print(f"  Analyzing page...")
    plan = optimizer.optimize_page(
        page_url=page_url,
        page_content=content,
        page_title=title or f"Page: {urlparse(page_url).path}",
        meta_description=meta_description,
        screenshot_path=screenshot_path,
        days=days
    )

    return plan


def run_batch(config_path: str, output_dir: Optional[str] = None):
    """
    Run optimization on multiple pages from a JSON config file.

    Config format:
    {
        "pages": [
            {
                "url": "https://your-site.com/page/",
                "content_file": "content1.txt",
                "screenshot": "screenshot1.png",
                "title": "Page Title"
            },
            ...
        ],
        "settings": {
            "days": 90,
            "use_market_data": true,
            "use_anthropic": false
        }
    }
    """
    config_path = Path(config_path)
    if not config_path.exists():
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path) as f:
        config = json.load(f)

    pages = config.get('pages', [])
    settings = config.get('settings', {})

    output_path = Path(output_dir) if output_dir else OUTPUT_DIR
    output_path.mkdir(parents=True, exist_ok=True)

    print(f"Processing {len(pages)} pages...")

    results = []
    for i, page in enumerate(pages, 1):
        print(f"\n[{i}/{len(pages)}] Processing {page['url']}")

        try:
            # Load content if file specified
            content = ""
            if page.get('content_file'):
                content_file = Path(page['content_file'])
                if content_file.exists():
                    content = content_file.read_text(encoding='utf-8')

            # Check screenshot
            screenshot = page.get('screenshot')
            if screenshot and not Path(screenshot).exists():
                print(f"  Warning: Screenshot not found: {screenshot}")
                screenshot = None

            plan = optimize_single_page(
                page_url=page['url'],
                content=content,
                title=page.get('title', ''),
                meta_description=page.get('meta_description', ''),
                screenshot_path=screenshot,
                days=settings.get('days', 90),
                use_market_data=settings.get('use_market_data', True),
                use_anthropic=settings.get('use_anthropic', False)
            )

            # Create analysis folder
            parsed = urlparse(page['url'])
            safe_name = parsed.path.strip('/').replace('/', '-') or 'homepage'
            folder_name = f"{safe_name}-{datetime.now().strftime('%Y-%m-%d')}"
            analysis_folder = output_path / folder_name
            analysis_folder.mkdir(parents=True, exist_ok=True)

            # Save all files in the folder
            report = plan.to_markdown()
            (analysis_folder / "report.md").write_text(report, encoding='utf-8')
            (analysis_folder / "url.txt").write_text(page['url'], encoding='utf-8')

            if content:
                (analysis_folder / "content.txt").write_text(content, encoding='utf-8')

            if screenshot and Path(screenshot).exists():
                shutil.copy(screenshot, analysis_folder / Path(screenshot).name)

            # Save GSC query data as JSON
            if plan.gsc_review:
                gsc_data = {
                    'total_clicks': plan.gsc_review.gsc_performance.total_clicks,
                    'total_impressions': plan.gsc_review.gsc_performance.total_impressions,
                    'queries': [
                        {
                            'query': q.query,
                            'clicks': q.clicks,
                            'impressions': q.impressions,
                            'ctr': q.ctr,
                            'position': q.position
                        }
                        for q in plan.gsc_review.gsc_performance.queries
                    ]
                }
                (analysis_folder / "gsc-queries.json").write_text(
                    json.dumps(gsc_data, indent=2, ensure_ascii=False),
                    encoding='utf-8'
                )

            results.append({
                'url': page['url'],
                'status': 'success',
                'folder': str(analysis_folder)
            })
            print(f"  Saved: {analysis_folder}/")

        except Exception as e:
            results.append({
                'url': page['url'],
                'status': 'error',
                'error': str(e)
            })
            print(f"  Error: {e}")

    # Save summary
    summary_file = output_path / f"batch-summary-{datetime.now().strftime('%Y%m%d-%H%M%S')}.json"
    with open(summary_file, 'w') as f:
        json.dump({
            'processed': len(pages),
            'successful': len([r for r in results if r['status'] == 'success']),
            'failed': len([r for r in results if r['status'] == 'error']),
            'results': results
        }, f, indent=2)

    print(f"\n{'='*60}")
    print(f"Batch complete: {len([r for r in results if r['status'] == 'success'])}/{len(pages)} successful")
    print(f"Summary saved: {summary_file}")


def run_interactive():
    """
    Interactive mode for optimizing pages one at a time.
    """
    print("\n" + "="*60)
    print("CONTENT OPTIMIZATION - INTERACTIVE MODE")
    print("="*60)
    print("\nThis will guide you through optimizing a page with all available data sources.")
    print("Press Ctrl+C to exit at any time.\n")

    while True:
        try:
            # Get URL
            print("-" * 40)
            page_url = input("\nEnter page URL (or 'quit' to exit): ").strip()
            if page_url.lower() in ['quit', 'exit', 'q']:
                break

            if not page_url.startswith('http'):
                print("Error: Please enter a full URL starting with http:// or https://")
                continue

            # Get title
            title = input("Page title (optional, press Enter to skip): ").strip()

            # Get content
            content = ""
            content_input = input("Content file path OR paste content (press Enter twice to finish, or skip): ").strip()
            if content_input:
                if Path(content_input).exists():
                    content = Path(content_input).read_text(encoding='utf-8')
                    print(f"  Loaded {len(content)} characters from file")
                else:
                    # Allow multi-line paste
                    content_lines = [content_input]
                    print("  (Paste content, press Enter twice when done)")
                    empty_count = 0
                    while empty_count < 1:
                        line = input()
                        if line == "":
                            empty_count += 1
                        else:
                            empty_count = 0
                            content_lines.append(line)
                    content = '\n'.join(content_lines)
                    print(f"  Captured {len(content)} characters")

            # Get screenshot
            screenshot_path = input("Screenshot file path (optional, press Enter to skip): ").strip()
            if screenshot_path and not Path(screenshot_path).exists():
                print(f"  Warning: Screenshot not found, skipping")
                screenshot_path = None

            # Options
            use_market = input("Use Property Monitor market data? (Y/n): ").strip().lower() != 'n'
            use_anthropic = False
            if screenshot_path:
                use_anthropic = input("Use AI for screenshot analysis? (y/N): ").strip().lower() == 'y'

            # Run optimization
            print("\nOptimizing page...")
            report = optimize_single_page(
                page_url=page_url,
                content=content,
                title=title,
                screenshot_path=screenshot_path,
                use_market_data=use_market,
                use_anthropic=use_anthropic
            )

            # Save report
            parsed = urlparse(page_url)
            safe_name = parsed.path.strip('/').replace('/', '-') or 'homepage'
            report_file = OUTPUT_DIR / f"optimize-{safe_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.md"
            OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
            report_file.write_text(report, encoding='utf-8')

            print(f"\nReport saved: {report_file}")

            # Show preview
            print("\n" + "="*60)
            print("REPORT PREVIEW")
            print("="*60)
            preview_lines = report.split('\n')[:50]
            print('\n'.join(preview_lines))
            if len(report.split('\n')) > 50:
                print(f"\n... ({len(report.split(chr(10))) - 50} more lines)")

            input("\nPress Enter to continue...")

        except KeyboardInterrupt:
            print("\n\nExiting...")
            break
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(
        description="Supercharged content optimization with GSC + Property Monitor + Visual Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Single page:
    python scripts/optimize_content.py --url "https://your-site.com/off-plan-projects/"

  With all inputs:
    python scripts/optimize_content.py \\
        --url "https://your-site.com/off-plan-projects/" \\
        --content-file page_content.txt \\
        --screenshot screenshot.png \\
        --title "Off-Plan Projects in Dubai"

  Batch processing:
    python scripts/optimize_content.py --batch pages_config.json

  Interactive mode:
    python scripts/optimize_content.py --interactive
        """
    )

    parser.add_argument(
        "--url",
        type=str,
        help="Full URL of the page to optimize"
    )

    parser.add_argument(
        "--content-file",
        type=str,
        help="Path to file containing page content text"
    )

    parser.add_argument(
        "--screenshot",
        type=str,
        help="Path to page screenshot image"
    )

    parser.add_argument(
        "--title",
        type=str,
        default="",
        help="Page title"
    )

    parser.add_argument(
        "--meta",
        type=str,
        default="",
        help="Meta description"
    )

    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Days of GSC data to analyze (default: 90)"
    )

    parser.add_argument(
        "--batch",
        type=str,
        help="Path to JSON config file for batch processing"
    )

    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Run in interactive mode"
    )

    parser.add_argument(
        "--no-market-data",
        action="store_true",
        help="Skip Property Monitor market data enrichment"
    )

    parser.add_argument(
        "--use-anthropic",
        action="store_true",
        help="Use Anthropic API for screenshot analysis (requires ANTHROPIC_API_KEY)"
    )

    parser.add_argument(
        "--output",
        type=str,
        help="Output file path (default: auto-generated)"
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for batch mode"
    )

    args = parser.parse_args()

    # Check credentials
    if not GSC_CREDENTIALS_PATH.exists():
        print(f"Error: GSC credentials not found at {GSC_CREDENTIALS_PATH}")
        sys.exit(1)

    try:
        if args.interactive:
            run_interactive()
        elif args.batch:
            run_batch(args.batch, args.output_dir)
        elif args.url:
            # Load content if file provided
            content = ""
            if args.content_file:
                content = Path(args.content_file).read_text(encoding='utf-8')

            # Check screenshot
            screenshot = args.screenshot
            if screenshot and not Path(screenshot).exists():
                print(f"Warning: Screenshot not found: {screenshot}")
                screenshot = None

            plan = optimize_single_page(
                page_url=args.url,
                content=content,
                title=args.title,
                meta_description=args.meta,
                screenshot_path=screenshot,
                days=args.days,
                use_market_data=not args.no_market_data,
                use_anthropic=args.use_anthropic
            )

            report = plan.to_markdown()

            # Create analysis folder
            parsed = urlparse(args.url)
            safe_name = parsed.path.strip('/').replace('/', '-') or 'homepage'
            folder_name = f"{safe_name}-{datetime.now().strftime('%Y-%m-%d')}"

            if args.output:
                output_folder = Path(args.output)
            else:
                output_folder = OUTPUT_DIR / folder_name

            output_folder.mkdir(parents=True, exist_ok=True)

            # Save all files in the folder
            (output_folder / "report.md").write_text(report, encoding='utf-8')
            (output_folder / "url.txt").write_text(args.url, encoding='utf-8')

            if content:
                (output_folder / "content.txt").write_text(content, encoding='utf-8')

            if screenshot and Path(screenshot).exists():
                shutil.copy(screenshot, output_folder / Path(screenshot).name)

            # Save GSC query data as JSON
            if plan.gsc_review:
                gsc_data = {
                    'total_clicks': plan.gsc_review.gsc_performance.total_clicks,
                    'total_impressions': plan.gsc_review.gsc_performance.total_impressions,
                    'queries': [
                        {
                            'query': q.query,
                            'clicks': q.clicks,
                            'impressions': q.impressions,
                            'ctr': q.ctr,
                            'position': q.position
                        }
                        for q in plan.gsc_review.gsc_performance.queries
                    ]
                }
                (output_folder / "gsc-queries.json").write_text(
                    json.dumps(gsc_data, indent=2, ensure_ascii=False),
                    encoding='utf-8'
                )

            print(f"\nAnalysis saved: {output_folder}/")

            # Show preview
            print("\n" + "="*60)
            print("REPORT PREVIEW")
            print("="*60)
            preview_lines = report.split('\n')[:50]
            print('\n'.join(preview_lines))
            if len(report.split('\n')) > 50:
                print(f"\n... ({len(report.split(chr(10))) - 50} more lines)")

        else:
            parser.print_help()
            print("\nError: Either --url, --batch, or --interactive is required")
            sys.exit(1)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
