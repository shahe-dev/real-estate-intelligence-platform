#!/usr/bin/env python3
"""
Folder-Based Content Optimization

Quick setup and processing for content optimization.

Usage:
    # Quick create - just paste URL
    python scripts/optimize_folder.py --new "https://your-site.com/palm-jumeirah/"

    # Create with title
    python scripts/optimize_folder.py --new "https://..." --title "Page Title"

    # Create and paste content interactively
    python scripts/optimize_folder.py --new "https://..." --paste

    # Process a folder
    python scripts/optimize_folder.py --folder palm-jumeirah

    # Process and create in one step
    python scripts/optimize_folder.py --new "https://..." --run

    # List available input folders
    python scripts/optimize_folder.py --list
"""

import argparse
import os
import sys
import json
import shutil
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).parent.parent))

# Paths
INPUTS_DIR = Path(os.getenv("CONTENT_INPUT_DIR", "data/generated_content/content-reviews/inputs"))
OUTPUT_DIR = Path(os.getenv("CONTENT_OUTPUT_DIR", "data/generated_content/content-reviews"))
GSC_CREDENTIALS_PATH = Path(os.getenv("GSC_CREDENTIALS_PATH", "credentials/gsc-service-account.json"))

# GSC Property mapping
# Configure your GSC properties in environment or config file
# Example: GSC_PROPERTY_MAP = {"your-site.com": "https://your-site.com/"}
GSC_PROPERTY_MAP = {}


def get_gsc_property(page_url: str) -> str:
    """Get GSC property for a URL."""
    parsed = urlparse(page_url)
    domain = parsed.netloc

    for key, value in GSC_PROPERTY_MAP.items():
        if key in domain:
            return value

    raise ValueError(f"Unknown domain: {domain}")


def find_screenshot(folder: Path) -> str | None:
    """Find screenshot file in folder."""
    for ext in ['.png', '.jpg', '.jpeg', '.webp']:
        for pattern in ['screenshot', 'screen', 'page', '*']:
            matches = list(folder.glob(f"{pattern}{ext}"))
            if matches:
                return str(matches[0])
    return None


def read_folder_inputs(folder: Path) -> dict:
    """Read all inputs from a folder."""
    inputs = {
        'url': None,
        'content': '',
        'screenshot': None,
        'title': '',
        'meta_description': '',
    }

    # URL (required)
    url_file = folder / 'url.txt'
    if url_file.exists():
        inputs['url'] = url_file.read_text(encoding='utf-8').strip()
    else:
        raise ValueError(f"Missing url.txt in {folder.name}")

    # Content (optional)
    content_file = folder / 'content.txt'
    if content_file.exists():
        inputs['content'] = content_file.read_text(encoding='utf-8')

    # Screenshot (optional)
    inputs['screenshot'] = find_screenshot(folder)

    # Meta info (optional)
    meta_file = folder / 'meta.json'
    if meta_file.exists():
        meta = json.loads(meta_file.read_text(encoding='utf-8'))
        inputs['title'] = meta.get('title', '')
        inputs['meta_description'] = meta.get('meta_description', '')

    return inputs


def process_folder(folder: Path, use_market_data: bool = True, use_anthropic: bool = False) -> Path:
    """Process a single input folder and generate optimization report."""
    from src.analytics.gsc_client import GSCClient
    from src.analytics.content_optimizer import ContentOptimizer

    print(f"\n{'='*60}")
    print(f"Processing: {folder.name}")
    print(f"{'='*60}")

    # Read inputs
    inputs = read_folder_inputs(folder)

    print(f"  URL: {inputs['url']}")
    print(f"  Content: {len(inputs['content'])} chars" if inputs['content'] else "  Content: (none)")
    print(f"  Screenshot: {Path(inputs['screenshot']).name if inputs['screenshot'] else '(none)'}")
    print(f"  Title: {inputs['title'][:50]}..." if len(inputs['title']) > 50 else f"  Title: {inputs['title'] or '(none)'}")

    # Setup optimizer
    gsc_property = get_gsc_property(inputs['url'])
    gsc_client = GSCClient(str(GSC_CREDENTIALS_PATH), gsc_property)

    # Database connection
    db_connection = None
    if use_market_data:
        try:
            import duckdb
            from config.bigquery_settings import bq_settings
            if Path(bq_settings.PM_DB_PATH).exists():
                db_connection = duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=True)
                print(f"  Market data: Connected")
        except Exception as e:
            print(f"  Market data: Unavailable ({e})")

    # Anthropic client
    anthropic_client = None
    if use_anthropic and inputs['screenshot']:
        try:
            import anthropic
            anthropic_client = anthropic.Anthropic()
            print(f"  Screenshot analysis: Enabled")
        except Exception:
            print(f"  Screenshot analysis: Unavailable")

    optimizer = ContentOptimizer(
        gsc_client=gsc_client,
        db_connection=db_connection,
        anthropic_client=anthropic_client
    )

    print(f"\n  Fetching GSC data...")
    plan = optimizer.optimize_page(
        page_url=inputs['url'],
        page_content=inputs['content'],
        page_title=inputs['title'] or f"Page: {urlparse(inputs['url']).path}",
        meta_description=inputs['meta_description'],
        screenshot_path=inputs['screenshot'],
        days=90
    )

    # Generate report
    report = plan.to_markdown()

    # Create analysis output folder
    analysis_folder = OUTPUT_DIR / f"{folder.name}-{datetime.now().strftime('%Y-%m-%d')}"
    analysis_folder.mkdir(parents=True, exist_ok=True)

    # Save all files
    (analysis_folder / "report.md").write_text(report, encoding='utf-8')
    (analysis_folder / "url.txt").write_text(inputs['url'], encoding='utf-8')

    if inputs['content']:
        (analysis_folder / "content.txt").write_text(inputs['content'], encoding='utf-8')

    if inputs['screenshot']:
        shutil.copy(inputs['screenshot'], analysis_folder / Path(inputs['screenshot']).name)

    # Save GSC query data as JSON for reference
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

    print(f"\n  Analysis saved: {analysis_folder.name}/")
    print(f"  Queries found: {len(plan.gsc_review.gsc_performance.queries) if plan.gsc_review else 0}")
    print(f"  Priority actions: {len(plan.priority_actions)}")

    return analysis_folder


def create_new_input(url: str, title: str = "", meta_desc: str = "", paste_content: bool = False) -> Path:
    """
    Create a new input folder from a URL.

    Returns the folder path.
    """
    parsed = urlparse(url)

    # Generate folder name from URL path
    path_parts = parsed.path.strip('/').split('/')
    if path_parts and path_parts[0]:
        folder_name = path_parts[0]  # Use first path segment
    else:
        folder_name = parsed.netloc.replace('.', '-')

    # Clean folder name
    folder_name = folder_name.lower().replace(' ', '-')

    folder = INPUTS_DIR / folder_name

    # Handle existing folder
    if folder.exists():
        print(f"Folder already exists: {folder_name}/")
        response = input("Overwrite? (y/N): ").strip().lower()
        if response != 'y':
            print("Cancelled.")
            return folder

    # Create folder
    folder.mkdir(parents=True, exist_ok=True)

    # Write URL
    (folder / 'url.txt').write_text(url, encoding='utf-8')
    print(f"Created: {folder_name}/url.txt")

    # Write meta if provided
    if title or meta_desc:
        meta = {}
        if title:
            meta['title'] = title
        if meta_desc:
            meta['meta_description'] = meta_desc
        (folder / 'meta.json').write_text(json.dumps(meta, indent=2), encoding='utf-8')
        print(f"Created: {folder_name}/meta.json")

    # Paste content interactively
    if paste_content:
        print("\nPaste page content (press Enter twice when done, or Ctrl+C to skip):")
        try:
            lines = []
            empty_count = 0
            while empty_count < 2:
                line = input()
                if line == "":
                    empty_count += 1
                else:
                    empty_count = 0
                    lines.append(line)

            if lines:
                content = '\n'.join(lines)
                (folder / 'content.txt').write_text(content, encoding='utf-8')
                print(f"Created: {folder_name}/content.txt ({len(content)} chars)")
        except KeyboardInterrupt:
            print("\nSkipped content.")

    # Create empty content.txt placeholder
    content_file = folder / 'content.txt'
    if not content_file.exists():
        content_file.write_text("", encoding='utf-8')

    print(f"\nFolder ready: {folder}")
    print(f"  - Drop screenshot.png into this folder")
    print(f"  - Paste content into content.txt")
    print(f"  - Run: python scripts/optimize_folder.py --folder {folder_name}")

    # Try to open folder in explorer (Windows)
    try:
        import subprocess
        subprocess.Popen(f'explorer "{folder}"', shell=True)
    except:
        pass

    return folder


def list_folders():
    """List available input folders."""
    if not INPUTS_DIR.exists():
        print(f"No inputs directory found at: {INPUTS_DIR}")
        print(f"\nCreate it and add folders with url.txt files.")
        return

    folders = [f for f in INPUTS_DIR.iterdir() if f.is_dir()]

    if not folders:
        print(f"No input folders found in: {INPUTS_DIR}")
        return

    print(f"\nAvailable input folders ({INPUTS_DIR}):\n")

    for folder in sorted(folders):
        url_file = folder / 'url.txt'
        url = url_file.read_text(encoding='utf-8').strip() if url_file.exists() else "(missing url.txt)"

        has_content = (folder / 'content.txt').exists()
        has_screenshot = find_screenshot(folder) is not None
        has_meta = (folder / 'meta.json').exists()

        status = []
        if has_content:
            status.append("content")
        if has_screenshot:
            status.append("screenshot")
        if has_meta:
            status.append("meta")

        status_str = f"[{', '.join(status)}]" if status else ""

        print(f"  {folder.name}/")
        print(f"    URL: {url}")
        if status_str:
            print(f"    Has: {status_str}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Folder-based content optimization",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Quick Start:
    # Create new input from URL (opens folder in explorer)
    python scripts/optimize_folder.py --new "https://your-site.com/palm-jumeirah/"

    # Create and paste content
    python scripts/optimize_folder.py --new "https://..." --paste

    # Create and run immediately
    python scripts/optimize_folder.py --new "https://..." --run

    # Process existing folder
    python scripts/optimize_folder.py --folder palm-jumeirah

    # List all folders
    python scripts/optimize_folder.py --list
        """
    )

    # Create new input
    parser.add_argument("--new", type=str, metavar="URL", help="Create new input folder from URL")
    parser.add_argument("--title", type=str, default="", help="Page title (with --new)")
    parser.add_argument("--meta", type=str, default="", help="Meta description (with --new)")
    parser.add_argument("--paste", action="store_true", help="Paste content interactively (with --new)")
    parser.add_argument("--run", action="store_true", help="Run optimization after creating (with --new)")

    # Process existing
    parser.add_argument("--folder", type=str, help="Process specific folder")
    parser.add_argument("--all", action="store_true", help="Process all folders")
    parser.add_argument("--list", action="store_true", help="List available folders")
    parser.add_argument("--no-market-data", action="store_true", help="Skip Property Monitor data")
    parser.add_argument("--use-anthropic", action="store_true", help="Use Anthropic for screenshot analysis")

    args = parser.parse_args()

    # Ensure inputs directory exists
    INPUTS_DIR.mkdir(parents=True, exist_ok=True)

    # Handle --new
    if args.new:
        folder = create_new_input(
            url=args.new,
            title=args.title,
            meta_desc=args.meta,
            paste_content=args.paste
        )

        if args.run:
            if not GSC_CREDENTIALS_PATH.exists():
                print(f"Error: GSC credentials not found at {GSC_CREDENTIALS_PATH}")
                sys.exit(1)
            process_folder(
                folder,
                use_market_data=not args.no_market_data,
                use_anthropic=args.use_anthropic
            )
        return

    if args.list:
        list_folders()
        return

    if not GSC_CREDENTIALS_PATH.exists():
        print(f"Error: GSC credentials not found at {GSC_CREDENTIALS_PATH}")
        sys.exit(1)

    folders_to_process = []

    if args.folder:
        folder_path = INPUTS_DIR / args.folder
        if not folder_path.exists():
            print(f"Error: Folder not found: {folder_path}")
            sys.exit(1)
        folders_to_process = [folder_path]
    elif args.all:
        folders_to_process = [f for f in INPUTS_DIR.iterdir() if f.is_dir()]
    else:
        # Default: show help and list folders
        parser.print_help()
        print("\n")
        list_folders()
        return

    if not folders_to_process:
        print("No folders to process.")
        return

    results = []
    for folder in folders_to_process:
        try:
            output = process_folder(
                folder,
                use_market_data=not args.no_market_data,
                use_anthropic=args.use_anthropic
            )
            results.append({'folder': folder.name, 'status': 'success', 'output': str(output)})
        except Exception as e:
            print(f"\n  Error: {e}")
            results.append({'folder': folder.name, 'status': 'error', 'error': str(e)})

    # Summary
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    success = len([r for r in results if r['status'] == 'success'])
    print(f"Processed: {success}/{len(results)} folders")

    for r in results:
        status = "OK" if r['status'] == 'success' else "FAILED"
        print(f"  [{status}] {r['folder']}")


if __name__ == "__main__":
    main()
