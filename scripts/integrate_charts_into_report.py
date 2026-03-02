"""
Integrate generated charts into the 2025 Annual Report.
Adds chart references at appropriate sections.
"""

import re
from pathlib import Path

# Paths
REPORT_PATH = Path("data/generated_content/pm_market_report_2025_annual.md")
CHARTS_DIR = Path("data/generated_content/charts/2025_annual")
OUTPUT_PATH = Path("data/generated_content/pm_market_report_2025_annual_with_charts.md")

# Chart mappings - which charts go in which sections
CHART_SECTIONS = {
    "## Executive Summary": [
        ("yoy_metrics_comparison.png", "### 2025 vs 2024: Key Market Metrics"),
    ],
    "## Transaction Volume Analysis": [
        ("monthly_trend.png", "### Monthly Transaction Volume & Average Price"),
        ("monthly_yoy_overlay.png", "### Year-over-Year Monthly Comparison"),
    ],
    "## Price Performance": [
        ("price_segments_pyramid.png", "### Market Distribution by Price Segment"),
        ("price_segment_donut.png", "### Price Segment Breakdown"),
        ("price_momentum.png", "### Quarterly Price Momentum (2024-2025)"),
    ],
    "## Geographic Analysis": [
        ("top_areas_horizontal.png", "### Top 10 Areas by Transaction Volume"),
        ("area_bubble_chart.png", "### Area Performance: Volume vs Price"),
        ("emerging_hotspots.png", "### Emerging Areas by Transaction Growth"),
    ],
    "## Developer Landscape": [
        ("developer_market_share.png", "### Top 10 Developers by Market Share"),
    ],
    "## Market Segmentation": [
        ("offplan_vs_ready.png", "### Off-Plan vs Ready Transactions by Month"),
    ],
    "## Market Outlook": [
        ("record_transactions.png", "### Top 5 Record Transactions in 2025"),
    ],
}


def create_chart_markdown(chart_file: str, title: str) -> str:
    """Create markdown for a chart with title."""
    rel_path = f"charts/2025_annual/{chart_file}"
    return f"""
{title}

![{title}]({rel_path})

"""


def integrate_charts():
    """Read the report and integrate charts at appropriate sections."""

    print("=" * 70)
    print("INTEGRATING CHARTS INTO 2025 ANNUAL REPORT")
    print("=" * 70)

    # Read original report
    with open(REPORT_PATH, 'r', encoding='utf-8') as f:
        content = f.read()

    print(f"\nOriginal report: {len(content):,} characters")

    # Process each section
    charts_added = 0

    for section_header, charts in CHART_SECTIONS.items():
        # Find the section in the content
        section_pattern = re.escape(section_header)
        match = re.search(section_pattern, content)

        if match:
            # Find the end of the section header line
            section_start = match.end()

            # Find the next line break after the header
            next_newline = content.find('\n', section_start)
            if next_newline == -1:
                next_newline = len(content)

            # Build chart markdown
            charts_md = "\n"
            for chart_file, chart_title in charts:
                chart_path = CHARTS_DIR / chart_file
                if chart_path.exists():
                    charts_md += create_chart_markdown(chart_file, chart_title)
                    charts_added += 1
                    print(f"  [+] {chart_file} -> {section_header}")
                else:
                    print(f"  [!] Missing: {chart_file}")

            # Insert charts after the section header
            content = content[:next_newline] + charts_md + content[next_newline:]
        else:
            print(f"  [!] Section not found: {section_header}")

    # Write enhanced report
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"\n{'=' * 70}")
    print(f"INTEGRATION COMPLETE")
    print(f"{'=' * 70}")
    print(f"Charts integrated: {charts_added}")
    print(f"Enhanced report: {OUTPUT_PATH}")
    print(f"New size: {len(content):,} characters")

    return OUTPUT_PATH


if __name__ == "__main__":
    integrate_charts()
