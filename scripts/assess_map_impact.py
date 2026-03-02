"""
Assess impact of a site map page improvement (before/after analysis).

Pulls GSC data for before/after periods and generates a comparison report.
"""

import os
import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.gsc_client import GSCClient

CREDENTIALS_PATH = os.getenv("GSC_CREDENTIALS_PATH", "credentials/gsc-service-account.json")
SITE_URL = os.getenv("GSC_SITE_URL", "https://your-site.com/")
LAUNCH_DATE = "2026-01-23"

# GSC data has ~3 day delay
DATA_END = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")

# Before: 30 days before launch (Dec 24 - Jan 22)
BEFORE_START = "2025-12-24"
BEFORE_END = "2026-01-22"

# After: Jan 23 to latest available
AFTER_START = "2026-01-23"
AFTER_END = DATA_END

before_days = (datetime.strptime(BEFORE_END, "%Y-%m-%d") - datetime.strptime(BEFORE_START, "%Y-%m-%d")).days + 1
after_days = (datetime.strptime(AFTER_END, "%Y-%m-%d") - datetime.strptime(AFTER_START, "%Y-%m-%d")).days + 1


def get_daily_data(client, page_filter):
    """Get date-level data for trend analysis."""
    request = {
        "startDate": BEFORE_START,
        "endDate": AFTER_END,
        "dimensions": ["date"],
        "dimensionFilterGroups": [{
            "filters": [{
                "dimension": "page",
                "operator": "contains",
                "expression": page_filter
            }]
        }],
        "rowLimit": 500
    }
    response = client.service.searchanalytics().query(
        siteUrl=client.site_url,
        body=request
    ).execute()
    rows = []
    for row in response.get("rows", []):
        rows.append({
            "date": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"]
        })
    return sorted(rows, key=lambda r: r["date"])


def get_query_data(client, page_filter, start_date, end_date):
    """Get query-level data for a date range."""
    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["query"],
        "dimensionFilterGroups": [{
            "filters": [{
                "dimension": "page",
                "operator": "contains",
                "expression": page_filter
            }]
        }],
        "rowLimit": 500
    }
    response = client.service.searchanalytics().query(
        siteUrl=client.site_url,
        body=request
    ).execute()
    queries = []
    for row in response.get("rows", []):
        queries.append({
            "query": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"]
        })
    return queries


def get_page_level_data(client, page_filter, start_date, end_date):
    """Get page-level aggregate data for a date range."""
    request = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": ["page"],
        "dimensionFilterGroups": [{
            "filters": [{
                "dimension": "page",
                "operator": "contains",
                "expression": page_filter
            }]
        }],
        "rowLimit": 100
    }
    response = client.service.searchanalytics().query(
        siteUrl=client.site_url,
        body=request
    ).execute()
    pages = []
    for row in response.get("rows", []):
        pages.append({
            "page": row["keys"][0],
            "clicks": row["clicks"],
            "impressions": row["impressions"],
            "ctr": row["ctr"],
            "position": row["position"]
        })
    return pages


def main():
    print(f"Connecting to GSC for {SITE_URL}...")
    client = GSCClient(CREDENTIALS_PATH, SITE_URL)

    # Verify access
    sites = client.list_sites()
    print(f"Accessible sites: {sites}")
    if SITE_URL not in sites and SITE_URL.rstrip("/") not in [s.rstrip("/") for s in sites]:
        # Try sc-domain variant
        print(f"WARNING: {SITE_URL} not in accessible sites. Trying sc-domain variant...")

    print(f"\nDate ranges:")
    print(f"  Before: {BEFORE_START} to {BEFORE_END} ({before_days} days)")
    print(f"  After:  {AFTER_START} to {AFTER_END} ({after_days} days)")

    # --- Page-level aggregates ---
    print("\n--- Fetching page-level data (before) ---")
    pages_before = get_page_level_data(client, "map", BEFORE_START, BEFORE_END)
    print(f"  Pages found: {len(pages_before)}")
    for p in pages_before:
        print(f"    {p['page']}: {p['clicks']} clicks, {p['impressions']} impr, CTR={p['ctr']:.4f}, pos={p['position']:.1f}")

    print("\n--- Fetching page-level data (after) ---")
    pages_after = get_page_level_data(client, "map", AFTER_START, AFTER_END)
    print(f"  Pages found: {len(pages_after)}")
    for p in pages_after:
        print(f"    {p['page']}: {p['clicks']} clicks, {p['impressions']} impr, CTR={p['ctr']:.4f}, pos={p['position']:.1f}")

    # --- Daily trend ---
    print("\n--- Fetching daily trend data ---")
    daily = get_daily_data(client, "map")
    print(f"  Days with data: {len(daily)}")

    daily_before = [d for d in daily if d["date"] < AFTER_START]
    daily_after = [d for d in daily if d["date"] >= AFTER_START]

    if daily_before:
        avg_clicks_before = sum(d["clicks"] for d in daily_before) / len(daily_before)
        avg_impr_before = sum(d["impressions"] for d in daily_before) / len(daily_before)
        avg_pos_before = sum(d["position"] for d in daily_before) / len(daily_before)
    else:
        avg_clicks_before = avg_impr_before = avg_pos_before = 0

    if daily_after:
        avg_clicks_after = sum(d["clicks"] for d in daily_after) / len(daily_after)
        avg_impr_after = sum(d["impressions"] for d in daily_after) / len(daily_after)
        avg_pos_after = sum(d["position"] for d in daily_after) / len(daily_after)
    else:
        avg_clicks_after = avg_impr_after = avg_pos_after = 0

    print(f"\n  Daily averages (before): {avg_clicks_before:.1f} clicks, {avg_impr_before:.1f} impr, pos {avg_pos_before:.1f}")
    print(f"  Daily averages (after):  {avg_clicks_after:.1f} clicks, {avg_impr_after:.1f} impr, pos {avg_pos_after:.1f}")

    # --- Query-level data ---
    print("\n--- Fetching query data (before) ---")
    queries_before = get_query_data(client, "map", BEFORE_START, BEFORE_END)
    print(f"  Queries: {len(queries_before)}")

    print("\n--- Fetching query data (after) ---")
    queries_after = get_query_data(client, "map", AFTER_START, AFTER_END)
    print(f"  Queries: {len(queries_after)}")

    # --- Build output JSON for report generation ---
    output = {
        "metadata": {
            "site": SITE_URL,
            "launch_date": LAUNCH_DATE,
            "before_period": {"start": BEFORE_START, "end": BEFORE_END, "days": before_days},
            "after_period": {"start": AFTER_START, "end": AFTER_END, "days": after_days},
            "data_pulled": datetime.now().isoformat()
        },
        "pages_before": pages_before,
        "pages_after": pages_after,
        "daily_trend": daily,
        "daily_averages": {
            "before": {
                "avg_clicks": round(avg_clicks_before, 2),
                "avg_impressions": round(avg_impr_before, 2),
                "avg_position": round(avg_pos_before, 2),
                "days_with_data": len(daily_before)
            },
            "after": {
                "avg_clicks": round(avg_clicks_after, 2),
                "avg_impressions": round(avg_impr_after, 2),
                "avg_position": round(avg_pos_after, 2),
                "days_with_data": len(daily_after)
            }
        },
        "queries_before": sorted(queries_before, key=lambda q: q["impressions"], reverse=True),
        "queries_after": sorted(queries_after, key=lambda q: q["impressions"], reverse=True),
    }

    # Compute top-level summary
    total_clicks_before = sum(p["clicks"] for p in pages_before)
    total_impr_before = sum(p["impressions"] for p in pages_before)
    total_clicks_after = sum(p["clicks"] for p in pages_after)
    total_impr_after = sum(p["impressions"] for p in pages_after)
    overall_ctr_before = total_clicks_before / total_impr_before if total_impr_before else 0
    overall_ctr_after = total_clicks_after / total_impr_after if total_impr_after else 0

    # Weighted avg position
    wavg_pos_before = sum(p["position"] * p["impressions"] for p in pages_before) / total_impr_before if total_impr_before else 0
    wavg_pos_after = sum(p["position"] * p["impressions"] for p in pages_after) / total_impr_after if total_impr_after else 0

    output["summary"] = {
        "before": {
            "total_clicks": total_clicks_before,
            "total_impressions": total_impr_before,
            "ctr": round(overall_ctr_before, 4),
            "weighted_avg_position": round(wavg_pos_before, 2),
            "clicks_per_day": round(total_clicks_before / before_days, 2),
            "impressions_per_day": round(total_impr_before / before_days, 2)
        },
        "after": {
            "total_clicks": total_clicks_after,
            "total_impressions": total_impr_after,
            "ctr": round(overall_ctr_after, 4),
            "weighted_avg_position": round(wavg_pos_after, 2),
            "clicks_per_day": round(total_clicks_after / after_days, 2),
            "impressions_per_day": round(total_impr_after / after_days, 2)
        }
    }

    outpath = Path("data/generated_content/map_impact_assessment.json")
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nFull data saved to {outpath}")
    print(json.dumps(output["summary"], indent=2))
    print(json.dumps(output["daily_averages"], indent=2))

    # Print top queries comparison
    print("\n=== TOP 15 QUERIES (BEFORE) ===")
    for q in output["queries_before"][:15]:
        print(f"  {q['query']}: {q['clicks']} clicks, {q['impressions']} impr, CTR={q['ctr']:.4f}, pos={q['position']:.1f}")

    print("\n=== TOP 15 QUERIES (AFTER) ===")
    for q in output["queries_after"][:15]:
        print(f"  {q['query']}: {q['clicks']} clicks, {q['impressions']} impr, CTR={q['ctr']:.4f}, pos={q['position']:.1f}")

    # Print daily data for sparkline-style view
    print("\n=== DAILY TREND ===")
    print(f"{'Date':<12} {'Clicks':>7} {'Impr':>7} {'CTR':>8} {'Pos':>6}")
    for d in daily:
        marker = " <<< LAUNCH" if d["date"] == LAUNCH_DATE else ""
        print(f"{d['date']:<12} {d['clicks']:>7} {d['impressions']:>7} {d['ctr']:>8.4f} {d['position']:>6.1f}{marker}")


if __name__ == "__main__":
    main()
