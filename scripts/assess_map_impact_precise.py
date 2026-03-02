"""
Precise assessment for a single /map page (excludes gmap UTM links and maple pages).
"""

import sys
import json
from pathlib import Path
from datetime import datetime, timedelta

import os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, str(Path(__file__).parent.parent))
from src.analytics.gsc_client import GSCClient

CREDENTIALS_PATH = os.getenv("GSC_CREDENTIALS_PATH", "credentials/gsc-service-account.json")
SITE_URL = os.getenv("GSC_SITE_URL", "https://your-site.com/")
LAUNCH_DATE = "2026-01-23"
DATA_END = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
BEFORE_START = "2025-12-24"
BEFORE_END = "2026-01-22"
AFTER_START = "2026-01-23"
AFTER_END = DATA_END
MAP_URL = os.getenv("GSC_SITE_URL", "https://your-site.com/").rstrip("/") + "/map"

before_days = (datetime.strptime(BEFORE_END, "%Y-%m-%d") - datetime.strptime(BEFORE_START, "%Y-%m-%d")).days + 1
after_days = (datetime.strptime(AFTER_END, "%Y-%m-%d") - datetime.strptime(AFTER_START, "%Y-%m-%d")).days + 1


def query_gsc(client, dimensions, start_date, end_date, page_url=None):
    body = {
        "startDate": start_date,
        "endDate": end_date,
        "dimensions": dimensions,
        "rowLimit": 1000
    }
    if page_url:
        body["dimensionFilterGroups"] = [{
            "filters": [{
                "dimension": "page",
                "operator": "equals",
                "expression": page_url
            }]
        }]
    response = client.service.searchanalytics().query(
        siteUrl=client.site_url, body=body
    ).execute()
    return response.get("rows", [])


def main():
    client = GSCClient(CREDENTIALS_PATH, SITE_URL)
    print(f"Periods: Before {BEFORE_START} to {BEFORE_END} ({before_days}d), After {AFTER_START} to {AFTER_END} ({after_days}d)")
    print(f"Filtering to exact URL: {MAP_URL}\n")

    # --- Daily trend for /map only ---
    print("=== DAILY TREND (site /map page only) ===")
    daily_rows = query_gsc(client, ["date"], BEFORE_START, AFTER_END, MAP_URL)
    daily = []
    for r in daily_rows:
        daily.append({
            "date": r["keys"][0], "clicks": r["clicks"],
            "impressions": r["impressions"], "ctr": r["ctr"], "position": r["position"]
        })
    daily.sort(key=lambda d: d["date"])

    daily_before = [d for d in daily if d["date"] < AFTER_START]
    daily_after = [d for d in daily if d["date"] >= AFTER_START]

    print(f"{'Date':<12} {'Clicks':>7} {'Impr':>7} {'CTR':>8} {'Pos':>6}")
    for d in daily:
        marker = " <<< LAUNCH" if d["date"] == LAUNCH_DATE else ""
        print(f"{d['date']:<12} {d['clicks']:>7} {d['impressions']:>7} {d['ctr']:>8.4f} {d['position']:>6.1f}{marker}")

    # Aggregates
    def agg(rows):
        if not rows:
            return {"clicks": 0, "impressions": 0, "avg_ctr": 0, "avg_pos": 0,
                    "clicks_per_day": 0, "impressions_per_day": 0}
        tc = sum(r["clicks"] for r in rows)
        ti = sum(r["impressions"] for r in rows)
        return {
            "clicks": tc,
            "impressions": ti,
            "avg_ctr": round(tc / ti, 4) if ti else 0,
            "avg_pos": round(sum(r["position"] * r["impressions"] for r in rows) / ti, 2) if ti else 0,
            "clicks_per_day": round(tc / len(rows), 2),
            "impressions_per_day": round(ti / len(rows), 2),
            "days": len(rows)
        }

    before_agg = agg(daily_before)
    after_agg = agg(daily_after)

    print(f"\n=== AGGREGATE COMPARISON (site /map page only) ===")
    print(f"{'Metric':<25} {'Before':>12} {'After':>12} {'Change':>12}")
    print("-" * 65)

    def pct(b, a):
        if b == 0:
            return "N/A"
        return f"{((a - b) / b) * 100:+.1f}%"

    print(f"{'Total Clicks':<25} {before_agg['clicks']:>12} {after_agg['clicks']:>12} {pct(before_agg['clicks'], after_agg['clicks']):>12}")
    print(f"{'Total Impressions':<25} {before_agg['impressions']:>12} {after_agg['impressions']:>12} {pct(before_agg['impressions'], after_agg['impressions']):>12}")
    print(f"{'Clicks/Day':<25} {before_agg['clicks_per_day']:>12} {after_agg['clicks_per_day']:>12} {pct(before_agg['clicks_per_day'], after_agg['clicks_per_day']):>12}")
    print(f"{'Impressions/Day':<25} {before_agg['impressions_per_day']:>12} {after_agg['impressions_per_day']:>12} {pct(before_agg['impressions_per_day'], after_agg['impressions_per_day']):>12}")
    print(f"{'CTR':<25} {before_agg['avg_ctr']:>12.4f} {after_agg['avg_ctr']:>12.4f} {pct(before_agg['avg_ctr'], after_agg['avg_ctr']):>12}")
    print(f"{'Avg Position':<25} {before_agg['avg_pos']:>12.2f} {after_agg['avg_pos']:>12.2f} {pct(before_agg['avg_pos'], after_agg['avg_pos']):>12}")

    # --- Query data ---
    print(f"\n=== TOP QUERIES - BEFORE ({BEFORE_START} to {BEFORE_END}) ===")
    q_before = query_gsc(client, ["query"], BEFORE_START, BEFORE_END, MAP_URL)
    q_before.sort(key=lambda r: r["impressions"], reverse=True)
    print(f"{'Query':<45} {'Clicks':>7} {'Impr':>7} {'CTR':>8} {'Pos':>6}")
    for r in q_before[:25]:
        print(f"{r['keys'][0]:<45} {r['clicks']:>7} {r['impressions']:>7} {r['ctr']:>8.4f} {r['position']:>6.1f}")

    print(f"\n=== TOP QUERIES - AFTER ({AFTER_START} to {AFTER_END}) ===")
    q_after = query_gsc(client, ["query"], AFTER_START, AFTER_END, MAP_URL)
    q_after.sort(key=lambda r: r["impressions"], reverse=True)
    print(f"{'Query':<45} {'Clicks':>7} {'Impr':>7} {'CTR':>8} {'Pos':>6}")
    for r in q_after[:25]:
        print(f"{r['keys'][0]:<45} {r['clicks']:>7} {r['impressions']:>7} {r['ctr']:>8.4f} {r['position']:>6.1f}")

    # --- Query comparison (same queries appearing in both periods) ---
    before_map = {r["keys"][0]: r for r in q_before}
    after_map = {r["keys"][0]: r for r in q_after}
    common = set(before_map.keys()) & set(after_map.keys())

    print(f"\n=== QUERY POSITION CHANGES (common queries, sorted by position improvement) ===")
    changes = []
    for q in common:
        b = before_map[q]
        a = after_map[q]
        pos_delta = a["position"] - b["position"]
        changes.append({
            "query": q,
            "pos_before": b["position"], "pos_after": a["position"], "pos_delta": pos_delta,
            "ctr_before": b["ctr"], "ctr_after": a["ctr"],
            "impr_before": b["impressions"], "impr_after": a["impressions"],
            "clicks_before": b["clicks"], "clicks_after": a["clicks"]
        })
    changes.sort(key=lambda c: c["pos_delta"])

    print(f"{'Query':<40} {'Pos Before':>10} {'Pos After':>10} {'Delta':>7} {'CTR Bef':>8} {'CTR Aft':>8}")
    for c in changes[:20]:
        print(f"{c['query']:<40} {c['pos_before']:>10.1f} {c['pos_after']:>10.1f} {c['pos_delta']:>+7.1f} {c['ctr_before']:>8.4f} {c['ctr_after']:>8.4f}")

    print(f"\n=== QUERIES THAT WORSENED ===")
    worsened = [c for c in changes if c["pos_delta"] > 2]
    worsened.sort(key=lambda c: c["pos_delta"], reverse=True)
    for c in worsened[:10]:
        print(f"{c['query']:<40} {c['pos_before']:>10.1f} {c['pos_after']:>10.1f} {c['pos_delta']:>+7.1f} {c['ctr_before']:>8.4f} {c['ctr_after']:>8.4f}")

    # New queries appearing only in after period
    new_queries = set(after_map.keys()) - set(before_map.keys())
    print(f"\n=== NEW QUERIES (appearing only after launch): {len(new_queries)} total ===")
    new_sorted = sorted([after_map[q] for q in new_queries], key=lambda r: r["impressions"], reverse=True)
    for r in new_sorted[:15]:
        print(f"  {r['keys'][0]:<45} {r['clicks']:>5} clicks, {r['impressions']:>5} impr, pos {r['position']:.1f}")

    # Lost queries (only in before)
    lost_queries = set(before_map.keys()) - set(after_map.keys())
    print(f"\n=== LOST QUERIES (only in before period): {len(lost_queries)} total ===")
    lost_sorted = sorted([before_map[q] for q in lost_queries], key=lambda r: r["impressions"], reverse=True)
    for r in lost_sorted[:15]:
        print(f"  {r['keys'][0]:<45} {r['clicks']:>5} clicks, {r['impressions']:>5} impr, pos {r['position']:.1f}")

    # Save all data
    output = {
        "metadata": {
            "page": MAP_URL, "launch_date": LAUNCH_DATE,
            "before": {"start": BEFORE_START, "end": BEFORE_END, "days": before_days},
            "after": {"start": AFTER_START, "end": AFTER_END, "days": after_days},
        },
        "before_aggregate": before_agg,
        "after_aggregate": after_agg,
        "daily_trend": daily,
        "queries_before": [{
            "query": r["keys"][0], "clicks": r["clicks"], "impressions": r["impressions"],
            "ctr": r["ctr"], "position": r["position"]
        } for r in q_before],
        "queries_after": [{
            "query": r["keys"][0], "clicks": r["clicks"], "impressions": r["impressions"],
            "ctr": r["ctr"], "position": r["position"]
        } for r in q_after],
        "query_changes": changes,
    }
    outpath = Path("data/generated_content/map_impact_precise.json")
    outpath.parent.mkdir(parents=True, exist_ok=True)
    with open(outpath, "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nData saved to {outpath}")


if __name__ == "__main__":
    main()
