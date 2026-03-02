#!/usr/bin/env python3
"""
API Endpoint Testing Script
Tests all Supply Intelligence endpoints with fresh metrics data
"""

import requests
import json

BASE_URL = "http://localhost:8001"

def test_endpoint(name, url, show_sample=True):
    """Test an API endpoint and show results"""
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"URL: {url}")
    print('='*60)

    try:
        response = requests.get(url, timeout=10)

        if response.status_code == 200:
            data = response.json()
            print(f"[PASS] Status: 200 OK")

            if show_sample:
                print(f"\nSample Data:")
                print(json.dumps(data, indent=2)[:500] + "...")

            return True
        else:
            print(f"[FAIL] Status: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False

    except Exception as e:
        print(f"[ERROR] {e}")
        return False

def main():
    print("\n" + "=" * 60)
    print("SUPPLY INTELLIGENCE API - ENDPOINT TESTING")
    print("=" * 60)

    tests = [
        ("Supply Overview", f"{BASE_URL}/api/supply/overview"),
        ("Business Bay Area Intelligence", f"{BASE_URL}/api/supply/area/Business%20Bay"),
        ("Dubai Marina Area Intelligence", f"{BASE_URL}/api/supply/area/Dubai%20Marina"),
        ("Slightly Oversupplied Areas (Interactive Chart)", f"{BASE_URL}/api/supply/areas-by-balance?balance=Slightly%20Oversupplied"),
        ("Investment Opportunities", f"{BASE_URL}/api/supply/opportunities"),
        ("Developer Reliability", f"{BASE_URL}/api/supply/developers/reliability"),
        ("Delivery Forecast", f"{BASE_URL}/api/supply/forecast"),
        ("Market Alerts", f"{BASE_URL}/api/supply/alerts"),
    ]

    results = []
    for name, url in tests:
        passed = test_endpoint(name, url, show_sample=False)
        results.append((name, passed))

    # Detailed validation for critical endpoints
    print("\n" + "=" * 60)
    print("DETAILED DATA VALIDATION")
    print("=" * 60)

    # Test Business Bay metrics
    print("\n[Business Bay Metrics]")
    bb_response = requests.get(f"{BASE_URL}/api/supply/area/Business%20Bay").json()
    metrics = bb_response.get('supply_demand_metrics', {})
    print(f"  Offplan Transactions (2024+2025): {metrics.get('demand_offplan_tx', 'N/A'):,}")
    print(f"  Supply-Demand Ratio: {metrics.get('supply_demand_ratio', 'N/A'):.2f}")
    print(f"  Market Balance: {metrics.get('market_balance', 'N/A')}")
    print(f"  Price YoY Change: {metrics.get('price_yoy_change_pct', 'N/A'):.1f}%")
    print(f"  Transaction YoY Change: {metrics.get('tx_yoy_change_pct', 'N/A'):.1f}%")

    # Validate against expected benchmark
    expected_offplan = 14440  # From verified Excel (2024: 6313 + 2025: 8127)
    actual_offplan = metrics.get('demand_offplan_tx', 0)

    if abs(actual_offplan - expected_offplan) <= 5:  # Allow small variance
        print(f"  [PASS] Offplan TX matches expected: {expected_offplan:,}")
    else:
        print(f"  [WARN] Offplan TX variance: Expected {expected_offplan:,}, Got {actual_offplan:,}")

    # Test interactive chart endpoint
    print("\n[Interactive Chart - Slightly Oversupplied]")
    chart_response = requests.get(f"{BASE_URL}/api/supply/areas-by-balance?balance=Slightly%20Oversupplied").json()
    print(f"  Total Areas: {chart_response.get('total_areas', 'N/A')}")
    print(f"  Top 3 Areas:")
    for i, area in enumerate(chart_response.get('areas', [])[:3], 1):
        print(f"    {i}. {area['area']}")
        print(f"       - SD Ratio: {area.get('supply_demand_ratio', 'N/A'):.2f}")
        print(f"       - Offplan TX: {area.get('demand_offplan_tx', 'N/A'):,}")
        print(f"       - Opportunity Score: {area.get('opportunity_score', 'N/A')}")
        print(f"       - Investment Timing: {area.get('investment_timing', 'N/A')}")

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, p in results if p)
    total = len(results)

    print(f"\nEndpoints Tested: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {total - passed}")

    if passed == total:
        print("\n[SUCCESS] All endpoints operational with fresh metrics!")
    else:
        print("\n[WARNING] Some endpoints failed - check details above")

    print("\n" + "=" * 60)
    print("Ready for frontend testing!")
    print("Open: frontend/index.html or http://localhost:8080")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    main()
