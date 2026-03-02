"""
Comprehensive Supply Intelligence Endpoints Test
Tests all 9 supply intelligence endpoints with NaN fixes
"""
import requests
import json

API_BASE = "http://localhost:8001"

def test_endpoint(name, url, expected_status=200):
    """Test a single endpoint"""
    try:
        response = requests.get(url, timeout=10)
        status = response.status_code

        if status == expected_status:
            # Try to parse JSON to verify it's valid
            try:
                data = response.json()
                print(f"[PASS] {name} ({status})")
                return True, None
            except json.JSONDecodeError as e:
                print(f"[FAIL] {name} ({status}) - Invalid JSON: {e}")
                return False, f"Invalid JSON: {e}"
        else:
            error_msg = response.text[:200] if response.text else "No error message"
            print(f"[FAIL] {name} ({status}) - {error_msg}")
            return False, error_msg
    except requests.exceptions.RequestException as e:
        print(f"[FAIL] {name} - Connection error: {e}")
        return False, str(e)

def main():
    print("=" * 70)
    print("SUPPLY INTELLIGENCE ENDPOINTS - COMPREHENSIVE TEST")
    print("Testing all 9 endpoints with NaN fixes applied")
    print("=" * 70)
    print()

    tests = [
        ("Supply Overview", f"{API_BASE}/api/supply/overview"),
        ("Oversaturated Areas", f"{API_BASE}/api/supply/oversaturated?threshold=3.0"),
        ("Investment Opportunities", f"{API_BASE}/api/supply/opportunities?limit=10"),
        ("Arbitrage Opportunities", f"{API_BASE}/api/supply/arbitrage"),
        ("Emerging Hotspots", f"{API_BASE}/api/supply/emerging-hotspots"),
        ("Developer Reliability", f"{API_BASE}/api/supply/developers/reliability?limit=20"),
        ("Delivery Forecast", f"{API_BASE}/api/supply/forecast?start_quarter=Q1%202026&quarters=8"),
        ("Area Intelligence", f"{API_BASE}/api/supply/area/Dubai%20Marina"),
        ("Market Alerts", f"{API_BASE}/api/supply/alerts"),
    ]

    results = []
    for name, url in tests:
        passed, error = test_endpoint(name, url)
        results.append((name, passed, error))
        print()

    # Summary
    print("=" * 70)
    passed_count = sum(1 for _, passed, _ in results if passed)
    failed_count = len(results) - passed_count
    print(f"RESULTS: {passed_count} passed, {failed_count} failed out of {len(results)} tests")
    print("=" * 70)
    print()

    if failed_count > 0:
        print("FAILED ENDPOINTS:")
        for name, passed, error in results:
            if not passed:
                print(f"  - {name}: {error}")
        print()

    if passed_count == len(results):
        print("SUCCESS! All supply intelligence endpoints are working correctly.")
        print("The NaN fixes have been successfully applied.")
    else:
        print(f"WARNING: {failed_count} endpoint(s) still failing.")
        print("Check the error messages above for details.")

if __name__ == "__main__":
    main()
