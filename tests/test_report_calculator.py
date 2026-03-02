# test_report_calculator.py

"""
Comprehensive test and demonstration of ReportCalculator functionality
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.report_calculator import ReportCalculator, PeriodType
import json


def format_currency(amount):
    """Format amount as AED currency"""
    if amount is None:
        return "N/A"
    return f"AED {amount:,.2f}"


def format_number(num):
    """Format number with commas"""
    if num is None:
        return "N/A"
    return f"{num:,}"


def format_percentage(pct):
    """Format percentage"""
    if pct is None:
        return "N/A"
    return f"{pct:.1f}%"


def test_monthly_metrics():
    """Test 1: Monthly metrics calculation"""
    print("\n" + "=" * 100)
    print("TEST 1: MONTHLY METRICS - November 2024")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        metrics = calculator.calculate_period_metrics(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11
        )

        print("\nPERIOD INFO:")
        print(f"  Period: {metrics['period_info']['period_type']} {metrics['period_info']['period_number']}/{metrics['period_info']['year']}")
        print(f"  Date Range: {metrics['period_info']['start_date']} to {metrics['period_info']['end_date']}")

        print("\nTRANSACTION METRICS:")
        tx = metrics['transaction_metrics']
        print(f"  Total Transactions: {format_number(tx['total_transactions'])}")
        print(f"  Total Sales Volume: {format_currency(tx['total_sales_volume'])}")
        print(f"  Average Transaction Size: {format_currency(tx['avg_transaction_size'])}")
        print(f"  Unique Areas: {format_number(tx['unique_areas'])}")
        print(f"  Unique Projects: {format_number(tx['unique_projects'])}")
        print(f"  Unique Developers: {format_number(tx['unique_developers'])}")

        print("\nPRICE METRICS:")
        price = metrics['price_metrics']
        print(f"  Average Price: {format_currency(price['avg_price'])}")
        print(f"  Median Price: {format_currency(price['median_price'])}")
        print(f"  Price Range: {format_currency(price['min_price'])} - {format_currency(price['max_price'])}")
        print(f"  Average Price per SQM: {format_currency(price['avg_price_per_sqm'])}")
        print(f"  Average Size: {price['avg_size_sqm']:.1f} sqm")

        print("\nMARKET SEGMENTS:")
        seg = metrics['market_segments']
        print(f"  Luxury Properties:")
        print(f"    Count: {format_number(seg['luxury']['count'])} ({format_percentage(seg['luxury']['percentage'])})")
        print(f"    Volume: {format_currency(seg['luxury']['volume'])}")
        print(f"    Avg Price: {format_currency(seg['luxury']['avg_price'])}")
        print(f"  Off-Plan Properties:")
        print(f"    Count: {format_number(seg['offplan']['count'])} ({format_percentage(seg['offplan']['percentage'])})")
        print(f"    Volume: {format_currency(seg['offplan']['volume'])}")
        print(f"    Avg Price: {format_currency(seg['offplan']['avg_price'])}")
        print(f"  Ready Properties:")
        print(f"    Count: {format_number(seg['ready']['count'])} ({format_percentage(seg['ready']['percentage'])})")
        print(f"    Volume: {format_currency(seg['ready']['volume'])}")
        print(f"    Avg Price: {format_currency(seg['ready']['avg_price'])}")

        print("\nPROPERTY TYPE DISTRIBUTION:")
        for pt in metrics['property_type_distribution']:
            print(f"  {pt['property_type']}: {format_number(pt['count'])} ({format_percentage(pt['percentage'])})")

    finally:
        calculator.close()


def test_quarterly_metrics():
    """Test 2: Quarterly metrics calculation"""
    print("\n" + "=" * 100)
    print("TEST 2: QUARTERLY METRICS - Q4 2024")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        metrics = calculator.calculate_period_metrics(
            year=2024,
            period_type=PeriodType.QUARTERLY,
            period_number=4
        )

        print(f"\nPeriod: Q{metrics['period_info']['period_number']} {metrics['period_info']['year']}")
        print(f"Date Range: {metrics['period_info']['start_date']} to {metrics['period_info']['end_date']}")
        print(f"\nTotal Transactions: {format_number(metrics['transaction_metrics']['total_transactions'])}")
        print(f"Total Sales Volume: {format_currency(metrics['transaction_metrics']['total_sales_volume'])}")
        print(f"Average Price: {format_currency(metrics['price_metrics']['avg_price'])}")
        print(f"Median Price: {format_currency(metrics['price_metrics']['median_price'])}")

    finally:
        calculator.close()


def test_yoy_comparison():
    """Test 3: Year-over-year comparison"""
    print("\n" + "=" * 100)
    print("TEST 3: YEAR-OVER-YEAR COMPARISON - Q4 2024 vs Q4 2023")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        comparison = calculator.get_comparison_metrics(
            current_year=2024,
            current_period_type=PeriodType.QUARTERLY,
            current_period_number=4,
            comparison_type="yoy"
        )

        print("\nCURRENT PERIOD (Q4 2024):")
        curr = comparison['current_period']
        print(f"  Transactions: {format_number(curr['transaction_metrics']['total_transactions'])}")
        print(f"  Sales Volume: {format_currency(curr['transaction_metrics']['total_sales_volume'])}")
        print(f"  Avg Price: {format_currency(curr['price_metrics']['avg_price'])}")

        print("\nCOMPARISON PERIOD (Q4 2023):")
        prev = comparison['comparison_period']
        print(f"  Transactions: {format_number(prev['transaction_metrics']['total_transactions'])}")
        print(f"  Sales Volume: {format_currency(prev['transaction_metrics']['total_sales_volume'])}")
        print(f"  Avg Price: {format_currency(prev['price_metrics']['avg_price'])}")

        print("\nCHANGES:")
        changes = comparison['changes']
        tx_change = changes['transaction_changes']['total_transactions']
        vol_change = changes['transaction_changes']['total_sales_volume']
        price_change = changes['price_changes']['avg_price']

        print(f"  Transaction Count: {tx_change['absolute']:+,.0f} ({format_percentage(tx_change['percentage'])})")
        print(f"  Sales Volume: {format_currency(vol_change['absolute'])} ({format_percentage(vol_change['percentage'])})")
        print(f"  Average Price: {format_currency(price_change['absolute'])} ({format_percentage(price_change['percentage'])})")

    finally:
        calculator.close()


def test_mom_comparison():
    """Test 4: Month-over-month comparison"""
    print("\n" + "=" * 100)
    print("TEST 4: MONTH-OVER-MONTH COMPARISON - November 2024 vs October 2024")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        comparison = calculator.get_comparison_metrics(
            current_year=2024,
            current_period_type=PeriodType.MONTHLY,
            current_period_number=11,
            comparison_type="mom"
        )

        print("\nCURRENT PERIOD (November 2024):")
        curr = comparison['current_period']
        print(f"  Transactions: {format_number(curr['transaction_metrics']['total_transactions'])}")
        print(f"  Avg Price: {format_currency(curr['price_metrics']['avg_price'])}")

        print("\nPREVIOUS PERIOD (October 2024):")
        prev = comparison['comparison_period']
        print(f"  Transactions: {format_number(prev['transaction_metrics']['total_transactions'])}")
        print(f"  Avg Price: {format_currency(prev['price_metrics']['avg_price'])}")

        print("\nCHANGES:")
        changes = comparison['changes']
        tx_change = changes['transaction_changes']['total_transactions']
        price_change = changes['price_changes']['avg_price']

        print(f"  Transaction Count: {tx_change['absolute']:+,.0f} ({format_percentage(tx_change['percentage'])})")
        print(f"  Average Price: {format_currency(price_change['absolute'])} ({format_percentage(price_change['percentage'])})")

    finally:
        calculator.close()


def test_top_performers():
    """Test 5: Top performers analysis"""
    print("\n" + "=" * 100)
    print("TEST 5: TOP PERFORMERS - November 2024")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        # Top areas by transaction count
        print("\nTOP 10 AREAS BY TRANSACTION COUNT:")
        top_areas = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            metric="transaction_count",
            category="areas",
            limit=10
        )

        for area in top_areas:
            print(f"\n{area['rank']}. {area['name']}")
            print(f"   Transactions: {format_number(area['transaction_count'])}")
            print(f"   Sales Volume: {format_currency(area['sales_volume'])}")
            print(f"   Avg Price: {format_currency(area['avg_price'])}")
            print(f"   Off-plan: {format_percentage(area['offplan_percentage'])}")
            print(f"   Luxury: {format_percentage(area['luxury_percentage'])}")

        # Top developers
        print("\n" + "-" * 100)
        print("TOP 10 DEVELOPERS BY TRANSACTION COUNT:")
        top_devs = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            metric="transaction_count",
            category="developers",
            limit=10
        )

        for dev in top_devs:
            print(f"{dev['rank']}. {dev['name']}: {format_number(dev['transaction_count'])} txs, "
                  f"Avg {format_currency(dev['avg_price'])}")

        # Top by sales volume
        print("\n" + "-" * 100)
        print("TOP 5 AREAS BY SALES VOLUME:")
        top_volume = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            metric="sales_volume",
            category="areas",
            limit=5
        )

        for area in top_volume:
            print(f"{area['rank']}. {area['name']}: {format_currency(area['sales_volume'])} "
                  f"({format_number(area['transaction_count'])} txs)")

    finally:
        calculator.close()


def test_area_filter():
    """Test 6: Area-specific analysis"""
    print("\n" + "=" * 100)
    print("TEST 6: AREA-SPECIFIC ANALYSIS - Dubai Marina (November 2024)")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        area_summary = calculator.get_area_summary(
            area_name="Dubai Marina",
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11
        )

        print("\nOVERALL METRICS:")
        print(f"  Transactions: {format_number(area_summary['transaction_metrics']['total_transactions'])}")
        print(f"  Sales Volume: {format_currency(area_summary['transaction_metrics']['total_sales_volume'])}")
        print(f"  Avg Price: {format_currency(area_summary['price_metrics']['avg_price'])}")
        print(f"  Median Price: {format_currency(area_summary['price_metrics']['median_price'])}")

        print("\nMARKET SEGMENTS:")
        seg = area_summary['market_segments']
        print(f"  Luxury: {format_percentage(seg['luxury']['percentage'])}")
        print(f"  Off-Plan: {format_percentage(seg['offplan']['percentage'])}")

        print("\nTOP PROJECTS:")
        for project in area_summary['top_projects'][:5]:
            print(f"  - {project['project_name']}")
            print(f"    Developer: {project['developer']}")
            print(f"    Transactions: {format_number(project['transaction_count'])}")
            print(f"    Avg Price: {format_currency(project['avg_price'])}")

    finally:
        calculator.close()


def test_time_series():
    """Test 7: Time series analysis"""
    print("\n" + "=" * 100)
    print("TEST 7: TIME SERIES ANALYSIS - Monthly Trend (Jan-Nov 2024)")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        time_series = calculator.get_time_series(
            start_year=2024,
            start_period=1,
            end_year=2024,
            end_period=11,
            period_type=PeriodType.MONTHLY
        )

        print("\nMONTH | TRANSACTIONS | AVG PRICE | VOLUME")
        print("-" * 80)

        for period in time_series:
            month = period['period_info']['period_number']
            tx_count = period['transaction_metrics']['total_transactions']
            avg_price = period['price_metrics']['avg_price']
            volume = period['transaction_metrics']['total_sales_volume']

            print(f"{month:4d}  | {tx_count:12,d} | {avg_price:13,.0f} | {volume:20,.0f}")

    finally:
        calculator.close()


def test_market_overview():
    """Test 8: Complete market overview"""
    print("\n" + "=" * 100)
    print("TEST 8: COMPLETE MARKET OVERVIEW - November 2024")
    print("=" * 100)

    calculator = ReportCalculator()
    try:
        overview = calculator.get_market_overview(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11
        )

        print("\nMARKET SUMMARY:")
        print(f"  Total Transactions: {format_number(overview['transaction_metrics']['total_transactions'])}")
        print(f"  Total Volume: {format_currency(overview['transaction_metrics']['total_sales_volume'])}")
        print(f"  Average Price: {format_currency(overview['price_metrics']['avg_price'])}")
        print(f"  Median Price: {format_currency(overview['price_metrics']['median_price'])}")

        print("\nTOP 5 AREAS:")
        for area in overview['top_areas'][:5]:
            print(f"  {area['rank']}. {area['name']}: {format_number(area['transaction_count'])} txs")

        print("\nTOP 5 DEVELOPERS:")
        for dev in overview['top_developers'][:5]:
            print(f"  {dev['rank']}. {dev['name']}: {format_number(dev['transaction_count'])} txs")

        print("\nTOP 5 PROJECTS:")
        for proj in overview['top_projects'][:5]:
            print(f"  {proj['rank']}. {proj['name']}: {format_number(proj['transaction_count'])} txs")

    finally:
        calculator.close()


def main():
    """Run all tests"""
    print("\n" + "=" * 100)
    print("DUBAI REAL ESTATE REPORT CALCULATOR - COMPREHENSIVE TEST SUITE")
    print("=" * 100)

    test_monthly_metrics()
    test_quarterly_metrics()
    test_yoy_comparison()
    test_mom_comparison()
    test_top_performers()
    test_area_filter()
    test_time_series()
    test_market_overview()

    print("\n" + "=" * 100)
    print("ALL TESTS COMPLETED SUCCESSFULLY!")
    print("=" * 100)


if __name__ == "__main__":
    main()
