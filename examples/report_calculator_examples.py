# examples/report_calculator_examples.py

"""
Simple examples for using the ReportCalculator
Copy and modify these examples for your use case
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.report_calculator import ReportCalculator, PeriodType


# ============================================================================
# EXAMPLE 1: Get metrics for a specific month
# ============================================================================

def example_monthly_metrics():
    """Get all metrics for November 2024"""
    calculator = ReportCalculator()
    try:
        metrics = calculator.calculate_period_metrics(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11
        )

        # Print key metrics
        print("November 2024 Metrics:")
        print(f"  Transactions: {metrics['transaction_metrics']['total_transactions']:,}")
        print(f"  Total Volume: AED {metrics['transaction_metrics']['total_sales_volume']:,.0f}")
        print(f"  Avg Price: AED {metrics['price_metrics']['avg_price']:,.2f}")
        print(f"  Median Price: AED {metrics['price_metrics']['median_price']:,.2f}")
        print(f"  Luxury %: {metrics['market_segments']['luxury']['percentage']:.1f}%")
        print(f"  Off-Plan %: {metrics['market_segments']['offplan']['percentage']:.1f}%")

        return metrics

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 2: Compare this month vs last month
# ============================================================================

def example_month_over_month():
    """Compare November 2024 vs October 2024"""
    calculator = ReportCalculator()
    try:
        comparison = calculator.get_comparison_metrics(
            current_year=2024,
            current_period_type=PeriodType.MONTHLY,
            current_period_number=11,
            comparison_type="mom"
        )

        changes = comparison['changes']
        print("\nMonth-over-Month Changes (Nov vs Oct 2024):")

        tx_change = changes['transaction_changes']['total_transactions']
        print(f"  Transactions: {tx_change['absolute']:+,} ({tx_change['percentage']:+.1f}%)")

        price_change = changes['price_changes']['avg_price']
        print(f"  Avg Price: AED {price_change['absolute']:+,.0f} ({price_change['percentage']:+.1f}%)")

        return comparison

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 3: Year-over-year comparison
# ============================================================================

def example_year_over_year():
    """Compare Q4 2024 vs Q4 2023"""
    calculator = ReportCalculator()
    try:
        comparison = calculator.get_comparison_metrics(
            current_year=2024,
            current_period_type=PeriodType.QUARTERLY,
            current_period_number=4,
            comparison_type="yoy"
        )

        changes = comparison['changes']
        print("\nYear-over-Year Changes (Q4 2024 vs Q4 2023):")

        tx_change = changes['transaction_changes']['total_transactions']
        print(f"  Transactions: {tx_change['absolute']:+,} ({tx_change['percentage']:+.1f}%)")

        vol_change = changes['transaction_changes']['total_sales_volume']
        print(f"  Sales Volume: AED {vol_change['absolute']:+,.0f} ({vol_change['percentage']:+.1f}%)")

        price_change = changes['price_changes']['avg_price']
        print(f"  Avg Price: AED {price_change['absolute']:+,.0f} ({price_change['percentage']:+.1f}%)")

        return comparison

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 4: Top performing areas
# ============================================================================

def example_top_areas():
    """Get top 10 areas by transaction count"""
    calculator = ReportCalculator()
    try:
        top_areas = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            metric="transaction_count",
            category="areas",
            limit=10
        )

        print("\nTop 10 Areas (November 2024):")
        for area in top_areas:
            print(f"  {area['rank']:2d}. {area['name']:<30} "
                  f"{area['transaction_count']:4,} txs, "
                  f"Avg: AED {area['avg_price']:>12,.0f}, "
                  f"Off-plan: {area['offplan_percentage']:5.1f}%")

        return top_areas

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 5: Top developers
# ============================================================================

def example_top_developers():
    """Get top 10 developers by transaction count"""
    calculator = ReportCalculator()
    try:
        top_devs = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            metric="transaction_count",
            category="developers",
            limit=10
        )

        print("\nTop 10 Developers (November 2024):")
        for dev in top_devs:
            print(f"  {dev['rank']:2d}. {dev['name']:<30} "
                  f"{dev['transaction_count']:4,} txs, "
                  f"Avg: AED {dev['avg_price']:>12,.0f}")

        return top_devs

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 6: Analyze a specific area
# ============================================================================

def example_area_analysis(area_name="Dubai Marina"):
    """Deep dive into a specific area"""
    calculator = ReportCalculator()
    try:
        area_summary = calculator.get_area_summary(
            area_name=area_name,
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11
        )

        print(f"\n{area_name} Analysis (November 2024):")
        print(f"  Transactions: {area_summary['transaction_metrics']['total_transactions']:,}")
        print(f"  Total Volume: AED {area_summary['transaction_metrics']['total_sales_volume']:,.0f}")
        print(f"  Avg Price: AED {area_summary['price_metrics']['avg_price']:,.2f}")
        print(f"  Median Price: AED {area_summary['price_metrics']['median_price']:,.2f}")
        print(f"  Luxury %: {area_summary['market_segments']['luxury']['percentage']:.1f}%")
        print(f"  Off-Plan %: {area_summary['market_segments']['offplan']['percentage']:.1f}%")

        print(f"\n  Top Projects in {area_name}:")
        for i, project in enumerate(area_summary['top_projects'][:5], 1):
            print(f"    {i}. {project['project_name']}")
            print(f"       {project['transaction_count']} txs, Avg: AED {project['avg_price']:,.0f}")

        return area_summary

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 7: Monthly trend for the year
# ============================================================================

def example_monthly_trend():
    """Get monthly transaction trend for 2024"""
    calculator = ReportCalculator()
    try:
        time_series = calculator.get_time_series(
            start_year=2024,
            start_period=1,
            end_year=2024,
            end_period=11,
            period_type=PeriodType.MONTHLY
        )

        print("\n2024 Monthly Trend:")
        print("  Month | Transactions |   Avg Price   |    Total Volume")
        print("  ------|--------------|---------------|------------------")

        for period in time_series:
            month = period['period_info']['period_number']
            tx = period['transaction_metrics']['total_transactions']
            avg = period['price_metrics']['avg_price']
            vol = period['transaction_metrics']['total_sales_volume']

            print(f"    {month:2d}  |   {tx:8,}   | AED {avg:9,.0f} | AED {vol:14,.0f}")

        return time_series

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 8: Filter by property type
# ============================================================================

def example_property_type_filter():
    """Get metrics for villas only"""
    calculator = ReportCalculator()
    try:
        villa_metrics = calculator.calculate_period_metrics(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            property_type_filter="Villa"
        )

        print("\nVilla Market (November 2024):")
        print(f"  Transactions: {villa_metrics['transaction_metrics']['total_transactions']:,}")
        print(f"  Avg Price: AED {villa_metrics['price_metrics']['avg_price']:,.2f}")
        print(f"  Median Price: AED {villa_metrics['price_metrics']['median_price']:,.2f}")
        print(f"  Avg Size: {villa_metrics['price_metrics']['avg_size_sqm']:.1f} sqm")
        print(f"  Avg Price/SQM: AED {villa_metrics['price_metrics']['avg_price_per_sqm']:,.2f}")

        return villa_metrics

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 9: Filter by area and property type
# ============================================================================

def example_combined_filters():
    """Get metrics for villas in Palm Jumeirah"""
    calculator = ReportCalculator()
    try:
        metrics = calculator.calculate_period_metrics(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            area_filter="Palm Jumeirah",
            property_type_filter="Villa"
        )

        print("\nPalm Jumeirah Villas (November 2024):")
        print(f"  Transactions: {metrics['transaction_metrics']['total_transactions']:,}")
        print(f"  Avg Price: AED {metrics['price_metrics']['avg_price']:,.2f}")
        print(f"  Median Price: AED {metrics['price_metrics']['median_price']:,.2f}")
        print(f"  Price Range: AED {metrics['price_metrics']['min_price']:,.0f} - "
              f"AED {metrics['price_metrics']['max_price']:,.0f}")

        return metrics

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 10: Areas with highest price growth
# ============================================================================

def example_price_growth():
    """Find areas with highest price growth (Nov 2024 vs Oct 2024)"""
    calculator = ReportCalculator()
    try:
        growth_leaders = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11,
            metric="price_growth",
            category="areas",
            limit=10
        )

        print("\nTop 10 Areas by Price Growth (Nov vs Oct 2024):")
        for area in growth_leaders:
            print(f"  {area['rank']:2d}. {area['name']:<30}")
            print(f"      Growth: {area['price_growth_percentage']:+6.1f}% "
                  f"(AED {area['price_growth_absolute']:+12,.0f})")
            print(f"      Current: AED {area['current_avg_price']:,.0f}, "
                  f"Previous: AED {area['previous_avg_price']:,.0f}")

        return growth_leaders

    finally:
        calculator.close()


# ============================================================================
# EXAMPLE 11: Complete market overview
# ============================================================================

def example_market_overview():
    """Get complete market overview with top performers"""
    calculator = ReportCalculator()
    try:
        overview = calculator.get_market_overview(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=11
        )

        print("\nMarket Overview (November 2024):")
        print(f"  Total Transactions: {overview['transaction_metrics']['total_transactions']:,}")
        print(f"  Total Volume: AED {overview['transaction_metrics']['total_sales_volume']:,.0f}")
        print(f"  Avg Price: AED {overview['price_metrics']['avg_price']:,.2f}")
        print(f"  Median Price: AED {overview['price_metrics']['median_price']:,.2f}")

        print("\n  Top 5 Areas:")
        for area in overview['top_areas'][:5]:
            print(f"    {area['rank']}. {area['name']}: {area['transaction_count']:,} txs")

        print("\n  Top 5 Developers:")
        for dev in overview['top_developers'][:5]:
            print(f"    {dev['rank']}. {dev['name']}: {dev['transaction_count']:,} txs")

        return overview

    finally:
        calculator.close()


# ============================================================================
# Run all examples
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("REPORT CALCULATOR EXAMPLES")
    print("=" * 80)

    example_monthly_metrics()
    example_month_over_month()
    example_year_over_year()
    example_top_areas()
    example_top_developers()
    example_area_analysis("Dubai Marina")
    example_monthly_trend()
    example_property_type_filter()
    example_combined_filters()
    example_price_growth()
    example_market_overview()

    print("\n" + "=" * 80)
    print("ALL EXAMPLES COMPLETED")
    print("=" * 80)
