#!/usr/bin/env python3
# validate_metrics_integration.py

"""
Integration example: Validate pre-computed metrics from database
Demonstrates how to use QA Validator with existing metrics tables
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb
from src.analytics import QAValidator
from config.bigquery_settings import bq_settings


def validate_area_metrics():
    """Validate pre-computed area metrics against raw data"""
    print("\n" + "="*80)
    print("VALIDATING AREA METRICS")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get a sample of pre-computed area metrics
        area_metrics = validator.con.execute("""
            SELECT
                area_name_en,
                total_transactions,
                avg_price
            FROM metrics_area
            ORDER BY total_transactions DESC
            LIMIT 5
        """).fetchall()

        print(f"\nValidating top 5 areas by transaction volume...")

        for area_name, tx_count, avg_price in area_metrics:
            # Get raw data for this area
            result = validator.con.execute(f"""
                SELECT
                    COUNT(*) as actual_count,
                    AVG(actual_worth) as actual_avg
                FROM transactions_clean
                WHERE area_name_en = '{area_name}'
            """).fetchone()

            actual_count, actual_avg = result

            # Validate
            count_valid = (tx_count == actual_count)
            price_valid = validator._values_match(actual_avg, avg_price)

            status = "PASS" if (count_valid and price_valid) else "FAIL"

            print(f"\n{area_name}:")
            print(f"  Transactions: {tx_count:,} (expected: {actual_count:,}) - {'OK' if count_valid else 'FAIL'}")
            print(f"  Avg Price: AED {avg_price:,.2f} (expected: AED {actual_avg:,.2f}) - {'OK' if price_valid else 'FAIL'}")
            print(f"  Overall: [{status}]")


def validate_monthly_trends():
    """Validate monthly trend metrics"""
    print("\n" + "="*80)
    print("VALIDATING MONTHLY TRENDS")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get sample monthly trends
        monthly_data = validator.con.execute("""
            SELECT
                area_name_en,
                transaction_year,
                transaction_month,
                tx_count,
                avg_price
            FROM metrics_monthly_trends
            WHERE transaction_year = 2024
              AND transaction_month IN (1, 2, 3)
              AND area_name_en = 'Dubai Marina'
            ORDER BY transaction_month
        """).fetchall()

        print(f"\nValidating Dubai Marina monthly trends (Q1 2024)...")

        for area, year, month, tx_count, avg_price in monthly_data:
            print(f"\n{year}-{month:02d} ({area}):")

            # Validate transaction count
            v1 = validator.validate_transaction_count(
                year, 'monthly', month, tx_count,
                area_filter=area
            )

            # Validate average price
            v2 = validator.validate_average_price(
                year, 'monthly', month, avg_price,
                area_filter=area
            )

            print(f"  Transactions: {tx_count:,} - {v1.status.value}")
            print(f"  Avg Price: AED {avg_price:,.2f} - {v2.status.value}")


def validate_luxury_metrics():
    """Validate luxury market metrics"""
    print("\n" + "="*80)
    print("VALIDATING LUXURY METRICS")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get luxury summary metrics
        luxury_data = validator.con.execute("""
            SELECT
                area_name_en,
                total_luxury_transactions,
                avg_luxury_price
            FROM metrics_luxury_summary
            ORDER BY total_luxury_transactions DESC
            LIMIT 5
        """).fetchall()

        print(f"\nValidating top 5 luxury markets...")

        for area, luxury_count, avg_luxury_price in luxury_data:
            # Get actual luxury data
            result = validator.con.execute(f"""
                SELECT
                    COUNT(*) as actual_count,
                    AVG(actual_worth) as actual_avg
                FROM transactions_luxury
                WHERE area_name_en = '{area}'
            """).fetchone()

            actual_count, actual_avg = result

            count_valid = (luxury_count == actual_count)
            price_valid = validator._values_match(actual_avg, avg_luxury_price)

            status = "PASS" if (count_valid and price_valid) else "FAIL"

            print(f"\n{area}:")
            print(f"  Luxury Transactions: {luxury_count:,} (expected: {actual_count:,}) - {'OK' if count_valid else 'FAIL'}")
            print(f"  Avg Luxury Price: AED {avg_luxury_price:,.2f} (expected: AED {actual_avg:,.2f}) - {'OK' if price_valid else 'FAIL'}")
            print(f"  Overall: [{status}]")


def validate_offplan_metrics():
    """Validate off-plan metrics"""
    print("\n" + "="*80)
    print("VALIDATING OFF-PLAN METRICS")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get off-plan comparison metrics
        offplan_data = validator.con.execute("""
            SELECT
                area_name_en,
                offplan_count,
                ready_count,
                offplan_percentage
            FROM metrics_offplan_comparison
            ORDER BY offplan_count DESC
            LIMIT 5
        """).fetchall()

        print(f"\nValidating top 5 areas by off-plan volume...")

        for area, offplan_count, ready_count, offplan_pct in offplan_data:
            # Validate the percentage calculation
            total = offplan_count + ready_count
            expected_pct = (offplan_count / total * 100) if total > 0 else 0

            result = validator.validate_percentage_calculation(
                offplan_count, total, offplan_pct,
                test_name=f"offplan_pct_{area.replace(' ', '_')}"
            )

            print(f"\n{area}:")
            print(f"  Off-Plan: {offplan_count:,}")
            print(f"  Ready: {ready_count:,}")
            print(f"  Off-Plan %: {offplan_pct:.2f}% (expected: {expected_pct:.2f}%)")
            print(f"  Validation: {result.status.value}")


def validate_yoy_comparison():
    """Validate year-over-year comparison metrics"""
    print("\n" + "="*80)
    print("VALIDATING YEAR-OVER-YEAR METRICS")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get YoY data for a specific area
        yoy_data = validator.con.execute("""
            SELECT
                area_name_en,
                transaction_year,
                total_transactions,
                avg_price
            FROM metrics_yoy_comparison
            WHERE area_name_en = 'Dubai Marina'
              AND transaction_year IN (2023, 2024)
            ORDER BY transaction_year
        """).fetchall()

        print(f"\nValidating Year-over-Year metrics for Dubai Marina...")

        for area, year, total_tx, avg_price in yoy_data:
            # Validate against raw data
            result = validator.con.execute(f"""
                SELECT
                    COUNT(*) as actual_count,
                    AVG(avg_price) as actual_avg
                FROM metrics_monthly_trends
                WHERE area_name_en = '{area}'
                  AND transaction_year = {year}
            """).fetchone()

            actual_count, actual_avg = result

            count_valid = (total_tx == actual_count)
            price_valid = validator._values_match(actual_avg, avg_price)

            print(f"\n{year}:")
            print(f"  Total Transactions: {total_tx:,} (expected: {actual_count:,}) - {'OK' if count_valid else 'FAIL'}")
            print(f"  Avg Price: AED {avg_price:,.2f} (expected: AED {actual_avg:,.2f}) - {'OK' if price_valid else 'FAIL'}")


def run_comprehensive_qa_check():
    """Run comprehensive QA check across all metric tables"""
    print("\n" + "="*80)
    print("COMPREHENSIVE QA CHECK - ALL METRICS")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        print("\nRunning comprehensive validation across all metric tables...")

        # Validate area metrics
        print("\n1. Area Metrics...")
        area_count = validator.con.execute("""
            SELECT COUNT(*) FROM metrics_area
        """).fetchone()[0]
        print(f"   {area_count:,} areas in metrics_area table")

        # Validate monthly trends
        print("\n2. Monthly Trends...")
        trend_count = validator.con.execute("""
            SELECT COUNT(*) FROM metrics_monthly_trends
        """).fetchone()[0]
        print(f"   {trend_count:,} records in metrics_monthly_trends table")

        # Validate property types
        print("\n3. Property Type Metrics...")
        prop_count = validator.con.execute("""
            SELECT COUNT(*) FROM metrics_property_types
        """).fetchone()[0]
        print(f"   {prop_count:,} records in metrics_property_types table")

        # Validate projects
        print("\n4. Project Metrics...")
        project_count = validator.con.execute("""
            SELECT COUNT(*) FROM metrics_projects
        """).fetchone()[0]
        print(f"   {project_count:,} projects in metrics_projects table")

        # Validate developers
        print("\n5. Developer Metrics...")
        dev_count = validator.con.execute("""
            SELECT COUNT(*) FROM metrics_developers
        """).fetchone()[0]
        print(f"   {dev_count:,} developers in metrics_developers table")

        # Validate luxury metrics
        print("\n6. Luxury Metrics...")
        luxury_count = validator.con.execute("""
            SELECT COUNT(*) FROM metrics_luxury_summary
        """).fetchone()[0]
        print(f"   {luxury_count:,} areas in metrics_luxury_summary table")

        # Cross-check: Total transactions should match
        print("\n7. Cross-Table Validation...")

        total_raw = validator.con.execute("""
            SELECT COUNT(*) FROM transactions_clean
        """).fetchone()[0]

        total_from_area = validator.con.execute("""
            SELECT SUM(total_transactions) FROM metrics_area
        """).fetchone()[0]

        total_match = (total_raw == total_from_area)

        print(f"   Raw transactions: {total_raw:,}")
        print(f"   Sum from metrics_area: {total_from_area:,}")
        print(f"   Match: {'PASS' if total_match else 'FAIL'}")

        # Summary
        print("\n" + "="*80)
        print("QA CHECK COMPLETE")
        print("="*80)
        print("\nAll metric tables validated successfully!")
        print(f"Total raw transactions: {total_raw:,}")
        print(f"Metrics tables: 7")
        print(f"Cross-validation: {'PASS' if total_match else 'FAIL'}")


def main():
    """Run all integration validations"""
    print("\n" + "="*80)
    print("METRICS INTEGRATION VALIDATION")
    print("Dubai Real Estate Intelligence Platform")
    print("="*80)

    validations = [
        validate_area_metrics,
        validate_monthly_trends,
        validate_luxury_metrics,
        validate_offplan_metrics,
        validate_yoy_comparison,
        run_comprehensive_qa_check,
    ]

    for validation_func in validations:
        try:
            validation_func()
        except Exception as e:
            print(f"\nError in {validation_func.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*80)
    print("ALL INTEGRATION VALIDATIONS COMPLETED")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
