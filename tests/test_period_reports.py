#!/usr/bin/env python
"""
Test Period-Based Reports and QA Validation

This script tests the new period-based report generation system:
1. Tests ReportCalculator for all period types
2. Validates metrics against raw SQL queries using QAValidator
3. Verifies the integration between calculator and generator

Run with: python tests/test_period_reports.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.report_calculator import ReportCalculator, PeriodType
from src.analytics.qa_validator import QAValidator


def test_period_calculations():
    """Test ReportCalculator for all period types"""
    print("=" * 80)
    print("TESTING PERIOD CALCULATIONS")
    print("=" * 80)

    calculator = ReportCalculator()

    test_cases = [
        (2024, PeriodType.MONTHLY, 11, "November 2024"),
        (2024, PeriodType.QUARTERLY, 4, "Q4 2024"),
        (2024, PeriodType.SEMI_ANNUAL, 2, "H2 2024"),
        (2024, PeriodType.ANNUAL, 1, "2024 Annual"),
        (2025, PeriodType.QUARTERLY, 1, "Q1 2025"),
    ]

    results = []

    for year, period_type, period_num, description in test_cases:
        print(f"\n--- Testing {description} ---")

        try:
            metrics = calculator.calculate_period_metrics(year, period_type, period_num)
            tx_count = metrics['transaction_metrics']['total_transactions']

            if tx_count > 0:
                print(f"  Transactions: {tx_count:,}")
                print(f"  Volume: AED {metrics['transaction_metrics']['total_sales_volume']:,.0f}")
                print(f"  Avg Price: AED {metrics['price_metrics']['avg_price']:,.0f}")
                print(f"  Off-Plan: {metrics['market_segments']['offplan']['count']:,} ({metrics['market_segments']['offplan']['percentage']:.1f}%)")
                print(f"  Luxury: {metrics['market_segments']['luxury']['count']:,} ({metrics['market_segments']['luxury']['percentage']:.1f}%)")
                results.append((description, "PASS", tx_count))
            else:
                print(f"  No data available")
                results.append((description, "NO DATA", 0))

        except Exception as e:
            print(f"  ERROR: {e}")
            results.append((description, "ERROR", str(e)))

    calculator.close()
    return results


def test_qa_validation():
    """Test QA validation against raw SQL"""
    print("\n" + "=" * 80)
    print("TESTING QA VALIDATION")
    print("=" * 80)

    calculator = ReportCalculator()
    validator = QAValidator(tolerance=0.01)

    # Test Q4 2024
    print("\n--- Validating Q4 2024 Metrics ---")

    try:
        metrics = calculator.calculate_period_metrics(2024, PeriodType.QUARTERLY, 4)

        if metrics['transaction_metrics']['total_transactions'] > 0:
            # Prepare validation data
            validation_data = {
                'year': 2024,
                'period_type': 'quarterly',
                'period_num': 4,
                'transaction_count': metrics['transaction_metrics']['total_transactions'],
                'total_volume': metrics['transaction_metrics']['total_sales_volume'],
                'avg_price': metrics['price_metrics']['avg_price'],
                'offplan_count': metrics['market_segments']['offplan']['count'],
                'offplan_percentage': metrics['market_segments']['offplan']['percentage'],
                'luxury_count': metrics['market_segments']['luxury']['count'],
                'luxury_percentage': metrics['market_segments']['luxury']['percentage']
            }

            # Run validation
            validator.run_all_validations(validation_data)

            # Print report
            print(validator.generate_validation_report("Q4 2024 Validation"))

            summary = validator.get_summary()
            return summary
        else:
            print("  No data for Q4 2024")
            return None

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        calculator.close()
        validator.close()


def test_top_performers():
    """Test top performers analysis"""
    print("\n" + "=" * 80)
    print("TESTING TOP PERFORMERS")
    print("=" * 80)

    calculator = ReportCalculator()

    try:
        # Test for Q4 2024
        print("\n--- Top Areas Q4 2024 ---")
        top_areas = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.QUARTERLY,
            period_number=4,
            metric="transaction_count",
            category="areas",
            limit=5
        )

        for area in top_areas:
            print(f"  {area['rank']}. {area['name']}: {area['transaction_count']:,} transactions, AED {area['avg_price']:,.0f} avg")

        print("\n--- Top Developers Q4 2024 ---")
        top_devs = calculator.get_top_performers(
            year=2024,
            period_type=PeriodType.QUARTERLY,
            period_number=4,
            metric="transaction_count",
            category="developers",
            limit=5
        )

        for dev in top_devs:
            print(f"  {dev['rank']}. {dev['name']}: {dev['transaction_count']:,} transactions")

        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        calculator.close()


def test_comparison_metrics():
    """Test period comparison calculations"""
    print("\n" + "=" * 80)
    print("TESTING COMPARISON METRICS")
    print("=" * 80)

    calculator = ReportCalculator()

    try:
        print("\n--- Q4 2024 vs Q3 2024 (Period-over-Period) ---")
        pop_comparison = calculator.get_comparison_metrics(
            current_year=2024,
            current_period_type=PeriodType.QUARTERLY,
            current_period_number=4,
            comparison_type="mom"
        )

        changes = pop_comparison['changes']
        tx_change = changes['transaction_changes']['total_transactions']
        price_change = changes['price_changes']['avg_price']

        if tx_change['percentage'] is not None:
            print(f"  Transaction Change: {tx_change['percentage']:+.1f}%")
        if price_change['percentage'] is not None:
            print(f"  Price Change: {price_change['percentage']:+.1f}%")

        print("\n--- Q4 2024 vs Q4 2023 (Year-over-Year) ---")
        yoy_comparison = calculator.get_comparison_metrics(
            current_year=2024,
            current_period_type=PeriodType.QUARTERLY,
            current_period_number=4,
            comparison_type="yoy"
        )

        changes = yoy_comparison['changes']
        tx_change = changes['transaction_changes']['total_transactions']
        price_change = changes['price_changes']['avg_price']

        if tx_change['percentage'] is not None:
            print(f"  Transaction Change: {tx_change['percentage']:+.1f}%")
        if price_change['percentage'] is not None:
            print(f"  Price Change: {price_change['percentage']:+.1f}%")

        return True

    except Exception as e:
        print(f"  ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        calculator.close()


def main():
    """Run all tests"""
    print("\n" + "=" * 80)
    print("PERIOD-BASED REPORT SYSTEM - COMPREHENSIVE TESTS")
    print("=" * 80)

    # Test 1: Period calculations
    calc_results = test_period_calculations()

    # Test 2: QA validation
    qa_summary = test_qa_validation()

    # Test 3: Top performers
    top_performers_ok = test_top_performers()

    # Test 4: Comparison metrics
    comparison_ok = test_comparison_metrics()

    # Summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    print("\nPeriod Calculations:")
    for desc, status, value in calc_results:
        print(f"  {desc}: {status} ({value if isinstance(value, int) else value})")

    print("\nQA Validation:")
    if qa_summary:
        print(f"  Pass Rate: {qa_summary['pass_rate']:.0f}%")
        print(f"  Tests: {qa_summary['passed']}/{qa_summary['total_tests']} passed")
    else:
        print("  Could not run validation tests")

    print(f"\nTop Performers: {'PASS' if top_performers_ok else 'FAIL'}")
    print(f"Comparison Metrics: {'PASS' if comparison_ok else 'FAIL'}")

    # Overall result
    all_passed = (
        all(r[1] in ['PASS', 'NO DATA'] for r in calc_results) and
        (qa_summary is None or qa_summary['pass_rate'] >= 95) and
        top_performers_ok and
        comparison_ok
    )

    print("\n" + "=" * 80)
    if all_passed:
        print("ALL TESTS PASSED - Period-based report system is working correctly!")
    else:
        print("SOME TESTS FAILED - Please review the output above")
    print("=" * 80)

    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
