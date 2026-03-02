#!/usr/bin/env python3
# example_qa_validation.py

"""
Simple example demonstrating QA Validator usage
Validates metrics from the Property Monitor database
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics import QAValidator


def example_1_basic_validation():
    """Example 1: Basic validation of a single metric"""
    print("\n" + "="*80)
    print("EXAMPLE 1: Basic Validation")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get actual data from database
        result = validator.con.execute("""
            SELECT COUNT(*) FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        actual_count = result[0]
        print(f"\nValidating transaction count for January 2024")
        print(f"Database count: {actual_count:,}")

        # Validate with correct value
        validation = validator.validate_transaction_count(
            year=2024,
            period_type='monthly',
            period_num=1,
            calculated_value=actual_count
        )

        print(f"Result: {validation}")


def example_2_quarterly_validation():
    """Example 2: Validate quarterly metrics"""
    print("\n" + "="*80)
    print("EXAMPLE 2: Quarterly Validation")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get Q1 2024 data
        result = validator.con.execute("""
            SELECT
                COUNT(*) as tx_count,
                SUM(actual_worth) as total_volume,
                AVG(actual_worth) as avg_price
            FROM transactions_clean
            WHERE transaction_year = 2024
              AND transaction_month BETWEEN 1 AND 3
        """).fetchone()

        count, volume, avg_price = result

        print(f"\nValidating Q1 2024 metrics:")
        print(f"  Transactions: {count:,}")
        print(f"  Total Volume: AED {volume:,.2f}")
        print(f"  Average Price: AED {avg_price:,.2f}")

        # Validate all three metrics
        v1 = validator.validate_transaction_count(2024, 'quarterly', 1, count)
        v2 = validator.validate_total_volume(2024, 'quarterly', 1, volume)
        v3 = validator.validate_average_price(2024, 'quarterly', 1, avg_price)

        print(f"\nResults:")
        print(f"  Transaction Count: {v1.status.value}")
        print(f"  Total Volume: {v2.status.value}")
        print(f"  Average Price: {v3.status.value}")


def example_3_area_specific_validation():
    """Example 3: Validate area-specific metrics"""
    print("\n" + "="*80)
    print("EXAMPLE 3: Area-Specific Validation")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        areas = ['Dubai Marina', 'Downtown Dubai', 'Palm Jumeirah']

        for area in areas:
            result = validator.con.execute(f"""
                SELECT
                    COUNT(*) as tx_count,
                    AVG(actual_worth) as avg_price
                FROM transactions_clean
                WHERE transaction_year = 2024
                  AND transaction_month = 1
                  AND area_name_en = '{area}'
            """).fetchone()

            if result and result[0] > 0:
                count, avg_price = result

                print(f"\n{area}:")
                print(f"  Transactions: {count:,}")
                print(f"  Average Price: AED {avg_price:,.2f}")

                # Validate
                v1 = validator.validate_transaction_count(
                    2024, 'monthly', 1, count, area_filter=area
                )
                v2 = validator.validate_average_price(
                    2024, 'monthly', 1, avg_price, area_filter=area
                )

                print(f"  Validation: {v1.status.value} / {v2.status.value}")


def example_4_comprehensive_validation():
    """Example 4: Comprehensive validation with full report"""
    print("\n" + "="*80)
    print("EXAMPLE 4: Comprehensive Validation with Report")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get comprehensive metrics for Q1 2024
        result = validator.con.execute("""
            SELECT
                COUNT(*) as tx_count,
                SUM(actual_worth) as total_volume,
                AVG(actual_worth) as avg_price,
                SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count
            FROM transactions_clean
            WHERE transaction_year = 2024
              AND transaction_month BETWEEN 1 AND 3
        """).fetchone()

        tx_count, total_volume, avg_price, offplan_count, luxury_count = result

        # Build metrics dictionary
        metrics = {
            'year': 2024,
            'period_type': 'quarterly',
            'period_num': 1,
            'transaction_count': tx_count,
            'total_volume': total_volume,
            'avg_price': avg_price,
            'offplan_count': offplan_count,
            'luxury_count': luxury_count,
            'offplan_percentage': (offplan_count / tx_count * 100),
            'luxury_percentage': (luxury_count / tx_count * 100)
        }

        print(f"\nValidating complete metrics set for Q1 2024...")

        # Run all validations
        validator.run_all_validations(metrics)

        # Generate and print report
        print(validator.generate_validation_report("Q1 2024 Comprehensive Validation"))

        # Get summary
        summary = validator.get_summary()
        print(f"\nSummary:")
        print(f"  Pass Rate: {summary['pass_rate']:.1f}%")
        print(f"  Tests Passed: {summary['passed']}/{summary['total_tests']}")


def example_5_failure_detection():
    """Example 5: Detecting validation failures"""
    print("\n" + "="*80)
    print("EXAMPLE 5: Failure Detection")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Get actual value
        result = validator.con.execute("""
            SELECT AVG(actual_worth) FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        actual_avg = result[0]

        print(f"\nActual average price: AED {actual_avg:,.2f}")

        # Test with incorrect values
        test_cases = [
            ("Correct value", actual_avg, True),
            ("1% off (within tolerance)", actual_avg * 1.005, True),
            ("5% off (beyond tolerance)", actual_avg * 1.05, False),
            ("10% off (way beyond)", actual_avg * 1.10, False),
        ]

        for description, test_value, should_pass in test_cases:
            result = validator.validate_average_price(
                2024, 'monthly', 1, test_value
            )

            status_symbol = "[PASS]" if result.status.value == "PASS" else "[FAIL]"
            print(f"\n{description}:")
            print(f"  Test value: AED {test_value:,.2f}")
            print(f"  Status: {status_symbol}")
            if result.deviation_pct is not None:
                print(f"  Deviation: {result.deviation_pct:.2f}%")


def example_6_custom_tolerance():
    """Example 6: Using custom tolerance levels"""
    print("\n" + "="*80)
    print("EXAMPLE 6: Custom Tolerance Levels")
    print("="*80)

    # Get actual data
    with QAValidator() as validator:
        result = validator.con.execute("""
            SELECT AVG(actual_worth) FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()
        actual_avg = result[0]

    # Test value with 2% deviation
    test_value = actual_avg * 1.02

    print(f"\nActual average: AED {actual_avg:,.2f}")
    print(f"Test value (2% higher): AED {test_value:,.2f}")

    tolerance_levels = [
        ("Strict (0.1%)", 0.001),
        ("Normal (1%)", 0.01),
        ("Lenient (5%)", 0.05),
    ]

    for name, tolerance in tolerance_levels:
        with QAValidator(tolerance=tolerance) as validator:
            result = validator.validate_average_price(
                2024, 'monthly', 1, test_value
            )

            status = "PASS" if result.status.value == "PASS" else "FAIL"
            print(f"\n{name}: {status}")


def main():
    """Run all examples"""
    print("\n" + "="*80)
    print("QA VALIDATOR - PRACTICAL EXAMPLES")
    print("Dubai Real Estate Intelligence Platform")
    print("="*80)

    examples = [
        example_1_basic_validation,
        example_2_quarterly_validation,
        example_3_area_specific_validation,
        example_4_comprehensive_validation,
        example_5_failure_detection,
        example_6_custom_tolerance,
    ]

    for example_func in examples:
        try:
            example_func()
        except Exception as e:
            print(f"\nError in {example_func.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*80)
    print("ALL EXAMPLES COMPLETED")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
