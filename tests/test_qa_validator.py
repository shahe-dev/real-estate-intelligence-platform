#!/usr/bin/env python3
# test_qa_validator.py

"""
Comprehensive test suite for QA Validator
Demonstrates all validation capabilities
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics.qa_validator import QAValidator, ValidationStatus


def test_basic_validations():
    """Test basic validation functions"""
    print("\n" + "="*80)
    print("TEST 1: Basic Validation Functions")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        con = validator.con

        # Test 1.1: Transaction Count Validation
        print("\n1.1 Testing Transaction Count Validation...")

        # Get actual count from database
        result = con.execute("""
            SELECT COUNT(*) FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        if result and result[0] > 0:
            actual_count = result[0]
            print(f"   Actual count from DB: {actual_count:,}")

            # Test with correct value
            v1 = validator.validate_transaction_count(2024, 'monthly', 1, actual_count)
            print(f"   Correct value: {v1}")

            # Test with incorrect value
            v2 = validator.validate_transaction_count(2024, 'monthly', 1, actual_count - 100)
            print(f"   Incorrect value: {v2}")

        # Test 1.2: Total Volume Validation
        print("\n1.2 Testing Total Volume Validation...")

        result = con.execute("""
            SELECT SUM(actual_worth) FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        if result and result[0]:
            actual_volume = result[0]
            print(f"   Actual volume from DB: {actual_volume:,.2f}")

            # Test with correct value
            v3 = validator.validate_total_volume(2024, 'monthly', 1, actual_volume)
            print(f"   Correct value: {v3}")

            # Test with slightly incorrect value (within tolerance)
            v4 = validator.validate_total_volume(2024, 'monthly', 1, actual_volume * 1.005)
            print(f"   Within tolerance: {v4}")

            # Test with incorrect value (beyond tolerance)
            v5 = validator.validate_total_volume(2024, 'monthly', 1, actual_volume * 1.05)
            print(f"   Beyond tolerance: {v5}")

        # Test 1.3: Average Price Validation
        print("\n1.3 Testing Average Price Validation...")

        result = con.execute("""
            SELECT AVG(actual_worth) FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        if result and result[0]:
            actual_avg = result[0]
            print(f"   Actual average from DB: {actual_avg:,.2f}")

            # Test with correct value
            v6 = validator.validate_average_price(2024, 'monthly', 1, actual_avg)
            print(f"   Correct value: {v6}")


def test_period_types():
    """Test different period types (monthly, quarterly, semi-annual, annual)"""
    print("\n" + "="*80)
    print("TEST 2: Different Period Types")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        con = validator.con

        # Test 2.1: Monthly Period
        print("\n2.1 Testing Monthly Period (2024-01)...")
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'monthly', 1, count)
            validator.validate_total_volume(2024, 'monthly', 1, volume)
            validator.validate_average_price(2024, 'monthly', 1, avg)
            print(f"   Validated {count:,} transactions")

        # Test 2.2: Quarterly Period
        print("\n2.2 Testing Quarterly Period (2024-Q1)...")
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month BETWEEN 1 AND 3
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'quarterly', 1, count)
            validator.validate_total_volume(2024, 'quarterly', 1, volume)
            validator.validate_average_price(2024, 'quarterly', 1, avg)
            validator.validate_period_boundaries(2024, 'quarterly', 1)
            validator.validate_date_range_filter(2024, 'quarterly', 1)
            print(f"   Validated {count:,} transactions")

        # Test 2.3: Semi-Annual Period
        print("\n2.3 Testing Semi-Annual Period (2024-H1)...")
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month BETWEEN 1 AND 6
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'semi-annual', 1, count)
            validator.validate_total_volume(2024, 'semi-annual', 1, volume)
            validator.validate_average_price(2024, 'semi-annual', 1, avg)
            validator.validate_period_boundaries(2024, 'semi-annual', 1)
            validator.validate_date_range_filter(2024, 'semi-annual', 1)
            print(f"   Validated {count:,} transactions")

        # Test 2.4: Annual Period
        print("\n2.4 Testing Annual Period (2024)...")
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'annual', 1, count)
            validator.validate_total_volume(2024, 'annual', 1, volume)
            validator.validate_average_price(2024, 'annual', 1, avg)
            validator.validate_date_range_filter(2024, 'annual', 1)
            print(f"   Validated {count:,} transactions")


def test_filters():
    """Test area and property type filters"""
    print("\n" + "="*80)
    print("TEST 3: Area and Property Type Filters")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        con = validator.con

        # Test 3.1: Area Filter
        print("\n3.1 Testing Area Filter (Dubai Marina)...")
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024
              AND transaction_month = 1
              AND area_name_en = 'Dubai Marina'
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'monthly', 1, count,
                                                area_filter='Dubai Marina')
            validator.validate_total_volume(2024, 'monthly', 1, volume,
                                          area_filter='Dubai Marina')
            validator.validate_average_price(2024, 'monthly', 1, avg,
                                           area_filter='Dubai Marina')
            print(f"   Validated {count:,} transactions in Dubai Marina")

        # Test 3.2: Property Type Filter
        print("\n3.2 Testing Property Type Filter (Apartment)...")
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024
              AND transaction_month = 1
              AND property_type_en = 'Unit'
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'monthly', 1, count,
                                                property_type_filter='Unit')
            validator.validate_total_volume(2024, 'monthly', 1, volume,
                                          property_type_filter='Unit')
            validator.validate_average_price(2024, 'monthly', 1, avg,
                                           property_type_filter='Unit')
            print(f"   Validated {count:,} Unit transactions")

        # Test 3.3: Combined Filters
        print("\n3.3 Testing Combined Filters (Dubai Marina Apartments)...")
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024
              AND transaction_month = 1
              AND area_name_en = 'Dubai Marina'
              AND property_type_en = 'Unit'
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'monthly', 1, count,
                                                area_filter='Dubai Marina',
                                                property_type_filter='Unit')
            print(f"   Validated {count:,} Unit transactions in Dubai Marina")


def test_percentage_calculations():
    """Test percentage calculation validations"""
    print("\n" + "="*80)
    print("TEST 4: Percentage Calculation Validations")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        con = validator.con

        # Test 4.1: Off-Plan Percentage
        print("\n4.1 Testing Off-Plan Percentage Calculation...")
        result = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan
            FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        if result and result[0] > 0:
            total, offplan = result
            correct_pct = (offplan / total * 100)
            print(f"   Total: {total:,}, Off-Plan: {offplan:,}, Correct %: {correct_pct:.2f}%")

            # Test with correct percentage
            v1 = validator.validate_percentage_calculation(
                offplan, total, correct_pct,
                test_name="offplan_pct_correct"
            )
            print(f"   Correct: {v1}")

            # Test with incorrect percentage
            v2 = validator.validate_percentage_calculation(
                offplan, total, correct_pct + 5.0,
                test_name="offplan_pct_incorrect"
            )
            print(f"   Incorrect: {v2}")

        # Test 4.2: Luxury Percentage
        print("\n4.2 Testing Luxury Percentage Calculation...")
        result = con.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury
            FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        if result and result[0] > 0:
            total, luxury = result
            correct_pct = (luxury / total * 100)
            print(f"   Total: {total:,}, Luxury: {luxury:,}, Correct %: {correct_pct:.2f}%")

            validator.validate_percentage_calculation(
                luxury, total, correct_pct,
                test_name="luxury_pct"
            )


def test_run_all_validations():
    """Test run_all_validations method"""
    print("\n" + "="*80)
    print("TEST 5: run_all_validations() Method")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        con = validator.con

        # Get comprehensive metrics for Q1 2024
        print("\n5.1 Validating complete metrics set for 2024 Q1...")
        result = con.execute("""
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

        if result and result[0] > 0:
            tx_count, total_volume, avg_price, offplan_count, luxury_count = result
            offplan_pct = (offplan_count / tx_count * 100)
            luxury_pct = (luxury_count / tx_count * 100)

            metrics = {
                'year': 2024,
                'period_type': 'quarterly',
                'period_num': 1,
                'transaction_count': tx_count,
                'total_volume': total_volume,
                'avg_price': avg_price,
                'offplan_count': offplan_count,
                'luxury_count': luxury_count,
                'offplan_percentage': offplan_pct,
                'luxury_percentage': luxury_pct
            }

            print(f"   Metrics to validate:")
            print(f"   - Transaction Count: {tx_count:,}")
            print(f"   - Total Volume: {total_volume:,.2f}")
            print(f"   - Average Price: {avg_price:,.2f}")
            print(f"   - Off-Plan %: {offplan_pct:.2f}%")
            print(f"   - Luxury %: {luxury_pct:.2f}%")

            validator.run_all_validations(metrics)

            # Show summary
            summary = validator.get_summary()
            print(f"\n   Validation Summary:")
            print(f"   - Total Tests: {summary['total_tests']}")
            print(f"   - Passed: {summary['passed']}")
            print(f"   - Failed: {summary['failed']}")
            print(f"   - Pass Rate: {summary['pass_rate']:.1f}%")


def test_validation_report():
    """Test validation report generation"""
    print("\n" + "="*80)
    print("TEST 6: Validation Report Generation")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        con = validator.con

        # Run multiple validations
        print("\n6.1 Running multiple validations across different periods...")

        # Validate Jan, Feb, Mar 2024
        for month in [1, 2, 3]:
            result = con.execute(f"""
                SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
                FROM transactions_clean
                WHERE transaction_year = 2024 AND transaction_month = {month}
            """).fetchone()

            if result and result[0] > 0:
                count, volume, avg = result
                validator.validate_transaction_count(2024, 'monthly', month, count)
                validator.validate_total_volume(2024, 'monthly', month, volume)
                validator.validate_average_price(2024, 'monthly', month, avg)

        # Validate Q1 2024
        result = con.execute("""
            SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
            FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month BETWEEN 1 AND 3
        """).fetchone()

        if result and result[0] > 0:
            count, volume, avg = result
            validator.validate_transaction_count(2024, 'quarterly', 1, count)
            validator.validate_total_volume(2024, 'quarterly', 1, volume)
            validator.validate_average_price(2024, 'quarterly', 1, avg)
            validator.validate_period_boundaries(2024, 'quarterly', 1)

        # Generate and display report
        print(validator.generate_validation_report("2024 Q1 Comprehensive Validation Report"))


def test_edge_cases():
    """Test edge cases and error handling"""
    print("\n" + "="*80)
    print("TEST 7: Edge Cases and Error Handling")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Test 7.1: Zero denominator percentage
        print("\n7.1 Testing percentage with zero denominator...")
        v1 = validator.validate_percentage_calculation(
            0, 0, 0,
            test_name="zero_denominator"
        )
        print(f"   {v1}")

        # Test 7.2: Invalid period type
        print("\n7.2 Testing invalid period type...")
        try:
            validator.validate_period_boundaries(2024, 'invalid_period', 1)
        except Exception as e:
            print(f"   Expected error: {type(e).__name__}")

        # Test 7.3: Non-existent data
        print("\n7.3 Testing validation with non-existent data...")
        v3 = validator.validate_transaction_count(2099, 'monthly', 1, 0)
        print(f"   {v3}")

        # Test 7.4: Custom tolerance
        print("\n7.4 Testing custom tolerance levels...")
        result = validator.con.execute("""
            SELECT AVG(actual_worth) FROM transactions_clean
            WHERE transaction_year = 2024 AND transaction_month = 1
        """).fetchone()

        if result and result[0]:
            actual_avg = result[0]

            # Strict tolerance (0.1%)
            v4a = validator.validate_average_price(
                2024, 'monthly', 1, actual_avg * 1.002,
                tolerance=0.001
            )
            print(f"   Strict (0.1%): {v4a}")

            # Lenient tolerance (5%)
            v4b = validator.validate_average_price(
                2024, 'monthly', 1, actual_avg * 1.03,
                tolerance=0.05
            )
            print(f"   Lenient (5%): {v4b}")


def run_all_tests():
    """Run all test suites"""
    print("\n" + "="*80)
    print("QA VALIDATOR COMPREHENSIVE TEST SUITE")
    print("Dubai Real Estate Intelligence Platform")
    print("="*80)

    tests = [
        ("Basic Validations", test_basic_validations),
        ("Period Types", test_period_types),
        ("Filters", test_filters),
        ("Percentage Calculations", test_percentage_calculations),
        ("run_all_validations()", test_run_all_validations),
        ("Validation Report", test_validation_report),
        ("Edge Cases", test_edge_cases),
    ]

    for test_name, test_func in tests:
        try:
            test_func()
        except Exception as e:
            print(f"\n✗ Test '{test_name}' failed with error: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*80)
    print("ALL TESTS COMPLETED")
    print("="*80 + "\n")


if __name__ == "__main__":
    run_all_tests()
