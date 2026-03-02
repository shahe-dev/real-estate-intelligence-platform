"""
Emerging Hotspots Data Quality Validation Script
=================================================
This script independently verifies the calculations in the emerging hotspots analysis
by re-running SQL queries and spot-checking specific areas.

Author: Data Quality Team
Date: 2026-01-06
"""

import duckdb
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any
import sys

# Configuration
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = str(PROJECT_ROOT / "data" / "database" / "property_monitor.db")
JSON_PATH = str(PROJECT_ROOT / "data" / "generated_content" / "emerging_hotspots_data.json")
OUTPUT_PATH = str(PROJECT_ROOT / "data" / "generated_content" / "validation_report.xlsx")

# Test areas as specified
TEST_AREAS = [
    "Jumeirah Heights",
    "Meydan Horizon",
    "Dubai Design District",
    "Al Qusais Industrial",
    "Majan",
    "Dubai Islands",
    "Motor City",
    "Business Bay",
    "Jumeirah Village Circle",
    "Dubai South"
]

class ValidationResult:
    """Container for validation results"""
    def __init__(self, area_name: str):
        self.area_name = area_name
        self.checks = []
        self.passed = True
        self.discrepancies = []

    def add_check(self, check_name: str, expected: Any, actual: Any, tolerance: float = 0.01):
        """Add a validation check with tolerance for floating point comparisons"""
        if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
            if expected == 0 and actual == 0:
                passed = True
            elif expected == 0 or actual == 0:
                passed = (expected == actual)
            else:
                # Use relative tolerance for percentages and counts
                relative_diff = abs(expected - actual) / max(abs(expected), abs(actual))
                passed = relative_diff <= tolerance
        elif expected == "INFINITE" or actual == "INFINITE":
            passed = (expected == actual)
        else:
            passed = (expected == actual)

        self.checks.append({
            'check': check_name,
            'expected': expected,
            'actual': actual,
            'passed': passed
        })

        if not passed:
            self.passed = False
            discrepancy = f"{check_name}: Expected {expected}, Got {actual}"
            self.discrepancies.append(discrepancy)

        return passed

def calculate_growth_rate(old_value: float, new_value: float) -> float:
    """
    Calculate growth rate with proper handling of edge cases
    Formula: ((new - old) / old) * 100
    """
    if old_value == 0 and new_value == 0:
        return 0.0
    elif old_value == 0:
        return "INFINITE"
    else:
        return round(((new_value - old_value) / old_value) * 100, 2)

def calculate_compound_growth(start_value: float, end_value: float, periods: int = 2) -> float:
    """
    Calculate compound annual growth rate
    CAGR = ((end_value / start_value) ^ (1/periods) - 1) * 100
    """
    if start_value == 0 and end_value == 0:
        return 0.0
    elif start_value == 0:
        return "INFINITE"
    elif end_value == 0:
        return -100.0
    else:
        try:
            cagr = ((end_value / start_value) ** (1 / periods) - 1) * 100
            return round(cagr, 2)
        except:
            return "ERROR"

def query_area_transactions(conn: duckdb.DuckDBPyConnection, area_name: str) -> Dict[int, int]:
    """
    Query transaction counts for a specific area across years
    Returns: {year: count}
    """
    query = """
    SELECT
        CAST(strftime(CAST(instance_date AS DATE), '%Y') AS INTEGER) as year,
        COUNT(*) as transaction_count
    FROM transactions_clean
    WHERE area_name_en = ?
        AND instance_date IS NOT NULL
        AND CAST(strftime(CAST(instance_date AS DATE), '%Y') AS INTEGER) IN (2023, 2024, 2025)
    GROUP BY year
    ORDER BY year
    """

    results = conn.execute(query, [area_name]).fetchall()
    return {year: count for year, count in results}

def query_area_avg_values(conn: duckdb.DuckDBPyConnection, area_name: str) -> Dict[int, float]:
    """
    Query average transaction values for a specific area across years
    Returns: {year: avg_value}
    """
    query = """
    SELECT
        CAST(strftime(CAST(instance_date AS DATE), '%Y') AS INTEGER) as year,
        AVG(actual_worth) as avg_value
    FROM transactions_clean
    WHERE area_name_en = ?
        AND instance_date IS NOT NULL
        AND actual_worth IS NOT NULL
        AND actual_worth > 0
        AND CAST(strftime(CAST(instance_date AS DATE), '%Y') AS INTEGER) IN (2023, 2024, 2025)
    GROUP BY year
    ORDER BY year
    """

    results = conn.execute(query, [area_name]).fetchall()
    return {year: round(avg_val, 2) for year, avg_val in results}

def validate_area(conn: duckdb.DuckDBPyConnection, area_name: str, json_data: Dict) -> ValidationResult:
    """
    Validate all calculations for a specific area
    """
    print(f"\n{'='*80}")
    print(f"Validating: {area_name}")
    print(f"{'='*80}")

    result = ValidationResult(area_name)

    # Get JSON data for this area
    area_json = None
    for area in json_data['areas']:
        if area['area_name_en'] == area_name:
            area_json = area
            break

    if not area_json:
        result.passed = False
        result.discrepancies.append(f"Area '{area_name}' not found in JSON data")
        print(f"ERROR: Area not found in JSON")
        return result

    # Query independent transaction counts
    transactions = query_area_transactions(conn, area_name)
    trans_2023 = transactions.get(2023, 0)
    trans_2024 = transactions.get(2024, 0)
    trans_2025 = transactions.get(2025, 0)

    print(f"\nTransaction Counts:")
    print(f"  JSON:   2023={area_json['transactions_2023']}, 2024={area_json['transactions_2024']}, 2025={area_json['transactions_2025']}")
    print(f"  Query:  2023={trans_2023}, 2024={trans_2024}, 2025={trans_2025}")

    # Validate transaction counts
    result.add_check("transactions_2023", area_json['transactions_2023'], trans_2023, tolerance=0)
    result.add_check("transactions_2024", area_json['transactions_2024'], trans_2024, tolerance=0)
    result.add_check("transactions_2025", area_json['transactions_2025'], trans_2025, tolerance=0)

    # Query independent average values
    avg_values = query_area_avg_values(conn, area_name)
    avg_2023 = avg_values.get(2023, 0.0)
    avg_2024 = avg_values.get(2024, 0.0)
    avg_2025 = avg_values.get(2025, 0.0)

    print(f"\nAverage Values:")
    print(f"  JSON:   2023={area_json['avg_value_2023']:.2f}, 2024={area_json['avg_value_2024']:.2f}, 2025={area_json['avg_value_2025']:.2f}")
    print(f"  Query:  2023={avg_2023:.2f}, 2024={avg_2024:.2f}, 2025={avg_2025:.2f}")

    # Validate average values (with 1% tolerance for rounding)
    result.add_check("avg_value_2023", area_json['avg_value_2023'], avg_2023, tolerance=0.01)
    result.add_check("avg_value_2024", area_json['avg_value_2024'], avg_2024, tolerance=0.01)
    result.add_check("avg_value_2025", area_json['avg_value_2025'], avg_2025, tolerance=0.01)

    # Calculate and validate growth rates
    # 2024 vs 2023
    calc_growth_2024 = calculate_growth_rate(trans_2023, trans_2024)
    json_growth_2024 = area_json['yoy_growth_2024_vs_2023_pct']

    print(f"\nGrowth Rate 2024 vs 2023:")
    print(f"  JSON:       {json_growth_2024}")
    print(f"  Calculated: {calc_growth_2024}")

    result.add_check("yoy_growth_2024_vs_2023_pct", json_growth_2024, calc_growth_2024, tolerance=0.01)

    # 2025 vs 2024
    calc_growth_2025 = calculate_growth_rate(trans_2024, trans_2025)
    json_growth_2025 = area_json['yoy_growth_2025_vs_2024_pct']

    print(f"\nGrowth Rate 2025 vs 2024:")
    print(f"  JSON:       {json_growth_2025}")
    print(f"  Calculated: {calc_growth_2025}")

    result.add_check("yoy_growth_2025_vs_2024_pct", json_growth_2025, calc_growth_2025, tolerance=0.01)

    # Compound growth
    calc_compound = calculate_compound_growth(trans_2023, trans_2025, periods=2)
    json_compound = area_json['compound_growth_2023_2025_pct']

    print(f"\nCompound Growth 2023-2025:")
    print(f"  JSON:       {json_compound}")
    print(f"  Calculated: {calc_compound}")

    result.add_check("compound_growth_2023_2025_pct", json_compound, calc_compound, tolerance=0.01)

    # Validate absolute changes
    calc_abs_2024 = trans_2024 - trans_2023
    calc_abs_2025 = trans_2025 - trans_2024
    calc_abs_total = trans_2025 - trans_2023

    print(f"\nAbsolute Changes:")
    print(f"  2024: JSON={area_json['absolute_change_2024']}, Calc={calc_abs_2024}")
    print(f"  2025: JSON={area_json['absolute_change_2025']}, Calc={calc_abs_2025}")
    print(f"  Total: JSON={area_json['absolute_change_total']}, Calc={calc_abs_total}")

    result.add_check("absolute_change_2024", area_json['absolute_change_2024'], calc_abs_2024, tolerance=0)
    result.add_check("absolute_change_2025", area_json['absolute_change_2025'], calc_abs_2025, tolerance=0)
    result.add_check("absolute_change_total", area_json['absolute_change_total'], calc_abs_total, tolerance=0)

    # Summary
    if result.passed:
        print(f"\nPASSED: All checks passed for {area_name}")
    else:
        print(f"\nFAILED: {len(result.discrepancies)} discrepancies found for {area_name}")
        for disc in result.discrepancies:
            print(f"  - {disc}")

    return result

def verify_overall_statistics(conn: duckdb.DuckDBPyConnection) -> Dict[str, int]:
    """
    Verify overall transaction counts across all years
    """
    query = """
    SELECT
        CAST(strftime(CAST(instance_date AS DATE), '%Y') AS INTEGER) as year,
        COUNT(*) as transaction_count
    FROM transactions_clean
    WHERE instance_date IS NOT NULL
        AND CAST(strftime(CAST(instance_date AS DATE), '%Y') AS INTEGER) IN (2023, 2024, 2025)
    GROUP BY year
    ORDER BY year
    """

    results = conn.execute(query, []).fetchall()
    return {year: count for year, count in results}

def validate_formula_correctness():
    """
    Test the growth rate formula with known values
    """
    print("\n" + "="*80)
    print("FORMULA VALIDATION TESTS")
    print("="*80)

    test_cases = [
        # (old, new, expected_growth_pct)
        (100, 200, 100.0),  # 100% growth
        (100, 150, 50.0),   # 50% growth
        (100, 50, -50.0),   # -50% decline
        (50, 100, 100.0),   # 100% growth
        (0, 100, "INFINITE"),  # Infinite growth (division by zero)
        (100, 0, -100.0),   # -100% decline
        (0, 0, 0.0),        # No change from zero
        (33, 595, 1703.03), # Jumeirah Heights case
    ]

    all_passed = True
    for old, new, expected in test_cases:
        calculated = calculate_growth_rate(old, new)

        if isinstance(expected, str):
            passed = calculated == expected
        else:
            passed = abs(calculated - expected) < 0.01

        status = "PASS" if passed else "FAIL"
        print(f"{status}: ({old} -> {new}) = {calculated}% (expected {expected}%)")

        if not passed:
            all_passed = False

    return all_passed

def main():
    """
    Main validation workflow
    """
    print("="*80)
    print("EMERGING HOTSPOTS DATA QUALITY VALIDATION")
    print("="*80)
    print(f"Database: {DB_PATH}")
    print(f"JSON Data: {JSON_PATH}")
    print(f"Output: {OUTPUT_PATH}")
    print(f"Timestamp: {datetime.now().isoformat()}")

    # Step 1: Validate formulas
    formula_valid = validate_formula_correctness()

    # Step 2: Connect to database
    print("\n" + "="*80)
    print("CONNECTING TO DATABASE")
    print("="*80)
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
        print("Database connection established")
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        sys.exit(1)

    # Step 3: Load JSON data
    print("\n" + "="*80)
    print("LOADING JSON DATA")
    print("="*80)
    try:
        with open(JSON_PATH, 'r') as f:
            json_data = json.load(f)
        print(f"Loaded data for {json_data['metadata']['total_areas']} areas")
        print(f"  - Areas with misleading growth: {json_data['metadata']['areas_with_misleading_growth']}")
        print(f"  - Areas with legitimate growth: {json_data['metadata']['areas_with_legitimate_growth']}")
    except Exception as e:
        print(f"ERROR: Failed to load JSON: {e}")
        sys.exit(1)

    # Step 4: Verify overall statistics
    print("\n" + "="*80)
    print("OVERALL TRANSACTION STATISTICS")
    print("="*80)
    overall_stats = verify_overall_statistics(conn)
    print("Total transactions by year:")
    for year, count in sorted(overall_stats.items()):
        print(f"  {year}: {count:,} transactions")

    # Step 5: Validate test areas
    validation_results = []
    for area_name in TEST_AREAS:
        result = validate_area(conn, area_name, json_data)
        validation_results.append(result)

    # Step 6: Calculate quality score
    total_checks = sum(len(r.checks) for r in validation_results)
    passed_checks = sum(sum(1 for c in r.checks if c['passed']) for r in validation_results)
    quality_score = (passed_checks / total_checks * 100) if total_checks > 0 else 0

    # Step 7: Generate summary report
    print("\n" + "="*80)
    print("VALIDATION SUMMARY")
    print("="*80)

    passed_areas = sum(1 for r in validation_results if r.passed)
    failed_areas = len(validation_results) - passed_areas

    print(f"Areas validated: {len(validation_results)}")
    print(f"  Passed: {passed_areas}")
    print(f"  Failed: {failed_areas}")
    print(f"\nTotal checks performed: {total_checks}")
    print(f"  Passed: {passed_checks}")
    print(f"  Failed: {total_checks - passed_checks}")
    print(f"\nData Quality Score: {quality_score:.2f}%")

    if failed_areas > 0:
        print("\n" + "="*80)
        print("DISCREPANCIES FOUND")
        print("="*80)
        for result in validation_results:
            if not result.passed:
                print(f"\n{result.area_name}:")
                for disc in result.discrepancies:
                    print(f"  - {disc}")

    # Step 8: Export to Excel
    print("\n" + "="*80)
    print("EXPORTING VALIDATION REPORT")
    print("="*80)

    try:
        # Create summary sheet
        summary_data = {
            'Metric': [
                'Validation Date',
                'Database Path',
                'JSON Path',
                'Areas Validated',
                'Areas Passed',
                'Areas Failed',
                'Total Checks',
                'Checks Passed',
                'Checks Failed',
                'Data Quality Score (%)',
                'Formula Validation'
            ],
            'Value': [
                datetime.now().isoformat(),
                DB_PATH,
                JSON_PATH,
                len(validation_results),
                passed_areas,
                failed_areas,
                total_checks,
                passed_checks,
                total_checks - passed_checks,
                f"{quality_score:.2f}",
                "PASSED" if formula_valid else "FAILED"
            ]
        }
        summary_df = pd.DataFrame(summary_data)

        # Create detailed results sheet
        detail_rows = []
        for result in validation_results:
            for check in result.checks:
                detail_rows.append({
                    'Area': result.area_name,
                    'Check': check['check'],
                    'Expected': check['expected'],
                    'Actual': check['actual'],
                    'Status': 'PASS' if check['passed'] else 'FAIL'
                })
        detail_df = pd.DataFrame(detail_rows)

        # Create discrepancies sheet
        discrepancy_rows = []
        for result in validation_results:
            if not result.passed:
                for disc in result.discrepancies:
                    discrepancy_rows.append({
                        'Area': result.area_name,
                        'Discrepancy': disc
                    })

        if discrepancy_rows:
            discrepancy_df = pd.DataFrame(discrepancy_rows)
        else:
            discrepancy_df = pd.DataFrame({'Message': ['No discrepancies found - all validations passed!']})

        # Create area overview sheet
        area_overview_rows = []
        for result in validation_results:
            area_overview_rows.append({
                'Area': result.area_name,
                'Total Checks': len(result.checks),
                'Passed': sum(1 for c in result.checks if c['passed']),
                'Failed': sum(1 for c in result.checks if not c['passed']),
                'Status': 'PASS' if result.passed else 'FAIL',
                'Discrepancy Count': len(result.discrepancies)
            })
        area_overview_df = pd.DataFrame(area_overview_rows)

        # Write to Excel
        with pd.ExcelWriter(OUTPUT_PATH, engine='openpyxl') as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            area_overview_df.to_excel(writer, sheet_name='Area Overview', index=False)
            detail_df.to_excel(writer, sheet_name='Detailed Results', index=False)
            discrepancy_df.to_excel(writer, sheet_name='Discrepancies', index=False)

        print(f"Validation report exported to: {OUTPUT_PATH}")

    except Exception as e:
        print(f"ERROR: Failed to export report: {e}")
        import traceback
        traceback.print_exc()

    # Step 9: Final verdict
    print("\n" + "="*80)
    print("FINAL VERDICT")
    print("="*80)

    if quality_score == 100 and formula_valid:
        print("ALL VALIDATIONS PASSED")
        print("  The emerging hotspots data is ACCURATE and can be trusted.")
        exit_code = 0
    elif quality_score >= 95:
        print("MOSTLY PASSED WITH MINOR ISSUES")
        print(f"  Quality score: {quality_score:.2f}%")
        print("  Review discrepancies in the Excel report.")
        exit_code = 0
    else:
        print("VALIDATION FAILED")
        print(f"  Quality score: {quality_score:.2f}%")
        print("  CRITICAL: Significant discrepancies found!")
        print("  Review the Excel report immediately.")
        exit_code = 1

    conn.close()
    return exit_code

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
