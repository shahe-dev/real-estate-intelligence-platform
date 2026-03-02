# src/analytics/qa_validator.py

"""
QA Validation Framework for Dubai Real Estate Intelligence Platform
Validates calculated metrics against raw SQL queries from DuckDB
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
from config.bigquery_settings import bq_settings


class ValidationStatus(Enum):
    """Validation test status"""
    PASS = "PASS"
    FAIL = "FAIL"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"


@dataclass
class ValidationResult:
    """Result of a single validation test"""
    test_name: str
    status: ValidationStatus
    expected_value: Any
    actual_value: Any
    deviation: Optional[float] = None
    deviation_pct: Optional[float] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __str__(self):
        if self.status == ValidationStatus.PASS:
            return f"[PASS] {self.test_name}"
        elif self.status == ValidationStatus.FAIL:
            msg = f"[FAIL] {self.test_name}\n"
            msg += f"  Expected: {self.expected_value}\n"
            msg += f"  Actual:   {self.actual_value}\n"
            if self.deviation is not None:
                msg += f"  Deviation: {self.deviation:,.2f}"
            if self.deviation_pct is not None:
                msg += f" ({self.deviation_pct:.2f}%)"
            if self.error_message:
                msg += f"\n  Error: {self.error_message}"
            return msg
        elif self.status == ValidationStatus.ERROR:
            return f"[ERROR] {self.test_name}: {self.error_message}"
        else:
            return f"[SKIP] {self.test_name}: SKIPPED"


class QAValidator:
    """
    QA Validation Framework for Property Monitor Metrics
    Validates calculated metrics against raw SQL queries
    """

    def __init__(self, db_path: str = None, tolerance: float = 0.01):
        """
        Initialize QA Validator

        Args:
            db_path: Path to DuckDB database (default: property_monitor.db)
            tolerance: Tolerance threshold for floating point comparisons (default: 1%)
        """
        self.db_path = db_path or str(bq_settings.PM_DB_PATH)
        self.tolerance = tolerance
        self.con = duckdb.connect(self.db_path, read_only=True)
        self.results: List[ValidationResult] = []

    def close(self):
        """Close database connection"""
        if self.con:
            self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _calculate_deviation(self, expected: float, actual: float) -> Tuple[float, float]:
        """
        Calculate absolute and percentage deviation

        Args:
            expected: Expected value
            actual: Actual value

        Returns:
            Tuple of (absolute_deviation, percentage_deviation)
        """
        deviation = actual - expected
        if expected != 0:
            deviation_pct = (deviation / abs(expected)) * 100
        else:
            deviation_pct = 0 if actual == 0 else float('inf')
        return deviation, deviation_pct

    def _values_match(self, expected: float, actual: float, tolerance: float = None) -> bool:
        """
        Check if two values match within tolerance

        Args:
            expected: Expected value
            actual: Actual value
            tolerance: Override tolerance (default: self.tolerance)

        Returns:
            True if values match within tolerance
        """
        if tolerance is None:
            tolerance = self.tolerance

        if expected == 0 and actual == 0:
            return True

        if expected == 0:
            return abs(actual) <= tolerance

        deviation_pct = abs((actual - expected) / expected)
        return deviation_pct <= tolerance

    def _get_date_range_for_period(self, year: int, period_type: str,
                                   period_num: int) -> Tuple[str, str]:
        """
        Get date range for a given period

        Args:
            year: Year
            period_type: 'monthly', 'quarterly', 'semi-annual', 'annual'
            period_num: Period number (1-12 for monthly, 1-4 for quarterly, etc.)

        Returns:
            Tuple of (start_date, end_date) in YYYY-MM-DD format
        """
        if period_type == 'monthly':
            start_month = period_num
            end_month = period_num

        elif period_type == 'quarterly':
            start_month = (period_num - 1) * 3 + 1
            end_month = period_num * 3

        elif period_type == 'semi-annual':
            start_month = (period_num - 1) * 6 + 1
            end_month = period_num * 6

        elif period_type == 'annual':
            start_month = 1
            end_month = 12

        else:
            raise ValueError(f"Invalid period_type: {period_type}")

        start_date = f"{year}-{start_month:02d}-01"

        # Calculate end date (last day of end_month)
        if end_month == 12:
            end_date = f"{year}-12-31"
        else:
            # First day of next month minus 1 day
            import calendar
            last_day = calendar.monthrange(year, end_month)[1]
            end_date = f"{year}-{end_month:02d}-{last_day:02d}"

        return start_date, end_date

    def _build_period_filter(self, year: int, period_type: str,
                            period_num: int) -> str:
        """
        Build SQL WHERE clause for period filtering

        Args:
            year: Year
            period_type: 'monthly', 'quarterly', 'semi-annual', 'annual'
            period_num: Period number

        Returns:
            SQL WHERE clause string
        """
        if period_type == 'monthly':
            return f"transaction_year = {year} AND transaction_month = {period_num}"

        elif period_type == 'quarterly':
            start_month = (period_num - 1) * 3 + 1
            end_month = period_num * 3
            return f"transaction_year = {year} AND transaction_month BETWEEN {start_month} AND {end_month}"

        elif period_type == 'semi-annual':
            start_month = (period_num - 1) * 6 + 1
            end_month = period_num * 6
            return f"transaction_year = {year} AND transaction_month BETWEEN {start_month} AND {end_month}"

        elif period_type == 'annual':
            return f"transaction_year = {year}"

        else:
            raise ValueError(f"Invalid period_type: {period_type}")

    def validate_transaction_count(self, year: int, period_type: str,
                                   period_num: int, calculated_value: int,
                                   area_filter: str = None,
                                   property_type_filter: str = None) -> ValidationResult:
        """
        Validate transaction count against raw COUNT(*) query

        Args:
            year: Transaction year
            period_type: 'monthly', 'quarterly', 'semi-annual', 'annual'
            period_num: Period number
            calculated_value: Pre-calculated count to validate
            area_filter: Optional area filter (e.g., 'Dubai Marina')
            property_type_filter: Optional property type filter (e.g., 'Apartment')

        Returns:
            ValidationResult
        """
        test_name = f"transaction_count_{year}_{period_type}_P{period_num}"
        metadata = {
            'year': year,
            'period_type': period_type,
            'period_num': period_num,
            'area_filter': area_filter,
            'property_type_filter': property_type_filter
        }

        try:
            # Build WHERE clause
            where_clause = self._build_period_filter(year, period_type, period_num)

            if area_filter:
                where_clause += f" AND area_name_en = '{area_filter}'"
                test_name += f"_{area_filter.replace(' ', '_')}"

            if property_type_filter:
                where_clause += f" AND property_type_en = '{property_type_filter}'"
                test_name += f"_{property_type_filter.replace(' ', '_')}"

            # Execute raw SQL query
            query = f"""
                SELECT COUNT(*) as tx_count
                FROM transactions_clean
                WHERE {where_clause}
            """

            result = self.con.execute(query).fetchone()
            expected_value = result[0] if result else 0

            # Compare values
            if expected_value == calculated_value:
                status = ValidationStatus.PASS
                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_value,
                    metadata=metadata
                )
            else:
                status = ValidationStatus.FAIL
                deviation = calculated_value - expected_value
                deviation_pct = (deviation / expected_value * 100) if expected_value != 0 else 0

                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_value,
                    deviation=deviation,
                    deviation_pct=deviation_pct,
                    error_message=f"Count mismatch: expected {expected_value}, got {calculated_value}",
                    metadata=metadata
                )

            self.results.append(validation_result)
            return validation_result

        except Exception as e:
            validation_result = ValidationResult(
                test_name=test_name,
                status=ValidationStatus.ERROR,
                expected_value=None,
                actual_value=calculated_value,
                error_message=str(e),
                metadata=metadata
            )
            self.results.append(validation_result)
            return validation_result

    def validate_total_volume(self, year: int, period_type: str,
                             period_num: int, calculated_value: float,
                             area_filter: str = None,
                             property_type_filter: str = None,
                             tolerance: float = None) -> ValidationResult:
        """
        Validate total sales volume against raw SUM() query

        Args:
            year: Transaction year
            period_type: 'monthly', 'quarterly', 'semi-annual', 'annual'
            period_num: Period number
            calculated_value: Pre-calculated sum to validate
            area_filter: Optional area filter
            property_type_filter: Optional property type filter
            tolerance: Optional tolerance override

        Returns:
            ValidationResult
        """
        test_name = f"total_volume_{year}_{period_type}_P{period_num}"
        metadata = {
            'year': year,
            'period_type': period_type,
            'period_num': period_num,
            'area_filter': area_filter,
            'property_type_filter': property_type_filter
        }

        try:
            # Build WHERE clause
            where_clause = self._build_period_filter(year, period_type, period_num)

            if area_filter:
                where_clause += f" AND area_name_en = '{area_filter}'"
                test_name += f"_{area_filter.replace(' ', '_')}"

            if property_type_filter:
                where_clause += f" AND property_type_en = '{property_type_filter}'"
                test_name += f"_{property_type_filter.replace(' ', '_')}"

            # Execute raw SQL query
            query = f"""
                SELECT SUM(actual_worth) as total_volume
                FROM transactions_clean
                WHERE {where_clause}
            """

            result = self.con.execute(query).fetchone()
            expected_value = result[0] if result and result[0] else 0

            # Compare values with tolerance
            if self._values_match(expected_value, calculated_value, tolerance):
                status = ValidationStatus.PASS
                deviation, deviation_pct = self._calculate_deviation(expected_value, calculated_value)

                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_value,
                    deviation=deviation,
                    deviation_pct=deviation_pct,
                    metadata=metadata
                )
            else:
                status = ValidationStatus.FAIL
                deviation, deviation_pct = self._calculate_deviation(expected_value, calculated_value)

                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_value,
                    deviation=deviation,
                    deviation_pct=deviation_pct,
                    error_message=f"Volume mismatch beyond tolerance ({tolerance or self.tolerance:.2%})",
                    metadata=metadata
                )

            self.results.append(validation_result)
            return validation_result

        except Exception as e:
            validation_result = ValidationResult(
                test_name=test_name,
                status=ValidationStatus.ERROR,
                expected_value=None,
                actual_value=calculated_value,
                error_message=str(e),
                metadata=metadata
            )
            self.results.append(validation_result)
            return validation_result

    def validate_average_price(self, year: int, period_type: str,
                              period_num: int, calculated_value: float,
                              area_filter: str = None,
                              property_type_filter: str = None,
                              tolerance: float = None) -> ValidationResult:
        """
        Validate average price against raw AVG() query

        Args:
            year: Transaction year
            period_type: 'monthly', 'quarterly', 'semi-annual', 'annual'
            period_num: Period number
            calculated_value: Pre-calculated average to validate
            area_filter: Optional area filter
            property_type_filter: Optional property type filter
            tolerance: Optional tolerance override

        Returns:
            ValidationResult
        """
        test_name = f"avg_price_{year}_{period_type}_P{period_num}"
        metadata = {
            'year': year,
            'period_type': period_type,
            'period_num': period_num,
            'area_filter': area_filter,
            'property_type_filter': property_type_filter
        }

        try:
            # Build WHERE clause
            where_clause = self._build_period_filter(year, period_type, period_num)

            if area_filter:
                where_clause += f" AND area_name_en = '{area_filter}'"
                test_name += f"_{area_filter.replace(' ', '_')}"

            if property_type_filter:
                where_clause += f" AND property_type_en = '{property_type_filter}'"
                test_name += f"_{property_type_filter.replace(' ', '_')}"

            # Execute raw SQL query
            query = f"""
                SELECT AVG(actual_worth) as avg_price
                FROM transactions_clean
                WHERE {where_clause}
            """

            result = self.con.execute(query).fetchone()
            expected_value = result[0] if result and result[0] else 0

            # Compare values with tolerance
            if self._values_match(expected_value, calculated_value, tolerance):
                status = ValidationStatus.PASS
                deviation, deviation_pct = self._calculate_deviation(expected_value, calculated_value)

                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_value,
                    deviation=deviation,
                    deviation_pct=deviation_pct,
                    metadata=metadata
                )
            else:
                status = ValidationStatus.FAIL
                deviation, deviation_pct = self._calculate_deviation(expected_value, calculated_value)

                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_value,
                    deviation=deviation,
                    deviation_pct=deviation_pct,
                    error_message=f"Average price mismatch beyond tolerance ({tolerance or self.tolerance:.2%})",
                    metadata=metadata
                )

            self.results.append(validation_result)
            return validation_result

        except Exception as e:
            validation_result = ValidationResult(
                test_name=test_name,
                status=ValidationStatus.ERROR,
                expected_value=None,
                actual_value=calculated_value,
                error_message=str(e),
                metadata=metadata
            )
            self.results.append(validation_result)
            return validation_result

    def validate_percentage_calculation(self, numerator: float, denominator: float,
                                       calculated_percentage: float,
                                       test_name: str = "percentage_calc",
                                       tolerance: float = None) -> ValidationResult:
        """
        Validate percentage calculation is mathematically correct

        Args:
            numerator: Numerator value
            denominator: Denominator value
            calculated_percentage: Pre-calculated percentage to validate
            test_name: Name for this test
            tolerance: Optional tolerance override

        Returns:
            ValidationResult
        """
        metadata = {
            'numerator': numerator,
            'denominator': denominator
        }

        try:
            if denominator == 0:
                if calculated_percentage == 0:
                    status = ValidationStatus.PASS
                    expected_value = 0
                else:
                    status = ValidationStatus.FAIL
                    expected_value = 0
                    validation_result = ValidationResult(
                        test_name=test_name,
                        status=status,
                        expected_value=expected_value,
                        actual_value=calculated_percentage,
                        error_message="Denominator is 0, percentage should be 0",
                        metadata=metadata
                    )
                    self.results.append(validation_result)
                    return validation_result

            expected_value = (numerator / denominator) * 100

            # Compare values with tolerance
            if self._values_match(expected_value, calculated_percentage, tolerance):
                status = ValidationStatus.PASS
                deviation, deviation_pct = self._calculate_deviation(expected_value, calculated_percentage)

                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_percentage,
                    deviation=deviation,
                    deviation_pct=deviation_pct,
                    metadata=metadata
                )
            else:
                status = ValidationStatus.FAIL
                deviation, deviation_pct = self._calculate_deviation(expected_value, calculated_percentage)

                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=expected_value,
                    actual_value=calculated_percentage,
                    deviation=deviation,
                    deviation_pct=deviation_pct,
                    error_message=f"Percentage calculation incorrect: ({numerator}/{denominator})*100",
                    metadata=metadata
                )

            self.results.append(validation_result)
            return validation_result

        except Exception as e:
            validation_result = ValidationResult(
                test_name=test_name,
                status=ValidationStatus.ERROR,
                expected_value=None,
                actual_value=calculated_percentage,
                error_message=str(e),
                metadata=metadata
            )
            self.results.append(validation_result)
            return validation_result

    def validate_period_boundaries(self, year: int, period_type: str,
                                   period_num: int) -> ValidationResult:
        """
        Validate that period boundaries (quarterly, semi-annual) are correctly defined

        Args:
            year: Year
            period_type: 'quarterly' or 'semi-annual'
            period_num: Period number

        Returns:
            ValidationResult
        """
        test_name = f"period_boundaries_{year}_{period_type}_P{period_num}"
        metadata = {
            'year': year,
            'period_type': period_type,
            'period_num': period_num
        }

        try:
            if period_type == 'quarterly':
                expected_months = {
                    1: [1, 2, 3],
                    2: [4, 5, 6],
                    3: [7, 8, 9],
                    4: [10, 11, 12]
                }
            elif period_type == 'semi-annual':
                expected_months = {
                    1: [1, 2, 3, 4, 5, 6],
                    2: [7, 8, 9, 10, 11, 12]
                }
            else:
                validation_result = ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.SKIPPED,
                    expected_value=None,
                    actual_value=None,
                    error_message=f"Period type {period_type} does not require boundary validation",
                    metadata=metadata
                )
                self.results.append(validation_result)
                return validation_result

            if period_num not in expected_months:
                validation_result = ValidationResult(
                    test_name=test_name,
                    status=ValidationStatus.FAIL,
                    expected_value=list(expected_months.keys()),
                    actual_value=period_num,
                    error_message=f"Invalid period_num {period_num} for {period_type}",
                    metadata=metadata
                )
                self.results.append(validation_result)
                return validation_result

            months = expected_months[period_num]
            start_month = months[0]
            end_month = months[-1]

            # Query to check transactions fall within expected months
            query = f"""
                SELECT
                    MIN(transaction_month) as min_month,
                    MAX(transaction_month) as max_month,
                    COUNT(DISTINCT transaction_month) as distinct_months
                FROM transactions_clean
                WHERE transaction_year = {year}
                  AND transaction_month BETWEEN {start_month} AND {end_month}
            """

            result = self.con.execute(query).fetchone()

            if result and result[0]:
                min_month, max_month, distinct_months = result

                # Verify months are within expected range
                if min_month >= start_month and max_month <= end_month:
                    status = ValidationStatus.PASS
                    validation_result = ValidationResult(
                        test_name=test_name,
                        status=status,
                        expected_value=f"Months {start_month}-{end_month}",
                        actual_value=f"Months {min_month}-{max_month} ({distinct_months} distinct)",
                        metadata=metadata
                    )
                else:
                    status = ValidationStatus.FAIL
                    validation_result = ValidationResult(
                        test_name=test_name,
                        status=status,
                        expected_value=f"Months {start_month}-{end_month}",
                        actual_value=f"Months {min_month}-{max_month}",
                        error_message=f"Period boundary violation",
                        metadata=metadata
                    )
            else:
                status = ValidationStatus.PASS
                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=f"Months {start_month}-{end_month}",
                    actual_value="No data in period",
                    metadata=metadata
                )

            self.results.append(validation_result)
            return validation_result

        except Exception as e:
            validation_result = ValidationResult(
                test_name=test_name,
                status=ValidationStatus.ERROR,
                expected_value=None,
                actual_value=None,
                error_message=str(e),
                metadata=metadata
            )
            self.results.append(validation_result)
            return validation_result

    def validate_date_range_filter(self, year: int, period_type: str,
                                   period_num: int) -> ValidationResult:
        """
        Validate that date range filtering works correctly

        Args:
            year: Year
            period_type: 'monthly', 'quarterly', 'semi-annual', 'annual'
            period_num: Period number

        Returns:
            ValidationResult
        """
        test_name = f"date_range_filter_{year}_{period_type}_P{period_num}"
        metadata = {
            'year': year,
            'period_type': period_type,
            'period_num': period_num
        }

        try:
            where_clause = self._build_period_filter(year, period_type, period_num)

            # Get all transactions in the period
            query = f"""
                SELECT
                    MIN(transaction_year) as min_year,
                    MAX(transaction_year) as max_year,
                    MIN(transaction_month) as min_month,
                    MAX(transaction_month) as max_month,
                    COUNT(*) as tx_count
                FROM transactions_clean
                WHERE {where_clause}
            """

            result = self.con.execute(query).fetchone()

            if result and result[4] > 0:  # tx_count > 0
                min_year, max_year, min_month, max_month, tx_count = result

                # All transactions should be in the specified year
                if min_year == year and max_year == year:
                    status = ValidationStatus.PASS
                    validation_result = ValidationResult(
                        test_name=test_name,
                        status=status,
                        expected_value=f"Year {year}",
                        actual_value=f"Year range {min_year}-{max_year}, {tx_count} transactions",
                        metadata=metadata
                    )
                else:
                    status = ValidationStatus.FAIL
                    validation_result = ValidationResult(
                        test_name=test_name,
                        status=status,
                        expected_value=f"Year {year}",
                        actual_value=f"Year range {min_year}-{max_year}",
                        error_message=f"Date range filter includes transactions from other years",
                        metadata=metadata
                    )
            else:
                status = ValidationStatus.PASS
                validation_result = ValidationResult(
                    test_name=test_name,
                    status=status,
                    expected_value=f"Year {year}",
                    actual_value="No data in period",
                    metadata=metadata
                )

            self.results.append(validation_result)
            return validation_result

        except Exception as e:
            validation_result = ValidationResult(
                test_name=test_name,
                status=ValidationStatus.ERROR,
                expected_value=None,
                actual_value=None,
                error_message=str(e),
                metadata=metadata
            )
            self.results.append(validation_result)
            return validation_result

    def run_all_validations(self, metrics_dict: Dict[str, Any],
                           area_filter: str = None,
                           property_type_filter: str = None) -> List[ValidationResult]:
        """
        Run all validations for a set of metrics

        Args:
            metrics_dict: Dictionary containing metrics to validate
                Expected keys: 'year', 'period_type', 'period_num',
                              'transaction_count', 'total_volume', 'avg_price', etc.
            area_filter: Optional area filter
            property_type_filter: Optional property type filter

        Returns:
            List of ValidationResult objects
        """
        results = []

        year = metrics_dict.get('year')
        period_type = metrics_dict.get('period_type')
        period_num = metrics_dict.get('period_num')

        if not all([year, period_type, period_num]):
            error_result = ValidationResult(
                test_name="run_all_validations",
                status=ValidationStatus.ERROR,
                expected_value=None,
                actual_value=None,
                error_message="Missing required fields: year, period_type, period_num"
            )
            self.results.append(error_result)
            return [error_result]

        # Validate period boundaries
        if period_type in ['quarterly', 'semi-annual']:
            results.append(self.validate_period_boundaries(year, period_type, period_num))

        # Validate date range filtering
        results.append(self.validate_date_range_filter(year, period_type, period_num))

        # Validate transaction count
        if 'transaction_count' in metrics_dict:
            results.append(self.validate_transaction_count(
                year, period_type, period_num,
                metrics_dict['transaction_count'],
                area_filter, property_type_filter
            ))

        # Validate total volume
        if 'total_volume' in metrics_dict:
            results.append(self.validate_total_volume(
                year, period_type, period_num,
                metrics_dict['total_volume'],
                area_filter, property_type_filter
            ))

        # Validate average price
        if 'avg_price' in metrics_dict:
            results.append(self.validate_average_price(
                year, period_type, period_num,
                metrics_dict['avg_price'],
                area_filter, property_type_filter
            ))

        # Validate percentage calculations
        if all(k in metrics_dict for k in ['offplan_count', 'transaction_count', 'offplan_percentage']):
            results.append(self.validate_percentage_calculation(
                metrics_dict['offplan_count'],
                metrics_dict['transaction_count'],
                metrics_dict['offplan_percentage'],
                test_name=f"offplan_pct_{year}_{period_type}_P{period_num}"
            ))

        if all(k in metrics_dict for k in ['luxury_count', 'transaction_count', 'luxury_percentage']):
            results.append(self.validate_percentage_calculation(
                metrics_dict['luxury_count'],
                metrics_dict['transaction_count'],
                metrics_dict['luxury_percentage'],
                test_name=f"luxury_pct_{year}_{period_type}_P{period_num}"
            ))

        return results

    def generate_validation_report(self, title: str = "QA Validation Report") -> str:
        """
        Generate a summary validation report

        Args:
            title: Report title

        Returns:
            Formatted report string
        """
        if not self.results:
            return f"\n{title}\n{'='*60}\nNo validation tests have been run.\n"

        # Count results by status
        status_counts = {
            ValidationStatus.PASS: 0,
            ValidationStatus.FAIL: 0,
            ValidationStatus.ERROR: 0,
            ValidationStatus.SKIPPED: 0
        }

        for result in self.results:
            status_counts[result.status] += 1

        total_tests = len(self.results)
        pass_rate = (status_counts[ValidationStatus.PASS] / total_tests * 100) if total_tests > 0 else 0

        # Build report
        report = []
        report.append("\n" + "="*80)
        report.append(f"{title}")
        report.append("="*80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"Database: {self.db_path}")
        report.append(f"Tolerance: {self.tolerance:.2%}")
        report.append("")
        report.append("SUMMARY")
        report.append("-" * 80)
        report.append(f"Total Tests:    {total_tests}")
        report.append(f"Passed:         {status_counts[ValidationStatus.PASS]} ({status_counts[ValidationStatus.PASS]/total_tests*100:.1f}%)")
        report.append(f"Failed:         {status_counts[ValidationStatus.FAIL]} ({status_counts[ValidationStatus.FAIL]/total_tests*100:.1f}%)")
        report.append(f"Errors:         {status_counts[ValidationStatus.ERROR]} ({status_counts[ValidationStatus.ERROR]/total_tests*100:.1f}%)")
        report.append(f"Skipped:        {status_counts[ValidationStatus.SKIPPED]} ({status_counts[ValidationStatus.SKIPPED]/total_tests*100:.1f}%)")
        report.append(f"Pass Rate:      {pass_rate:.1f}%")
        report.append("")

        # Group results by status
        passed = [r for r in self.results if r.status == ValidationStatus.PASS]
        failed = [r for r in self.results if r.status == ValidationStatus.FAIL]
        errors = [r for r in self.results if r.status == ValidationStatus.ERROR]
        skipped = [r for r in self.results if r.status == ValidationStatus.SKIPPED]

        # Show failed tests first
        if failed:
            report.append("FAILED TESTS")
            report.append("-" * 80)
            for result in failed:
                report.append(str(result))
                report.append("")

        # Show errors
        if errors:
            report.append("ERROR TESTS")
            report.append("-" * 80)
            for result in errors:
                report.append(str(result))
                report.append("")

        # Show passed tests (summarized)
        if passed:
            report.append("PASSED TESTS")
            report.append("-" * 80)
            for result in passed:
                report.append(str(result))

        # Show skipped tests
        if skipped:
            report.append("")
            report.append("SKIPPED TESTS")
            report.append("-" * 80)
            for result in skipped:
                report.append(str(result))

        report.append("")
        report.append("="*80)

        return "\n".join(report)

    def clear_results(self):
        """Clear all validation results"""
        self.results = []

    def get_summary(self) -> Dict[str, Any]:
        """
        Get validation summary as dictionary

        Returns:
            Dictionary with summary statistics
        """
        if not self.results:
            return {
                'total_tests': 0,
                'passed': 0,
                'failed': 0,
                'errors': 0,
                'skipped': 0,
                'pass_rate': 0.0
            }

        status_counts = {
            'passed': len([r for r in self.results if r.status == ValidationStatus.PASS]),
            'failed': len([r for r in self.results if r.status == ValidationStatus.FAIL]),
            'errors': len([r for r in self.results if r.status == ValidationStatus.ERROR]),
            'skipped': len([r for r in self.results if r.status == ValidationStatus.SKIPPED])
        }

        total = len(self.results)
        pass_rate = (status_counts['passed'] / total * 100) if total > 0 else 0

        return {
            'total_tests': total,
            'passed': status_counts['passed'],
            'failed': status_counts['failed'],
            'errors': status_counts['errors'],
            'skipped': status_counts['skipped'],
            'pass_rate': pass_rate
        }


def run_sample_validation():
    """Sample validation run for testing"""
    print("Running Sample QA Validation...")
    print("="*80)

    with QAValidator(tolerance=0.01) as validator:
        # Sample validation for 2024 Q1
        print("\nValidating 2024 Q1 Metrics...")

        # Get actual metrics from database
        con = validator.con

        # Get Q1 2024 data
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
            offplan_pct = (offplan_count / tx_count * 100) if tx_count > 0 else 0
            luxury_pct = (luxury_count / tx_count * 100) if tx_count > 0 else 0

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

            # Run all validations
            validator.run_all_validations(metrics)

            # Print report
            print(validator.generate_validation_report())
        else:
            print("No data found for 2024 Q1")

        # Additional test: validate specific area
        print("\n" + "="*80)
        print("Validating Dubai Marina 2024 Monthly Metrics...")

        result = con.execute("""
            SELECT
                COUNT(*) as tx_count,
                SUM(actual_worth) as total_volume,
                AVG(actual_worth) as avg_price
            FROM transactions_clean
            WHERE transaction_year = 2024
              AND transaction_month = 1
              AND area_name_en = 'Dubai Marina'
        """).fetchone()

        if result and result[0] > 0:
            validator.clear_results()  # Clear previous results

            tx_count, total_volume, avg_price = result

            validator.validate_transaction_count(2024, 'monthly', 1, tx_count,
                                                area_filter='Dubai Marina')
            validator.validate_total_volume(2024, 'monthly', 1, total_volume,
                                          area_filter='Dubai Marina')
            validator.validate_average_price(2024, 'monthly', 1, avg_price,
                                           area_filter='Dubai Marina')

            print(validator.generate_validation_report("Dubai Marina January 2024 Validation"))
        else:
            print("No data found for Dubai Marina January 2024")


if __name__ == "__main__":
    run_sample_validation()
