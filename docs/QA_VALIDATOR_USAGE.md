# QA Validation Framework - Usage Guide

## Overview

The QA Validation Framework provides comprehensive validation capabilities for the Dubai Real Estate Intelligence Platform. It validates calculated metrics against raw SQL queries from the DuckDB database to ensure data accuracy and consistency.

## Features

- **Transaction Count Validation**: Validates COUNT(*) queries
- **Total Volume Validation**: Validates SUM() calculations
- **Average Price Validation**: Validates AVG() calculations
- **Percentage Validation**: Ensures percentage calculations are mathematically correct
- **Period Boundary Validation**: Validates quarterly and semi-annual period definitions
- **Date Range Validation**: Ensures date filtering works correctly
- **Flexible Filtering**: Supports area and property type filters
- **Tolerance Thresholds**: Configurable tolerance for floating-point comparisons
- **Comprehensive Reporting**: Detailed validation reports with pass/fail status

## Installation

The QA Validator is part of the analytics module:

```python
from src.analytics import QAValidator, ValidationResult, ValidationStatus
```

## Quick Start

### Basic Usage

```python
from src.analytics import QAValidator

# Initialize validator with default tolerance (1%)
with QAValidator(tolerance=0.01) as validator:
    # Validate transaction count
    result = validator.validate_transaction_count(
        year=2024,
        period_type='monthly',
        period_num=1,
        calculated_value=15000
    )

    print(result)
```

### Validating a Complete Metrics Set

```python
from src.analytics import QAValidator

# Prepare metrics dictionary
metrics = {
    'year': 2024,
    'period_type': 'quarterly',
    'period_num': 1,
    'transaction_count': 45000,
    'total_volume': 12500000000,
    'avg_price': 850000,
    'offplan_count': 22500,
    'luxury_count': 4500,
    'offplan_percentage': 50.0,
    'luxury_percentage': 10.0
}

# Run all validations
with QAValidator() as validator:
    validator.run_all_validations(metrics)

    # Generate report
    print(validator.generate_validation_report())
```

## API Reference

### QAValidator Class

#### Constructor

```python
QAValidator(db_path=None, tolerance=0.01)
```

**Parameters:**
- `db_path` (str, optional): Path to DuckDB database. Defaults to property_monitor.db
- `tolerance` (float, optional): Tolerance threshold for floating-point comparisons. Default: 0.01 (1%)

#### Methods

##### validate_transaction_count()

Validates transaction count against raw COUNT(*) query.

```python
validate_transaction_count(
    year: int,
    period_type: str,
    period_num: int,
    calculated_value: int,
    area_filter: str = None,
    property_type_filter: str = None
) -> ValidationResult
```

**Parameters:**
- `year`: Transaction year
- `period_type`: One of 'monthly', 'quarterly', 'semi-annual', 'annual'
- `period_num`: Period number (1-12 for monthly, 1-4 for quarterly, etc.)
- `calculated_value`: Pre-calculated count to validate
- `area_filter`: Optional area filter (e.g., 'Dubai Marina')
- `property_type_filter`: Optional property type filter (e.g., 'Apartment')

**Example:**
```python
validator.validate_transaction_count(
    year=2024,
    period_type='quarterly',
    period_num=1,
    calculated_value=45000,
    area_filter='Dubai Marina'
)
```

##### validate_total_volume()

Validates total sales volume against raw SUM() query.

```python
validate_total_volume(
    year: int,
    period_type: str,
    period_num: int,
    calculated_value: float,
    area_filter: str = None,
    property_type_filter: str = None,
    tolerance: float = None
) -> ValidationResult
```

**Parameters:**
- Same as `validate_transaction_count()`
- `tolerance`: Optional tolerance override for this validation

**Example:**
```python
validator.validate_total_volume(
    year=2024,
    period_type='monthly',
    period_num=1,
    calculated_value=3500000000,
    tolerance=0.001  # 0.1% tolerance
)
```

##### validate_average_price()

Validates average price against raw AVG() query.

```python
validate_average_price(
    year: int,
    period_type: str,
    period_num: int,
    calculated_value: float,
    area_filter: str = None,
    property_type_filter: str = None,
    tolerance: float = None
) -> ValidationResult
```

**Example:**
```python
validator.validate_average_price(
    year=2024,
    period_type='quarterly',
    period_num=1,
    calculated_value=850000,
    property_type_filter='Unit'
)
```

##### validate_percentage_calculation()

Validates that a percentage calculation is mathematically correct.

```python
validate_percentage_calculation(
    numerator: float,
    denominator: float,
    calculated_percentage: float,
    test_name: str = "percentage_calc",
    tolerance: float = None
) -> ValidationResult
```

**Example:**
```python
# Validate off-plan percentage: (22500 / 45000) * 100 = 50%
validator.validate_percentage_calculation(
    numerator=22500,
    denominator=45000,
    calculated_percentage=50.0,
    test_name="offplan_percentage"
)
```

##### validate_period_boundaries()

Validates that period boundaries (quarterly, semi-annual) are correctly defined.

```python
validate_period_boundaries(
    year: int,
    period_type: str,
    period_num: int
) -> ValidationResult
```

**Example:**
```python
# Validate Q1 includes only months 1-3
validator.validate_period_boundaries(
    year=2024,
    period_type='quarterly',
    period_num=1
)
```

##### validate_date_range_filter()

Validates that date range filtering works correctly.

```python
validate_date_range_filter(
    year: int,
    period_type: str,
    period_num: int
) -> ValidationResult
```

##### run_all_validations()

Runs all applicable validations for a metrics dictionary.

```python
run_all_validations(
    metrics_dict: Dict[str, Any],
    area_filter: str = None,
    property_type_filter: str = None
) -> List[ValidationResult]
```

**Required metrics_dict keys:**
- `year`: int
- `period_type`: str
- `period_num`: int

**Optional metrics_dict keys:**
- `transaction_count`: int
- `total_volume`: float
- `avg_price`: float
- `offplan_count`: int
- `offplan_percentage`: float
- `luxury_count`: int
- `luxury_percentage`: float

##### generate_validation_report()

Generates a comprehensive validation report.

```python
generate_validation_report(title: str = "QA Validation Report") -> str
```

**Example:**
```python
report = validator.generate_validation_report("Q1 2024 Validation")
print(report)
```

##### get_summary()

Returns validation summary as a dictionary.

```python
get_summary() -> Dict[str, Any]
```

**Returns:**
```python
{
    'total_tests': 15,
    'passed': 14,
    'failed': 1,
    'errors': 0,
    'skipped': 0,
    'pass_rate': 93.3
}
```

## Period Types

The validator supports four period types:

### Monthly
- `period_num`: 1-12 (representing months)
- Example: `period_type='monthly', period_num=1` = January

### Quarterly
- `period_num`: 1-4 (representing quarters)
- Q1: Months 1-3
- Q2: Months 4-6
- Q3: Months 7-9
- Q4: Months 10-12

### Semi-Annual
- `period_num`: 1-2 (representing half-years)
- H1: Months 1-6
- H2: Months 7-12

### Annual
- `period_num`: 1 (always 1 for annual)
- Covers all 12 months

## Advanced Examples

### Example 1: Validate Specific Area with Custom Tolerance

```python
with QAValidator(tolerance=0.005) as validator:
    # Validate Dubai Marina January 2024
    result = validator.validate_average_price(
        year=2024,
        period_type='monthly',
        period_num=1,
        calculated_value=1250000,
        area_filter='Dubai Marina',
        tolerance=0.001  # Stricter 0.1% tolerance
    )

    if result.status == ValidationStatus.PASS:
        print("Validation passed!")
    else:
        print(f"Validation failed: {result.error_message}")
        print(f"Expected: {result.expected_value:,.2f}")
        print(f"Actual: {result.actual_value:,.2f}")
        print(f"Deviation: {result.deviation_pct:.2f}%")
```

### Example 2: Batch Validation for Multiple Periods

```python
with QAValidator() as validator:
    periods = [
        (2024, 'monthly', 1),
        (2024, 'monthly', 2),
        (2024, 'monthly', 3),
        (2024, 'quarterly', 1)
    ]

    for year, period_type, period_num in periods:
        # Get data from your metrics source
        metrics = get_metrics(year, period_type, period_num)

        # Validate
        validator.run_all_validations(metrics)

    # Generate comprehensive report
    report = validator.generate_validation_report("Q1 2024 Multi-Period Validation")
    print(report)

    # Get summary stats
    summary = validator.get_summary()
    print(f"\nPass Rate: {summary['pass_rate']:.1f}%")
```

### Example 3: Validation Pipeline Integration

```python
def validate_calculated_metrics(metrics_df):
    """
    Validate metrics in a pandas DataFrame
    """
    with QAValidator(tolerance=0.01) as validator:
        failed_validations = []

        for _, row in metrics_df.iterrows():
            metrics = {
                'year': row['year'],
                'period_type': row['period_type'],
                'period_num': row['period_num'],
                'transaction_count': row['tx_count'],
                'total_volume': row['total_volume'],
                'avg_price': row['avg_price']
            }

            results = validator.run_all_validations(metrics)

            # Collect failures
            for result in results:
                if result.status == ValidationStatus.FAIL:
                    failed_validations.append({
                        'test': result.test_name,
                        'year': row['year'],
                        'period': f"{row['period_type']}-{row['period_num']}",
                        'deviation_pct': result.deviation_pct
                    })

        if failed_validations:
            print(f"⚠ {len(failed_validations)} validation(s) failed!")
            return False, failed_validations
        else:
            print("✓ All validations passed!")
            return True, []
```

### Example 4: Continuous Monitoring

```python
import schedule
import time

def daily_validation_check():
    """Run daily QA validation checks"""
    with QAValidator() as validator:
        # Get yesterday's metrics
        yesterday = datetime.now() - timedelta(days=1)
        metrics = get_daily_metrics(yesterday)

        # Validate
        validator.run_all_validations(metrics)

        # Generate report
        report = validator.generate_validation_report(
            f"Daily Validation - {yesterday.strftime('%Y-%m-%d')}"
        )

        # Save report
        with open(f"qa_reports/daily_{yesterday.strftime('%Y%m%d')}.txt", 'w') as f:
            f.write(report)

        # Alert on failures
        summary = validator.get_summary()
        if summary['failed'] > 0:
            send_alert(f"QA Validation Failed: {summary['failed']} tests")

# Schedule daily at 2 AM
schedule.every().day.at("02:00").do(daily_validation_check)
```

## ValidationResult Object

Each validation returns a `ValidationResult` object with the following attributes:

- `test_name`: Name of the test
- `status`: ValidationStatus enum (PASS, FAIL, ERROR, SKIPPED)
- `expected_value`: Expected value from database
- `actual_value`: Actual calculated value
- `deviation`: Absolute deviation (actual - expected)
- `deviation_pct`: Percentage deviation
- `error_message`: Error description (if failed)
- `metadata`: Additional context information

## Best Practices

1. **Set Appropriate Tolerance**: Use stricter tolerance (0.001) for critical metrics, lenient (0.05) for aggregated data
2. **Run Regular Validations**: Integrate into CI/CD pipeline or scheduled jobs
3. **Archive Reports**: Save validation reports for audit trail
4. **Monitor Trends**: Track validation pass rates over time
5. **Alert on Failures**: Set up notifications for validation failures
6. **Validate Before Publishing**: Always validate metrics before exposing via API

## Troubleshooting

### Common Issues

**Issue**: All validations fail with high deviations
- **Cause**: Database may be out of sync with calculated metrics
- **Solution**: Rebuild metrics tables using `rebuild_pm_metrics()`

**Issue**: Floating-point precision errors
- **Cause**: Default tolerance too strict
- **Solution**: Increase tolerance or use custom tolerance per metric type

**Issue**: Period boundary validation fails
- **Cause**: Data spans incorrect months
- **Solution**: Check data import logic and date parsing

## Running Tests

Run the comprehensive test suite:

```bash
python tests/test_qa_validator.py
```

Run the sample validation:

```bash
python src/analytics/qa_validator.py
```

## Support

For issues or questions, refer to the main project documentation or contact the development team.
