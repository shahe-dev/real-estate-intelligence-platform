# QA Validator - Quick Start Guide

## Installation

Already installed! Part of the Dubai Real Estate Intelligence Platform.

```python
from src.analytics import QAValidator
```

## 5-Minute Quick Start

### 1. Basic Validation (30 seconds)

```python
from src.analytics import QAValidator

# Validate a single metric
with QAValidator() as validator:
    result = validator.validate_transaction_count(
        year=2024,
        period_type='monthly',
        period_num=1,
        calculated_value=10709
    )
    print(result)  # [PASS] transaction_count_2024_monthly_P1
```

### 2. Validate Multiple Metrics (1 minute)

```python
with QAValidator() as validator:
    # Get data from database
    result = validator.con.execute("""
        SELECT COUNT(*), SUM(actual_worth), AVG(actual_worth)
        FROM transactions_clean
        WHERE transaction_year = 2024 AND transaction_month = 1
    """).fetchone()

    count, volume, avg = result

    # Validate all three
    validator.validate_transaction_count(2024, 'monthly', 1, count)
    validator.validate_total_volume(2024, 'monthly', 1, volume)
    validator.validate_average_price(2024, 'monthly', 1, avg)

    # Get summary
    summary = validator.get_summary()
    print(f"Pass Rate: {summary['pass_rate']:.1f}%")
```

### 3. Comprehensive Validation (2 minutes)

```python
# Prepare metrics dictionary
metrics = {
    'year': 2024,
    'period_type': 'quarterly',
    'period_num': 1,
    'transaction_count': 34255,
    'total_volume': 88244831920,
    'avg_price': 2576115.37,
    'offplan_count': 23157,
    'luxury_count': 14272,
    'offplan_percentage': 67.58,
    'luxury_percentage': 41.66
}

# Validate everything at once
with QAValidator() as validator:
    validator.run_all_validations(metrics)
    print(validator.generate_validation_report())
```

### 4. Area-Specific Validation (1 minute)

```python
with QAValidator() as validator:
    result = validator.validate_average_price(
        year=2024,
        period_type='monthly',
        period_num=1,
        calculated_value=2361277.41,
        area_filter='Dubai Marina'
    )
    print(result)
```

### 5. Custom Tolerance (30 seconds)

```python
# Strict validation (0.1% tolerance)
with QAValidator(tolerance=0.001) as validator:
    result = validator.validate_total_volume(
        2024, 'monthly', 1, 28990954397
    )

# Lenient validation (5% tolerance)
with QAValidator(tolerance=0.05) as validator:
    result = validator.validate_average_price(
        2024, 'monthly', 1, 2707157.94
    )
```

## Period Types Cheat Sheet

```python
# Monthly (1-12)
validator.validate_transaction_count(2024, 'monthly', 1, count)  # January

# Quarterly (1-4)
validator.validate_transaction_count(2024, 'quarterly', 1, count)  # Q1 (Jan-Mar)
validator.validate_transaction_count(2024, 'quarterly', 2, count)  # Q2 (Apr-Jun)

# Semi-Annual (1-2)
validator.validate_transaction_count(2024, 'semi-annual', 1, count)  # H1 (Jan-Jun)
validator.validate_transaction_count(2024, 'semi-annual', 2, count)  # H2 (Jul-Dec)

# Annual (always 1)
validator.validate_transaction_count(2024, 'annual', 1, count)  # Full year
```

## Common Patterns

### Pattern 1: Pre-Deployment Check

```python
def validate_before_deploy(metrics):
    with QAValidator() as validator:
        validator.run_all_validations(metrics)
        if validator.get_summary()['pass_rate'] < 100:
            raise Exception("Validation failed!")
        return True
```

### Pattern 2: Daily Monitoring

```python
def daily_qa():
    with QAValidator() as validator:
        metrics = get_yesterday_metrics()
        validator.run_all_validations(metrics)

        if validator.get_summary()['failed'] > 0:
            send_alert()
```

### Pattern 3: API Validation

```python
@app.get("/api/metrics")
def get_metrics():
    metrics = calculate_metrics()

    with QAValidator() as validator:
        validator.run_all_validations(metrics)

    return metrics
```

### Pattern 4: Debugging

```python
def debug_metric(value):
    with QAValidator() as validator:
        result = validator.validate_average_price(
            2024, 'monthly', 1, value
        )

        if result.status.value == "FAIL":
            print(f"Expected: {result.expected_value:,.2f}")
            print(f"Got: {result.actual_value:,.2f}")
            print(f"Deviation: {result.deviation_pct:.2f}%")
```

## Quick Reference

### Available Validations

```python
# Count validation (exact match)
validate_transaction_count(year, period_type, period_num, value, area_filter, property_type_filter)

# Volume validation (with tolerance)
validate_total_volume(year, period_type, period_num, value, area_filter, property_type_filter, tolerance)

# Average validation (with tolerance)
validate_average_price(year, period_type, period_num, value, area_filter, property_type_filter, tolerance)

# Percentage validation
validate_percentage_calculation(numerator, denominator, percentage, test_name, tolerance)

# Period boundary check
validate_period_boundaries(year, period_type, period_num)

# Date range check
validate_date_range_filter(year, period_type, period_num)

# Batch validation
run_all_validations(metrics_dict, area_filter, property_type_filter)
```

### Reporting

```python
# Generate report
report = validator.generate_validation_report("My Report Title")
print(report)

# Get summary dictionary
summary = validator.get_summary()
# Returns: {'total_tests': 7, 'passed': 7, 'failed': 0, 'errors': 0, 'skipped': 0, 'pass_rate': 100.0}

# Clear results
validator.clear_results()
```

## Running Examples

```bash
# Sample validation
python src/analytics/qa_validator.py

# Comprehensive tests
python tests/test_qa_validator.py

# Practical examples
python examples/example_qa_validation.py

# Integration validation
python tests/validate_metrics_integration.py
```

## Tolerance Guide

| Use Case | Tolerance | Value |
|----------|-----------|-------|
| Critical metrics | Strict | 0.001 (0.1%) |
| Standard metrics | Normal | 0.01 (1%) |
| Aggregated data | Lenient | 0.05 (5%) |

## Common Errors

### Error 1: Division by Zero
```python
# Handle zero denominator
validator.validate_percentage_calculation(0, 0, 0)  # Returns PASS (0/0 = 0%)
```

### Error 2: Database Connection
```python
# Use context manager for automatic cleanup
with QAValidator() as validator:
    # Your code here
    pass
# Connection automatically closed
```

### Error 3: Invalid Period
```python
# Quarterly: 1-4 only
validator.validate_transaction_count(2024, 'quarterly', 5, count)  # ERROR

# Semi-annual: 1-2 only
validator.validate_transaction_count(2024, 'semi-annual', 3, count)  # ERROR
```

## Tips & Tricks

1. **Use context manager**: Always use `with QAValidator() as validator:` for automatic cleanup
2. **Check status**: Use `result.status.value` to get status string
3. **Custom tolerance**: Override per-validation for flexibility
4. **Batch validations**: Use `run_all_validations()` for efficiency
5. **Archive reports**: Save validation reports for audit trail

## Documentation

- **Complete Guide**: `docs/QA_VALIDATOR_USAGE.md`
- **Summary**: `docs/QA_VALIDATOR_SUMMARY.md`
- **Examples**: `examples/example_qa_validation.py`
- **Tests**: `tests/test_qa_validator.py`

## Support

```python
# Import the validator
from src.analytics import QAValidator, ValidationResult, ValidationStatus

# Create instance
validator = QAValidator(db_path=None, tolerance=0.01)

# Use context manager (recommended)
with QAValidator() as validator:
    pass
```

---

**Happy Validating!** 🎯
