# Analytics Module

The Analytics module provides quality assurance and validation tools for the Dubai Real Estate Intelligence Platform.

## Components

### QA Validator (`qa_validator.py`)

A comprehensive validation framework that ensures data quality and accuracy by validating calculated metrics against raw SQL queries from the DuckDB database.

**Key Features:**
- Transaction count validation
- Total volume validation
- Average price validation
- Percentage calculation validation
- Period boundary validation
- Date range filtering validation
- Customizable tolerance thresholds
- Comprehensive reporting

**Quick Start:**

```python
from src.analytics import QAValidator

# Validate a single metric
with QAValidator(tolerance=0.01) as validator:
    result = validator.validate_transaction_count(
        year=2024,
        period_type='quarterly',
        period_num=1,
        calculated_value=45000
    )
    print(result)
```

**Full Documentation:**
See [docs/QA_VALIDATOR_USAGE.md](../../docs/QA_VALIDATOR_USAGE.md) for complete usage guide and API reference.

## Installation

The analytics module is part of the main project. No additional installation required.

## Running Tests

Sample validation:
```bash
python src/analytics/qa_validator.py
```

Comprehensive test suite:
```bash
python test_qa_validator.py
```

## Use Cases

1. **Pre-deployment Validation**: Validate all metrics before deploying new data
2. **Continuous Monitoring**: Run scheduled validations to catch data quality issues
3. **API Quality Assurance**: Validate API responses before serving to clients
4. **Debugging**: Identify discrepancies in metric calculations
5. **Audit Trail**: Generate validation reports for compliance

## Architecture

```
src/analytics/
├── __init__.py          # Module exports
├── qa_validator.py      # Main QA validation framework
└── README.md           # This file

test_qa_validator.py     # Comprehensive test suite
docs/QA_VALIDATOR_USAGE.md  # Detailed usage guide
```

## Dependencies

- DuckDB (for database queries)
- Python 3.8+ (for type hints and dataclasses)
- Standard library only (no external dependencies)

## Contributing

When adding new validation methods:

1. Follow the existing naming convention: `validate_*`
2. Return a `ValidationResult` object
3. Append results to `self.results`
4. Include comprehensive docstrings
5. Add tests to `test_qa_validator.py`
6. Update documentation

## Support

For detailed documentation and examples, see:
- [QA Validator Usage Guide](../../docs/QA_VALIDATOR_USAGE.md)
- [Test Suite](../../test_qa_validator.py)
