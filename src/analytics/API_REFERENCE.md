# ReportCalculator API Reference

Complete API documentation for the `ReportCalculator` class.

## Module Import

```python
from src.analytics.report_calculator import ReportCalculator, PeriodType
```

## Enumerations

### PeriodType

```python
class PeriodType(Enum):
    MONTHLY = "monthly"        # period_number: 1-12
    QUARTERLY = "quarterly"    # period_number: 1-4
    SEMI_ANNUAL = "semi_annual"  # period_number: 1-2
    ANNUAL = "annual"          # period_number: 1
```

## Class: ReportCalculator

### Constructor

```python
ReportCalculator(db_path: Optional[str] = None, read_only: bool = True)
```

**Parameters:**
- `db_path` (str, optional): Path to DuckDB database. Defaults to `data/database/property_monitor.db`
- `read_only` (bool): Open database in read-only mode. Default: `True`

**Example:**
```python
# Default database, read-only
calculator = ReportCalculator()

# Custom database
calculator = ReportCalculator(db_path="/path/to/custom.db")

# Write mode
calculator = ReportCalculator(read_only=False)
```

### Methods

#### close()

```python
close() -> None
```

Close the database connection.

**Example:**
```python
calculator.close()
```

---

#### calculate_period_metrics()

```python
calculate_period_metrics(
    year: int,
    period_type: PeriodType,
    period_number: int,
    area_filter: Optional[str] = None,
    property_type_filter: Optional[str] = None
) -> Dict[str, Any]
```

Calculate comprehensive metrics for a specific period.

**Parameters:**
- `year`: Year for the period (e.g., 2024)
- `period_type`: Type of period (MONTHLY, QUARTERLY, SEMI_ANNUAL, ANNUAL)
- `period_number`: Period number within the year
- `area_filter`: Optional area name to filter
- `property_type_filter`: Optional property type to filter

**Returns:**
Dictionary with structure:
```python
{
    'period_info': {
        'year': int,
        'period_type': str,
        'period_number': int,
        'start_date': str,
        'end_date': str,
        'area_filter': Optional[str],
        'property_type_filter': Optional[str]
    },
    'transaction_metrics': {
        'total_transactions': int,
        'unique_areas': int,
        'unique_projects': int,
        'unique_developers': int,
        'total_sales_volume': float,
        'avg_transaction_size': float
    },
    'price_metrics': {
        'avg_price': float,
        'median_price': float,
        'min_price': float,
        'max_price': float,
        'price_stddev': float,
        'price_q1': float,
        'price_q3': float,
        'avg_size_sqm': float,
        'avg_price_per_sqm': float,
        'median_price_per_sqm': float
    },
    'market_segments': {
        'luxury': {
            'count': int,
            'volume': float,
            'percentage': float,
            'avg_price': float
        },
        'offplan': {
            'count': int,
            'volume': float,
            'percentage': float,
            'avg_price': float
        },
        'ready': {
            'count': int,
            'volume': float,
            'percentage': float,
            'avg_price': float
        }
    },
    'statistical_metrics': {
        'price_range': float,
        'price_iqr': float,
        'coefficient_of_variation': float
    },
    'property_type_distribution': [
        {
            'property_type': str,
            'count': int,
            'volume': float,
            'percentage': float,
            'avg_price': float,
            'avg_price_sqm': float
        }
    ]
}
```

**Raises:**
- `ValueError`: If period_number is invalid for period_type

**Example:**
```python
metrics = calculator.calculate_period_metrics(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11
)
print(f"Transactions: {metrics['transaction_metrics']['total_transactions']:,}")
```

---

#### get_comparison_metrics()

```python
get_comparison_metrics(
    current_year: int,
    current_period_type: PeriodType,
    current_period_number: int,
    comparison_type: str = "mom",
    area_filter: Optional[str] = None,
    property_type_filter: Optional[str] = None
) -> Dict[str, Any]
```

Compare current period with previous period (MoM) or year-over-year (YoY).

**Parameters:**
- `current_year`: Current period year
- `current_period_type`: Type of period
- `current_period_number`: Current period number
- `comparison_type`: "mom" for month-over-month or "yoy" for year-over-year
- `area_filter`: Optional area filter
- `property_type_filter`: Optional property type filter

**Returns:**
Dictionary with structure:
```python
{
    'comparison_type': str,  # "mom" or "yoy"
    'current_period': { ... },  # Full metrics dict
    'comparison_period': { ... },  # Full metrics dict
    'changes': {
        'transaction_changes': {
            'total_transactions': {
                'absolute': float,
                'percentage': float
            },
            'total_sales_volume': {
                'absolute': float,
                'percentage': float
            }
        },
        'price_changes': {
            'avg_price': {
                'absolute': float,
                'percentage': float
            },
            'median_price': {
                'absolute': float,
                'percentage': float
            },
            'avg_price_per_sqm': {
                'absolute': float,
                'percentage': float
            }
        },
        'segment_changes': {
            'luxury_count': {
                'absolute': float,
                'percentage': float
            },
            'offplan_count': {
                'absolute': float,
                'percentage': float
            },
            'offplan_percentage': {
                'absolute': float,
                'percentage': None
            }
        }
    }
}
```

**Raises:**
- `ValueError`: If comparison_type is not "mom" or "yoy"

**Example:**
```python
# Year-over-year
yoy = calculator.get_comparison_metrics(
    current_year=2024,
    current_period_type=PeriodType.QUARTERLY,
    current_period_number=4,
    comparison_type="yoy"
)

# Month-over-month
mom = calculator.get_comparison_metrics(
    current_year=2024,
    current_period_type=PeriodType.MONTHLY,
    current_period_number=11,
    comparison_type="mom"
)

print(f"YoY Growth: {yoy['changes']['transaction_changes']['total_transactions']['percentage']:.1f}%")
```

---

#### get_top_performers()

```python
get_top_performers(
    year: int,
    period_type: PeriodType,
    period_number: int,
    metric: str = "transaction_count",
    category: str = "areas",
    limit: int = 10
) -> List[Dict[str, Any]]
```

Get top performers for a specific metric and category.

**Parameters:**
- `year`: Period year
- `period_type`: Type of period
- `period_number`: Period number
- `metric`: Metric to rank by
  - `"transaction_count"`: Total transactions
  - `"sales_volume"`: Total sales value
  - `"avg_price"`: Average price
  - `"price_growth"`: Price growth vs previous period
- `category`: What to rank
  - `"areas"`: Geographic areas
  - `"developers"`: Developer companies
  - `"projects"`: Individual projects
  - `"property_types"`: Property types
- `limit`: Number of results to return (default: 10)

**Returns:**
List of dictionaries. For regular metrics:
```python
[
    {
        'rank': int,
        'name': str,
        'transaction_count': int,
        'sales_volume': float,
        'avg_price': float,
        'median_price': float,
        'avg_price_sqm': float,
        'luxury_count': int,
        'luxury_percentage': float,
        'offplan_count': int,
        'offplan_percentage': float,
        'unique_projects': Optional[int]  # Only for areas
    }
]
```

For `metric="price_growth"`:
```python
[
    {
        'rank': int,
        'name': str,
        'current_transaction_count': int,
        'current_avg_price': float,
        'previous_avg_price': float,
        'price_growth_percentage': float,
        'price_growth_absolute': float
    }
]
```

**Raises:**
- `ValueError`: If metric or category is invalid

**Example:**
```python
# Top areas by transactions
top_areas = calculator.get_top_performers(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11,
    metric="transaction_count",
    category="areas",
    limit=10
)

# Top developers by sales volume
top_devs = calculator.get_top_performers(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11,
    metric="sales_volume",
    category="developers",
    limit=5
)

# Areas with highest price growth
growth = calculator.get_top_performers(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11,
    metric="price_growth",
    category="areas",
    limit=10
)
```

---

#### get_area_summary()

```python
get_area_summary(
    area_name: str,
    year: int,
    period_type: PeriodType,
    period_number: int
) -> Dict[str, Any]
```

Get comprehensive summary for a specific area.

**Parameters:**
- `area_name`: Name of the area
- `year`: Period year
- `period_type`: Type of period
- `period_number`: Period number

**Returns:**
Dictionary with all period metrics plus additional field:
```python
{
    # ... all fields from calculate_period_metrics() ...
    'top_projects': [
        {
            'project_name': str,
            'developer': str,
            'transaction_count': int,
            'avg_price': float,
            'avg_price_sqm': float,
            'offplan_count': int,
            'offplan_percentage': float
        }
    ]
}
```

**Example:**
```python
area_summary = calculator.get_area_summary(
    area_name="Dubai Marina",
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11
)

for project in area_summary['top_projects']:
    print(f"{project['project_name']}: {project['transaction_count']} txs")
```

---

#### get_market_overview()

```python
get_market_overview(
    year: int,
    period_type: PeriodType,
    period_number: int
) -> Dict[str, Any]
```

Get overall market overview for a period.

**Parameters:**
- `year`: Period year
- `period_type`: Type of period
- `period_number`: Period number

**Returns:**
Dictionary with all period metrics plus additional fields:
```python
{
    # ... all fields from calculate_period_metrics() ...
    'top_areas': [ ... ],        # Top 10 areas by transaction count
    'top_developers': [ ... ],   # Top 10 developers by transaction count
    'top_projects': [ ... ]      # Top 10 projects by transaction count
}
```

**Example:**
```python
overview = calculator.get_market_overview(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11
)

print(f"Market Total: {overview['transaction_metrics']['total_transactions']:,}")
print("\nTop 5 Areas:")
for area in overview['top_areas'][:5]:
    print(f"  {area['rank']}. {area['name']}: {area['transaction_count']:,}")
```

---

#### get_time_series()

```python
get_time_series(
    start_year: int,
    start_period: int,
    end_year: int,
    end_period: int,
    period_type: PeriodType,
    area_filter: Optional[str] = None,
    property_type_filter: Optional[str] = None
) -> List[Dict[str, Any]]
```

Get time series data across multiple periods.

**Parameters:**
- `start_year`: Starting year
- `start_period`: Starting period number
- `end_year`: Ending year
- `end_period`: Ending period number
- `period_type`: Type of period
- `area_filter`: Optional area filter
- `property_type_filter`: Optional property type filter

**Returns:**
List of period metrics in chronological order:
```python
[
    { ... },  # Period 1 metrics
    { ... },  # Period 2 metrics
    # ... etc
]
```

**Example:**
```python
# Monthly trend for 2024
trend = calculator.get_time_series(
    start_year=2024,
    start_period=1,
    end_year=2024,
    end_period=11,
    period_type=PeriodType.MONTHLY
)

# Extract for charting
months = [p['period_info']['period_number'] for p in trend]
transactions = [p['transaction_metrics']['total_transactions'] for p in trend]
avg_prices = [p['price_metrics']['avg_price'] for p in trend]

# Area-specific trend
marina_trend = calculator.get_time_series(
    start_year=2023,
    start_period=1,
    end_year=2024,
    end_period=12,
    period_type=PeriodType.MONTHLY,
    area_filter="Dubai Marina"
)
```

---

## Usage Patterns

### Basic Pattern

```python
calculator = ReportCalculator()
try:
    # Perform calculations
    metrics = calculator.calculate_period_metrics(...)
    # Use metrics
finally:
    calculator.close()
```

### Context Manager Pattern (Recommended)

Note: The class doesn't implement context manager protocol yet, but you can use try/finally as above.

### Error Handling

```python
from src.analytics.report_calculator import ReportCalculator, PeriodType

calculator = ReportCalculator()
try:
    try:
        metrics = calculator.calculate_period_metrics(
            year=2024,
            period_type=PeriodType.MONTHLY,
            period_number=13  # Invalid!
        )
    except ValueError as e:
        print(f"Invalid period: {e}")

    # Check for empty results
    metrics = calculator.calculate_period_metrics(
        year=2020,
        period_type=PeriodType.MONTHLY,
        period_number=1
    )

    if metrics['transaction_metrics']['total_transactions'] == 0:
        print("No data available for this period")

finally:
    calculator.close()
```

## Data Types

### Period Numbers

| Period Type | Valid Numbers | Description |
|-------------|--------------|-------------|
| MONTHLY | 1-12 | January=1, February=2, etc. |
| QUARTERLY | 1-4 | Q1=1, Q2=2, Q3=3, Q4=4 |
| SEMI_ANNUAL | 1-2 | H1=1, H2=2 |
| ANNUAL | 1 | Full year |

### Metrics

All monetary values are in AED (UAE Dirhams).
All areas are in square meters (sqm).
All percentages are 0-100 (not 0-1).

### Null Handling

- Missing values return `None`
- Division by zero returns `0` or `None` as appropriate
- Empty result sets return metrics with all values set to 0

## Performance

Typical execution times on standard hardware:

| Operation | Time |
|-----------|------|
| calculate_period_metrics() | < 100ms |
| get_comparison_metrics() | < 200ms |
| get_top_performers() | < 300ms |
| get_area_summary() | < 500ms |
| get_market_overview() | < 1s |
| get_time_series() (12 periods) | < 2s |

## See Also

- [README.md](README.md) - Complete documentation
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Quick reference guide
- [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - Implementation details
