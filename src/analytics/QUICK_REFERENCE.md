# ReportCalculator - Quick Reference

## Import

```python
from src.analytics.report_calculator import ReportCalculator, PeriodType
```

## Period Types

```python
PeriodType.MONTHLY      # 1-12
PeriodType.QUARTERLY    # 1-4
PeriodType.SEMI_ANNUAL  # 1-2
PeriodType.ANNUAL       # 1
```

## Basic Usage Pattern

```python
calculator = ReportCalculator()
try:
    # ... do calculations
finally:
    calculator.close()
```

## Common Operations

### 1. Monthly Metrics

```python
metrics = calculator.calculate_period_metrics(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11
)

# Access metrics
total_tx = metrics['transaction_metrics']['total_transactions']
avg_price = metrics['price_metrics']['avg_price']
luxury_pct = metrics['market_segments']['luxury']['percentage']
```

### 2. Year-over-Year Comparison

```python
yoy = calculator.get_comparison_metrics(
    current_year=2024,
    current_period_type=PeriodType.QUARTERLY,
    current_period_number=4,
    comparison_type="yoy"
)

# Access changes
tx_growth = yoy['changes']['transaction_changes']['total_transactions']['percentage']
price_change = yoy['changes']['price_changes']['avg_price']['percentage']
```

### 3. Month-over-Month Comparison

```python
mom = calculator.get_comparison_metrics(
    current_year=2024,
    current_period_type=PeriodType.MONTHLY,
    current_period_number=11,
    comparison_type="mom"
)
```

### 4. Top Performers

```python
# Top areas by transaction count
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
    limit=10
)

# Top areas by price growth
growth = calculator.get_top_performers(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11,
    metric="price_growth",
    category="areas",
    limit=10
)
```

### 5. Area-Specific Analysis

```python
area_summary = calculator.get_area_summary(
    area_name="Dubai Marina",
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11
)

# Includes all metrics + top_projects
```

### 6. Market Overview

```python
overview = calculator.get_market_overview(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11
)

# Includes:
# - All period metrics
# - top_areas
# - top_developers
# - top_projects
```

### 7. Time Series

```python
# Monthly trend for 2024
time_series = calculator.get_time_series(
    start_year=2024,
    start_period=1,
    end_year=2024,
    end_period=11,
    period_type=PeriodType.MONTHLY
)

# With area filter
dubai_marina_trend = calculator.get_time_series(
    start_year=2024,
    start_period=1,
    end_year=2024,
    end_period=11,
    period_type=PeriodType.MONTHLY,
    area_filter="Dubai Marina"
)
```

## Filtering

```python
# Filter by area
metrics = calculator.calculate_period_metrics(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11,
    area_filter="Business Bay"
)

# Filter by property type
metrics = calculator.calculate_period_metrics(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11,
    property_type_filter="Villa"
)

# Both filters
metrics = calculator.calculate_period_metrics(
    year=2024,
    period_type=PeriodType.MONTHLY,
    period_number=11,
    area_filter="Palm Jumeirah",
    property_type_filter="Villa"
)
```

## Metric Categories

### Transaction Metrics
- `total_transactions`: Count of transactions
- `unique_areas`: Number of unique areas
- `unique_projects`: Number of unique projects
- `unique_developers`: Number of unique developers
- `total_sales_volume`: Sum of all prices
- `avg_transaction_size`: Average transaction value

### Price Metrics
- `avg_price`: Average transaction price
- `median_price`: Median transaction price
- `min_price`: Minimum price
- `max_price`: Maximum price
- `price_stddev`: Standard deviation of prices
- `price_q1`: 25th percentile
- `price_q3`: 75th percentile
- `avg_size_sqm`: Average property size
- `avg_price_per_sqm`: Average price per square meter
- `median_price_per_sqm`: Median price per square meter

### Market Segments
Each segment (luxury, offplan, ready) includes:
- `count`: Number of transactions
- `volume`: Total sales value
- `percentage`: Percentage of total transactions
- `avg_price`: Average price in segment

### Statistical Metrics
- `price_range`: Max - Min
- `price_iqr`: Interquartile range (Q3 - Q1)
- `coefficient_of_variation`: (StdDev / Mean) * 100

## Comparison Metrics & Categories

### Metrics for get_top_performers()
- `"transaction_count"`: Rank by number of transactions
- `"sales_volume"`: Rank by total sales value
- `"avg_price"`: Rank by average price
- `"price_growth"`: Rank by price growth vs previous period

### Categories for get_top_performers()
- `"areas"`: Geographic areas
- `"developers"`: Developer companies
- `"projects"`: Individual projects
- `"property_types"`: Property types

### Comparison Types for get_comparison_metrics()
- `"mom"`: Month-over-month (or period-over-period)
- `"yoy"`: Year-over-year (same period, previous year)

## Example: Complete Monthly Report

```python
from src.analytics.report_calculator import ReportCalculator, PeriodType

calculator = ReportCalculator()
try:
    year, month = 2024, 11

    # 1. Current month metrics
    current = calculator.calculate_period_metrics(
        year=year,
        period_type=PeriodType.MONTHLY,
        period_number=month
    )

    # 2. Month-over-month comparison
    mom = calculator.get_comparison_metrics(
        current_year=year,
        current_period_type=PeriodType.MONTHLY,
        current_period_number=month,
        comparison_type="mom"
    )

    # 3. Year-over-year comparison
    yoy = calculator.get_comparison_metrics(
        current_year=year,
        current_period_type=PeriodType.MONTHLY,
        current_period_number=month,
        comparison_type="yoy"
    )

    # 4. Top performers
    top_areas = calculator.get_top_performers(
        year=year, period_type=PeriodType.MONTHLY, period_number=month,
        metric="transaction_count", category="areas", limit=10
    )

    top_developers = calculator.get_top_performers(
        year=year, period_type=PeriodType.MONTHLY, period_number=month,
        metric="transaction_count", category="developers", limit=10
    )

    # 5. Market overview
    overview = calculator.get_market_overview(
        year=year,
        period_type=PeriodType.MONTHLY,
        period_number=month
    )

    # 6. Time series (last 12 months)
    trend = calculator.get_time_series(
        start_year=2023, start_period=12,
        end_year=2024, end_period=11,
        period_type=PeriodType.MONTHLY
    )

    # Use data for report...

finally:
    calculator.close()
```

## Common Patterns

### Extract values for charts

```python
# Transaction trend
time_series = calculator.get_time_series(...)
months = [p['period_info']['period_number'] for p in time_series]
transactions = [p['transaction_metrics']['total_transactions'] for p in time_series]

# Price trend
avg_prices = [p['price_metrics']['avg_price'] for p in time_series]
median_prices = [p['price_metrics']['median_price'] for p in time_series]
```

### Format for display

```python
def format_currency(amount):
    return f"AED {amount:,.2f}"

def format_percentage(pct):
    return f"{pct:.1f}%"

def format_change(change_dict):
    abs_val = change_dict['absolute']
    pct_val = change_dict['percentage']
    return f"{abs_val:+,.0f} ({pct_val:+.1f}%)"

# Usage
price = format_currency(metrics['price_metrics']['avg_price'])
luxury_pct = format_percentage(metrics['market_segments']['luxury']['percentage'])
tx_change = format_change(yoy['changes']['transaction_changes']['total_transactions'])
```

### Check if data exists

```python
metrics = calculator.calculate_period_metrics(...)

if metrics['transaction_metrics']['total_transactions'] == 0:
    print("No data available for this period")
else:
    # Process metrics...
```

### Safe percentage calculation

```python
# All percentage calculations already handle division by zero
# They return 0 or None when appropriate

luxury_pct = metrics['market_segments']['luxury']['percentage']  # Safe
# Returns 0 if no transactions
```

## Tips

1. **Always close the connection**: Use try/finally pattern
2. **Check for zero transactions**: Before generating reports
3. **Use filters wisely**: Filtering can significantly speed up queries
4. **Time series can be memory intensive**: Limit to necessary periods
5. **Comparison requires previous data**: YoY needs last year's data, MoM needs last period

## Performance

- Single period metrics: < 100ms
- Comparison (2 periods): < 200ms
- Top performers: < 300ms
- Time series (12 months): < 2 seconds
- Market overview: < 1 second

## See Also

- Full documentation: `README.md`
- Test examples: `../../test_report_calculator.py`
- Source code: `report_calculator.py`
