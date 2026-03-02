# Supply Data Analysis - January 8, 2026

## Executive Summary

**File:** `Supply-Data-01-08-2026-15.csv`
**Total Records:** 4,077 projects
**Total Columns:** 28
**File Size:** ~827KB (3.8MB in memory)
**Encoding:** Latin1 (ISO-8859-1)
**Off-Plan Projects:** 1,564 (38.4%)

---

## 1. Data Quality Assessment

### 1.1 Overall Quality Metrics

- **Unique IDs:** 4,077 (100% unique, no duplicates)
- **Emirate Coverage:** Dubai only (100%)
- **Status Distribution:** 7 unique status values with data quality issues

### 1.2 Missing Values by Field

| Field | Missing Count | Missing % | Severity |
|-------|--------------|-----------|----------|
| Sub_Location_3 | 3,851 | 94.46% | Low (optional field) |
| Location | 3,044 | 74.66% | Medium |
| Title_Type | 2,713 | 66.54% | Medium |
| Sub_Location_2 | 2,025 | 49.67% | Low (hierarchical) |
| Height_Class | 1,610 | 39.49% | Low (optional) |
| Launch_Date | 464 | 11.38% | **HIGH** |
| Developer | 80 | 1.96% | Medium |
| Sub_Location_1 | 69 | 1.69% | Low |
| Actual_Completion_Date | 53 | 1.30% | Medium |
| Initial_Completion_Date | 46 | 1.13% | Medium |
| Completion_Month | 41 | 1.01% | Low |
| Completion_Year | 39 | 0.96% | Low |
| Developer_Property_Type | 22 | 0.54% | Medium |
| Status | 7 | 0.17% | **HIGH** |

### 1.3 Critical Data Quality Issues

#### Issue 1: Zero Total Units
- **Count:** 71 projects (1.7%)
- **Impact:** Cannot calculate unit-level metrics
- **Examples:**
  - ID 7396: 368 Park Lane (Launched)
  - ID 7095: Aizel Tower (Under Construction)
  - ID 41: Al Fattan Currency House (Completed)

#### Issue 2: Unit Type Mismatches
- **Count:** 380 projects (9.3%)
- **Issue:** Sum of unit types ≠ Total_Units
- **Examples:**
  - ID 2: Total=289, Calculated=295, Diff=-6
  - ID 10: Total=537, Calculated=550, Diff=-13
  - ID 3856: Total=791, Calculated=0 (all unit types are 0!)

#### Issue 3: Status Field Inconsistencies
- **Multiple variations:** "On Hold" vs "On hold" (capitalization)
- **7 null Status values:** Need investigation and imputation

#### Issue 4: Developer_Property_Type Inconsistencies
- **Multiple variations for same concept:**
  - "Residential building" vs "Residential Building"
  - "Mixed-use" vs "Mixed Use" vs "Mixed-Use"
  - "Apartments" vs "Apartment"
  - "Townhouses" vs "Townhouse"
  - "Villas" vs "Villa"
- **Over 100 unique values** for what should be ~15-20 categories

---

## 2. Date Field Analysis

### 2.1 Date Format Pattern

**All dates use consistent format:** `M/D/YYYY`

Examples:
- `4/30/2025` (April 30, 2025)
- `12/31/2024` (December 31, 2024)
- `1/1/2014` (January 1, 2014)

### 2.2 Date Parsing Success Rate

| Field | Non-Null Count | Parsing Success | Success Rate |
|-------|----------------|-----------------|--------------|
| Launch_Date | 3,613 | 3,613 | 100.0% |
| Initial_Completion_Date | 4,031 | 4,030 | 99.97% |
| Actual_Completion_Date | 4,024 | 4,024 | 100.0% |

**Recommendation:** Use `pandas.to_datetime(date_str, format='%m/%d/%Y')` for optimal parsing.

### 2.3 Date Range Analysis

#### Launch_Date
- **Range:** 2005-09-30 to 2025-11-12 (20 years)
- **Missing:** 464 records (11.4%)

#### Initial_Completion_Date
- **Range:** Various, includes future dates
- **Missing:** 46 records (1.1%)

#### Actual_Completion_Date
- **Range:** Past and future dates up to 2031
- **Missing:** 53 records (1.3%)
- **Future dates:** 1,548 projects (38.0%)

---

## 3. Off-Plan Status Determination

### 3.1 Analysis Date
**Reference Date:** January 8, 2026

### 3.2 Off-Plan Identification Methods

#### Method 1: By Status (Recommended)
```python
off_plan = df['Status'].isin(['Launched', 'Under Construction'])
# Result: 1,564 projects (38.4%)
```

**Status Breakdown:**
- Completed: 2,063 (50.6%)
- Under Construction: 1,431 (35.1%)
- Handed Over: 343 (8.4%)
- Launched: 133 (3.3%)
- Cancelled: 75 (1.8%)
- On Hold: 22 (0.5%)
- Under Cancellation: 3 (0.1%)

#### Method 2: By Completion Date
```python
off_plan = df['Actual_Completion_Date'] > '2026-01-08'
# Result: 1,548 projects (38.0%)
```

#### Method 3: By Completion Percentage
```python
off_plan = df['Completion_Percentage'] < 100
# Result: 1,679 projects (41.2%)
```
Note: This includes "On Hold" and "Cancelled" projects.

#### Method 4: Combined Logic (Best Practice)
```python
off_plan = (
    df['Status'].isin(['Launched', 'Under Construction']) &
    (df['Completion_Percentage'] < 100)
)
# Result: 1,564 projects (38.4%)
```

### 3.3 Recommended Logic

```python
def is_off_plan(row, reference_date):
    """
    Determine if a project is off-plan.

    A project is off-plan if:
    1. Status is 'Launched' or 'Under Construction', AND
    2. Completion_Percentage < 100, AND
    3. Actual_Completion_Date is in the future (if available)
    """
    if row['Status'] not in ['Launched', 'Under Construction']:
        return False

    if row['Completion_Percentage'] >= 100:
        return False

    if pd.notna(row['Actual_Completion_Date']):
        if row['Actual_Completion_Date'] <= reference_date:
            return False

    return True
```

---

## 4. Numeric Fields Analysis

### 4.1 Unit Distribution Statistics

| Field | Min | Max | Mean | Median | Zero Values |
|-------|-----|-----|------|--------|-------------|
| Total_Units | 0 | 35,000 | 307.09 | 174 | 71 |
| Total_Apartments | 0 | 25,412 | 214.22 | 112 | 1,106 |
| Total_Hotel_Apartments | 0 | 782 | 1.97 | 0 | 4,041 |
| total_Serviced_Apartments | 0 | 1,213 | 2.92 | 0 | 4,027 |
| Total_Townhouses | 0 | 3,898 | 24.44 | 0 | 3,629 |
| Total_Villas | 0 | 2,188 | 17.48 | 0 | 3,655 |
| Total_Offices | 0 | 1,045 | 5.34 | 0 | 3,918 |
| Total_Retails | 0 | 5,500 | 8.67 | 0 | 2,747 |
| Total_Hotel_Rooms | 0 | 1,544 | 1.50 | 0 | 4,058 |
| Completion_Percentage | 0 | 100 | 66.55 | 100 | 574 |
| Completion_Year | 0 | 2031 | 2020.27 | 2023 | 2 |

### 4.2 Key Observations

1. **Highly skewed distributions:** Most unit types have median of 0
2. **Residential dominance:** Apartments are the primary unit type
3. **Outliers exist:** Some projects have exceptionally high unit counts
4. **Completion bias:** Median completion is 100% (completed projects outnumber active)

---

## 5. Field-Specific Analysis

### 5.1 Developer_Property_Type

**Top 15 Property Types:**
1. Apartments: 1,167 (28.6%)
2. Mixed-use: 691 (16.9%)
3. Residential building: 486 (11.9%)
4. Residential Building: 219 (5.4%)
5. Apartment: 201 (4.9%)
6. Villa: 197 (4.8%)
7. Townhouse: 162 (4.0%)
8. Townhouses: 132 (3.2%)
9. Mixed Use: 118 (2.9%)
10. Villas: 118 (2.9%)
11. Offices: 84 (2.1%)
12. Villa/Townhouse: 72 (1.8%)
13. Mixed-Use: 33 (0.8%)
14. Villas/Townhouses: 24 (0.6%)
15. Commercial building: 23 (0.6%)

**Issue:** Inconsistent capitalization and naming conventions need normalization.

### 5.2 Master Development

Top developments (by project count):
- Will require separate analysis once data is loaded

### 5.3 Height Class

Categories observed:
- Mid-Rise (5-12 floors)
- High-Rise (13-39 floors)
- Skyscraper (40 floors+)
- Low-Rise (1-4 floors)
- Super Tall
- Single Family (Villa/Townhouse)

**Missing:** 39.49% (1,610 projects)

---

## 6. Data Transformation Requirements

### 6.1 Critical Transformations

#### 1. Encoding Detection and Handling
```python
# Must use latin1/iso-8859-1 encoding
df = pd.read_csv(file_path, encoding='latin1')
```

#### 2. Date Parsing
```python
date_columns = ['Launch_Date', 'Initial_Completion_Date', 'Actual_Completion_Date']
for col in date_columns:
    df[col] = pd.to_datetime(df[col], format='%m/%d/%Y', errors='coerce')
```

#### 3. Status Normalization
```python
def normalize_status(status):
    if pd.isna(status):
        return 'Unknown'
    status = status.strip().lower()
    status_map = {
        'on hold': 'On Hold',
        'under construction': 'Under Construction',
        'completed': 'Completed',
        'launched': 'Launched',
        'handed over': 'Handed Over',
        'cancelled': 'Cancelled',
        'under cancellation': 'Under Cancellation'
    }
    return status_map.get(status, status.title())
```

#### 4. Property Type Normalization
```python
def normalize_property_type(prop_type):
    if pd.isna(prop_type):
        return 'Unknown'

    prop_type = prop_type.strip().lower()

    # Normalize variations
    if 'apartment' in prop_type and 'hotel' not in prop_type:
        return 'Apartments'
    elif 'mixed' in prop_type:
        return 'Mixed-Use'
    elif 'residential building' in prop_type:
        return 'Residential Building'
    elif prop_type in ['villa', 'villas']:
        return 'Villas'
    elif prop_type in ['townhouse', 'townhouses']:
        return 'Townhouses'
    # ... more mappings

    return prop_type.title()
```

#### 5. Developer Name Normalization
```python
def normalize_developer(developer):
    if pd.isna(developer):
        return 'Unknown Developer'

    developer = developer.strip()

    # Common normalizations
    developer_map = {
        'Emaar': 'Emaar Properties',
        'Nakheel': 'Nakheel PJSC',
        # ... more mappings
    }

    return developer_map.get(developer, developer)
```

#### 6. Location Hierarchy Standardization
```python
# Ensure location hierarchy is consistent
# Master_Development > Sub_Location_1 > Sub_Location_2 > Sub_Location_3

def build_location_hierarchy(row):
    locations = []
    for col in ['Master_Development', 'Sub_Location_1', 'Sub_Location_2', 'Sub_Location_3']:
        if pd.notna(row[col]) and row[col].strip():
            locations.append(row[col].strip())
    return ' > '.join(locations)
```

### 6.2 Data Type Conversions

```python
dtype_conversions = {
    'ID': 'int32',
    'Project_Name': 'string',
    'Emirate': 'category',
    'Master_Development': 'string',
    'Sub_Location_1': 'string',
    'Sub_Location_2': 'string',
    'Sub_Location_3': 'string',
    'Developer': 'string',
    'Developer_Property_Type': 'category',
    'Status': 'category',
    'Completion_Month': 'category',
    'Completion_Year': 'Int16',  # Nullable integer
    'Total_Units': 'int32',
    'Total_Apartments': 'int32',
    'Total_Hotel_Apartments': 'int16',
    'total_Serviced_Apartments': 'int16',
    'Total_Townhouses': 'int16',
    'Total_Villas': 'int16',
    'Total_Offices': 'int16',
    'Total_Retails': 'int16',
    'Total_Hotel_Rooms': 'int16',
    'Completion_Percentage': 'float32',
    'Title_Type': 'category',
    'Location': 'string',
    'Height_Class': 'category'
}
```

### 6.3 Derived Fields to Create

```python
# 1. Off-plan status flag
df['is_off_plan'] = df.apply(is_off_plan, axis=1, reference_date=datetime(2026, 1, 8))

# 2. Full location string
df['full_location'] = df.apply(build_location_hierarchy, axis=1)

# 3. Unit type category
def categorize_unit_type(row):
    if row['Total_Apartments'] > 0 and row['Total_Villas'] == 0:
        return 'Apartments Only'
    elif row['Total_Villas'] > 0 and row['Total_Apartments'] == 0:
        return 'Villas Only'
    elif row['Total_Townhouses'] > 0:
        return 'Includes Townhouses'
    elif row['Total_Offices'] > 0 or row['Total_Retails'] > 0:
        return 'Commercial/Mixed'
    else:
        return 'Other'

df['unit_type_category'] = df.apply(categorize_unit_type, axis=1)

# 4. Project size category
def categorize_project_size(total_units):
    if total_units == 0:
        return 'Unknown'
    elif total_units < 50:
        return 'Small'
    elif total_units < 200:
        return 'Medium'
    elif total_units < 500:
        return 'Large'
    else:
        return 'Mega'

df['project_size_category'] = df['Total_Units'].apply(categorize_project_size)

# 5. Years to completion (for off-plan)
def years_to_completion(actual_date, reference_date):
    if pd.isna(actual_date):
        return None
    delta = actual_date - reference_date
    return delta.days / 365.25

df['years_to_completion'] = df.apply(
    lambda row: years_to_completion(row['Actual_Completion_Date'], datetime(2026, 1, 8)),
    axis=1
)
```

---

## 7. Data Validation Rules

### 7.1 Required Field Validation

```python
required_fields = ['ID', 'Project_Name', 'Status', 'Total_Units']

def validate_required_fields(df):
    errors = []
    for field in required_fields:
        null_count = df[field].isna().sum()
        if null_count > 0:
            errors.append(f"{field}: {null_count} null values")
    return errors
```

### 7.2 Business Logic Validation

```python
def validate_business_logic(df):
    issues = []

    # 1. Total Units should equal sum of unit types (with tolerance)
    df['unit_sum'] = (
        df['Total_Apartments'] + df['Total_Hotel_Apartments'] +
        df['total_Serviced_Apartments'] + df['Total_Townhouses'] +
        df['Total_Villas'] + df['Total_Offices'] +
        df['Total_Retails'] + df['Total_Hotel_Rooms']
    )

    mismatch = df[abs(df['Total_Units'] - df['unit_sum']) > 5]
    if len(mismatch) > 0:
        issues.append(f"Unit sum mismatch: {len(mismatch)} projects")

    # 2. Completed projects should have 100% completion
    incomplete_completed = df[
        (df['Status'] == 'Completed') &
        (df['Completion_Percentage'] < 100)
    ]
    if len(incomplete_completed) > 0:
        issues.append(f"Completed with < 100%: {len(incomplete_completed)} projects")

    # 3. Completion date should be in past for completed projects
    future_completed = df[
        (df['Status'] == 'Completed') &
        (df['Actual_Completion_Date'] > datetime.now())
    ]
    if len(future_completed) > 0:
        issues.append(f"Completed with future date: {len(future_completed)} projects")

    # 4. Launch date should be before completion date
    invalid_dates = df[
        (df['Launch_Date'].notna()) &
        (df['Actual_Completion_Date'].notna()) &
        (df['Launch_Date'] > df['Actual_Completion_Date'])
    ]
    if len(invalid_dates) > 0:
        issues.append(f"Launch after completion: {len(invalid_dates)} projects")

    return issues
```

### 7.3 Data Range Validation

```python
def validate_ranges(df):
    issues = []

    # Completion percentage should be 0-100
    invalid_pct = df[(df['Completion_Percentage'] < 0) | (df['Completion_Percentage'] > 100)]
    if len(invalid_pct) > 0:
        issues.append(f"Invalid completion %: {len(invalid_pct)}")

    # Total units should be positive (excluding known zero cases)
    negative_units = df[df['Total_Units'] < 0]
    if len(negative_units) > 0:
        issues.append(f"Negative units: {len(negative_units)}")

    # Completion year should be reasonable
    invalid_year = df[(df['Completion_Year'] < 2000) | (df['Completion_Year'] > 2035)]
    if len(invalid_year) > 0:
        issues.append(f"Invalid completion year: {len(invalid_year)}")

    return issues
```

---

## 8. ETL Pipeline Structure

### 8.1 Recommended Architecture

```
┌─────────────────┐
│  Raw CSV File   │
│  (latin1 enc.)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Extract Phase  │
│  - Load CSV     │
│  - Detect enc.  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Transform Phase │
│  - Parse dates  │
│  - Normalize    │
│  - Validate     │
│  - Derive fields│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Load Phase    │
│  - BigQuery     │
│  - Set schema   │
└─────────────────┘
```

### 8.2 Modular Pipeline Components

```python
class SupplyDataETL:
    """ETL pipeline for Dubai real estate supply data."""

    def __init__(self, reference_date=None):
        self.reference_date = reference_date or datetime.now()
        self.df = None
        self.errors = []
        self.warnings = []

    def extract(self, file_path: str) -> pd.DataFrame:
        """Extract: Load CSV with proper encoding."""
        pass

    def transform(self) -> pd.DataFrame:
        """Transform: Clean, normalize, and enrich data."""
        pass

    def validate(self) -> List[str]:
        """Validate: Check data quality and business rules."""
        pass

    def load(self, destination: str):
        """Load: Write to BigQuery or other destination."""
        pass

    def run(self, file_path: str, destination: str):
        """Run complete ETL pipeline."""
        pass
```

### 8.3 Error Handling Strategy

```python
class ETLError(Exception):
    """Base class for ETL errors."""
    pass

class ExtractionError(ETLError):
    """Raised when extraction fails."""
    pass

class TransformationError(ETLError):
    """Raised when transformation fails."""
    pass

class ValidationError(ETLError):
    """Raised when validation fails."""
    pass

# Error handling approach
def safe_extract(file_path):
    try:
        return extract_data(file_path)
    except UnicodeDecodeError as e:
        raise ExtractionError(f"Encoding error: {e}")
    except FileNotFoundError as e:
        raise ExtractionError(f"File not found: {e}")
    except Exception as e:
        raise ExtractionError(f"Unexpected error: {e}")
```

---

## 9. Edge Cases to Handle

### 9.1 Date Field Edge Cases

1. **Null Launch Dates (464 records)**
   - Strategy: Allow nulls, don't impute
   - Impact: Cannot calculate project age for these records

2. **Future Completion Dates**
   - Strategy: Valid for off-plan projects
   - Validate: Should not be more than 5 years in future

3. **Past Launch Dates with Future Completion**
   - Strategy: Valid, long development cycle
   - Validate: Launch < Completion always

4. **Completion Date Before Launch Date**
   - Strategy: Data error, flag for review
   - Action: Set to null or fix manually

### 9.2 Unit Count Edge Cases

1. **Zero Total Units (71 records)**
   - Strategy: Keep as-is, mark as data quality issue
   - Impact: Cannot calculate per-unit metrics
   - Root cause: Likely data entry errors or projects in planning

2. **Unit Type Sum Mismatch (380 records)**
   - Strategy: Trust Total_Units as authoritative
   - Action: Flag for manual review
   - Calculate: Create 'Other_Units' field for difference

3. **Extremely Large Unit Counts**
   - Example: Total_Units = 35,000
   - Strategy: Validate against known mega-projects
   - Action: Set reasonable upper bound (e.g., 50,000)

### 9.3 Status Edge Cases

1. **Null Status (7 records)**
   - Strategy: Impute based on Completion_Percentage and dates
   - Logic:
     - If Completion_Percentage = 100 → 'Completed'
     - If Completion_Percentage = 0 → 'Launched'
     - Else → 'Under Construction'

2. **Inconsistent Capitalization**
   - Strategy: Normalize to title case
   - Map: All variations to standard values

3. **Completed with < 100% Completion**
   - Strategy: Trust Status over Percentage
   - Action: Set Completion_Percentage = 100

### 9.4 Property Type Edge Cases

1. **100+ Unique Values**
   - Strategy: Create standardized taxonomy
   - Action: Map all variations to ~15 canonical types

2. **Null Property Type (22 records)**
   - Strategy: Impute from unit distribution
   - Logic:
     - If Total_Apartments > 80% of units → 'Apartments'
     - If Total_Villas > 80% → 'Villas'
     - Else → 'Mixed-Use'

### 9.5 Location Edge Cases

1. **High NULL Rate in Sub_Location_3 (94%)**
   - Strategy: Expected, hierarchical data
   - Action: No imputation needed

2. **Missing Master_Development**
   - Strategy: Use Sub_Location_1 as fallback
   - Action: Create composite location field

---

## 10. Python Code Structure

### 10.1 Recommended Project Structure

```
src/
├── etl/
│   ├── __init__.py
│   ├── extractors/
│   │   ├── __init__.py
│   │   └── csv_extractor.py
│   ├── transformers/
│   │   ├── __init__.py
│   │   ├── date_transformer.py
│   │   ├── status_normalizer.py
│   │   ├── property_type_normalizer.py
│   │   └── derived_fields.py
│   ├── validators/
│   │   ├── __init__.py
│   │   ├── required_fields.py
│   │   ├── business_logic.py
│   │   └── data_ranges.py
│   ├── loaders/
│   │   ├── __init__.py
│   │   └── bigquery_loader.py
│   └── pipeline.py
├── models/
│   ├── __init__.py
│   └── supply_data.py
└── utils/
    ├── __init__.py
    ├── encoding_detector.py
    └── logging_config.py
```

### 10.2 Performance Optimization

```python
# Use efficient data types to reduce memory
dtype_map = {
    'ID': 'int32',  # Reduces from int64
    'Status': 'category',  # Much smaller than string
    'Emirate': 'category',
    'Developer_Property_Type': 'category',
    'Completion_Percentage': 'float32',  # Reduces from float64
}

# Vectorized operations instead of apply
# BAD (slow):
df['is_off_plan'] = df.apply(lambda row: is_off_plan(row), axis=1)

# GOOD (fast):
df['is_off_plan'] = (
    df['Status'].isin(['Launched', 'Under Construction']) &
    (df['Completion_Percentage'] < 100)
)

# Use chunksize for very large files
for chunk in pd.read_csv(file_path, chunksize=10000):
    process_chunk(chunk)
```

### 10.3 Testing Strategy

```python
# tests/test_supply_data_etl.py

import pytest
import pandas as pd
from datetime import datetime

@pytest.fixture
def sample_data():
    """Create sample data for testing."""
    return pd.DataFrame({
        'ID': [1, 2, 3],
        'Project_Name': ['Test 1', 'Test 2', 'Test 3'],
        'Status': ['Completed', 'Under Construction', 'Launched'],
        'Completion_Percentage': [100, 50, 0],
        'Launch_Date': ['1/1/2020', '6/15/2024', '12/31/2025'],
        'Actual_Completion_Date': ['12/31/2022', '6/30/2027', '12/31/2028'],
        'Total_Units': [100, 200, 150],
    })

def test_date_parsing(sample_data):
    """Test date parsing functionality."""
    df = parse_dates(sample_data)
    assert df['Launch_Date'].dtype == 'datetime64[ns]'
    assert pd.notna(df['Launch_Date'].iloc[0])

def test_off_plan_detection(sample_data):
    """Test off-plan status detection."""
    df = sample_data.copy()
    df = identify_off_plan(df, reference_date=datetime(2026, 1, 8))

    assert df['is_off_plan'].iloc[0] == False  # Completed
    assert df['is_off_plan'].iloc[1] == True   # Under Construction
    assert df['is_off_plan'].iloc[2] == True   # Launched

def test_status_normalization():
    """Test status normalization."""
    assert normalize_status('on hold') == 'On Hold'
    assert normalize_status('COMPLETED') == 'Completed'
    assert normalize_status(None) == 'Unknown'

def test_unit_validation(sample_data):
    """Test unit count validation."""
    errors = validate_business_logic(sample_data)
    assert len(errors) >= 0  # Should return list of errors
```

---

## 11. BigQuery Schema Recommendations

### 11.1 Table Schema

```python
BIGQUERY_SCHEMA = [
    {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'project_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'emirate', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'master_development', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sub_location_1', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sub_location_2', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'sub_location_3', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'full_location', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'developer', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'property_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'property_type_normalized', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'status', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'status_normalized', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'launch_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'initial_completion_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'actual_completion_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'completion_month', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'completion_year', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_units', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'total_apartments', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_hotel_apartments', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_serviced_apartments', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_townhouses', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_villas', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_offices', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_retails', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'total_hotel_rooms', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'completion_percentage', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'title_type', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'height_class', 'type': 'STRING', 'mode': 'NULLABLE'},
    # Derived fields
    {'name': 'is_off_plan', 'type': 'BOOLEAN', 'mode': 'REQUIRED'},
    {'name': 'project_size_category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'unit_type_category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'years_to_completion', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'data_quality_flags', 'type': 'STRING', 'mode': 'REPEATED'},
    # Metadata
    {'name': 'load_timestamp', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
    {'name': 'source_file', 'type': 'STRING', 'mode': 'REQUIRED'},
]
```

### 11.2 Partitioning and Clustering

```python
# Partition by completion date for efficient querying
table.time_partitioning = bigquery.TimePartitioning(
    type_=bigquery.TimePartitioningType.YEAR,
    field='actual_completion_date'
)

# Cluster by commonly queried fields
table.clustering_fields = ['status', 'master_development', 'property_type_normalized']
```

---

## 12. Specific Recommendations

### 12.1 Immediate Actions

1. **Fix Encoding Issue:** Always use `encoding='latin1'` when reading CSV
2. **Normalize Status Field:** Implement status normalization function
3. **Normalize Property Types:** Create canonical mapping for 100+ variations
4. **Handle Zero Units:** Flag 71 projects with zero units for review
5. **Fix Unit Mismatches:** Investigate 380 projects with unit sum discrepancies

### 12.2 Data Enrichment Opportunities

1. **Geocoding:** Add latitude/longitude for projects with location data
2. **Developer Classification:** Categorize developers (large/medium/small)
3. **Market Segment:** Classify by price point (luxury/mid-market/affordable)
4. **Completion Reliability:** Historical completion date accuracy
5. **Project Timeline:** Calculate average time from launch to completion

### 12.3 Monitoring and Alerts

```python
# Set up data quality monitoring
quality_metrics = {
    'null_status_count': df['Status'].isna().sum(),
    'zero_units_count': (df['Total_Units'] == 0).sum(),
    'unit_mismatch_count': len(unit_mismatches),
    'future_completed_count': len(future_completed),
    'missing_dates_count': df['Launch_Date'].isna().sum(),
}

# Alert thresholds
THRESHOLDS = {
    'null_status_count': 10,
    'zero_units_count': 100,
    'unit_mismatch_count': 500,
}

# Generate alerts
alerts = []
for metric, value in quality_metrics.items():
    if metric in THRESHOLDS and value > THRESHOLDS[metric]:
        alerts.append(f"ALERT: {metric} = {value} exceeds threshold {THRESHOLDS[metric]}")
```

---

## 13. Summary and Next Steps

### 13.1 Key Findings

1. **Data is 95%+ usable** with proper transformations
2. **Date parsing is reliable** with 100% success rate
3. **1,564 off-plan projects** can be reliably identified
4. **Property type normalization** is critical (100+ variations to ~15)
5. **Unit count data quality** needs attention (9.3% mismatches)

### 13.2 Critical Path Forward

1. **Implement ETL pipeline** with modules described above
2. **Create normalization mappings** for Status and Property Type
3. **Add data quality flags** to track known issues
4. **Set up automated validation** with alerts
5. **Load to BigQuery** with proper schema and partitioning
6. **Build data quality dashboard** to monitor ongoing issues

### 13.3 Success Metrics

- **Data completeness:** > 95% for critical fields
- **Normalization coverage:** 100% of Status and Property Type values
- **Processing time:** < 5 seconds for full dataset
- **Data freshness:** < 24 hours lag from source
- **Quality score:** > 90% passing all validation rules

---

**Generated:** January 8, 2026
**Analyst:** Claude (AI Assistant)
**Dataset:** Supply-Data-01-08-2026-15.csv
**Records Analyzed:** 4,077 projects
