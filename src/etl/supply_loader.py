# src/etl/supply_loader.py

"""
Dubai Projects Supply Data Loader
Loads project supply data from CSV into DuckDB (property_monitor.db)

Follows the same patterns as BigQueryLoader:
- Versioning with data_versions table
- Quality scoring and validation
- Data normalization and transformation
"""

import duckdb
import pandas as pd
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.bigquery_settings import bq_settings


class SupplyLoader:
    """Load Dubai projects supply data from CSV into local DuckDB"""

    # Property type normalization map
    PROPERTY_TYPE_NORMALIZATION = {
        # Residential
        'Apartment': 'Apartment',
        'Apartments': 'Apartment',
        'Residential building': 'Apartment',
        'Residential Building': 'Apartment',
        'Residential': 'Apartment',
        'Residential Tower': 'Apartment',

        # Villas/Townhouses
        'Villa': 'Villa',
        'Villas': 'Villa',
        'Townhouse': 'Townhouse',
        'Townhouses': 'Townhouse',

        # Mixed Use
        'Mixed-use': 'Mixed Use',
        'Mixed Use': 'Mixed Use',
        'Mixed-Use': 'Mixed Use',
        'Mixed use': 'Mixed Use',

        # Hotel/Hospitality
        'Hotel': 'Hotel',
        'Hotel Apartment': 'Hotel Apartment',
        'Hotel Apartments': 'Hotel Apartment',
        'Serviced Apartments': 'Serviced Apartment',
        'Serviced Apartment': 'Serviced Apartment',

        # Commercial
        'Office': 'Office',
        'Offices': 'Office',
        'Retail': 'Retail',
        'Shopping Mall': 'Retail',
        'Commercial': 'Commercial',

        # Land/Development
        'Land': 'Land',
        'Plot': 'Land',
        'Community': 'Community',
        'Master Community': 'Community',

        # Industrial/Warehouse
        'Warehouse': 'Commercial',
        'Industrial': 'Commercial',
    }

    # Status normalization map
    STATUS_NORMALIZATION = {
        'Completed': 'Completed',
        'completed': 'Completed',
        'COMPLETED': 'Completed',

        'Handed Over': 'Completed',
        'handed over': 'Completed',
        'HANDED OVER': 'Completed',

        'Under Construction': 'Under Construction',
        'under construction': 'Under Construction',
        'UNDER CONSTRUCTION': 'Under Construction',

        'Launched': 'Launched',
        'launched': 'Launched',
        'LAUNCHED': 'Launched',

        'On Hold': 'On Hold',
        'on hold': 'On Hold',
        'On hold': 'On Hold',
        'ON HOLD': 'On Hold',

        'Cancelled': 'Cancelled',
        'cancelled': 'Cancelled',
        'CANCELLED': 'Cancelled',

        'Under Cancellation': 'Cancelled',
        'under cancellation': 'Cancelled',
        'UNDER CANCELLATION': 'Cancelled',
    }

    # Developer name normalization (canonical forms for known duplicates)
    DEVELOPER_NORMALIZATION = {
        'Damac Properties': 'DAMAC Properties',
        'Union properties': 'Union Properties',
        'Five Holdings': 'FIVE Holdings',
        'Al Manal Development Fzco': 'Al Manal Development FZCO',
        'Abdulrazzq Ali Almadani': 'ABDULRAZZQ ALI ALMADANI',
    }

    # Height class normalization
    HEIGHT_CLASS_NORMALIZATION = {
        'Mid-Rise (5-12 floors)': 'Mid-Rise',
        'Mid-rise': 'Mid-Rise',
        'High-rise': 'High-Rise',
        'High-Rise (13-39 floors)': 'High-Rise',
        'Low-rise': 'Low-Rise',
        'Low-Rise (1-4 floors)': 'Low-Rise',
        'Single Famliy (Villa/Townhouse)': 'Villa/Townhouse',
        'Single Family (Villa/Townhouse)': 'Villa/Townhouse',
        'Skyscraper (40 floors+)': 'Skyscraper',
        'Super Tall': 'Supertall',
        'MI': None,
    }

    def __init__(self, db_path=None):
        """Initialize loader with database connection"""
        self.db_path = db_path or str(bq_settings.PM_DB_PATH)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        print(f"Connected to database: {self.db_path}")

    def load_csv(self, csv_path: str, version_name: Optional[str] = None) -> int:
        """
        Load supply data from CSV file into database

        Args:
            csv_path: Path to CSV file
            version_name: Optional version name (defaults to filename + timestamp)

        Returns:
            version_id of the loaded data
        """
        csv_path = Path(csv_path)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        print(f"\n{'='*60}")
        print(f"Loading Supply Data: {csv_path.name}")
        print(f"{'='*60}\n")

        # Generate version name if not provided
        if version_name is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            version_name = f"Supply:{csv_path.name}:{timestamp}"

        # Load and transform CSV data
        print("Step 1: Reading CSV file...")
        df = self._read_csv(csv_path)
        print(f"  [OK] Loaded {len(df):,} projects from CSV")

        # Transform data
        print("\nStep 2: Transforming and normalizing data...")
        df = self._transform_data(df)
        print(f"  [OK] Transformation complete")

        # Validate and score quality
        print("\nStep 3: Validating data and calculating quality scores...")
        df = self._validate_and_score(df)
        valid_count = int(df['is_valid'].sum())  # Convert numpy.int64 to Python int
        print(f"  [OK] Valid projects: {valid_count:,} / {len(df):,} ({valid_count/len(df)*100:.1f}%)")

        # Create version entry
        print("\nStep 4: Creating version entry...")
        version_id = self._create_version(version_name, str(csv_path), len(df), valid_count)
        df['version_id'] = version_id
        df['imported_at'] = datetime.now()
        print(f"  [OK] Version ID: {version_id}")

        # Insert into database
        print("\nStep 5: Inserting data into database...")
        self._insert_data(df)
        print(f"  [OK] Inserted {len(df):,} records into supply_projects_all")

        # Build indexes
        print("\nStep 6: Building indexes...")
        self._build_indexes()
        print(f"  [OK] Indexes built successfully")

        # Print summary
        self._print_summary(df, version_id)

        print(f"\n{'='*60}")
        print(f"[SUCCESS] Supply data loaded successfully!")
        print(f"  Version ID: {version_id}")
        print(f"  Total Projects: {len(df):,}")
        print(f"  Valid Projects: {valid_count:,}")
        print(f"{'='*60}\n")

        return version_id

    def _read_csv(self, csv_path: Path) -> pd.DataFrame:
        """Read CSV file with proper encoding"""
        # CSV uses Latin1 encoding and M/D/YYYY date format
        df = pd.read_csv(
            csv_path,
            encoding='latin1',
            low_memory=False
        )
        return df

    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and normalize CSV data"""

        # Rename columns to standardized names
        column_mapping = {
            'ID': 'project_id',
            'Project_Name': 'project_name',
            'Emirate': 'emirate',
            'Master_Development': 'master_development',
            'Sub_Location_1': 'sub_location_1',
            'Sub_Location_2': 'sub_location_2',
            'Sub_Location_3': 'sub_location_3',
            'Developer': 'developer',
            'Developer_Property_Type': 'developer_property_type',
            'Status': 'status',
            'Launch_Date': 'launch_date',
            'Initial_Completion_Date': 'initial_completion_date',
            'Actual_Completion_Date': 'actual_completion_date',
            'Completion_Month': 'completion_month',
            'Completion_Year': 'completion_year',
            'Total_Units': 'total_units',
            'Total_Apartments': 'total_apartments',
            'Total_Hotel_Apartments': 'total_hotel_apartments',
            'total_Serviced_Apartments': 'total_serviced_apartments',
            'Total_Townhouses': 'total_townhouses',
            'Total_Villas': 'total_villas',
            'Total_Offices': 'total_offices',
            'Total_Retails': 'total_retails',
            'Total_Hotel_Rooms': 'total_hotel_rooms',
            'Completion_Percentage': 'completion_percentage',
            'Title_Type': 'title_type',
            'Location': 'location_address',
            'Height_Class': 'height_class',
        }
        df = df.rename(columns=column_mapping)

        # Parse dates (auto-detect format: M/D/YYYY or YYYY-MM-DD)
        for date_col in ['launch_date', 'initial_completion_date', 'actual_completion_date']:
            df[date_col] = pd.to_datetime(df[date_col], format='mixed', dayfirst=False, errors='coerce')

        # Normalize property types
        df['developer_property_type_normalized'] = df['developer_property_type'].map(
            lambda x: self.PROPERTY_TYPE_NORMALIZATION.get(str(x).strip(), 'Other') if pd.notna(x) else None
        )

        # Normalize statuses (case-insensitive lookup)
        status_lower_map = {k.lower(): v for k, v in self.STATUS_NORMALIZATION.items()}
        df['status_normalized'] = df['status'].map(
            lambda x: status_lower_map.get(str(x).strip().lower(), str(x).strip()) if pd.notna(x) else None
        )

        # Normalize developer names
        df['developer'] = df['developer'].map(
            lambda x: None if pd.isna(x) or str(x).strip() in ('None', '')
            else self.DEVELOPER_NORMALIZATION.get(str(x).strip(), str(x).strip())
        )

        # Normalize height class
        df['height_class'] = df['height_class'].map(
            lambda x: self.HEIGHT_CLASS_NORMALIZATION.get(str(x).strip(), str(x).strip())
            if pd.notna(x) and str(x).strip() not in ('None', '') else None
        )

        # Calculate delivery quarter
        df['delivery_quarter'] = df.apply(
            lambda row: self._calculate_delivery_quarter(row), axis=1
        )

        # Create location_full (concatenated hierarchy)
        df['location_full'] = df.apply(
            lambda row: ' > '.join(filter(pd.notna, [
                row.get('master_development'),
                row.get('sub_location_1'),
                row.get('sub_location_2'),
                row.get('sub_location_3')
            ])), axis=1
        )

        # Fill NaN numeric columns with 0
        numeric_cols = ['total_units', 'total_apartments', 'total_hotel_apartments',
                       'total_serviced_apartments', 'total_townhouses', 'total_villas',
                       'total_offices', 'total_retails', 'total_hotel_rooms', 'completion_percentage']
        df[numeric_cols] = df[numeric_cols].fillna(0).astype(int)

        # Calculate unit aggregations
        df['residential_units'] = (
            df['total_apartments'] +
            df['total_townhouses'] +
            df['total_villas']
        )
        df['commercial_units'] = (
            df['total_offices'] +
            df['total_retails']
        )
        df['hospitality_units'] = (
            df['total_hotel_apartments'] +
            df['total_serviced_apartments'] +
            df['total_hotel_rooms']
        )

        # Detect unit mismatches
        df['unit_sum'] = (
            df['total_apartments'] +
            df['total_hotel_apartments'] +
            df['total_serviced_apartments'] +
            df['total_townhouses'] +
            df['total_villas'] +
            df['total_offices'] +
            df['total_retails'] +
            df['total_hotel_rooms']
        )
        df['unit_mismatch_diff'] = df['total_units'] - df['unit_sum']
        df['has_unit_mismatch'] = df['unit_mismatch_diff'] != 0

        # Data quality flags
        df['has_zero_units'] = df['total_units'] == 0
        df['has_missing_developer'] = df['developer'].isna() | (df['developer'] == '')
        df['has_missing_launch_date'] = df['launch_date'].isna()
        df['has_missing_location'] = df['location_address'].isna() | (df['location_address'] == '')

        # Estimate missing launch dates (completion_date - 24 months)
        df['launch_date_is_estimated'] = False
        mask_missing_launch = df['has_missing_launch_date'] & df['actual_completion_date'].notna()
        df.loc[mask_missing_launch, 'launch_date'] = df.loc[mask_missing_launch, 'actual_completion_date'] - pd.DateOffset(months=24)
        df.loc[mask_missing_launch, 'launch_date_is_estimated'] = True

        # Derived business logic flags
        df['is_residential'] = df['residential_units'] > 0
        df['is_offplan'] = df['status_normalized'].isin(['Launched', 'Under Construction'])
        df['is_mixed_use'] = (df['residential_units'] > 0) & (df['commercial_units'] > 0)
        df['is_completed'] = df['status_normalized'] == 'Completed'

        return df

    def _calculate_delivery_quarter(self, row) -> Optional[str]:
        """Calculate delivery quarter from completion date"""
        if pd.notna(row.get('actual_completion_date')):
            date = row['actual_completion_date']
            quarter = (date.month - 1) // 3 + 1
            return f"Q{quarter} {date.year}"
        elif pd.notna(row.get('completion_year')) and pd.notna(row.get('completion_month')):
            try:
                month = int(row['completion_month'])
                year = int(row['completion_year'])
                quarter = (month - 1) // 3 + 1
                return f"Q{quarter} {year}"
            except:
                return None
        return None

    def _validate_and_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate data and calculate quality scores"""

        def calculate_quality_score(row):
            """Calculate quality score for a single row (0.0-1.0)"""
            score = 1.0

            # Required fields
            if pd.isna(row['project_id']) or pd.isna(row['project_name']):
                return 0.0, False

            # Location quality
            if pd.isna(row['master_development']) or row['master_development'] == '':
                score *= 0.6
                is_valid = False
            else:
                is_valid = True

            if row['has_missing_location']:
                score *= 0.9

            # Developer quality
            if row['has_missing_developer']:
                score *= 0.85

            # Timeline quality
            if row['has_missing_launch_date'] and not row['launch_date_is_estimated']:
                score *= 0.9

            # Unit data quality
            if row['has_zero_units']:
                is_valid = False  # Exclude from clean data

            if row['has_unit_mismatch']:
                if row['total_units'] >= 10:
                    score *= 0.8  # Flag but allow in project-level analysis
                else:
                    is_valid = False  # Exclude small projects with errors

            # Status quality
            if pd.isna(row['status_normalized']):
                score *= 0.9

            return score, is_valid

        # Apply quality scoring
        results = df.apply(lambda row: calculate_quality_score(row), axis=1)
        df['quality_score'] = results.apply(lambda x: x[0])
        df['is_valid'] = results.apply(lambda x: x[1])

        return df

    def _create_version(self, version_name: str, source_file: str,
                       record_count: int, valid_records: int) -> int:
        """Create version entry in data_versions table"""

        # Get next version_id
        result = self.con.execute(
            "SELECT COALESCE(MAX(version_id), 0) + 1 as next_id FROM data_versions"
        ).fetchone()
        version_id = result[0]

        # Deactivate previous supply versions
        self.con.execute("""
            UPDATE data_versions
            SET is_active = FALSE
            WHERE version_name LIKE 'Supply:%'
        """)

        # Insert version record
        self.con.execute("""
            INSERT INTO data_versions (
                version_id, version_name, import_date, source_file,
                record_count, valid_records, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, FALSE)
        """, [version_id, version_name, datetime.now(), source_file,
              record_count, valid_records])

        return version_id

    def _insert_data(self, df: pd.DataFrame):
        """Insert data into supply_projects_all table"""

        # Select columns for database (exclude temporary calculation columns)
        db_columns = [
            'project_id', 'project_name', 'emirate', 'master_development',
            'sub_location_1', 'sub_location_2', 'sub_location_3',
            'location_full', 'location_address',
            'developer', 'developer_property_type', 'developer_property_type_normalized',
            'status', 'status_normalized',
            'launch_date', 'initial_completion_date', 'actual_completion_date',
            'completion_month', 'completion_year', 'completion_percentage',
            'delivery_quarter',
            'total_units', 'total_apartments', 'total_hotel_apartments',
            'total_serviced_apartments', 'total_townhouses', 'total_villas',
            'total_offices', 'total_retails', 'total_hotel_rooms',
            'residential_units', 'commercial_units', 'hospitality_units',
            'height_class', 'title_type',
            'has_unit_mismatch', 'unit_mismatch_diff', 'has_zero_units',
            'has_missing_developer', 'has_missing_launch_date', 'has_missing_location',
            'launch_date_is_estimated',
            'is_residential', 'is_offplan', 'is_mixed_use', 'is_completed',
            'version_id', 'imported_at', 'quality_score', 'is_valid'
        ]

        df_insert = df[db_columns].copy()

        # Register DataFrame with DuckDB and insert
        self.con.register('df_temp', df_insert)
        self.con.execute("""
            INSERT INTO supply_projects_all
            SELECT * FROM df_temp
        """)
        self.con.unregister('df_temp')

    def _build_indexes(self):
        """Build performance indexes"""
        indexes_path = Path(__file__).parent.parent.parent / 'data' / 'pm-projects-supply' / 'sql' / 'indexes.sql'
        if indexes_path.exists():
            with open(indexes_path, 'r') as f:
                sql = f.read()
            # Execute each CREATE INDEX statement
            for statement in sql.split(';'):
                if statement.strip() and 'CREATE INDEX' in statement:
                    self.con.execute(statement)

    def _print_summary(self, df: pd.DataFrame, version_id: int):
        """Print data quality summary"""
        print(f"\n{'='*60}")
        print("DATA QUALITY SUMMARY")
        print(f"{'='*60}")

        print(f"\nTotal Projects: {len(df):,}")
        print(f"Valid Projects: {df['is_valid'].sum():,} ({df['is_valid'].sum()/len(df)*100:.1f}%)")
        print(f"Off-Plan Projects: {df['is_offplan'].sum():,} ({df['is_offplan'].sum()/len(df)*100:.1f}%)")
        print(f"Completed Projects: {df['is_completed'].sum():,} ({df['is_completed'].sum()/len(df)*100:.1f}%)")

        print(f"\n Quality Flags:")
        print(f"  - Projects with unit mismatches: {df['has_unit_mismatch'].sum():,} ({df['has_unit_mismatch'].sum()/len(df)*100:.1f}%)")
        print(f"  - Projects with zero units: {df['has_zero_units'].sum():,} ({df['has_zero_units'].sum()/len(df)*100:.1f}%)")
        print(f"  - Projects missing developer: {df['has_missing_developer'].sum():,} ({df['has_missing_developer'].sum()/len(df)*100:.1f}%)")
        print(f"  - Projects missing launch date: {df['has_missing_launch_date'].sum():,} ({df['has_missing_launch_date'].sum()/len(df)*100:.1f}%)")

        print(f"\n Quality Score Distribution:")
        print(f"  - Avg Quality Score: {df['quality_score'].mean():.2f}")
        print(f"  - Score >= 0.9: {(df['quality_score'] >= 0.9).sum():,} projects")
        print(f"  - Score >= 0.7: {(df['quality_score'] >= 0.7).sum():,} projects")
        print(f"  - Score < 0.7: {(df['quality_score'] < 0.7).sum():,} projects")

        print(f"\n Property Types (Top 10):")
        top_types = df['developer_property_type_normalized'].value_counts().head(10)
        for prop_type, count in top_types.items():
            print(f"  - {prop_type}: {count:,} projects")

        print(f"\n Top 10 Areas by Project Count:")
        top_areas = df[df['is_valid']]['master_development'].value_counts().head(10)
        for area, count in top_areas.items():
            print(f"  - {area}: {count:,} projects")

        print(f"\n Top 10 Developers by Project Count:")
        top_devs = df[df['is_valid'] & ~df['has_missing_developer']]['developer'].value_counts().head(10)
        for dev, count in top_devs.items():
            print(f"  - {dev}: {count:,} projects")

    def close(self):
        """Close database connection"""
        if hasattr(self, 'con'):
            self.con.close()


if __name__ == "__main__":
    # Example usage
    loader = SupplyLoader()
    csv_path = "data/pm-projects-supply/raw/Supply-Data-01-08-2026-15.csv"
    version_id = loader.load_csv(csv_path)
    loader.close()
    print(f"\n[OK] Data loaded successfully with version_id: {version_id}")
