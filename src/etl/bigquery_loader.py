# src/etl/bigquery_loader.py

"""
BigQuery Data Loader for Property Monitor Sales Data
Fetches data from BigQuery authorized view and loads into local DuckDB

This uses a SEPARATE database from the DLD data to keep systems independent.
Database: data/database/property_monitor.db
"""

import duckdb
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from google.cloud import bigquery
from google.oauth2 import service_account

from config.bigquery_settings import bq_settings
from config.validation_rules import ValidationRules


class BigQueryLoader:
    """Load Property Monitor data from BigQuery into local DuckDB"""

    # BigQuery configuration (from separate settings)
    SERVICE_ACCOUNT_FILE = bq_settings.SERVICE_ACCOUNT_FILE
    BILLING_PROJECT = bq_settings.BILLING_PROJECT
    PROPERTY_MONITOR_VIEW = bq_settings.PROPERTY_MONITOR_VIEW

    # Column mapping: Property Monitor -> Internal schema
    COLUMN_MAPPING = {
        'id': 'transaction_id',
        'evidence_date': 'instance_date',
        'master_development': 'area_name_en',
        'sub_loc_1': 'project_name_en',
        'dev_name': 'master_project_en',  # Developer name. Aliased to 'developer' in transactions_verified.
        'no_beds': 'rooms_en',
        'total_sales_price': 'actual_worth',
        'sales_price_sqm_unit': 'meter_sale_price',
        'unit_bua_sqm': 'procedure_area',
        'unit_type': 'property_type_en',
        'sale_sequence': 'reg_type_en',
        'Year': 'transaction_year',
        'Month': 'transaction_month',
    }

    # Room type mapping: PM format -> DLD format
    ROOMS_MAPPING = {
        's': 'Studio',
        'S': 'Studio',
        'studio': 'Studio',
        '1': '1 B/R',
        '2': '2 B/R',
        '3': '3 B/R',
        '4': '4 B/R',
        '5': '5 B/R',
        '6': '6 B/R',
        '7': '7 B/R',
        '7+': '7+ B/R',
    }

    # Property type mapping: PM format -> standardized
    PROPERTY_TYPE_MAPPING = {
        'Apartment': 'Unit',
        'Hotel Apartment': 'Unit',
        'Townhouse': 'Townhouse',
        'Villa': 'Villa',
        'Land': 'Land',
        'Whole Building': 'Building',
        'Park': 'Land',
        'Resort': 'Building',
    }

    def __init__(self, db_path=None):
        # Use separate Property Monitor database by default
        self.db_path = db_path or bq_settings.PM_DB_PATH
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self.rules = ValidationRules()
        self.bq_client = None
        self._init_schema()

    def _get_bq_client(self):
        """Get or create BigQuery client

        Supports two authentication methods:
        1. Environment variables (for Codespaces): GOOGLE_PROJECT_ID, GOOGLE_PRIVATE_KEY, GOOGLE_CLIENT_EMAIL
        2. Service account file (for local development): Falls back to JSON file
        """
        if self.bq_client is None:
            # Try environment variables first (for Codespaces)
            if os.getenv("GOOGLE_PROJECT_ID") and os.getenv("GOOGLE_PRIVATE_KEY"):
                print("  Using environment variable credentials (Codespaces mode)")
                credentials = service_account.Credentials.from_service_account_info(
                    {
                        "type": "service_account",
                        "project_id": os.getenv("GOOGLE_PROJECT_ID"),
                        "private_key": os.getenv("GOOGLE_PRIVATE_KEY").replace("\\n", "\n"),
                        "client_email": os.getenv("GOOGLE_CLIENT_EMAIL"),
                        "token_uri": "https://oauth2.googleapis.com/token",
                    },
                    scopes=["https://www.googleapis.com/auth/bigquery"]
                )
            else:
                # Fall back to file (for local development)
                print("  Using service account file credentials (local mode)")
                credentials = service_account.Credentials.from_service_account_file(
                    self.SERVICE_ACCOUNT_FILE,
                    scopes=["https://www.googleapis.com/auth/bigquery"]
                )

            self.bq_client = bigquery.Client(
                credentials=credentials,
                project=self.BILLING_PROJECT
            )
        return self.bq_client

    def _init_schema(self):
        """Initialize database schema for Property Monitor data"""

        # Data versions table
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS data_versions (
                version_id INTEGER PRIMARY KEY,
                version_name VARCHAR,
                import_date TIMESTAMP,
                source_file VARCHAR,
                record_count INTEGER,
                valid_records INTEGER,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)

        # Main transactions table - adapted for Property Monitor
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS transactions_all (
                transaction_id VARCHAR,
                procedure_id INTEGER,
                trans_group_id INTEGER,
                trans_group_en VARCHAR,
                instance_date VARCHAR,
                property_type_en VARCHAR,
                property_sub_type_en VARCHAR,
                reg_type_en VARCHAR,
                area_name_en VARCHAR,
                project_name_en VARCHAR,
                master_project_en VARCHAR,
                nearest_metro_en VARCHAR,
                nearest_mall_en VARCHAR,
                nearest_landmark_en VARCHAR,
                rooms_en VARCHAR,
                has_parking INTEGER,
                procedure_area DOUBLE,
                actual_worth DOUBLE,
                meter_sale_price DOUBLE,
                rent_value DOUBLE,

                version_id INTEGER,
                imported_at TIMESTAMP,
                quality_score DOUBLE,
                is_valid BOOLEAN,
                is_luxury BOOLEAN,
                transaction_year INTEGER,
                transaction_month INTEGER,

                -- Property Monitor specific fields
                unit_type_original VARCHAR,
                off_plan INTEGER,
                sale_sequence VARCHAR,
                sub_loc_2 VARCHAR,
                sub_loc_3 VARCHAR,

                PRIMARY KEY (transaction_id, version_id)
            )
        """)

        # Current transactions view
        self.con.execute("""
            CREATE OR REPLACE VIEW transactions_current AS
            SELECT t.*
            FROM transactions_all t
            INNER JOIN (
                SELECT version_id
                FROM data_versions
                WHERE is_active = TRUE
                ORDER BY import_date DESC
                LIMIT 1
            ) v ON t.version_id = v.version_id
        """)

        # Clean transactions (valid only, excludes Unknown transaction types)
        self.con.execute("""
            CREATE OR REPLACE VIEW transactions_clean AS
            SELECT *
            FROM transactions_current
            WHERE is_valid = TRUE
              AND quality_score >= 0.7
              AND reg_type_en != 'Unknown'
        """)

        # Luxury view
        self.con.execute("""
            CREATE OR REPLACE VIEW transactions_luxury AS
            SELECT *
            FROM transactions_clean
            WHERE is_luxury = TRUE
        """)

        # Verified view: STRICT column whitelist for safe downstream use.
        # Renames ambiguous fields to prevent misclassification:
        #   - property_type_en dropped (lossy mapping: Townhouse was merged into Villa)
        #   - off_plan renamed to originally_offplan (distinct from transaction_type)
        #   - sale_sequence dropped (raw value; transaction_type is the clean version)
        #
        # Three off-plan columns (each answers a DIFFERENT question):
        #   transaction_type:    Is THIS transaction off-plan or resale? (from sale_sequence)
        #   originally_offplan:  Was this property EVER sold off-plan? (historical flag)
        #   sale_classification: Human-readable label combining both dimensions:
        #       'Off-Plan (Developer Sale)'  = bought directly from developer off-plan
        #       'Resale (Off-Plan Origin)'   = resale of a property first sold off-plan
        #       'Resale'                     = standard secondary market resale
        #
        # For reports: use sale_classification. For filtering: use transaction_type.
        # NEVER use originally_offplan alone to count "off-plan transactions."
        #
        # All PM metrics, API, and content queries MUST use this view.
        self.con.execute("""
            CREATE OR REPLACE VIEW transactions_verified AS
            SELECT
                transaction_id,
                instance_date,
                unit_type_original AS property_type,
                reg_type_en AS transaction_type,
                off_plan AS originally_offplan,
                CASE
                    WHEN reg_type_en = 'Off-Plan' THEN 'Off-Plan (Developer Sale)'
                    WHEN reg_type_en = 'Existing' AND off_plan = 1 THEN 'Resale (Off-Plan Origin)'
                    WHEN reg_type_en = 'Existing' THEN 'Resale'
                END AS sale_classification,
                area_name_en,
                project_name_en,
                sub_loc_2 AS building_name,
                sub_loc_3 AS sub_location_3,
                master_project_en AS developer,
                rooms_en,
                has_parking,
                procedure_area,
                actual_worth,
                meter_sale_price,
                rent_value,
                is_luxury,
                transaction_year,
                transaction_month
            FROM transactions_clean
        """)

        self.con.execute("""
            CREATE OR REPLACE VIEW transactions_verified_luxury AS
            SELECT *
            FROM transactions_verified
            WHERE is_luxury = TRUE
        """)

    def fetch_from_bigquery(self, limit=None, year_filter=None):
        """
        Fetch data from Property Monitor BigQuery view

        Args:
            limit: Optional row limit for testing
            year_filter: Optional year to filter (e.g., 2024)

        Returns:
            List of row dictionaries
        """
        client = self._get_bq_client()

        where_clauses = []
        if year_filter:
            where_clauses.append(f"Year = {year_filter}")

        where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        limit_sql = f"LIMIT {limit}" if limit else ""

        query = f"""
            SELECT
                CAST(id AS STRING) as id,
                transaction_type,
                evidence_date,
                master_development,
                sub_loc_1,
                sub_loc_2,
                sub_loc_3,
                no_beds,
                total_sales_price,
                sales_price_sqm_unit,
                unit_bua_sqft,
                unit_bua_sqm,
                plot_size_sqft,
                plot_size_sqm,
                unit_type,
                sale_sequence,
                dev_name,
                Month,
                Month_Name,
                Year,
                sales_price_sqft_unit,
                off_plan
            FROM `{self.PROPERTY_MONITOR_VIEW}`
            {where_sql}
            ORDER BY evidence_date DESC
            {limit_sql}
        """

        print(f"   Executing BigQuery...")
        query_job = client.query(query)
        results = query_job.result()

        rows = [dict(row) for row in results]
        print(f"   Fetched {len(rows):,} rows from BigQuery")
        print(f"   Bytes processed: {query_job.total_bytes_processed:,}")

        return rows

    def _transform_row(self, row):
        """Transform Property Monitor row to internal schema"""

        # Map rooms
        rooms_raw = str(row.get('no_beds', '') or '').strip()
        rooms_en = self.ROOMS_MAPPING.get(rooms_raw, rooms_raw + ' B/R' if rooms_raw.isdigit() else rooms_raw)

        # Map property type
        unit_type_original = row.get('unit_type', '')
        property_type_en = self.PROPERTY_TYPE_MAPPING.get(unit_type_original, 'Unit')

        # Map registration type (off-plan vs secondary/existing)
        # IMPORTANT: Use sale_sequence as the sole source of truth
        # The off_plan flag indicates if property was ORIGINALLY sold off-plan,
        # NOT whether this specific transaction is off-plan vs resale.
        # See audit findings: 109,021 transactions had sale_sequence='ReSale'
        # but off_plan=1, causing incorrect classification.
        sale_sequence = row.get('sale_sequence', '')
        if sale_sequence == 'Offplan':
            reg_type_en = 'Off-Plan'
        elif sale_sequence == 'ReSale':
            reg_type_en = 'Existing'
        else:
            # Unknown/null sale_sequence - mark as Unknown for audit trail
            reg_type_en = 'Unknown'

        # Format date
        evidence_date = row.get('evidence_date')
        if evidence_date:
            instance_date = str(evidence_date)
        else:
            instance_date = None

        return {
            'transaction_id': str(row.get('id', '')),
            'procedure_id': None,
            'trans_group_id': 1,  # Sales
            'trans_group_en': 'Sales',
            'instance_date': instance_date,
            'property_type_en': property_type_en,
            'property_sub_type_en': unit_type_original,
            'reg_type_en': reg_type_en,
            'area_name_en': row.get('master_development', ''),
            'project_name_en': row.get('sub_loc_1', ''),
            'master_project_en': row.get('dev_name', ''),
            'nearest_metro_en': None,
            'nearest_mall_en': None,
            'nearest_landmark_en': None,
            'rooms_en': rooms_en,
            'has_parking': None,
            'procedure_area': row.get('unit_bua_sqm'),
            'actual_worth': row.get('total_sales_price'),
            'meter_sale_price': row.get('sales_price_sqm_unit'),
            'rent_value': None,
            'transaction_year': row.get('Year'),
            'transaction_month': row.get('Month'),
            'unit_type_original': unit_type_original,
            'off_plan': row.get('off_plan'),
            'sale_sequence': sale_sequence,
            'sub_loc_2': row.get('sub_loc_2', ''),
            'sub_loc_3': row.get('sub_loc_3', ''),
        }

    # Reliable property types for analysis
    # See docs/PROPERTY_MONITOR_DATA.md for full details
    RELIABLE_UNIT_TYPES = {'Apartment', 'Townhouse', 'Villa'}

    # Unreliable types excluded from clean data:
    # - Land: Only 2023 data, varied quality
    # - Hotel Apartment: Only 2023 data
    # - Whole Building: Commercial deals, not residential
    # - Park: Outliers (2 transactions - Miracle Garden, Butterfly Garden)
    # - Resort: Single 550M deal
    UNRELIABLE_UNIT_TYPES = {'Land', 'Hotel Apartment', 'Whole Building', 'Park', 'Resort'}

    # Minimum price thresholds (no upper limits for Property Monitor data)
    MIN_PRICE_THRESHOLDS = {
        'Unit': 50_000,      # Apartments
        'Townhouse': 100_000,  # Townhouses
        'Villa': 100_000,    # Villas
    }

    def _validate_row(self, row):
        """Validate a transformed row and calculate quality score

        Property Monitor validation rules:
        - Only include reliable property types (Apartment, Townhouse, Villa)
        - Exclude Land, Hotel Apartment, Whole Building, Park, Resort
        - Price must be above minimum threshold (no upper limits)
        - Required fields must exist

        See docs/PROPERTY_MONITOR_DATA.md for rationale.

        Returns:
            tuple: (quality_score, is_valid, is_luxury)
        """

        quality_score = 1.0
        is_valid = True

        # Check required fields
        if not row.get('transaction_id'):
            return 0.0, False, False
        if not row.get('area_name_en'):
            quality_score *= 0.8

        # Filter out unreliable property types
        unit_type = row.get('unit_type_original', '')
        if unit_type in self.UNRELIABLE_UNIT_TYPES:
            return 0.0, False, False

        # Only allow reliable unit types
        if unit_type not in self.RELIABLE_UNIT_TYPES:
            return 0.0, False, False

        # Validate price - only check minimum, no upper limit
        price = row.get('actual_worth')
        prop_type = row.get('property_type_en', 'Unit')

        if price and price > 0:
            min_price = self.MIN_PRICE_THRESHOLDS.get(prop_type, 10_000)
            if price < min_price:
                return 0.0, False, False
        else:
            # Missing price - invalid
            return 0.0, False, False

        # Check luxury status (5M+ AED threshold)
        is_luxury = price >= 5_000_000

        return quality_score, is_valid, is_luxury

    def load_from_bigquery(self, limit=None, year_filter=None):
        """
        Full ETL: Fetch from BigQuery, transform, validate, and load to DuckDB

        Args:
            limit: Optional row limit
            year_filter: Optional year filter

        Returns:
            version_id of the loaded data
        """
        print("=" * 60)
        print("Property Monitor BigQuery Data Import")
        print("=" * 60)

        # Get next version ID
        version_id = self._get_next_version_id()
        version_name = f"pm_v{version_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        print(f"\n1. Creating version: {version_name}")

        # Fetch from BigQuery
        print(f"\n2. Fetching from BigQuery...")
        rows = self.fetch_from_bigquery(limit=limit, year_filter=year_filter)

        if not rows:
            print("   No data fetched!")
            return None

        # Transform and validate
        print(f"\n3. Transforming and validating {len(rows):,} rows...")

        transformed_rows = []
        valid_count = 0
        invalid_count = 0
        luxury_count = 0

        for i, raw_row in enumerate(rows):
            if i % 50000 == 0 and i > 0:
                print(f"   Processed {i:,} rows...")

            # Transform
            row = self._transform_row(raw_row)

            # Validate
            quality_score, is_valid, is_luxury = self._validate_row(row)

            row['quality_score'] = quality_score
            row['is_valid'] = is_valid
            row['is_luxury'] = is_luxury
            row['version_id'] = version_id
            row['imported_at'] = datetime.now().isoformat()

            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1

            if is_luxury:
                luxury_count += 1

            transformed_rows.append(row)

        print(f"   Valid: {valid_count:,} ({valid_count/len(rows)*100:.1f}%)")
        print(f"   Invalid: {invalid_count:,}")
        print(f"   Luxury: {luxury_count:,}")

        # Insert into DuckDB
        print(f"\n4. Inserting into DuckDB...")

        # Create temp table from transformed data
        import pandas as pd
        df = pd.DataFrame(transformed_rows)

        self.con.execute("DROP TABLE IF EXISTS temp_pm_import")
        self.con.execute("CREATE TABLE temp_pm_import AS SELECT * FROM df")

        # Insert into main table (deduplicate: BigQuery view can return duplicate IDs)
        self.con.execute(f"""
            INSERT INTO transactions_all
            SELECT
                transaction_id,
                procedure_id,
                trans_group_id,
                trans_group_en,
                instance_date,
                property_type_en,
                property_sub_type_en,
                reg_type_en,
                area_name_en,
                project_name_en,
                master_project_en,
                nearest_metro_en,
                nearest_mall_en,
                nearest_landmark_en,
                rooms_en,
                has_parking,
                procedure_area,
                actual_worth,
                meter_sale_price,
                rent_value,
                version_id,
                CAST(imported_at AS TIMESTAMP) as imported_at,
                quality_score,
                is_valid,
                is_luxury,
                transaction_year,
                transaction_month,
                unit_type_original,
                off_plan,
                sale_sequence,
                sub_loc_2,
                sub_loc_3
            FROM temp_pm_import
            QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY instance_date DESC) = 1
        """)

        # Save version metadata
        source_desc = f"BigQuery: {self.PROPERTY_MONITOR_VIEW}"
        if year_filter:
            source_desc += f" (Year={year_filter})"
        if limit:
            source_desc += f" (Limit={limit})"

        self.con.execute(f"""
            INSERT INTO data_versions VALUES (
                {version_id},
                '{version_name}',
                CURRENT_TIMESTAMP,
                '{source_desc}',
                {len(rows)},
                {valid_count},
                TRUE
            )
        """)

        # Deactivate old transaction versions (preserve supply versions)
        self.con.execute(f"""
            UPDATE data_versions
            SET is_active = FALSE
            WHERE version_id < {version_id}
              AND version_name NOT LIKE 'Supply:%'
        """)

        # Cleanup
        self.con.execute("DROP TABLE IF EXISTS temp_pm_import")

        print(f"\n{'=' * 60}")
        print(f"SUCCESS: Version {version_id} loaded")
        print(f"{'=' * 60}")
        print(f"  Total records: {len(rows):,}")
        print(f"  Valid records: {valid_count:,}")
        print(f"  Luxury properties: {luxury_count:,}")

        return version_id

    def _get_next_version_id(self):
        """Get next version ID"""
        result = self.con.execute("""
            SELECT COALESCE(MAX(version_id), 0) + 1 FROM data_versions
        """).fetchone()
        return result[0]

    def get_data_summary(self):
        """Get summary of loaded data"""
        try:
            summary = self.con.execute("""
                SELECT
                    COUNT(*) as total_transactions,
                    COUNT(DISTINCT area_name_en) as unique_areas,
                    COUNT(DISTINCT project_name_en) as unique_projects,
                    MIN(instance_date) as earliest_date,
                    MAX(instance_date) as latest_date,
                    SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
                    AVG(actual_worth) as avg_price,
                    SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count
                FROM transactions_clean
            """).fetchone()

            return {
                'total_transactions': summary[0],
                'unique_areas': summary[1],
                'unique_projects': summary[2],
                'earliest_date': summary[3],
                'latest_date': summary[4],
                'luxury_count': summary[5],
                'avg_price': summary[6],
                'offplan_count': summary[7],
            }
        except Exception as e:
            return {'error': str(e)}

    def close(self):
        """Close connections"""
        self.con.close()


def run_import(limit=None, year_filter=None):
    """Run the Property Monitor import"""
    loader = BigQueryLoader()
    try:
        version_id = loader.load_from_bigquery(limit=limit, year_filter=year_filter)
        if version_id:
            print("\nData Summary:")
            summary = loader.get_data_summary()
            for key, value in summary.items():
                if isinstance(value, float):
                    print(f"  {key}: {value:,.2f}")
                elif isinstance(value, int):
                    print(f"  {key}: {value:,}")
                else:
                    print(f"  {key}: {value}")
        return version_id
    finally:
        loader.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Import Property Monitor data from BigQuery')
    parser.add_argument('--limit', type=int, help='Limit number of rows to import')
    parser.add_argument('--year', type=int, help='Filter by year (e.g., 2024)')

    args = parser.parse_args()

    run_import(limit=args.limit, year_filter=args.year)
