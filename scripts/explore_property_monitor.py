"""
Explore Property Monitor BigQuery schema and sample data
"""
import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Configuration
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
BILLING_PROJECT = os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")
PROPERTY_MONITOR_VIEW = os.getenv("BQ_PROPERTY_MONITOR_VIEW", "your-project.property_monitor.property_monitor_sales_view")


def get_bigquery_client():
    """Create authenticated BigQuery client"""
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    return bigquery.Client(credentials=credentials, project=BILLING_PROJECT)


def explore_schema():
    """Explore the Property Monitor view schema"""
    print("=" * 70)
    print("Property Monitor Sales View - Schema Exploration")
    print("=" * 70)

    client = get_bigquery_client()

    # Get schema information
    print(f"\n1. Fetching schema from: {PROPERTY_MONITOR_VIEW}")

    # Query to get column names and types
    schema_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM `{BILLING_PROJECT}.property_monitor.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'property_monitor_sales_view'
        ORDER BY ordinal_position
    """

    try:
        # Try to get schema via INFORMATION_SCHEMA
        schema_job = client.query(schema_query)
        schema_results = list(schema_job.result())

        if schema_results:
            print(f"\n   Schema ({len(schema_results)} columns):")
            print("-" * 70)
            for row in schema_results:
                print(f"   {row.column_name:40} {row.data_type:20} {'NULL' if row.is_nullable == 'YES' else 'NOT NULL'}")
    except Exception as e:
        print(f"   Could not fetch INFORMATION_SCHEMA: {e}")
        print("   Falling back to sample query...")

    # Get sample data to understand the structure
    print(f"\n2. Fetching sample data...")
    sample_query = f"""
        SELECT *
        FROM `{PROPERTY_MONITOR_VIEW}`
        LIMIT 5
    """

    query_job = client.query(sample_query)
    results = query_job.result()

    # Get field names from the results
    rows = list(results)
    if rows:
        print(f"\n   Columns detected from sample ({len(rows[0].keys())} fields):")
        print("-" * 70)
        for i, key in enumerate(rows[0].keys(), 1):
            sample_value = rows[0][key]
            value_type = type(sample_value).__name__
            sample_str = str(sample_value)[:50] if sample_value is not None else "NULL"
            print(f"   {i:3}. {key:40} ({value_type:10}) = {sample_str}")

    # Get row count
    print(f"\n3. Getting total row count...")
    count_query = f"""
        SELECT COUNT(*) as total_rows
        FROM `{PROPERTY_MONITOR_VIEW}`
    """
    count_job = client.query(count_query)
    count_result = list(count_job.result())[0]
    print(f"   Total rows: {count_result.total_rows:,}")

    # Get date range
    print(f"\n4. Checking date range...")
    date_query = f"""
        SELECT
            MIN(transaction_date) as earliest_date,
            MAX(transaction_date) as latest_date
        FROM `{PROPERTY_MONITOR_VIEW}`
    """
    try:
        date_job = client.query(date_query)
        date_result = list(date_job.result())[0]
        print(f"   Date range: {date_result.earliest_date} to {date_result.latest_date}")
    except Exception as e:
        print(f"   Could not determine date range: {e}")
        # Try to find date column
        print("   Looking for date columns in sample data...")

    # Get property type distribution
    print(f"\n5. Property type distribution...")
    prop_query = f"""
        SELECT
            property_type,
            COUNT(*) as count
        FROM `{PROPERTY_MONITOR_VIEW}`
        GROUP BY property_type
        ORDER BY count DESC
        LIMIT 20
    """
    try:
        prop_job = client.query(prop_query)
        prop_results = list(prop_job.result())
        print("-" * 40)
        for row in prop_results:
            print(f"   {row.property_type:30} {row.count:>10,}")
    except Exception as e:
        print(f"   Could not get property types: {e}")

    # Get area distribution
    print(f"\n6. Top areas by transaction count...")
    area_query = f"""
        SELECT
            area,
            COUNT(*) as count
        FROM `{PROPERTY_MONITOR_VIEW}`
        GROUP BY area
        ORDER BY count DESC
        LIMIT 15
    """
    try:
        area_job = client.query(area_query)
        area_results = list(area_job.result())
        print("-" * 40)
        for row in area_results:
            print(f"   {row.area:30} {row.count:>10,}")
    except Exception as e:
        print(f"   Could not get areas: {e}")

    # Sample full rows as DataFrame
    print(f"\n7. Full sample rows (as DataFrame):")
    print("-" * 70)
    df = pd.DataFrame([dict(row) for row in rows])
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df.to_string())

    print(f"\n{'=' * 70}")
    print("Schema exploration complete!")
    print("=" * 70)

    return df


if __name__ == "__main__":
    df = explore_schema()
