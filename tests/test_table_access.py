"""
Test direct table access to property_monitor_sales
"""
import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def test_table_access():
    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    BILLING_PROJECT = os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")

    print("=" * 60)
    print("Testing Table Access: property_monitor_sales")
    print("=" * 60)

    # Load credentials
    print("\n1. Loading service account credentials...")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    print(f"   ✓ Service Account: {credentials.service_account_email}")

    # Create client
    print(f"\n2. Creating BigQuery client (billing to {BILLING_PROJECT})...")
    client = bigquery.Client(
        credentials=credentials,
        project=BILLING_PROJECT
    )
    print(f"   ✓ Client created")

    # Test 1: Get table metadata
    print(f"\n3. Attempting to get table metadata...")
    try:
        table = client.get_table(f"{os.getenv(chr(39) + chr(66) + chr(81) + chr(95) + chr(68) + chr(65) + chr(84) + chr(65) + chr(95) + chr(80) + chr(82) + chr(79) + chr(74) + chr(69) + chr(67) + chr(84) + chr(95) + chr(73) + chr(68) + chr(39), chr(39) + chr(121) + chr(111) + chr(117) + chr(114) + chr(45) + chr(100) + chr(97) + chr(116) + chr(97) + chr(45) + chr(112) + chr(114) + chr(111) + chr(106) + chr(101) + chr(99) + chr(116) + chr(39))}.Test1C.property_monitor_sales")
        print(f"   ✓ Table exists!")
        print(f"   Schema fields: {len(table.schema)}")
        print(f"   Table size: {table.num_rows:,} rows")
        print(f"   Created: {table.created}")
        print(f"\n   Columns:")
        for field in table.schema[:10]:  # Show first 10 columns
            print(f"     - {field.name} ({field.field_type})")
    except Exception as e:
        print(f"   ❌ Cannot access table metadata: {type(e).__name__}")
        print(f"   {str(e)}")

    # Test 2: Try simple query
    print(f"\n4. Attempting to query table (LIMIT 1)...")
    data_project = os.getenv("BQ_DATA_PROJECT_ID", "your-data-project")
    try:
        query = f"""
            SELECT *
            FROM `{data_project}.Test1C.property_monitor_sales`
            LIMIT 1
        """
        print(f"   Running query...")
        query_job = client.query(query)
        results = list(query_job.result())

        print(f"   ✓ Query successful!")
        print(f"   Rows returned: {len(results)}")

        if results:
            print(f"\n   First row columns: {list(dict(results[0]).keys())}")

        return True

    except Exception as e:
        print(f"   ❌ Query failed: {type(e).__name__}")
        print(f"   {str(e)}")

        # Check if it's a specific permission issue
        error_str = str(e).lower()
        if "permission" in error_str:
            print(f"\n   This appears to be a permission issue.")
            print(f"   The service account needs:")
            print(f"   1. bigquery.tables.getData - to read table data")
            print(f"   2. bigquery.jobs.create - to run queries (already have this)")
        elif "does not exist" in error_str:
            print(f"\n   The table might not exist or the path is incorrect")

        return False

    print(f"\n{'=' * 60}")

if __name__ == "__main__":
    test_table_access()
