"""
Test script to verify BigQuery connection and query access
"""
import os
from google.cloud import bigquery
from google.oauth2 import service_account

def test_bigquery_connection():
    """Test BigQuery connection and query capabilities"""

    # Configuration
    # Service account project used for billing (GOOGLE_PROJECT_ID)
    # Data project to query (BQ_DATA_PROJECT_ID)
    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    SERVICE_ACCOUNT_PROJECT = os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")  # Project where service account was created (used for billing)
    DATA_PROJECT_ID = os.getenv("BQ_DATA_PROJECT_ID", "your-data-project")  # Project containing the datasets to query

    # Optional: specify the dataset and project you want to query
    # Format: project_id.dataset_id.table_id
    # Query to list accessible datasets in the data project
    # Note: You need to know at least one dataset name to query, or use __TABLES__ summary
    TEST_QUERY = f"""
        SELECT
            schema_name as dataset_name,
            option_name,
            option_value
        FROM
            `{DATA_PROJECT_ID}.region-us.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
        LIMIT 10
    """

    print("=" * 60)
    print("BigQuery Connection Test")
    print("=" * 60)

    # Step 1: Check if service account file exists
    print(f"\n1. Checking service account file...")
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"   ❌ ERROR: Service account file not found at: {SERVICE_ACCOUNT_FILE}")
        print(f"   Please update SERVICE_ACCOUNT_FILE path in this script")
        return False
    print(f"   ✓ Service account file found")

    try:
        # Step 2: Load credentials
        print(f"\n2. Loading credentials from service account...")
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )
        print(f"   ✓ Credentials loaded successfully")
        print(f"   Service Account: {credentials.service_account_email}")

        # Step 3: Create BigQuery client
        print(f"\n3. Creating BigQuery client...")
        print(f"   Service Account Project: {SERVICE_ACCOUNT_PROJECT}")
        print(f"   Data Project: {DATA_PROJECT_ID}")
        print(f"   Using service account project for billing...")
        client = bigquery.Client(
            credentials=credentials,
            project=SERVICE_ACCOUNT_PROJECT  # Use service account project for billing
        )
        print(f"   ✓ BigQuery client created")
        print(f"   Billing Project: {client.project}")

        # Step 4: List accessible datasets
        print(f"\n4. Listing accessible datasets in {DATA_PROJECT_ID}...")
        try:
            datasets = list(client.list_datasets(project=DATA_PROJECT_ID))
            if datasets:
                print(f"   ✓ Found {len(datasets)} accessible dataset(s):")
                for dataset in datasets:
                    print(f"     - {dataset.project}.{dataset.dataset_id}")
            else:
                print(f"   ℹ No datasets found in project {DATA_PROJECT_ID}")
                print(f"   The service account may not have 'bigquery.datasets.get' permission")
                print(f"   or datasets exist but aren't visible to list")
        except Exception as e:
            print(f"   ⚠ Cannot list datasets: {e}")
            print(f"   This is OK if you can still query specific tables")

        # Step 5: Test query execution
        print(f"\n5. Testing query execution...")
        print(f"   Running query: {TEST_QUERY[:100]}...")

        query_job = client.query(TEST_QUERY)
        results = query_job.result()

        print(f"   ✓ Query executed successfully")
        print(f"   Total bytes processed: {query_job.total_bytes_processed:,} bytes")
        print(f"   Total bytes billed: {query_job.total_bytes_billed:,} bytes")

        # Display results
        print(f"\n6. Query Results:")
        row_count = 0
        for row in results:
            row_count += 1
            print(f"   Row {row_count}: {dict(row)}")

        if row_count == 0:
            print(f"   No rows returned")

        print(f"\n{'=' * 60}")
        print(f"✓ SUCCESS: BigQuery connection and query test completed!")
        print(f"{'=' * 60}")
        return True

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(f"   {str(e)}")
        print(f"\n{'=' * 60}")
        print(f"Test failed. Please check:")
        print(f"  1. Service account file path is correct")
        print(f"  2. Service account has BigQuery access")
        print(f"  3. The dataset you're trying to query is accessible")
        print(f"  4. Your project ID is correct")
        print(f"{'=' * 60}")
        return False

def test_specific_dataset(dataset_project_id, dataset_id):
    """Test access to a specific dataset from another project"""

    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    BILLING_PROJECT = os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")  # Project for billing the queries

    print(f"\n{'=' * 60}")
    print(f"Testing access to dataset: {dataset_project_id}.{dataset_id}")
    print(f"{'=' * 60}")

    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/bigquery"]
        )

        client = bigquery.Client(
            credentials=credentials,
            project=BILLING_PROJECT  # Use for billing
        )

        # List tables in the dataset
        print(f"\nListing tables in dataset...")
        dataset_ref = f"{dataset_project_id}.{dataset_id}"
        tables = list(client.list_tables(dataset_ref))

        if tables:
            print(f"✓ Found {len(tables)} table(s):")
            for table in tables:
                print(f"  - {table.table_id}")

            # Try to query the first table
            if tables:
                first_table = tables[0]
                query = f"""
                    SELECT *
                    FROM `{dataset_project_id}.{dataset_id}.{first_table.table_id}`
                    LIMIT 5
                """
                print(f"\nTesting query on first table: {first_table.table_id}")
                query_job = client.query(query)
                results = query_job.result()

                print(f"✓ Query successful! Sample rows:")
                for i, row in enumerate(results, 1):
                    print(f"  Row {i}: {dict(row)}")
        else:
            print(f"⚠ No tables found in dataset")

        return True

    except Exception as e:
        print(f"❌ ERROR: {type(e).__name__}: {str(e)}")
        return False

if __name__ == "__main__":
    # Basic connection test
    success = test_bigquery_connection()

    # Uncomment and fill in to test access to a specific dataset from another project:
    # test_specific_dataset(
    #     dataset_project_id="other-project-id",
    #     dataset_id="dataset_name"
    # )
