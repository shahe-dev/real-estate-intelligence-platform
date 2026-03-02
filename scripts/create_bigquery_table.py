"""
Script to create a BigQuery table with dummy data in the propmonitor dataset
"""
import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from datetime import datetime, timedelta

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def create_dummy_table():
    """Create a table with dummy real estate data in the propmonitor dataset"""

    # Configuration
    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")
    DATASET_ID = "propmonitor"
    TABLE_ID = "dubai_listings_sample"

    print("=" * 60)
    print("Creating BigQuery Table with Dummy Data")
    print("=" * 60)

    # Step 1: Load credentials
    print(f"\n1. Loading credentials...")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    print(f"   ✓ Credentials loaded")
    print(f"   Service Account: {credentials.service_account_email}")

    # Step 2: Create BigQuery client
    print(f"\n2. Creating BigQuery client...")
    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )
    print(f"   ✓ Client created for project: {PROJECT_ID}")

    # Step 3: Check if dataset exists, create if not
    print(f"\n3. Checking dataset '{DATASET_ID}'...")
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    try:
        dataset = client.get_dataset(dataset_ref)
        print(f"   ✓ Dataset exists: {dataset_ref}")
    except Exception as e:
        print(f"   Dataset doesn't exist, creating it...")
        try:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            dataset = client.create_dataset(dataset, timeout=30)
            print(f"   ✓ Dataset created: {dataset_ref}")
        except Exception as create_error:
            if "Already Exists" in str(create_error):
                print(f"   ✓ Dataset already exists: {dataset_ref}")
            else:
                raise

    # Step 4: Create dummy data
    print(f"\n4. Creating dummy real estate data...")

    # Generate sample Dubai real estate listings
    base_date = datetime(2024, 1, 1)
    dummy_data = []

    communities = ["Dubai Marina", "Downtown Dubai", "Palm Jumeirah", "Business Bay", "JBR"]
    property_types = ["Apartment", "Villa", "Townhouse", "Penthouse"]

    for i in range(50):
        dummy_data.append({
            "listing_id": f"PROP_{1000 + i}",
            "property_type": property_types[i % len(property_types)],
            "community": communities[i % len(communities)],
            "bedrooms": (i % 4) + 1,
            "bathrooms": (i % 3) + 1,
            "area_sqft": 800 + (i * 50),
            "price_aed": 500000 + (i * 100000),
            "listing_date": (base_date + timedelta(days=i*7)).strftime("%Y-%m-%d"),
            "status": "Active" if i % 3 != 0 else "Sold",
            "agent_name": f"Agent_{(i % 10) + 1}",
            "views": i * 15,
            "created_at": datetime.now().isoformat()
        })

    df = pd.DataFrame(dummy_data)

    # Convert data types to match BigQuery schema
    df['listing_date'] = pd.to_datetime(df['listing_date']).dt.date
    df['created_at'] = pd.to_datetime(df['created_at'])

    print(f"   ✓ Generated {len(df)} dummy listings")
    print(f"\n   Sample data:")
    print(df.head(3).to_string(index=False))

    # Step 5: Create table and load data
    print(f"\n5. Creating table and loading data...")
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Define schema
    schema = [
        bigquery.SchemaField("listing_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("property_type", "STRING"),
        bigquery.SchemaField("community", "STRING"),
        bigquery.SchemaField("bedrooms", "INTEGER"),
        bigquery.SchemaField("bathrooms", "INTEGER"),
        bigquery.SchemaField("area_sqft", "INTEGER"),
        bigquery.SchemaField("price_aed", "INTEGER"),
        bigquery.SchemaField("listing_date", "DATE"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("agent_name", "STRING"),
        bigquery.SchemaField("views", "INTEGER"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    # Configure load job
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",  # Overwrite if table exists
    )

    # Load data from DataFrame
    job = client.load_table_from_dataframe(
        df, table_ref, job_config=job_config
    )

    # Wait for job to complete
    job.result()

    print(f"   ✓ Table created and data loaded: {table_ref}")
    print(f"   Rows loaded: {len(df)}")

    # Step 6: Verify table
    print(f"\n6. Verifying table...")
    query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT community) as unique_communities,
            AVG(price_aed) as avg_price,
            COUNT(CASE WHEN status = 'Active' THEN 1 END) as active_listings
        FROM `{table_ref}`
    """

    query_job = client.query(query)
    results = query_job.result()

    for row in results:
        print(f"   ✓ Table verification:")
        print(f"     Total rows: {row.total_rows}")
        print(f"     Unique communities: {row.unique_communities}")
        print(f"     Average price: {row.avg_price:,.0f} AED")
        print(f"     Active listings: {row.active_listings}")

    print(f"\n{'=' * 60}")
    print(f"✓ SUCCESS: Table created with dummy data!")
    print(f"{'=' * 60}")
    print(f"\nYou can now query this table:")
    print(f"  SELECT * FROM `{table_ref}` LIMIT 10")

    return table_ref


def query_prod_confidential_dataset(dataset_name, table_name=None):
    """Query data from the shared data project dataset"""

    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    BILLING_PROJECT = os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")
    DATA_PROJECT = os.getenv("BQ_DATA_PROJECT_ID", "your-data-project")

    print(f"\n{'=' * 60}")
    print(f"Querying shared dataset: {DATA_PROJECT}.{dataset_name}")
    print(f"{'=' * 60}")

    # Load credentials
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )

    # Create client (billing goes to your project)
    client = bigquery.Client(
        credentials=credentials,
        project=BILLING_PROJECT
    )

    print(f"\n1. Listing tables in {DATA_PROJECT}.{dataset_name}...")
    try:
        dataset_ref = f"{DATA_PROJECT}.{dataset_name}"
        tables = list(client.list_tables(dataset_ref))

        if tables:
            print(f"   ✓ Found {len(tables)} table(s):")
            for table in tables:
                print(f"     - {table.table_id}")

            # If no specific table specified, query the first one
            if not table_name and tables:
                table_name = tables[0].table_id
                print(f"\n2. Querying first table: {table_name}")

            if table_name:
                query = f"""
                    SELECT *
                    FROM `{DATA_PROJECT}.{dataset_name}.{table_name}`
                    LIMIT 5
                """

                print(f"   Running query...")
                query_job = client.query(query)
                results = query_job.result()

                print(f"   ✓ Query successful!")
                print(f"   Bytes processed: {query_job.total_bytes_processed:,}")
                print(f"\n   Sample rows:")

                for i, row in enumerate(results, 1):
                    print(f"   Row {i}: {dict(row)}")

                return True
        else:
            print(f"   ⚠ No tables found in dataset")
            return False

    except Exception as e:
        print(f"   ❌ ERROR: {type(e).__name__}")
        print(f"   {str(e)}")
        return False


if __name__ == "__main__":
    # Part 1: Create dummy table in your dataset
    table_ref = create_dummy_table()

    print("\n" + "=" * 60)
    print("NEXT STEPS:")
    print("=" * 60)
    print("\nTo query data from the shared data project dataset:")
    print("1. Find out the dataset name you have access to")
    print("2. Run this command:")
    print("\n   python create_bigquery_table.py --query-shared")
    print("\nOr use the function in your code:")
    print("   query_prod_confidential_dataset('dataset_name')")
    print("   query_prod_confidential_dataset('dataset_name', 'table_name')")
