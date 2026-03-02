"""
Export BigQuery table data to CSV
"""
import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from pathlib import Path

# Fix encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

def export_table_to_csv(project_id, dataset_id, table_id, output_file=None):
    """
    Export a BigQuery table to CSV

    Args:
        project_id: The project containing the table
        dataset_id: The dataset containing the table
        table_id: The table to export
        output_file: Path to output CSV file (optional)
    """

    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")
    BILLING_PROJECT = os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")

    print("=" * 60)
    print(f"Exporting BigQuery Table to CSV")
    print("=" * 60)

    # Load credentials
    print(f"\n1. Loading credentials...")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )
    print(f"   ✓ Credentials loaded")

    # Create client
    print(f"\n2. Creating BigQuery client...")
    client = bigquery.Client(
        credentials=credentials,
        project=BILLING_PROJECT
    )
    print(f"   ✓ Client created (billing to: {BILLING_PROJECT})")

    # Build table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    print(f"\n3. Querying table: {table_ref}")

    try:
        # Query all data from the table
        query = f"SELECT * FROM `{table_ref}`"

        print(f"   Running query...")
        query_job = client.query(query)
        results = query_job.result()

        # Convert to pandas DataFrame manually to avoid read session permission issue
        rows = [dict(row) for row in results]
        df = pd.DataFrame(rows)

        print(f"   ✓ Query successful!")
        print(f"   Rows retrieved: {len(df):,}")
        print(f"   Columns: {len(df.columns)}")

        # Show column names
        print(f"\n   Columns in table:")
        for col in df.columns:
            print(f"     - {col} ({df[col].dtype})")

        # Generate output filename if not provided
        if output_file is None:
            output_dir = Path("data/exports")
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / f"{table_id}.csv"
        else:
            output_file = Path(output_file)
            output_file.parent.mkdir(parents=True, exist_ok=True)

        # Export to CSV
        print(f"\n4. Exporting to CSV...")
        df.to_csv(output_file, index=False, encoding='utf-8')
        print(f"   ✓ Data exported to: {output_file.absolute()}")

        # Show sample data
        print(f"\n5. Sample data (first 3 rows):")
        print(df.head(3).to_string(index=False))

        print(f"\n{'=' * 60}")
        print(f"✓ SUCCESS: Table exported to CSV!")
        print(f"{'=' * 60}")
        print(f"\nFile location: {output_file.absolute()}")
        print(f"Total rows: {len(df):,}")

        return str(output_file.absolute())

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(f"   {str(e)}")
        return None


def export_query_to_csv(query, output_file, billing_project=os.getenv("GOOGLE_PROJECT_ID", "your-billing-project")):
    """
    Execute a custom query and export results to CSV

    Args:
        query: SQL query to execute
        output_file: Path to output CSV file
        billing_project: Project to bill the query to
    """

    SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "credentials/service-account.json")

    print("=" * 60)
    print(f"Executing Query and Exporting to CSV")
    print("=" * 60)

    # Load credentials
    print(f"\n1. Loading credentials...")
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/bigquery"]
    )

    # Create client
    client = bigquery.Client(
        credentials=credentials,
        project=billing_project
    )
    print(f"   ✓ Client ready (billing to: {billing_project})")

    # Execute query
    print(f"\n2. Executing query...")
    print(f"   Query preview: {query[:200]}...")

    try:
        query_job = client.query(query)
        results = query_job.result()

        # Convert to pandas DataFrame manually to avoid read session permission issue
        rows = [dict(row) for row in results]
        df = pd.DataFrame(rows)

        print(f"   ✓ Query successful!")
        print(f"   Rows retrieved: {len(df):,}")
        print(f"   Columns: {list(df.columns)}")

        # Ensure output directory exists
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Export to CSV
        print(f"\n3. Exporting to CSV...")
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"   ✓ Data exported to: {output_path.absolute()}")

        # Show sample data
        if len(df) > 0:
            print(f"\n4. Sample data (first 3 rows):")
            print(df.head(3).to_string(index=False))

        print(f"\n{'=' * 60}")
        print(f"✓ SUCCESS!")
        print(f"{'=' * 60}")

        return str(output_path.absolute())

    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}")
        print(f"   {str(e)}")
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Export BigQuery data to CSV')
    parser.add_argument('--project', default=os.getenv('GOOGLE_PROJECT_ID', 'your-billing-project'),
                        help='Project ID containing the table')
    parser.add_argument('--dataset', default='propmonitor',
                        help='Dataset ID')
    parser.add_argument('--table', default='dubai_listings_sample',
                        help='Table ID to export')
    parser.add_argument('--output', help='Output CSV file path')
    parser.add_argument('--query', help='Custom SQL query to execute')

    args = parser.parse_args()

    if args.query:
        # Custom query mode
        output_file = args.output or "data/exports/query_results.csv"
        export_query_to_csv(args.query, output_file)
    else:
        # Table export mode
        export_table_to_csv(
            project_id=args.project,
            dataset_id=args.dataset,
            table_id=args.table,
            output_file=args.output
        )
