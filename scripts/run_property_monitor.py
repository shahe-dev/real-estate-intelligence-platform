#!/usr/bin/env python
"""
Property Monitor Management Script

Usage:
    python scripts/run_property_monitor.py refresh    # Refresh data from BigQuery
    python scripts/run_property_monitor.py metrics    # Rebuild metrics only
    python scripts/run_property_monitor.py api        # Start API server
    python scripts/run_property_monitor.py status     # Check data status
    python scripts/run_property_monitor.py all        # Full refresh + metrics + start API
"""

import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


def refresh_data(limit=None, year=None):
    """Refresh data from BigQuery"""
    print("\n" + "=" * 60)
    print("REFRESHING DATA FROM BIGQUERY")
    print("=" * 60)

    from src.etl.bigquery_loader import run_import
    run_import(limit=limit, year_filter=year)


def rebuild_metrics():
    """Rebuild all metrics"""
    print("\n" + "=" * 60)
    print("REBUILDING METRICS")
    print("=" * 60)

    from src.metrics.pm_calculator import rebuild_pm_metrics
    rebuild_pm_metrics()


def start_api(port=8001):
    """Start API server"""
    from src.api.pm_api import start_server
    start_server(port=port)


def check_status():
    """Check current data status"""
    print("\n" + "=" * 60)
    print("PROPERTY MONITOR DATA STATUS")
    print("=" * 60)

    try:
        import duckdb
        from config.bigquery_settings import bq_settings

        con = duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=True)

        # Version info
        print("\nData Versions:")
        versions = con.execute("""
            SELECT version_id, version_name, import_date, record_count, valid_records, is_active
            FROM data_versions
            ORDER BY version_id DESC
            LIMIT 5
        """).df()
        print(versions.to_string(index=False))

        # Current data stats
        print("\nCurrent Data Statistics:")
        stats = con.execute("""
            SELECT
                COUNT(*) as total_transactions,
                COUNT(DISTINCT area_name_en) as unique_areas,
                COUNT(DISTINCT project_name_en) as unique_projects,
                MIN(instance_date) as earliest_date,
                MAX(instance_date) as latest_date,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
                SUM(CASE WHEN reg_type_en = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
                AVG(actual_worth) as avg_price
            FROM transactions_clean
        """).fetchone()

        print(f"  Total Transactions: {stats[0]:,}")
        print(f"  Unique Areas: {stats[1]:,}")
        print(f"  Unique Projects: {stats[2]:,}")
        print(f"  Date Range: {stats[3]} to {stats[4]}")
        print(f"  Luxury Properties: {stats[5]:,} ({stats[5]/stats[0]*100:.1f}%)")
        print(f"  Off-Plan Sales: {stats[6]:,} ({stats[6]/stats[0]*100:.1f}%)")
        print(f"  Average Price: AED {stats[7]:,.0f}")

        # Top areas
        print("\nTop 10 Areas by Transactions:")
        top_areas = con.execute("""
            SELECT area_name_en, total_transactions, avg_price
            FROM metrics_area
            ORDER BY total_transactions DESC
            LIMIT 10
        """).df()
        print(top_areas.to_string(index=False))

        con.close()
        print("\n" + "=" * 60)

    except Exception as e:
        print(f"\nError: {e}")
        print("Run 'python run_property_monitor.py refresh' to load data first.")


def main():
    parser = argparse.ArgumentParser(
        description='Property Monitor Management',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  refresh    Refresh data from BigQuery
  metrics    Rebuild metrics tables
  api        Start API server (default port 8001)
  status     Check current data status
  all        Full refresh + metrics + start API

Examples:
  python run_property_monitor.py status
  python run_property_monitor.py refresh
  python run_property_monitor.py refresh --limit 1000
  python run_property_monitor.py api --port 8001
  python run_property_monitor.py all
        """
    )

    parser.add_argument('command', choices=['refresh', 'metrics', 'api', 'status', 'all'],
                        help='Command to run')
    parser.add_argument('--limit', type=int, help='Limit rows for refresh (testing)')
    parser.add_argument('--year', type=int, help='Filter by year for refresh')
    parser.add_argument('--port', type=int, default=8001, help='API port (default: 8001)')

    args = parser.parse_args()

    if args.command == 'refresh':
        refresh_data(limit=args.limit, year=args.year)

    elif args.command == 'metrics':
        rebuild_metrics()

    elif args.command == 'api':
        start_api(port=args.port)

    elif args.command == 'status':
        check_status()

    elif args.command == 'all':
        refresh_data(limit=args.limit, year=args.year)
        rebuild_metrics()
        start_api(port=args.port)


if __name__ == "__main__":
    main()
