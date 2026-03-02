#!/usr/bin/env python3
"""
Rebuild Supply-Demand Correlation Metrics
Executes metrics_correlation.sql to refresh all supply-demand analysis tables
"""

import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = str(PROJECT_ROOT / "data" / "database" / "property_monitor.db")
SQL_PATH = str(PROJECT_ROOT / "data" / "pm-projects-supply" / "sql" / "metrics_correlation.sql")

def rebuild_metrics():
    """Rebuild all supply-demand correlation metrics"""
    print("=" * 60)
    print("REBUILDING SUPPLY-DEMAND CORRELATION METRICS")
    print("=" * 60)

    # Connect to database
    print(f"\n[1/3] Connecting to database: {DB_PATH}")
    con = duckdb.connect(DB_PATH)

    # Read SQL script
    print(f"[2/3] Reading SQL script: {SQL_PATH}")
    with open(SQL_PATH, 'r') as f:
        sql = f.read()

    # Execute SQL
    print("[3/3] Executing SQL (creating metrics tables)...")
    result = con.execute(sql).fetchall()

    # Show verification results
    if result:
        status, areas, devs, opps, oversupplied, undersupplied, reliable = result[0]
        print("\n" + "=" * 60)
        print("METRICS REBUILD COMPLETE")
        print("=" * 60)
        print(f"\nStatus: {status}")
        print(f"\nTables Created:")
        print(f"  - metrics_supply_demand_area: {areas} areas")
        print(f"  - metrics_developer_performance: {devs} developers")
        print(f"  - metrics_market_opportunities: {opps} opportunities")
        print(f"\nMarket Balance:")
        print(f"  - Oversupplied areas: {oversupplied}")
        print(f"  - Undersupplied areas: {undersupplied}")
        print(f"\nDevelopers:")
        print(f"  - Reliable developers: {reliable}")

    # Sample check: Verify transaction counts for key areas
    print("\n" + "=" * 60)
    print("SAMPLE DATA VALIDATION")
    print("=" * 60)

    sample = con.execute("""
        SELECT
            area,
            demand_offplan_tx,
            supply_demand_ratio,
            market_balance
        FROM metrics_supply_demand_area
        WHERE area IN ('Business Bay', 'Dubai Marina', 'Jumeirah Village Circle')
        ORDER BY area
    """).df()

    print("\nKey Areas:")
    for _, row in sample.iterrows():
        print(f"\n{row['area']}:")
        print(f"  - Offplan Transactions (2024+2025): {row['demand_offplan_tx']}")
        print(f"  - Supply-Demand Ratio: {row['supply_demand_ratio']:.2f}" if row['supply_demand_ratio'] else "  - Supply-Demand Ratio: N/A")
        print(f"  - Market Balance: {row['market_balance']}")

    con.close()
    print("\n" + "=" * 60)
    print("[SUCCESS] Metrics rebuild complete!")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    rebuild_metrics()
