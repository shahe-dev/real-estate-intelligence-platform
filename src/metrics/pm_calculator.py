# src/metrics/pm_calculator.py

"""
Property Monitor Metrics Calculator
Pre-computed analytics from Property Monitor BigQuery data
Uses separate database: data/database/property_monitor.db
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from config.bigquery_settings import bq_settings


class PMMetricsCalculator:
    """Calculate pre-computed metrics for Property Monitor data"""

    def __init__(self, connection=None):
        if connection:
            self.con = connection
            self._owns_connection = False
        else:
            self.con = duckdb.connect(str(bq_settings.PM_DB_PATH), read_only=False)
            self._owns_connection = True

    def rebuild_all_metrics(self):
        """Rebuild all metric tables"""
        print("Building Property Monitor Metrics...")

        self._build_area_metrics()
        self._build_monthly_trends()
        self._build_property_type_metrics()
        self._build_luxury_metrics()
        self._build_project_metrics()
        self._build_developer_metrics()
        self._build_offplan_metrics()

        print("All Property Monitor metrics rebuilt!")

    def _build_area_metrics(self):
        """Area-level metrics"""
        print("   Building area metrics...")

        self.con.execute("DROP TABLE IF EXISTS metrics_area")
        self.con.execute("""
            CREATE TABLE metrics_area AS
            SELECT
                area_name_en,
                COUNT(*) as total_transactions,
                COUNT(DISTINCT property_type) as property_types,
                COUNT(DISTINCT project_name_en) as unique_projects,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                MIN(actual_worth) as min_price,
                MAX(actual_worth) as max_price,
                AVG(meter_sale_price) as avg_price_sqm,
                AVG(procedure_area) as avg_size_sqm,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
                SUM(CASE WHEN transaction_type = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
                MAX(transaction_year) as last_transaction_year,
                STRING_AGG(DISTINCT developer, ', ') as top_developers
            FROM transactions_verified
            GROUP BY area_name_en
            HAVING COUNT(*) >= 10
            ORDER BY total_transactions DESC
        """)

    def _build_monthly_trends(self):
        """Monthly price trends by area"""
        print("   Building monthly trends...")

        self.con.execute("DROP TABLE IF EXISTS metrics_monthly_trends")
        self.con.execute("""
            CREATE TABLE metrics_monthly_trends AS
            SELECT
                area_name_en,
                transaction_year,
                transaction_month,
                property_type,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                AVG(meter_sale_price) as avg_price_sqm,
                SUM(CASE WHEN transaction_type = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
                STRING_AGG(transaction_id, ',' ORDER BY actual_worth DESC) as sample_tx_ids
            FROM transactions_verified
            GROUP BY area_name_en, transaction_year, transaction_month, property_type
            HAVING COUNT(*) >= 3
        """)

        # Calculate month-over-month changes
        self.con.execute("DROP TABLE IF EXISTS metrics_price_changes")
        self.con.execute("""
            CREATE TABLE metrics_price_changes AS
            SELECT
                area_name_en,
                transaction_year,
                transaction_month,
                property_type,
                avg_price,
                tx_count,
                LAG(avg_price) OVER (
                    PARTITION BY area_name_en, property_type
                    ORDER BY transaction_year, transaction_month
                ) as prev_month_price,
                LAG(tx_count) OVER (
                    PARTITION BY area_name_en, property_type
                    ORDER BY transaction_year, transaction_month
                ) as prev_month_tx_count,
                ((avg_price - LAG(avg_price) OVER (
                    PARTITION BY area_name_en, property_type
                    ORDER BY transaction_year, transaction_month
                )) / NULLIF(LAG(avg_price) OVER (
                    PARTITION BY area_name_en, property_type
                    ORDER BY transaction_year, transaction_month
                ), 0) * 100) as pct_change_mom
            FROM metrics_monthly_trends
        """)

        # Year-over-year comparison
        self.con.execute("DROP TABLE IF EXISTS metrics_yoy_comparison")
        self.con.execute("""
            CREATE TABLE metrics_yoy_comparison AS
            SELECT
                area_name_en,
                transaction_year,
                property_type,
                SUM(tx_count) as total_transactions,
                AVG(avg_price) as avg_price,
                AVG(avg_price_sqm) as avg_price_sqm,
                LAG(SUM(tx_count)) OVER (
                    PARTITION BY area_name_en, property_type
                    ORDER BY transaction_year
                ) as prev_year_transactions,
                LAG(AVG(avg_price)) OVER (
                    PARTITION BY area_name_en, property_type
                    ORDER BY transaction_year
                ) as prev_year_price
            FROM metrics_monthly_trends
            GROUP BY area_name_en, transaction_year, property_type
        """)

    def _build_property_type_metrics(self):
        """Property type analysis (Studio vs 1BR vs 2BR etc)"""
        print("   Building property type metrics...")

        self.con.execute("DROP TABLE IF EXISTS metrics_property_types")
        self.con.execute("""
            CREATE TABLE metrics_property_types AS
            SELECT
                area_name_en,
                property_type,
                rooms_en,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                AVG(meter_sale_price) as avg_price_sqm,
                AVG(procedure_area) as avg_size_sqm,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY actual_worth) as price_q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY actual_worth) as price_q3,
                MIN(actual_worth) as min_price,
                MAX(actual_worth) as max_price
            FROM transactions_verified
            GROUP BY area_name_en, property_type, rooms_en
            HAVING COUNT(*) >= 5
        """)

    def _build_luxury_metrics(self):
        """Luxury market metrics (5M+ AED)"""
        print("   Building luxury metrics...")

        self.con.execute("DROP TABLE IF EXISTS metrics_luxury")
        self.con.execute("""
            CREATE TABLE metrics_luxury AS
            SELECT
                area_name_en,
                transaction_year,
                transaction_month,
                COUNT(*) as luxury_tx_count,
                AVG(actual_worth) as avg_luxury_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_luxury_price,
                MIN(actual_worth) as min_luxury_price,
                MAX(actual_worth) as max_luxury_price,
                STRING_AGG(DISTINCT project_name_en, ', ') as luxury_projects,
                STRING_AGG(DISTINCT developer, ', ') as luxury_developers
            FROM transactions_verified_luxury
            GROUP BY area_name_en, transaction_year, transaction_month
            HAVING COUNT(*) >= 3
        """)

        # Luxury summary by area
        self.con.execute("DROP TABLE IF EXISTS metrics_luxury_summary")
        self.con.execute("""
            CREATE TABLE metrics_luxury_summary AS
            SELECT
                area_name_en,
                COUNT(*) as total_luxury_transactions,
                AVG(actual_worth) as avg_luxury_price,
                MAX(actual_worth) as highest_sale,
                AVG(meter_sale_price) as avg_price_sqm,
                COUNT(DISTINCT project_name_en) as luxury_projects_count,
                COUNT(DISTINCT developer) as luxury_developers_count
            FROM transactions_verified_luxury
            GROUP BY area_name_en
            HAVING COUNT(*) >= 5
            ORDER BY total_luxury_transactions DESC
        """)

    def _build_project_metrics(self):
        """Project-level metrics"""
        print("   Building project metrics...")

        self.con.execute("DROP TABLE IF EXISTS metrics_projects")
        self.con.execute("""
            CREATE TABLE metrics_projects AS
            SELECT
                project_name_en,
                developer,
                area_name_en,
                transaction_type,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                AVG(meter_sale_price) as avg_price_sqm,
                MIN(instance_date) as first_sale_date,
                MAX(instance_date) as last_sale_date,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_units,
                STRING_AGG(DISTINCT rooms_en, ', ') as unit_types_available
            FROM transactions_verified
            WHERE project_name_en IS NOT NULL
              AND project_name_en != ''
            GROUP BY project_name_en, developer, area_name_en, transaction_type
            HAVING COUNT(*) >= 5
            ORDER BY tx_count DESC
        """)

    def _build_developer_metrics(self):
        """Developer-level metrics"""
        print("   Building developer metrics...")

        self.con.execute("DROP TABLE IF EXISTS metrics_developers")
        self.con.execute("""
            CREATE TABLE metrics_developers AS
            SELECT
                developer,
                COUNT(*) as total_transactions,
                COUNT(DISTINCT area_name_en) as areas_active,
                COUNT(DISTINCT project_name_en) as projects_count,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                SUM(actual_worth) as total_sales_volume,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_units,
                SUM(CASE WHEN transaction_type = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_sales,
                MIN(instance_date) as first_sale_date,
                MAX(instance_date) as last_sale_date
            FROM transactions_verified
            WHERE developer IS NOT NULL
              AND developer != ''
            GROUP BY developer
            HAVING COUNT(*) >= 10
            ORDER BY total_transactions DESC
        """)

    def _build_offplan_metrics(self):
        """Off-plan vs Ready market metrics"""
        print("   Building off-plan metrics...")

        self.con.execute("DROP TABLE IF EXISTS metrics_offplan")
        self.con.execute("""
            CREATE TABLE metrics_offplan AS
            SELECT
                area_name_en,
                transaction_year,
                transaction_month,
                transaction_type,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                AVG(meter_sale_price) as avg_price_sqm
            FROM transactions_verified
            GROUP BY area_name_en, transaction_year, transaction_month, transaction_type
            HAVING COUNT(*) >= 3
        """)

        # Off-plan vs Ready comparison by area
        self.con.execute("DROP TABLE IF EXISTS metrics_offplan_comparison")
        self.con.execute("""
            CREATE TABLE metrics_offplan_comparison AS
            SELECT
                area_name_en,
                SUM(CASE WHEN transaction_type = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count,
                SUM(CASE WHEN transaction_type = 'Existing' THEN 1 ELSE 0 END) as ready_count,
                AVG(CASE WHEN transaction_type = 'Off-Plan' THEN actual_worth END) as avg_offplan_price,
                AVG(CASE WHEN transaction_type = 'Existing' THEN actual_worth END) as avg_ready_price,
                (SUM(CASE WHEN transaction_type = 'Off-Plan' THEN 1 ELSE 0 END) * 100.0 /
                 NULLIF(COUNT(*), 0)) as offplan_percentage
            FROM transactions_verified
            GROUP BY area_name_en
            HAVING COUNT(*) >= 20
            ORDER BY offplan_percentage DESC
        """)

    def get_market_overview(self):
        """Get overall market statistics"""
        result = self.con.execute("""
            SELECT
                COUNT(*) as total_transactions,
                COUNT(DISTINCT area_name_en) as unique_areas,
                COUNT(DISTINCT project_name_en) as unique_projects,
                COUNT(DISTINCT developer) as unique_developers,
                MIN(instance_date) as earliest_date,
                MAX(instance_date) as latest_date,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                SUM(actual_worth) as total_market_volume,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
                SUM(CASE WHEN transaction_type = 'Off-Plan' THEN 1 ELSE 0 END) as offplan_count
            FROM transactions_verified
        """).fetchone()

        return {
            'total_transactions': result[0],
            'unique_areas': result[1],
            'unique_projects': result[2],
            'unique_developers': result[3],
            'earliest_date': str(result[4]),
            'latest_date': str(result[5]),
            'avg_price': result[6],
            'median_price': result[7],
            'total_market_volume': result[8],
            'luxury_count': result[9],
            'offplan_count': result[10],
        }

    def close(self):
        """Close connection if we own it"""
        if self._owns_connection:
            self.con.close()


def rebuild_pm_metrics():
    """Standalone function to rebuild Property Monitor metrics"""
    calc = PMMetricsCalculator()
    try:
        calc.rebuild_all_metrics()

        print("\nMarket Overview:")
        overview = calc.get_market_overview()
        for k, v in overview.items():
            if isinstance(v, float):
                print(f"  {k}: {v:,.2f}")
            elif isinstance(v, int):
                print(f"  {k}: {v:,}")
            else:
                print(f"  {k}: {v}")

        print("\nProperty Monitor metrics rebuild complete!")
    finally:
        calc.close()


if __name__ == "__main__":
    rebuild_pm_metrics()
