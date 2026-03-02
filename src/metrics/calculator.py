# src/metrics/calculator.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from config.settings import settings

class MetricsCalculator:
    """Calculate pre-computed metrics for fast content generation"""
    
    def __init__(self, connection):
        self.con = connection
    
    def rebuild_all_metrics(self):
        """Rebuild all metric tables"""
        print("[INFO] Rebuilding metrics...")

        self._build_area_metrics()
        self._build_monthly_trends()
        self._build_property_type_metrics()
        self._build_luxury_metrics()
        self._build_project_metrics()

        print("[SUCCESS] All metrics rebuilt")
    
    def _build_area_metrics(self):
        """Area-level metrics"""
        print("   Building area metrics...")
        
        self.con.execute("DROP TABLE IF EXISTS metrics_area")
        self.con.execute("""
            CREATE TABLE metrics_area AS
            SELECT 
                area_name_en,
                COUNT(*) as total_transactions,
                COUNT(DISTINCT property_type_en) as property_types,
                AVG(actual_worth) as avg_price,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_worth) as median_price,
                MIN(actual_worth) as min_price,
                MAX(actual_worth) as max_price,
                AVG(meter_sale_price) as avg_price_sqm,
                AVG(procedure_area) as avg_size_sqm,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_count,
                MAX(transaction_year) as last_transaction_year,
                STRING_AGG(DISTINCT nearest_metro_en, ', ') as nearby_metros,
                STRING_AGG(DISTINCT nearest_mall_en, ', ') as nearby_malls
            FROM transactions_clean
            WHERE trans_group_en = 'Sales'
              AND transaction_year >= 2020
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
                property_type_en,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                AVG(meter_sale_price) as avg_price_sqm,
                STRING_AGG(transaction_id, ',' ORDER BY actual_worth DESC) as sample_tx_ids
            FROM transactions_clean
            WHERE trans_group_en = 'Sales'
              AND transaction_year >= 2020
            GROUP BY area_name_en, transaction_year, transaction_month, property_type_en
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
                property_type_en,
                avg_price,
                LAG(avg_price) OVER (
                    PARTITION BY area_name_en, property_type_en 
                    ORDER BY transaction_year, transaction_month
                ) as prev_month_price,
                ((avg_price - LAG(avg_price) OVER (
                    PARTITION BY area_name_en, property_type_en 
                    ORDER BY transaction_year, transaction_month
                )) / LAG(avg_price) OVER (
                    PARTITION BY area_name_en, property_type_en 
                    ORDER BY transaction_year, transaction_month
                ) * 100) as pct_change_mom
            FROM metrics_monthly_trends
        """)
    
    def _build_property_type_metrics(self):
        """Property type analysis (Studio vs 1BR vs 2BR etc)"""
        print("   Building property type metrics...")
        
        self.con.execute("DROP TABLE IF EXISTS metrics_property_types")
        self.con.execute("""
            CREATE TABLE metrics_property_types AS
            SELECT 
                area_name_en,
                property_type_en,
                rooms_en,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                AVG(meter_sale_price) as avg_price_sqm,
                AVG(procedure_area) as avg_size_sqm,
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY actual_worth) as price_q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY actual_worth) as price_q3
            FROM transactions_clean
            WHERE trans_group_en = 'Sales'
              AND transaction_year >= 2020
              AND property_type_en = 'Unit'
            GROUP BY area_name_en, property_type_en, rooms_en
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
                COUNT(*) as luxury_tx_count,
                AVG(actual_worth) as avg_luxury_price,
                MIN(actual_worth) as min_luxury_price,
                MAX(actual_worth) as max_luxury_price,
                STRING_AGG(DISTINCT project_name_en, ', ') as luxury_projects
            FROM transactions_luxury
            WHERE transaction_year >= 2020
            GROUP BY area_name_en, transaction_year
            HAVING COUNT(*) >= 3
        """)
    
    def _build_project_metrics(self):
        """Developer/Project metrics"""
        print("   Building project metrics...")
        
        self.con.execute("DROP TABLE IF EXISTS metrics_projects")
        self.con.execute("""
            CREATE TABLE metrics_projects AS
            SELECT 
                project_name_en,
                master_project_en,
                area_name_en,
                reg_type_en,
                COUNT(*) as tx_count,
                AVG(actual_worth) as avg_price,
                MIN(transaction_year) as first_sale_year,
                MAX(transaction_year) as last_sale_year,
                SUM(CASE WHEN is_luxury THEN 1 ELSE 0 END) as luxury_units
            FROM transactions_clean
            WHERE trans_group_en = 'Sales'
              AND project_name_en IS NOT NULL
              AND project_name_en != 'Not Applicable'
              AND transaction_year >= 2020
            GROUP BY project_name_en, master_project_en, area_name_en, reg_type_en
            HAVING COUNT(*) >= 5
            ORDER BY tx_count DESC
        """)

def rebuild_metrics():
    """Standalone function to rebuild metrics"""
    from src.utils.db import get_db
    
    con = get_db()
    calc = MetricsCalculator(con)
    calc.rebuild_all_metrics()

    print("\n[SUCCESS] Metrics rebuild complete!")

if __name__ == "__main__":
    rebuild_metrics()