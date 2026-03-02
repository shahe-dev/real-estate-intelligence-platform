# src/etl/loader.py

import duckdb
from datetime import datetime
from pathlib import Path
import sys

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from config.settings import settings
from config.validation_rules import ValidationRules

class DataLoader:
    """Load and validate data with versioning"""
    
    def __init__(self, db_path=None):
        self.db_path = db_path or settings.DB_PATH
        # Ensure database directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self.rules = ValidationRules()
        self._init_schema()
    
    def _init_schema(self):
        """Initialize database schema"""
        
        # Versions table
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
        
        # Main transactions table
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
    
    def load_new_version(self, csv_file):
        """Load new data version"""
        print(f"[INFO] Loading data from: {csv_file}")

        # Check if file exists
        if not Path(csv_file).exists():
            raise FileNotFoundError(f"[ERROR] CSV file not found: {csv_file}")
        
        # Get next version ID
        version_id = self._get_next_version_id()
        version_name = f"v{version_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"   Version: {version_name}")
        
        # Load to temp table
        print(f"   Reading CSV...")
        self.con.execute(f"""
            CREATE TEMP TABLE temp_import AS
            SELECT * FROM read_csv_auto('{csv_file}',
                nullstr='null',
                header=True,
                ignore_errors=false
            )
        """)
        
        record_count = self.con.execute("SELECT COUNT(*) FROM temp_import").fetchone()[0]
        print(f"   Records loaded: {record_count:,}")
        
        # Validate and enrich
        print("   Validating...")
        self._validate_data('temp_import', version_id)
        
        # Insert into main table
        print("   Inserting into database...")
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
                {version_id},
                CURRENT_TIMESTAMP,
                quality_score,
                is_valid,
                is_luxury,
                transaction_year,
                transaction_month
            FROM temp_validated
        """)
        
        # Get stats
        stats = self.con.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid
            FROM temp_validated
        """).fetchone()
        
        # Save version metadata
        self.con.execute(f"""
            INSERT INTO data_versions VALUES (
                {version_id},
                '{version_name}',
                CURRENT_TIMESTAMP,
                '{csv_file}',
                {stats[0]},
                {stats[1]},
                TRUE
            )
        """)
        
        # Deactivate old versions
        self.con.execute(f"""
            UPDATE data_versions 
            SET is_active = FALSE 
            WHERE version_id < {version_id}
        """)
        
        print(f"[SUCCESS] Version {version_id} loaded")
        print(f"   Valid: {stats[1]:,} ({stats[1]/stats[0]*100:.1f}%)")
        print(f"   Invalid: {stats[0]-stats[1]:,}")

        return version_id
    
    def _validate_data(self, temp_table, version_id):
        """Validate and enrich data"""
        
        self.con.execute(f"""
            CREATE TEMP TABLE temp_validated AS
            SELECT 
                *,
                TRY_CAST(YEAR(TRY_CAST(instance_date AS DATE)) AS INTEGER) as transaction_year,
                TRY_CAST(MONTH(TRY_CAST(instance_date AS DATE)) AS INTEGER) as transaction_month,
                1.0 as quality_score,
                TRUE as is_valid,
                FALSE as is_luxury
            FROM {temp_table}
        """)
        
        # Validate prices
        for prop_type, rules in self.rules.PRICE_RULES['sales'].items():
            self.con.execute(f"""
                UPDATE temp_validated
                SET is_valid = FALSE, quality_score = 0.0
                WHERE property_type_en = '{prop_type}'
                  AND trans_group_en = 'Sales'
                  AND (actual_worth < {rules['min']} OR actual_worth > {rules['max']})
            """)
        
        # Mark luxury (5M+ AED threshold)
        self.con.execute("""
            UPDATE temp_validated
            SET is_luxury = TRUE
            WHERE trans_group_en = 'Sales'
              AND actual_worth >= 5000000
        """)
    
    def _get_next_version_id(self):
        """Get next version ID"""
        result = self.con.execute("""
            SELECT COALESCE(MAX(version_id), 0) + 1 FROM data_versions
        """).fetchone()
        return result[0]
    
    def close(self):
        self.con.close()