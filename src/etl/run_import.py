# src/etl/run_import.py

"""
RUN THIS FIRST to load your data
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.etl.loader import DataLoader
from config.settings import settings

def main():
    print("=" * 60)
    print("DUBAI REAL ESTATE DATA IMPORTER")
    print("=" * 60)
    
    csv_file = settings.DEFAULT_CSV_FILE
    
    print(f"\nLooking for CSV at: {csv_file}")
    
    if not Path(csv_file).exists():
        print(f"\n[ERROR] ERROR: CSV file not found!")
        print(f"   Expected location: {csv_file}")
        print(f"\n   Please copy your Transactions.csv to: {Path(csv_file).parent}")
        return

    print(f"[OK] CSV file found!")
    
    loader = DataLoader()
    
    try:
        version_id = loader.load_new_version(csv_file)
        print("\n" + "=" * 60)
        print(f"SUCCESS! Data loaded as version {version_id}")
        print("=" * 60)
        
        # Show quick stats
        print("\n📊 Quick Stats:")
        stats = loader.con.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT area_name_en) as areas,
                MIN(transaction_year) as oldest_year,
                MAX(transaction_year) as newest_year
            FROM transactions_clean
        """).fetchone()
        
        print(f"   Total transactions: {stats[0]:,}")
        print(f"   Unique areas: {stats[1]:,}")
        print(f"   Date range: {stats[2]} - {stats[3]}")

    except Exception as e:
        print(f"\n[ERROR] ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        loader.close()

if __name__ == "__main__":
    main()