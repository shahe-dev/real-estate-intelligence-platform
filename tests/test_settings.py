import sys
print("Starting test...")

try:
    print("Step 1: Importing os and pathlib...")
    import os
    from pathlib import Path
    print("✅ Success")
    
    print("Step 2: Importing dotenv...")
    from dotenv import load_dotenv
    print("✅ Success")
    
    print("Step 3: Loading .env...")
    load_dotenv()
    print("✅ Success")
    
    print("Step 4: Creating Settings class...")
    class Settings:
        BASE_DIR = Path(__file__).parent.parent
        DATA_DIR = BASE_DIR / "data"
        DEFAULT_CSV_FILE = str(DATA_DIR / "raw" / "Transactions.csv")
    print("✅ Success")
    
    print("Step 5: Creating settings instance...")
    settings = Settings()
    print("✅ Success")
    
    print(f"\nSettings object created! CSV: {settings.DEFAULT_CSV_FILE}")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()