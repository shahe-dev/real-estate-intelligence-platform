# config/settings.py

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Project paths
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    RAW_DATA_DIR = DATA_DIR / "raw"
    DB_DIR = DATA_DIR / "database"
    CONTENT_DIR = DATA_DIR / "generated_content"
    
    # Database
    DB_PATH = os.getenv("DB_PATH", str(DB_DIR / "dubai_land.db"))
    
    # API
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", 8000))
    
    # Data Processing
    DEFAULT_CSV_FILE = str(RAW_DATA_DIR / "Transactions.csv")
    
    # Content Generation
    CONTENT_OUTPUT_DIR = Path(os.getenv("CONTENT_OUTPUT_DIR", str(CONTENT_DIR)))
    
    # Business Rules
    LUXURY_THRESHOLD = 5_000_000  # Luxury defined as 5M+ AED
    FOCUS_YEARS = [2020, 2021, 2022, 2023, 2024]
    CONTENT_FREQUENCY = "monthly"
    
    @classmethod
    def ensure_dirs(cls):
        """Create necessary directories"""
        for dir_path in [cls.DATA_DIR, cls.RAW_DATA_DIR, cls.DB_DIR, cls.CONTENT_DIR]:
            dir_path.mkdir(parents=True, exist_ok=True)

settings = Settings()
settings.ensure_dirs()