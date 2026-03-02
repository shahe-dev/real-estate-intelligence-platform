# config/bigquery_settings.py

"""
BigQuery Configuration for Property Monitor Data
Separate from the main DLD data settings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Google Cloud credentials from environment (for Codespaces)
# These are used as an alternative to the service account JSON file
GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
GOOGLE_PRIVATE_KEY = os.getenv("GOOGLE_PRIVATE_KEY")
GOOGLE_CLIENT_EMAIL = os.getenv("GOOGLE_CLIENT_EMAIL")


class BigQuerySettings:
    """Settings for Property Monitor BigQuery data source"""

    # Project paths
    BASE_DIR = Path(__file__).parent.parent
    DATA_DIR = BASE_DIR / "data"
    DB_DIR = DATA_DIR / "database"

    # Property Monitor Database (separate from DLD)
    PM_DB_PATH = os.getenv("PM_DB_PATH", str(DB_DIR / "property_monitor.db"))

    # BigQuery Configuration
    # Set GOOGLE_SERVICE_ACCOUNT_FILE to your service account JSON path,
    # or use GOOGLE_PROJECT_ID / GOOGLE_PRIVATE_KEY / GOOGLE_CLIENT_EMAIL env vars.
    SERVICE_ACCOUNT_FILE = os.getenv(
        "GOOGLE_SERVICE_ACCOUNT_FILE",
        str(BASE_DIR / "service-account" / "credentials.json"),
    )
    BILLING_PROJECT = os.getenv("GOOGLE_PROJECT_ID", "your-gcp-project-id")
    PROPERTY_MONITOR_VIEW = os.getenv(
        "BQ_PROPERTY_MONITOR_VIEW",
        "your-project.property_monitor.property_monitor_sales_view",
    )

    # Business Rules for Property Monitor
    LUXURY_THRESHOLD = 5_000_000  # Luxury defined as 5M+ AED
    FOCUS_YEARS = [2023, 2024, 2025]  # Property Monitor data range

    # Premium areas (for luxury classification)
    LUXURY_AREAS = [
        'Palm Jumeirah',
        'Dubai Marina',
        'Downtown Dubai',
        'Business Bay',
        'Dubai Hills Estate',
        'Dubai Creek Harbour',
        'Mohammed Bin Rashid City',
        'Jumeirah Beach Residence',
        'Bluewaters Island',
        'Emirates Hills',
        'Al Barari',
        'Jumeirah Bay Island',
    ]

    @classmethod
    def ensure_dirs(cls):
        """Create necessary directories"""
        cls.DB_DIR.mkdir(parents=True, exist_ok=True)


bq_settings = BigQuerySettings()
bq_settings.ensure_dirs()
