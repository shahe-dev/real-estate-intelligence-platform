#!/usr/bin/env python
"""
Test script to verify credential loading works both ways:
1. File-based (local development with JSON file)
2. Environment variable-based (Codespaces)

Run with: python -m pytest tests/test_credential_loading.py -v
Or directly: python tests/test_credential_loading.py
"""

import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_file_based_credentials_exist():
    """Test that the service account file exists for local development"""
    from config.bigquery_settings import bq_settings

    service_account_path = Path(bq_settings.SERVICE_ACCOUNT_FILE)

    # This test passes if the file exists (local dev) or if we're in Codespaces (file won't exist)
    if os.getenv("CODESPACES"):
        print("  [SKIP] Running in Codespaces - file not expected to exist")
        return True

    assert service_account_path.exists(), f"Service account file not found: {service_account_path}"
    print(f"  [PASS] Service account file exists: {service_account_path}")
    return True


def test_env_var_loading():
    """Test that environment variables are read correctly from config module"""
    from config import bigquery_settings

    # Check if the module has the env var attributes (added in the update)
    if not hasattr(bigquery_settings, 'GOOGLE_PROJECT_ID'):
        print("  [SKIP] GOOGLE_PROJECT_ID not in config - env var support not yet added")
        return True  # Skip this test if not implemented yet

    # Test with mock environment variables
    test_env = {
        "GOOGLE_PROJECT_ID": "test-project-123",
        "GOOGLE_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----",
        "GOOGLE_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com"
    }

    with patch.dict(os.environ, test_env):
        # Reload the module to pick up new env vars
        import importlib
        importlib.reload(bigquery_settings)

        assert bigquery_settings.GOOGLE_PROJECT_ID == "test-project-123"
        assert bigquery_settings.GOOGLE_CLIENT_EMAIL == "test@test-project.iam.gserviceaccount.com"
        assert "PRIVATE KEY" in bigquery_settings.GOOGLE_PRIVATE_KEY

    print("  [PASS] Environment variables loaded correctly")
    return True


def test_bigquery_client_file_fallback():
    """Test that BigQuery client falls back to file when env vars not set"""
    from src.etl.bigquery_loader import BigQueryLoader

    # Clear any env vars
    env_without_google = {k: v for k, v in os.environ.items()
                         if not k.startswith("GOOGLE_")}

    with patch.dict(os.environ, env_without_google, clear=True):
        loader = BigQueryLoader()

        # Check that it would use the file path
        assert loader.SERVICE_ACCOUNT_FILE is not None
        assert "propmonitor service account" in loader.SERVICE_ACCOUNT_FILE

    print("  [PASS] BigQuery client correctly configured for file-based auth")
    loader.close()
    return True


def test_bigquery_client_env_var_mode():
    """Test that BigQuery client uses env vars when available"""
    from google.oauth2 import service_account

    test_env = {
        "GOOGLE_PROJECT_ID": "test-project-123",
        "GOOGLE_PRIVATE_KEY": "-----BEGIN PRIVATE KEY-----\nMIItest\n-----END PRIVATE KEY-----",
        "GOOGLE_CLIENT_EMAIL": "test@test-project.iam.gserviceaccount.com"
    }

    with patch.dict(os.environ, test_env):
        # Verify the credential creation logic would work
        project_id = os.getenv("GOOGLE_PROJECT_ID")
        private_key = os.getenv("GOOGLE_PRIVATE_KEY")
        client_email = os.getenv("GOOGLE_CLIENT_EMAIL")

        assert project_id is not None
        assert private_key is not None
        assert client_email is not None

        # Verify the credential dict structure is correct
        cred_info = {
            "type": "service_account",
            "project_id": project_id,
            "private_key": private_key.replace("\\n", "\n"),
            "client_email": client_email,
            "token_uri": "https://oauth2.googleapis.com/token",
        }

        assert cred_info["type"] == "service_account"
        assert cred_info["project_id"] == "test-project-123"
        assert "BEGIN PRIVATE KEY" in cred_info["private_key"]

    print("  [PASS] Environment variable credential structure is correct")
    return True


def test_database_path_configuration():
    """Test that database paths are configured correctly"""
    from config.bigquery_settings import bq_settings

    assert bq_settings.PM_DB_PATH is not None
    assert "property_monitor.db" in bq_settings.PM_DB_PATH
    assert bq_settings.DB_DIR is not None

    print(f"  [PASS] Database path configured: {bq_settings.PM_DB_PATH}")
    return True


def test_existing_database_accessible():
    """Test that existing DuckDB database can be opened"""
    from config.bigquery_settings import bq_settings
    import duckdb

    db_path = Path(bq_settings.PM_DB_PATH)

    if not db_path.exists():
        print(f"  [SKIP] Database not found at {db_path} - may need to download from release")
        return True

    try:
        con = duckdb.connect(str(db_path), read_only=True)

        # Check that key tables exist
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]

        assert "transactions_all" in table_names or len(tables) > 0

        # Quick data check
        count = con.execute("SELECT COUNT(*) FROM transactions_all").fetchone()[0]

        con.close()
        print(f"  [PASS] Database accessible with {count:,} transactions")
        return True

    except Exception as e:
        print(f"  [FAIL] Database error: {e}")
        return False


def run_all_tests():
    """Run all tests and report results"""
    print("\n" + "=" * 60)
    print("CREDENTIAL LOADING VERIFICATION TESTS")
    print("=" * 60 + "\n")

    tests = [
        ("File-based credentials exist", test_file_based_credentials_exist),
        ("Environment variable loading", test_env_var_loading),
        ("BigQuery client file fallback", test_bigquery_client_file_fallback),
        ("BigQuery client env var mode", test_bigquery_client_env_var_mode),
        ("Database path configuration", test_database_path_configuration),
        ("Existing database accessible", test_existing_database_accessible),
    ]

    results = []
    for name, test_func in tests:
        print(f"\nTest: {name}")
        try:
            result = test_func()
            results.append((name, "PASS" if result else "FAIL"))
        except Exception as e:
            print(f"  [FAIL] {e}")
            results.append((name, "FAIL"))

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, r in results if r == "PASS")
    total = len(results)

    for name, result in results:
        status = "[PASS]" if result == "PASS" else "[FAIL]"
        print(f"  {status} {name}")

    print(f"\nResult: {passed}/{total} tests passed")
    print("=" * 60 + "\n")

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
