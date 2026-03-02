# src/utils/db.py

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import duckdb
from config.settings import settings

class DatabaseConnection:
    """Database connection manager"""

    _instance = None
    _read_only_instance = None

    def __new__(cls, read_only=False):
        if read_only:
            if cls._read_only_instance is None:
                instance = super().__new__(cls)
                # Read-only connection allows multiple processes
                instance.con = duckdb.connect(str(settings.DB_PATH), read_only=True)
                cls._read_only_instance = instance
            return cls._read_only_instance
        else:
            if cls._instance is None:
                instance = super().__new__(cls)
                instance.con = duckdb.connect(str(settings.DB_PATH))
                cls._instance = instance
            return cls._instance

    def get_connection(self):
        return self.con

    def close(self):
        if self.con:
            self.con.close()
            DatabaseConnection._instance = None

def get_db(read_only=False):
    """Get database connection

    Args:
        read_only: If True, opens database in read-only mode (allows multiple processes)
    """
    return DatabaseConnection(read_only=read_only).get_connection()