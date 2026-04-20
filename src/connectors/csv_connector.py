import os

import duckdb
from connectors.base import BaseConnector


class CSVConnector(BaseConnector):
    """Extract data from a CSV file."""

    def __init__(self, path: str):
        self.path = path

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{self.path}')
        """)
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[CSVConnector] Loaded {count} rows from {self.path} → {table_name}")

    def test_connection(self) -> bool:
        return os.path.exists(self.path)
