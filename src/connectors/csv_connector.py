import os

import duckdb
from connectors.base import BaseConnector
from utils.sql_safety import safe_identifier, safe_path


class CSVConnector(BaseConnector):
    """Extract data from a CSV file."""

    def __init__(self, path: str):
        self.path = path

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        table_name = safe_identifier(table_name, label="table_name")
        safe_path(self.path)  # raises on dangerous chars
        # Use parameterised query for the file path (?)
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?)",
            [self.path],
        )
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = result[0] if result is not None else 0
        print(f"[CSVConnector] Loaded {count} rows from {self.path} → {table_name}")

    def test_connection(self) -> bool:
        return os.path.exists(self.path)
