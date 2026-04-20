import duckdb
import requests
from connectors.base import BaseConnector


class APIConnector(BaseConnector):
    """Extract data from a REST API endpoint that returns JSON."""

    def __init__(self, url: str, headers: dict = None, json_path: str = None):
        self.url = url
        self.headers = headers or {}
        self.json_path = json_path  # e.g. "data.results" to drill into nested JSON

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        response = requests.get(self.url, headers=self.headers)
        response.raise_for_status()
        data = response.json()

        # Drill into nested path if specified
        if self.json_path:
            for key in self.json_path.split("."):
                data = data[key]

        # Register as DuckDB table via Python objects
        conn.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM (SELECT unnest(?::JSON[]))
        """,
            [data],
        )
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[APIConnector] Loaded {count} rows from {self.url} → {table_name}")

    def test_connection(self) -> bool:
        try:
            r = requests.get(self.url, headers=self.headers, timeout=5)
            return r.status_code == 200
        except Exception:
            return False
