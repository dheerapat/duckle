import duckdb
import requests
from connectors.base import BaseConnector
from utils.sql_safety import safe_identifier
from typing import Optional


class APIConnector(BaseConnector):
    """Extract data from a REST API endpoint that returns JSON."""

    def __init__(self, url: str, headers: Optional[dict] = None, json_path: Optional[str] = None):
        self.url = url
        self.headers = headers or {}
        self.json_path = json_path  # e.g. "data.results" to drill into nested JSON

    def _resolve_json_path(self, data: dict | list) -> list:
        """Drill into nested JSON and return a list of records."""
        if not self.json_path:
            return data if isinstance(data, list) else [data]
        for key in self.json_path.split("."):
            if not isinstance(data, dict) or key not in data:
                raise KeyError(
                    f"json_path '{self.json_path}' not found in API response "
                    f"(stopped at key '{key}')"
                )
            data = data[key]
        return data if isinstance(data, list) else [data]

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        response = requests.get(self.url, headers=self.headers)
        response.raise_for_status()
        data = response.json()

        records = self._resolve_json_path(data)
        table_name = safe_identifier(table_name, label="table_name")

        # Register as DuckDB table via Python objects (parameterised)
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM unnest(?)",
            [records],
        )
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = result[0] if result is not None else 0
        print(f"[APIConnector] Loaded {count} rows from {self.url} → {table_name}")

    def test_connection(self) -> bool:
        try:
            r = requests.get(self.url, headers=self.headers, timeout=5)
            return r.status_code == 200
        except Exception:
            return False
