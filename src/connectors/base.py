from abc import ABC, abstractmethod
import duckdb

from utils.sql_safety import safe_identifier


class BaseConnector(ABC):
    """All source connectors implement this interface."""

    @abstractmethod
    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """Extract data from source and register it as a DuckDB table."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Verify the source is reachable."""
        pass

    def extract_incremental(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        cursor_column: str,
        since_value: str,
    ) -> None:
        """Default fallback: extract full, then filter in DuckDB."""
        self.extract(conn, table_name)
        safe = safe_identifier(cursor_column, label="cursor_column")
        safe_tbl = safe_identifier(table_name, label="table_name")
        filtered = f"{safe_tbl}_filtered"
        conn.execute(
            f"CREATE OR REPLACE TABLE {filtered} AS "
            f"SELECT * FROM {safe_tbl} WHERE {safe} > ?",
            [since_value],
        )
        conn.execute(f"DROP TABLE {safe_tbl}")
        conn.execute(f"ALTER TABLE {filtered} RENAME TO {safe_tbl}")
