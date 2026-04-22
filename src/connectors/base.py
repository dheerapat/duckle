from abc import ABC, abstractmethod
from typing import Optional

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
        until_value: Optional[str] = None,
        from_is_explicit: bool = False,
    ) -> None:
        """Default fallback: extract full, then filter in DuckDB."""
        self.extract(conn, table_name)
        safe = safe_identifier(cursor_column, label="cursor_column")
        safe_tbl = safe_identifier(table_name, label="table_name")
        filtered = f"{safe_tbl}_filtered"
        op = ">=" if from_is_explicit else ">"
        if until_value is not None:
            conn.execute(
                f"CREATE OR REPLACE TABLE {filtered} AS "
                f"SELECT * FROM {safe_tbl} WHERE {safe} {op} ? AND {safe} < ?",
                [since_value, until_value],
            )
        else:
            conn.execute(
                f"CREATE OR REPLACE TABLE {filtered} AS "
                f"SELECT * FROM {safe_tbl} WHERE {safe} {op} ?",
                [since_value],
            )
        conn.execute(f"DROP TABLE {safe_tbl}")
        conn.execute(f"ALTER TABLE {filtered} RENAME TO {safe_tbl}")
