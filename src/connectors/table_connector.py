"""Connector that reads from an existing table in the same DuckDB connection.

Useful for wiring DuckLake layers (e.g. bronze → silver) into the
PipelineRunner without needing a separate external source.
"""

import duckdb
from typing import Optional
from connectors.base import BaseConnector
from utils.sql_safety import safe_identifier


def _resolve_table_ref(table_ref: str) -> str:
    """Validate and return a schema-qualified table reference.

    Accepts both plain names (``products``) and schema-qualified names
    (``bronze.products``).  Each component is validated via
    :func:`safe_identifier` to prevent injection.
    """
    parts = table_ref.split(".")
    if len(parts) == 1:
        return safe_identifier(parts[0], label="table name")
    if len(parts) == 2:
        safe_identifier(parts[0], label="schema name")
        safe_identifier(parts[1], label="table name")
        return f"{parts[0]}.{parts[1]}"
    raise ValueError(
        f"Invalid table reference: {table_ref!r}. "
        f"Expected 'name' or 'schema.name', got {len(parts)} parts."
    )


class TableConnector(BaseConnector):
    """Extract data from an existing DuckDB table in the same connection.

    Copies the source table into ``table_name`` via
    ``CREATE OR REPLACE TABLE … AS SELECT * FROM …``, making it
    compatible with PipelineRunner's extract step.
    """

    def __init__(self, source_table: str):
        self.source_table = source_table

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        table_name = safe_identifier(table_name, label="table_name")
        source = _resolve_table_ref(self.source_table)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {source}")
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = result[0] if result is not None else 0
        print(f"[TableConnector] Copied {count} rows from {source} → {table_name}")

    def extract_incremental(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        cursor_column: str,
        since_value: str,
        until_value: Optional[str] = None,
        from_is_explicit: bool = False,
    ) -> None:
        source = _resolve_table_ref(self.source_table)
        safe_col = safe_identifier(cursor_column, label="cursor_column")
        safe_tbl = safe_identifier(table_name, label="table_name")
        op = ">=" if from_is_explicit else ">"
        if until_value is not None:
            conn.execute(
                f"CREATE OR REPLACE TABLE {safe_tbl} AS "
                f"SELECT * FROM {source} WHERE {safe_col} {op} ? AND {safe_col} < ?",
                [since_value, until_value],
            )
        else:
            conn.execute(
                f"CREATE OR REPLACE TABLE {safe_tbl} AS "
                f"SELECT * FROM {source} WHERE {safe_col} {op} ?",
                [since_value],
            )
        result = conn.execute(f"SELECT COUNT(*) FROM {safe_tbl}").fetchone()
        count = result[0] if result is not None else 0
        print(
            f"[TableConnector] Incrementally loaded {count} rows from {source} → {safe_tbl}"
        )

    def test_connection(self) -> bool:
        # We can't test without a connection, so assume the table exists
        return True
