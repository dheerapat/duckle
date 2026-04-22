import re

import duckdb
from typing import Optional
from connectors.base import BaseConnector
from utils.sql_safety import safe_identifier, safe_path

# Stable alias used when ATTACH-ing the Postgres database into DuckDB.
_PG_ALIAS = "__duckle_pg_src"


class PostgresConnector(BaseConnector):
    """Extract data from a Postgres table.

    The DuckDB ``postgres`` extension requires an explicit ATTACH before
    ``postgres_query()`` can be used.  We ATTACH with a stable alias
    (``__duckle_pg_src``) so that ``postgres_query`` can reference it.
    """

    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query

    def _attach(self, conn: duckdb.DuckDBPyConnection) -> None:
        """ATTACH the Postgres database if not already attached."""
        conn.execute("INSTALL postgres; LOAD postgres;")
        result = conn.execute(
            "SELECT count(*) FROM duckdb_databases() WHERE database_name = ?",
            [_PG_ALIAS],
        ).fetchone()
        attached = result[0] if result is not None else 0
        if not attached:
            # DuckDB ATTACH does not support parameterised connection strings,
            # so we validate the string contains no risky characters first.
            safe_path(self.connection_string)  # rejects quotes, semicolons, etc.
            conn.execute(
                f"ATTACH '{self.connection_string}' AS {_PG_ALIAS} (TYPE postgres)"
            )

    def _detach(self, conn: duckdb.DuckDBPyConnection) -> None:
        """DETACH the Postgres database (clean up after extraction)."""
        try:
            conn.execute(f"DETACH {_PG_ALIAS}")
        except Exception:
            pass  # already detached or never attached

    def _validate_cursor_value(self, value: str) -> None:
        """Reject cursor values that could break out of a SQL string."""
        if re.search(r"['\";\\]", str(value)):
            raise ValueError(f"Unsafe cursor value: {value!r}")

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        self._attach(conn)
        table_name = safe_identifier(table_name, label="table_name")

        # Use the attached alias as the first argument to postgres_query,
        # and parameterise the query itself.
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM postgres_query('{_PG_ALIAS}', ?)",
            [self.query],
        )
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = result[0] if result is not None else 0
        print(f"[PostgresConnector] Loaded {count} rows → {table_name}")

        self._detach(conn)

    def extract_incremental(
        self,
        conn: duckdb.DuckDBPyConnection,
        table_name: str,
        cursor_column: str,
        since_value: str,
        until_value: Optional[str] = None,
        from_is_explicit: bool = False,
    ) -> None:
        self._attach(conn)
        self._validate_cursor_value(since_value)
        if until_value is not None:
            self._validate_cursor_value(until_value)
        safe_col = safe_identifier(cursor_column, label="cursor_column")
        safe_tbl = safe_identifier(table_name, label="table_name")

        op = ">=" if from_is_explicit else ">"
        if until_value is not None:
            filtered_query = (
                f"SELECT * FROM ({self.query}) AS _src "
                f"WHERE {safe_col} {op} '{since_value}' AND {safe_col} < '{until_value}'"
            )
        else:
            filtered_query = (
                f"SELECT * FROM ({self.query}) AS _src WHERE {safe_col} {op} '{since_value}'"
            )
        conn.execute(
            f"CREATE OR REPLACE TABLE {safe_tbl} AS "
            f"SELECT * FROM postgres_query('{_PG_ALIAS}', ?)",
            [filtered_query],
        )
        result = conn.execute(f"SELECT COUNT(*) FROM {safe_tbl}").fetchone()
        count = result[0] if result is not None else 0
        print(f"[PostgresConnector] Incrementally loaded {count} rows → {safe_tbl}")
        self._detach(conn)

    def test_connection(self) -> bool:
        try:
            conn = duckdb.connect()
            self._attach(conn)
            conn.execute(
                f"SELECT 1 FROM postgres_query('{_PG_ALIAS}', 'SELECT 1')",
            )
            self._detach(conn)
            conn.close()
            return True
        except Exception as e:
            print(f"[PostgresConnector] Connection failed: {e}")
            try:
                conn.close()
            except Exception:
                pass
            return False
