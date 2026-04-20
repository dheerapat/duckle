import duckdb
from connectors.base import BaseConnector
from utils.sql_safety import safe_identifier


class PostgresConnector(BaseConnector):
    """Extract data from a Postgres table."""

    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        # DuckDB can read Postgres directly via the postgres scanner extension
        conn.execute("INSTALL postgres; LOAD postgres;")
        table_name = safe_identifier(table_name, label="table_name")
        # Parameterise connection string and query to prevent injection
        conn.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            "SELECT * FROM postgres_query(?, ?)",
            [self.connection_string, self.query],
        )
        result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        count = result[0] if result is not None else 0
        print(f"[PostgresConnector] Loaded {count} rows → {table_name}")

    def test_connection(self) -> bool:
        try:
            conn = duckdb.connect()
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn.execute(
                "SELECT 1 FROM postgres_query(?, 'SELECT 1')",
                [self.connection_string],
            )
            return True
        except Exception as e:
            print(f"[PostgresConnector] Connection failed: {e}")
            return False
