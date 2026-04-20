import duckdb
from connectors.base import BaseConnector


class PostgresConnector(BaseConnector):
    """Extract data from a Postgres table."""

    def __init__(self, connection_string: str, query: str):
        self.connection_string = connection_string
        self.query = query

    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        # DuckDB can read Postgres directly via the postgres scanner extension
        conn.execute("INSTALL postgres; LOAD postgres;")
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM postgres_query('{self.connection_string}', '{self.query}')
        """)
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"[PostgresConnector] Loaded {count} rows → {table_name}")

    def test_connection(self) -> bool:
        try:
            conn = duckdb.connect()
            conn.execute("INSTALL postgres; LOAD postgres;")
            conn.execute(
                f"SELECT 1 FROM postgres_query('{self.connection_string}', 'SELECT 1')"
            )
            return True
        except Exception as e:
            print(f"[PostgresConnector] Connection failed: {e}")
            return False
