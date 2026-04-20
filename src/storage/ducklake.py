import os
from datetime import datetime

import duckdb


class DuckLakeStorage:
    """
    Persist DuckDB tables as Parquet files with a lightweight catalog.
    Supports Bronze / Silver / Gold layers.
    """

    LAYERS = ["bronze", "silver", "gold"]

    def __init__(self, base_path: str = "./lake"):
        self.base_path = base_path
        self._init_catalog()

    def _init_catalog(self):
        """Create catalog DB to track all stored datasets."""
        os.makedirs(self.base_path, exist_ok=True)
        self.catalog_conn = duckdb.connect(f"{self.base_path}/_catalog.db")
        self.catalog_conn.execute("""
            CREATE TABLE IF NOT EXISTS catalog (
                id          VARCHAR PRIMARY KEY,
                name        VARCHAR,
                layer       VARCHAR,
                path        VARCHAR,
                row_count   INTEGER,
                created_at  TIMESTAMP,
                pipeline    VARCHAR
            )
        """)

    def write(
        self,
        conn: duckdb.DuckDBPyConnection,
        table: str,
        name: str,
        layer: str = "gold",
        pipeline: str = None,
    ) -> str:
        """Write a DuckDB table to Parquet and register in catalog."""
        assert layer in self.LAYERS, f"Layer must be one of {self.LAYERS}"

        layer_path = os.path.join(self.base_path, layer)
        os.makedirs(layer_path, exist_ok=True)

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_path = os.path.join(layer_path, f"{name}_{ts}.parquet")

        conn.execute(f"COPY {table} TO '{file_path}' (FORMAT PARQUET)")
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        self.catalog_conn.execute(
            """
            INSERT OR REPLACE INTO catalog VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            [
                f"{layer}/{name}",
                name,
                layer,
                file_path,
                count,
                datetime.utcnow(),
                pipeline,
            ],
        )

        print(f"[DuckLake] Written {count} rows → {file_path}")
        return file_path

    def read(self, name: str, layer: str = "gold") -> duckdb.DuckDBPyRelation:
        """Read the latest Parquet file for a dataset."""
        row = self.catalog_conn.execute(
            """
            SELECT path FROM catalog
            WHERE name = ? AND layer = ?
            ORDER BY created_at DESC LIMIT 1
        """,
            [name, layer],
        ).fetchone()

        if not row:
            raise FileNotFoundError(f"Dataset '{layer}/{name}' not found in catalog.")

        conn = duckdb.connect()
        return conn.execute(f"SELECT * FROM read_parquet('{row[0]}')")

    def list_datasets(self) -> list:
        """List all datasets in the catalog."""
        return self.catalog_conn.execute("""
            SELECT name, layer, row_count, created_at, pipeline
            FROM catalog ORDER BY created_at DESC
        """).fetchdf().to_dict("records")
