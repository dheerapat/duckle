"""
DuckLake-backed storage for DuckETL.

Uses the official `ducklake` extension with a local DuckDB file as the
metadata catalog.  All data is automatically stored as Parquet files
managed by DuckLake (supports time-travel, updates, deletes, etc.).

Bronze / Silver / Gold layers are implemented as DuckLake schemas.
"""
import os
from datetime import datetime, timezone
from typing import Optional

import duckdb
from utils.sql_safety import safe_identifier


class DuckLakeStorage:
    """
    Persist data using the DuckLake extension with a local DuckDB catalog.
    Data is stored as Parquet files managed by DuckLake automatically.
    Supports Bronze / Silver / Gold layers as schemas.
    """

    LAYERS = ["bronze", "silver", "gold"]

    def __init__(self):
        self.catalog_path = os.environ.get("DUCKLAKE_CATALOG_PATH", "./lake/ducklake.ducklake")
        self.ducklake_path = os.environ.get("DUCKLAKE_DATA_PATH", "./lake/ducklake.files")
        self.metadata_path = os.environ.get("DUCKLAKE_METADATA_PATH", "./lake/_metadata.db")
        self.conn: duckdb.DuckDBPyConnection
        self._meta_conn: duckdb.DuckDBPyConnection
        self._ensure_paths()
        self._init_ducklake()

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _ensure_paths(self):
        """Ensure parent directories for all paths exist."""
        for path in (self.catalog_path, self.ducklake_path, self.metadata_path):
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)

    def _init_ducklake(self):
        """
        Create or attach the DuckLake database.

        • Catalog file : <catalog_path>
        • Data files   : <ducklake_path>/  (Parquet)
        """
        conn = duckdb.connect()
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
        conn.execute(
            f"ATTACH 'ducklake:{self.catalog_path}' AS ducklake "
            f"(DATA_PATH '{self.ducklake_path}')"
        )
        conn.execute("USE ducklake")

        # Create layer schemas
        for layer in self.LAYERS:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")

        # Lightweight metadata table for pipeline-level info
        # (DuckLake itself does not track arbitrary metadata like pipeline name)
        # Stored in a separate DuckDB file because DuckLake tables don't support PKs
        meta_conn = duckdb.connect(self.metadata_path)
        meta_conn.execute("""
            CREATE TABLE IF NOT EXISTS _metadata (
                id          VARCHAR PRIMARY KEY,
                name        VARCHAR,
                layer       VARCHAR,
                row_count   INTEGER,
                created_at  TIMESTAMP,
                pipeline    VARCHAR
            )
        """)

        self.conn = conn
        self._meta_conn = meta_conn

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write(
        self,
        table: str,
        name: str,
        layer: str = "gold",
        pipeline: Optional[str] = None,
    ) -> str:
        """
        Write a DuckDB table into a DuckLake layer.

        Uses the shared DuckLake connection so no temp files or
        cross-connection data transfer is needed — the table is
        copied in-place via ``CREATE OR REPLACE TABLE … AS SELECT``.

        The caller must ensure the source table exists in ``self.conn``
        (e.g. the pipeline runner uses the same connection for extract,
        transform, and load).
        """
        assert layer in self.LAYERS, f"Layer must be one of {self.LAYERS}"

        table = safe_identifier(table, label="table")
        safe_name = safe_identifier(name, label="dataset name")

        result = self.conn.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()
        count = result[0] if result is not None else 0

        # Direct in-connection write — no temp file, no cross-connection copy
        self.conn.execute(
            f"CREATE OR REPLACE TABLE {layer}.{safe_name} "
            f"AS SELECT * FROM {table}"
        )

        now = datetime.now(timezone.utc)
        self._meta_conn.execute(
            """
            INSERT OR REPLACE INTO _metadata
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [f"{layer}/{safe_name}", safe_name, layer, count, now, pipeline],
        )
        self._meta_conn.commit()

        print(f"[DuckLake] Written {count} rows → {layer}.{safe_name}")
        return f"{layer}.{safe_name}"

    def read(self, name: str, layer: str = "gold") -> duckdb.DuckDBPyRelation:
        """Read a dataset from the DuckLake layer."""
        if layer not in self.LAYERS:
            raise ValueError(f"Layer must be one of {self.LAYERS}")

        safe_name = safe_identifier(name, label="dataset name")
        return self.conn.table(f"{layer}.{safe_name}")

    def list_datasets(self) -> list[dict]:
        """List all datasets registered in the metadata catalog."""
        return self._meta_conn.execute("""
            SELECT name, layer, row_count, created_at, pipeline
            FROM _metadata
            ORDER BY created_at DESC
        """).fetchdf().to_dict("records")

    def snapshots(self) -> list[dict]:
        """Return all DuckLake snapshots for the catalog (time-travel history)."""
        return self.conn.execute(
            "FROM ducklake.snapshots()"
        ).fetchdf().to_dict("records")

    def read_at_version(
        self, name: str, layer: str = "gold", version: int = 0
    ) -> duckdb.DuckDBPyRelation:
        """Read a dataset at a specific DuckLake snapshot version."""
        if layer not in self.LAYERS:
            raise ValueError(f"Layer must be one of {self.LAYERS}")
        safe_name = safe_identifier(name, label="dataset name")
        return self.conn.query(
            f"SELECT * FROM {layer}.{safe_name} AT (VERSION => {version})"
        )

    def close(self):
        """Close all connections."""
        if hasattr(self, "_meta_conn"):
            self._meta_conn.close()
        if hasattr(self, "conn"):
            self.conn.close()

    def __del__(self):
        self.close()
