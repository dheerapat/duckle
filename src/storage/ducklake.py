"""
DuckLake-backed storage for DuckETL.

Uses the official `ducklake` extension with a local DuckDB file as the
metadata catalog.  All data is automatically stored as Parquet files
managed by DuckLake (supports time-travel, updates, deletes, etc.).

Bronze / Silver / Gold layers are implemented as DuckLake schemas.
"""
import os
import tempfile
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

    def __init__(self, base_path: str = "./lake"):
        self.base_path = base_path
        self.conn: duckdb.DuckDBPyConnection
        self._meta_conn: duckdb.DuckDBPyConnection
        self._ensure_base_path()
        self._init_ducklake()

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _ensure_base_path(self):
        """Ensure the base directory exists (DuckLake ATTACH requires it)."""
        os.makedirs(self.base_path, exist_ok=True)

    def _init_ducklake(self):
        """
        Create or attach the DuckLake database.

        • Catalog file : <base_path>/ducklake.ducklake
        • Data files   : <base_path>/ducklake.ducklake.files/  (Parquet)
        """
        catalog_path = os.path.join(self.base_path, "ducklake.ducklake")

        conn = duckdb.connect()
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
        conn.execute(
            f"ATTACH 'ducklake:{catalog_path}' AS ducklake"
        )
        conn.execute("USE ducklake")

        # Create layer schemas
        for layer in self.LAYERS:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {layer}")

        # Lightweight metadata table for pipeline-level info
        # (DuckLake itself does not track arbitrary metadata like pipeline name)
        # Stored in a separate DuckDB file because DuckLake tables don't support PKs
        metadata_file = os.path.join(self.base_path, "_metadata.db")
        meta_conn = duckdb.connect(metadata_file)
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
        conn: duckdb.DuckDBPyConnection,
        table: str,
        name: str,
        layer: str = "gold",
        pipeline: Optional[str] = None,
    ) -> str:
        """
        Write a DuckDB table into the DuckLake layer.

        The source ``conn`` is a *separate* DuckDB connection, so we
        export its table to a temporary Parquet file and let DuckLake
        ingest it natively via ``CREATE TABLE … AS SELECT * FROM
        read_parquet(…)``.
        """
        assert layer in self.LAYERS, f"Layer must be one of {self.LAYERS}"

        table = safe_identifier(table, label="table")
        safe_name = safe_identifier(name, label="dataset name")

        # Export source table → temp Parquet → DuckLake table
        # NOTE: COPY TO does not support parameterised file paths, so we
        # use a fixed tempfile name rather than interpolating an arbitrary path.
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp_path = tmp.name
        tmp.close()
        try:
            conn.execute(f"COPY {table} TO '{tmp_path}' (FORMAT PARQUET)")
            result = conn.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()
            count = result[0] if result is not None else 0

            # Use parameterised query for the file path to prevent injection
            self.conn.execute(
                f"CREATE OR REPLACE TABLE {layer}.{safe_name} "
                "AS SELECT * FROM read_parquet(?)",
                [tmp_path],
            )
        finally:
            # Clean up the temp file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

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
