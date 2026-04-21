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
        self.catalog_path = os.environ.get(
            "DUCKLAKE_CATALOG_PATH", "./lake/ducklake.ducklake"
        )
        self.ducklake_path = os.environ.get(
            "DUCKLAKE_DATA_PATH", "./lake/ducklake.files"
        )
        self.metadata_path = os.environ.get(
            "DUCKLAKE_METADATA_PATH", "./lake/_metadata.db"
        )
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
                id                VARCHAR PRIMARY KEY,
                name              VARCHAR,
                layer             VARCHAR,
                row_count         INTEGER,
                created_at        TIMESTAMP,
                pipeline          VARCHAR,
                cursor_column     VARCHAR DEFAULT NULL,
                last_cursor_value VARCHAR DEFAULT NULL,
                merge_keys        VARCHAR DEFAULT NULL,
                run_mode          VARCHAR DEFAULT 'full'
            )
        """)
        # Migrate existing _metadata databases created before incremental loading
        meta_conn.execute("ALTER TABLE _metadata ADD COLUMN IF NOT EXISTS cursor_column VARCHAR DEFAULT NULL")
        meta_conn.execute("ALTER TABLE _metadata ADD COLUMN IF NOT EXISTS last_cursor_value VARCHAR DEFAULT NULL")
        meta_conn.execute("ALTER TABLE _metadata ADD COLUMN IF NOT EXISTS merge_keys VARCHAR DEFAULT NULL")
        meta_conn.execute("ALTER TABLE _metadata ADD COLUMN IF NOT EXISTS run_mode VARCHAR DEFAULT 'full'")

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

        result = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
        count = result[0] if result is not None else 0

        # Direct in-connection write — no temp file, no cross-connection copy
        self.conn.execute(
            f"CREATE OR REPLACE TABLE {layer}.{safe_name} " f"AS SELECT * FROM {table}"
        )

        now = datetime.now(timezone.utc)
        self._meta_conn.execute(
            """
            INSERT OR REPLACE INTO _metadata
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"{layer}/{safe_name}",
                safe_name,
                layer,
                count,
                now,
                pipeline,
                None,
                None,
                None,
                "full",
            ],
        )
        self._meta_conn.commit()

        print(f"[DuckLake] Written {count} rows → {layer}.{safe_name}")
        return f"{layer}.{safe_name}"

    def merge(
        self,
        table: str,
        name: str,
        layer: str,
        merge_keys: list[str],
        pipeline: Optional[str] = None,
    ) -> str:
        """Upsert a DuckDB table into DuckLake using MERGE INTO."""
        assert layer in self.LAYERS, f"Layer must be one of {self.LAYERS}"

        safe_table = safe_identifier(table, label="table")
        safe_name = safe_identifier(name, label="dataset name")
        dest = f"{layer}.{safe_name}"

        row = self.conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [layer, safe_name],
        ).fetchone()
        assert row is not None
        exists = row[0]

        if not exists:
            return self.write(table, name, layer, pipeline)

        on_clause = " AND ".join(
            f"dest.{safe_identifier(k, label='merge key')} = src.{safe_identifier(k, label='merge key')}"
            for k in merge_keys
        )

        self.conn.execute(f"""
            MERGE INTO {dest} AS dest
            USING {safe_table} AS src
            ON ({on_clause})
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

        result = self.conn.execute(f"SELECT COUNT(*) FROM {dest}").fetchone()
        count = result[0] if result is not None else 0

        now = datetime.now(timezone.utc)
        merge_keys_str = ",".join(merge_keys)
        self._meta_conn.execute(
            """
            INSERT OR REPLACE INTO _metadata
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                f"{layer}/{safe_name}",
                safe_name,
                layer,
                count,
                now,
                pipeline,
                None,
                None,
                merge_keys_str,
                "incremental",
            ],
        )
        self._meta_conn.commit()

        print(f"[DuckLake] Merged into {dest} ({count} total rows)")
        return dest

    def get_last_cursor(self, name: str, layer: str = "gold") -> Optional[str]:
        """Return the last high-water mark cursor value for a dataset."""
        safe_name = safe_identifier(name, label="dataset name")
        row = self._meta_conn.execute(
            "SELECT last_cursor_value FROM _metadata WHERE id = ?",
            [f"{layer}/{safe_name}"],
        ).fetchone()
        return row[0] if row is not None else None

    def update_pipeline_cursor(
        self,
        name: str,
        layer: str,
        cursor_column: Optional[str],
        last_cursor_value: Optional[str],
        merge_keys: Optional[list[str]],
        run_mode: str,
    ) -> None:
        """Update cursor metadata for a dataset after a successful run."""
        safe_name = safe_identifier(name, label="dataset name")
        merge_keys_str = ",".join(merge_keys) if merge_keys else None
        self._meta_conn.execute(
            """
            UPDATE _metadata
            SET cursor_column = ?,
                last_cursor_value = ?,
                merge_keys = ?,
                run_mode = ?
            WHERE id = ?
            """,
            [
                cursor_column,
                last_cursor_value,
                merge_keys_str,
                run_mode,
                f"{layer}/{safe_name}",
            ],
        )
        self._meta_conn.commit()

    def get_pipeline_state(self, name: str, layer: str = "gold") -> Optional[dict]:
        """Return full metadata state for a dataset."""
        safe_name = safe_identifier(name, label="dataset name")
        row = self._meta_conn.execute(
            """
            SELECT name, layer, row_count, created_at, pipeline,
                   cursor_column, last_cursor_value, merge_keys, run_mode
            FROM _metadata
            WHERE id = ?
            """,
            [f"{layer}/{safe_name}"],
        ).fetchone()
        if row is None:
            return None
        return {
            "name": row[0],
            "layer": row[1],
            "row_count": row[2],
            "created_at": row[3],
            "pipeline": row[4],
            "cursor_column": row[5],
            "last_cursor_value": row[6],
            "merge_keys": row[7],
            "run_mode": row[8],
        }

    def read(self, name: str, layer: str = "gold") -> duckdb.DuckDBPyRelation:
        """Read a dataset from the DuckLake layer."""
        if layer not in self.LAYERS:
            raise ValueError(f"Layer must be one of {self.LAYERS}")

        safe_name = safe_identifier(name, label="dataset name")
        return self.conn.table(f"{layer}.{safe_name}")

    def list_datasets(self) -> list[dict]:
        """List all datasets registered in the metadata catalog."""
        return self._meta_conn.execute("""
            SELECT name, layer, row_count, created_at, pipeline,
                   cursor_column, last_cursor_value, merge_keys, run_mode
            FROM _metadata
            ORDER BY created_at DESC
        """).fetchdf().to_dict("records")

    def snapshots(self) -> list[dict]:
        """Return all DuckLake snapshots for the catalog (time-travel history)."""
        return (
            self.conn.execute("FROM ducklake.snapshots()").fetchdf().to_dict("records")
        )

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
