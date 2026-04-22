import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from connectors.base import BaseConnector
from utils.sql_safety import safe_identifier

from engine.transformer import Transformer
from quality.checker import QualityChecker, QualityRule
from storage.ducklake import DuckLakeStorage


@dataclass
class IncrementalConfig:
    cursor_column: str
    merge_keys: list[str]
    default_full_refresh: bool = False

    # NEW — optional explicit bounds
    from_value: Optional[str] = None   # inclusive floor
    to_value: Optional[str] = None     # exclusive ceiling


class Pipeline:
    """
    Defines an ETL pipeline with one or more data sources.

    * Single-source (backward compat): pass ``source=`` — it becomes
      the alias ``"input"`` and can be referenced in SQL as ``{{input}}``.
    * Multi-source: pass ``sources={"alias": Connector, ...}`` — each
      alias is materialised as a DuckDB table and can be referenced
      directly by name in the transform SQL.

    Exactly one of ``source`` or ``sources`` must be provided.
    """

    def __init__(
        self,
        name: str,
        transforms: List[str],
        destination_name: str,
        destination_layer: str = "gold",
        quality_rules: Optional[List[QualityRule]] = None,
        source: Optional[BaseConnector] = None,
        sources: Optional[dict[str, BaseConnector]] = None,
        incremental: Optional[IncrementalConfig] = None,
    ):
        if source is not None and sources is not None:
            raise ValueError("Provide only one of 'source' or 'sources'")
        if source is None and sources is None:
            raise ValueError("One of 'source' or 'sources' is required")

        self.name = name
        self.transforms = transforms
        self.destination_name = destination_name
        self.destination_layer = destination_layer
        self.quality_rules = quality_rules or []
        self.incremental = incremental
        self.sources: dict[str, BaseConnector]

        if incremental is not None:
            if not incremental.merge_keys:
                raise ValueError("incremental.merge_keys must be non-empty")
            safe_identifier(incremental.cursor_column, label="cursor_column")

        # Normalise into a dict of {alias: connector}
        if source is not None:
            self.sources = {"input": source}
        else:
            assert sources is not None
            for alias in sources:
                safe_identifier(alias, label="source alias")
            self.sources = sources


class PipelineRunner:
    def __init__(self, storage: Optional[DuckLakeStorage] = None):
        self.storage = storage or DuckLakeStorage()
        self.run_history = []

    def run(self, pipeline: Pipeline, full_refresh: bool = False) -> dict:
        run = {
            "pipeline": pipeline.name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "error": None,
        }
        print(f"\n🚀 Starting pipeline: {pipeline.name}")

        conn = self.storage.conn
        intermediate_tables: List[str] = []

        incremental = pipeline.incremental
        since_value: Optional[str] = None
        until_value: Optional[str] = None
        from_is_explicit = False
        run_mode = "full"

        if incremental is not None and not full_refresh:
            # Resolve from_value: explicit > stored cursor > None
            if incremental.from_value is not None:
                since_value = incremental.from_value
                from_is_explicit = True
                run_mode = "incremental"
                print(
                    f"[Incremental] Explicit from: {incremental.cursor_column} >= {since_value}"
                )
            else:
                since_value = self.storage.get_last_cursor(
                    pipeline.destination_name, pipeline.destination_layer
                )
                if since_value is not None:
                    run_mode = "incremental"
                    print(
                        f"[Incremental] High-water mark: {incremental.cursor_column} > {since_value}"
                    )
                else:
                    print(
                        "[Incremental] No prior cursor found, falling back to full load."
                    )

            # Resolve to_value: explicit > None (no ceiling)
            if incremental.to_value is not None:
                until_value = incremental.to_value
                print(
                    f"[Incremental] Explicit to: {incremental.cursor_column} < {until_value}"
                )

        try:
            # 1. EXTRACT
            print("\n[1/4] Extracting...")
            for alias, connector in pipeline.sources.items():
                if run_mode == "incremental" and since_value is not None:
                    assert incremental is not None
                    connector.extract_incremental(
                        conn,
                        alias,
                        incremental.cursor_column,
                        since_value,
                        until_value=until_value,
                        from_is_explicit=from_is_explicit,
                    )
                else:
                    connector.extract(conn, alias)
                intermediate_tables.append(alias)

            # 2. TRANSFORM
            print("\n[2/4] Transforming...")
            transformer = Transformer(conn)
            # Single-source → use the "input" alias for {{input}} compat.
            # Multi-source   → source_table=None; SQL references aliases directly.
            source_table = "input" if len(pipeline.sources) == 1 else None
            final_table = transformer.run(pipeline.transforms, source_table)
            for i in range(len(pipeline.transforms)):
                intermediate_tables.append(f"_transform_step_{i}")

            # 3. QUALITY CHECKS
            if pipeline.quality_rules:
                print("\n[3/4] Running quality checks...")
                checker = QualityChecker(conn)
                result = checker.check(final_table, pipeline.quality_rules)
                run["quality"] = result["results"]
                if not result["passed"]:
                    raise ValueError("Quality checks failed — pipeline aborted.")
            else:
                print("\n[3/4] No quality rules defined, skipping.")

            # 4. LOAD
            print("\n[4/4] Loading to DuckLake...")
            if run_mode == "incremental" and since_value is not None:
                assert pipeline.incremental is not None
                self.storage.merge(
                    final_table,
                    name=pipeline.destination_name,
                    layer=pipeline.destination_layer,
                    merge_keys=pipeline.incremental.merge_keys,
                    pipeline=pipeline.name,
                )
            else:
                self.storage.write(
                    final_table,
                    name=pipeline.destination_name,
                    layer=pipeline.destination_layer,
                    pipeline=pipeline.name,
                )

            run["status"] = "success"
            print(f"\n✅ Pipeline '{pipeline.name}' completed successfully.")

            # 5. UPDATE CURSOR
            if incremental is not None and run["status"] == "success":
                safe_cursor = safe_identifier(
                    incremental.cursor_column, label="cursor_column"
                )
                # Verify cursor column exists in final output
                cols = (
                    conn.execute(f"SELECT * FROM {final_table} LIMIT 0")
                    .fetchdf()
                    .columns.tolist()
                )
                if safe_cursor not in cols:
                    raise ValueError(
                        f"Incremental cursor column '{incremental.cursor_column}' not found "
                        f"in transformed output. Available columns: {cols}"
                    )
                row = conn.execute(
                    f"SELECT MAX({safe_cursor}) FROM {final_table}"
                ).fetchone()
                assert row is not None
                new_cursor = row[0]
                if new_cursor is not None:
                    self.storage.update_pipeline_cursor(
                        pipeline.destination_name,
                        pipeline.destination_layer,
                        cursor_column=incremental.cursor_column,
                        last_cursor_value=str(new_cursor),
                        merge_keys=incremental.merge_keys,
                        run_mode=run_mode,
                    )
                    print(f"[Incremental] Cursor updated to {new_cursor}")
                else:
                    print("[Incremental] No rows in range — cursor not advanced.")

        except Exception as e:
            run["status"] = "failed"
            run["error"] = str(e)
            print(f"\n❌ Pipeline '{pipeline.name}' failed: {e}")
            traceback.print_exc()

        finally:
            # Clean up intermediate tables from the shared connection
            for table_name in intermediate_tables:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                except Exception:
                    pass

        run["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.run_history.append(run)
        return run
