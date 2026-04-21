import traceback
from datetime import datetime, timezone
from typing import List, Optional

from connectors.base import BaseConnector
from utils.sql_safety import safe_identifier

from engine.transformer import Transformer
from quality.checker import QualityChecker, QualityRule
from storage.ducklake import DuckLakeStorage


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
        self.sources: dict[str, BaseConnector]

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

    def run(self, pipeline: Pipeline) -> dict:
        run = {
            "pipeline": pipeline.name,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "running",
            "error": None,
        }
        print(f"\n🚀 Starting pipeline: {pipeline.name}")

        conn = self.storage.conn
        intermediate_tables: List[str] = []

        try:
            # 1. EXTRACT
            print("\n[1/4] Extracting...")
            for alias, connector in pipeline.sources.items():
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
            self.storage.write(
                final_table,
                name=pipeline.destination_name,
                layer=pipeline.destination_layer,
                pipeline=pipeline.name,
            )

            run["status"] = "success"
            print(f"\n✅ Pipeline '{pipeline.name}' completed successfully.")

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
