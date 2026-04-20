import traceback
from datetime import datetime, timezone
from typing import List, Optional

import duckdb
from connectors.base import BaseConnector

from engine.transformer import Transformer
from quality.checker import QualityChecker, QualityRule
from storage.ducklake import DuckLakeStorage


class Pipeline:
    def __init__(
        self,
        name: str,
        source: BaseConnector,
        transforms: List[str],
        destination_name: str,
        destination_layer: str = "gold",
        quality_rules: Optional[List[QualityRule]] = None,
    ):
        self.name = name
        self.source = source
        self.transforms = transforms
        self.destination_name = destination_name
        self.destination_layer = destination_layer
        self.quality_rules = quality_rules or []


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

        try:
            conn = duckdb.connect()

            # 1. EXTRACT
            print("\n[1/4] Extracting...")
            pipeline.source.extract(conn, "raw")

            # 2. TRANSFORM
            print("\n[2/4] Transforming...")
            transformer = Transformer(conn)
            final_table = transformer.run(pipeline.transforms, "raw")

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
                conn,
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

        run["finished_at"] = datetime.now(timezone.utc).isoformat()
        self.run_history.append(run)
        return run
