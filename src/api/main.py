from typing import List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from connectors.csv_connector import CSVConnector
from connectors.postgres_connector import PostgresConnector
from connectors.table_connector import TableConnector
from orchestrator.runner import IncrementalConfig, Pipeline, PipelineRunner
from quality.checker import QualityRule
from storage.ducklake import DuckLakeStorage

app = FastAPI(title="DuckETL API", version="0.1.0")
storage = DuckLakeStorage()
runner = PipelineRunner(storage=storage)


# --- Models ---


class IncrementalConfigModel(BaseModel):
    cursor_column: str
    merge_keys: list[str]
    default_full_refresh: bool = False


class RunPipelineRequest(BaseModel):
    name: str
    transforms: list[str]
    destination_name: str
    destination_layer: str = "gold"
    source_type: str  # "csv" | "postgres" | "table"
    source_config: dict
    incremental: Optional[IncrementalConfigModel] = None
    quality_rules: Optional[list[dict]] = None


class RunStatus(BaseModel):
    pipeline: str
    started_at: str
    finished_at: Optional[str]
    status: str
    error: Optional[str]


class DatasetInfo(BaseModel):
    name: str
    layer: str
    row_count: int
    created_at: str
    pipeline: Optional[str]
    cursor_column: Optional[str] = None
    last_cursor_value: Optional[str] = None
    merge_keys: Optional[str] = None
    run_mode: Optional[str] = None


class PipelineState(BaseModel):
    name: str
    layer: str
    row_count: int
    created_at: str
    pipeline: Optional[str]
    cursor_column: Optional[str] = None
    last_cursor_value: Optional[str] = None
    merge_keys: Optional[str] = None
    run_mode: Optional[str] = None


# --- Helpers ---


def _build_connector(source_type: str, source_config: dict):
    if source_type == "csv":
        return CSVConnector(source_config["path"])
    if source_type == "postgres":
        return PostgresConnector(
            source_config["connection_string"], source_config["query"]
        )
    if source_type == "table":
        return TableConnector(source_config["source_table"])
    raise ValueError(f"Unsupported source_type: {source_type}")


def _build_quality_rules(rules: list[dict]) -> list[QualityRule]:
    """Build QualityRule objects from dict descriptions."""
    result = []
    for r in rules:
        result.append(
            QualityRule(
                name=r["name"],
                sql=r["sql"],
                description=r.get("description", ""),
                blocking=r.get("blocking", True),
            )
        )
    return result


# --- Routes ---


@app.get("/")
def root():
    return {"message": "DuckETL is running 🦆"}


@app.post("/run")
def run_pipeline(req: RunPipelineRequest, full_refresh: bool = False):
    """Execute a pipeline definition synchronously."""
    try:
        incremental = None
        if req.incremental is not None:
            incremental = IncrementalConfig(
                cursor_column=req.incremental.cursor_column,
                merge_keys=req.incremental.merge_keys,
                default_full_refresh=req.incremental.default_full_refresh,
            )

        connector = _build_connector(req.source_type, req.source_config)

        quality_rules = None
        if req.quality_rules:
            quality_rules = _build_quality_rules(req.quality_rules)

        pipeline = Pipeline(
            name=req.name,
            transforms=req.transforms,
            destination_name=req.destination_name,
            destination_layer=req.destination_layer,
            source=connector,
            incremental=incremental,
            quality_rules=quality_rules,
        )
        result = runner.run(pipeline, full_refresh=full_refresh)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/runs", response_model=List[RunStatus])
def get_runs():
    """Return all pipeline run history."""
    return runner.run_history


@app.get("/datasets", response_model=List[DatasetInfo])
def list_datasets():
    """List all datasets stored in DuckLake."""
    return storage.list_datasets()


@app.get("/datasets/{layer}/{name}/preview")
def preview_dataset(layer: str, name: str, limit: int = 10):
    """Preview rows from a stored dataset."""
    try:
        rel = storage.read(name, layer)
        rows = rel.fetchdf().head(limit).to_dict("records")
        return {"name": name, "layer": layer, "rows": rows}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/pipelines/{name}/state")
def pipeline_state(
    name: str,
    destination_name: str = Query(...),
    layer: str = Query("gold"),
):
    """Return last cursor value, run mode, row count for a dataset."""
    state = storage.get_pipeline_state(destination_name, layer)
    if state is None:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return state


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
