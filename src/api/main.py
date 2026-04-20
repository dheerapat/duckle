from typing import List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from orchestrator.runner import PipelineRunner
from storage.ducklake import DuckLakeStorage

app = FastAPI(title="DuckETL API", version="0.1.0")
storage = DuckLakeStorage()
runner = PipelineRunner(storage=storage)


# --- Models ---


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


# --- Routes ---


@app.get("/")
def root():
    return {"message": "DuckETL is running 🦆"}


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


@app.get("/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=8000, reload=True)
