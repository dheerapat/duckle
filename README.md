# Duckle 🦆

A lightweight ETL platform for small companies, powered by DuckDB + DuckLake.

## Philosophy
- No clusters. No servers. Just a process.
- SQL-first transformations.
- Runs on a $20/month VM.

## Architecture
```
connectors/   → Extract: pull data from sources (CSV, Postgres, APIs)
engine/       → Transform: DuckDB SQL execution
storage/      → Load: DuckLake / Parquet persistence
orchestrator/ → Schedule & run pipelines
quality/      → Data quality checks
api/          → FastAPI REST layer
ui/           → React dashboard (coming soon)
```

## Example Pipeline
```python
from connectors.csv_connector import CSVConnector
from engine.transformer import Transformer
from storage.ducklake import DuckLakeStorage
from orchestrator.runner import PipelineRunner

pipeline = {
    "name": "sales_etl",
    "source": CSVConnector("data/sales.csv"),
    "transforms": [
        "SELECT date, SUM(amount) as revenue FROM raw GROUP BY date"
    ],
    "destination": DuckLakeStorage("lake/gold/daily_revenue")
}

runner = PipelineRunner()
runner.run(pipeline)
```
