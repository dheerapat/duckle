# Duckle 🦆

A lightweight data engineering platform for small companies, powered by DuckDB + DuckLake.

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

## Source Database

Docker Compose provides a PostgreSQL database seeded with restaurant data.

```bash
docker compose up -d              # start PostgreSQL
docker compose down -v           # stop & wipe data (re-seeds on next up)
```

Copy `.env.example` → `.env` and adjust credentials/port if needed.

### Tables

**menu_items** (20 rows) — what the restaurant sells:

| id | name | category | price |
|----|------|----------|------:|
| 1 | Bruschetta | Appetizers | 8.50 |
| 2 | Caesar Salad | Appetizers | 9.00 |
| 6 | Grilled Salmon | Mains | 24.00 |
| 9 | Margherita Pizza | Mains | 16.00 |
| 16 | Tiramisu | Desserts | 9.50 |
| 19 | Craft Beer | Drinks | 7.00 |

**customers** (39 rows) — people who place orders:

| id | name | email | phone | joined_at |
|----|------|-------|-------|----------|
| 1 | Alice Johnson | alice@example.com | 555-0101 | 2023-06-15 |
| 2 | Bob Martinez | bob@example.com | 555-0102 | 2023-07-20 |
| 3 | Carol Chen | carol@example.com | 555-0103 | 2023-08-03 |

**orders** (~2,400 rows) — one per customer visit, spanning 2024-01-01 → 2024-06-30:

| id | order_date | customer_id |
|----|------------|-------------|
| 1 | 2024-01-01 | 13 |
| 2 | 2024-01-01 | 29 |
| 3 | 2024-01-01 | 27 |
| 4 | 2024-01-01 | 37 |

**order_items** (~6,000 rows) — line items within each order:

| id | order_id | menu_item_id | quantity |
|----|----------|--------------|--------:|
| 1 | 1 | 3 | 2 |
| 2 | 1 | 11 | 3 |
| 3 | 1 | 9 | 1 |
| 4 | 1 | 16 | 1 |
| 5 | 2 | 16 | 2 |

> Weekends get more orders (13–22/day) vs weekdays (8–15/day). The `orders.order_date` index and 6-month spread are designed for testing incremental ETL by date range.

Connection string for the PostgresConnector:
```
host=localhost port=5433 dbname=restaurant user=duckle password=duckle_secret
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
