# Getting Started with Duckle 🦆

A hands-on guide for data engineers who want to build lightweight, SQL-first ETL pipelines with DuckDB.

---

## What is Duckle?

Duckle is a single-process ETL platform built on **DuckDB + DuckLake**. No Spark clusters, no Kubernetes, no infrastructure headaches — just Python and SQL.

**Core idea:**
- Extract from CSV, Postgres, or existing DuckDB tables
- Transform with plain DuckDB SQL (chained steps, `{{input}}` templating)
- Validate with built-in data quality rules
- Load into Bronze → Silver → Gold layers with automatic MERGE upserts
- Track incremental state (high-water mark cursors) out of the box
- Gate data quality with **Write-Audit-Publish** (opt-in `wap_mode`) — write to staging, check quality, then publish

---

## 1. Setup (2 minutes)

### Prerequisites
- Python 3.12+
- `uv` (recommended) or `pip`
- Docker (for the Postgres demo database)

### Install dependencies

```bash
# Clone the repo
cd duckle

# Install with uv
uv sync

# Or with pip
pip install -e ".[dev]"
```

### Start the demo Postgres database

```bash
# Start a seeded restaurant database
docker compose up -d

# Verify it's running
docker compose ps
```

Connection string (if you need it):
```
host=localhost port=5433 dbname=restaurant user=duckle password=duckle_secret
```

> **Tip:** `docker compose down -v` wipes and re-seeds the database. Useful when you want a clean slate.

---

## 2. Run a Full Pipeline in 30 Seconds

### Option A: CSV Pipeline (self-contained, generates its own data)

```bash
uv run python scripts/example_csv.py
```

This script:
1. Generates realistic CSV data (`products`, `customers`, `order_lines`, `returns`)
2. Loads them into **bronze** (raw staging)
3. Cleans and joins them into **silver**
4. Aggregates into **gold** (revenue, customer LTV, product performance)
5. Demonstrates incremental loading with explicit date ranges

### Option B: Postgres Pipeline (real database extract)

```bash
uv run python scripts/example_postgres.py
```

This script:
1. Extracts from the Docker Postgres restaurant database (`menu_items`, `customers`, `orders`, `order_items`)
2. Runs the same bronze → silver → gold flow
3. Shows incremental backfill and resume patterns

---

## 3. Understand the Medallion Layers

| Layer | Purpose | SQL Pattern |
|-------|---------|-------------|
| **Bronze** | Raw ingest, minimal change | `SELECT * FROM {{input}}` |
| **Silver** | Cleaned, typed, enriched, deduped | `SELECT DISTINCT ... CAST(... AS DATE) ... JOIN` |
| **Gold** | Business aggregations | `SELECT ... GROUP BY ... SUM(...) ... CASE WHEN` |
| **Staging** | WAP transient layer (opt-in) | Data awaits quality gate before publish to bronze/silver/gold |

### Example: Bronze → Silver → Gold

```python
from connectors.csv_connector import CSVConnector
from connectors.table_connector import TableConnector
from orchestrator.runner import Pipeline, PipelineRunner
from quality.checker import no_nulls, unique_column, column_positive
from storage.ducklake import DuckLakeStorage

storage = DuckLakeStorage()
runner = PipelineRunner(storage=storage)

# ── Bronze: raw CSV ingest ──
bronze = Pipeline(
    name="bronze_orders",
    source=CSVConnector("data/orders.csv"),
    transforms=["SELECT * FROM {{input}}"],
    destination_name="orders",
    destination_layer="bronze",
)
runner.run(bronze)

# ── Silver: clean & validate ──
silver = Pipeline(
    name="silver_orders",
    source=TableConnector("bronze.orders"),
    transforms=["""
        SELECT DISTINCT
            order_id,
            CAST(order_date AS DATE) AS order_date,
            customer_id,
            quantity
        FROM {{input}}
        WHERE order_id IS NOT NULL
    """],
    destination_name="orders",
    destination_layer="silver",
    quality_rules=[
        no_nulls("order_id"),
        unique_column("order_id"),
        column_positive("quantity"),
    ],
)
runner.run(silver)

# ── Gold: aggregate ──
gold = Pipeline(
    name="gold_daily_revenue",
    source=TableConnector("silver.orders"),
    transforms=["""
        SELECT
            order_date,
            COUNT(*) AS total_orders,
            SUM(quantity) AS total_items
        FROM {{input}}
        GROUP BY order_date
    """],
    destination_name="daily_revenue",
    destination_layer="gold",
)
runner.run(gold)
```

---

## 4. Write Transforms with DuckDB SQL

Duckle is **SQL-first**. You write plain DuckDB SQL. No custom DSL.

### Single source: use `{{input}}`

```python
Pipeline(
    name="my_pipeline",
    source=CSVConnector("data.csv"),
    transforms=[
        "SELECT * FROM {{input}} WHERE amount > 0",
        "SELECT date, SUM(amount) AS revenue FROM {{input}} GROUP BY date",
    ],
    destination_name="daily_revenue",
    destination_layer="gold",
)
```

### Multi-source: reference aliases directly

```python
Pipeline(
    name="enriched_orders",
    sources={
        "orders": CSVConnector("data/orders.csv"),
        "customers": CSVConnector("data/customers.csv"),
    },
    transforms=["""
        SELECT
            o.order_id,
            o.order_date,
            c.name AS customer_name,
            c.region
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
    """],
    destination_name="enriched_orders",
    destination_layer="silver",
)
```

### Chained transforms

Each transform step feeds into the next. The final step is what gets written to DuckLake.

```python
transforms=[
    # Step 1: filter and cast
    """
    SELECT
        order_id,
        CAST(order_date AS DATE) AS order_date,
        customer_id,
        quantity
    FROM {{input}}
    WHERE quantity > 0
    """,
    # Step 2: aggregate
    """
    SELECT
        order_date,
        COUNT(*) AS total_orders,
        SUM(quantity) AS total_items
    FROM {{input}}
    GROUP BY order_date
    """,
    # Step 3: business logic
    """
    SELECT *,
        CASE
            WHEN total_orders >= 100 THEN 'high_volume'
            ELSE 'normal'
        END AS volume_tier
    FROM {{input}}
    """,
]
```

---

## 5. Data Quality Checks

Quality rules are **blocking by default** — if a check fails, the pipeline aborts before writing data.

### Built-in rules

```python
from quality.checker import no_nulls, unique_column, column_positive, min_row_count

quality_rules=[
    no_nulls("order_id"),           # No NULLs allowed
    unique_column("order_id"),      # All values must be distinct
    column_positive("amount"),      # Minimum must be > 0
    min_row_count(100),             # At least 100 rows expected
]
```

### Custom rules

```python
from quality.checker import QualityRule

QualityRule(
    name="no_future_dates",
    sql="SELECT MAX(order_date) <= CURRENT_DATE AS passed FROM {{table}}",
    description="No orders from the future",
    blocking=True,
)
```

> The `{{table}}` placeholder is replaced with the actual intermediate table name at runtime.
>
> **Tip:** When `wap_mode=True`, quality checks run against `staging.{destination_name}` instead of the intermediate transform table, so bad data is caught before it reaches the destination layer.

---

## 6. Incremental Loading

Duckle supports two incremental patterns:

### Pattern A: Cursor-based resume (auto)

The runner stores `MAX(cursor_column)` after each run. Next run only extracts rows `>` that cursor.

```python
from orchestrator.runner import IncrementalConfig

Pipeline(
    name="orders_incremental",
    source=PostgresConnector(PG_CONN, "SELECT * FROM orders"),
    transforms=["SELECT * FROM {{input}}"],
    destination_name="orders",
    destination_layer="bronze",
    incremental=IncrementalConfig(
        cursor_column="order_date",
        merge_keys=["order_id"],   # Used for MERGE upsert
    ),
)
```

**First run:** Full load (no cursor exists yet)  
**Second run:** Only `order_date > '2024-01-15'` (or whatever the last cursor was)

### Pattern B: Explicit `[from, to)` range (backfill)

Set `from_value` and/or `to_value` for backfills or bounded extracts.

```python
IncrementalConfig(
    cursor_column="order_date",
    merge_keys=["order_id"],
    from_value="2024-01-01",   # inclusive
    to_value="2024-02-01",     # exclusive
)
```

This is perfect for:
- Backfilling historical data
- Nightly bounded extracts (`from=yesterday, to=today`)
- Reprocessing a specific window after a bug fix

### Full refresh override

```python
runner.run(pipeline, full_refresh=True)  # Ignores cursor, does CREATE OR REPLACE TABLE
```

---

## 6. Write-Audit-Publish (WAP)

WAP is an **opt-in** pattern that makes quality checks gate the final write. Instead of writing directly to the destination layer, the pipeline:

1. Writes to `staging.{destination_name}`
2. Runs quality checks against the staging table
3. Only if all blocking checks pass → **publishes** to the destination layer
4. Staging table is dropped on success, **left intact on failure** for debugging

### When to use WAP

- **Critical pipelines** where bad data must never land in gold
- **Debugging data issues** — inspect the staging table after a failure
- **Downstream consumers** that cannot tolerate partially-written or invalid data

### Enabling WAP

```python
from quality.checker import no_nulls, min_row_count

pipeline = Pipeline(
    name="gold_revenue_wap",
    source=TableConnector("silver.orders"),
    transforms=["""
        SELECT
            order_date,
            SUM(line_total) AS revenue
        FROM {{input}}
        GROUP BY order_date
    """],
    destination_name="daily_revenue",
    destination_layer="gold",
    wap_mode=True,  # <-- opt-in
    quality_rules=[
        no_nulls("order_date"),
        min_row_count(1),
    ],
)
```

### What happens on failure

```python
runner = PipelineRunner()
result = runner.run(pipeline)

if result["status"] == "failed":
    # Staging table still exists — inspect it
    df = storage.conn.execute("SELECT * FROM staging.daily_revenue").fetchdf()
    print(df)  # Debug the bad data
```

### WAP + incremental loading

WAP works seamlessly with incremental loads. The delta is written to staging, checked, then published via `MERGE INTO`:

```python
pipeline = Pipeline(
    name="orders_wap_incremental",
    source=PostgresConnector(PG_CONN, "SELECT * FROM orders"),
    transforms=["SELECT * FROM {{input}}"],
    destination_name="orders",
    destination_layer="bronze",
    wap_mode=True,
    incremental=IncrementalConfig(
        cursor_column="order_date",
        merge_keys=["order_id"],
    ),
    quality_rules=[no_nulls("order_id")],
)
```

---

## 7. Available Connectors

---

## 8. Available Connectors

| Connector | Use case | Example |
|-----------|----------|---------|
| `CSVConnector(path)` | Local CSV files | `CSVConnector("data/sales.csv")` |
| `PostgresConnector(conn_str, query)` | Postgres via DuckDB extension | `PostgresConnector(PG_CONN, "SELECT * FROM orders")` |
| `TableConnector(table_name)` | DuckDB tables (cross-layer pipelines) | `TableConnector("silver.orders")` |

All connectors implement:
- `.extract(conn, table_name)` — register data as a DuckDB table
- `.test_connection()` — verify reachability
- `.extract_incremental(...)` — filter by cursor for incremental loads

---

## 9. REST API

Start the API server:

```bash
uv run uvicorn api.main:app --reload --port 8000
```

### Key endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `POST /run` | Execute a pipeline |
| `GET /runs` | Pipeline run history |
| `GET /datasets` | List all datasets |
| `GET /datasets/{layer}/{name}/preview?limit=10` | Preview rows |
| `GET /pipelines/{name}/state?destination_name=X&layer=gold` | Cursor + metadata |

### Example: Run a pipeline via API

```bash
curl -X POST "http://localhost:8000/run" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api_demo",
    "source_type": "csv",
    "source_config": {"path": "data/products.csv"},
    "transforms": ["SELECT * FROM {{input}}"],
    "destination_name": "products",
    "destination_layer": "bronze",
    "wap_mode": false
  }'
```

### WAP via API

```bash
curl -X POST "http://localhost:8000/run" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "api_wap_demo",
    "source_type": "csv",
    "source_config": {"path": "data/products.csv"},
    "transforms": ["SELECT * FROM {{input}}"],
    "destination_name": "products",
    "destination_layer": "bronze",
    "wap_mode": true,
    "quality_rules": [
      {"name": "min_row_count", "sql": "SELECT COUNT(*) >= 1 AS passed", "blocking": true}
    ]
  }'
```

### Example: Preview a dataset

```bash
curl "http://localhost:8000/datasets/gold/daily_category_revenue/preview?limit=5"
```

---

## 10. Testing

```bash
# Run all tests (~100)
uv run pytest -v

# Run specific test modules
uv run pytest tests/test_connectors.py -v
uv run pytest tests/test_runner.py -v
uv run pytest tests/test_quality.py -v

# Run incremental loading tests only
uv run pytest -k incremental -v
```

---

## 11. Project Structure Cheat Sheet

```
duckle/
├── src/
│   ├── connectors/          # Extract (CSV, Postgres, Table)
│   ├── engine/              # Transform (SQL step runner)
│   ├── storage/             # Load (DuckLake bronze/silver/gold/staging)
│   ├── orchestrator/        # Pipeline + PipelineRunner
│   ├── quality/             # Data quality rules
│   ├── api/                 # FastAPI REST server
│   └── utils/               # SQL safety helpers
├── tests/                   # pytest suite
├── scripts/                 # Standalone demo pipelines
├── seed/                    # Postgres init scripts
├── data/                    # CSV data directory
└── lake/                    # DuckLake storage (Parquet + catalog)
```

---

## 12. Safety & SQL Injection Defence

Duckle takes SQL injection seriously:

- **Table/column names** → validated against whitelist via `safe_identifier()`
- **File paths** → validated via `safe_path()`
- **Values** → always use `?` parameterised queries where DuckDB supports it
- **Cursor values / bounds** → validated and cast before use

Never interpolate identifiers directly into SQL strings. If you extend the codebase, route all identifiers through `safe_identifier()`.

---

## 13. Common Patterns

### Cross-layer JOIN (Silver → Gold)

```python
Pipeline(
    name="gold_revenue",
    sources={
        "orders": TableConnector("silver.orders"),
        "items": TableConnector("silver.order_items"),
    },
    transforms=["""
        SELECT
            o.order_date,
            SUM(i.line_total) AS revenue
        FROM orders o
        JOIN items i ON o.order_id = i.order_id
        GROUP BY o.order_date
    """],
    destination_name="daily_revenue",
    destination_layer="gold",
)
```

### Write-Audit-Publish (WAP) with failure recovery

```python
pipeline = Pipeline(
    name="critical_gold_metric",
    source=TableConnector("silver.orders"),
    transforms=["SELECT order_date, SUM(amount) AS revenue FROM {{input}} GROUP BY order_date"],
    destination_name="daily_revenue",
    destination_layer="gold",
    wap_mode=True,
    quality_rules=[
        no_nulls("order_date"),
        min_row_count(1),
    ],
)

result = runner.run(pipeline)

if result["status"] == "failed":
    # Inspect staging table before fixing upstream data
    bad_data = storage.conn.execute("SELECT * FROM staging.daily_revenue").fetchdf()
    print("Quality failed. Staging data:", bad_data)
    # Fix upstream, then re-run
```

### Preview before writing

```python
# Run transforms manually to inspect output
import duckdb
from engine.transformer import Transformer

conn = duckdb.connect()
conn.execute("CREATE TABLE raw AS SELECT * FROM 'data/test.csv'")

transformer = Transformer(conn)
result = transformer.run(["SELECT * FROM {{input}} WHERE amount > 100"], "raw")
print(conn.execute(f"SELECT * FROM {result} LIMIT 5").fetchdf())
```

---

## Next Steps

1. **Try WAP mode:** Edit a demo script and add `wap_mode=True` + a quality rule that fails. Inspect the staging table.
2. **Modify a demo script:** Edit `scripts/example_csv.py` and add a new gold-layer aggregation.
3. **Add a custom quality rule:** Extend `quality/checker.py` with a domain-specific check.
4. **Connect your own Postgres:** Replace the Docker connection string with your own database.
5. **Build a new connector:** Implement `BaseConnector` for an API or cloud storage source.
6. **Schedule pipelines:** Call the `/run` API from cron, Airflow, or your scheduler of choice.

---

Happy pipelining! 🦆
