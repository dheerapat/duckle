# Duckle — Agent Context

## What is this project

A lightweight, SQL-first ETL platform for small teams, powered by **DuckDB + DuckLake**. No clusters, no servers — just a single process that can run on a $20/month VM.

**Core data flow:**

```
Source (CSV / Postgres / in-table)
  → Extract (connectors)
  → Transform (DuckDB SQL, chained steps)
  → Quality (blocking / non-blocking checks)
  → Load (DuckLake bronze → silver → gold)
```

**Optional Write-Audit-Publish (WAP) flow:**

```
Extract → Transform → Write to staging → Quality Check → Publish to destination
```

When `wap_mode=True`, data is written to a persistent `staging` layer first, quality checks run against staging, and only on success is it published (CREATE OR REPLACE or MERGE) to the destination layer. Failed quality checks leave staging intact for debugging.

## Key design decisions

- **SQL-first transforms** — no custom DSL; users write plain DuckDB SQL with `{{input}}` or named source aliases
- **Single DuckDB connection** — connectors, transforms, and storage share one connection; no temp files or cross-connection transfers
- **SQL injection defence in depth** — `safe_identifier()` whitelist for table/column names, `safe_path()` for paths, `?` parameterised queries for values, manual validation for cursor values
- **Incremental via MERGE** — high-water mark cursor tracked per dataset; `MERGE INTO` for upsert; explicit `[from, to)` range support for backfill
- **Write-Audit-Publish (WAP)** — opt-in `wap_mode` on `Pipeline`; writes to `staging.{name}`, runs quality checks, then publishes to destination. Backward-compatible default (`wap_mode=False`).

## How to run things

```bash
# Setup
uv sync

# Source database (Docker)
docker compose up -d          # Postgres with restaurant seed data
docker compose down -v        # stop & wipe (re-seeds on next up)

# Tests
uv run pytest -v              # all tests (~100)
uv run pytest -k incremental  # incremental loading tests only
uv run pytest -k wap          # WAP pattern tests only

# Demo scripts (standalone, generate data + run full pipelines)
python scripts/example_csv.py
python scripts/example_postgres.py

# API server
uv run uvicorn api.main:app --reload --port 8000
```

Postgres connection string for the seeded database:
```
host=localhost port=5433 dbname=restaurant user=duckle password=duckle_secret
```

## Directory layout

```
src/
├── connectors/
│   ├── base.py                # Abstract BaseConnector (extract + test_connection)
│   ├── csv_connector.py       # read_csv_auto with parameterised path
│   ├── postgres_connector.py  # DuckDB postgres extension (ATTACH/DETACH)
│   └── table_connector.py     # Copy from existing DuckDB table (enables cross-layer pipelines)
├── engine/
│   └── transformer.py         # Chained SQL steps, {{input}} substitution
├── storage/
│   └── ducklake.py            # DuckLake bronze/silver/gold/staging schemas, MERGE, publish, cursor metadata
├── orchestrator/
│   └── runner.py              # Pipeline + PipelineRunner, incremental loading
├── quality/
│   └── checker.py             # QualityRule, built-in checks (no_nulls, min_row_count, etc.)
├── api/
│   └── main.py                # FastAPI REST endpoints
└── utils/
    └── sql_safety.py          # safe_identifier(), safe_path()

tests/
├── conftest.py                # duck_conn (in-memory), ducklake_storage (tmp_path), sample_csv
├── test_connectors.py         # CSV, Postgres, Table connectors + incremental ranges
├── test_ducklake.py           # write, merge, cursor, snapshots, cleanup
├── test_quality.py            # all built-in rules, blocking/non-blocking
├── test_runner.py             # full pipeline, incremental, multi-source, edge cases
├── test_transformer.py        # single/multi-step, preview, safety
└── test_sql_safety.py         # identifier + path validation

scripts/
├── example_csv.py             # Generate CSVs → bronze → silver → gold + incremental demo
└── example_postgres.py        # Postgres → bronze → silver → gold + incremental demo

seed/init/
├── 01_schema.sql              # Restaurant DB: menu_items, customers, orders, order_items
└── 02_seed_data.sql           # ~20 products, ~39 customers, ~2400 orders, ~6000 line items
```

## Source code conventions

- **Python 3.12+**, modern type hints (`list[str]` not `List[str]`)
- Imports: stdlib → third-party → local (blank line between groups)
- Docstrings: docstring on every public class and method
- Error handling: `ValueError` for validation; runner catches all exceptions and records in `run_history`
- Logging: `print()` with `[Module]` prefix (e.g. `[Transformer]`, `[Quality]`)
- SQL identifiers are **never** interpolated directly — always pass through `safe_identifier()`
- SQL values are **always** parameterised with `?` where DuckDB supports it

## Connector contract

Every connector must implement:

```python
class BaseConnector(ABC):
    @abstractmethod
    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """Extract data and register it as a DuckDB table named table_name."""

    @abstractmethod
    def test_connection(self) -> bool:
        """Verify the source is reachable."""

    def extract_incremental(self, conn, table_name, cursor_column, since_value,
                            until_value=None, from_is_explicit=False) -> None:
        """Default: full extract then filter. Override for native incremental."""
```

See `table_connector.py` or `postgres_connector.py` for examples of overriding `extract_incremental`.

## Pipeline model

```python
Pipeline(
    name="my_pipeline",
    source=SomeConnector(...)                # single source, alias is "input"
    # or:
    sources={"alias": Connector(...)},       # multi-source, aliases in SQL directly
    transforms=[
        "SELECT ... FROM {{input}}",         # single-source: {{input}} → "input"
        "SELECT ... FROM a JOIN b",          # multi-source: use aliases directly
    ],
    destination_name="my_dataset",
    destination_layer="gold",                # bronze | silver | gold
    quality_rules=[no_nulls("id"), ...],
    incremental=IncrementalConfig(
        cursor_column="order_date",
        merge_keys=["order_id"],
        from_value="2024-01-01",             # optional, inclusive
        to_value="2024-02-01",               # optional, exclusive
    ),
    wap_mode=False,                          # opt-in Write-Audit-Publish
)
```

## Medallion layers

| Layer | Purpose | Example |
|-------|---------|---------|
| Layer | Purpose | Example |
|-------|---------|---------|
| `bronze` | Raw staging, minimal transform | `SELECT * FROM {{input}}` |
| `silver` | Cleaned, validated, enriched, JOINs | Dedup, cast types, JOIN with reference tables |
| `gold` | Aggregated business insights | Daily revenue, customer LTV, product performance |
| `staging` | WAP-only transient layer | Holds data awaiting quality gate before publish |

## Incremental loading

The runner supports two incremental modes:

1. **Cursor-based resume** — stores `MAX(cursor_column)` after each run; next run extracts rows `> cursor`. Falls back to full load on first run (no prior cursor).

2. **Explicit `[from, to)` range** — set `from_value` (inclusive) and/or `to_value` (exclusive) on `IncrementalConfig`. Enables backfill by setting `from_value` below the stored cursor — MERGE deduplicates.

Full refresh (`full_refresh=True`) ignores all cursor/range logic and does `CREATE OR REPLACE TABLE`.

### WAP with incremental loading

WAP works with both incremental and full-refresh modes. In incremental + WAP:
1. Extract the delta using cursor/range
2. Transform
3. Write delta to `staging.{name}`
4. Run quality checks against staging
5. Publish via `MERGE INTO` (upsert) to destination
6. Update cursor

If quality fails, the staging table remains for inspection and the destination is untouched.

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Health ping |
| `GET` | `/health` | Health check |
| `POST` | `/run` | Execute a pipeline (body: `RunPipelineRequest`, supports `wap_mode`) |
| `GET` | `/runs` | Pipeline run history |
| `GET` | `/datasets` | List all DuckLake datasets |
| `GET` | `/datasets/{layer}/{name}/preview` | Preview rows |
| `GET` | `/pipelines/{name}/state` | Cursor + metadata for a dataset |

## What does not exist yet (see MILESTONE.md)

- Scheduler / cron triggers
- REST API connector
- Cloud storage connectors (S3/GCS)
- Declarative YAML pipeline definitions
- Monitoring/metrics endpoint
- RBAC / secret management
- React dashboard
