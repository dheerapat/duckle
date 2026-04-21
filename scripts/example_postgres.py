#!/usr/bin/env python3
"""
Duckle Postgres ETL integration script.

Extracts data from the Docker Compose PostgreSQL restaurant database and runs
a multi-layer ETL pipeline:

    Postgres  →  bronze (raw staging)  →  silver (cleaned / enriched)  →  gold (aggregated)

All pipelines are orchestrated through Pipeline / PipelineRunner.

Prerequisites:
    docker compose up -d       # start the seeded restaurant database

Usage:
    python scripts/example_postgres.py
"""

import os
import shutil
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from connectors.postgres_connector import PostgresConnector
from connectors.table_connector import TableConnector
from orchestrator.runner import Pipeline, PipelineRunner
from quality.checker import column_positive, min_row_count, no_nulls, unique_column
from storage.ducklake import DuckLakeStorage

# ── Config ────────────────────────────────────────────────────────────
PG_CONN = "host=localhost port=5433 dbname=restaurant user=duckle password=duckle_secret"


# ── Helpers ────────────────────────────────────────────────────────────


def wait_for_postgres(conn: str, timeout: int = 30) -> bool:
    """Poll until the Postgres connector can connect."""
    connector = PostgresConnector(conn, "SELECT 1")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if connector.test_connection():
            return True
        print("[wait] Postgres not ready yet, retrying in 2s...")
        time.sleep(2)
    return False


def _run_pipeline(runner: PipelineRunner, pipeline: Pipeline) -> dict:
    """Run a pipeline, assert success, and return the result."""
    result = runner.run(pipeline)
    assert result["status"] == "success", f"Pipeline '{pipeline.name}' failed: {result['error']}"
    return result


# ── Bronze layer ─────────────────────────────────────────────────────


def test_bronze_ingest() -> DuckLakeStorage:
    """Ingest all restaurant tables into the bronze layer via PipelineRunner."""
    print("\n" + "=" * 60)
    print("STEP 1 — Bronze: raw ingest from Postgres")
    print("=" * 60)

    for f in ["./ducklake.ducklake", "./ducklake.files", "./_metadata.db"]:
        if os.path.exists(f):
            shutil.rmtree(f) if os.path.isdir(f) else os.remove(f)

    storage = DuckLakeStorage()
    runner = PipelineRunner(storage=storage)

    tables = {
        "menu_items":  "SELECT * FROM menu_items",
        "customers":   "SELECT * FROM customers",
        "orders":       "SELECT * FROM orders",
        "order_items":  "SELECT * FROM order_items",
    }

    for name, query in tables.items():
        pipeline = Pipeline(
            name=f"pg_bronze_{name}",
            source=PostgresConnector(PG_CONN, query),
            transforms=["SELECT * FROM {{input}}"],
            destination_name=name,
            destination_layer="bronze",
        )
        _run_pipeline(runner, pipeline)

    print("\n  Bronze datasets:")
    for name in tables:
        df = storage.read(name, "bronze").fetchdf()
        print(f"    bronze.{name}: {len(df)} rows  columns={list(df.columns)}")

    print("✅ Bronze ingest complete")
    return storage


# ── Silver layer ─────────────────────────────────────────────────────


def test_silver_clean(storage: DuckLakeStorage) -> None:
    """Clean and validate data from bronze → silver."""
    print("\n" + "=" * 60)
    print("STEP 2 — Silver: clean & validate")
    print("=" * 60)

    runner = PipelineRunner(storage=storage)

    # ── menu_items: dedupe, compute margin ────────────────────────────
    print("\n  → menu_items")
    pipeline = Pipeline(
        name="pg_silver_menu_items",
        source=TableConnector("bronze.menu_items"),
        transforms=[
            """
            SELECT DISTINCT id AS menu_item_id, name, category, price,
                   ROUND(price * 0.35, 2) AS estimated_cost,
                   ROUND(price * 0.65, 2) AS estimated_margin
            FROM {{input}}
            WHERE price > 0
            """,
        ],
        destination_name="menu_items",
        destination_layer="silver",
        quality_rules=[no_nulls("menu_item_id"), unique_column("menu_item_id"), column_positive("price")],
    )
    _run_pipeline(runner, pipeline)

    # ── customers: dedupe, validate ──────────────────────────────────
    print("  → customers")
    pipeline = Pipeline(
        name="pg_silver_customers",
        source=TableConnector("bronze.customers"),
        transforms=[
            """
            SELECT DISTINCT id AS customer_id, name, email, phone,
                   joined_at, EXTRACT(MONTH FROM joined_at) AS join_month
            FROM {{input}}
            WHERE email IS NOT NULL
            """,
        ],
        destination_name="customers",
        destination_layer="silver",
        quality_rules=[no_nulls("customer_id"), no_nulls("email"),
                       unique_column("email"), min_row_count(30)],
    )
    _run_pipeline(runner, pipeline)

    # ── orders: dedupe, validate dates ────────────────────────────────
    print("  → orders")
    pipeline = Pipeline(
        name="pg_silver_orders",
        source=TableConnector("bronze.orders"),
        transforms=[
            """
            SELECT DISTINCT id AS order_id, order_date, customer_id,
                   EXTRACT(DOW FROM order_date) AS day_of_week,
                   CASE WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
            FROM {{input}}
            WHERE order_date IS NOT NULL
            """,
        ],
        destination_name="orders",
        destination_layer="silver",
        quality_rules=[no_nulls("order_id"), no_nulls("order_date"),
                       unique_column("order_id"), min_row_count(100)],
    )
    _run_pipeline(runner, pipeline)

    # ── order_items: enrich with price from silver.menu_items ─────────
    # This requires a JOIN across schemas, so we stage the join result
    # then feed it through PipelineRunner for the quality check + write.
    print("  → order_items")
    pipeline = Pipeline(
        name="pg_silver_order_items",
        sources={
            "oi": TableConnector("bronze.order_items"),
            "m": TableConnector("silver.menu_items"),
        },
        transforms=[
            """
            SELECT
                oi.id AS order_item_id,
                oi.order_id,
                oi.menu_item_id,
                oi.quantity,
                m.price AS unit_price,
                ROUND(oi.quantity * m.price, 2) AS line_total
            FROM oi
            JOIN m ON oi.menu_item_id = m.menu_item_id
            WHERE oi.quantity > 0
            """,
        ],
        destination_name="order_items",
        destination_layer="silver",
        quality_rules=[no_nulls("order_item_id"), no_nulls("order_id"),
                       column_positive("quantity"), column_positive("line_total"),
                       min_row_count(100)],
    )
    _run_pipeline(runner, pipeline)

    # Summary
    print("\n  Silver datasets:")
    for name in ["menu_items", "customers", "orders", "order_items"]:
        df = storage.read(name, "silver").fetchdf()
        print(f"    silver.{name}: {len(df)} rows")

    print("✅ Silver clean complete")


# ── Gold layer ────────────────────────────────────────────────────────


def test_gold_aggregates(storage: DuckLakeStorage) -> None:
    """Build aggregated gold-layer tables from silver."""
    print("\n" + "=" * 60)
    print("STEP 3 — Gold: aggregated insights")
    print("=" * 60)

    runner = PipelineRunner(storage=storage)

    # ── daily_category_revenue ─────────────────────────────────────────
    print("\n  → daily_category_revenue")
    pipeline = Pipeline(
        name="gold_daily_revenue",
        sources={
            "o": TableConnector("silver.orders"),
            "oi": TableConnector("silver.order_items"),
            "m": TableConnector("silver.menu_items"),
        },
        transforms=[
            """
            SELECT
                o.order_date,
                m.category,
                SUM(oi.line_total)  AS revenue,
                SUM(oi.quantity)     AS items_sold,
                COUNT(DISTINCT o.customer_id) AS unique_customers,
                COUNT(*)             AS order_lines
            FROM o
            JOIN oi ON o.order_id = oi.order_id
            JOIN m ON oi.menu_item_id = m.menu_item_id
            GROUP BY o.order_date, m.category
            """,
            """
            SELECT *,
                CASE
                    WHEN revenue >= 300 THEN 'high'
                    WHEN revenue >= 100 THEN 'medium'
                    ELSE 'low'
                END AS revenue_tier,
                ROUND(revenue / NULLIF(items_sold, 0), 2) AS avg_item_price
            FROM {{input}}
            """,
        ],
        destination_name="daily_category_revenue",
        destination_layer="gold",
        quality_rules=[no_nulls("order_date"), no_nulls("category"),
                       column_positive("revenue"), min_row_count(10)],
    )
    _run_pipeline(runner, pipeline)

    # ── customer_spending (lifetime value) ────────────────────────────
    print("  → customer_spending")
    pipeline = Pipeline(
        name="gold_customer_spending",
        sources={
            "o": TableConnector("silver.orders"),
            "oi": TableConnector("silver.order_items"),
            "c": TableConnector("silver.customers"),
        },
        transforms=[
            """
            SELECT
                c.customer_id,
                c.name           AS customer_name,
                c.email,
                EXTRACT(MONTH FROM c.joined_at) AS join_month,
                COUNT(DISTINCT o.order_id) AS total_orders,
                SUM(oi.line_total)         AS total_spent,
                ROUND(AVG(oi.line_total), 2) AS avg_order_value,
                MIN(o.order_date)          AS first_order,
                MAX(o.order_date)          AS last_order,
                DATEDIFF('day', MIN(o.order_date), MAX(o.order_date)) AS active_days
            FROM o
            JOIN oi ON o.order_id = oi.order_id
            JOIN c ON o.customer_id = c.customer_id
            GROUP BY c.customer_id, c.name, c.email, c.joined_at
            """,
            """
            SELECT *,
                CASE
                    WHEN total_spent >= 500 THEN 'platinum'
                    WHEN total_spent >= 200 THEN 'gold'
                    WHEN total_spent >= 50  THEN 'silver'
                    ELSE 'bronze'
                END AS loyalty_tier,
                CASE
                    WHEN active_days > 90 THEN 'loyal'
                    WHEN active_days > 30 THEN 'regular'
                    ELSE 'new'
                END AS customer_segment
            FROM {{input}}
            """,
        ],
        destination_name="customer_spending",
        destination_layer="gold",
        quality_rules=[no_nulls("customer_id"), unique_column("customer_id"), min_row_count(10)],
    )
    _run_pipeline(runner, pipeline)

    # ── menu_item_performance ─────────────────────────────────────────
    print("  → menu_item_performance")
    pipeline = Pipeline(
        name="gold_menu_item_performance",
        sources={
            "m": TableConnector("silver.menu_items"),
            "oi": TableConnector("silver.order_items"),
        },
        transforms=[
            """
            SELECT
                m.menu_item_id,
                m.name           AS item_name,
                m.category,
                m.estimated_margin,
                SUM(oi.quantity)     AS total_sold,
                SUM(oi.line_total)   AS total_revenue,
                COUNT(*)              AS times_ordered
            FROM m
            JOIN oi ON oi.menu_item_id = m.menu_item_id
            GROUP BY m.menu_item_id, m.name, m.category, m.estimated_margin
            """,
            """
            SELECT *,
                ROUND(estimated_margin * total_sold, 2) AS total_margin,
                CASE
                    WHEN total_sold >= 200 THEN 'top_seller'
                    WHEN total_sold >= 80  THEN 'steady'
                    ELSE 'slow_mover'
                END AS sales_velocity
            FROM {{input}}
            """,
        ],
        destination_name="menu_item_performance",
        destination_layer="gold",
        quality_rules=[no_nulls("menu_item_id"), unique_column("menu_item_id"),
                       column_positive("total_revenue")],
    )
    _run_pipeline(runner, pipeline)

    # ── weekend_vs_weekday ─────────────────────────────────────────────
    print("  → weekend_vs_weekday")
    pipeline = Pipeline(
        name="gold_weekend_vs_weekday",
        sources={
            "o": TableConnector("silver.orders"),
            "oi": TableConnector("silver.order_items"),
        },
        transforms=[
            """
            SELECT
                o.is_weekend,
                COUNT(DISTINCT o.order_id)  AS total_orders,
                SUM(oi.line_total)          AS total_revenue,
                ROUND(AVG(oi.line_total), 2) AS avg_line_value,
                SUM(oi.quantity)            AS total_items,
                COUNT(*)                     AS total_lines
            FROM o
            JOIN oi ON o.order_id = oi.order_id
            GROUP BY o.is_weekend
            """,
            """
            SELECT *,
                ROUND(total_revenue / NULLIF(total_orders, 0), 2) AS revenue_per_order,
                ROUND(total_items / NULLIF(total_orders, 0), 2)   AS items_per_order
            FROM {{input}}
            """,
        ],
        destination_name="weekend_vs_weekday",
        destination_layer="gold",
        quality_rules=[min_row_count(2)],
    )
    _run_pipeline(runner, pipeline)

    print("✅ Gold aggregates complete")


# ── Preview ────────────────────────────────────────────────────────────


def test_preview_all(storage: DuckLakeStorage) -> None:
    """Print a summary of all datasets across all layers."""
    print("\n" + "=" * 60)
    print("FINAL — All datasets in DuckLake")
    print("=" * 60)

    datasets = storage.list_datasets()
    print(f"\n  {'Layer':<10} {'Name':<30} {'Rows':>6}  Pipeline")
    print(f"  {'─'*10} {'─'*30} {'─'*6}  {'─'*30}")
    for ds in datasets:
        print(f"  {ds['layer']:<10} {ds['name']:<30} {ds['row_count']:>6}  {ds.get('pipeline', '—')}")

    for name in ["daily_category_revenue", "customer_spending", "menu_item_performance", "weekend_vs_weekday"]:
        try:
            df = storage.read(name, "gold").fetchdf()
            print(f"\n  gold.{name} — {len(df)} rows, {len(df.columns)} columns")
            print(f"  columns: {list(df.columns)}")
            print(df.head(5).to_string(index=False))
        except Exception as e:
            print(f"\n  gold.{name} — error: {e}")


# ── Main ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🦆 Duckle Postgres ETL Integration Script")
    print("=" * 60)

    if not wait_for_postgres(PG_CONN):
        print("\n❌ Cannot connect to PostgreSQL.")
        print("   Run:  docker compose up -d")
        sys.exit(1)

    try:
        storage = test_bronze_ingest()
        test_silver_clean(storage)
        test_gold_aggregates(storage)
        test_preview_all(storage)
    except Exception as e:
        print(f"\n❌ Failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        try:
            storage.close()
        except Exception:
            pass

    print("\n" + "=" * 60)
    print("🎉 Postgres ETL pipeline completed successfully!")
    print("=" * 60)
