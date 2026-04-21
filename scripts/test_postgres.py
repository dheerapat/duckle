#!/usr/bin/env python3
"""
Duckle Postgres ETL integration script.

Extracts data from the Docker Compose PostgreSQL restaurant database and runs
a multi-layer ETL pipeline:

    Postgres  →  bronze (raw staging)  →  silver (cleaned / enriched)  →  gold (aggregated)

Prerequisites:
    docker compose up -d       # start the seeded restaurant database

Usage:
    python scripts/test_postgres.py
"""

import os
import shutil
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from connectors.postgres_connector import PostgresConnector
from engine.transformer import Transformer
from orchestrator.runner import Pipeline, PipelineRunner
from quality.checker import QualityChecker, column_positive, min_row_count, no_nulls, unique_column
from storage.ducklake import DuckLakeStorage

# ── Config ────────────────────────────────────────────────────────────
PG_CONN = "host=localhost port=5433 dbname=restaurant user=duckle password=duckle_secret"
LAKE_DIR = os.path.join(os.path.dirname(__file__), "..", "lake_test_pg")


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


def _create_staging(conn, sql: str, table_name: str) -> str:
    """Execute a CREATE OR REPLACE TABLE from arbitrary SQL and return the table name."""
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    count = result[0] if result is not None else 0
    print(f"    Staging: {count} rows → {table_name}")
    return table_name


def _extract_to_bronze(storage: DuckLakeStorage, query: str, name: str) -> None:
    """Extract from Postgres into the bronze layer."""
    conn = storage.conn
    connector = PostgresConnector(PG_CONN, query)
    connector.extract(conn, "raw")
    storage.write("raw", name=name, layer="bronze", pipeline="pg_bronze_ingest")
    conn.execute("DROP TABLE IF EXISTS raw")


# ── Bronze layer ─────────────────────────────────────────────────────


def test_bronze_ingest() -> DuckLakeStorage:
    """Ingest all restaurant tables into the bronze layer."""
    print("\n" + "=" * 60)
    print("STEP 1 — Bronze: raw ingest from Postgres")
    print("=" * 60)

    if os.path.exists(LAKE_DIR):
        shutil.rmtree(LAKE_DIR)

    storage = DuckLakeStorage(base_path=LAKE_DIR)

    _extract_to_bronze(storage, "SELECT * FROM menu_items", "menu_items")
    _extract_to_bronze(storage, "SELECT * FROM customers", "customers")
    _extract_to_bronze(storage, "SELECT * FROM orders", "orders")
    _extract_to_bronze(storage, "SELECT * FROM order_items", "order_items")

    print("\n  Bronze datasets:")
    for name in ["menu_items", "customers", "orders", "order_items"]:
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

    conn = storage.conn
    transformer = Transformer(conn)
    checker = QualityChecker(conn)

    # ── menu_items: dedupe, compute margin ────────────────────────────
    print("\n  → menu_items")
    conn.execute("CREATE OR REPLACE TABLE stg__menu_items AS SELECT * FROM bronze.menu_items")
    silver_menu = transformer.run([
        """
        SELECT DISTINCT id AS menu_item_id, name, category, price,
               ROUND(price * 0.35, 2) AS estimated_cost,
               ROUND(price * 0.65, 2) AS estimated_margin
        FROM {{input}}
        WHERE price > 0
        """,
    ], source_table="stg__menu_items")
    result = checker.check(silver_menu, [
        no_nulls("menu_item_id"), unique_column("menu_item_id"),
        column_positive("price"),
    ])
    assert result["passed"], "Silver menu_items quality check failed"
    storage.write(silver_menu, name="menu_items", layer="silver", pipeline="pg_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__menu_items")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── customers: dedupe, validate ──────────────────────────────────
    print("  → customers")
    conn.execute("CREATE OR REPLACE TABLE stg__customers AS SELECT * FROM bronze.customers")
    silver_customers = transformer.run([
        """
        SELECT DISTINCT id AS customer_id, name, email, phone,
               joined_at, EXTRACT(MONTH FROM joined_at) AS join_month
        FROM {{input}}
        WHERE email IS NOT NULL
        """,
    ], source_table="stg__customers")
    result = checker.check(silver_customers, [
        no_nulls("customer_id"), no_nulls("email"),
        unique_column("email"), min_row_count(30),
    ])
    assert result["passed"], "Silver customers quality check failed"
    storage.write(silver_customers, name="customers", layer="silver", pipeline="pg_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__customers")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── orders: dedupe, validate dates ────────────────────────────────
    print("  → orders")
    conn.execute("CREATE OR REPLACE TABLE stg__orders AS SELECT * FROM bronze.orders")
    silver_orders = transformer.run([
        """
        SELECT DISTINCT id AS order_id, order_date, customer_id,
               EXTRACT(DOW FROM order_date) AS day_of_week,
               CASE WHEN EXTRACT(DOW FROM order_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
        FROM {{input}}
        WHERE order_date IS NOT NULL
        """,
    ], source_table="stg__orders")
    result = checker.check(silver_orders, [
        no_nulls("order_id"), no_nulls("order_date"),
        unique_column("order_id"), min_row_count(100),
    ])
    assert result["passed"], "Silver orders quality check failed"
    storage.write(silver_orders, name="orders", layer="silver", pipeline="pg_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__orders")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── order_items: enrich with price from silver.menu_items ─────────
    print("  → order_items")
    _create_staging(conn, """
        SELECT
            oi.id AS order_item_id,
            oi.order_id,
            oi.menu_item_id,
            oi.quantity,
            m.price AS unit_price,
            ROUND(oi.quantity * m.price, 2) AS line_total
        FROM bronze.order_items AS oi
        JOIN silver.menu_items AS m ON oi.menu_item_id = m.menu_item_id
        WHERE oi.quantity > 0
    """, "stg__order_items")
    result = checker.check("stg__order_items", [
        no_nulls("order_item_id"), no_nulls("order_id"),
        column_positive("quantity"), column_positive("line_total"),
        min_row_count(100),
    ])
    assert result["passed"], "Silver order_items quality check failed"
    storage.write("stg__order_items", name="order_items", layer="silver", pipeline="pg_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__order_items")

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

    conn = storage.conn
    transformer = Transformer(conn)
    checker = QualityChecker(conn)

    # ── daily_category_revenue ─────────────────────────────────────────
    print("\n  → daily_category_revenue")
    _create_staging(conn, """
        SELECT
            o.order_date,
            m.category,
            SUM(oi.line_total)  AS revenue,
            SUM(oi.quantity)     AS items_sold,
            COUNT(DISTINCT o.customer_id) AS unique_customers,
            COUNT(*)             AS order_lines
        FROM silver.orders AS o
        JOIN silver.order_items AS oi ON o.order_id = oi.order_id
        JOIN silver.menu_items  AS m ON oi.menu_item_id = m.menu_item_id
        GROUP BY o.order_date, m.category
    """, "stg__dcr")

    dcr = transformer.run([
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
    ], source_table="stg__dcr")
    result = checker.check(dcr, [
        no_nulls("order_date"), no_nulls("category"),
        column_positive("revenue"), min_row_count(10),
    ])
    assert result["passed"], "Gold daily_category_revenue quality check failed"
    storage.write(dcr, name="daily_category_revenue", layer="gold", pipeline="gold_daily_revenue")
    conn.execute("DROP TABLE IF EXISTS stg__dcr")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── customer_spending (lifetime value) ────────────────────────────
    print("  → customer_spending")
    _create_staging(conn, """
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
        FROM silver.orders AS o
        JOIN silver.order_items AS oi ON o.order_id = oi.order_id
        JOIN silver.customers   AS c ON o.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name, c.email, c.joined_at
    """, "stg__cs")

    cs = transformer.run([
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
    ], source_table="stg__cs")
    result = checker.check(cs, [
        no_nulls("customer_id"), unique_column("customer_id"), min_row_count(10),
    ])
    assert result["passed"], "Gold customer_spending quality check failed"
    storage.write(cs, name="customer_spending", layer="gold", pipeline="gold_customer_spending")
    conn.execute("DROP TABLE IF EXISTS stg__cs")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── menu_item_performance ─────────────────────────────────────────
    print("  → menu_item_performance")
    _create_staging(conn, """
        SELECT
            m.menu_item_id,
            m.name           AS item_name,
            m.category,
            m.estimated_margin,
            SUM(oi.quantity)     AS total_sold,
            SUM(oi.line_total)   AS total_revenue,
            COUNT(*)              AS times_ordered
        FROM silver.menu_items   AS m
        JOIN silver.order_items  AS oi ON oi.menu_item_id = m.menu_item_id
        GROUP BY m.menu_item_id, m.name, m.category, m.estimated_margin
    """, "stg__mip")

    mip = transformer.run([
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
    ], source_table="stg__mip")
    result = checker.check(mip, [
        no_nulls("menu_item_id"), unique_column("menu_item_id"),
        column_positive("total_revenue"),
    ])
    assert result["passed"], "Gold menu_item_performance quality check failed"
    storage.write(mip, name="menu_item_performance", layer="gold", pipeline="gold_menu_item_performance")
    conn.execute("DROP TABLE IF EXISTS stg__mip")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── weekend_vs_weekday ─────────────────────────────────────────────
    print("  → weekend_vs_weekday")
    _create_staging(conn, """
        SELECT
            o.is_weekend,
            COUNT(DISTINCT o.order_id)  AS total_orders,
            SUM(oi.line_total)          AS total_revenue,
            ROUND(AVG(oi.line_total), 2) AS avg_line_value,
            SUM(oi.quantity)            AS total_items,
            COUNT(*)                     AS total_lines
        FROM silver.orders      AS o
        JOIN silver.order_items AS oi ON o.order_id = oi.order_id
        GROUP BY o.is_weekend
    """, "stg__wvw")

    wvw = transformer.run([
        """
        SELECT *,
            ROUND(total_revenue / NULLIF(total_orders, 0), 2) AS revenue_per_order,
            ROUND(total_items / NULLIF(total_orders, 0), 2)   AS items_per_order
        FROM {{input}}
        """,
    ], source_table="stg__wvw")
    result = checker.check(wvw, [
        min_row_count(2),
    ])
    assert result["passed"], "Gold weekend_vs_weekday quality check failed"
    storage.write(wvw, name="weekend_vs_weekday", layer="gold", pipeline="gold_weekend_vs_weekday")
    conn.execute("DROP TABLE IF EXISTS stg__wvw")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

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