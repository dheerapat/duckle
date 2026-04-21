#!/usr/bin/env python3
"""
Duckle CSV ETL integration script.

Generates realistic multi-file CSV data, then runs a multi-layer ETL pipeline:

    raw CSVs  →  bronze (staging)  →  silver (cleaned)  →  gold (aggregated)

Usage:
    python scripts/test_csv.py
"""

import csv
import os
import random
import shutil
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from connectors.csv_connector import CSVConnector
from engine.transformer import Transformer
from quality.checker import QualityChecker, QualityRule, column_positive, min_row_count, no_nulls, unique_column
from storage.ducklake import DuckLakeStorage

# ── Paths ─────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data_test_csv")
LAKE_DIR = os.path.join(BASE_DIR, "lake_test_csv")


def _write_csv(path: str, header: list[str], rows: list[list]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


# ── Data generation ───────────────────────────────────────────────────


def generate_data() -> dict[str, str]:
    """Create realistic multi-file CSV data and return {name: path}."""
    random.seed(42)

    # ── products.csv ──────────────────────────────────────────────────
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
    products = []
    for i in range(1, 26):
        cat = categories[(i - 1) // 5]
        cost = round(random.uniform(5, 80), 2)
        price = round(cost * random.uniform(1.4, 2.2), 2)
        products.append([i, f"Product-{i:02d}", cat, cost, price])
    products_path = os.path.join(DATA_DIR, "products.csv")
    _write_csv(products_path, ["product_id", "name", "category", "cost", "price"], products)

    # ── customers.csv ─────────────────────────────────────────────────
    regions = ["North", "South", "East", "West"]
    customers = []
    for i in range(1, 51):
        joined = date(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        customers.append([
            i, f"Customer-{i:03d}", f"cust{i}@example.com",
            regions[i % 4], joined.isoformat(),
        ])
    customers_path = os.path.join(DATA_DIR, "customers.csv")
    _write_csv(customers_path, ["customer_id", "name", "email", "region", "joined_at"], customers)

    # ── order_lines.csv ────────────────────────────────────────────────
    # 3 months of daily orders, more on weekends
    order_lines = []
    line_id = 1
    for day_offset in range(90):
        order_date = date(2024, 7, 1) + timedelta(days=day_offset)
        is_weekend = order_date.weekday() >= 5
        num_orders = random.randint(13, 20) if is_weekend else random.randint(8, 15)

        for _ in range(num_orders):
            customer_id = random.randint(1, 50)
            num_lines = random.randint(1, 4)
            for _ in range(num_lines):
                product_id = random.randint(1, 25)
                qty = random.randint(1, 5)
                discount = random.choice([0, 0, 0, 5, 10, 15])
                order_lines.append([
                    line_id, order_date.isoformat(), customer_id,
                    product_id, qty, discount,
                ])
                line_id += 1

    lines_path = os.path.join(DATA_DIR, "order_lines.csv")
    _write_csv(lines_path, ["line_id", "order_date", "customer_id", "product_id", "quantity", "discount_pct"], order_lines)

    # ── returns.csv ───────────────────────────────────────────────────
    returns = []
    for i in range(1, random.randint(30, 60) + 1):
        line = random.choice(order_lines)
        return_date = date.fromisoformat(line[1]) + timedelta(days=random.randint(1, 14))
        reason = random.choice(["defective", "wrong_size", "changed_mind", "late_delivery"])
        refund = round(random.uniform(5, 50), 2)
        returns.append([i, line[0], return_date.isoformat(), reason, refund])
    returns_path = os.path.join(DATA_DIR, "returns.csv")
    _write_csv(returns_path, ["return_id", "line_id", "return_date", "reason", "refund_amount"], returns)

    print("Generated CSV data:")
    print(f"  products:     {len(products):>5} rows")
    print(f"  customers:    {len(customers):>5} rows")
    print(f"  order_lines:  {len(order_lines):>5} rows")
    print(f"  returns:      {len(returns):>5} rows")

    return {
        "products": products_path,
        "customers": customers_path,
        "order_lines": lines_path,
        "returns": returns_path,
    }


# ── Helpers ────────────────────────────────────────────────────────────

def _create_staging(conn, sql: str, table_name: str) -> str:
    """Execute a CREATE OR REPLACE TABLE from arbitrary SQL and return the table name.

    This is used for multi-table joins that reference DuckLake schemas (e.g. bronze.*, silver.*),
    which can't go through Transformer's {{input}} placeholder since those identifiers contain dots.
    """
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    count = result[0] if result is not None else 0
    print(f"    Staging: {count} rows → {table_name}")
    return table_name


# ── Bronze layer ─────────────────────────────────────────────────────


def test_bronze_ingest(paths: dict[str, str]) -> DuckLakeStorage:
    """Ingest all CSVs into the bronze layer (raw staging)."""
    print("\n" + "=" * 60)
    print("STEP 1 — Bronze: raw ingest")
    print("=" * 60)

    if os.path.exists(LAKE_DIR):
        shutil.rmtree(LAKE_DIR)

    storage = DuckLakeStorage(base_path=LAKE_DIR)
    conn = storage.conn

    for name, path in paths.items():
        connector = CSVConnector(path)
        connector.extract(conn, "raw")
        storage.write("raw", name=name, layer="bronze", pipeline="csv_bronze_ingest")
        conn.execute("DROP TABLE IF EXISTS raw")

    print("\n  Bronze datasets:")
    for name in paths:
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

    # ── products: dedupe, filter, compute margin ──────────────────────
    print("\n  → products")
    conn.execute("CREATE OR REPLACE TABLE stg__products AS SELECT * FROM bronze.products")
    silver_products = transformer.run([
        """
        SELECT DISTINCT product_id, name, category,
               cost, price, ROUND(price - cost, 2) AS margin
        FROM {{input}}
        WHERE product_id IS NOT NULL AND price > 0 AND cost > 0
        """,
    ], source_table="stg__products")
    result = checker.check(silver_products, [
        no_nulls("product_id"), unique_column("product_id"),
        column_positive("price"), column_positive("cost"),
    ])
    assert result["passed"], "Silver products quality check failed"
    storage.write(silver_products, name="products", layer="silver", pipeline="csv_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__products")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── customers: dedupe, cast date ──────────────────────────────────
    print("  → customers")
    conn.execute("CREATE OR REPLACE TABLE stg__customers AS SELECT * FROM bronze.customers")
    silver_customers = transformer.run([
        """
        SELECT DISTINCT customer_id, name, email, region,
               CAST(joined_at AS DATE) AS joined_at
        FROM {{input}}
        WHERE customer_id IS NOT NULL AND email IS NOT NULL
        """,
    ], source_table="stg__customers")
    result = checker.check(silver_customers, [
        no_nulls("customer_id"), no_nulls("email"), unique_column("customer_id"),
    ])
    assert result["passed"], "Silver customers quality check failed"
    storage.write(silver_customers, name="customers", layer="silver", pipeline="csv_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__customers")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── order_lines: enrich with price from silver.products ────────────
    # This requires a JOIN across schemas, so we use direct SQL
    print("  → order_lines")
    _create_staging(conn, """
        SELECT
            ol.line_id,
            CAST(ol.order_date AS DATE) AS order_date,
            ol.customer_id,
            ol.product_id,
            ol.quantity,
            ol.discount_pct,
            p.price AS unit_price,
            ROUND(ol.quantity * p.price, 2) AS gross_amount,
            ROUND(ol.quantity * p.price * (1 - ol.discount_pct / 100.0), 2) AS net_amount
        FROM bronze.order_lines AS ol
        JOIN silver.products AS p ON ol.product_id = p.product_id
        WHERE ol.quantity > 0 AND ol.discount_pct >= 0 AND ol.discount_pct <= 100
    """, "stg__order_lines")
    result = checker.check("stg__order_lines", [
        no_nulls("line_id"), no_nulls("order_date"),
        column_positive("net_amount"), min_row_count(100),
    ])
    assert result["passed"], "Silver order_lines quality check failed"
    storage.write("stg__order_lines", name="order_lines", layer="silver", pipeline="csv_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__order_lines")

    # ── returns: dedupe, filter, cast date ────────────────────────────
    print("  → returns")
    conn.execute("CREATE OR REPLACE TABLE stg__returns AS SELECT * FROM bronze.returns")
    silver_returns = transformer.run([
        """
        SELECT DISTINCT return_id, line_id,
               CAST(return_date AS DATE) AS return_date,
               reason, refund_amount
        FROM {{input}}
        WHERE refund_amount > 0
        """,
    ], source_table="stg__returns")
    result = checker.check(silver_returns, [
        no_nulls("return_id"), no_nulls("reason"), column_positive("refund_amount"),
    ])
    assert result["passed"], "Silver returns quality check failed"
    storage.write(silver_returns, name="returns", layer="silver", pipeline="csv_silver_clean")
    conn.execute("DROP TABLE IF EXISTS stg__returns")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # Summary
    print("\n  Silver datasets:")
    for name in ["products", "customers", "order_lines", "returns"]:
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
            ol.order_date,
            p.category,
            SUM(ol.net_amount)   AS revenue,
            SUM(ol.quantity)      AS items_sold,
            COUNT(DISTINCT ol.customer_id) AS unique_customers,
            COUNT(*)              AS order_lines
        FROM silver.order_lines AS ol
        JOIN silver.products  AS p ON ol.product_id = p.product_id
        GROUP BY ol.order_date, p.category
    """, "stg__dcr")

    dcr = transformer.run([
        """
        SELECT *,
            CASE
                WHEN revenue >= 200 THEN 'high'
                WHEN revenue >= 50  THEN 'medium'
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

    # ── customer_lifetime_value ────────────────────────────────────────
    print("  → customer_lifetime_value")
    _create_staging(conn, """
        SELECT
            c.customer_id,
            c.name             AS customer_name,
            c.region,
            COUNT(*)           AS total_orders,
            SUM(ol.net_amount) AS total_spent,
            ROUND(AVG(ol.net_amount), 2) AS avg_order_value,
            MIN(ol.order_date) AS first_order,
            MAX(ol.order_date) AS last_order,
            DATEDIFF('day', MIN(ol.order_date), MAX(ol.order_date)) AS active_days
        FROM silver.order_lines AS ol
        JOIN silver.customers  AS c ON ol.customer_id = c.customer_id
        GROUP BY c.customer_id, c.name, c.region
    """, "stg__clv")

    clv = transformer.run([
        """
        SELECT *,
            CASE
                WHEN total_spent >= 500 THEN 'platinum'
                WHEN total_spent >= 200 THEN 'gold'
                WHEN total_spent >= 50  THEN 'silver'
                ELSE 'bronze'
            END AS loyalty_tier,
            CASE
                WHEN active_days > 30 THEN 'loyal'
                WHEN active_days > 7  THEN 'regular'
                ELSE 'new'
            END AS customer_segment
        FROM {{input}}
        """,
    ], source_table="stg__clv")
    result = checker.check(clv, [
        no_nulls("customer_id"), unique_column("customer_id"), min_row_count(10),
    ])
    assert result["passed"], "Gold customer_lifetime_value quality check failed"
    storage.write(clv, name="customer_lifetime_value", layer="gold", pipeline="gold_customer_clv")
    conn.execute("DROP TABLE IF EXISTS stg__clv")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── product_performance ────────────────────────────────────────────
    print("  → product_performance")
    _create_staging(conn, """
        SELECT
            p.product_id,
            p.name           AS product_name,
            p.category,
            p.margin,
            SUM(ol.quantity)     AS total_sold,
            SUM(ol.net_amount)   AS total_revenue,
            COUNT(*)              AS times_ordered,
            ROUND(p.margin * SUM(ol.quantity), 2) AS total_margin
        FROM silver.products    AS p
        JOIN silver.order_lines AS ol ON ol.product_id = p.product_id
        GROUP BY p.product_id, p.name, p.category, p.margin
    """, "stg__pp")

    pp = transformer.run([
        """
        SELECT *,
            ROUND(total_margin / NULLIF(total_revenue, 0) * 100, 2) AS margin_pct,
            CASE
                WHEN total_sold >= 50  THEN 'top_seller'
                WHEN total_sold >= 20  THEN 'steady'
                ELSE 'slow_mover'
            END AS sales_velocity
        FROM {{input}}
        """,
    ], source_table="stg__pp")
    result = checker.check(pp, [
        no_nulls("product_id"), unique_column("product_id"), column_positive("total_revenue"),
    ])
    assert result["passed"], "Gold product_performance quality check failed"
    storage.write(pp, name="product_performance", layer="gold", pipeline="gold_product_performance")
    conn.execute("DROP TABLE IF EXISTS stg__pp")
    conn.execute("DROP TABLE IF EXISTS _transform_step_0")

    # ── regional_summary ───────────────────────────────────────────────
    print("  → regional_summary")
    _create_staging(conn, """
        SELECT
            c.region,
            COUNT(DISTINCT c.customer_id)            AS customers,
            SUM(ol.net_amount)                        AS total_revenue,
            ROUND(AVG(ol.net_amount), 2)              AS avg_order_value,
            COUNT(*)                                   AS total_lines,
            SUM(ol.quantity)                           AS total_items
        FROM silver.order_lines AS ol
        JOIN silver.customers AS c ON ol.customer_id = c.customer_id
        GROUP BY c.region
    """, "stg__rs")

    rs = transformer.run([
        """
        SELECT *,
            ROUND(total_revenue / NULLIF(customers, 0), 2) AS revenue_per_customer,
            ROUND(total_items / NULLIF(customers, 0), 2)   AS items_per_customer
        FROM {{input}}
        """,
    ], source_table="stg__rs")
    result = checker.check(rs, [
        no_nulls("region"), unique_column("region"), min_row_count(4),
    ])
    assert result["passed"], "Gold regional_summary quality check failed"
    storage.write(rs, name="regional_summary", layer="gold", pipeline="gold_regional_summary")
    conn.execute("DROP TABLE IF EXISTS stg__rs")
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

    for name in ["daily_category_revenue", "customer_lifetime_value", "product_performance", "regional_summary"]:
        try:
            df = storage.read(name, "gold").fetchdf()
            print(f"\n  gold.{name} — {len(df)} rows, {len(df.columns)} columns")
            print(f"  columns: {list(df.columns)}")
            print(df.head(5).to_string(index=False))
        except Exception as e:
            print(f"\n  gold.{name} — error: {e}")


# ── Main ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    for path in [DATA_DIR, LAKE_DIR]:
        if os.path.exists(path):
            shutil.rmtree(path)

    print("🦆 Duckle CSV ETL Integration Script")
    print("=" * 60)

    print("\nGenerating CSV data...")
    paths = generate_data()

    try:
        storage = test_bronze_ingest(paths)
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
    print("🎉 CSV ETL pipeline completed successfully!")
    print("=" * 60)