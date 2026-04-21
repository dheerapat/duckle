"""End-to-end pipeline tests for the CSV ETL pipeline.

Uses Pipeline / PipelineRunner for all stages, mirroring
scripts/test_csv.py.
"""

import csv
import os
import sys
import random
from datetime import date, timedelta

import pytest

sys_path = os.path.join(os.path.dirname(__file__), "..", "src")
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from connectors.csv_connector import CSVConnector
from connectors.table_connector import TableConnector
from orchestrator.runner import Pipeline, PipelineRunner
from quality.checker import column_positive, min_row_count, no_nulls, unique_column
from storage.ducklake import DuckLakeStorage


# ── Helpers ────────────────────────────────────────────────────────────


def _write_csv(path: str, header: list[str], rows: list[list]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def _create_staging(conn, sql: str, table_name: str) -> str:
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    count = result[0] if result is not None else 0
    return table_name


def _run_pipeline(runner: PipelineRunner, pipeline: Pipeline) -> dict:
    result = runner.run(pipeline)
    assert result["status"] == "success", f"Pipeline '{pipeline.name}' failed: {result['error']}"
    return result


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def csv_data_dir(tmp_path_factory):
    """Generate realistic multi-file CSV data into a temp directory."""
    data_dir = str(tmp_path_factory.mktemp("csv_data"))
    random.seed(42)

    categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
    products = []
    for i in range(1, 26):
        cat = categories[(i - 1) // 5]
        cost = round(random.uniform(5, 80), 2)
        price = round(cost * random.uniform(1.4, 2.2), 2)
        products.append([i, f"Product-{i:02d}", cat, cost, price])
    products_path = os.path.join(data_dir, "products.csv")
    _write_csv(products_path, ["product_id", "name", "category", "cost", "price"], products)

    regions = ["North", "South", "East", "West"]
    customers = []
    for i in range(1, 51):
        joined = date(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        customers.append([
            i, f"Customer-{i:03d}", f"cust{i}@example.com",
            regions[i % 4], joined.isoformat(),
        ])
    customers_path = os.path.join(data_dir, "customers.csv")
    _write_csv(customers_path, ["customer_id", "name", "email", "region", "joined_at"], customers)

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
    lines_path = os.path.join(data_dir, "order_lines.csv")
    _write_csv(lines_path, ["line_id", "order_date", "customer_id", "product_id", "quantity", "discount_pct"], order_lines)

    returns = []
    for i in range(1, random.randint(30, 60) + 1):
        line = random.choice(order_lines)
        return_date = date.fromisoformat(line[1]) + timedelta(days=random.randint(1, 14))
        reason = random.choice(["defective", "wrong_size", "changed_mind", "late_delivery"])
        refund = round(random.uniform(5, 50), 2)
        returns.append([i, line[0], return_date.isoformat(), reason, refund])
    returns_path = os.path.join(data_dir, "returns.csv")
    _write_csv(returns_path, ["return_id", "line_id", "return_date", "reason", "refund_amount"], returns)

    yield {
        "products": products_path,
        "customers": customers_path,
        "order_lines": lines_path,
        "returns": returns_path,
        "data_dir": data_dir,
    }


@pytest.fixture(scope="module")
def csv_lake(tmp_path_factory):
    """Provide a DuckLakeStorage for the CSV e2e pipeline."""
    lake_dir = str(tmp_path_factory.mktemp("lake_csv"))
    storage = DuckLakeStorage(base_path=lake_dir)
    yield storage
    storage.close()


# ── Bronze Layer ──────────────────────────────────────────────────────


class TestCSVBronzeIngest:
    """Ingest all CSV files into the bronze layer via PipelineRunner."""

    def test_ingest_products(self, csv_data_dir, csv_lake):
        pipeline = Pipeline(
            name="csv_bronze_products",
            source=CSVConnector(csv_data_dir["products"]),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="products",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=csv_lake)
        result = _run_pipeline(runner, pipeline)

        df = csv_lake.read("products", "bronze").fetchdf()
        assert len(df) == 25
        assert set(df.columns) == {"product_id", "name", "category", "cost", "price"}

    def test_ingest_customers(self, csv_data_dir, csv_lake):
        pipeline = Pipeline(
            name="csv_bronze_customers",
            source=CSVConnector(csv_data_dir["customers"]),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="customers",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("customers", "bronze").fetchdf()
        assert len(df) == 50
        assert "customer_id" in df.columns
        assert "email" in df.columns

    def test_ingest_order_lines(self, csv_data_dir, csv_lake):
        pipeline = Pipeline(
            name="csv_bronze_order_lines",
            source=CSVConnector(csv_data_dir["order_lines"]),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="order_lines",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("order_lines", "bronze").fetchdf()
        assert len(df) > 100
        assert "line_id" in df.columns
        assert "order_date" in df.columns

    def test_ingest_returns(self, csv_data_dir, csv_lake):
        pipeline = Pipeline(
            name="csv_bronze_returns",
            source=CSVConnector(csv_data_dir["returns"]),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="returns",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("returns", "bronze").fetchdf()
        assert len(df) >= 30
        assert "return_id" in df.columns


# ── Silver Layer ───────────────────────────────────────────────────────


class TestCSVSilverClean:
    """Clean and validate data from bronze → silver via PipelineRunner."""

    def test_silver_products(self, csv_lake):
        pipeline = Pipeline(
            name="csv_silver_products",
            source=TableConnector("bronze.products"),
            transforms=[
                """
                SELECT DISTINCT product_id, name, category,
                       cost, price, ROUND(price - cost, 2) AS margin
                FROM {{input}}
                WHERE product_id IS NOT NULL AND price > 0 AND cost > 0
                """,
            ],
            destination_name="products",
            destination_layer="silver",
            quality_rules=[no_nulls("product_id"), unique_column("product_id"),
                           column_positive("price"), column_positive("cost")],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("products", "silver").fetchdf()
        assert len(df) == 25
        assert "margin" in df.columns
        assert (df["margin"] > 0).all()

    def test_silver_customers(self, csv_lake):
        pipeline = Pipeline(
            name="csv_silver_customers",
            source=TableConnector("bronze.customers"),
            transforms=[
                """
                SELECT DISTINCT customer_id, name, email, region,
                       CAST(joined_at AS DATE) AS joined_at
                FROM {{input}}
                WHERE customer_id IS NOT NULL AND email IS NOT NULL
                """,
            ],
            destination_name="customers",
            destination_layer="silver",
            quality_rules=[no_nulls("customer_id"), no_nulls("email"), unique_column("customer_id")],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("customers", "silver").fetchdf()
        assert len(df) == 50
        assert "joined_at" in df.columns

    def test_silver_order_lines(self, csv_lake):
        conn = csv_lake.conn
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

        pipeline = Pipeline(
            name="csv_silver_order_lines",
            source=TableConnector("stg__order_lines"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="order_lines",
            destination_layer="silver",
            quality_rules=[no_nulls("line_id"), no_nulls("order_date"),
                           column_positive("net_amount"), min_row_count(100)],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("order_lines", "silver").fetchdf()
        assert len(df) > 100
        assert "net_amount" in df.columns
        assert (df["net_amount"] > 0).all()

    def test_silver_returns(self, csv_lake):
        pipeline = Pipeline(
            name="csv_silver_returns",
            source=TableConnector("bronze.returns"),
            transforms=[
                """
                SELECT DISTINCT return_id, line_id,
                       CAST(return_date AS DATE) AS return_date,
                       reason, refund_amount
                FROM {{input}}
                WHERE refund_amount > 0
                """,
            ],
            destination_name="returns",
            destination_layer="silver",
            quality_rules=[no_nulls("return_id"), no_nulls("reason"), column_positive("refund_amount")],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("returns", "silver").fetchdf()
        assert len(df) >= 20
        assert "return_date" in df.columns


# ── Gold Layer ─────────────────────────────────────────────────────────


class TestCSVGoldAggregates:
    """Build aggregated gold-layer tables from silver via PipelineRunner."""

    def test_gold_daily_category_revenue(self, csv_lake):
        conn = csv_lake.conn
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

        pipeline = Pipeline(
            name="gold_daily_revenue",
            source=TableConnector("stg__dcr"),
            transforms=[
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
            ],
            destination_name="daily_category_revenue",
            destination_layer="gold",
            quality_rules=[no_nulls("order_date"), no_nulls("category"),
                           column_positive("revenue"), min_row_count(10)],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("daily_category_revenue", "gold").fetchdf()
        assert len(df) >= 10
        assert "revenue_tier" in df.columns
        assert "avg_item_price" in df.columns

    def test_gold_customer_lifetime_value(self, csv_lake):
        conn = csv_lake.conn
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

        pipeline = Pipeline(
            name="gold_customer_clv",
            source=TableConnector("stg__clv"),
            transforms=[
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
            ],
            destination_name="customer_lifetime_value",
            destination_layer="gold",
            quality_rules=[no_nulls("customer_id"), unique_column("customer_id"), min_row_count(10)],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("customer_lifetime_value", "gold").fetchdf()
        assert len(df) >= 10
        assert "loyalty_tier" in df.columns
        assert "customer_segment" in df.columns

    def test_gold_product_performance(self, csv_lake):
        conn = csv_lake.conn
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

        pipeline = Pipeline(
            name="gold_product_performance",
            source=TableConnector("stg__pp"),
            transforms=[
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
            ],
            destination_name="product_performance",
            destination_layer="gold",
            quality_rules=[no_nulls("product_id"), unique_column("product_id"), column_positive("total_revenue")],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("product_performance", "gold").fetchdf()
        assert len(df) > 0
        assert "margin_pct" in df.columns
        assert "sales_velocity" in df.columns

    def test_gold_regional_summary(self, csv_lake):
        conn = csv_lake.conn
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

        pipeline = Pipeline(
            name="gold_regional_summary",
            source=TableConnector("stg__rs"),
            transforms=[
                """
                SELECT *,
                    ROUND(total_revenue / NULLIF(customers, 0), 2) AS revenue_per_customer,
                    ROUND(total_items / NULLIF(customers, 0), 2)   AS items_per_customer
                FROM {{input}}
                """,
            ],
            destination_name="regional_summary",
            destination_layer="gold",
            quality_rules=[no_nulls("region"), unique_column("region"), min_row_count(4)],
        )
        runner = PipelineRunner(storage=csv_lake)
        _run_pipeline(runner, pipeline)

        df = csv_lake.read("regional_summary", "gold").fetchdf()
        assert len(df) >= 4
        assert "revenue_per_customer" in df.columns
        assert "items_per_customer" in df.columns


# ── Cross-layer assertions ────────────────────────────────────────────


class TestCSVCrossLayerIntegrity:
    """Assert structural integrity across all three layers."""

    def test_all_layers_populated(self, csv_lake):
        datasets = csv_lake.list_datasets()
        names = {d["name"] for d in datasets}
        for name in ["products", "customers", "order_lines", "returns"]:
            assert name in names
        for name in ["daily_category_revenue", "customer_lifetime_value", "product_performance", "regional_summary"]:
            assert name in names

    def test_bronze_row_counts(self, csv_lake):
        assert len(csv_lake.read("products", "bronze").fetchdf()) == 25
        assert len(csv_lake.read("customers", "bronze").fetchdf()) == 50
        assert len(csv_lake.read("order_lines", "bronze").fetchdf()) > 100
        assert len(csv_lake.read("returns", "bronze").fetchdf()) >= 30

    def test_silver_products_has_margin(self, csv_lake):
        df = csv_lake.read("products", "silver").fetchdf()
        assert "margin" in df.columns
        assert (df["margin"] > 0).all()

    def test_gold_datasets_have_positive_revenue(self, csv_lake):
        for name in ["daily_category_revenue", "product_performance"]:
            df = csv_lake.read(name, "gold").fetchdf()
            revenue_col = "revenue" if "revenue" in df.columns else "total_revenue"
            assert (df[revenue_col] > 0).all(), f"{name} has non-positive revenue"
