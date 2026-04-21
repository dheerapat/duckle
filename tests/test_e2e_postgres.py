"""End-to-end pipeline tests for the Postgres ETL pipeline.

Uses Pipeline / PipelineRunner for all stages, mirroring
scripts/test_postgres.py.

Requires a running Postgres instance (docker compose up -d).
Tests are skipped automatically if Postgres is unavailable.
"""

import os
import sys

import pytest

sys_path = os.path.join(os.path.dirname(__file__), "..", "src")
if sys_path not in sys.path:
    sys.path.insert(0, sys_path)

from connectors.postgres_connector import PostgresConnector
from connectors.table_connector import TableConnector
from orchestrator.runner import Pipeline, PipelineRunner
from quality.checker import column_positive, min_row_count, no_nulls, unique_column
from storage.ducklake import DuckLakeStorage

# ── Config ────────────────────────────────────────────────────────────

PG_CONN = "host=localhost port=5433 dbname=restaurant user=duckle password=duckle_secret"


def _pg_available() -> bool:
    connector = PostgresConnector(PG_CONN, "SELECT 1")
    return connector.test_connection()


def _create_staging(conn, sql: str, table_name: str) -> str:
    conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
    result = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    count = result[0] if result is not None else 0
    return table_name


def _run_pipeline(runner: PipelineRunner, pipeline: Pipeline) -> dict:
    result = runner.run(pipeline)
    assert result["status"] == "success", f"Pipeline '{pipeline.name}' failed: {result['error']}"
    return result


# Skip entire module if Postgres is not available
pytestmark = pytest.mark.skipif(
    not _pg_available(),
    reason="Postgres not available — run: docker compose up -d",
)


# ── Fixtures ───────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def pg_lake(tmp_path_factory):
    """Provide a DuckLakeStorage for the Postgres e2e pipeline."""
    lake_dir = str(tmp_path_factory.mktemp("lake_pg"))
    storage = DuckLakeStorage(base_path=lake_dir)
    yield storage
    storage.close()


# ── Bronze Layer ──────────────────────────────────────────────────────


class TestPostgresBronzeIngest:
    """Ingest all restaurant tables from Postgres into bronze via PipelineRunner."""

    def test_ingest_menu_items(self, pg_lake):
        pipeline = Pipeline(
            name="pg_bronze_menu_items",
            source=PostgresConnector(PG_CONN, "SELECT * FROM menu_items"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="menu_items",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("menu_items", "bronze").fetchdf()
        assert len(df) > 0
        assert "id" in df.columns or "menu_item_id" in df.columns

    def test_ingest_customers(self, pg_lake):
        pipeline = Pipeline(
            name="pg_bronze_customers",
            source=PostgresConnector(PG_CONN, "SELECT * FROM customers"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="customers",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("customers", "bronze").fetchdf()
        assert len(df) > 0
        assert "email" in df.columns

    def test_ingest_orders(self, pg_lake):
        pipeline = Pipeline(
            name="pg_bronze_orders",
            source=PostgresConnector(PG_CONN, "SELECT * FROM orders"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="orders",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("orders", "bronze").fetchdf()
        assert len(df) > 0
        assert "order_date" in df.columns or "id" in df.columns

    def test_ingest_order_items(self, pg_lake):
        pipeline = Pipeline(
            name="pg_bronze_order_items",
            source=PostgresConnector(PG_CONN, "SELECT * FROM order_items"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="order_items",
            destination_layer="bronze",
        )
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("order_items", "bronze").fetchdf()
        assert len(df) > 0
        assert "quantity" in df.columns or "id" in df.columns

    def test_bronze_row_counts(self, pg_lake):
        for name in ["menu_items", "customers", "orders", "order_items"]:
            df = pg_lake.read(name, "bronze").fetchdf()
            assert len(df) > 0, f"bronze.{name} is empty"


# ── Silver Layer ───────────────────────────────────────────────────────


class TestPostgresSilverClean:
    """Clean and validate data from bronze → silver via PipelineRunner."""

    def test_silver_menu_items(self, pg_lake):
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
            quality_rules=[no_nulls("menu_item_id"), unique_column("menu_item_id"),
                           column_positive("price")],
        )
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("menu_items", "silver").fetchdf()
        assert len(df) > 0
        assert "estimated_margin" in df.columns
        assert "menu_item_id" in df.columns

    def test_silver_customers(self, pg_lake):
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
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("customers", "silver").fetchdf()
        assert len(df) >= 30
        assert "join_month" in df.columns

    def test_silver_orders(self, pg_lake):
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
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("orders", "silver").fetchdf()
        assert len(df) >= 100
        assert "is_weekend" in df.columns
        assert "day_of_week" in df.columns

    def test_silver_order_items(self, pg_lake):
        conn = pg_lake.conn
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

        pipeline = Pipeline(
            name="pg_silver_order_items",
            source=TableConnector("stg__order_items"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="order_items",
            destination_layer="silver",
            quality_rules=[no_nulls("order_item_id"), no_nulls("order_id"),
                           column_positive("quantity"), column_positive("line_total"),
                           min_row_count(100)],
        )
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("order_items", "silver").fetchdf()
        assert len(df) >= 100
        assert "line_total" in df.columns
        assert (df["line_total"] > 0).all()


# ── Gold Layer ─────────────────────────────────────────────────────────


class TestPostgresGoldAggregates:
    """Build aggregated gold-layer tables from silver via PipelineRunner."""

    def test_gold_daily_category_revenue(self, pg_lake):
        conn = pg_lake.conn
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

        pipeline = Pipeline(
            name="gold_daily_revenue",
            source=TableConnector("stg__dcr"),
            transforms=[
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
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("daily_category_revenue", "gold").fetchdf()
        assert len(df) >= 10
        assert "revenue_tier" in df.columns
        assert "avg_item_price" in df.columns

    def test_gold_customer_spending(self, pg_lake):
        conn = pg_lake.conn
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

        pipeline = Pipeline(
            name="gold_customer_spending",
            source=TableConnector("stg__cs"),
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
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("customer_spending", "gold").fetchdf()
        assert len(df) >= 10
        assert "loyalty_tier" in df.columns
        assert "customer_segment" in df.columns

    def test_gold_menu_item_performance(self, pg_lake):
        conn = pg_lake.conn
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

        pipeline = Pipeline(
            name="gold_menu_item_performance",
            source=TableConnector("stg__mip"),
            transforms=[
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
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("menu_item_performance", "gold").fetchdf()
        assert len(df) > 0
        assert "total_margin" in df.columns
        assert "sales_velocity" in df.columns

    def test_gold_weekend_vs_weekday(self, pg_lake):
        conn = pg_lake.conn
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

        pipeline = Pipeline(
            name="gold_weekend_vs_weekday",
            source=TableConnector("stg__wvw"),
            transforms=[
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
        runner = PipelineRunner(storage=pg_lake)
        _run_pipeline(runner, pipeline)

        df = pg_lake.read("weekend_vs_weekday", "gold").fetchdf()
        assert len(df) >= 2
        assert "revenue_per_order" in df.columns
        assert "items_per_order" in df.columns


# ── Cross-layer assertions ────────────────────────────────────────────


class TestPostgresCrossLayerIntegrity:
    """Assert structural integrity across all three layers."""

    def test_all_layers_populated(self, pg_lake):
        datasets = pg_lake.list_datasets()
        names = {d["name"] for d in datasets}
        for name in ["menu_items", "customers", "orders", "order_items"]:
            assert name in names, f"Missing bronze/silver.{name}"
        for name in ["daily_category_revenue", "customer_spending", "menu_item_performance", "weekend_vs_weekday"]:
            assert name in names, f"Missing gold.{name}"

    def test_gold_datasets_have_positive_revenue(self, pg_lake):
        for name in ["daily_category_revenue", "menu_item_performance"]:
            df = pg_lake.read(name, "gold").fetchdf()
            revenue_col = "revenue" if "revenue" in df.columns else "total_revenue"
            assert (df[revenue_col] > 0).all(), f"{name} has non-positive revenue"
