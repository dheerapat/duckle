"""Tests for the pipeline runner (end-to-end)."""

import csv
import os
import tempfile

import duckdb
import pytest
from connectors.csv_connector import CSVConnector
from connectors.table_connector import TableConnector
from orchestrator.runner import IncrementalConfig, Pipeline, PipelineRunner
from quality.checker import column_positive, min_row_count, no_nulls
from storage.ducklake import DuckLakeStorage


class TestPipelineRunner:
    def test_successful_pipeline(self, ducklake_storage, sample_csv):
        pipeline = Pipeline(
            name="test_pipe",
            source=CSVConnector(sample_csv),
            transforms=[
                "SELECT date, SUM(amount) AS revenue FROM {{input}} GROUP BY date"
            ],
            destination_name="daily_rev",
            destination_layer="gold",
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "success"
        assert result["error"] is None

    def test_pipeline_with_quality_rules(self, ducklake_storage, sample_csv):
        pipeline = Pipeline(
            name="quality_pipe",
            source=CSVConnector(sample_csv),
            transforms=[
                "SELECT date, SUM(amount) AS revenue FROM {{input}} GROUP BY date"
            ],
            destination_name="quality_rev",
            destination_layer="gold",
            quality_rules=[no_nulls("date"), min_row_count(1)],
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "success"
        assert "quality" in result

    def test_pipeline_fails_on_quality(self, ducklake_storage, sample_csv):
        pipeline = Pipeline(
            name="fail_pipe",
            source=CSVConnector(sample_csv),
            transforms=[
                "SELECT date, SUM(amount) AS revenue FROM {{input}} GROUP BY date"
            ],
            destination_name="fail_rev",
            destination_layer="gold",
            quality_rules=[min_row_count(9999)],  # impossible to pass
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "failed"
        assert "Quality checks failed" in result["error"]

    def test_pipeline_still_usable_after_success(self, ducklake_storage, sample_csv):
        """Verify the shared storage connection is usable after a successful run."""
        pipeline = Pipeline(
            name="conn_test",
            source=CSVConnector(sample_csv),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="conn_out",
            destination_layer="gold",
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "success"
        # Shared connection should still be usable for reads
        df = ducklake_storage.read("conn_out", "gold").fetchdf()
        assert len(df) > 0

    def test_shared_connection_usable_after_failure(self, ducklake_storage):
        """Verify shared storage connection is usable even after a failed pipeline."""
        # Use a source that will fail
        from connectors.postgres_connector import PostgresConnector

        pipeline = Pipeline(
            name="conn_fail",
            source=PostgresConnector("host=nonexistent dbname=test", "SELECT 1"),
            transforms=["SELECT 1"],
            destination_name="conn_fail_out",
            destination_layer="gold",
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "failed"
        assert result["error"] is not None
        # Shared connection should still be usable
        datasets = ducklake_storage.list_datasets()
        assert isinstance(datasets, list)

    def test_run_history_tracks_runs(self, ducklake_storage, sample_csv):
        pipeline = Pipeline(
            name="history_pipe",
            source=CSVConnector(sample_csv),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="hist_out",
            destination_layer="gold",
        )
        runner = PipelineRunner(storage=ducklake_storage)
        runner.run(pipeline)
        runner.run(pipeline)

        assert len(runner.run_history) == 2
        for entry in runner.run_history:
            assert "started_at" in entry
            assert "finished_at" in entry

    def test_multi_source_pipeline(self, ducklake_storage):
        """Pipeline with multiple sources can JOIN across extracted tables."""
        from connectors.table_connector import TableConnector

        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE tbl_a AS SELECT * FROM (VALUES (1, 'alpha'), (2, 'beta')) t(id, label)"
        )
        conn.execute(
            "CREATE TABLE tbl_b AS SELECT * FROM (VALUES (1, 100), (2, 200)) t(id, value)"
        )

        pipeline = Pipeline(
            name="multi_join_pipe",
            sources={
                "a": TableConnector("tbl_a"),
                "b": TableConnector("tbl_b"),
            },
            transforms=[
                "SELECT a.id, a.label, b.value FROM a JOIN b ON a.id = b.id",
            ],
            destination_name="joined",
            destination_layer="gold",
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "success"
        df = ducklake_storage.read("joined", "gold").fetchdf()
        assert len(df) == 2
        assert list(df["label"]) == ["alpha", "beta"]
        assert list(df["value"]) == [100, 200]

    def test_multi_source_transform_chain(self, ducklake_storage):
        """Multi-source pipeline with chained transform steps."""
        from connectors.table_connector import TableConnector

        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE orders AS SELECT * FROM (VALUES (1, 50), (2, 75)) t(order_id, amount)"
        )
        conn.execute(
            "CREATE TABLE customers AS SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) t(id, name)"
        )

        pipeline = Pipeline(
            name="multi_chain_pipe",
            sources={
                "o": TableConnector("orders"),
                "c": TableConnector("customers"),
            },
            transforms=[
                "SELECT o.order_id, o.amount, c.name FROM o JOIN c ON o.order_id = c.id",
                "SELECT * FROM {{input}} WHERE amount >= 50",
            ],
            destination_name="filtered_join",
            destination_layer="gold",
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "success"
        df = ducklake_storage.read("filtered_join", "gold").fetchdf()
        assert len(df) == 2

    def test_pipeline_rejects_both_source_and_sources(self):
        from connectors.csv_connector import CSVConnector

        with pytest.raises(ValueError, match="only one"):
            Pipeline(
                name="bad",
                source=CSVConnector("x.csv"),
                sources={"a": CSVConnector("y.csv")},
                transforms=["SELECT 1"],
                destination_name="out",
            )

    def test_pipeline_rejects_neither_source_nor_sources(self):
        with pytest.raises(ValueError, match="required"):
            Pipeline(
                name="bad",
                transforms=["SELECT 1"],
                destination_name="out",
            )

    def test_pipeline_rejects_invalid_alias(self):
        from connectors.csv_connector import CSVConnector

        with pytest.raises(ValueError, match="alias"):
            Pipeline(
                name="bad",
                sources={"bad-alias": CSVConnector("x.csv")},
                transforms=["SELECT 1"],
                destination_name="out",
            )


class TestIncrementalPipeline:
    def test_incremental_first_run_fallback(self, ducklake_storage):
        """No prior cursor → behaves like full load, creates table."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_orders AS SELECT * FROM (VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-02')) t(order_id, amount, order_date)"
        )

        pipeline = Pipeline(
            name="inc_fallback",
            source=TableConnector("src_orders"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="orders",
            destination_layer="gold",
            incremental=IncrementalConfig(
                cursor_column="order_date",
                merge_keys=["order_id"],
            ),
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "success"
        df = ducklake_storage.read("orders", "gold").fetchdf()
        assert len(df) == 2

    def test_incremental_extracts_delta(self, ducklake_storage):
        """Second run with higher cursor value; verify only new rows extracted."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_orders AS SELECT * FROM (VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-02'), (3, 300, '2024-01-03')) t(order_id, amount, order_date)"
        )

        pipeline = Pipeline(
            name="inc_delta",
            source=TableConnector("src_orders"),
            transforms=["SELECT * FROM {{input}} WHERE order_date > '2024-01-01'"],
            destination_name="orders_delta",
            destination_layer="gold",
            incremental=IncrementalConfig(
                cursor_column="order_date",
                merge_keys=["order_id"],
            ),
        )
        runner = PipelineRunner(storage=ducklake_storage)
        # First run — full load because no cursor exists yet
        runner.run(pipeline)
        df1 = ducklake_storage.read("orders_delta", "gold").fetchdf()
        assert len(df1) == 2  # rows 2 and 3

        # Simulate new data arriving
        conn.execute(
            "INSERT INTO src_orders VALUES (4, 400, '2024-01-04')"
        )

        # Second run — incremental, should only process rows > '2024-01-03'
        result = runner.run(pipeline)
        assert result["status"] == "success"
        df2 = ducklake_storage.read("orders_delta", "gold").fetchdf()
        assert len(df2) == 3  # rows 2, 3, and 4

    def test_incremental_merge_updates(self, ducklake_storage):
        """Update an existing merge key; verify destination row changes."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_orders AS SELECT * FROM (VALUES (1, 100, '2024-01-01')) t(order_id, amount, order_date)"
        )

        pipeline = Pipeline(
            name="inc_update",
            source=TableConnector("src_orders"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="orders_update",
            destination_layer="gold",
            incremental=IncrementalConfig(
                cursor_column="order_date",
                merge_keys=["order_id"],
            ),
        )
        runner = PipelineRunner(storage=ducklake_storage)
        runner.run(pipeline)

        # Update existing row
        conn.execute("UPDATE src_orders SET amount = 999, order_date = '2024-01-02' WHERE order_id = 1")
        result = runner.run(pipeline)
        assert result["status"] == "success"

        df = ducklake_storage.read("orders_update", "gold").fetchdf()
        assert len(df) == 1
        assert df.iloc[0]["amount"] == 999

    def test_incremental_merge_inserts(self, ducklake_storage):
        """New merge key arrives; verify row inserted."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_orders AS SELECT * FROM (VALUES (1, 100, '2024-01-01')) t(order_id, amount, order_date)"
        )

        pipeline = Pipeline(
            name="inc_insert",
            source=TableConnector("src_orders"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="orders_insert",
            destination_layer="gold",
            incremental=IncrementalConfig(
                cursor_column="order_date",
                merge_keys=["order_id"],
            ),
        )
        runner = PipelineRunner(storage=ducklake_storage)
        runner.run(pipeline)

        conn.execute("INSERT INTO src_orders VALUES (2, 200, '2024-01-02')")
        result = runner.run(pipeline)
        assert result["status"] == "success"

        df = ducklake_storage.read("orders_insert", "gold").fetchdf()
        assert len(df) == 2

    def test_full_refresh_resets_cursor(self, ducklake_storage):
        """Pass full_refresh=True; verify table replaced and cursor reset."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_orders AS SELECT * FROM (VALUES (1, 100, '2024-01-01'), (2, 200, '2024-01-02')) t(order_id, amount, order_date)"
        )

        pipeline = Pipeline(
            name="inc_refresh",
            source=TableConnector("src_orders"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="orders_refresh",
            destination_layer="gold",
            incremental=IncrementalConfig(
                cursor_column="order_date",
                merge_keys=["order_id"],
            ),
        )
        runner = PipelineRunner(storage=ducklake_storage)
        runner.run(pipeline)

        # Add a new row but delete an old one from source
        conn.execute("DELETE FROM src_orders WHERE order_id = 1")
        conn.execute("INSERT INTO src_orders VALUES (3, 300, '2024-01-03')")

        # Full refresh should rebuild the entire table
        result = runner.run(pipeline, full_refresh=True)
        assert result["status"] == "success"

        df = ducklake_storage.read("orders_refresh", "gold").fetchdf()
        assert len(df) == 2
        assert set(df["order_id"].tolist()) == {2, 3}

    def test_incremental_quality_on_delta(self, ducklake_storage):
        """Quality rules run on the delta only."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_orders AS SELECT * FROM (VALUES (1, 100, '2024-01-01')) t(order_id, amount, order_date)"
        )

        pipeline = Pipeline(
            name="inc_quality",
            source=TableConnector("src_orders"),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="orders_quality",
            destination_layer="gold",
            incremental=IncrementalConfig(
                cursor_column="order_date",
                merge_keys=["order_id"],
            ),
            quality_rules=[min_row_count(1)],
        )
        runner = PipelineRunner(storage=ducklake_storage)
        runner.run(pipeline)

        # Add a new row; delta has 1 row, quality should pass
        conn.execute("INSERT INTO src_orders VALUES (2, 200, '2024-01-02')")
        result = runner.run(pipeline)
        assert result["status"] == "success"

    def test_incremental_missing_cursor_column(self, ducklake_storage):
        """Transform drops cursor column → pipeline fails fast with clear error."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_orders AS SELECT * FROM (VALUES (1, 100, '2024-01-01')) t(order_id, amount, order_date)"
        )

        pipeline = Pipeline(
            name="inc_missing_cursor",
            source=TableConnector("src_orders"),
            transforms=["SELECT order_id, amount FROM {{input}}"],  # drops order_date
            destination_name="orders_missing",
            destination_layer="gold",
            incremental=IncrementalConfig(
                cursor_column="order_date",
                merge_keys=["order_id"],
            ),
        )
        runner = PipelineRunner(storage=ducklake_storage)
        result = runner.run(pipeline)

        assert result["status"] == "failed"
        assert "cursor column" in result["error"].lower() or "not found" in result["error"].lower()

    def test_table_connector_incremental(self, ducklake_storage):
        """Verify TableConnector appends WHERE in DuckDB."""
        conn = ducklake_storage.conn
        conn.execute(
            "CREATE TABLE src_tbl AS SELECT * FROM (VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02')) t(id, label, ts)"
        )

        connector = TableConnector("src_tbl")
        connector.extract_incremental(conn, "out_tbl", "ts", "2024-01-01")
        result = conn.execute("SELECT COUNT(*) FROM out_tbl").fetchone()
        assert result[0] == 1
        assert conn.execute("SELECT id FROM out_tbl").fetchone()[0] == 2

    def test_csv_connector_incremental_fallback(self, tmp_path):
        """CSVConnector uses the default fallback for incremental extraction."""
        csv_path = tmp_path / "inc.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "amount", "dt"])
            writer.writerows([
                [1, 100, "2024-01-01"],
                [2, 200, "2024-01-02"],
                [3, 300, "2024-01-03"],
            ])

        conn = duckdb.connect()
        connector = CSVConnector(str(csv_path))
        connector.extract_incremental(conn, "raw", "dt", "2024-01-01")
        result = conn.execute("SELECT COUNT(*) FROM raw").fetchone()
        assert result is not None
        assert result[0] == 2
        conn.close()

    def test_pipeline_rejects_empty_merge_keys(self):
        with pytest.raises(ValueError, match="merge_keys"):
            Pipeline(
                name="bad",
                source=CSVConnector("x.csv"),
                transforms=["SELECT 1"],
                destination_name="out",
                incremental=IncrementalConfig(
                    cursor_column="id",
                    merge_keys=[],
                ),
            )

    def test_pipeline_rejects_invalid_cursor_column(self):
        with pytest.raises(ValueError, match="cursor_column"):
            Pipeline(
                name="bad",
                source=CSVConnector("x.csv"),
                transforms=["SELECT 1"],
                destination_name="out",
                incremental=IncrementalConfig(
                    cursor_column="bad;column",
                    merge_keys=["id"],
                ),
            )
