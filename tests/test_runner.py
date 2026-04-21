"""Tests for the pipeline runner (end-to-end)."""

import csv

import pytest
from connectors.csv_connector import CSVConnector
from orchestrator.runner import Pipeline, PipelineRunner
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
            source=PostgresConnector(
                "host=nonexistent dbname=test", "SELECT 1"
            ),
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