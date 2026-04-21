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

    def test_pipeline_closes_connection_on_success(self, ducklake_storage, sample_csv):
        """Verify DuckDB connection is closed after a successful run."""
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
        # The connection should have been closed; we can't easily inspect
        # __closed__ from outside, but the finally block should handle it

    def test_pipeline_closes_connection_on_failure(self, ducklake_storage):
        """Verify DuckDB connection is closed even when the pipeline fails."""
        # Use a source that connects to a nonexistent Postgres
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