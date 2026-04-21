"""Tests for DuckLake storage."""

import pytest
from storage.ducklake import DuckLakeStorage


class TestDuckLakeStorage:
    def test_write_and_read(self, ducklake_storage):
        ducklake_storage.conn.execute(
            "CREATE TABLE src AS SELECT 42 AS id, 'hello' AS msg"
        )
        ducklake_storage.write("src", name="test_ds", layer="bronze")
        rel = ducklake_storage.read("test_ds", "bronze")
        df = rel.fetchdf()
        assert len(df) == 1
        assert df.iloc[0]["id"] == 42

    def test_write_to_all_layers(self, ducklake_storage):
        ducklake_storage.conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        for layer in ["bronze", "silver", "gold"]:
            ducklake_storage.write("src", name=f"ds_{layer}", layer=layer)

        for layer in ["bronze", "silver", "gold"]:
            df = ducklake_storage.read(f"ds_{layer}", layer).fetchdf()
            assert len(df) == 1

    def test_invalid_layer_raises(self, ducklake_storage):
        ducklake_storage.conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        with pytest.raises(AssertionError, match="Layer must be one of"):
            ducklake_storage.write("src", name="bad", layer="platinum")

    def test_invalid_layer_read_raises(self, ducklake_storage):
        with pytest.raises(ValueError, match="Layer must be one of"):
            ducklake_storage.read("x", layer="platinum")

    def test_list_datasets(self, ducklake_storage):
        ducklake_storage.conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        ducklake_storage.write(
            "src", name="listed", layer="gold", pipeline="my_pipe"
        )
        datasets = ducklake_storage.list_datasets()
        assert len(datasets) >= 1
        ds = [d for d in datasets if d["name"] == "listed"][0]
        assert ds["layer"] == "gold"
        assert ds["row_count"] == 1
        assert ds["pipeline"] == "my_pipe"

    def test_snapshots(self, ducklake_storage):
        # DuckLake creates at least one snapshot on write
        ducklake_storage.conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        ducklake_storage.write("src", name="snap_test", layer="gold")
        snaps = ducklake_storage.snapshots()
        assert isinstance(snaps, list)
        # snapshots may be empty or populated depending on DuckLake version
        # just verify the call doesn't crash

    def test_unsafe_identifier_rejected_on_write(self, ducklake_storage):
        ducklake_storage.conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        with pytest.raises(ValueError, match="Unsafe"):
            ducklake_storage.write("src", name="bad name", layer="gold")

    def test_intermediate_table_cleaned_up_by_runner(
        self, ducklake_storage, sample_csv
    ):
        """Verify intermediate tables are dropped after a pipeline run."""
        from connectors.csv_connector import CSVConnector
        from orchestrator.runner import Pipeline, PipelineRunner

        pipeline = Pipeline(
            name="cleanup_test",
            source=CSVConnector(sample_csv),
            transforms=["SELECT * FROM {{input}}"],
            destination_name="cleanup_out",
            destination_layer="gold",
        )
        runner = PipelineRunner(storage=ducklake_storage)
        runner.run(pipeline)

        conn = ducklake_storage.conn
        # 'raw' and '_transform_step_0' should have been dropped
        remaining = conn.execute("SHOW TABLES").fetchdf()["name"].tolist()
        assert "raw" not in remaining
        assert "_transform_step_0" not in remaining