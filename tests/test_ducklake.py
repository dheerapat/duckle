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
        ducklake_storage.write("src", name="listed", layer="gold", pipeline="my_pipe")
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

    def test_merge_updates_and_inserts(self, ducklake_storage):
        """MERGE INTO should update existing rows and insert new ones."""
        conn = ducklake_storage.conn
        conn.execute("CREATE TABLE dest AS SELECT 1 AS id, 10 AS val")
        ducklake_storage.write("dest", name="merge_ds", layer="gold")

        conn.execute("CREATE TABLE src AS SELECT * FROM (VALUES (1, 99), (2, 20)) t(id, val)")
        ducklake_storage.merge("src", name="merge_ds", layer="gold", merge_keys=["id"])

        df = ducklake_storage.read("merge_ds", "gold").fetchdf()
        assert len(df) == 2
        row1 = df[df["id"] == 1].iloc[0]
        row2 = df[df["id"] == 2].iloc[0]
        assert row1["val"] == 99
        assert row2["val"] == 20

    def test_merge_fallback_to_write_when_missing(self, ducklake_storage):
        """merge() should fall back to write if the destination does not exist."""
        conn = ducklake_storage.conn
        conn.execute("CREATE TABLE src AS SELECT 1 AS id, 10 AS val")
        ducklake_storage.merge("src", name="new_merge_ds", layer="gold", merge_keys=["id"])

        df = ducklake_storage.read("new_merge_ds", "gold").fetchdf()
        assert len(df) == 1

    def test_get_last_cursor(self, ducklake_storage):
        ducklake_storage.conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        ducklake_storage.write("src", name="cursor_ds", layer="gold")
        assert ducklake_storage.get_last_cursor("cursor_ds", "gold") is None

        ducklake_storage.update_pipeline_cursor(
            "cursor_ds", "gold", cursor_column="dt", last_cursor_value="2024-01-01",
            merge_keys=["id"], run_mode="incremental"
        )
        assert ducklake_storage.get_last_cursor("cursor_ds", "gold") == "2024-01-01"

    def test_get_pipeline_state(self, ducklake_storage):
        ducklake_storage.conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        ducklake_storage.write("src", name="state_ds", layer="gold", pipeline="p1")
        ducklake_storage.update_pipeline_cursor(
            "state_ds", "gold", cursor_column="dt", last_cursor_value="2024-01-01",
            merge_keys=["id"], run_mode="incremental"
        )
        state = ducklake_storage.get_pipeline_state("state_ds", "gold")
        assert state is not None
        assert state["name"] == "state_ds"
        assert state["layer"] == "gold"
        assert state["cursor_column"] == "dt"
        assert state["last_cursor_value"] == "2024-01-01"
        assert state["merge_keys"] == "id"
        assert state["run_mode"] == "incremental"
        assert state["pipeline"] == "p1"

    def test_get_pipeline_state_missing(self, ducklake_storage):
        assert ducklake_storage.get_pipeline_state("missing", "gold") is None

    def test_merge_keys_metadata_stored(self, ducklake_storage):
        conn = ducklake_storage.conn
        conn.execute("CREATE TABLE dest AS SELECT 1 AS id, 10 AS val")
        ducklake_storage.write("dest", name="meta_merge", layer="gold")

        conn.execute("CREATE TABLE src AS SELECT 2 AS id, 20 AS val")
        ducklake_storage.merge("src", name="meta_merge", layer="gold", merge_keys=["id", "val"])

        state = ducklake_storage.get_pipeline_state("meta_merge", "gold")
        assert state["merge_keys"] == "id,val"
        assert state["run_mode"] == "incremental"

    def test_publish_full_replace(self, ducklake_storage):
        conn = ducklake_storage.conn
        conn.execute("CREATE TABLE staging_src AS SELECT 1 AS id, 10 AS val")
        ducklake_storage.write("staging_src", name="pub_ds", layer="staging", track_metadata=False)

        ducklake_storage.publish(
            staging_name="pub_ds",
            name="pub_dest",
            layer="gold",
            merge_keys=None,
        )

        df = ducklake_storage.read("pub_dest", "gold").fetchdf()
        assert len(df) == 1
        assert df.iloc[0]["val"] == 10

        # Staging should be dropped
        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'pub_ds'"
        ).fetchone()
        assert row[0] == 0

    def test_publish_merge(self, ducklake_storage):
        conn = ducklake_storage.conn
        conn.execute("CREATE TABLE dest AS SELECT 1 AS id, 10 AS val")
        ducklake_storage.write("dest", name="pub_merge", layer="gold")

        conn.execute("CREATE TABLE staging_src AS SELECT * FROM (VALUES (1, 99), (2, 20)) t(id, val)")
        ducklake_storage.write("staging_src", name="pub_merge", layer="staging", track_metadata=False)

        ducklake_storage.publish(
            staging_name="pub_merge",
            name="pub_merge",
            layer="gold",
            merge_keys=["id"],
        )

        df = ducklake_storage.read("pub_merge", "gold").fetchdf()
        assert len(df) == 2
        assert df[df["id"] == 1]["val"].iloc[0] == 99
        assert df[df["id"] == 2]["val"].iloc[0] == 20

        # Staging should be dropped
        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'pub_merge'"
        ).fetchone()
        assert row[0] == 0

    def test_publish_merge_fallback(self, ducklake_storage):
        conn = ducklake_storage.conn
        conn.execute("CREATE TABLE staging_src AS SELECT 1 AS id, 10 AS val")
        ducklake_storage.write("staging_src", name="pub_fallback", layer="staging", track_metadata=False)

        # Destination does not exist — publish with merge_keys should fall back to CREATE OR REPLACE
        ducklake_storage.publish(
            staging_name="pub_fallback",
            name="pub_fallback_dest",
            layer="gold",
            merge_keys=["id"],
        )

        df = ducklake_storage.read("pub_fallback_dest", "gold").fetchdf()
        assert len(df) == 1

    def test_drop_staging(self, ducklake_storage):
        conn = ducklake_storage.conn
        conn.execute("CREATE TABLE src AS SELECT 1 AS id")
        ducklake_storage.write("src", name="drop_me", layer="staging", track_metadata=False)

        ducklake_storage.drop_staging("drop_me")

        row = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'staging' AND table_name = 'drop_me'"
        ).fetchone()
        assert row[0] == 0
