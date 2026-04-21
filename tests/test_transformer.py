"""Tests for the Transformer engine."""

import pytest
from engine.transformer import Transformer


class TestTransformer:
    def test_single_step(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE raw AS SELECT 1 AS id, 'Alice' AS name"
        )
        t = Transformer(duck_conn)
        result = t.run(["SELECT id, name FROM {{input}}"], "raw")
        rows = duck_conn.execute(f"SELECT * FROM {result}").fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "Alice"

    def test_multi_step(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE raw AS SELECT 10 AS amount UNION ALL SELECT 20"
        )
        t = Transformer(duck_conn)
        result = t.run(
            [
                "SELECT SUM(amount) AS total FROM {{input}}",
                "SELECT total * 2 AS doubled FROM {{input}}",
            ],
            "raw",
        )
        row = duck_conn.execute(f"SELECT * FROM {result}").fetchone()
        assert row[0] == 60  # (10+20)*2

    def test_rejects_unsafe_source_name(self, duck_conn):
        t = Transformer(duck_conn)
        with pytest.raises(ValueError, match="Unsafe"):
            t.run(["SELECT 1"], "bad name; DROP TABLE--")

    def test_preview(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE raw AS SELECT range AS id FROM range(10)"
        )
        t = Transformer(duck_conn)
        result = t.preview("raw", limit=3)
        assert len(result) == 3

    def test_preview_invalid_limit(self, duck_conn):
        t = Transformer(duck_conn)
        with pytest.raises(ValueError, match="positive integer"):
            t.preview("raw", limit=-1)

    def test_preview_string_limit_rejected(self, duck_conn):
        t = Transformer(duck_conn)
        with pytest.raises(ValueError, match="positive integer"):
            t.preview("raw", limit="five")  # type: ignore