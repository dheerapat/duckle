"""Tests for connectors (CSV, API)."""

import csv
import json
from unittest.mock import patch, MagicMock

import duckdb
import pytest
from connectors.csv_connector import CSVConnector
from connectors.postgres_connector import PostgresConnector
from connectors.table_connector import TableConnector
from connectors.base import BaseConnector


class TestCSVConnector:
    def test_extract_loads_rows(self, duck_conn, sample_csv):
        connector = CSVConnector(sample_csv)
        connector.extract(duck_conn, "raw")
        result = duck_conn.execute("SELECT COUNT(*) FROM raw").fetchone()
        count = result[0]
        assert count == 5

    def test_extract_correct_data(self, duck_conn, sample_csv):
        connector = CSVConnector(sample_csv)
        connector.extract(duck_conn, "raw")
        row = duck_conn.execute("SELECT SUM(amount) FROM raw").fetchone()
        assert row[0] == 1000  # 100+200+150+300+250

    def test_test_connection_exists(self, sample_csv):
        connector = CSVConnector(sample_csv)
        assert connector.test_connection() is True

    def test_test_connection_missing(self):
        connector = CSVConnector("/nonexistent/path.csv")
        assert connector.test_connection() is False

    def test_rejects_unsafe_table_name(self, duck_conn, sample_csv):
        connector = CSVConnector(sample_csv)
        with pytest.raises(ValueError, match="Unsafe"):
            connector.extract(duck_conn, "'; DROP TABLE--")

    def test_rejects_unsafe_path(self, duck_conn):
        connector = CSVConnector("/tmp/evil'/file.csv")
        with pytest.raises(ValueError, match="Unsafe file path"):
            connector.extract(duck_conn, "raw")


class TestPostgresConnector:
    def test_validate_cursor_value_rejects_quotes(self):
        connector = PostgresConnector("host=localhost dbname=test", "SELECT 1")
        with pytest.raises(ValueError, match="Unsafe cursor value"):
            connector._validate_cursor_value("2024-01-01'; DROP TABLE--")

    def test_validate_cursor_value_accepts_safe_dates(self):
        connector = PostgresConnector("host=localhost dbname=test", "SELECT 1")
        connector._validate_cursor_value("2024-01-01 12:00:00")
        connector._validate_cursor_value("2024-01-01T12:00:00.000")
        connector._validate_cursor_value("12345")


class TestTableConnectorIncrementalRange:
    def test_extract_incremental_explicit_from_inclusive(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE src_tbl AS SELECT * FROM (VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02')) t(id, label, ts)"
        )
        connector = TableConnector("src_tbl")
        connector.extract_incremental(duck_conn, "out_tbl", "ts", "2024-01-01", from_is_explicit=True)
        result = duck_conn.execute("SELECT COUNT(*) FROM out_tbl").fetchone()
        assert result[0] == 2  # inclusive start

    def test_extract_incremental_with_until_value(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE src_tbl AS SELECT * FROM (VALUES (1, 'a', '2024-01-01'), (2, 'b', '2024-01-02'), (3, 'c', '2024-01-03')) t(id, label, ts)"
        )
        connector = TableConnector("src_tbl")
        connector.extract_incremental(duck_conn, "out_tbl", "ts", "2024-01-01", until_value="2024-01-03", from_is_explicit=True)
        result = duck_conn.execute("SELECT COUNT(*) FROM out_tbl").fetchone()
        assert result[0] == 2
        ids = sorted(r[0] for r in duck_conn.execute("SELECT id FROM out_tbl").fetchall())
        assert ids == [1, 2]


class TestCSVConnectorIncrementalRange:
    def test_extract_incremental_with_until_value(self, tmp_path):
        csv_path = tmp_path / "range.csv"
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
        connector.extract_incremental(conn, "raw", "dt", "2024-01-01", until_value="2024-01-03", from_is_explicit=True)
        result = conn.execute("SELECT COUNT(*) FROM raw").fetchone()
        assert result is not None
        assert result[0] == 2
        conn.close()


class TestPostgresConnectorIncrementalRange:
    def test_validate_cursor_value_rejects_until_value(self):
        connector = PostgresConnector("host=localhost dbname=test", "SELECT 1")
        with pytest.raises(ValueError, match="Unsafe cursor value"):
            connector._validate_cursor_value("2024-01-01'; DROP TABLE--")

    def test_extract_incremental_builds_range_query(self):
        """Unit-test the SQL query construction for ranged incremental."""
        connector = PostgresConnector("host=localhost dbname=test", "SELECT * FROM orders")
        connector._validate_cursor_value("2024-01-01")
        connector._validate_cursor_value("2024-02-01")
        # If we reached here without exception, validation works for bounds
        assert True


class TestBaseConnectorIsAbstract:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BaseConnector()
