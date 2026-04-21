"""Tests for connectors (CSV, API)."""

import json
from unittest.mock import patch, MagicMock

import duckdb
import pytest
from connectors.csv_connector import CSVConnector
from connectors.postgres_connector import PostgresConnector
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


class TestBaseConnectorIsAbstract:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BaseConnector()
