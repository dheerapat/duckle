"""Tests for connectors (CSV, API)."""

import json
from unittest.mock import patch, MagicMock

import pytest
from connectors.csv_connector import CSVConnector
from connectors.api_connector import APIConnector
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
        row = duck_conn.execute(
            "SELECT SUM(amount) FROM raw"
        ).fetchone()
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


class TestAPIConnector:
    def _mock_response(self, data, status_code=200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.json.return_value = data
        resp.raise_for_status = MagicMock()
        return resp

    def test_extract_flat_list(self, duck_conn):
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        with patch("connectors.api_connector.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(data)
            connector = APIConnector("https://example.com/api")
            connector.extract(duck_conn, "raw")
            result = duck_conn.execute("SELECT COUNT(*) FROM raw").fetchone()
            count = result[0]
            assert count == 2

    def test_extract_nested_json_path(self, duck_conn):
        data = {"data": {"results": [{"x": 10}, {"x": 20}]}}
        with patch("connectors.api_connector.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(data)
            connector = APIConnector(
                "https://example.com/api", json_path="data.results"
            )
            connector.extract(duck_conn, "raw")
            result = duck_conn.execute("SELECT COUNT(*) FROM raw").fetchone()
            count = result[0]
            assert count == 2

    def test_extract_bad_json_path_raises(self, duck_conn):
        data = {"data": []}
        with patch("connectors.api_connector.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(data)
            connector = APIConnector(
                "https://example.com/api", json_path="missing.key"
            )
            with pytest.raises(KeyError, match="json_path"):
                connector.extract(duck_conn, "raw")

    def test_test_connection_success(self):
        with patch("connectors.api_connector.requests.get") as mock_get:
            mock_get.return_value = self._mock_response({}, 200)
            connector = APIConnector("https://example.com/api")
            assert connector.test_connection() is True

    def test_test_connection_failure(self):
        with patch("connectors.api_connector.requests.get") as mock_get:
            mock_get.side_effect = Exception("timeout")
            connector = APIConnector("https://example.com/api")
            assert connector.test_connection() is False


class TestBaseConnectorIsAbstract:
    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError):
            BaseConnector()
