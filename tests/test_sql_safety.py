"""Tests for the sql_safety utility module."""

import pytest
from utils.sql_safety import safe_identifier, safe_path


class TestSafeIdentifier:
    def test_valid_identifiers(self):
        assert safe_identifier("my_table") == "my_table"
        assert safe_identifier("_private") == "_private"
        assert safe_identifier("Table1") == "Table1"
        assert safe_identifier("a") == "a"
        assert safe_identifier("Z_123") == "Z_123"

    def test_reject_spaces(self):
        with pytest.raises(ValueError, match="Unsafe"):
            safe_identifier("my table")

    def test_reject_dots(self):
        with pytest.raises(ValueError, match="Unsafe"):
            safe_identifier("schema.table")

    def test_reject_hyphens(self):
        with pytest.raises(ValueError, match="Unsafe"):
            safe_identifier("my-table")

    def test_reject_quotes(self):
        with pytest.raises(ValueError, match="Unsafe"):
            safe_identifier('"; DROP TABLE --')

    def test_reject_starts_with_digit(self):
        with pytest.raises(ValueError, match="Unsafe"):
            safe_identifier("1table")

    def test_reject_empty(self):
        with pytest.raises(ValueError, match="Unsafe"):
            safe_identifier("")

    def test_custom_label_in_error(self):
        with pytest.raises(ValueError, match="table_name"):
            safe_identifier("bad name", label="table_name")


class TestSafePath:
    def test_valid_paths(self):
        assert safe_path("/tmp/data/file.csv") == "/tmp/data/file.csv"
        assert safe_path("./data/sales.csv") == "./data/sales.csv"
        assert safe_path("file.parquet") == "file.parquet"

    def test_reject_single_quotes(self):
        with pytest.raises(ValueError, match="Unsafe file path"):
            safe_path("/tmp/'; DROP TABLE--/file.csv")

    def test_reject_double_quotes(self):
        with pytest.raises(ValueError, match="Unsafe file path"):
            safe_path('/tmp/"; DROP TABLE--/file.csv')

    def test_reject_pipe(self):
        with pytest.raises(ValueError, match="Unsafe file path"):
            safe_path("/tmp/data | rm -rf/file.csv")

    def test_reject_backtick(self):
        with pytest.raises(ValueError, match="Unsafe file path"):
            safe_path("/tmp/`cmd`/file.csv")

    def test_reject_dollar(self):
        with pytest.raises(ValueError, match="Unsafe file path"):
            safe_path("/tmp/$HOME/file.csv")

    def test_reject_semicolon(self):
        with pytest.raises(ValueError, match="Unsafe file path"):
            safe_path("/tmp/;rm/file.csv")
