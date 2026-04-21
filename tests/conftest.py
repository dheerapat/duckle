"""Shared test fixtures for Duckle."""

import csv
import os
import sys
import tempfile

import duckdb
import pytest

# Ensure src/ is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


@pytest.fixture
def duck_conn():
    """Provide a fresh in-memory DuckDB connection, closed after the test."""
    conn = duckdb.connect()
    yield conn
    conn.close()


@pytest.fixture
def sample_csv(tmp_path):
    """Create a small CSV file and return its path."""
    csv_path = tmp_path / "sample.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "product", "amount"])
        writer.writerows(
            [
                ["2024-01-01", "Widget A", 100],
                ["2024-01-01", "Widget B", 200],
                ["2024-01-02", "Widget A", 150],
                ["2024-01-02", "Widget C", 300],
                ["2024-01-03", "Widget B", 250],
            ]
        )
    return str(csv_path)


@pytest.fixture
def ducklake_storage(tmp_path, monkeypatch):
    """Provide a DuckLakeStorage instance rooted in a temp dir, closed after test."""
    from storage.ducklake import DuckLakeStorage

    lake = str(tmp_path / "lake")
    monkeypatch.setenv("DUCKLAKE_CATALOG_PATH", f"{lake}/catalog.ducklake")
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", f"{lake}/ducklake.files")
    monkeypatch.setenv("DUCKLAKE_METADATA_PATH", f"{lake}/_metadata.db")

    storage = DuckLakeStorage()
    yield storage
    storage.close()
