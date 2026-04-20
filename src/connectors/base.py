from abc import ABC, abstractmethod
import duckdb


class BaseConnector(ABC):
    """All source connectors implement this interface."""

    @abstractmethod
    def extract(self, conn: duckdb.DuckDBPyConnection, table_name: str) -> None:
        """Extract data from source and register it as a DuckDB table."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Verify the source is reachable."""
        pass
