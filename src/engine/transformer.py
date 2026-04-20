from typing import List

import duckdb
from utils.sql_safety import safe_identifier


class Transformer:
    """
    Runs SQL transformations inside DuckDB.
    Each step takes the output of the previous step as input.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def run(self, steps: List[str], source_table: str) -> str:
        """
        Run a list of SQL transform steps.
        Returns the name of the final output table.
        """
        source_table = safe_identifier(source_table, label="source_table")
        current_table = source_table

        for i, sql in enumerate(steps):
            output_table = safe_identifier(f"_transform_step_{i}", label="output_table")
            # Allow referencing previous step via {{input}} placeholder
            resolved_sql = sql.replace("{{input}}", current_table)
            self.conn.execute(
                f"CREATE OR REPLACE TABLE {output_table} AS {resolved_sql}"
            )
            result = self.conn.execute(
                f"SELECT COUNT(*) FROM {output_table}"
            ).fetchone()
            count = result[0] if result is not None else 0
            print(f"[Transformer] Step {i + 1}: {count} rows → {output_table}")
            current_table = output_table

        return current_table

    def preview(self, table: str, limit: int = 5) -> list:
        """Return a sample of rows from a table."""
        table = safe_identifier(table, label="table")
        if not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        return (
            self.conn.execute(f"SELECT * FROM {table} LIMIT {int(limit)}")
            .fetchdf()
            .to_dict("records")
        )
