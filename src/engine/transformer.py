from typing import List

import duckdb


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
        current_table = source_table

        for i, sql in enumerate(steps):
            output_table = f"_transform_step_{i}"
            # Allow referencing previous step as "raw" or by step table name
            resolved_sql = sql.replace("{{input}}", current_table)
            self.conn.execute(f"""
                CREATE OR REPLACE TABLE {output_table} AS {resolved_sql}
            """)
            count = self.conn.execute(
                f"SELECT COUNT(*) FROM {output_table}"
            ).fetchone()[0]
            print(f"[Transformer] Step {i + 1}: {count} rows → {output_table}")
            current_table = output_table

        return current_table

    def preview(self, table: str, limit: int = 5) -> list:
        """Return a sample of rows from a table."""
        return (
            self.conn.execute(f"SELECT * FROM {table} LIMIT {limit}")
            .fetchdf()
            .to_dict("records")
        )
