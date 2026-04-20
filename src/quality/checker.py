from dataclasses import dataclass
from typing import List

import duckdb


@dataclass
class QualityRule:
    name: str
    sql: str  # Must return a single boolean column 'passed'
    description: str
    blocking: bool = True  # If True, pipeline fails on violation


class QualityChecker:
    """Run data quality checks against a DuckDB table."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn

    def check(self, table: str, rules: List[QualityRule]) -> dict:
        results = []
        all_passed = True

        for rule in rules:
            sql = rule.sql.replace("{{table}}", table)
            row = self.conn.execute(sql).fetchone()
            passed = bool(row[0]) if row else False

            if not passed and rule.blocking:
                all_passed = False

            results.append(
                {
                    "rule": rule.name,
                    "description": rule.description,
                    "passed": passed,
                    "blocking": rule.blocking,
                }
            )
            status = "✅" if passed else "❌"
            print(f"[Quality] {status} {rule.name}: {rule.description}")

        return {"passed": all_passed, "results": results}


# --- Built-in common rules ---


def no_nulls(column: str) -> QualityRule:
    return QualityRule(
        name=f"no_nulls_{column}",
        sql=f"SELECT COUNT(*) = 0 AS passed FROM {{{{table}}}} WHERE {column} IS NULL",
        description=f"No nulls in column '{column}'",
    )


def min_row_count(n: int) -> QualityRule:
    return QualityRule(
        name="min_row_count",
        sql=f"SELECT COUNT(*) >= {n} AS passed FROM {{{{table}}}}",
        description=f"At least {n} rows expected",
    )


def column_positive(column: str) -> QualityRule:
    return QualityRule(
        name=f"{column}_positive",
        sql=f"SELECT MIN({column}) > 0 AS passed FROM {{{{table}}}}",
        description=f"All values in '{column}' must be positive",
    )


def unique_column(column: str) -> QualityRule:
    return QualityRule(
        name=f"{column}_unique",
        sql=f"SELECT COUNT(*) = COUNT(DISTINCT {column}) AS passed FROM {{{{table}}}}",
        description=f"All values in '{column}' must be unique",
    )
