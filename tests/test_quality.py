"""Tests for the quality checker."""

import pytest
from quality.checker import (
    QualityChecker,
    QualityRule,
    no_nulls,
    min_row_count,
    column_positive,
    unique_column,
)


class TestQualityChecker:
    def test_passing_rule(self, duck_conn):
        duck_conn.execute("CREATE TABLE test AS SELECT 1 AS id, 'Alice' AS name")
        rule = QualityRule(
            name="always_true",
            sql="SELECT TRUE AS passed FROM {{table}}",
            description="Always passes",
        )
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [rule])
        assert result["passed"] is True

    def test_failing_blocking_rule(self, duck_conn):
        duck_conn.execute("CREATE TABLE test AS SELECT NULL AS name")
        rule = no_nulls("name")
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [rule])
        assert result["passed"] is False
        assert result["results"][0]["passed"] is False
        assert result["results"][0]["blocking"] is True

    def test_non_blocking_rule_does_not_fail_pipeline(self, duck_conn):
        duck_conn.execute("CREATE TABLE test AS SELECT NULL AS name")
        rule = QualityRule(
            name="nulls_ok",
            sql="SELECT COUNT(*) = 0 AS passed FROM {{table}} WHERE name IS NULL",
            description="Non-blocking null check",
            blocking=False,
        )
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [rule])
        # overall passes because rule is non-blocking
        assert result["passed"] is True
        # but the individual rule failed
        assert result["results"][0]["passed"] is False

    def test_no_nulls_builtin(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE test AS SELECT 'Alice' AS name UNION ALL SELECT 'Bob'"
        )
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [no_nulls("name")])
        assert result["passed"] is True

    def test_min_row_count_builtin(self, duck_conn):
        duck_conn.execute("CREATE TABLE test AS SELECT range AS id FROM range(5)")
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [min_row_count(3)])
        assert result["passed"] is True

    def test_min_row_count_fails(self, duck_conn):
        duck_conn.execute("CREATE TABLE test AS SELECT 1 AS id")
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [min_row_count(5)])
        assert result["passed"] is False

    def test_column_positive_builtin(self, duck_conn):
        duck_conn.execute("CREATE TABLE test AS SELECT 5 AS amount UNION ALL SELECT 10")
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [column_positive("amount")])
        assert result["passed"] is True

    def test_column_positive_fails_on_zero(self, duck_conn):
        duck_conn.execute("CREATE TABLE test AS SELECT 0 AS amount")
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [column_positive("amount")])
        assert result["passed"] is False

    def test_unique_column_builtin(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE test AS SELECT 'a' AS letter UNION ALL SELECT 'b'"
        )
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [unique_column("letter")])
        assert result["passed"] is True

    def test_unique_column_fails(self, duck_conn):
        duck_conn.execute(
            "CREATE TABLE test AS SELECT 'a' AS letter UNION ALL SELECT 'a'"
        )
        checker = QualityChecker(duck_conn)
        result = checker.check("test", [unique_column("letter")])
        assert result["passed"] is False

    def test_rejects_unsafe_table_name(self, duck_conn):
        checker = QualityChecker(duck_conn)
        with pytest.raises(ValueError, match="Unsafe"):
            checker.check("; DROP TABLE", [no_nulls("id")])

    def test_min_row_count_rejects_negative(self):
        with pytest.raises(ValueError, match="non-negative"):
            min_row_count(-1)
