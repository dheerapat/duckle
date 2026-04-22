# Plan: From-To Date-Range Incremental Loading

## 1. Goal
Extend Duckle’s incremental loading to support extracting data within an explicit date range. The existing high-water-mark cursor behavior remains unchanged for callers who do not provide a range.

**Date semantics:** `[from, to)` — inclusive start, exclusive end.

> Example: `from=2004-01-01`, `to=2004-02-01` extracts **only January 2004** data.

---

## 2. API Changes

### 2.1 `IncrementalConfig` (`src/orchestrator/runner.py`)

```python
@dataclass
class IncrementalConfig:
    cursor_column: str
    merge_keys: list[str]
    default_full_refresh: bool = False

    # NEW — optional explicit bounds
    from_value: Optional[str] = None   # inclusive floor
    to_value: Optional[str] = None     # exclusive ceiling
```

### 2.2 FastAPI Request Body (`src/api/main.py`)

The new fields are accepted **inside the JSON request body**, not as query parameters.

```python
class IncrementalConfigModel(BaseModel):
    cursor_column: str
    merge_keys: list[str]
    default_full_refresh: bool = False
    from_value: Optional[str] = None   # inclusive
    to_value: Optional[str] = None     # exclusive
```

**Example request body:**
```json
{
  "name": "daily_revenue_backfill",
  "transforms": ["SELECT * FROM {{input}}"],
  "destination_name": "daily_revenue",
  "destination_layer": "gold",
  "source_type": "postgres",
  "source_config": { "connection_string": "...", "query": "SELECT * FROM orders" },
  "incremental": {
    "cursor_column": "order_date",
    "merge_keys": ["order_id"],
    "from_value": "2004-01-01",
    "to_value": "2004-02-01"
  }
}
```

---

## 3. Resolution Logic in `PipelineRunner.run()`

| Field | Resolution Rule |
|-------|-----------------|
| `from_value` | If provided explicitly → use it. Otherwise fall back to `storage.get_last_cursor()` (existing behavior). If neither exists → full load. |
| `to_value` | If provided explicitly → use it. Otherwise no ceiling (`None`). |

**SQL generated:**

| Bound source | Operator | Reason |
|---|---|---|
| Explicit `from_value` | `>=` | User explicitly requested this date; include it. |
| Stored cursor (high-water mark) | `>` | The cursor value was already processed in the previous run. |
| Explicit `to_value` | `<` | Exclusive ceiling; consistent `[from, to)` semantics. |

```sql
-- Explicit range
WHERE cursor >= 'from_value' AND cursor < 'to_value'

-- Stored cursor only (normal incremental, existing behavior)
WHERE cursor > 'stored_cursor'

-- Explicit from only
WHERE cursor >= 'from_value'
```

> **Note:** Stored cursor uses `>` so the boundary row is not re-fetched every run. `MERGE INTO` via `merge_keys` remains a safety net for overlapping ranges or backfills, but it is not the primary boundary guard.

---

## 4. Connector Changes

### 4.1 `BaseConnector.extract_incremental()`

```python
def extract_incremental(
    self,
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    cursor_column: str,
    since_value: str,
    until_value: Optional[str] = None,
    from_is_explicit: bool = False,
) -> None:
    ...
    # Choose operator based on whether 'since' came from explicit user input
    # or from the stored high-water mark.
    op = ">=" if from_is_explicit else ">"

    if until_value is not None:
        conn.execute(
            f"CREATE OR REPLACE TABLE {filtered} AS "
            f"SELECT * FROM {safe_tbl} WHERE {safe} {op} ? AND {safe} < ?",
            [since_value, until_value],
        )
    else:
        conn.execute(
            f"CREATE OR REPLACE TABLE {filtered} AS "
            f"SELECT * FROM {safe_tbl} WHERE {safe} {op} ?",
            [since_value],
        )
```

### 4.2 `PostgresConnector.extract_incremental()`

- Update signature to accept `until_value: Optional[str] = None`.
- Validate both `since_value` and `until_value` with `_validate_cursor_value()`.
- Build query:
  ```sql
  SELECT * FROM (...) AS _src WHERE safe_col >= 'since' AND safe_col < 'until'
  ```

### 4.3 `TableConnector.extract_incremental()`

- Update signature.
- Use parameterized queries for both bounds.

### 4.4 `CSVConnector.extract_incremental()`

- Inherits the same fallback logic from `BaseConnector`.

---

## 5. Cursor Storage & Update Policy

After a successful ranged run, the stored cursor **must be updated** so the pipeline can resume incrementally afterward.

**Rule:** Store the **maximum cursor value actually present in the transformed output**, not the request’s `to_value`.

```python
row = conn.execute(f"SELECT MAX({safe_cursor}) FROM {final_table}").fetchone()
new_cursor = row[0]

if new_cursor is not None:
    storage.update_pipeline_cursor(
        pipeline.destination_name,
        pipeline.destination_layer,
        cursor_column=incremental.cursor_column,
        last_cursor_value=str(new_cursor),
        merge_keys=incremental.merge_keys,
        run_mode=run_mode,
    )
```

**Empty batch policy:** If no rows match the range, `new_cursor` is `NULL`. **Do not advance the cursor.** Leave it at its previous value so late-arriving data in that range is not silently skipped.

---

## 6. Metadata Schema

No breaking changes to `_metadata`. The existing fields suffice:

| Column | Purpose |
|--------|---------|
| `last_cursor_value` | Stores the max processed cursor value after each run. |
| `cursor_column` | Name of the cursor column. |
| `run_mode` | `"full"` or `"incremental"`. |

Optionally add `to_value` and `from_value` columns later for **audit/traceability**, but this is not required for correctness.

---

## 7. Quality Checks

Quality rules run against the **delta batch** (the extracted & transformed range). No changes needed to `QualityChecker`.

---

## 8. Full Refresh Interaction

When `full_refresh=True` is passed to `PipelineRunner.run()`:

- **Ignore** `from_value` and `to_value`.
- Replace the entire destination table via `storage.write()`.
- Reset the stored cursor to `NULL` (or leave it untouched; to be decided).

This preserves the existing semantics: full refresh means "rebuild everything."

---

## 9. Test Scenarios

| # | Scenario | Expected Behavior |
|---|----------|-------------------|
| 1 | Explicit `from_value` + `to_value` | Extracts `[from, to)` range, merges, updates cursor to max seen. |
| 2 | `to_value` only (no explicit `from_value`) | Uses stored cursor as inclusive floor, explicit value as exclusive ceiling. |
| 3 | `from_value` < stored cursor | Explicit `from_value` wins; may re-process rows. `MERGE` deduplicates via `merge_keys`. |
| 4 | Empty result in range | No rows extracted. **Do not advance cursor.** Destination unchanged. |
| 5 | Full refresh + explicit range | Ignores bounds. Replaces entire table. |
| 6 | Backfill then resume | After `[Jan 1, Feb 1)` backfill, next incremental run (no bounds) starts from max cursor of Jan data. |
| 7 | Non-blocking quality fails in range | Pipeline aborts before merge. Cursor stays unchanged. |
| 8 | `to_value` == `from_value` | Empty range. No rows. Cursor unchanged. |

---

## 10. Backward Compatibility

- All new fields (`from_value`, `to_value`) are `Optional[str]` defaulting to `None`.
- Existing `IncrementalConfig(cursor_column="x", merge_keys=["id"])` works without modification.
- Existing stored cursors continue to function as inclusive lower bounds once `>` is changed to `>=`.
- The FastAPI `/run` endpoint accepts the same JSON shape; new fields are optional.

---

## 11. Files to Modify

1. `src/orchestrator/runner.py` — `IncrementalConfig`, `PipelineRunner.run()`
2. `src/connectors/base.py` — `extract_incremental()` signature & SQL
3. `src/connectors/postgres_connector.py` — `extract_incremental()` & validation
4. `src/connectors/table_connector.py` — `extract_incremental()` & SQL
5. `src/connectors/csv_connector.py` — inherits from base (verify no override needed)
6. `src/api/main.py` — `IncrementalConfigModel`
7. `tests/test_runner.py` — add ranged incremental tests
8. `tests/test_connectors.py` — add connector-level range tests
