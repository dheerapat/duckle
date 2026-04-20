"""
Example: Run a full ETL pipeline
  - Source: CSV file
  - Transform: aggregate sales by date
  - Quality: no nulls, min row count
  - Load: DuckLake gold layer
"""

import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from connectors.csv_connector import CSVConnector

from orchestrator.runner import Pipeline, PipelineRunner
from quality.checker import column_positive, min_row_count, no_nulls
from storage.ducklake import DuckLakeStorage

# --- Create sample data ---
os.makedirs("data", exist_ok=True)
with open("data/sales.csv", "w", newline="") as f:
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

# --- Define pipeline ---
pipeline = Pipeline(
    name="daily_revenue",
    source=CSVConnector("data/sales.csv"),
    transforms=[
        # Step 1: aggregate by date
        "SELECT date, SUM(amount) as revenue, COUNT(*) as orders FROM {{input}} GROUP BY date",
        # Step 2: add a simple label
        "SELECT *, CASE WHEN revenue > 200 THEN 'high' ELSE 'normal' END as day_type FROM {{input}}",
    ],
    destination_name="daily_revenue",
    destination_layer="gold",
    quality_rules=[
        no_nulls("date"),
        no_nulls("revenue"),
        column_positive("revenue"),
        min_row_count(1),
    ],
)

# --- Run it ---
runner = PipelineRunner(storage=DuckLakeStorage("./lake"))
result = runner.run(pipeline)

print("\n--- Run Result ---")
for k, v in result.items():
    print(f"  {k}: {v}")

# --- Preview the output ---
print("\n--- Preview gold/daily_revenue ---")
rel = runner.storage.read("daily_revenue", "gold")
print(rel.fetchdf().to_string(index=False))
runner.storage.close()
