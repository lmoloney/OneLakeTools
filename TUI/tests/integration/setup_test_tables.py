"""Setup script for integration test tables in a Fabric Lakehouse.

This script creates the Delta tables expected by integration tests,
including the CDF-enabled-after-creation scenario.

Prerequisites:
    - An existing Fabric Lakehouse accessible via Spark
    - Run from a Fabric notebook or Spark environment with write access

Usage (in a Fabric notebook cell):
    %run ./setup_test_tables

Or locally (if you have a Spark-connected environment):
    python tests/integration/setup_test_tables.py

Tables created:
    - cdf_tracking: Created WITHOUT CDF, data inserted, CDF enabled,
      then more data inserted/updated. Tests the partial-enable scenario.

See fabric-test-env.json for the expected table configurations.
"""

from __future__ import annotations

# ── PySpark setup code (for Fabric notebooks) ──────────────────────────
#
# Paste the following into a Fabric notebook cell to create the tables.
# This is provided as documentation since setup_test_tables.py can't
# directly connect to Fabric Spark from a local environment.

NOTEBOOK_CELLS = """
# Cell 1: Create cdf_tracking table WITHOUT CDF enabled
# ─────────────────────────────────────────────────────────────────────

spark.sql('''
    CREATE TABLE IF NOT EXISTS cdf_tracking (
        id INT,
        name STRING,
        status STRING,
        updated_at TIMESTAMP
    )
    USING DELTA
''')

# Cell 2: Insert initial data (versions 0-1, no CDF)
# ─────────────────────────────────────────────────────────────────────

from pyspark.sql import Row
from datetime import datetime

initial_data = [
    Row(id=1, name="Alice", status="active", updated_at=datetime(2024, 1, 1)),
    Row(id=2, name="Bob", status="active", updated_at=datetime(2024, 1, 1)),
    Row(id=3, name="Carol", status="active", updated_at=datetime(2024, 1, 1)),
]
df = spark.createDataFrame(initial_data)
df.write.mode("append").format("delta").saveAsTable("cdf_tracking")

# Cell 3: Enable CDF (version 2-3)
# ─────────────────────────────────────────────────────────────────────

spark.sql('''
    ALTER TABLE cdf_tracking
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
''')

# Cell 4: Insert + Update data WITH CDF enabled (versions 4+)
# ─────────────────────────────────────────────────────────────────────

# Insert new rows
new_data = [
    Row(id=4, name="Dave", status="active", updated_at=datetime(2024, 6, 1)),
    Row(id=5, name="Eve", status="active", updated_at=datetime(2024, 6, 1)),
]
df_new = spark.createDataFrame(new_data)
df_new.write.mode("append").format("delta").saveAsTable("cdf_tracking")

# Update existing rows
spark.sql('''
    UPDATE cdf_tracking SET status = 'inactive', updated_at = current_timestamp()
    WHERE id = 2
''')

# Delete a row
spark.sql("DELETE FROM cdf_tracking WHERE id = 3")

# Cell 5: Verify
# ─────────────────────────────────────────────────────────────────────

from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "cdf_tracking")
print(f"Version: {dt.history().count()}")
props = spark.sql('SHOW TBLPROPERTIES cdf_tracking')
cdf_prop = props.filter('key = \\'delta.enableChangeDataFeed\\'').collect()
print(f"CDF enabled: {cdf_prop}")
print(f"Row count: {spark.table('cdf_tracking').count()}")
"""

if __name__ == "__main__":
    print("This script provides PySpark cell content for Fabric notebooks.")
    print("Copy the cells from NOTEBOOK_CELLS into a Fabric notebook to create test tables.")
    print()
    print("─" * 70)
    print(NOTEBOOK_CELLS)
