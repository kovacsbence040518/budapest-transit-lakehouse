# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,00 - Configuration and Shared Module
import sys
import os
from pyspark.sql import functions as F

# Import shared module from the current notebook directory (same pattern as Silver).
MODULE_DIR = os.getcwd()
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)
from snapshot_comparison import compare_snapshot_pair

SCHEMA = 'workspace.default'
OUTPUT_TABLE = f'{SCHEMA}.gold_snapshot_changes'

# COMMAND ----------

# DBTITLE 1,01 - Select Latest Two Complete Snapshots
required_tables = ['silver_routes', 'silver_stops', 'silver_trips', 'gold_daily_service']
available = None
for name in required_tables:
    ids = {r['snapshot_id'] for r in spark.table(f'{SCHEMA}.{name}').select('snapshot_id').distinct().collect()}
    available = ids if available is None else (available & ids)
    print(f'{name}: {len(ids)} snapshot(s)')
if not available or len(available) < 2:
    raise ValueError('Need at least two snapshots present in all required tables')
PREVIOUS_ID, CURRENT_ID = sorted(available)[-2:]
print(f'Previous snapshot: {PREVIOUS_ID}')
print(f'Current snapshot:  {CURRENT_ID}')

# COMMAND ----------

# DBTITLE 1,02 - Read Inputs
routes = spark.table(f'{SCHEMA}.silver_routes').filter(F.col('snapshot_id').isin(PREVIOUS_ID, CURRENT_ID))
stops = spark.table(f'{SCHEMA}.silver_stops').filter(F.col('snapshot_id').isin(PREVIOUS_ID, CURRENT_ID))
trips = spark.table(f'{SCHEMA}.silver_trips').filter(F.col('snapshot_id').isin(PREVIOUS_ID, CURRENT_ID))
daily = spark.table(f'{SCHEMA}.gold_daily_service').filter(F.col('snapshot_id').isin(PREVIOUS_ID, CURRENT_ID))
print('Loaded input snapshots from Silver and Gold')

# COMMAND ----------

# DBTITLE 1,03 - Calculate Snapshot Comparison
comparison = compare_snapshot_pair(routes, stops, trips, daily, PREVIOUS_ID, CURRENT_ID)
display(comparison)

# COMMAND ----------

# DBTITLE 1,04 - Validate Changes Before Write
result = comparison.first()
for name in ('routes', 'stops', 'trips'):
    old_count = result[f'previous_{name}']
    new_count = result[f'current_{name}']
    added = result[f'{name}_added']
    removed = result[f'{name}_removed']
    if old_count + added - removed != new_count:
        raise ValueError(f'Count reconciliation failed for {name}')
    print(f'PASS: {name}: {old_count:,} + {added:,} - {removed:,} = {new_count:,}')
print(f"PASS: Compared {result['shared_departure_dates']} shared service date(s)")
print(f"Scheduled departure change on shared dates: {result['shared_date_departures_change']:+,}")

# COMMAND ----------

# DBTITLE 1,05 - Save Production Snapshot Comparison
(
    comparison.write.format('delta').mode('overwrite')
    .option('replaceWhere', f"current_snapshot_id = '{CURRENT_ID}'")
    .saveAsTable(OUTPUT_TABLE)
)
actual = spark.table(OUTPUT_TABLE).filter(F.col('current_snapshot_id') == CURRENT_ID)
if actual.count() != 1:
    raise ValueError('Expected exactly one saved comparison for current snapshot')
print(f'PASS: Saved production comparison to {OUTPUT_TABLE}')
display(actual)