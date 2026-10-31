# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,00 - Shared Gold Production Configuration
import sys
import os
from pyspark.sql import functions as F

# Import shared module from the current notebook directory (same pattern as Silver).
WORKSPACE_DIR = os.getcwd()
if WORKSPACE_DIR not in sys.path:
    sys.path.insert(0, WORKSPACE_DIR)
from gold_spark import (
    route_statistics, stop_activity, expand_scheduled_trips,
    hourly_departures, daily_service,
)

SCHEMA = 'workspace.default'
INPUTS = ('routes', 'stops', 'trips', 'stop_times', 'calendar_dates')
OUTPUT_PREFIX = 'gold_'

SNAPSHOT_ID = spark.table(f'{SCHEMA}.silver_trips').agg(
    F.max('snapshot_id').alias('latest')
).first()['latest']
if not SNAPSHOT_ID:
    raise ValueError('No Silver snapshot available')

silver = {}
for name in INPUTS:
    df = spark.table(f'{SCHEMA}.silver_{name}').filter(F.col('snapshot_id') == SNAPSHOT_ID)
    if not df.limit(1).count():
        raise ValueError(f'Missing Silver {name} for {SNAPSHOT_ID}')
    silver[name] = df
    print(f'PASS: silver_{name} is available for {SNAPSHOT_ID}')
print(f'Processing Gold snapshot: {SNAPSHOT_ID}')
print(f'Output prefix: {SCHEMA}.{OUTPUT_PREFIX}')

# COMMAND ----------

# DBTITLE 1,01 - Shared Gold Route Statistics
gold_routes = route_statistics(silver['routes'], silver['trips'], silver['stop_times'])
print(f'Prepared route_statistics: {gold_routes.count():,} rows')

# COMMAND ----------

# DBTITLE 1,02 - Shared Gold Stop Activity
gold_stops = stop_activity(silver['stops'], silver['trips'], silver['stop_times'])
print(f'Prepared stop_activity: {gold_stops.count():,} rows')

# COMMAND ----------

# DBTITLE 1,03 - Shared Gold Scheduled Trips
# This BKK feed uses calendar_dates.txt without calendar.txt.
scheduled = expand_scheduled_trips(
    silver['trips'], silver['stop_times'], silver['calendar_dates']
)
print(f'Prepared scheduled trip-date rows: {scheduled.count():,}')

# COMMAND ----------

# DBTITLE 1,04 - Shared Gold Hourly and Daily Departures
gold_hourly = hourly_departures(scheduled, silver['routes'])
gold_daily = daily_service(gold_hourly)
print(f'Prepared hourly_departures: {gold_hourly.count():,} rows')
print(f'Prepared daily_service: {gold_daily.count():,} rows')

# COMMAND ----------

# DBTITLE 1,05 - Validate Outputs Before Writing
outputs = {
    'route_statistics': gold_routes,
    'stop_activity': gold_stops,
    'hourly_departures': gold_hourly,
    'daily_service': gold_daily,
}
for name, df in outputs.items():
    n = df.count()
    if n <= 0:
        raise ValueError(f'Empty Gold output: {name}')
    if df.filter(F.col('snapshot_id') != SNAPSHOT_ID).limit(1).count():
        raise ValueError(f'Unexpected snapshot in {name}')
    print(f'PASS: {name} = {n:,} rows')

hourly_total = gold_hourly.agg(F.sum('scheduled_departures')).first()[0]
daily_total = gold_daily.agg(F.sum('total_scheduled_departures')).first()[0]
if hourly_total != daily_total:
    raise ValueError(f'Hourly/daily mismatch: {hourly_total} vs {daily_total}')
print(f'PASS: departure totals reconcile: {hourly_total:,}')

# COMMAND ----------

# DBTITLE 1,06 - Save Production Gold Tables
for name, df in outputs.items():
    dest = f'{SCHEMA}.{OUTPUT_PREFIX}{name}'
    (
        df.write.format('delta').mode('overwrite')
        .option('replaceWhere', f"snapshot_id = '{SNAPSHOT_ID}'")
        .saveAsTable(dest)
    )
    actual = spark.table(dest).filter(F.col('snapshot_id') == SNAPSHOT_ID).count()
    expected = df.count()
    if actual != expected:
        raise ValueError(f'Persistence mismatch in {dest}: {actual} vs {expected}')
    print(f'PASS: {dest} = {actual:,} persisted rows')
print('PASS: Shared Gold production run completed')
