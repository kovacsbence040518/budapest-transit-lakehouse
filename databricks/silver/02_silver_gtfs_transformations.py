# Databricks notebook source
# DBTITLE 1,00 - Shared Silver Production Configuration
import os
import sys
import re
from pyspark.sql import functions as F

# Keep silver_spark.py as a workspace Python file in the SAME Drafts folder as this notebook.
# Databricks workspace notebook working directory should resolve to its own folder.
MODULE_DIR = os.getcwd()
if MODULE_DIR not in sys.path:
    sys.path.insert(0, MODULE_DIR)

from silver_spark import (
    transform_routes,
    transform_stops,
    transform_trips,
    transform_stop_times,
    transform_calendar_dates,
    assert_references,
)

CATALOG_SCHEMA = "workspace.default"
SOURCE_PREFIX = "bronze_"
TARGET_PREFIX = "silver_"
DATASETS = ["routes", "stops", "trips", "stop_times", "calendar_dates"]

SNAPSHOT_ID = (
    spark.table(f"{CATALOG_SCHEMA}.bronze_trips")
    .agg(F.max("snapshot_id").alias("id"))
    .first()["id"]
)
if not SNAPSHOT_ID or not re.fullmatch(r"\d{8}T\d{6}Z", SNAPSHOT_ID):
    raise ValueError(f"Missing or invalid snapshot ID: {SNAPSHOT_ID}")

print(f"Shared module path: {MODULE_DIR}")
print(f"Processing snapshot: {SNAPSHOT_ID}")
print(f"Silver table prefix: {CATALOG_SCHEMA}.{TARGET_PREFIX}")


def source(dataset):
    data = spark.table(f"{CATALOG_SCHEMA}.{SOURCE_PREFIX}{dataset}")
    selected = data.filter(F.col("snapshot_id") == SNAPSHOT_ID)
    if selected.limit(1).count() == 0:
        raise ValueError(f"Missing Bronze {dataset} for snapshot {SNAPSHOT_ID}")
    return selected


def silver_table(dataset):
    return f"{CATALOG_SCHEMA}.{TARGET_PREFIX}{dataset}"

# COMMAND ----------
# DBTITLE 1,01 - Transform Routes
bronze_routes = source("routes")
silver_routes = transform_routes(bronze_routes)
print(f"Routes: Bronze={bronze_routes.count():,} Silver={silver_routes.count():,}")

# COMMAND ----------
# DBTITLE 1,02 - Transform Stops
bronze_stops = source("stops")
silver_stops = transform_stops(bronze_stops)
print(f"Stops: Bronze={bronze_stops.count():,} Silver={silver_stops.count():,}")

# COMMAND ----------
# DBTITLE 1,03 - Transform Trips
bronze_trips = source("trips")
silver_trips = transform_trips(bronze_trips)
print(f"Trips: Bronze={bronze_trips.count():,} Silver={silver_trips.count():,}")

# COMMAND ----------
# DBTITLE 1,04 - Transform Stop Times
bronze_stop_times = source("stop_times")
silver_stop_times = transform_stop_times(bronze_stop_times)
print(f"Stop times: Bronze={bronze_stop_times.count():,} Silver={silver_stop_times.count():,}")

# COMMAND ----------
# DBTITLE 1,05 - Transform Calendar Dates
bronze_calendar = source("calendar_dates")
silver_calendar = transform_calendar_dates(bronze_calendar)
print(f"Calendar dates: Bronze={bronze_calendar.count():,} Silver={silver_calendar.count():,}")

# COMMAND ----------
# DBTITLE 1,06 - Validate All Transformations Before Writing
frames = {
    "routes": silver_routes,
    "stops": silver_stops,
    "trips": silver_trips,
    "stop_times": silver_stop_times,
    "calendar_dates": silver_calendar,
}
for name, data in frames.items():
    if data.limit(1).count() == 0:
        raise ValueError(f"Empty Silver output for {name}")
    bad_snapshot = data.filter(F.col("snapshot_id") != SNAPSHOT_ID).limit(1).count()
    if bad_snapshot:
        raise ValueError(f"Mixed snapshot data in {name}")

assert_references(silver_trips, silver_routes,
                  ["snapshot_id", "route_id"], "trips missing routes")
assert_references(silver_stop_times, silver_trips,
                  ["snapshot_id", "trip_id"], "stop times missing trips")
assert_references(silver_stop_times, silver_stops,
                  ["snapshot_id", "stop_id"], "stop times missing stops")
print("PASS: all five transformations have data, and references are valid")

# COMMAND ----------
# DBTITLE 1,07 - Write Production Delta Tables and Audit
# Writes happen only after all validation passes. Separate Delta tables are not a single transaction.
for name, data in frames.items():
    (
        data.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'")
        .saveAsTable(silver_table(name))
    )
    expected = data.count()
    actual = spark.table(silver_table(name)).filter(F.col("snapshot_id") == SNAPSHOT_ID).count()
    if expected != actual:
        raise ValueError(f"Silver persistence mismatch for {name}: {expected} vs {actual}")
    print(f"PASS: {name}: {actual:,} persisted rows")
print("PASS: shared Silver production run completed")
