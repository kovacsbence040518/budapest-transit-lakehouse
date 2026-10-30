# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,00 - Select and Validate Silver Snapshot
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCHEMA = "workspace.default"
SILVER_PREFIX = "silver_"
GOLD_PREFIX = "gold_"
DATASETS = ["routes", "stops", "trips", "stop_times", "calendar_dates"]


def silver_table(name):
    return f"{SCHEMA}.{SILVER_PREFIX}{name}"


def gold_table(name):
    return f"{SCHEMA}.{GOLD_PREFIX}{name}"


# Select newest trips snapshot; refuse to build Gold if other inputs are missing.
SNAPSHOT_ID = (
    spark.table(silver_table("trips"))
    .agg(F.max("snapshot_id").alias("latest_snapshot"))
    .first()["latest_snapshot"]
)
if not SNAPSHOT_ID:
    raise ValueError("No trip snapshot found in Silver")

silver = {}
for name in DATASETS:
    df = spark.table(silver_table(name)).filter(F.col("snapshot_id") == SNAPSHOT_ID)
    if not df.limit(1).count():
        raise ValueError(f"Missing or empty Silver input: {name}, snapshot={SNAPSHOT_ID}")
    silver[name] = df
    print(f"PASS: Silver {name} contains snapshot {SNAPSHOT_ID}")

print(f"Building Gold snapshot: {SNAPSHOT_ID}")

# COMMAND ----------

# DBTITLE 1,01 - Validate Keys and References Before Gold
# Fail before writing ANY Gold tables if required source keys are broken.
keys = {
    "routes": ["route_id"],
    "stops": ["stop_id"],
    "trips": ["trip_id"],
    "stop_times": ["trip_id", "stop_sequence"],
    "calendar_dates": ["service_id", "service_date"],
}

for name, key_cols in keys.items():
    df = silver[name]
    invalid = df.filter(
        " OR ".join(f"`{col}` IS NULL" for col in key_cols)
    ).limit(1).count()
    duplicates = (
        df.groupBy(*key_cols).count().filter(F.col("count") > 1).limit(1).count()
    )
    if invalid or duplicates:
        raise ValueError(f"Invalid or duplicate keys in {name}: invalid={invalid}, duplicate_groups={duplicates}")
    print(f"PASS: {name} keys")

references = [
    ("Trips -> routes", silver["trips"], silver["routes"], ["route_id"]),
    ("Stop times -> trips", silver["stop_times"], silver["trips"], ["trip_id"]),
    ("Stop times -> stops", silver["stop_times"], silver["stops"], ["stop_id"]),
]
for label, child, parent, join_keys in references:
    orphan = child.join(parent.select(*join_keys), join_keys, "left_anti").limit(1).count()
    if orphan:
        raise ValueError(f"Broken reference: {label}")
    print(f"PASS: {label}")

# Feed has calendar_dates.txt but no calendar.txt: additions define service dates.
invalid_calendar = silver["calendar_dates"].filter(
    ~F.col("exception_type").isin(1, 2)
).limit(1).count()
if invalid_calendar:
    raise ValueError("Calendar contains unknown exception_type values")

# COMMAND ----------

# DBTITLE 1,02 - Build Gold Route Statistics
routes = silver["routes"]
trips = silver["trips"]
stop_times = silver["stop_times"]

trip_counts = trips.groupBy("snapshot_id", "route_id").agg(
    F.countDistinct("trip_id").alias("total_trips")
)
route_stop_events = stop_times.join(
    trips.select("snapshot_id", "trip_id", "route_id"),
    ["snapshot_id", "trip_id"], "inner"
)
stop_counts = route_stop_events.groupBy("snapshot_id", "route_id").agg(
    F.countDistinct("stop_id").alias("unique_stops")
)

gold_routes = (
    routes.select("snapshot_id", "route_id", "route_short_name", "route_type")
    .join(trip_counts, ["snapshot_id", "route_id"], "left")
    .join(stop_counts, ["snapshot_id", "route_id"], "left")
    .fillna({"total_trips": 0, "unique_stops": 0})
)
print("Prepared Gold route statistics")

# COMMAND ----------

# DBTITLE 1,03 - Build Gold Stop Activity
gold_stop_events = route_stop_events.groupBy("snapshot_id", "stop_id").agg(
    F.count("*").alias("total_stop_events"),
    F.countDistinct("trip_id").alias("unique_trips"),
    F.countDistinct("route_id").alias("unique_routes"),
)
gold_stops = (
    silver["stops"]
    .select("snapshot_id", "stop_id", "stop_name", "stop_lat", "stop_lon")
    .join(gold_stop_events, ["snapshot_id", "stop_id"], "left")
    .fillna({"total_stop_events": 0, "unique_trips": 0, "unique_routes": 0})
)
print("Prepared Gold stop activity")

# COMMAND ----------

# DBTITLE 1,04 - Build Gold Hourly and Daily Departures
# Use the first stop by stop_sequence; MIN(departure_seconds) may select a later stop.
first_stop_window = Window.partitionBy("snapshot_id", "trip_id").orderBy("stop_sequence")
trip_starts = (
    stop_times
    .withColumn("stop_rank", F.row_number().over(first_stop_window))
    .filter(F.col("stop_rank") == 1)
    .select("snapshot_id", "trip_id", F.col("departure_seconds").alias("first_departure_seconds"))
)
if trip_starts.filter(
    F.col("first_departure_seconds").isNull() |
    (F.col("first_departure_seconds") < 0)
).limit(1).count():
    raise ValueError("A trip has no valid departure time at its first stop")

active_services = (
    silver["calendar_dates"]
    .filter(F.col("exception_type") == 1)
    .select("snapshot_id", "service_id", "service_date")
    .dropDuplicates()
)

scheduled_trips = (
    trips.select("snapshot_id", "trip_id", "route_id", "service_id")
    .join(trip_starts, ["snapshot_id", "trip_id"], "inner")
    .join(active_services, ["snapshot_id", "service_id"], "inner")
    .withColumn(
        "departure_date",
        F.date_add(
            F.col("service_date"),
            F.floor(F.col("first_departure_seconds") / F.lit(86400)).cast("int")
        )
    )
    .withColumn(
        "departure_hour",
        F.floor(F.pmod("first_departure_seconds", F.lit(86400)) / F.lit(3600)).cast("int")
    )
)

gold_hourly = (
    scheduled_trips
    .groupBy("snapshot_id", "route_id", "departure_date", "departure_hour")
    .agg(F.count("*").alias("scheduled_departures"))
    .join(
        routes.select("snapshot_id", "route_id", "route_short_name", "route_type"),
        ["snapshot_id", "route_id"], "left"
    )
)

gold_daily = (
    gold_hourly.groupBy("snapshot_id", "departure_date")
    .agg(
        F.sum("scheduled_departures").alias("total_scheduled_departures"),
        F.countDistinct("route_id").alias("active_routes"),
        F.countDistinct("route_type").alias("transport_types")
    )
)
print("Prepared Gold hourly departures and daily service")

# COMMAND ----------

# DBTITLE 1,05 - Validate All Gold Results BEFORE Writing
outputs = {
    "route_statistics": gold_routes,
    "stop_activity": gold_stops,
    "hourly_departures": gold_hourly,
    "daily_service": gold_daily,
}

for name, df in outputs.items():
    count = df.count()
    if count == 0:
        raise ValueError(f"Empty Gold result: {name}; refusing to overwrite")
    bad_snapshots = df.filter(F.col("snapshot_id") != SNAPSHOT_ID).limit(1).count()
    if bad_snapshots:
        raise ValueError(f"Unexpected snapshot in Gold result: {name}")
    print(f"PASS: {name} = {count:,} rows")

# The daily summary must reconcile with the hourly detail.
hourly_total = gold_hourly.agg(F.sum("scheduled_departures")).first()[0]
daily_total = gold_daily.agg(F.sum("total_scheduled_departures")).first()[0]
if hourly_total != daily_total:
    raise ValueError(f"Hourly/daily departure mismatch: {hourly_total} != {daily_total}")
print(f"PASS: Hourly and daily totals reconcile: {hourly_total:,} scheduled departures")

# COMMAND ----------

# DBTITLE 1,06 - Write Snapshot-Scoped Gold Delta Tables
# replaceWhere replaces only this snapshot. Other snapshots remain intact.
for name, df in outputs.items():
    destination = gold_table(name)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'")
        .saveAsTable(destination)
    )
    print(f"SAVED: {destination}, snapshot={SNAPSHOT_ID}")

# COMMAND ----------

# DBTITLE 1,07 - Verify Persisted Gold Tables
for name, df in outputs.items():
    actual_count = spark.table(gold_table(name)).filter(
        F.col("snapshot_id") == SNAPSHOT_ID
    ).count()
    expected_count = df.count()
    if actual_count != expected_count:
        raise ValueError(f"Persisted count differs for {name}: {actual_count} != {expected_count}")
    print(f"PASS: {name} = {actual_count:,} persisted rows")

display(
    spark.table(gold_table("daily_service"))
    .filter(F.col("snapshot_id") == SNAPSHOT_ID)
    .orderBy("departure_date")
)