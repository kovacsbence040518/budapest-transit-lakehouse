# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,00 - Configure Silver Snapshot and Output
from pyspark.sql import functions as F
import re

# Main Silver tables: snapshot-scoped Delta writes.
CATALOG_SCHEMA = "workspace.default"
SOURCE_PREFIX = "bronze_"
TARGET_PREFIX = "silver_"

SNAPSHOT_ID = (
    spark.table(f"{CATALOG_SCHEMA}.bronze_trips")
    .agg(F.max("snapshot_id").alias("id"))
    .first()["id"]
)
if not SNAPSHOT_ID:
    raise ValueError("Bronze contains no snapshot IDs")
if not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", SNAPSHOT_ID):
    raise ValueError(f"Unexpected snapshot ID format: {SNAPSHOT_ID}")

print(f"Processing Silver snapshot: {SNAPSHOT_ID}")
print(f"Output prefix: {CATALOG_SCHEMA}.{TARGET_PREFIX}")

def bronze(dataset):
    df = spark.table(f"{CATALOG_SCHEMA}.{SOURCE_PREFIX}{dataset}")
    selected = df.filter(F.col("snapshot_id") == SNAPSHOT_ID)
    if selected.limit(1).count() == 0:
        raise ValueError(f"Missing Bronze {dataset} for snapshot {SNAPSHOT_ID}")
    return selected

def target(dataset):
    return f"{CATALOG_SCHEMA}.{TARGET_PREFIX}{dataset}"

def save_snapshot(df, dataset):
    # Check before writing so a failed transformation cannot erase existing rows.
    if df.limit(1).count() == 0:
        raise ValueError(f"Refusing to write empty Silver {dataset} for {SNAPSHOT_ID}")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_id = '{SNAPSHOT_ID}'")
        .saveAsTable(target(dataset))
    )
    print(f"Saved {dataset}: {df.count():,} rows, snapshot={SNAPSHOT_ID}")

# COMMAND ----------

# DBTITLE 1,silver_routes
from pyspark.sql import functions as F

bronze_routes = bronze("routes")

silver_routes = (
    bronze_routes
    .select(
        F.trim("route_id").alias("route_id"),
        F.trim("agency_id").alias("agency_id"),
        F.trim("route_short_name").alias("route_short_name"),
        F.trim("route_long_name").alias("route_long_name"),
        F.col("route_type").cast("int").alias("route_type"),
        F.col("snapshot_id"),
        F.col("ingested_at")
    )
    .filter(
        F.col("route_id").isNotNull() &
        (F.col("route_id") != "") &
        F.col("route_type").isNotNull()
    )
    .dropDuplicates(["route_id", "snapshot_id"])
)

save_snapshot(silver_routes, "routes")

print(f"Silver routes loaded: {silver_routes.count()} rows")

display(spark.table(target("routes")).limit(5))

# COMMAND ----------

# DBTITLE 1,02 - Silver Stops Transformation
from pyspark.sql import functions as F

bronze_stops = bronze("stops")

silver_stops = (
    bronze_stops
    .select(
        F.trim(F.col("stop_id")).alias("stop_id"),
        F.trim(F.col("stop_name")).alias("stop_name"),
        F.col("stop_lat").cast("double").alias("stop_lat"),
        F.col("stop_lon").cast("double").alias("stop_lon"),
        F.col("location_type").cast("int").alias("location_type"),
        F.trim(F.col("parent_station")).alias("parent_station"),
        F.col("wheelchair_boarding").cast("int").alias("wheelchair_boarding"),
        F.col("snapshot_id"),
        F.col("ingested_at")
    )
    .filter(
        F.col("stop_id").isNotNull() &
        (F.col("stop_id") != "") &
        F.col("stop_name").isNotNull() &
        F.col("stop_lat").between(-90, 90) &
        F.col("stop_lon").between(-180, 180)
    )
    .dropDuplicates(["stop_id", "snapshot_id"])
)

save_snapshot(silver_stops, "stops")

print(f"Bronze stops: {bronze_stops.count()}")
print(f"Silver stops: {silver_stops.count()}")

display(spark.table(target("stops")).limit(5))

# COMMAND ----------

# DBTITLE 1,03 - Silver Trips Transformation
from pyspark.sql import functions as F

bronze_trips = bronze("trips")

silver_trips = (
    bronze_trips
    .select(
        F.trim("trip_id").alias("trip_id"),
        F.trim("route_id").alias("route_id"),
        F.trim("service_id").alias("service_id"),
        F.trim("trip_headsign").alias("trip_headsign"),
        F.col("direction_id").cast("int").alias("direction_id"),
        F.trim("shape_id").alias("shape_id"),
        F.col("wheelchair_accessible").cast("int").alias("wheelchair_accessible"),
        F.col("bikes_allowed").cast("int").alias("bikes_allowed"),
        F.col("snapshot_id"),
        F.col("ingested_at")
    )
    .filter(
        F.col("trip_id").isNotNull() &
        (F.col("trip_id") != "") &
        F.col("route_id").isNotNull() &
        (F.col("route_id") != "") &
        F.col("service_id").isNotNull() &
        (F.col("service_id") != "")
    )
    .dropDuplicates(["trip_id", "snapshot_id"])
)

save_snapshot(silver_trips, "trips")

print(f"Bronze trips: {bronze_trips.count()}")
print(f"Silver trips: {silver_trips.count()}")

display(spark.table(target("trips")).limit(5))

# COMMAND ----------

# DBTITLE 1,04 - Silver Stop Times Transformation
from pyspark.sql import functions as F

bronze_stop_times = bronze("stop_times")
# Validate non-empty GTFS time values before transformation
for time_column in ["arrival_time", "departure_time"]:
    raw_time = F.trim(F.col(time_column))

    invalid_count = (
        bronze_stop_times
        .filter(
            F.col(time_column).isNotNull()
            & (raw_time != "")
            & ~raw_time.rlike(r"^\d{1,3}:[0-5]\d:[0-5]\d$")
        )
        .count()
    )

    print(f"Invalid {time_column} values: {invalid_count:,}")

    if invalid_count > 0:
        raise ValueError(
            f"Invalid GTFS times in {time_column}: {invalid_count:,}"
        )

def time_to_seconds(column_name):
    parts = F.split(F.col(column_name), ":")

    seconds = (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )

    return F.when(
        F.col(column_name).rlike(r"^\d{1,3}:[0-5]\d:[0-5]\d$"),
        seconds
    )

silver_stop_times = (
    bronze_stop_times
    .select(
        F.trim("trip_id").alias("trip_id"),
        F.trim("stop_id").alias("stop_id"),
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        F.col("arrival_time"),
        F.col("departure_time"),
        time_to_seconds("arrival_time").alias("arrival_seconds"),
        time_to_seconds("departure_time").alias("departure_seconds"),
        F.col("snapshot_id"),
        F.col("ingested_at")
    )
    .filter(
        F.col("trip_id").isNotNull()
        & (F.col("trip_id") != "")
        & F.col("stop_id").isNotNull()
        & (F.col("stop_id") != "")
        & F.col("stop_sequence").isNotNull()
    )
    .dropDuplicates(["snapshot_id", "trip_id", "stop_sequence"])
)

save_snapshot(silver_stop_times, "stop_times")

print(
    "Silver stop times:",
    spark.table(target("stop_times")).count()
)

display(
    spark.table(target("stop_times")).limit(5)
)

# COMMAND ----------

# DBTITLE 1,05 - Silver Calendar Dates Transformation
from pyspark.sql import functions as F

bronze_calendar = bronze("calendar_dates")

silver_calendar = (
    bronze_calendar
    .select(
        F.trim("service_id").alias("service_id"),
        F.to_date(F.col("date"), "yyyyMMdd").alias("service_date"),
        F.col("exception_type").cast("int").alias("exception_type"),
        F.col("snapshot_id"),
        F.col("ingested_at")
    )
    .filter(
        F.col("service_id").isNotNull()
        & (F.col("service_id") != "")
        & F.col("service_date").isNotNull()
        & F.col("exception_type").isin(1, 2)
    )
    .dropDuplicates(["service_id", "service_date", "snapshot_id"])
)

save_snapshot(silver_calendar, "calendar_dates")

print(f"Bronze calendar dates: {bronze_calendar.count()}")
print(f"Silver calendar dates: {silver_calendar.count()}")

display(
    spark.table(target("calendar_dates")).limit(5)
)

# COMMAND ----------

# DBTITLE 1,06 - Silver Referential Integrity Validation
from pyspark.sql import functions as F

routes = spark.table(target("routes"))
stops = spark.table(target("stops"))
trips = spark.table(target("trips"))
stop_times = spark.table(target("stop_times"))

checks = [
    (
        "Trips without matching routes",
        trips,
        routes,
        ["snapshot_id", "route_id"]
    ),
    (
        "Stop times without matching trips",
        stop_times,
        trips,
        ["snapshot_id", "trip_id"]
    ),
    (
        "Stop times without matching stops",
        stop_times,
        stops,
        ["snapshot_id", "stop_id"]
    )
]

for name, child, parent, keys in checks:
    missing = (
        child.join(
            parent.select(*keys).dropDuplicates(),
            on=keys,
            how="left_anti"
        )
        .count()
    )

    status = "PASS" if missing == 0 else "FAIL"
    print(f"{status}: {name} = {missing:,}")

    if missing > 0:
        raise ValueError(f"Referential integrity failed: {name} = {missing:,}")

# COMMAND ----------

# DBTITLE 1,07 - Bronze vs Silver Route Count Audit

datasets = [
    "routes",
    "stops",
    "trips",
    "stop_times",
    "calendar_dates",
]

audit_results = []

for dataset in datasets:
    bronze_count = spark.table(
        f"{CATALOG_SCHEMA}.bronze_{dataset}"
    ).filter(F.col("snapshot_id") == SNAPSHOT_ID).count()

    silver_count = spark.table(
        target(dataset)
    ).filter(F.col("snapshot_id") == SNAPSHOT_ID).count()

    removed_count = bronze_count - silver_count

    audit_results.append((
        dataset,
        bronze_count,
        silver_count,
        removed_count
    ))

    print(
        f"{dataset}: Bronze={bronze_count:,}, "
        f"Silver={silver_count:,}, "
        f"Difference={removed_count:,}"
    )

    if removed_count < 0:
        raise ValueError(
            f"Silver contains more rows than Bronze: {dataset}"
        )

audit_df = spark.createDataFrame(
    audit_results,
    ["dataset", "bronze_rows", "silver_rows", "removed_rows"]
)

display(audit_df)