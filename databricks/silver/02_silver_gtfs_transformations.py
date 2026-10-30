# Databricks notebook source
# DBTITLE 1,silver_routes
from pyspark.sql import functions as F

bronze_routes = spark.table("workspace.default.bronze_routes")

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

(
    silver_routes.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_routes")
)

print(f"Silver routes loaded: {silver_routes.count()} rows")

display(spark.table("workspace.default.silver_routes").limit(5))

# COMMAND ----------

# DBTITLE 1,02 - Silver Stops Transformation
from pyspark.sql import functions as F

bronze_stops = spark.table("workspace.default.bronze_stops")

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

(
    silver_stops.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_stops")
)

print(f"Bronze stops: {bronze_stops.count()}")
print(f"Silver stops: {silver_stops.count()}")

display(spark.table("workspace.default.silver_stops").limit(5))

# COMMAND ----------

# DBTITLE 1,03 - Silver Trips Transformation
from pyspark.sql import functions as F

bronze_trips = spark.table("workspace.default.bronze_trips")

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

(
    silver_trips.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_trips")
)

print(f"Bronze trips: {bronze_trips.count()}")
print(f"Silver trips: {silver_trips.count()}")

display(spark.table("workspace.default.silver_trips").limit(5))

# COMMAND ----------

# DBTITLE 1,04 - Silver Stop Times Transformation
from pyspark.sql import functions as F

bronze_stop_times = spark.table(
    "workspace.default.bronze_stop_times"
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
        & F.col("arrival_seconds").isNotNull()
        & F.col("departure_seconds").isNotNull()
    )
    .dropDuplicates(["snapshot_id", "trip_id", "stop_sequence"])
)

(
    silver_stop_times.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_stop_times")
)

print(
    "Silver stop times:",
    spark.table("workspace.default.silver_stop_times").count()
)

display(
    spark.table("workspace.default.silver_stop_times").limit(5)
)

# COMMAND ----------

# DBTITLE 1,05 - Silver Calendar Dates Transformation
from pyspark.sql import functions as F

bronze_calendar = spark.table(
    "workspace.default.bronze_calendar_dates"
)

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

(
    silver_calendar.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.silver_calendar_dates")
)

print(f"Bronze calendar dates: {bronze_calendar.count()}")
print(f"Silver calendar dates: {silver_calendar.count()}")

display(
    spark.table("workspace.default.silver_calendar_dates").limit(5)
)

# COMMAND ----------

# DBTITLE 1,06 - Silver Referential Integrity Validation
from pyspark.sql import functions as F

routes = spark.table("workspace.default.silver_routes")
stops = spark.table("workspace.default.silver_stops")
trips = spark.table("workspace.default.silver_trips")
stop_times = spark.table("workspace.default.silver_stop_times")

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