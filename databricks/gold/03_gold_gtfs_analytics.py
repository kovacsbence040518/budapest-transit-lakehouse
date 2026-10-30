# Databricks notebook source
# DBTITLE 1,01 - Gold Route Statistics
from pyspark.sql import functions as F

routes = spark.table("workspace.default.silver_routes")
trips = spark.table("workspace.default.silver_trips")
stop_times = spark.table("workspace.default.silver_stop_times")

# 1. Count scheduled trips per route
trip_counts = (
    trips
    .groupBy("snapshot_id", "route_id")
    .agg(
        F.countDistinct("trip_id").alias("total_trips")
    )
)

# 2. Connect stop events to their routes
route_stops = (
    stop_times
    .join(
        trips.select("snapshot_id", "trip_id", "route_id"),
        on=["snapshot_id", "trip_id"],
        how="inner"
    )
)

# 3. Count distinct stops served by each route
stop_counts = (
    route_stops
    .groupBy("snapshot_id", "route_id")
    .agg(
        F.countDistinct("stop_id").alias("unique_stops")
    )
)

# 4. Build the Gold analytical table
gold_routes = (
    routes
    .select(
        "snapshot_id",
        "route_id",
        "route_short_name",
        "route_type"
    )
    .join(
        trip_counts,
        on=["snapshot_id", "route_id"],
        how="left"
    )
    .join(
        stop_counts,
        on=["snapshot_id", "route_id"],
        how="left"
    )
    .fillna({
        "total_trips": 0,
        "unique_stops": 0
    })
)

# 5. Save as a Delta table
(
    gold_routes.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_route_statistics")
)

print(
    "Gold route statistics:",
    spark.table("workspace.default.gold_route_statistics").count(),
    "rows"
)

display(
    spark.table("workspace.default.gold_route_statistics")
    .orderBy(F.desc("total_trips"))
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,02 - Gold Stop Activity
from pyspark.sql import functions as F

stops = spark.table("workspace.default.silver_stops")
trips = spark.table("workspace.default.silver_trips")
stop_times = spark.table("workspace.default.silver_stop_times")

# 1. Connect stop events to routes
stop_events = (
    stop_times
    .join(
        trips.select("snapshot_id", "trip_id", "route_id"),
        on=["snapshot_id", "trip_id"],
        how="inner"
    )
)

# 2. Calculate activity at each stop
stop_activity = (
    stop_events
    .groupBy("snapshot_id", "stop_id")
    .agg(
        F.count("*").alias("total_stop_events"),
        F.countDistinct("trip_id").alias("unique_trips"),
        F.countDistinct("route_id").alias("unique_routes")
    )
)

# 3. Add stop names and coordinates
gold_stop_activity = (
    stops
    .select(
        "snapshot_id",
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon"
    )
    .join(
        stop_activity,
        on=["snapshot_id", "stop_id"],
        how="left"
    )
    .fillna({
        "total_stop_events": 0,
        "unique_trips": 0,
        "unique_routes": 0
    })
)

# 4. Save Gold Delta table
(
    gold_stop_activity.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_stop_activity")
)

print(
    "Gold stop activity:",
    spark.table("workspace.default.gold_stop_activity").count(),
    "rows"
)

display(
    spark.table("workspace.default.gold_stop_activity")
    .orderBy(F.desc("total_stop_events"))
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,03 - Calendar Service Coverage
from pyspark.sql import functions as F

trips = spark.table("workspace.default.silver_trips")
calendar = spark.table("workspace.default.silver_calendar_dates")

trip_services = (
    trips
    .select("snapshot_id", "service_id")
    .distinct()
)

calendar_services = (
    calendar
    .filter(F.col("exception_type") == 1)
    .select("snapshot_id", "service_id")
    .distinct()
)

matched = trip_services.join(
    calendar_services,
    on=["snapshot_id", "service_id"],
    how="left_semi"
)

unmatched = trip_services.join(
    calendar_services,
    on=["snapshot_id", "service_id"],
    how="left_anti"
)

print("Distinct trip service IDs:", trip_services.count())
print("Service IDs with calendar additions:", matched.count())
print("Service IDs without calendar additions:", unmatched.count())

display(unmatched.limit(5))

# COMMAND ----------

# DBTITLE 1,04 - Gold Hourly Departures
from pyspark.sql import functions as F

trips = spark.table("workspace.default.silver_trips")
stop_times = spark.table("workspace.default.silver_stop_times")
calendar = spark.table("workspace.default.silver_calendar_dates")
routes = spark.table("workspace.default.silver_routes")

# 1. Find the first scheduled departure of each trip
trip_starts = (
    stop_times
    .groupBy("snapshot_id", "trip_id")
    .agg(
        F.min("departure_seconds").alias("first_departure_seconds")
    )
)

# 2. Find the dates on which each service operates
active_services = (
    calendar
    .filter(F.col("exception_type") == 1)
    .select("snapshot_id", "service_id", "service_date")
    .dropDuplicates()
)

# 3. Connect trips with their operating dates
scheduled_trips = (
    trips
    .select("snapshot_id", "trip_id", "route_id", "service_id")
    .join(
        trip_starts,
        on=["snapshot_id", "trip_id"],
        how="inner"
    )
    .join(
        active_services,
        on=["snapshot_id", "service_id"],
        how="inner"
    )
)

# 4. Calculate the actual departure date and hour
# GTFS times can exceed 24:00:00
scheduled_trips = (
    scheduled_trips
    .withColumn(
        "departure_date",
        F.date_add(
            F.col("service_date"),
            F.floor(F.col("first_departure_seconds") / 86400).cast("int")
        )
    )
    .withColumn(
        "departure_hour",
        F.floor(
            F.pmod(F.col("first_departure_seconds"), F.lit(86400)) / 3600
        ).cast("int")
    )
)

# 5. Aggregate scheduled departures by route, date and hour
hourly_departures = (
    scheduled_trips
    .groupBy(
        "snapshot_id",
        "route_id",
        "departure_date",
        "departure_hour"
    )
    .agg(
        F.count("*").alias("scheduled_departures")
    )
)

# 6. Add route information
gold_hourly = (
    hourly_departures
    .join(
        routes.select(
            "snapshot_id",
            "route_id",
            "route_short_name",
            "route_type"
        ),
        on=["snapshot_id", "route_id"],
        how="left"
    )
)

# 7. Save Gold Delta table
(
    gold_hourly.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_hourly_departures")
)

print(
    "Gold hourly departures:",
    spark.table("workspace.default.gold_hourly_departures").count(),
    "rows"
)

display(
    spark.table("workspace.default.gold_hourly_departures")
    .orderBy(F.desc("scheduled_departures"))
    .limit(10)
)

# COMMAND ----------

# DBTITLE 1,05 - Gold Daily Service
from pyspark.sql import functions as F

hourly = spark.table(
    "workspace.default.gold_hourly_departures"
)

# Aggregate scheduled departures across all routes and hours
gold_daily = (
    hourly
    .groupBy("snapshot_id", "departure_date")
    .agg(
        F.sum("scheduled_departures").alias(
            "total_scheduled_departures"
        ),
        F.countDistinct("route_id").alias("active_routes"),
        F.countDistinct("route_type").alias("transport_types")
    )
    .orderBy("departure_date")
)

# Save Gold Delta table
(
    gold_daily.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.gold_daily_service")
)

print(
    "Gold daily service:",
    spark.table("workspace.default.gold_daily_service").count(),
    "rows"
)

display(
    spark.table("workspace.default.gold_daily_service")
    .orderBy("departure_date")
)