"""Reusable GTFS Gold scheduling transformations (Spark DataFrames only).

These are extracted from the existing integrated Gold notebook.
No Databricks table access or writes occur here.
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def first_trip_departures(stop_times):
    """Use the first stop_sequence, not the smallest departure time."""
    window = Window.partitionBy("snapshot_id", "trip_id").orderBy("stop_sequence")
    starts = (
        stop_times.withColumn("_stop_rank", F.row_number().over(window))
        .filter(F.col("_stop_rank") == 1)
        .select("snapshot_id", "trip_id", F.col("departure_seconds").alias("first_departure_seconds"))
    )
    if starts.filter(F.col("first_departure_seconds").isNull() | (F.col("first_departure_seconds") < 0)).limit(1).count():
        raise ValueError("A trip has no valid departure time at its first stop")
    return starts


def expand_scheduled_trips(trips, stop_times, calendar_dates):
    """Expand GTFS calendar_dates-only feed into dated departures.

    Assumes the feed has no calendar.txt and service dates are defined by
    exception_type=1 entries. Each departure time is measured from its service date.
    """
    starts = first_trip_departures(stop_times)
    active = (
        calendar_dates.filter(F.col("exception_type") == 1)
        .select("snapshot_id", "service_id", "service_date")
        .dropDuplicates()
    )
    return (
        trips.select("snapshot_id", "trip_id", "route_id", "service_id")
        .join(starts, ["snapshot_id", "trip_id"], "inner")
        .join(active, ["snapshot_id", "service_id"], "inner")
        .withColumn("departure_date", F.date_add(
            F.col("service_date"),
            F.floor(F.col("first_departure_seconds") / F.lit(86400)).cast("int")))
        .withColumn("departure_hour", F.floor(
            F.pmod(F.col("first_departure_seconds"), F.lit(86400)) / F.lit(3600)).cast("int"))
    )


def hourly_departures(scheduled_trips, routes):
    return (
        scheduled_trips.groupBy("snapshot_id", "route_id", "departure_date", "departure_hour")
        .agg(F.count("*").alias("scheduled_departures"))
        .join(routes.select("snapshot_id", "route_id", "route_short_name", "route_type"),
              ["snapshot_id", "route_id"], "left")
    )


def daily_service(hourly):
    return hourly.groupBy("snapshot_id", "departure_date").agg(
        F.sum("scheduled_departures").alias("total_scheduled_departures"),
        F.countDistinct("route_id").alias("active_routes"),
        F.countDistinct("route_type").alias("transport_types"),
    )


def route_statistics(routes, trips, stop_times):
    """Route-level trip and unique-stop counts (same schema as production Gold)."""
    trip_counts = trips.groupBy("snapshot_id", "route_id").agg(
        F.countDistinct("trip_id").alias("total_trips")
    )
    events = stop_times.join(
        trips.select("snapshot_id", "trip_id", "route_id"),
        ["snapshot_id", "trip_id"], "inner"
    )
    stop_counts = events.groupBy("snapshot_id", "route_id").agg(
        F.countDistinct("stop_id").alias("unique_stops")
    )
    return (
        routes.select("snapshot_id", "route_id", "route_short_name", "route_type")
        .join(trip_counts, ["snapshot_id", "route_id"], "left")
        .join(stop_counts, ["snapshot_id", "route_id"], "left")
        .fillna({"total_trips": 0, "unique_stops": 0})
    )


def stop_activity(stops, trips, stop_times):
    """Stop-level events, unique trips and routes (same schema as production Gold)."""
    events = stop_times.join(
        trips.select("snapshot_id", "trip_id", "route_id"),
        ["snapshot_id", "trip_id"], "inner"
    )
    activity = events.groupBy("snapshot_id", "stop_id").agg(
        F.count("*").alias("total_stop_events"),
        F.countDistinct("trip_id").alias("unique_trips"),
        F.countDistinct("route_id").alias("unique_routes"),
    )
    return (
        stops.select("snapshot_id", "stop_id", "stop_name", "stop_lat", "stop_lon")
        .join(activity, ["snapshot_id", "stop_id"], "left")
        .fillna({"total_stop_events": 0, "unique_trips": 0, "unique_routes": 0})
    )
