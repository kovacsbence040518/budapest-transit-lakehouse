"""Compare two real GTFS feed snapshots using Spark DataFrames.

Counts and IDs describe feed records, not actual passenger journeys.
Scheduled departure totals compare only overlapping departure dates.
"""
from pyspark.sql import functions as F
from pyspark.sql import types as T


COMPARISON_SCHEMA = T.StructType([
    T.StructField("previous_snapshot_id", T.StringType(), False),
    T.StructField("current_snapshot_id", T.StringType(), False),
    T.StructField("previous_routes", T.LongType(), False),
    T.StructField("current_routes", T.LongType(), False),
    T.StructField("routes_added", T.LongType(), False),
    T.StructField("routes_removed", T.LongType(), False),
    T.StructField("previous_stops", T.LongType(), False),
    T.StructField("current_stops", T.LongType(), False),
    T.StructField("stops_added", T.LongType(), False),
    T.StructField("stops_removed", T.LongType(), False),
    T.StructField("previous_trips", T.LongType(), False),
    T.StructField("current_trips", T.LongType(), False),
    T.StructField("trips_added", T.LongType(), False),
    T.StructField("trips_removed", T.LongType(), False),
    T.StructField("shared_departure_dates", T.LongType(), False),
    T.StructField("previous_shared_date_departures", T.LongType(), False),
    T.StructField("current_shared_date_departures", T.LongType(), False),
    T.StructField("shared_date_departures_change", T.LongType(), False),
])


def compare_snapshot_pair(routes, stops, trips, daily, previous_id, current_id):
    """Return a one-row Spark DataFrame comparing two snapshots.

    All keys must be unique within their snapshot; duplicate keys cause failure.
    An overlap of at least one departure date is required for fair comparison.
    """
    if not previous_id or not current_id or previous_id >= current_id:
        raise ValueError("Snapshots must be distinct and ordered oldest to newest")

    spark = routes.sparkSession
    values = {}

    for name, df, key in (
        ("routes", routes, "route_id"),
        ("stops", stops, "stop_id"),
        ("trips", trips, "trip_id"),
    ):
        earlier = df.filter(F.col("snapshot_id") == previous_id)
        later = df.filter(F.col("snapshot_id") == current_id)
        for label, side in (("previous", earlier), ("current", later)):
            count = side.count()
            if count == 0:
                raise ValueError(f"No {name} rows for {label} snapshot")
            distinct = side.select(key).distinct().count()
            if count != distinct:
                raise ValueError(f"Duplicate {name} keys in {label} snapshot")
            if side.filter(F.col(key).isNull()).limit(1).count():
                raise ValueError(f"Null {name} key in {label} snapshot")
            values[f"{label}_{name}"] = count
        old_keys = earlier.select(key)
        new_keys = later.select(key)
        values[f"{name}_added"] = new_keys.join(old_keys, key, "left_anti").count()
        values[f"{name}_removed"] = old_keys.join(new_keys, key, "left_anti").count()

    old_daily = daily.filter(F.col("snapshot_id") == previous_id).select(
        "departure_date", F.col("total_scheduled_departures").alias("previous_departures")
    )
    new_daily = daily.filter(F.col("snapshot_id") == current_id).select(
        "departure_date", F.col("total_scheduled_departures").alias("current_departures")
    )
    for label, dates in (("previous", old_daily), ("current", new_daily)):
        if dates.count() != dates.select("departure_date").distinct().count():
            raise ValueError(f"Duplicate {label} daily departure dates")

    shared = old_daily.join(new_daily, "departure_date", "inner")
    totals = shared.agg(
        F.count("*").alias("days"),
        F.sum("previous_departures").alias("old_total"),
        F.sum("current_departures").alias("new_total"),
    ).first()
    if not totals["days"]:
        raise ValueError("Snapshots have no overlapping departure dates")
    if totals["old_total"] is None or totals["new_total"] is None:
        raise ValueError("Scheduled departure totals are missing")

    values["previous_snapshot_id"] = previous_id
    values["current_snapshot_id"] = current_id
    values["shared_departure_dates"] = totals["days"]
    values["previous_shared_date_departures"] = totals["old_total"]
    values["current_shared_date_departures"] = totals["new_total"]
    values["shared_date_departures_change"] = totals["new_total"] - totals["old_total"]
    return spark.createDataFrame([tuple(values[field.name] for field in COMPARISON_SCHEMA)], COMPARISON_SCHEMA)
