"""Reusable portions of the GTFS Silver transformation logic.

These functions accept Spark DataFrames and do not read/write Databricks tables.
"""
from pyspark.sql import functions as F

GTFS_TIME_REGEX = r"^\d{1,3}:[0-5]\d:[0-5]\d$"


def gtfs_time_seconds(column_name):
    """Convert valid GTFS time strings to seconds; missing/invalid become null.

    Caller should validate malformed nonempty values before allowing writes.
    """
    column = F.trim(F.col(column_name))
    parts = F.split(column, ":")
    seconds = (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )
    return F.when(column.rlike(GTFS_TIME_REGEX), seconds)


def transform_routes(bronze_df):
    return (
        bronze_df.select(
            F.trim("route_id").alias("route_id"),
            F.trim("agency_id").alias("agency_id"),
            F.trim("route_short_name").alias("route_short_name"),
            F.trim("route_long_name").alias("route_long_name"),
            F.col("route_type").cast("int").alias("route_type"),
            "snapshot_id", "ingested_at",
        )
        .filter(F.col("route_id").isNotNull() & (F.col("route_id") != "") & F.col("route_type").isNotNull())
        .dropDuplicates(["route_id", "snapshot_id"])
    )


def transform_trips(bronze_df):
    return (
        bronze_df.select(
            F.trim("trip_id").alias("trip_id"),
            F.trim("route_id").alias("route_id"),
            F.trim("service_id").alias("service_id"),
            F.trim("trip_headsign").alias("trip_headsign"),
            F.col("direction_id").cast("int").alias("direction_id"),
            F.trim("shape_id").alias("shape_id"),
            F.col("wheelchair_accessible").cast("int").alias("wheelchair_accessible"),
            F.col("bikes_allowed").cast("int").alias("bikes_allowed"),
            "snapshot_id", "ingested_at",
        )
        .filter(
            F.col("trip_id").isNotNull() & (F.col("trip_id") != "")
            & F.col("route_id").isNotNull() & (F.col("route_id") != "")
            & F.col("service_id").isNotNull() & (F.col("service_id") != "")
        )
        .dropDuplicates(["trip_id", "snapshot_id"])
    )


def invalid_nonempty_times(bronze_df, column_name):
    raw = F.trim(F.col(column_name))
    return bronze_df.filter(
        F.col(column_name).isNotNull() & (raw != "") & ~raw.rlike(GTFS_TIME_REGEX)
    )


def transform_stop_times(bronze_df):
    for column in ("arrival_time", "departure_time"):
        if invalid_nonempty_times(bronze_df, column).limit(1).count():
            raise ValueError(f"Invalid GTFS times in {column}")
    return (
        bronze_df.select(
            F.trim("trip_id").alias("trip_id"),
            F.trim("stop_id").alias("stop_id"),
            F.col("stop_sequence").cast("int").alias("stop_sequence"),
            "arrival_time", "departure_time",
            gtfs_time_seconds("arrival_time").alias("arrival_seconds"),
            gtfs_time_seconds("departure_time").alias("departure_seconds"),
            "snapshot_id", "ingested_at",
        )
        .filter(
            F.col("trip_id").isNotNull() & (F.col("trip_id") != "")
            & F.col("stop_id").isNotNull() & (F.col("stop_id") != "")
            & F.col("stop_sequence").isNotNull()
        )
        .dropDuplicates(["snapshot_id", "trip_id", "stop_sequence"])
    )


def assert_references(child_df, parent_df, keys, description):
    missing = child_df.join(parent_df.select(*keys).dropDuplicates(), keys, "left_anti")
    count = missing.count()
    if count:
        raise ValueError(f"Referential integrity failed: {description} = {count}")
