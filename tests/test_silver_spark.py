import pytest
from pyspark.sql import types as T
from src.silver_spark import (
    transform_routes, transform_trips, transform_stop_times,
    assert_references,
)

SNAPSHOT = "20261030T123209Z"


def records(spark, fields, rows):
    schema = T.StructType([T.StructField(name, T.StringType(), True) for name in fields])
    return spark.createDataFrame(rows, schema)


def test_route_trims_and_casts(spark):
    df = records(spark,
        ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "snapshot_id", "ingested_at"],
        [(" 33 ", " BKK ", " 33A ", None, "3", SNAPSHOT, None)])
    route = transform_routes(df).first()
    assert (route.route_id, route.agency_id, route.route_short_name, route.route_type) == ("33", "BKK", "33A", 3)


def test_route_invalid_keys_are_filtered(spark):
    df = records(spark,
        ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "snapshot_id", "ingested_at"],
        [(" ", "BKK", "33", None, "3", SNAPSHOT, None),
         ("10", "BKK", "10", None, "3", SNAPSHOT, None)])
    assert [r.route_id for r in transform_routes(df).collect()] == ["10"]


def test_routes_deduplicate_within_snapshot_but_preserve_other_snapshot(spark):
    df = records(spark,
        ["route_id", "agency_id", "route_short_name", "route_long_name", "route_type", "snapshot_id", "ingested_at"],
        [("10", "BKK", "10", None, "3", "s1", None),
         ("10", "BKK", "10", None, "3", "s1", None),
         ("10", "BKK", "10", None, "3", "s2", None)])
    assert transform_routes(df).count() == 2


def test_trip_missing_service_id_filtered(spark):
    fields = ["trip_id", "route_id", "service_id", "trip_headsign", "direction_id", "shape_id", "wheelchair_accessible", "bikes_allowed", "snapshot_id", "ingested_at"]
    df = records(spark, fields, [
        ("t1", "r1", " ", None, "0", None, None, None, SNAPSHOT, None),
        ("t2", "r1", "weekday", None, "0", None, None, None, SNAPSHOT, None),
    ])
    assert [r.trip_id for r in transform_trips(df).collect()] == ["t2"]


def test_stop_times_over_24_hours_and_missing_time(spark):
    fields = ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "snapshot_id", "ingested_at"]
    df = records(spark, fields, [
        ("t1", "s1", "1", "25:30:00", "25:30:00", SNAPSHOT, None),
        ("t1", "s2", "2", "", None, SNAPSHOT, None),
    ])
    rows = {r.stop_id: r for r in transform_stop_times(df).collect()}
    assert rows["s1"].arrival_seconds == 91800
    assert rows["s1"].departure_seconds == 91800
    assert rows["s2"].arrival_seconds is None
    assert rows["s2"].departure_seconds is None


def test_invalid_stop_time_raises(spark):
    df = records(spark,
        ["trip_id", "stop_id", "stop_sequence", "arrival_time", "departure_time", "snapshot_id", "ingested_at"],
        [("t1", "s1", "1", "10:75:00", "10:00:00", SNAPSHOT, None)])
    with pytest.raises(ValueError, match="Invalid GTFS times in arrival_time"):
        transform_stop_times(df)


def test_valid_references(spark):
    child = records(spark, ["snapshot_id", "route_id"], [("s1", "10")])
    parent = records(spark, ["snapshot_id", "route_id"], [("s1", "10")])
    assert_references(child, parent, ["snapshot_id", "route_id"], "trips without routes")


def test_missing_reference_raises(spark):
    child = records(spark, ["snapshot_id", "route_id"], [("s1", "99")])
    parent = records(spark, ["snapshot_id", "route_id"], [("s1", "10")])
    with pytest.raises(ValueError, match="Referential integrity failed"):
        assert_references(child, parent, ["snapshot_id", "route_id"], "trips without routes")


def test_snapshot_id_is_part_of_reference(spark):
    child = records(spark, ["snapshot_id", "route_id"], [("new", "10")])
    parent = records(spark, ["snapshot_id", "route_id"], [("old", "10")])
    with pytest.raises(ValueError, match="Referential integrity failed"):
        assert_references(child, parent, ["snapshot_id", "route_id"], "cross snapshot mismatch")
