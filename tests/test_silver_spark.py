import pytest
from pyspark.sql import types as T
from src.silver_spark import (
    transform_routes, transform_stops, transform_trips, transform_stop_times,
    transform_calendar_dates,
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


def test_stops_cast_coordinates_and_optional_fields(spark):
    df = records(spark,
        ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "wheelchair_boarding", "snapshot_id", "ingested_at"],
        [(" s1 ", " Stop ", "47.5", "19.1", None, None, None, SNAPSHOT, None)])
    result = transform_stops(df).first()
    assert result.stop_id == "s1"
    assert result.stop_name == "Stop"
    assert result.stop_lat == 47.5 and result.stop_lon == 19.1
    assert result.location_type is None


def test_stops_filter_invalid_coordinates_and_keep_other_snapshots(spark):
    fields = ["stop_id", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station", "wheelchair_boarding", "snapshot_id", "ingested_at"]
    df = records(spark, fields, [
        ("s1", "Valid", "47.5", "19.1", None, None, None, "s1", None),
        ("s1", "Valid", "47.5", "19.1", None, None, None, "s1", None),
        ("s1", "Valid", "47.5", "19.1", None, None, None, "s2", None),
        ("s2", "Invalid", "95", "19", None, None, None, "s1", None),
    ])
    result = transform_stops(df)
    assert result.count() == 2
    assert {r.snapshot_id for r in result.select("snapshot_id").collect()} == {"s1", "s2"}


def test_calendar_dates_parse_and_preserve_exceptions(spark):
    df = records(spark, ["service_id", "date", "exception_type", "snapshot_id", "ingested_at"], [
        (" workday ", "20261030", "1", SNAPSHOT, None),
        ("holiday", "20261031", "2", SNAPSHOT, None),
    ])
    results = {r.service_id: r for r in transform_calendar_dates(df).collect()}
    assert str(results["workday"].service_date) == "2026-10-30"
    assert results["workday"].exception_type == 1
    assert results["holiday"].exception_type == 2


def test_calendar_dates_filter_bad_rows_and_deduplicate(spark):
    df = records(spark, ["service_id", "date", "exception_type", "snapshot_id", "ingested_at"], [
        ("s1", "20261030", "1", SNAPSHOT, None),
        ("s1", "20261030", "1", SNAPSHOT, None),
        ("s2", "20261031", "3", SNAPSHOT, None),
        (" ", "20261031", "1", SNAPSHOT, None),
        ("s3", "bad-date", "1", SNAPSHOT, None),
    ])
    assert [r.service_id for r in transform_calendar_dates(df).collect()] == ["s1"]
