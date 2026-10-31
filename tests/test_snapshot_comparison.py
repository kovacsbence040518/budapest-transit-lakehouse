"""Spark tests for additions/removals and comparable scheduled departures."""
import pytest
from src.snapshot_comparison import compare_snapshot_pair

OLD = "20261029T140343Z"
NEW = "20261030T123209Z"


def frames(spark):
    routes = spark.createDataFrame([(OLD, "R1"), (OLD, "R2"), (NEW, "R2"), (NEW, "R3")], "snapshot_id string, route_id string")
    stops = spark.createDataFrame([(OLD, "S1"), (NEW, "S1"), (NEW, "S2")], "snapshot_id string, stop_id string")
    trips = spark.createDataFrame([(OLD, "T1"), (OLD, "T2"), (NEW, "T2"), (NEW, "T3")], "snapshot_id string, trip_id string")
    daily = spark.createDataFrame([
        (OLD, "2026-10-29", 10), (OLD, "2026-10-30", 20),
        (NEW, "2026-10-30", 18), (NEW, "2026-10-31", 30),
    ], "snapshot_id string, departure_date string, total_scheduled_departures long")
    return routes, stops, trips, daily


def test_additions_and_removals(spark):
    result = compare_snapshot_pair(*frames(spark), OLD, NEW).first().asDict()
    assert (result["routes_added"], result["routes_removed"]) == (1, 1)
    assert (result["stops_added"], result["stops_removed"]) == (1, 0)
    assert (result["trips_added"], result["trips_removed"]) == (1, 1)
    assert result["previous_routes"] == result["current_routes"] == 2


def test_departure_comparison_only_uses_overlapping_dates(spark):
    result = compare_snapshot_pair(*frames(spark), OLD, NEW).first()
    assert result["shared_departure_dates"] == 1
    assert result["previous_shared_date_departures"] == 20
    assert result["current_shared_date_departures"] == 18
    assert result["shared_date_departures_change"] == -2


def test_missing_snapshot_raises(spark):
    with pytest.raises(ValueError, match="No routes rows"):
        compare_snapshot_pair(*frames(spark), OLD, "20261101T000000Z")


def test_duplicate_keys_raise(spark):
    routes, stops, trips, daily = frames(spark)
    duplicate = routes.unionByName(routes.filter("snapshot_id = '20261029T140343Z' AND route_id = 'R1'"))
    with pytest.raises(ValueError, match="Duplicate routes keys"):
        compare_snapshot_pair(duplicate, stops, trips, daily, OLD, NEW)


def test_no_overlapping_dates_raises(spark):
    routes, stops, trips, daily = frames(spark)
    no_overlap = daily.filter("departure_date != '2026-10-30'")
    with pytest.raises(ValueError, match="no overlapping departure dates"):
        compare_snapshot_pair(routes, stops, trips, no_overlap, OLD, NEW)
