"""Gold GTFS date/hour tests, using the project's existing Spark fixture."""
from datetime import date
import pytest
from pyspark.sql import types as T
from src.gold_spark import first_trip_departures, expand_scheduled_trips, hourly_departures, daily_service


def frames(spark, times, trip_rows=None, calendar_rows=None, route_rows=None):
    stops_schema = T.StructType([
        T.StructField('snapshot_id', T.StringType()),
        T.StructField('trip_id', T.StringType()),
        T.StructField('stop_sequence', T.IntegerType()),
        T.StructField('departure_seconds', T.IntegerType()),
    ])
    trips_schema = 'snapshot_id string, trip_id string, route_id string, service_id string'
    cal_schema = T.StructType([
        T.StructField('snapshot_id', T.StringType()),
        T.StructField('service_id', T.StringType()),
        T.StructField('service_date', T.DateType()),
        T.StructField('exception_type', T.IntegerType()),
    ])
    route_schema = 'snapshot_id string, route_id string, route_short_name string, route_type int'
    trips = trip_rows if trip_rows is not None else [('s1','t1','R1','weekday')]
    calendar = calendar_rows if calendar_rows is not None else [('s1','weekday',date(2026,10,30),1)]
    routes = route_rows if route_rows is not None else [('s1','R1','9',3)]
    return (spark.createDataFrame(times, stops_schema), spark.createDataFrame(trips, trips_schema),
            spark.createDataFrame(calendar, cal_schema), spark.createDataFrame(routes, route_schema))


def test_first_departure_uses_stop_sequence_not_earliest_time(spark):
    st, *_ = frames(spark, [('s1','t1',2,3600), ('s1','t1',1,7200)])
    assert first_trip_departures(st).first()['first_departure_seconds'] == 7200


def test_25_30_rolls_to_next_calendar_date_hour_one(spark):
    st, trips, calendar, _ = frames(spark, [('s1','t1',1,25*3600+30*60)])
    row = expand_scheduled_trips(trips,st,calendar).first()
    assert row['departure_date'] == date(2026,10,31)
    assert row['departure_hour'] == 1


def test_midnight_is_next_day_hour_zero(spark):
    st, trips, calendar, _ = frames(spark, [('s1','t1',1,86400)])
    row = expand_scheduled_trips(trips,st,calendar).first()
    assert (row['departure_date'],row['departure_hour']) == (date(2026,10,31),0)


def test_23_59_stays_on_service_date(spark):
    st, trips, calendar, _ = frames(spark, [('s1','t1',1,86399)])
    row = expand_scheduled_trips(trips,st,calendar).first()
    assert (row['departure_date'],row['departure_hour']) == (date(2026,10,30),23)


def test_removed_service_does_not_generate_departures(spark):
    st, trips, calendar, _ = frames(spark,[('s1','t1',1,3600)],
        calendar_rows=[('s1','weekday',date(2026,10,30),2)])
    assert expand_scheduled_trips(trips,st,calendar).count() == 0


def test_missing_first_stop_departure_fails(spark):
    st, *_ = frames(spark,[('s1','t1',1,None),('s1','t1',2,4200)])
    with pytest.raises(ValueError,match='first stop'):
        first_trip_departures(st)


def test_snapshot_isolation_on_joins(spark):
    st, trips, calendar, _ = frames(spark,[('s1','t1',1,3600)],
        trip_rows=[('s2','t1','R1','weekday')],
        calendar_rows=[('s2','weekday',date(2026,10,30),1)])
    assert expand_scheduled_trips(trips,st,calendar).count() == 0


def test_hourly_and_daily_reconcile(spark):
    st, trips, calendar, routes = frames(spark,
        [('s1','t1',1,3600),('s1','t2',1,3700),('s1','t3',1,25*3600)],
        trip_rows=[('s1','t1','R1','weekday'),('s1','t2','R1','weekday'),('s1','t3','R1','weekday')])
    expanded = expand_scheduled_trips(trips,st,calendar)
    hourly = hourly_departures(expanded,routes)
    daily = daily_service(hourly)
    by_hour = {(r['departure_date'],r['departure_hour']):r['scheduled_departures'] for r in hourly.collect()}
    assert by_hour == {(date(2026,10,30),1):2,(date(2026,10,31),1):1}
    by_day = {r['departure_date']:r['total_scheduled_departures'] for r in daily.collect()}
    assert by_day == {date(2026,10,30):2,date(2026,10,31):1}
    assert sum(by_hour.values()) == sum(by_day.values()) == 3
