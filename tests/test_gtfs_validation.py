# tests/test_gtfs_validation.py

import pytest
from src.gtfs_validation import validate_gtfs_files


def test_valid_gtfs_files():
    assert validate_gtfs_files([
        "routes.txt",
        "stops.txt",
        "trips.txt",
        "stop_times.txt",
        "calendar_dates.txt",
    ])


def test_missing_required_file():
    filenames = [
        "routes.txt",
        "stops.txt",
        "stop_times.txt",
        "calendar_dates.txt",
    ]

    with pytest.raises(ValueError, match="trips.txt"):
        validate_gtfs_files(filenames)


def test_calendar_txt_not_required():
    assert validate_gtfs_files([
        "routes.txt",
        "stops.txt",
        "trips.txt",
        "stop_times.txt",
        "calendar_dates.txt",
    ])


def test_extra_files_allowed():
    assert validate_gtfs_files([
        "routes.txt",
        "stops.txt",
        "trips.txt",
        "stop_times.txt",
        "calendar_dates.txt",
        "agency.txt",
        "shapes.txt",
    ])