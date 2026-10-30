# tests/test_transformation.py

import pytest

from src.gtfs_transformations import time_to_seconds


@pytest.mark.parametrize(
    "time_value, expected",
    [
        ("00:00:00", 0),
        ("10:30:00", 37800),
        ("23:59:59", 86399),
        ("24:00:00", 86400),
        ("25:30:00", 91800),
        ("48:15:30", 173730),
    ],
)
def test_valid_gtfs_times(time_value, expected):
    assert time_to_seconds(time_value) == expected


@pytest.mark.parametrize("value", [None, ""])
def test_missing_gtfs_times(value):
    assert time_to_seconds(value) is None


@pytest.mark.parametrize(
    "invalid_value",
    [
        "10:75:00",
        "10:30:99",
        "abc",
        "12:30",
        "-01:30:00",
        "10:30:00:00",
    ],
)
def test_invalid_gtfs_times(invalid_value):
    with pytest.raises(ValueError, match="Invalid GTFS time"):
        time_to_seconds(invalid_value)