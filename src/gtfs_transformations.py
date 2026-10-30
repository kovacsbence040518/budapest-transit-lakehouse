# src/gtfs_transformations.py

import re

GTFS_TIME_PATTERN = re.compile(r"^\d{1,3}:[0-5]\d:[0-5]\d$")


def time_to_seconds(value):
    if value is None or value == "":
        return None

    if not isinstance(value, str):
        raise ValueError(f"Invalid GTFS time: {value}")

    if not GTFS_TIME_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid GTFS time: {value}")

    hours, minutes, seconds = map(int, value.split(":"))

    return hours * 3600 + minutes * 60 + seconds