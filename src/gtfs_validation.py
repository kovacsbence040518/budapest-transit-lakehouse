# src/gtfs_validation.py

REQUIRED_FILES = {
    "routes.txt",
    "stops.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar_dates.txt",
}


def validate_gtfs_files(filenames):
    missing = REQUIRED_FILES - set(filenames)

    if missing:
        raise ValueError(
            f"Missing required GTFS files: {sorted(missing)}"
        )

    return True