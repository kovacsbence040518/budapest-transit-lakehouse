# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,00 - Download Configuration
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from zipfile import ZipFile, BadZipFile
import hashlib
import json
import os
import tempfile

GTFS_URL = "https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"
VOLUME = Path("/Volumes/workspace/default/gtfs_raw")
REQUIRED_GTFS_FILES = {
    "routes.txt", "stops.txt", "trips.txt", "stop_times.txt", "calendar_dates.txt"
}
VOLUME.mkdir(parents=True, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,01 - Define Safe Download and Deduplication
def fetch_gtfs_snapshot():
    hasher = hashlib.sha256()
    downloaded_at = datetime.now(timezone.utc)
    snapshot_id = downloaded_at.strftime("%Y%m%dT%H%M%SZ")

    # Local disk is appropriate for staging; the completed ZIP is then placed in the volume.
    fd, local_tmp = tempfile.mkstemp(prefix="bkk_gtfs_", suffix=".zip")
    os.close(fd)
    try:
        req = Request(GTFS_URL, headers={"User-Agent": "BudapestGTFSDataPipeline/1.0"})
        with urlopen(req, timeout=120) as response, open(local_tmp, "wb") as out:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                hasher.update(chunk)
        digest = hasher.hexdigest()
        print(f"Downloaded {os.path.getsize(local_tmp):,} bytes; SHA-256: {digest}")

        # Verify ZIP and detect unchanged feed
        with ZipFile(local_tmp) as archive:
            names = set(archive.namelist())
            missing = REQUIRED_GTFS_FILES - names
            if missing:
                raise ValueError(f"GTFS ZIP missing required files: {sorted(missing)}")
            invalid_member = archive.testzip()
            if invalid_member is not None:
                raise ValueError(f"Corrupt GTFS ZIP member: {invalid_member}")

        # Compare bytes with previous snapshots, including snapshots created before metadata existed.
        existing_hashes = set()
        for previous_zip in VOLUME.glob("budapest_gtfs_*.zip"):
            previous_hasher = hashlib.sha256()
            with open(previous_zip, "rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    previous_hasher.update(chunk)
            existing_hashes.add(previous_hasher.hexdigest())

        feed_changed = digest not in existing_hashes
        print("NEW FEED" if feed_changed else "UNCHANGED FEED — no new snapshot needed")

        # Save new snapshot and metadata
        if feed_changed:
            zip_target = VOLUME / f"budapest_gtfs_{snapshot_id}.zip"
            metadata_target = VOLUME / f"budapest_gtfs_{snapshot_id}.json"
            if zip_target.exists():
                raise FileExistsError(f"Snapshot already exists: {zip_target}")
            # Stage in volume then rename; avoids exposing a partially copied ZIP.
            stage_target = VOLUME / f".{zip_target.name}.uploading"
            if stage_target.exists():
                raise FileExistsError(f"Staging file already exists: {stage_target}")
            try:
                with open(local_tmp, "rb") as source, open(stage_target, "xb") as dest:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        dest.write(chunk)
                os.replace(stage_target, zip_target)
            finally:
                if stage_target.exists():
                    stage_target.unlink()

            metadata = {
                "snapshot_id": snapshot_id,
                "source_url": GTFS_URL,
                "downloaded_at_utc": downloaded_at.isoformat(),
                "sha256": digest,
                "file_size_bytes": zip_target.stat().st_size,
                "files": sorted(names),
            }
            metadata_target.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
            print(f"Saved: {zip_target}")
            print(f"Saved: {metadata_target}")
        else:
            print("Skipped snapshot creation; existing snapshots unchanged.")
        return feed_changed
    finally:
        if os.path.exists(local_tmp):
            os.remove(local_tmp)


# COMMAND ----------

# DBTITLE 1,02 - Fetch, Validate, and Store New Feed
feed_changed = fetch_gtfs_snapshot()

dbutils.jobs.taskValues.set(
    key="feed_changed",
    value=str(feed_changed).lower()
)

print(f"Job task value: feed_changed = {str(feed_changed).lower()}")

# COMMAND ----------

# DBTITLE 1,03 - Inspect Available Snapshots
for path in sorted(VOLUME.glob("budapest_gtfs_*.zip")):
    print(f"{path.name}: {path.stat().st_size:,} bytes")