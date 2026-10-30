from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile
import hashlib
import json

GTFS_URL = "https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"
RAW_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"


def download_gtfs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    downloaded_at = datetime.now(timezone.utc)
    snapshot_id = downloaded_at.strftime("%Y%m%dT%H%M%SZ")
    zip_path = RAW_DIR / f"budapest_gtfs_{snapshot_id}.zip"

    hasher = hashlib.sha256()

    with urlopen(GTFS_URL, timeout=60) as response:
        with open(zip_path, "wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                hasher.update(chunk)

    with ZipFile(zip_path) as archive:
        filenames = archive.namelist()

    metadata = {
        "source_url": GTFS_URL,
        "downloaded_at_utc": downloaded_at.isoformat(),
        "sha256": hasher.hexdigest(),
        "file_size_bytes": zip_path.stat().st_size,
        "files": filenames,
    }

    metadata_path = zip_path.with_suffix(".json")
    metadata_path.write_text(
        json.dumps(metadata, indent=2),
        encoding="utf-8",
    )

    print(f"Downloaded: {zip_path.name}")
    print(f"Size: {zip_path.stat().st_size / 1024**2:.2f} MB")
    print(f"SHA-256: {metadata['sha256']}")
    print(f"GTFS files: {filenames}")


if __name__ == "__main__":
    download_gtfs()