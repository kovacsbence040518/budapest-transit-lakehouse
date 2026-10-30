# Databricks notebook source
# DBTITLE 1,09 - Incremental Bronze Ingestion
from pathlib import Path
from zipfile import ZipFile
from pyspark.sql import functions as F

VOLUME = Path("/Volumes/workspace/default/gtfs_raw")

DATASETS = [
    "routes",
    "stops",
    "trips",
    "stop_times",
    "calendar_dates",
]



def ingest_snapshot(zip_path):
    snapshot_id = zip_path.stem.replace("budapest_gtfs_", "", 1)
    extract_dir = VOLUME / "extracted" / snapshot_id
    extract_dir.mkdir(parents=True, exist_ok=True)

    with ZipFile(zip_path) as archive:
        available_files = set(archive.namelist())

        # Check that all required GTFS files exist
        required_files = {f"{dataset}.txt" for dataset in DATASETS}
        missing_files = required_files - available_files

        if missing_files:
            raise ValueError(
                f"Missing GTFS files in {zip_path.name}: {missing_files}"
            )

        for dataset in DATASETS:
            table_name = f"workspace.default.bronze_{dataset}"
            filename = f"{dataset}.txt"

            archive.extract(filename, extract_dir)

            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", False)
                .csv(str(extract_dir / filename))
                .withColumn("snapshot_id", F.lit(snapshot_id))
                .withColumn("ingested_at", F.current_timestamp())
            )

            # Never replace existing data with an empty dataset
            if df.limit(1).count() == 0:
                raise ValueError(
                    f"Empty dataset: {dataset} ({snapshot_id})"
                )

            # Atomically replace only this snapshot's rows
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option(
                    "replaceWhere",
                    f"snapshot_id = '{snapshot_id}'"
                )
                .saveAsTable(table_name)
            )

            print(f"INGESTED: {dataset} ({snapshot_id})")



for zip_path in sorted(VOLUME.glob("budapest_gtfs_*.zip")):
    ingest_snapshot(zip_path)