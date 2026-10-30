# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,00 - Bronze Ingestion Configuration
from pathlib import Path
from zipfile import ZipFile
from pyspark.sql import functions as F

VOLUME = Path("/Volumes/workspace/default/gtfs_raw")

DATASETS = [
    "routes",
    "stops",
    "trips",
    "stop_times",
    "calendar_dates"
]

print("Bronze ingestion configured.")

# COMMAND ----------

# DBTITLE 1,01 - Incremental Bronze Ingestion
def ingest_snapshot(zip_path):
    snapshot_id = zip_path.stem.replace("budapest_gtfs_", "", 1)
    extract_dir = VOLUME / "extracted" / snapshot_id
    extract_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== Processing snapshot: {snapshot_id} ===")

    with ZipFile(zip_path) as archive:
        available_files = set(archive.namelist())

        required_files = {
            f"{dataset}.txt" for dataset in DATASETS
        }

        missing_files = required_files - available_files

        if missing_files:
            raise ValueError(
                f"Missing GTFS files: {sorted(missing_files)}"
            )

        for dataset in DATASETS:
            table_name = f"workspace.default.bronze_{dataset}"
            filename = f"{dataset}.txt"

            # Extract and read the source dataset
            archive.extract(filename, extract_dir)

            df = (
                spark.read
                .option("header", True)
                .option("inferSchema", False)
                .csv(str(extract_dir / filename))
                .withColumn("snapshot_id", F.lit(snapshot_id))
                .withColumn("ingested_at", F.current_timestamp())
            )

            source_count = df.count()

            if source_count == 0:
                raise ValueError(
                    f"Empty dataset: {dataset} ({snapshot_id})"
                )

            # Check whether this dataset is already ingested
            existing_count = 0

            if spark.catalog.tableExists(table_name):
                existing_count = (
                    spark.table(table_name)
                    .filter(F.col("snapshot_id") == snapshot_id)
                    .count()
                )

            if existing_count == source_count:
                print(
                    f"SKIPPED: {dataset} "
                    f"({source_count:,} rows already present)"
                )
                continue

            if existing_count > 0:
                print(
                    f"REPAIR: {dataset} "
                    f"(existing={existing_count:,}, "
                    f"expected={source_count:,})"
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

            print(
                f"INGESTED: {dataset} "
                f"({source_count:,} rows)"
            )

# COMMAND ----------

# DBTITLE 1,02 - Process Available Snapshots
zip_files = sorted(VOLUME.glob("budapest_gtfs_*.zip"))

if not zip_files:
    raise FileNotFoundError("No GTFS ZIP snapshots found.")

print(f"Found {len(zip_files)} snapshots.")

for zip_path in zip_files:
    ingest_snapshot(zip_path)

print("\nBronze ingestion completed.")