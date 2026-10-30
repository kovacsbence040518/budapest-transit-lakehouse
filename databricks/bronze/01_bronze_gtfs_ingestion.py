# Databricks notebook source
# DBTITLE 1,00 - PySpark Environment Test
from pyspark.sql import functions as F

df = spark.createDataFrame(
    [
        ("Budapest", "M4", 10),
        ("Budapest", "4-6", 20),
        ("Budapest", "7", 30),
    ],
    ["city", "route", "sample_value"],
)

display(df)

# COMMAND ----------

# DBTITLE 1,01 - Verify GTFS Files
display(dbutils.fs.ls("/Volumes/workspace/default/gtfs_raw/"))

# COMMAND ----------

# DBTITLE 1,02 - Extract GTFS Files
from pathlib import Path
from zipfile import ZipFile

volume = Path("/Volumes/workspace/default/gtfs_raw")
snapshot_id = "20261029T140343Z"

zip_path = volume / f"budapest_gtfs_{snapshot_id}.zip"
extract_dir = volume / "extracted" / snapshot_id

extract_dir.mkdir(parents=True, exist_ok=True)

with ZipFile(zip_path) as archive:
    archive.extract("routes.txt", extract_dir)
    archive.extract("stops.txt", extract_dir)

print("Extracted successfully:")
for filename in ["routes.txt", "stops.txt"]:
    path = extract_dir / filename
    print(f"{filename}: {path.stat().st_size / 1024**2:.2f} MB")

# COMMAND ----------

# DBTITLE 1,03 - Bronze Routes Ingestion
from pyspark.sql import functions as F

snapshot_id = "20261029T140343Z"

routes_path = (
    f"/Volumes/workspace/default/gtfs_raw/"
    f"extracted/{snapshot_id}/routes.txt"
)

routes_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(routes_path)
    .withColumn("snapshot_id", F.lit(snapshot_id))
    .withColumn("ingested_at", F.current_timestamp())
)

routes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_routes")

print(f"Bronze routes loaded: {routes_df.count()} rows")

display(spark.table("workspace.default.bronze_routes").limit(5))

# COMMAND ----------

# DBTITLE 1,04 - Bronze Stops Ingestion
from pyspark.sql import functions as F

snapshot_id = "20261029T140343Z"

stops_path = (
    f"/Volumes/workspace/default/gtfs_raw/"
    f"extracted/{snapshot_id}/stops.txt"
)

stops_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(stops_path)
    .withColumn("snapshot_id", F.lit(snapshot_id))
    .withColumn("ingested_at", F.current_timestamp())
)

stops_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_stops")

print(f"Bronze stops loaded: {stops_df.count()} rows")

display(spark.table("workspace.default.bronze_stops").limit(5))

# COMMAND ----------

# DBTITLE 1,05 - Extract Trip and Schedule Data
from pathlib import Path
from zipfile import ZipFile

snapshot_id = "20261029T140343Z"
volume = Path("/Volumes/workspace/default/gtfs_raw")

zip_path = volume / f"budapest_gtfs_{snapshot_id}.zip"
extract_dir = volume / "extracted" / snapshot_id

files_to_extract = [
    "trips.txt",
    "stop_times.txt",
    "calendar_dates.txt",
]

with ZipFile(zip_path) as archive:
    for filename in files_to_extract:
        archive.extract(filename, extract_dir)

        path = extract_dir / filename
        size_mb = path.stat().st_size / (1024 ** 2)

        print(f"{filename}: {size_mb:.2f} MB")

# COMMAND ----------

# DBTITLE 1,06 - Bronze Trips Ingestion
from pyspark.sql import functions as F

snapshot_id = "20261029T140343Z"

trips_path = (
    f"/Volumes/workspace/default/gtfs_raw/"
    f"extracted/{snapshot_id}/trips.txt"
)

trips_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(trips_path)
    .withColumn("snapshot_id", F.lit(snapshot_id))
    .withColumn("ingested_at", F.current_timestamp())
)

trips_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_trips")

print(f"Bronze trips loaded: {trips_df.count()} rows")

display(spark.table("workspace.default.bronze_trips").limit(5))

# COMMAND ----------

# DBTITLE 1,07 - Bronze Stop Times Ingestion
from pyspark.sql import functions as F

snapshot_id = "20261029T140343Z"

stop_times_path = (
    f"/Volumes/workspace/default/gtfs_raw/"
    f"extracted/{snapshot_id}/stop_times.txt"
)

stop_times_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(stop_times_path)
    .withColumn("snapshot_id", F.lit(snapshot_id))
    .withColumn("ingested_at", F.current_timestamp())
)

stop_times_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_stop_times")

print(
    "Bronze stop times loaded:",
    spark.table("workspace.default.bronze_stop_times").count(),
    "rows"
)

display(
    spark.table("workspace.default.bronze_stop_times").limit(5)
)

# COMMAND ----------

# DBTITLE 1,08 - Bronze Calendar Dates Ingestion
from pyspark.sql import functions as F

snapshot_id = "20261029T140343Z"

calendar_path = (
    f"/Volumes/workspace/default/gtfs_raw/"
    f"extracted/{snapshot_id}/calendar_dates.txt"
)

calendar_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(calendar_path)
    .withColumn("snapshot_id", F.lit(snapshot_id))
    .withColumn("ingested_at", F.current_timestamp())
)

calendar_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_calendar_dates")

print(
    "Bronze calendar dates loaded:",
    spark.table("workspace.default.bronze_calendar_dates").count(),
    "rows"
)

display(
    spark.table("workspace.default.bronze_calendar_dates").limit(5)
)

# COMMAND ----------

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

    with ZipFile(zip_path) as archive:
        for dataset in DATASETS:
            table_name = f"workspace.default.bronze_{dataset}"

            # Skip snapshots already ingested
            if spark.catalog.tableExists(table_name):
                exists = (
                    spark.table(table_name)
                    .filter(F.col("snapshot_id") == snapshot_id)
                    .limit(1)
                    .count() > 0
                )

                if exists:
                    print(f"SKIPPED: {dataset} ({snapshot_id})")
                    continue

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

            (
                df.write
                .format("delta")
                .mode("append")
                .saveAsTable(table_name)
            )

            print(f"INGESTED: {dataset} ({snapshot_id})")


for zip_path in sorted(VOLUME.glob("budapest_gtfs_*.zip")):
    ingest_snapshot(zip_path)