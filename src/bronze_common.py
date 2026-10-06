"""Shared CSV discovery and Bronze landing helpers used by per-table jobs."""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


SOURCES = {
    "customers": ("contacts", "contact-details/customers"),
    "sales_orders": ("api", "sales/orders"),
    "sales_order_lines": ("api", "sales/order-lines"),
    "products": ("api", "products/products"),
    "product_categories": ("api", "products/categories"),
}


def env(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def discover_files(landing_root: str | Path, batch_filter: str | None = None) -> dict[str, list[str]]:
    landing = Path(landing_root)
    if not landing.is_dir():
        raise FileNotFoundError(f"Landing root does not exist or is not mounted: {landing}")
    found = {dataset: [] for dataset in SOURCES}
    for batch in sorted(landing.glob("batch-*")):
        if not batch.is_dir() or (batch_filter and batch.name != batch_filter):
            continue
        for dataset, (prefix, relative_path) in SOURCES.items():
            for release in sorted(batch.glob(f"{prefix}-*")):
                csv_dir = release / "csv" / Path(relative_path)
                found[dataset].extend(str(path) for path in sorted(csv_dir.glob("*.csv")))
    return found


def spark_session(app_name: str, master: str | None = None) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master(master or env("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", env("TIMEZONE", "UTC"))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def batch_from_path(source_path: str) -> str:
    parts = source_path.replace("\\", "/").split("/")
    return next((part for part in parts if part.startswith("batch-")), "batch-unknown")


def path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return filesystem.exists(hadoop_path)


def land_dataset(
    spark: SparkSession,
    dataset: str,
    landing_root: str | Path,
    hdfs_upload_path: str,
    batch_filter: str | None = None,
) -> None:
    if dataset not in SOURCES:
        raise ValueError(f"Unsupported Bronze dataset: {dataset}")
    files = discover_files(landing_root, batch_filter)[dataset]
    if not files:
        raise FileNotFoundError(f"No CSV files found for {dataset} under {landing_root}")

    by_batch: dict[str, list[str]] = {}
    for source_file in files:
        by_batch.setdefault(batch_from_path(source_file), []).append(source_file)

    bronze_root = f"{hdfs_upload_path.rstrip('/')}/bronze"
    for batch_id, batch_files in sorted(by_batch.items()):
        target = f"{bronze_root}/{dataset}/batch_id={batch_id}"
        if path_exists(spark, target):
            print(f"Bronze already exists; preserving {target}")
            continue

        raw = (
            spark.read.option("header", True)
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .option("multiLine", True)
            .csv(batch_files)
            .withColumn("_source_file", F.input_file_name())
            .withColumn("_source_system", F.lit("odoo-csv"))
            .withColumn("_source_dataset", F.lit(dataset))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_ingestion_timestamp", F.current_timestamp())
        )
        row_count = raw.count()
        raw.write.mode("errorifexists").parquet(target)
        print(f"Bronze landed {dataset}, batch {batch_id}: {row_count} rows from {len(batch_files)} CSV file(s) at {target}")


def run_one(dataset: str, batch_id: str | None = None) -> None:
    landing_root = env(
        "LANDING_ROOT",
        "/mnt/d/DATA_ENG_TRAINING/capstone/Original/landing-zone-Oct-10-5PM",
    )
    hdfs_upload_path = env("HDFS_UPLOAD_PATH", "hdfs:///user/your_username/capstone")
    spark = spark_session(f"odoo-bronze-{dataset}")
    try:
        land_dataset(spark, dataset, landing_root, hdfs_upload_path, batch_id or env("BATCH_ID", "") or None)
    finally:
        spark.stop()
