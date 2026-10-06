#!/usr/bin/env python3
"""CSV-only Bronze and Silver pipeline for the Odoo capstone."""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


SOURCES = {
    "customers": ("contacts", "contact-details/customers"),
    "sales_orders": ("api", "sales/orders"),
    "sales_order_lines": ("api", "sales/order-lines"),
    "products": ("api", "products/products"),
    "product_categories": ("api", "products/categories"),
}


def required_env(name: str, default: str) -> str:
    return os.environ.get(name, default).strip()


def discover_files(landing: Path, batch_filter: str | None) -> dict[str, list[str]]:
    found: dict[str, list[str]] = {name: [] for name in SOURCES}
    if not landing.is_dir():
        raise FileNotFoundError(f"Landing root does not exist: {landing}")
    for batch in sorted(landing.glob("batch-*")):
        if not batch.is_dir() or (batch_filter and batch.name != batch_filter):
            continue
        for dataset, (release_prefix, relative) in SOURCES.items():
            for release in sorted(batch.glob(f"{release_prefix}-*")):
                folder = release / "csv" / Path(relative)
                found[dataset].extend(str(p) for p in sorted(folder.glob("*.csv")))
    return found


def spark_session(master: str) -> SparkSession:
    return (
        SparkSession.builder.appName("trusted-odoo-sales-risk-phase-1-2")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def bronze(spark: SparkSession, sources: dict[str, list[str]], data_root: str) -> None:
    for dataset, files in sources.items():
        for source_file in files:
            match = re.search(r"/(batch-[^/]+)/", source_file.replace("\\", "/"))
            batch_id = match.group(1) if match else "batch-unknown"
            target = f"{data_root.rstrip('/')}/bronze/{dataset}/batch_id={batch_id}"
            # One immutable raw partition per source dataset and batch. Never replace completed evidence.
            try:
                already_landed = bool(spark.read.parquet(target).take(1))
            except Exception:
                already_landed = False
            if already_landed:
                print(f"Bronze exists; preserving and skipping {dataset} {batch_id}")
                continue
            frame = (
                spark.read.option("header", True)
                .option("encoding", "UTF-8")
                .option("mode", "PERMISSIVE")
                .option("multiLine", True)
                .csv(source_file)
                .withColumn("_source_file", F.input_file_name())
                .withColumn("_source_system", F.lit("odoo-csv"))
                .withColumn("_source_dataset", F.lit(dataset))
                .withColumn("_batch_id", F.lit(batch_id))
                .withColumn("_ingestion_timestamp", F.current_timestamp())
            )
            frame.write.mode("append").parquet(target)
            print(f"Bronze landed {dataset} {batch_id}: {frame.count()} rows")


def raw_dataset(spark: SparkSession, data_root: str, dataset: str):
    path = f"{data_root.rstrip('/')}/bronze/{dataset}"
    try:
        return spark.read.parquet(path)
    except Exception:
        return None


def m2o(column):
    """Parse Odoo CSV many-to-one serialization without losing source value."""
    cleaned = F.trim(column.cast("string"))
    return F.when(cleaned.isNull() | cleaned.isin("", "False", "false", "[]"), F.lit(None).cast("long")) \
        .when(cleaned.rlike(r"^\s*\[\s*\d+\s*,"), F.regexp_extract(cleaned, r"^\s*\[\s*(\d+)\s*,", 1).cast("long")) \
        .when(cleaned.rlike(r"^\d+$"), cleaned.cast("long")) \
        .otherwise(F.lit(None).cast("long"))


def bool_col(column):
    return F.when(F.lower(F.trim(column.cast("string"))).isin("true", "1", "yes"), F.lit(True)) \
        .when(F.lower(F.trim(column.cast("string"))).isin("false", "0", "no"), F.lit(False)) \
        .otherwise(F.lit(None).cast("boolean"))


def timestamp_col(column):
    return F.to_utc_timestamp(F.to_timestamp(column.cast("string")), "UTC")


def ensure_columns(frame, names: list[str]):
    for name in names:
        if name not in frame.columns:
            frame = frame.withColumn(name, F.lit(None).cast("string"))
    return frame


def write_quarantine(frame, dataset: str, data_root: str) -> None:
    target = f"{data_root.rstrip('/')}/quarantine/{dataset}"
    if frame.take(1):
        frame.write.mode("overwrite").parquet(target)
    else:
        # Empty outputs are still useful as a stable, discoverable dataset.
        frame.write.mode("overwrite").parquet(target)


def split_valid(frame, dataset: str, key_condition, timestamp_condition, data_root: str):
    errors = (
        F.when(~key_condition, "MISSING_OR_INVALID_KEY")
        .when(~timestamp_condition, "INVALID_WRITE_DATE")
        .otherwise(None)
    )
    marked = frame.withColumn("_error_code", errors)
    rejected = marked.filter(F.col("_error_code").isNotNull()).withColumn(
        "error_description", F.when(F.col("_error_code") == "INVALID_WRITE_DATE", "Source write_date is missing or unparseable")
        .otherwise("Required business key is missing or invalid")
    )
    write_quarantine(rejected, dataset, data_root)
    return marked.filter(F.col("_error_code").isNull()).drop("_error_code")


def resolve_versions(frame, keys: list[str], dataset: str, data_root: str):
    # Hash only business payload columns: metadata does not make a replay look changed.
    metadata = {"_source_file", "_source_system", "_source_dataset", "_batch_id", "_ingestion_timestamp", "_source_version"}
    payload_cols = sorted(c for c in frame.columns if c not in metadata)
    frame = frame.withColumn("_source_version", F.col("write_date"))
    frame = frame.withColumn("_payload_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload_cols])), 256))
    wver = Window.partitionBy(*keys, "_source_version")
    frame = frame.withColumn("_version_hashes", F.size(F.collect_set("_payload_hash").over(wver)))
    conflicts = frame.filter(F.col("_version_hashes") > 1).withColumn("error_code", F.lit("SAME_KEY_VERSION_CONFLICT")) \
        .withColumn("error_description", F.lit("Different source payloads share the same business key and write_date"))
    write_quarantine(conflicts, dataset + "_version_conflicts", data_root)
    usable = frame.filter(F.col("_version_hashes") == 1)
    wkey = Window.partitionBy(*keys).orderBy(
        F.col("_source_version").desc(), F.col("_payload_hash").asc(), F.col("_source_file").asc()
    )
    ranked = usable.withColumn("_version_rank", F.row_number().over(wkey))
    latest = Window.partitionBy(*keys)
    ranked = ranked.withColumn("_latest_source_version", F.max("_source_version").over(latest))
    dispositions = ranked.filter(F.col("_version_rank") > 1).withColumn(
        "disposition",
        F.when(F.col("_source_version") < F.col("_latest_source_version"), "SUPERSEDED")
        .otherwise("DUPLICATE_REPLAY"),
    )
    if dispositions.take(1):
        dispositions.write.mode("append").parquet(f"{data_root.rstrip('/')}/audit/record_dispositions/{dataset}")
    chosen = ranked.filter(F.col("_version_rank") == 1).drop(
        "_version_rank", "_version_hashes", "_payload_hash", "_latest_source_version"
    )
    # Non-winning deliveries stay visible in audit; original evidence remains immutable in Bronze.
    return chosen, ranked


def add_lineage(frame):
    return frame.withColumnRenamed("_batch_id", "batch_id") \
        .withColumnRenamed("_source_file", "source_reference") \
        .withColumnRenamed("_source_system", "source_system") \
        .withColumnRenamed("_ingestion_timestamp", "ingestion_timestamp")


def transform(spark: SparkSession, data_root: str, ledger_rows: list[tuple]) -> dict[str, object]:
    bronze_frames = {name: raw_dataset(spark, data_root, name) for name in SOURCES}
    silver: dict[str, object] = {}

    # Customer master: supplied feed has name and city but no segment.
    f = bronze_frames["customers"]
    if f is not None:
        f = ensure_columns(f, ["id", "name", "city", "category_id", "active", "create_date", "write_date"])
        f = f.withColumn("customer_id", F.col("id").cast("long")) \
            .withColumn("customer_name", F.col("name")) \
            .withColumn("city", F.when(F.col("city").isin("False", "false", ""), None).otherwise(F.col("city"))) \
            .withColumn("category_ids_raw", F.col("category_id")) \
            .withColumn("active", bool_col(F.col("active"))) \
            .withColumn("create_timestamp", timestamp_col(F.col("create_date"))) \
            .withColumn("write_timestamp", timestamp_col(F.col("write_date"))) \
            .withColumn("write_date", F.col("write_timestamp"))
        f = split_valid(f, "customers", F.col("customer_id").isNotNull(), F.col("write_timestamp").isNotNull(), data_root)
        # Build effective-dated customer history from every distinct source version before selecting current state.
        customer_payload = ["customer_name", "city", "category_ids_raw", "active"]
        f = f.withColumn("_customer_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in customer_payload])), 256))
        same_version = Window.partitionBy("customer_id", "write_timestamp")
        f = f.withColumn("_version_hash_count", F.size(F.collect_set("_customer_hash").over(same_version)))
        conflicts = f.filter(F.col("_version_hash_count") > 1).withColumn("error_code", F.lit("SAME_KEY_VERSION_CONFLICT")) \
            .withColumn("error_description", F.lit("Customer payloads differ for the same customer_id and write_date"))
        write_quarantine(conflicts, "customers_version_conflicts", data_root)
        f = f.filter(F.col("_version_hash_count") == 1).dropDuplicates(["customer_id", "write_timestamp"])
        history_window = Window.partitionBy("customer_id").orderBy("write_timestamp")
        history = f.withColumn("effective_from", F.col("write_timestamp")) \
            .withColumn("effective_to", F.lead("write_timestamp").over(history_window)) \
            .withColumn("current_flag", F.col("effective_to").isNull()) \
            .withColumn("customer_sk", F.xxhash64("customer_id", "write_timestamp"))
        silver["silver_customer_history"] = add_lineage(history.select(
            "customer_sk", "customer_id", "customer_name", "city", "category_ids_raw", "active",
            "effective_from", "effective_to", "current_flag", "create_timestamp", "write_timestamp",
            "_batch_id", "_source_file", "_source_system", "_ingestion_timestamp"
        ))
        f, _ = resolve_versions(f, ["customer_id"], "customers", data_root)
        silver["silver_customer_current"] = add_lineage(f.select("customer_id", "customer_name", "city", "category_ids_raw", "active", "create_timestamp", "write_timestamp", "_batch_id", "_source_file", "_source_system", "_ingestion_timestamp"))

    # Product category and product descriptive state use deterministic SCD1 selection.
    f = bronze_frames["product_categories"]
    if f is not None:
        f = ensure_columns(f, ["id", "name", "parent_id", "complete_name", "write_date"])
        f = f.withColumn("category_id", F.col("id").cast("long")) \
            .withColumn("parent_category_id", m2o(F.col("parent_id"))) \
            .withColumn("category_name", F.col("name")) \
            .withColumn("category_path", F.col("complete_name")) \
            .withColumn("write_timestamp", timestamp_col(F.col("write_date"))) \
            .withColumn("write_date", F.col("write_timestamp"))
        f = split_valid(f, "product_categories", F.col("category_id").isNotNull(), F.col("write_timestamp").isNotNull(), data_root)
        f, _ = resolve_versions(f, ["category_id"], "product_categories", data_root)
        silver["silver_product_category"] = add_lineage(f.select("category_id", "category_name", "parent_category_id", "category_path", "write_timestamp", "_batch_id", "_source_file", "_source_system", "_ingestion_timestamp"))

    f = bronze_frames["products"]
    if f is not None:
        f = ensure_columns(f, ["id", "default_code", "name", "categ_id", "list_price", "active", "create_date", "write_date"])
        f = f.withColumn("product_id", F.col("id").cast("long")) \
            .withColumn("product_code", F.col("default_code")) \
            .withColumn("product_name", F.col("name")) \
            .withColumn("category_id", m2o(F.col("categ_id"))) \
            .withColumn("list_price", F.col("list_price").cast(DecimalType(18, 4))) \
            .withColumn("active", bool_col(F.col("active"))) \
            .withColumn("create_timestamp", timestamp_col(F.col("create_date"))) \
            .withColumn("write_timestamp", timestamp_col(F.col("write_date"))) \
            .withColumn("write_date", F.col("write_timestamp"))
        f = split_valid(f, "products", F.col("product_id").isNotNull(), F.col("write_timestamp").isNotNull(), data_root)
        f, _ = resolve_versions(f, ["product_id"], "products", data_root)
        silver["silver_product"] = add_lineage(f.select("product_id", "product_code", "product_name", "category_id", "list_price", "active", "create_timestamp", "write_timestamp", "_batch_id", "_source_file", "_source_system", "_ingestion_timestamp"))

    f = bronze_frames["sales_orders"]
    if f is not None:
        f = ensure_columns(f, ["id", "name", "partner_id", "date_order", "state", "amount_untaxed", "amount_total", "currency_id", "write_date"])
        f = f.withColumn("order_id", F.col("id").cast("long")) \
            .withColumn("order_name", F.col("name")) \
            .withColumn("customer_id", m2o(F.col("partner_id"))) \
            .withColumn("order_timestamp", timestamp_col(F.col("date_order"))) \
            .withColumn("state", F.col("state")) \
            .withColumn("amount_untaxed", F.col("amount_untaxed").cast(DecimalType(18, 2))) \
            .withColumn("amount_total", F.col("amount_total").cast(DecimalType(18, 2))) \
            .withColumn("currency_id", m2o(F.col("currency_id"))) \
            .withColumn("write_timestamp", timestamp_col(F.col("write_date"))) \
            .withColumn("write_date", F.col("write_timestamp"))
        f = split_valid(f, "sales_orders", F.col("order_id").isNotNull() & F.col("customer_id").isNotNull(), F.col("write_timestamp").isNotNull() & F.col("order_timestamp").isNotNull(), data_root)
        f, _ = resolve_versions(f, ["order_id"], "sales_orders", data_root)
        silver["silver_sales_order"] = add_lineage(f.select("order_id", "order_name", "customer_id", "order_timestamp", "state", "amount_untaxed", "amount_total", "currency_id", "write_timestamp", "_batch_id", "_source_file", "_source_system", "_ingestion_timestamp"))

    f = bronze_frames["sales_order_lines"]
    if f is not None:
        f = ensure_columns(f, ["id", "order_id", "product_id", "product_uom_qty", "price_unit", "discount", "price_subtotal", "create_date", "write_date"])
        f = f.withColumn("line_id", F.col("id").cast("long")) \
            .withColumn("order_id", m2o(F.col("order_id"))) \
            .withColumn("product_id", m2o(F.col("product_id"))) \
            .withColumn("quantity", F.col("product_uom_qty").cast(DecimalType(18, 4))) \
            .withColumn("unit_price", F.col("price_unit").cast(DecimalType(18, 4))) \
            .withColumn("discount_percent", F.col("discount").cast(DecimalType(9, 4))) \
            .withColumn("source_subtotal", F.col("price_subtotal").cast(DecimalType(18, 2))) \
            .withColumn("create_timestamp", timestamp_col(F.col("create_date"))) \
            .withColumn("write_timestamp", timestamp_col(F.col("write_date"))) \
            .withColumn("write_date", F.col("write_timestamp"))
        key_ok = F.col("line_id").isNotNull() & F.col("order_id").isNotNull() & F.col("product_id").isNotNull()
        f = split_valid(f, "sales_order_lines", key_ok, F.col("write_timestamp").isNotNull(), data_root)
        f = f.withColumn("_rule_error", F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "INVALID_QUANTITY")
            .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "INVALID_UNIT_PRICE")
            .when(F.col("discount_percent").isNull() | (F.col("discount_percent") < 0) | (F.col("discount_percent") > 100), "INVALID_DISCOUNT"))
        invalid = f.filter(F.col("_rule_error").isNotNull()).withColumnRenamed("_rule_error", "error_code") \
            .withColumn("error_description", F.concat(F.lit("Business rule failed: "), F.col("error_code")))
        write_quarantine(invalid, "sales_order_lines_business_rules", data_root)
        f = f.filter(F.col("_rule_error").isNull()).drop("_rule_error")
        f, _ = resolve_versions(f, ["order_id", "line_id"], "sales_order_lines", data_root)
        f = f.withColumn("line_net_value", F.bround(
            F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_percent") / F.lit(100)), 2
        ).cast(DecimalType(18, 2)))
        silver["silver_sales_order_line"] = add_lineage(f.select("order_id", "line_id", "product_id", "quantity", "unit_price", "discount_percent", "source_subtotal", "line_net_value", "create_timestamp", "write_timestamp", "_batch_id", "_source_file", "_source_system", "_ingestion_timestamp"))

    # Enforce referential validity once deduplicated parent state has been resolved.
    if "silver_customer_current" in silver and "silver_sales_order" in silver:
        customers = silver["silver_customer_current"].select("customer_id").distinct()
        orders = silver["silver_sales_order"].join(customers, "customer_id", "left_anti")
        if orders.take(1):
            write_quarantine(orders.withColumn("error_code", F.lit("UNKNOWN_CUSTOMER_REFERENCE"))
                .withColumn("error_description", F.lit("Order customer_id is absent from the customer source")), "sales_orders_references", data_root)
            valid_orders = silver["silver_sales_order"].join(customers, "customer_id", "inner")
            silver["silver_sales_order"] = valid_orders
    if "silver_product" in silver and "silver_sales_order_line" in silver:
        products = silver["silver_product"].select("product_id").distinct()
        invalid_lines = silver["silver_sales_order_line"].join(products, "product_id", "left_anti")
        if invalid_lines.take(1):
            write_quarantine(invalid_lines.withColumn("error_code", F.lit("UNKNOWN_PRODUCT_REFERENCE"))
                .withColumn("error_description", F.lit("Line product_id is absent from the product source")), "sales_order_lines_references", data_root)
            silver["silver_sales_order_line"] = silver["silver_sales_order_line"].join(products, "product_id", "inner")
    if "silver_sales_order" in silver and "silver_sales_order_line" in silver:
        orders = silver["silver_sales_order"].select("order_id").distinct()
        invalid_lines = silver["silver_sales_order_line"].join(orders, "order_id", "left_anti")
        if invalid_lines.take(1):
            write_quarantine(invalid_lines.withColumn("error_code", F.lit("UNKNOWN_ORDER_REFERENCE"))
                .withColumn("error_description", F.lit("Line order_id is absent from the sales order source")), "sales_order_lines_order_references", data_root)
            silver["silver_sales_order_line"] = silver["silver_sales_order_line"].join(orders, "order_id", "inner")

    for name, frame in silver.items():
        target = f"{data_root.rstrip('/')}/silver/{name}"
        frame.write.mode("overwrite").parquet(target)
        count = frame.count()
        ledger_rows.append((name, "silver", count, count, 0, 0, 0, datetime.now(timezone.utc).isoformat()))
        print(f"Silver published {name}: {count} current rows")
    return silver


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("bronze", "silver", "all"), default="all")
    parser.add_argument("--batch-id", help="Process only this landing batch folder, e.g. batch-1")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    landing_root = Path(required_env("LANDING_ROOT", "/mnt/d/DATA_ENG_TRAINING/capstone/Original/landing-zone-Oct-10-5PM"))
    data_root = required_env("DATA_ROOT", "/mnt/d/DATA_ENG_TRAINING/capstone/project/data")
    master = required_env("SPARK_MASTER", "local[*]")
    source_files = discover_files(landing_root, args.batch_id)
    missing = [name for name, files in source_files.items() if not files]
    if missing:
        raise FileNotFoundError("Required CSV datasets not found: " + ", ".join(missing))
    spark = spark_session(master)
    ledger_rows: list[tuple] = []
    try:
        if args.stage in ("bronze", "all"):
            bronze(spark, source_files, data_root)
        if args.stage in ("silver", "all"):
            transform(spark, data_root, ledger_rows)
        if ledger_rows:
            columns = ["dataset", "stage", "records_read", "accepted_records", "rejected_records", "duplicate_records", "superseded_records", "recorded_at_utc"]
            ledger = spark.createDataFrame(ledger_rows, columns)
            ledger.write.mode("append").parquet(f"{data_root.rstrip('/')}/audit/batch_ledger")
        print("Pipeline completed successfully")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # concise command-line error; Spark emits its own diagnostics above this line.
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        raise
