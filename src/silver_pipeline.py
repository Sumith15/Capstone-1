"""Incremental, replay-safe Silver processing for the five core Odoo CSV datasets."""

from __future__ import annotations

import uuid
import json
from datetime import datetime, timezone
from decimal import Decimal
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType, TimestampType

from bronze_common import SOURCES, path_exists


ENTITY_KEYS = {
    "customers": ["customer_id"],
    "product_categories": ["category_id"],
    "products": ["product_id"],
    "sales_orders": ["order_id"],
    "sales_order_lines": ["order_id", "line_id"],
}

LINEAGE = ["source_reference", "source_system", "source_dataset", "batch_id", "ingestion_timestamp", "raw_payload"]


def load_rules() -> dict:
    config = Path(__file__).resolve().parent.parent / "config" / "business_rules.yml"
    # The rules file uses JSON syntax, which is valid YAML and needs no extra Python dependency.
    return json.loads(config.read_text(encoding="utf-8"))


def timestamp(column):
    # Session timezone is UTC; source timestamps have no zone suffix and are interpreted as UTC.
    return F.to_timestamp(column.cast("string"))


def many_to_one(column):
    value = F.trim(column.cast("string"))
    return (
        F.when(value.isNull() | value.isin("", "False", "false", "[]"), F.lit(None).cast(LongType()))
        .when(value.rlike(r"^\s*\[\s*\d+\s*,"), F.regexp_extract(value, r"^\s*\[\s*(\d+)\s*,", 1).cast(LongType()))
        .when(value.rlike(r"^\d+$"), value.cast(LongType()))
        .otherwise(F.lit(None).cast(LongType()))
    )


def boolean(column):
    value = F.lower(F.trim(column.cast("string")))
    return (
        F.when(value.isin("true", "1", "yes"), F.lit(True))
        .when(value.isin("false", "0", "no"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def raw_payload(frame: DataFrame):
    raw_columns = [c for c in frame.columns if not c.startswith("_")]
    return F.to_json(F.struct(*[F.col(c) for c in raw_columns]))


def metadata_columns(frame: DataFrame):
    return [
        F.col("_source_file").alias("source_reference"),
        F.col("_source_system").alias("source_system"),
        F.col("_source_dataset").alias("source_dataset"),
        F.col("_batch_id").alias("batch_id"),
        F.col("_ingestion_timestamp").alias("ingestion_timestamp"),
        raw_payload(frame).alias("raw_payload"),
    ]


def prepare(frame: DataFrame, dataset: str) -> DataFrame:
    """Map raw CSV strings to explicit trusted columns; retain recoverable source evidence."""
    if dataset == "customers":
        return frame.select(
            F.col("id").cast(LongType()).alias("customer_id"), F.col("name").alias("customer_name"),
            F.when(F.lower(F.trim(F.col("city"))) == "false", None).otherwise(F.col("city")).alias("city"),
            F.col("category_id").alias("category_ids_raw"), boolean(F.col("active")).alias("active"),
            timestamp(F.col("create_date")).alias("create_timestamp"), timestamp(F.col("write_date")).alias("write_timestamp"),
            *metadata_columns(frame),
        )
    if dataset == "product_categories":
        return frame.select(
            F.col("id").cast(LongType()).alias("category_id"), F.col("name").alias("category_name"),
            many_to_one(F.col("parent_id")).alias("parent_category_id"), F.col("complete_name").alias("category_path"),
            timestamp(F.col("write_date")).alias("write_timestamp"),
            *metadata_columns(frame),
        )
    if dataset == "products":
        return frame.select(
            F.col("id").cast(LongType()).alias("product_id"), F.col("default_code").alias("product_code"),
            F.col("name").alias("product_name"), many_to_one(F.col("categ_id")).alias("category_id"),
            F.col("list_price").cast(DecimalType(18, 4)).alias("list_price"), boolean(F.col("active")).alias("active"),
            timestamp(F.col("create_date")).alias("create_timestamp"), timestamp(F.col("write_date")).alias("write_timestamp"),
            *metadata_columns(frame),
        )
    if dataset == "sales_orders":
        return frame.select(
            F.col("id").cast(LongType()).alias("order_id"), F.col("name").alias("order_name"),
            many_to_one(F.col("partner_id")).alias("customer_id"), timestamp(F.col("date_order")).alias("order_timestamp"),
            F.lower(F.trim(F.col("state"))).alias("state"),
            F.col("amount_untaxed").cast(DecimalType(18, 2)).alias("amount_untaxed"),
            F.col("amount_total").cast(DecimalType(18, 2)).alias("amount_total"),
            many_to_one(F.col("currency_id")).alias("currency_id"), timestamp(F.col("write_date")).alias("write_timestamp"),
            *metadata_columns(frame),
        )
    if dataset == "sales_order_lines":
        return frame.select(
            many_to_one(F.col("order_id")).alias("order_id"), F.col("id").cast(LongType()).alias("line_id"),
            many_to_one(F.col("product_id")).alias("product_id"),
            F.col("product_uom_qty").cast(DecimalType(18, 4)).alias("quantity"),
            F.col("price_unit").cast(DecimalType(18, 4)).alias("unit_price"),
            F.col("discount").cast(DecimalType(9, 4)).alias("discount_percent"),
            F.col("price_subtotal").cast(DecimalType(18, 2)).alias("source_subtotal"),
            timestamp(F.col("create_date")).alias("create_timestamp"), timestamp(F.col("write_date")).alias("write_timestamp"),
            *metadata_columns(frame),
        )
    raise ValueError(f"Unknown core dataset: {dataset}")


def invalid_condition(frame: DataFrame, dataset: str, rules: dict):
    errors = []
    keys = ENTITY_KEYS[dataset]
    for key in keys:
        errors.append((F.col(key).isNull(), f"INVALID_{key.upper()}"))
    errors.append((F.col("write_timestamp").isNull(), "INVALID_WRITE_TIMESTAMP"))
    if dataset in ("customers", "products", "sales_order_lines"):
        errors.append((F.col("create_timestamp").isNull(), "INVALID_CREATE_TIMESTAMP"))
        errors.append((F.col("create_timestamp") > F.col("write_timestamp"), "CREATE_AFTER_WRITE_TIMESTAMP"))
    if dataset == "customers":
        errors.append((F.col("customer_name").isNull() | (F.trim(F.col("customer_name")) == ""), "MISSING_CUSTOMER_NAME"))
    elif dataset == "product_categories":
        errors.append((F.col("category_name").isNull() | (F.trim(F.col("category_name")) == ""), "MISSING_CATEGORY_NAME"))
    elif dataset == "products":
        errors.append((F.col("product_name").isNull() | (F.trim(F.col("product_name")) == ""), "MISSING_PRODUCT_NAME"))
        errors.append((F.col("list_price").isNull() | (F.col("list_price") < F.lit(Decimal(rules["list_price_min_inclusive"]))), "INVALID_LIST_PRICE"))
    elif dataset == "sales_orders":
        errors.append((F.col("order_timestamp").isNull(), "INVALID_ORDER_TIMESTAMP"))
        errors.append((F.col("state").isNull() | (F.col("state") == ""), "MISSING_ORDER_STATE"))
        errors.append((F.col("amount_untaxed").isNull() | (F.col("amount_untaxed") < 0), "INVALID_ORDER_AMOUNT"))
        errors.append((F.col("currency_id").isNull(), "INVALID_CURRENCY_REFERENCE"))
    elif dataset == "sales_order_lines":
        errors.extend([
            (F.col("quantity").isNull() | (F.col("quantity") <= F.lit(Decimal(rules["quantity_min_exclusive"]))), "INVALID_QUANTITY"),
            (F.col("unit_price").isNull() | (F.col("unit_price") < F.lit(Decimal(rules["unit_price_min_inclusive"]))), "INVALID_UNIT_PRICE"),
            (F.col("discount_percent").isNull() | (F.col("discount_percent") < F.lit(Decimal(rules["discount_min_inclusive"]))) | (F.col("discount_percent") > F.lit(Decimal(rules["discount_max_inclusive"]))), "INVALID_DISCOUNT"),
        ])
    result = F.lit(None).cast(StringType())
    for condition, code in reversed(errors):
        result = F.when(condition, F.lit(code)).otherwise(result)
    return result


def quarantine(frame: DataFrame, dataset: str, attempt_id: str, quarantine_root: str, reason_col: str = "_error_code") -> int:
    bad = frame.filter(F.col(reason_col).isNotNull()).withColumnRenamed(reason_col, "error_code") \
        .withColumn("error_description", F.regexp_replace(F.col("error_code"), "_", " ")) \
        .withColumn("quarantined_at_utc", F.current_timestamp())
    count = bad.count()
    if count:
        path = f"{quarantine_root.rstrip('/')}/{dataset}/attempt_id={attempt_id}"
        bad.write.mode("append").parquet(path)
    return count


def read_optional(spark: SparkSession, path: str):
    if not path_exists(spark, path):
        return None
    return spark.read.parquet(path)


def batch_directories(spark: SparkSession, path: str) -> list[str]:
    if not path_exists(spark, path):
        return []
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return sorted(status.getPath().toString() for status in fs.listStatus(hadoop_path) if status.isDirectory() and status.getPath().getName().startswith("batch_id="))


def read_batch(spark: SparkSession, bronze_root: str, dataset: str, batch_id: str):
    path = f"{bronze_root.rstrip('/')}/{dataset}/batch_id={batch_id}"
    if not path_exists(spark, path):
        return None
    return spark.read.parquet(path)


def load_history(spark: SparkSession, silver_root: str, dataset: str):
    return read_optional(spark, f"{silver_root.rstrip('/')}/record_history/{dataset}")


def existing_successful_batches(spark: SparkSession, audit_root: str) -> set[str]:
    ledger = read_optional(spark, f"{audit_root.rstrip('/')}/batch_ledger")
    if ledger is None:
        return set()
    return {
        row.batch_id for row in ledger.filter((F.col("record_type") == "BATCH") & (F.col("status") == "SUCCESS"))
        .select("batch_id").distinct().collect()
    }


def valid_parent_ids(spark: SparkSession, silver_root: str, incoming, dataset: str, id_column: str):
    history = load_history(spark, silver_root, dataset)
    parts = [f.select(F.col(id_column).alias(id_column)) for f in (history, incoming) if f is not None]
    if not parts:
        return None
    return reduce(lambda left, right: left.unionByName(right), parts).filter(F.col(id_column).isNotNull()).distinct()


def validate_and_quarantine(frame: DataFrame, dataset: str, attempt_id: str, quarantine_root: str, rules: dict):
    marked = frame.withColumn("_error_code", invalid_condition(frame, dataset, rules))
    rejected = quarantine(marked, dataset, attempt_id, quarantine_root)
    return marked.filter(F.col("_error_code").isNull()).drop("_error_code"), rejected


def filter_reference(frame: DataFrame | None, dataset: str, column: str, parent_ids: DataFrame | None, attempt_id: str, quarantine_root: str):
    if frame is None or parent_ids is None:
        return frame, 0
    keys = parent_ids.select(F.col(parent_ids.columns[0]).alias(column)).distinct()
    bad = frame.join(keys, column, "left_anti").withColumn("_error_code", F.lit(f"UNKNOWN_{column.upper()}_REFERENCE"))
    rejected = quarantine(bad, dataset, attempt_id, quarantine_root)
    good = frame.join(keys, column, "inner")
    return good, rejected


def merge_versions(
    spark: SparkSession,
    dataset: str,
    incoming: DataFrame | None,
    silver_root: str,
    quarantine_root: str,
    audit_root: str,
    attempt_id: str,
    batch_id: str,
):
    old = load_history(spark, silver_root, dataset)
    if incoming is None:
        return old, 0, 0, 0
    keys = ENTITY_KEYS[dataset]
    payload_columns = [c for c in incoming.columns if c not in LINEAGE]
    incoming = incoming.withColumn("_is_new_delivery", F.lit(True))
    if old is not None:
        old = old.withColumn("_is_new_delivery", F.lit(False))
        combined = old.unionByName(incoming, allowMissingColumns=False)
    else:
        combined = incoming

    combined = combined.withColumn("_payload_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload_columns])), 256))
    by_version = Window.partitionBy(*keys, "write_timestamp")
    combined = combined.withColumn("_version_payload_count", F.size(F.collect_set("_payload_hash").over(by_version)))
    conflict_rows = combined.filter(F.col("_version_payload_count") > 1).withColumn("_error_code", F.lit("SAME_KEY_VERSION_CONFLICT"))
    conflict_count = quarantine(conflict_rows, dataset + "_version_conflicts", attempt_id, quarantine_root)
    if batch_id:
        conflict_count = conflict_rows.filter(F.col("_is_new_delivery") & (F.col("batch_id") == batch_id)).count()
    unique = combined.filter(F.col("_version_payload_count") == 1)
    choose_duplicate = Window.partitionBy(*keys, "write_timestamp").orderBy(
        F.col("_is_new_delivery").asc(), F.col("source_reference").asc(), F.col("_payload_hash").asc()
    )
    unique = unique.withColumn("_same_version_rank", F.row_number().over(choose_duplicate))
    duplicate_rows = unique.filter((F.col("_same_version_rank") > 1) & F.col("_is_new_delivery")) \
        .withColumn("disposition", F.lit("DUPLICATE_REPLAY"))
    duplicate_count = duplicate_rows.filter(F.col("batch_id") == batch_id).count() if batch_id else 0
    if duplicate_rows.take(1):
        duplicate_rows.write.mode("append").parquet(f"{audit_root.rstrip('/')}/record_dispositions/{dataset}/attempt_id={attempt_id}")
    distinct_versions = unique.filter(F.col("_same_version_rank") == 1).drop("_same_version_rank", "_version_payload_count")
    current_window = Window.partitionBy(*keys).orderBy(F.col("write_timestamp").desc(), F.col("_payload_hash").asc())
    ranked = distinct_versions.withColumn("_current_rank", F.row_number().over(current_window))
    superseded_rows = ranked.filter((F.col("_current_rank") > 1) & F.col("_is_new_delivery")) \
        .withColumn("disposition", F.lit("SUPERSEDED"))
    superseded_count = superseded_rows.filter(F.col("batch_id") == batch_id).count() if batch_id else 0
    if superseded_rows.take(1):
        superseded_rows.write.mode("append").parquet(f"{audit_root.rstrip('/')}/record_dispositions/{dataset}/attempt_id={attempt_id}")
    version_history = distinct_versions.drop("_payload_hash", "_is_new_delivery", "_current_rank")
    return version_history, conflict_count, duplicate_count, superseded_count


def current_state(frame: DataFrame, keys: list[str]) -> DataFrame:
    window = Window.partitionBy(*keys).orderBy(F.col("write_timestamp").desc())
    return frame.withColumn("_rank", F.row_number().over(window)).filter(F.col("_rank") == 1).drop("_rank")


def customer_scd2(customer_versions: DataFrame, tracked: list[str]) -> DataFrame:
    source = customer_versions.withColumn("_attribute_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in tracked])), 256))
    by_time = Window.partitionBy("customer_id").orderBy("write_timestamp")
    source = source.withColumn("_prior_hash", F.lag("_attribute_hash").over(by_time))
    changed = source.filter(F.col("_prior_hash").isNull() | (F.col("_attribute_hash") != F.col("_prior_hash")))
    changed = changed.withColumn("effective_from", F.col("write_timestamp"))
    effective_window = Window.partitionBy("customer_id").orderBy("effective_from")
    changed = changed.withColumn("effective_to", F.lead("effective_from").over(effective_window)) \
        .withColumn("current_flag", F.col("effective_to").isNull()) \
        .withColumn("customer_sk", F.xxhash64("customer_id", "effective_from"))
    return changed.drop("_attribute_hash", "_prior_hash")


def point_in_time_orders(orders: DataFrame, customer_history: DataFrame) -> DataFrame:
    o = orders.alias("o")
    c = customer_history.alias("c")
    in_range = (F.col("o.customer_id") == F.col("c.customer_id")) & \
        (F.col("o.order_timestamp") >= F.col("c.effective_from")) & \
        (F.col("c.effective_to").isNull() | (F.col("o.order_timestamp") < F.col("c.effective_to")))
    return o.join(c, in_range, "left").select(
        F.col("o.order_id"), F.col("o.order_name"), F.col("o.customer_id"), F.col("o.order_timestamp"),
        F.col("o.state"), F.col("o.amount_untaxed"), F.col("o.amount_total"), F.col("o.currency_id"),
        F.col("c.customer_sk"), F.col("c.customer_name").alias("customer_name_at_order"),
        F.col("c.city").alias("customer_city_at_order"), F.col("c.category_ids_raw").alias("customer_categories_at_order"),
        F.col("c.effective_from").alias("customer_effective_from"), F.col("c.effective_to").alias("customer_effective_to"),
        F.when(F.col("c.customer_sk").isNull(), "NO_VERSION_AT_ORDER_TIME").otherwise("MATCHED").alias("customer_version_match_status"),
        F.col("o.write_timestamp"), F.col("o.source_reference"), F.col("o.source_system"), F.col("o.source_dataset"),
        F.col("o.batch_id"), F.col("o.ingestion_timestamp"), F.col("o.raw_payload"),
    )


def publish_snapshots(spark: SparkSession, frames: dict[str, DataFrame], silver_root: str, attempt_id: str) -> None:
    """Write every snapshot to a staging tree, then replace each published directory."""
    fs_conf = spark._jsc.hadoopConfiguration()
    root = spark._jvm.org.apache.hadoop.fs.Path(silver_root)
    fs = root.getFileSystem(fs_conf)
    staged: list[tuple[str, str]] = []
    staging_root = f"{silver_root.rstrip('/')}/_staging/attempt_id={attempt_id}"
    for name, frame in frames.items():
        relative = name
        stage = f"{staging_root}/{relative}"
        target = f"{silver_root.rstrip('/')}/{relative}"
        frame.write.mode("errorifexists").parquet(stage)
        staged.append((stage, target))
    backup_root = f"{staging_root}/_backup"
    committed: list[tuple[object, object, bool]] = []
    try:
        for stage, target in staged:
            target_path = spark._jvm.org.apache.hadoop.fs.Path(target)
            stage_path = spark._jvm.org.apache.hadoop.fs.Path(stage)
            backup_path = spark._jvm.org.apache.hadoop.fs.Path(f"{backup_root}/{target.rsplit('/', 1)[-1]}")
            had_target = fs.exists(target_path)
            fs.mkdirs(target_path.getParent())
            if had_target:
                fs.mkdirs(backup_path.getParent())
                if not fs.rename(target_path, backup_path):
                    raise IOError(f"Could not move existing Silver output to backup: {target}")
            if not fs.rename(stage_path, target_path):
                if had_target and fs.exists(backup_path):
                    fs.rename(backup_path, target_path)
                raise IOError(f"Could not publish staged Silver output: {target}")
            committed.append((target_path, backup_path, had_target))
    except Exception:
        for target_path, backup_path, had_target in reversed(committed):
            if fs.exists(target_path):
                fs.delete(target_path, True)
            if had_target and fs.exists(backup_path):
                fs.rename(backup_path, target_path)
        raise
    if fs.exists(spark._jvm.org.apache.hadoop.fs.Path(staging_root)):
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(staging_root), True)


def ledger_schema():
    return StructType([
        StructField("record_type", StringType(), False), StructField("batch_id", StringType(), True),
        StructField("dataset", StringType(), True), StructField("status", StringType(), True),
        StructField("attempt_id", StringType(), False), StructField("started_at_utc", TimestampType(), False),
        StructField("completed_at_utc", TimestampType(), True), StructField("source_boundary", StringType(), True),
        StructField("records_read", LongType(), True), StructField("accepted_records", LongType(), True),
        StructField("rejected_records", LongType(), True), StructField("duplicate_records", LongType(), True),
        StructField("superseded_records", LongType(), True), StructField("max_source_write_timestamp", TimestampType(), True),
    ])


def _run_silver_impl(
    spark: SparkSession,
    hdfs_root: str,
    batch_filter: str | None = None,
    reprocess: bool = False,
) -> None:
    root = hdfs_root.rstrip("/")
    bronze_root, silver_root = f"{root}/bronze", f"{root}/silver"
    quarantine_root, audit_root = f"{root}/quarantine", f"{root}/audit"
    successful = existing_successful_batches(spark, audit_root)
    directories = {name: batch_directories(spark, f"{bronze_root}/{name}") for name in SOURCES}
    batches_by_dataset = {
        name: {path.rsplit("batch_id=", 1)[1].rstrip("/") for path in paths}
        for name, paths in directories.items()
    }
    all_batches = sorted(set.union(*batches_by_dataset.values()) if batches_by_dataset else set())
    if successful:
        selected = [batch_filter] if batch_filter else all_batches
    else:
        # Do not checkpoint an incomplete bootstrap batch before its parent datasets arrive.
        bootstrap_batches = sorted(set.intersection(*batches_by_dataset.values())) if batches_by_dataset else []
        if not bootstrap_batches:
            raise FileNotFoundError("Initial Silver load needs one batch containing all five core Bronze datasets")
        selected = [batch_filter] if batch_filter else bootstrap_batches
        if batch_filter and batch_filter not in bootstrap_batches:
            raise FileNotFoundError(f"Initial batch {batch_filter} is incomplete; land all five core datasets first")
    pending = [batch for batch in selected if batch and (reprocess or batch not in successful)]
    if not pending:
        print("Silver is already current; no unprocessed Bronze batches found")
        return
    attempt_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc).replace(tzinfo=None)
    rules = load_rules()
    spark.conf.set("spark.sql.session.timeZone", rules["timezone"])
    histories: dict[str, DataFrame | None] = {name: load_history(spark, silver_root, name) for name in SOURCES}
    all_stats: dict[tuple[str, str], dict[str, int]] = {}
    max_versions: dict[tuple[str, str], object] = {}

    for batch_id in pending:
        print(f"Processing Silver source boundary {batch_id}")
        prepared: dict[str, DataFrame | None] = {}
        for dataset in SOURCES:
            raw = read_batch(spark, bronze_root, dataset, batch_id)
            if raw is None:
                prepared[dataset] = None
                continue
            raw_count = raw.count()
            stats = all_stats.setdefault((batch_id, dataset), {"records_read": raw_count, "rejected": 0, "duplicates": 0, "superseded": 0})
            typed = prepare(raw, dataset)
            typed, rejected = validate_and_quarantine(typed, dataset, attempt_id, quarantine_root, rules)
            stats["rejected"] += rejected
            prepared[dataset] = typed

        # Resolve references using all known + just-landed master keys before accepting the dependent record.
        known_customer = valid_parent_ids(spark, silver_root, prepared["customers"], "customers", "customer_id")
        known_category = valid_parent_ids(spark, silver_root, prepared["product_categories"], "product_categories", "category_id")

        def ref_check(dataset: str, frame: DataFrame | None, column: str, ids: DataFrame | None):
            if frame is None:
                return None
            # Parent history may have advanced in an earlier batch in this same run.
            parent_entity, parent_column = {
                "customer_id": ("customers", "customer_id"),
                "category_id": ("product_categories", "category_id"),
                "parent_category_id": ("product_categories", "category_id"),
                "product_id": ("products", "product_id"),
                "order_id": ("sales_orders", "order_id"),
            }.get(column, (None, None))
            current_ids = histories.get(parent_entity) if parent_entity else None
            if current_ids is not None:
                prior_ids = current_ids.select(F.col(parent_column).alias(column)).distinct()
                ids = prior_ids if ids is None else ids.unionByName(prior_ids).distinct()
            if dataset == "product_categories" and frame is not None:
                optional_parent = frame.filter(F.col(column).isNull())
                non_null_parent = frame.filter(F.col(column).isNotNull())
                checked, rejected = filter_reference(non_null_parent, dataset, column, ids, attempt_id, quarantine_root)
                all_stats[(batch_id, dataset)]["rejected"] += rejected
                return checked.unionByName(optional_parent)
            checked, rejected = filter_reference(frame, dataset, column, ids, attempt_id, quarantine_root)
            all_stats[(batch_id, dataset)]["rejected"] += rejected
            return checked

        prepared["product_categories"] = ref_check("product_categories", prepared["product_categories"], "parent_category_id", known_category) \
            if prepared["product_categories"] is not None else None
        accepted_category_ids = valid_parent_ids(spark, silver_root, prepared["product_categories"], "product_categories", "category_id")
        prepared["products"] = ref_check("products", prepared["products"], "category_id", accepted_category_ids) \
            if prepared["products"] is not None else None
        prepared["sales_orders"] = ref_check("sales_orders", prepared["sales_orders"], "customer_id", known_customer) \
            if prepared["sales_orders"] is not None else None
        # Use both existing product/order state and rows landed in this batch.
        product_ids = valid_parent_ids(spark, silver_root, prepared["products"], "products", "product_id")
        order_ids = valid_parent_ids(spark, silver_root, prepared["sales_orders"], "sales_orders", "order_id")
        prepared["sales_order_lines"] = ref_check("sales_order_lines", prepared["sales_order_lines"], "product_id", product_ids) \
            if prepared["sales_order_lines"] is not None else None
        prepared["sales_order_lines"] = ref_check("sales_order_lines", prepared["sales_order_lines"], "order_id", order_ids) \
            if prepared["sales_order_lines"] is not None else None

        for dataset in SOURCES:
            incoming = prepared[dataset]
            old = histories[dataset]
            if incoming is None:
                continue
            keys = ENTITY_KEYS[dataset]
            payload_fields = [c for c in incoming.columns if c not in LINEAGE]
            marked_incoming = incoming.withColumn("_is_new_delivery", F.lit(True))
            combined = old.withColumn("_is_new_delivery", F.lit(False)).unionByName(marked_incoming) if old is not None else marked_incoming
            combined = combined.withColumn("_payload_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in payload_fields])), 256))
            same_version = Window.partitionBy(*keys, "write_timestamp")
            combined = combined.withColumn("_version_payload_count", F.size(F.collect_set("_payload_hash").over(same_version)))
            conflicts = combined.filter(F.col("_version_payload_count") > 1).withColumn("_error_code", F.lit("SAME_KEY_VERSION_CONFLICT"))
            new_conflicts = conflicts.filter(F.col("_is_new_delivery") & (F.col("batch_id") == batch_id))
            quarantine(conflicts, dataset + "_version_conflicts", attempt_id, quarantine_root)
            rejected_conflicts = new_conflicts.count()
            all_stats[(batch_id, dataset)]["rejected"] += rejected_conflicts
            unique = combined.filter(F.col("_version_payload_count") == 1)
            tie = Window.partitionBy(*keys, "write_timestamp").orderBy(
                F.col("_is_new_delivery").asc(), F.col("source_reference").asc(), F.col("_payload_hash").asc()
            )
            unique = unique.withColumn("_same_version_rank", F.row_number().over(tie))
            duplicate = unique.filter((F.col("_same_version_rank") > 1) & F.col("_is_new_delivery") & (F.col("batch_id") == batch_id))
            duplicate_count = duplicate.count()
            all_stats[(batch_id, dataset)]["duplicates"] += duplicate_count
            if duplicate_count:
                duplicate.withColumn("disposition", F.lit("DUPLICATE_REPLAY")) \
                    .write.mode("append").parquet(f"{audit_root}/record_dispositions/{dataset}/attempt_id={attempt_id}")
            distinct = unique.filter(F.col("_same_version_rank") == 1).drop("_same_version_rank", "_version_payload_count")
            current_window = Window.partitionBy(*keys).orderBy(F.col("write_timestamp").desc())
            ranked = distinct.withColumn("_current_rank", F.row_number().over(current_window))
            superseded = ranked.filter((F.col("_current_rank") > 1) & F.col("_is_new_delivery") & (F.col("batch_id") == batch_id))
            superseded_count = superseded.count()
            all_stats[(batch_id, dataset)]["superseded"] += superseded_count
            if superseded_count:
                superseded.withColumn("disposition", F.lit("SUPERSEDED")) \
                    .write.mode("append").parquet(f"{audit_root}/record_dispositions/{dataset}/attempt_id={attempt_id}")
            histories[dataset] = distinct.drop("_payload_hash", "_is_new_delivery", "_current_rank")
            max_ts = histories[dataset].agg(F.max("write_timestamp").alias("max_ts")).first()["max_ts"]
            max_versions[(batch_id, dataset)] = max_ts

    # Build published current-state tables from complete accepted version histories.
    outputs: dict[str, DataFrame] = {}
    for dataset, versions in histories.items():
        if versions is None:
            raise RuntimeError(f"No accepted Silver version history exists for {dataset}")
        current = current_state(versions, ENTITY_KEYS[dataset])
        if dataset == "customers":
            outputs["silver_customer_history"] = customer_scd2(versions, rules["customer_tracked_attributes"])
            outputs["silver_customer_current"] = current
        elif dataset == "product_categories":
            outputs["silver_product_category"] = current
        elif dataset == "products":
            outputs["silver_product"] = current
        elif dataset == "sales_orders":
            outputs["silver_sales_order"] = current
        elif dataset == "sales_order_lines":
            factor = F.lit(1).cast(DecimalType(10, 6)) - F.col("discount_percent") / F.lit(100).cast(DecimalType(10, 6))
            outputs["silver_sales_order_line"] = current.withColumn(
                "line_net_value", F.round(F.col("quantity") * F.col("unit_price") * factor, int(rules["money_scale"]))
                .cast(DecimalType(int(rules["money_precision"]), int(rules["money_scale"])))
            )
        outputs[f"record_history/{dataset}"] = versions

    if "silver_sales_order" in outputs and "silver_customer_history" in outputs:
        outputs["silver_sales_order_customer"] = point_in_time_orders(outputs["silver_sales_order"], outputs["silver_customer_history"])

    publish_snapshots(spark, outputs, silver_root, attempt_id)

    ended = datetime.now(timezone.utc).replace(tzinfo=None)
    ledger_rows = []
    hwm_rows = []
    for batch_id in pending:
        boundary_values = [max_versions.get((batch_id, dataset)) for dataset in SOURCES]
        boundary = max((v for v in boundary_values if v is not None), default=None)
        for dataset in SOURCES:
            stats = all_stats.get((batch_id, dataset))
            if stats is None:
                continue
            accepted = max(0, stats["records_read"] - stats["rejected"] - stats["duplicates"] - stats["superseded"])
            ledger_rows.append((
                "DATASET", batch_id, dataset, "SUCCESS", attempt_id, started, ended,
                None if boundary is None else boundary.isoformat(), stats["records_read"], accepted,
                stats["rejected"], stats["duplicates"], stats["superseded"], max_versions.get((batch_id, dataset)),
            ))
            hwm_rows.append((dataset, batch_id, max_versions.get((batch_id, dataset)), ended, attempt_id))
        ledger_rows.append(("BATCH", batch_id, None, "SUCCESS", attempt_id, started, ended,
            batch_id, None, None, None, None, None, boundary))
    if hwm_rows:
        hwm_schema = StructType([
            StructField("dataset", StringType(), False), StructField("batch_id", StringType(), False),
            StructField("high_watermark", TimestampType(), True), StructField("committed_at_utc", TimestampType(), False),
            StructField("attempt_id", StringType(), False),
        ])
        spark.createDataFrame(hwm_rows, hwm_schema).write.mode("append").parquet(f"{audit_root}/source_high_watermarks")
    # The success rows are the commit marker and are deliberately written after data and high-water marks.
    ledger = spark.createDataFrame(ledger_rows, ledger_schema())
    ledger.write.mode("append").parquet(f"{audit_root}/batch_ledger")
    print(f"Silver published {len(outputs)} snapshots for {len(pending)} batch(es). Attempt: {attempt_id}")


def run_silver(spark: SparkSession, hdfs_root: str, batch_filter: str | None = None, reprocess: bool = False) -> None:
    try:
        _run_silver_impl(spark, hdfs_root, batch_filter, reprocess)
    except Exception:
        root = hdfs_root.rstrip("/")
        audit_root = f"{root}/audit"
        try:
            successful = existing_successful_batches(spark, audit_root)
            directories = [batch_directories(spark, f"{root}/bronze/{dataset}") for dataset in SOURCES]
            available = sorted({p.rsplit("batch_id=", 1)[1].rstrip("/") for group in directories for p in group})
            failed = [batch_filter] if batch_filter else available
            failed = [batch for batch in failed if batch and (reprocess or batch not in successful)]
            if failed:
                attempt_id = str(uuid.uuid4())
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                rows = [("BATCH", batch, None, "FAILED", attempt_id, now, now, batch, None, None, None, None, None, None) for batch in failed]
                spark.createDataFrame(rows, ledger_schema()).write.mode("append").parquet(f"{audit_root}/batch_ledger")
        except Exception as audit_error:
            print(f"Could not append failed-attempt ledger record: {audit_error}")
        raise


def main() -> None:
    import argparse
    from bronze_common import env, spark_session

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", help="Limit processing to one landed batch")
    parser.add_argument("--reprocess", action="store_true", help="Replay a previously successful batch")
    args = parser.parse_args()
    hdfs_root = env("HDFS_UPLOAD_PATH", "hdfs:///user/your_username/capstone")
    spark = spark_session("trusted-odoo-sales-risk-silver")
    try:
        run_silver(spark, hdfs_root, args.batch_id or env("BATCH_ID", "") or None, args.reprocess)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
