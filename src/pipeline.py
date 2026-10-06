#!/usr/bin/env python3
"""Orchestrate CSV-only Bronze ingestion and incremental Silver publication."""

from __future__ import annotations

import argparse
from pathlib import Path

from bronze_common import SOURCES, discover_files, env, land_dataset, spark_session
from silver_pipeline import run_silver


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", choices=("bronze", "silver", "all"), default="all")
    parser.add_argument("--batch-id", help="Limit this run to one landing batch")
    parser.add_argument("--reprocess", action="store_true", help="Replay a previously successful Silver batch")
    args = parser.parse_args()

    landing_root = Path(env("LANDING_ROOT", "/mnt/d/DATA_ENG_TRAINING/capstone/Original/landing-zone-Oct-10-5PM"))
    hdfs_root = env("HDFS_UPLOAD_PATH", "hdfs:///user/your_username/capstone")
    batch_id = args.batch_id or env("BATCH_ID", "") or None
    spark = spark_session("trusted-odoo-sales-risk-phase-1-2")
    try:
        if args.stage in ("bronze", "all"):
            discovered = discover_files(landing_root, batch_id)
            available = [name for name in SOURCES if discovered[name]]
            if not available:
                raise FileNotFoundError(f"No supported CSV files found under {landing_root}")
            for dataset in available:
                land_dataset(spark, dataset, landing_root, hdfs_root, batch_id)
        if args.stage in ("silver", "all"):
            run_silver(spark, hdfs_root, batch_id, args.reprocess)
        print("Pipeline completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
