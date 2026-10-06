# Trusted Odoo Sales Risk Analytics

CSV-only implementation through Phase 2 (Bronze + trusted Silver). Runs in WSL with the Spark installation already provided there. No Excel, XML, JSON, Kafka, Airflow, Snowflake, or Gold processing is included yet.

## Data layout

Bronze reads only the five core CSV datasets below `LANDING_ROOT`. Other tables and every non-CSV format in a landing batch are ignored. Each table has its own executable script, so datasets can arrive or be ingested independently:

```
batch-1/api-001/csv/sales/orders/*.csv
batch-1/api-001/csv/sales/order-lines/*.csv
batch-1/api-001/csv/products/products/*.csv
batch-1/api-001/csv/products/categories/*.csv
batch-1/contacts-004/csv/contact-details/customers/*.csv
```

Customer city is present in the actual contact contract. No customer `segment` is present, so no segment is fabricated.

## Configure and run (WSL)

```bash
cd /mnt/d/DATA_ENG_TRAINING/capstone/project
cp .env.example .env
# Set HDFS_UPLOAD_PATH to the shared project root; jobs create /bronze, /silver, /quarantine, and /audit below it.
# Replace `your_username` in the example with your HDFS user.
set -a; source .env; set +a
spark-submit src/bronze_sales_orders.py
spark-submit src/bronze_sales_order_lines.py
spark-submit src/bronze_customers.py
spark-submit src/bronze_products.py
spark-submit src/bronze_product_categories.py
spark-submit src/pipeline.py --stage silver
```

Use the Spark/Python installation already configured in WSL. `requirements.txt` records the supported PySpark range for a fresh environment; it is not necessary to reinstall Spark when your existing runtime satisfies it.

Defaults point to `/mnt/d/DATA_ENG_TRAINING/capstone/Original/landing-zone-Oct-10-5PM`. `HDFS_UPLOAD_PATH` is the shared root, for example `hdfs:///user/alice/capstone`; the outputs are written below `/bronze`, `/silver`, `/quarantine`, and `/audit`. Each standalone Bronze script accepts `--batch-id batch-2` to ingest one arriving batch. Run `spark-submit src/pipeline.py --stage all` to ingest all available core CSV datasets and process unprocessed batches. Silver skips successful batches using its ledger; use `--reprocess` to replay a completed batch. A Silver run requires all five core datasets for the initial load; later batches may contain only changed datasets. Set `SPARK_MASTER=local[*]` by default; use the cluster URL supplied by your WSL setup if needed.

## Outputs

```
<HDFS_UPLOAD_PATH>/bronze/<dataset>/batch_id=<batch-id>/part-*.parquet
<HDFS_UPLOAD_PATH>/silver/silver_<dataset>/part-*.parquet
<HDFS_UPLOAD_PATH>/silver/record_history/<dataset>/part-*.parquet
<HDFS_UPLOAD_PATH>/quarantine/<dataset>/attempt_id=<attempt-id>/part-*.parquet
<HDFS_UPLOAD_PATH>/audit/batch_ledger/part-*.parquet
<HDFS_UPLOAD_PATH>/audit/source_high_watermarks/part-*.parquet
```

Re-running Bronze for a completed batch is a no-op. Each job reads only its mapped CSV folder and writes raw strings plus source metadata. Silver processes only batches absent from the successful batch ledger, unions the newly landed records with its persisted source-version history, and publishes deterministic snapshots. A `batch-*` folder is the delivery boundary; pass `--batch-id` to process only one batch.

## Current scope / assumptions

- Core datasets: customers, sales orders, sales order lines, products, product categories.
- Source version ordering uses `write_date`; missing/unparseable ordering is quarantined. Ingestion time is lineage only. Batch IDs are the replay-safe extraction boundaries, so equal source timestamps in later batches are still processed.
- CSV headers and Odoo many-to-one list values follow the supplied manifests. Odoo `False` is null; IDs embedded as `[id, "label"]` are parsed explicitly.
- Customer history is SCD2 on the supplied customer fields (name, city, category references, active); `effective_to` is exclusive. Consecutive unchanged customer attributes do not create a history version. Product and category are current-state SCD1.
- `silver_sales_order_customer` resolves each order against the customer SCD2 interval at the order timestamp. Orders without a known interval are retained with `NO_VERSION_AT_ORDER_TIME` for inspection.
- The customer export contains no `segment`; segment remains an explicit future source-contract placeholder.
- Amounts and line net values retain the source order currency (`currency_id`); Silver does not convert currencies. The sample contract reports USD, despite the earlier blueprint's INR assumption.
- `HDFS_UPLOAD_PATH` is the common HDFS project root for Bronze, Silver, quarantine, and audit.

See [docs/architecture.md](docs/architecture.md) and [docs/phase-2.md](docs/phase-2.md).
"# Capstone-1" 
