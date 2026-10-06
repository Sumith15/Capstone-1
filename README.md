# Trusted Odoo Sales Risk Analytics

CSV-only implementation through Phase 2 (Bronze + trusted Silver). Runs in WSL with the Spark installation already provided there. No Excel, XML, JSON, Kafka, Airflow, Snowflake, or Gold processing is included yet.

## Data layout

The reader discovers complete `batch-*` folders below `LANDING_ROOT` and scans the CSV source contracts in `api-*/csv` and `contacts-*/csv`. It supports the supplied hierarchy, for example:

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
# Edit LANDING_ROOT / DATA_ROOT in .env if your mount differs.
set -a; source .env; set +a
spark-submit src/pipeline.py --stage all
```

Use the Spark/Python installation already configured in WSL. `requirements.txt` records the supported PySpark range for a fresh environment; it is not necessary to reinstall Spark when your existing runtime satisfies it.

Defaults point to `/mnt/d/DATA_ENG_TRAINING/capstone/Original/landing-zone-Oct-10-5PM` and `/mnt/d/DATA_ENG_TRAINING/capstone/project/data`. Use `--stage bronze` or `--stage silver` to run a layer separately. `--batch-id batch-1` limits the run to one delivery batch. Bronze stores raw CSV strings and source metadata in Parquet; Silver stores typed Parquet datasets and actionable quarantine records. Set `SPARK_MASTER=local[*]` by default; use the cluster URL supplied by your WSL setup if needed.

## Outputs

```
data/bronze/<dataset>/batch_id=<batch-id>/part-*.parquet
data/silver/<dataset>/part-*.parquet
data/quarantine/<dataset>/part-*.parquet
data/audit/batch_ledger/part-*.parquet
```

Re-running Bronze for a completed batch is a no-op. Silver is deterministically rebuilt from the preserved Bronze batches, making replay safe. Configure a new unique `BATCH_ID` for new source deliveries; a `batch-*` folder is used as the default delivery boundary.

## Current scope / assumptions

- Core datasets: customers, sales orders, sales order lines, products, product categories.
- Source version ordering uses `write_date`; missing/unparseable ordering is quarantined. Ingestion time is lineage only.
- CSV headers and Odoo many-to-one list values follow the supplied manifests. Odoo `False` is null; IDs embedded as `[id, "label"]` are parsed explicitly.
- Customer history is SCD2 on the supplied customer fields (name, city, category references, active); `effective_to` is exclusive. Product/category are current-state SCD1.
- The customer export contains no `segment`; segment remains an explicit future source-contract placeholder.
- This first implementation uses local/cluster-compatible Parquet paths. HDFS can be configured by setting `DATA_ROOT=hdfs:///...`.

See [docs/architecture.md](docs/architecture.md) and [docs/phase-2.md](docs/phase-2.md).
"# Capstone-1" 
