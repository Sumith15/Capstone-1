# Phase 2 rules and outputs

- Timestamps are parsed as UTC. Source `write_date` controls version resolution; source filename/batch and ingestion timestamp never select current state.
- Identical deliveries are counted as duplicates; highest valid source version wins per business key. Same key/version with differing content is quarantined as a conflict.
- Quantity must be positive; unit price non-negative; discount in `[0,100]`; keys and parseable source timestamps are required. Decimal money is rounded per line to two decimals before aggregation.
- Order states remain in Silver; confirmed sales are defined later as `sale` / `done` only when Gold is implemented.
- Invalid mandatory fields and unresolved customer/product/order references are quarantined, not dropped.
- Customer SCD2 emits `[effective_from, effective_to)` intervals; `current_flag` marks the open interval. Each observed customer version begins at source `write_date`; city/name/category changes produce a new version. Product and category descriptions are SCD1.
- `line_net_value = round(quantity * unit_price * (1 - discount / 100), 2)` using Spark `DecimalType`.
- Batch ledger records rows seen, accepted, rejected, duplicate, and superseded counts for each dataset and delivery boundary. Success is recorded only after Silver snapshots publish; failed attempts are recorded as `FAILED` and remain eligible for retry.
- The successful batch ledger is the replay checkpoint. Source high-water marks are written only after publication and track the maximum accepted `write_date` by dataset. Batch IDs, rather than timestamps alone, preserve equal-timestamp updates across deliveries.
- Outputs share `HDFS_UPLOAD_PATH`: Bronze under `/bronze`, Silver under `/silver`, and quarantine/audit evidence under `/quarantine` and `/audit`.
- `silver_sales_order_customer` performs a point-in-time join using `[effective_from, effective_to)` customer intervals. Orders with no historical match remain present and are flagged.

## Deliberate placeholders

1. Customer segment is not in the supplied `contacts-004` CSV contract. Add it only if a future source manifest explicitly provides it.
2. For SCD2, this implementation uses customer `write_date` as the source-effective timestamp because no separate business-effective timestamp exists in the feed. Confirm this assumption if the source owner defines another field.
3. Silver uses source order currency as provided. The supplied CSV shows USD (`currency_id` 1); no INR conversion is applied because no exchange-rate policy is specified.
4. A full source snapshot cannot reconstruct customer attributes before its earliest observed customer `write_date`. Such historical orders remain in `silver_sales_order_customer` with `NO_VERSION_AT_ORDER_TIME` rather than being assigned a made-up version.
