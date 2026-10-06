# Phase 2 rules and outputs

- Timestamps are parsed as UTC. Source `write_date` controls version resolution; source filename/batch and ingestion timestamp never select current state.
- Identical deliveries are counted as duplicates; highest valid source version wins per business key. Same key/version with differing content is quarantined as a conflict.
- Quantity must be positive; unit price non-negative; discount in `[0,100]`; keys and parseable source timestamps are required. Decimal money is rounded per line to two decimals before aggregation.
- Order states remain in Silver; confirmed sales are defined later as `sale` / `done` only when Gold is implemented.
- Invalid mandatory fields and unresolved customer/product/order references are quarantined, not dropped.
- Customer SCD2 emits `[effective_from, effective_to)` intervals; `current_flag` marks the open interval. City/name/category changes produce a new version. Product and category descriptions are SCD1.
- `line_net_value = round(quantity * unit_price * (1 - discount / 100), 2)` using Spark `DecimalType`.
- Batch ledger records rows seen, accepted, rejected, duplicate, and superseded counts for each dataset and delivery boundary.

## Deliberate placeholders

1. Customer segment is not in the supplied `contacts-004` CSV contract. Add it only if a future source manifest explicitly provides it.
2. For SCD2, this implementation uses customer `write_date` as the source-effective timestamp because no separate business-effective timestamp exists in the feed. Confirm this assumption if the source owner defines another field.
3. New incremental pack naming and retention policy should be confirmed before the first production-scale change pack.
