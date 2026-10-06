# Architecture and source mapping

```text
batch folders (CSV only)
  ├─ api-*/csv/sales/orders                 ─> bronze_sales_orders.py ─┐
  ├─ api-*/csv/sales/order-lines            ─> bronze_sales_order_lines.py ┤
  ├─ api-*/csv/products/products            ─> bronze_products.py            ├─> HDFS_UPLOAD_PATH/bronze/<table>/batch_id=<batch>/
  ├─ api-*/csv/products/categories          ─> bronze_product_categories.py ┤        └─> Silver typed/current + quarantine
  └─ contacts-*/csv/contact-details/customers ─> bronze_customers.py          ┘
```

Each Bronze script reads only its mapped CSV directory; unrelated tables and non-CSV formats are ignored. The shared helper provides discovery, lineage columns, and idempotent batch writes. `HDFS_UPLOAD_PATH` is the shared HDFS root; Bronze, Silver, quarantine, and audit are children of that root.

| Silver dataset | Business key | Source version | Required references |
|---|---|---|---|
| `silver_customer_current` | `customer_id` | `write_date` | — |
| `silver_product` | `product_id` | `write_date` | category |
| `silver_product_category` | `category_id` | `write_date` | parent category (nullable) |
| `silver_sales_order` | `order_id` | `write_date` | customer |
| `silver_sales_order_line` | (`order_id`, `line_id`) | `write_date` | order, product |

The CSV `partner_id`, `order_id`, `product_id`, and `categ_id` fields may be serialized Odoo many-to-one values. The parser extracts the numeric identity while Bronze retains the untouched source value. The `contacts-004` customer export provides `city`, `name`, `category_id`, and timestamps. It has no segment attribute. Contact release is referenced by customer ID from business orders.

Source-level fields are retained where useful; standardized keys and types are added in Silver. Raw evidence stays in Bronze and a compact `raw_payload` reference is carried into Silver/quarantine. Invalid records are written to `<HDFS_UPLOAD_PATH>/quarantine/<dataset>/attempt_id=...` with batch, file reference, error code, and description. Silver uses its batch ledger to read only new Bronze batch folders; version history is merged with existing Silver state before current-state snapshots publish. Customer versions use source `write_date` as their effective start and join orders by effective interval.
