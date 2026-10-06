# Architecture and source mapping

```text
batch folders (CSV only)
  ├─ api-*/csv/sales/orders              ─┐
  ├─ api-*/csv/sales/order-lines          ├─> Bronze (raw strings + lineage, Parquet)
  ├─ api-*/csv/products/products          ┤       └─> Silver typed/current + quarantine
  ├─ api-*/csv/products/categories        ┤
  └─ contacts-*/csv/contact-details/customers ┘
```

| Silver dataset | Business key | Source version | Required references |
|---|---|---|---|
| `silver_customer_current` | `customer_id` | `write_date` | — |
| `silver_product` | `product_id` | `write_date` | category |
| `silver_product_category` | `category_id` | `write_date` | parent category (nullable) |
| `silver_sales_order` | `order_id` | `write_date` | customer |
| `silver_sales_order_line` | (`order_id`, `line_id`) | `write_date` | order, product |

The CSV `partner_id`, `order_id`, `product_id`, and `categ_id` fields may be serialized Odoo many-to-one values. The parser extracts the numeric identity while Bronze retains the untouched source value. The `contacts-004` customer export provides `city`, `name`, `category_id`, and timestamps. It has no segment attribute. Contact release is referenced by customer ID from business orders.

Source-level fields are retained where useful; standardized keys and types are added in Silver. Raw evidence stays in Bronze. Invalid records are written to `data/quarantine/<dataset>` with raw fields, batch, file reference, error code, and description.
