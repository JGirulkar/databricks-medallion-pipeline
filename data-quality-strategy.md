# Data Quality Strategy

## Checks

1. **Completeness** — email, customer_id, product_id non-null in critical contexts
2. **Uniqueness** — order_id, customer_id
3. **Referential integrity** — customer_id, product_id FKs
4. **Type / business logic** — valid types and business rules

## Intentional issues (~700 rows)

See `project-overview.mdc` and sample data generator notes.

## Reporting

Quality metrics report: % passed per check in Silver layer.
