# Seed Data Notes

The three CSVs in [`data/`](../data/) are committed so the pipeline can be run
end to end from a fresh clone without a Databricks connection.

## Regenerating

```bash
databricks/.venv/bin/python \
  databricks/jobs/data_generation/src/generate_sample_data.py \
  --output-dir data --batch-id seed
for e in customers products orders; do mv "data/${e}_seed.csv" "data/${e}.csv"; done
```

The generator seeds both `Faker` and `random` with 42, so the output is
reproducible run to run. Two fields are deliberately relative to the current
date — `future_signup_date` and `future_order_date` exist to violate a
`max_date: today` rule, so they shift if you regenerate on a different day.

## Row counts

| File | Rows | Composition |
|------|------|-------------|
| `customers.csv` | 10,015 | 10,000 base + 10 duplicate-key rows + 5 appended NULL-key rows |
| `products.csv` | 508 | 500 base + 5 duplicate-key rows + 3 appended NULL-key rows |
| `orders.csv` | 100,025 | 100,000 base + 20 duplicate-key rows + 5 appended NULL-key rows |

## Intentional quality issues

The counts the assessment names explicitly, verified against the committed files:

| Issue | Rows |
|-------|------|
| NULL email | 50 |
| duplicate `customer_id` | 10 |
| NULL `orders.customer_id` | 100 |
| NULL `orders.product_id` | 200 |
| `customer_id` not in customers | 50 |
| `product_id` not in products | 30 |
| duplicate `order_id` | 20 |

Beyond those, the generator produces one scenario per validation rule declared
in the `dq_schema` seed, so a single end-to-end run exercises the whole
validator surface: email format, enum membership, string length bounds, a
country pattern, numeric minimum / maximum / exclusive-minimum, and a
`min_date` / `max_date` window. `jobs/silver/tests/test_dq_coverage.py` fails
if a declared rule has no scenario here, or if a scenario names a rule nobody
declares.

## Why NULL-key rows are appended, not overwritten

A NULL primary key is injected by **appending** a row, never by nulling the key
on an existing one. Nulling a parent key in place removes it from the parent
table, which silently orphans every child that referenced it: with 100,000
orders over 500 products each product has roughly 200 children, so three nulled
products cascaded into 562 extra orphan orders against a spec of 30. An
appended row has no children by construction, so the completeness check gets
its data without disturbing referential integrity.
