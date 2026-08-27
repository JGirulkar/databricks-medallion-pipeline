# Data Generation Notes

How the sample data is generated, why the quality issues exist, and what each
one is designed to prove downstream.

## How it works

One script, [`src/generate_sample_data.py`](src/generate_sample_data.py), two
modes:

| Mode | What it emits | Purpose |
|---|---|---|
| `seed` (default) | 10,015 customers · 508 products · 100,025 orders, carrying all 725 intentional quality issues | first delivery — everything silver's checks must catch |
| `delta` | 500 **new** orders · full customer snapshot with 20 changed rows · full product snapshot with 3 rows removed · 15 late-arriving parents | second delivery — proves insert, update, delete and orphan healing |

Faker and `random` are both seeded with 42, so output is reproducible run to
run. The only fields that move between regenerations are the deliberately
future-dated ones (`future_signup_date`, `future_order_date`) — they exist to
violate a `max_date: today` rule, so they shift with the clock.

Locally the script writes CSVs to a directory (`--output-dir`); on Databricks
it writes to the landing volume (`--volume-root`, derived from `--catalog`),
one timestamped file per entity, which is what Auto Loader picks up.

## Why two deliveries

A single delivery cannot prove change-data-capture: everything in a first load
is an insert. The delta batch exists so a single end-to-end run demonstrates:

- **insert** — the 500 new orders use ids from 200001, disjoint from the
  seed's 1..100000, and never re-send seed rows (that is what `incremental`
  means for the orders feed)
- **update** — 20 customers keep their id but change `lifetime_value` and
  `customer_segment`; every other column is byte-identical, so the change is
  attributable
- **delete** — 3 products are omitted from the snapshot; a snapshot feed has
  no tombstone column, absence *is* the delete
- **orphan healing** — 10 of the 50 missing customers and 5 of the 30 missing
  products finally arrive. Partial on purpose: if every missing parent
  arrived, a healed flag would be indistinguishable from a check that never
  ran. Some orders must heal and some must stay flagged.

The delta batch injects **no** quality issues, so anything silver quarantines
after it came from the seed batch — results stay attributable.

## The intentional issues (725 rows)

The set the assessment names explicitly, exact against the committed CSVs:

| Issue | Rows | Silver check it exercises |
|---|---|---|
| NULL email | 50 | completeness |
| duplicate `customer_id` | 10 | uniqueness |
| NULL `orders.customer_id` | 100 | completeness |
| NULL `orders.product_id` | 200 | completeness |
| `customer_id` not in customers | 50 | referential |
| `product_id` not in products | 30 | referential |
| duplicate `order_id` | 20 | uniqueness |

Extended coverage — one scenario per validation rule the `dq_schema` config
declares, so a single run exercises the whole validator surface:

invalid email format (30) · invalid segment (20) · invalid status (20) ·
non-positive quantity (25) · negative price (15) · future signup (15) ·
NULL `customer_id` (5) · short/overlong names (8+8) · invalid country (12) ·
negative lifetime_value (10) · NULL `product_id` (3) · duplicate
`product_id` (5) · negative cost (8) · overlong product name (6) ·
negative/excessive stock (6+6) · NULL `order_id` (5) · excessive quantity (12)
· zero unit_price (12) · negative total (10) · pre-launch/future order date
(12+12).

`jobs/silver/tests/test_dq_coverage.py` enforces the pairing mechanically: it
fails if a declared rule has no scenario here, or if a scenario names a rule
nobody declares. A rule with no violating data reports 100% pass forever,
which is indistinguishable from a rule that works.

## Design decisions worth knowing

**A duplicate is an exact copy.** Copies are sampled from keyed rows *after*
the issues are injected, so a re-delivered bad row stays bad. An earlier
version overwrote the first N rows' key with row 0's key — that is a key
collision between unrelated entities, not a duplicate, and it made
survivorship outcome-dependent. Consequence: injected counts are floors, not
totals — copying a row that already had a NULL email adds another NULL email.

**NULL primary keys are appended, never written over an existing key.**
Nulling a parent key in place removes it from the parent table and silently
orphans every child: 3 nulled products cascaded into 562 unintended orphan
orders against a spec of 30 (100k orders over 500 products ≈ 200 children per
product). An appended row has no children by construction.

**Orphan ids live in a reserved range** (900001+ for customers, 910001+ for
products), so referential failures are recognisable on sight in any table.

## Running it

```bash
# local CSVs (what data/ in this repo was generated from)
databricks/.venv/bin/python databricks/jobs/data_generation/src/generate_sample_data.py \
  --output-dir data --batch-id seed

# on Databricks: the de_assessment_data_generation job runs seed mode by
# default; the end-to-end script fires delta mode via a python_params override
#   ["--catalog", "de_assessment", "--mode", "delta"]
```

Unit tests: `bash databricks/scripts/run_job_tests.sh data_generation --forbid-skips`
(17 tests: mode behaviour, id-space disjointness, changed-row attribution,
delete-by-omission, reproducibility, and the issue counts themselves).
