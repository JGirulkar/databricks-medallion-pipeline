# Gold Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Four gold aggregation tables built by full recompute from silver, with the business-rule contract stated once, proven by an independent-recompute contract tier, deployed as a table-update-triggered CE job, and verified end to end.

**Architecture:** Gold is pure declarative SQL over silver. Four `.sql` files are the executed source of truth; a thin Python runner creates one shared `qualifying_orders` temp view (the single stated business rule), executes the files in order, and writes one `ops.pipeline_manifest` row. Every run atomically replaces all four tables (`CREATE OR REPLACE TABLE … AS SELECT`), so any run from any silver state is correct and self-healing.

**Tech Stack:** Spark SQL on Databricks Free Edition serverless (jobs run workspace `.py` files — no wheels, no bundle), uv workspace for local test envs, pytest + local Spark/Delta for unit/spark/contract tiers, pandas for independent expectations.

**Spec:** `docs/superpowers/specs/2026-08-27-gold-layer-design.md`

## Global Constraints

- Git identity `JGirulkar <72917082+JGirulkar@users.noreply.github.com>` only; push with `GH_TOKEN=$(gh auth token -u JGirulkar)`. Never switch the global gh account.
- No tool attribution anywhere: no AI assistant names, no `Co-Authored-By`/`Generated-with` trailers, in any commit, file, or comment. No references to the author's employer or its projects.
- Commit style: lowercase conventional subjects; body = cause + mechanism + verification; atomic red→green pairs.
- Serverless bans (guard-tested in this repo): no `.cache()`/`.persist()`/`sparkContext` on the job path; no `sys.exit` in job code.
- Jobs are managed via `scripts/ce_job_registry.py` → Jobs API **reset** (never `update` — it merges the task array by key); every task carries `max_retries: 0` and `disable_auto_optimization: True`.
- Business-rule constants live in `databricks/jobs/gold/src/gold/config.py` and nowhere else: `INACTIVE_DAYS = 90`, `HIGH_VALUE_REVENUE = 5000`. The qualifying-orders rule is `order_status = 'Completed' AND NOT _is_orphan AND NOT _is_deleted`.
- Test gate per task: `bash databricks/scripts/run_job_tests.sh gold --forbid-skips` (all four jobs before the E2E task: `--all`). A skipped test is a defect.
- The cluster E2E (`scripts/run-medallion-e2e-ce.sh`, ~25 min) costs real serverless money — it runs exactly once, in Task 6, after every local tier is green.
- Workspace/profile: `de-assessment-ce`, host `https://dbc-06f970f4-0f19.cloud.databricks.com`, catalog `de_assessment`.

---

### Task 1: Gold package scaffold — SQL files as the executed source

**Files:**
- Modify: `databricks/pyproject.toml` (workspace members)
- Create: `databricks/jobs/gold/pyproject.toml`
- Create: `databricks/jobs/gold/src/gold/__init__.py` (empty)
- Create: `databricks/jobs/gold/src/gold/config.py`
- Create: `databricks/jobs/gold/src/gold/sql/01_sales_by_product.sql`
- Create: `databricks/jobs/gold/src/gold/sql/02_revenue_by_customer.sql`
- Create: `databricks/jobs/gold/src/gold/sql/03_daily_weekly_trends.sql`
- Create: `databricks/jobs/gold/src/gold/sql/04_customer_segmentation.sql`
- Create: `databricks/jobs/gold/tests/test_gold_config.py`
- Delete: `databricks/jobs/gold/src/create_gold_tables.py` (placeholder stub, replaced by this package)

**Interfaces:**
- Consumes: nothing new (mirrors `databricks/jobs/silver` layout).
- Produces: `gold.config` module — `DEFAULT_CATALOG: str`, `INACTIVE_DAYS: int`, `HIGH_VALUE_REVENUE: int`, `GOLD_SQL_FILES: tuple[str, ...]` (ordered), `GOLD_TABLES: tuple[str, ...]`, `load_sql(filename: str) -> str`, `render_sql(text: str, *, silver: str, gold: str) -> str`, `pipeline_manifest_table(catalog: str = DEFAULT_CATALOG) -> str`. Later tasks import all of these.

- [ ] **Step 1: Add gold to the uv workspace and scaffold the member**

In `databricks/pyproject.toml` change the members line to:

```toml
[tool.uv.workspace]
members = ["jobs/data_generation", "jobs/bronze", "jobs/silver", "jobs/gold"]
```

Create `databricks/jobs/gold/pyproject.toml` (mirror of silver's):

```toml
[project]
name = "gold-layer"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/gold"]

[tool.pytest.ini_options]
pythonpath = ["src"]
markers = [
  "unit: pure Python tests",
  "spark: local Spark tests",
]
```

Create empty `databricks/jobs/gold/src/gold/__init__.py`, delete `databricks/jobs/gold/src/create_gold_tables.py`, then sync from `databricks/`:

Run: `cd databricks && uv sync --all-packages --all-groups`
Expected: lockfile updates to include `gold-layer`; commit the `uv.lock` change with this task.

- [ ] **Step 2: Write the failing unit tests**

`databricks/jobs/gold/tests/test_gold_config.py`:

```python
"""Unit tier: the SQL files and their loading/substitution contract."""

from __future__ import annotations

import re

import pytest
from gold.config import (
    GOLD_SQL_FILES,
    GOLD_TABLES,
    HIGH_VALUE_REVENUE,
    INACTIVE_DAYS,
    load_sql,
    render_sql,
)

pytestmark = pytest.mark.unit


def test_the_four_files_exist_in_execution_order() -> None:
    assert GOLD_SQL_FILES == (
        "01_sales_by_product.sql",
        "02_revenue_by_customer.sql",
        "03_daily_weekly_trends.sql",
        "04_customer_segmentation.sql",
    )
    for name in GOLD_SQL_FILES:
        assert load_sql(name).strip(), f"{name} is empty"


def test_render_substitutes_every_placeholder() -> None:
    for name in GOLD_SQL_FILES:
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test")
        leftover = re.findall(r"\{[a-z_]+\}", rendered)
        assert not leftover, f"{name} has unsubstituted placeholders: {leftover}"


def test_each_file_replaces_exactly_its_own_table() -> None:
    for name, table in zip(GOLD_SQL_FILES, GOLD_TABLES):
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test")
        assert f"create or replace table g_test.{table}" in rendered.lower()


def test_aggregation_files_read_the_shared_view_not_raw_orders() -> None:
    # The business rule lives in the runner's qualifying_orders view. A file
    # that reads silver.orders directly has smuggled in its own rule.
    for name in GOLD_SQL_FILES:
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test").lower()
        assert "s_test.orders" not in rendered, f"{name} bypasses qualifying_orders"
    for name in GOLD_SQL_FILES[:3]:
        rendered = render_sql(load_sql(name), silver="s_test", gold="g_test").lower()
        assert "qualifying_orders" in rendered


def test_segmentation_derives_from_revenue_by_customer() -> None:
    # Cross-footing by construction: 04 reads the table 02 built.
    rendered = render_sql(load_sql(GOLD_SQL_FILES[3]), silver="s_test", gold="g_test").lower()
    assert "g_test.revenue_by_customer" in rendered
    assert "qualifying_orders" not in rendered


def test_thresholds_are_the_pinned_constants() -> None:
    assert INACTIVE_DAYS == 90
    assert HIGH_VALUE_REVENUE == 5000
    rendered = render_sql(load_sql(GOLD_SQL_FILES[3]), silver="s", gold="g")
    assert "90" in rendered and "5000" in rendered
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd databricks/jobs/gold && uv run --no-sync python -m pytest tests/test_gold_config.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'gold.config'`

- [ ] **Step 4: Write config.py and the four SQL files**

`databricks/jobs/gold/src/gold/config.py`:

```python
"""Gold configuration: pinned business constants and SQL file loading.

The business rules are defined ONCE here and in the runner's
qualifying_orders view — the SQL files receive them as substitutions.
"""

from __future__ import annotations

from pathlib import Path

DEFAULT_CATALOG = "de_assessment"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
OPS_SCHEMA = "ops"
PIPELINE_MANIFEST_TABLE_NAME = "pipeline_manifest"

# Segment ladder constants, pinned from the measured seed distribution
# (per-customer lifetime qualifying revenue p90 ~= 4,983 -> 5,000).
# See docs/superpowers/specs/2026-08-27-gold-layer-design.md §3.3.
INACTIVE_DAYS = 90
HIGH_VALUE_REVENUE = 5000

SQL_DIR = Path(__file__).resolve().parent / "sql"

# Execution order. 01–03 read only qualifying_orders and silver dims;
# 04 reads the table 02 built (cross-foots by construction).
GOLD_SQL_FILES: tuple[str, ...] = (
    "01_sales_by_product.sql",
    "02_revenue_by_customer.sql",
    "03_daily_weekly_trends.sql",
    "04_customer_segmentation.sql",
)
GOLD_TABLES: tuple[str, ...] = (
    "sales_by_product",
    "revenue_by_customer",
    "daily_weekly_trends",
    "customer_segmentation",
)


def load_sql(filename: str) -> str:
    return (SQL_DIR / filename).read_text(encoding="utf-8")


def render_sql(text: str, *, silver: str, gold: str) -> str:
    return text.format(
        silver=silver,
        gold=gold,
        inactive_days=INACTIVE_DAYS,
        high_value_revenue=HIGH_VALUE_REVENUE,
    )


def pipeline_manifest_table(catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{OPS_SCHEMA}.{PIPELINE_MANIFEST_TABLE_NAME}"
```

`databricks/jobs/gold/src/gold/sql/01_sales_by_product.sql`:

```sql
-- Sales by product. Reads qualifying_orders (order_status = 'Completed',
-- NOT _is_orphan, NOT _is_deleted) — the rule is defined once, in the
-- runner, never here. Zero-sales products are kept: a product missing from
-- a sales report is indistinguishable from a pipeline bug. avg_order_value
-- is NULL (not 0) when there are no orders — an average over nothing is
-- unknown, not zero.
CREATE OR REPLACE TABLE {gold}.sales_by_product AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  COUNT(q.order_id) AS total_orders,
  CAST(COALESCE(SUM(q.total_amount), 0) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(SUM(q.total_amount) / NULLIF(COUNT(q.order_id), 0) AS DECIMAL(18, 2)) AS avg_order_value
FROM {silver}.products p
LEFT JOIN qualifying_orders q
  ON q.product_id = p.product_id
WHERE NOT p._is_deleted
GROUP BY p.product_id, p.product_name, p.category
```

`databricks/jobs/gold/src/gold/sql/02_revenue_by_customer.sql`:

```sql
-- Revenue by customer. customer_segment is the source-declared column
-- (Premium/Standard/Basic), carried as delivered; the BEHAVIOURAL segment
-- lives in customer_segmentation. lifetime_value_actual is computed from
-- orders — it sits alongside the declared lifetime_value upstream as a
-- declared-vs-actual comparison. last_order_date feeds the segment ladder's
-- recency test and is NULL for customers with no qualifying orders.
CREATE OR REPLACE TABLE {gold}.revenue_by_customer AS
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_segment,
  COUNT(q.order_id) AS total_orders,
  CAST(COALESCE(SUM(q.total_amount), 0) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(SUM(q.total_amount) / NULLIF(COUNT(q.order_id), 0) AS DECIMAL(18, 2)) AS avg_order_value,
  CAST(COALESCE(SUM(q.total_amount), 0) AS DECIMAL(18, 2)) AS lifetime_value_actual,
  MAX(q.order_date) AS last_order_date
FROM {silver}.customers c
LEFT JOIN qualifying_orders q
  ON q.customer_id = c.customer_id
WHERE NOT c._is_deleted
GROUP BY c.customer_id, c.customer_name, c.customer_segment
```

`databricks/jobs/gold/src/gold/sql/03_daily_weekly_trends.sql`:

```sql
-- Daily grain with a week_start column: weekly views GROUP BY week_start,
-- so both grains come from one set of numbers. Days with no qualifying
-- orders are absent (grain = observed business days).
CREATE OR REPLACE TABLE {gold}.daily_weekly_trends AS
SELECT
  q.order_date,
  CAST(DATE_TRUNC('WEEK', q.order_date) AS DATE) AS week_start,
  COUNT(q.order_id) AS total_orders,
  CAST(SUM(q.total_amount) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(SUM(q.total_amount) / COUNT(q.order_id) AS DECIMAL(18, 2)) AS avg_order_value
FROM qualifying_orders q
GROUP BY q.order_date
```

`databricks/jobs/gold/src/gold/sql/04_customer_segmentation.sql`:

```sql
-- Behavioural segmentation. Derives from {gold}.revenue_by_customer — NOT
-- from silver — so the pie cross-foots with the customer table by
-- construction. ORDERING CONSTRAINT: 02_revenue_by_customer.sql must run
-- first (the runner executes files in name order).
--
-- Ladder, evaluated top-down, mutually exclusive and exhaustive:
--   Inactive    no qualifying order in the {inactive_days} days before
--               as_of (as_of = MAX(last_order_date), data-anchored — safe
--               because silver's order_date window check quarantines
--               future-dated rows). Includes customers with no qualifying
--               orders at all.
--   High-Value  active AND lifetime qualifying revenue >= {high_value_revenue}
--   Repeat      active AND >= 2 lifetime qualifying orders
--   One-Time    active AND exactly 1
-- Recency outranks value: a lapsed big spender is the win-back signal.
CREATE OR REPLACE TABLE {gold}.customer_segmentation AS
WITH as_of AS (
  SELECT MAX(last_order_date) AS as_of_date
  FROM {gold}.revenue_by_customer
),
labeled AS (
  SELECT
    CASE
      WHEN r.last_order_date IS NULL
        OR r.last_order_date < DATE_SUB(a.as_of_date, {inactive_days}) THEN 'Inactive'
      WHEN r.lifetime_value_actual >= {high_value_revenue} THEN 'High-Value'
      WHEN r.total_orders >= 2 THEN 'Repeat'
      ELSE 'One-Time'
    END AS segment_type,
    r.total_revenue
  FROM {gold}.revenue_by_customer r
  CROSS JOIN as_of a
)
SELECT
  segment_type,
  COUNT(*) AS customer_count,
  CAST(AVG(total_revenue) AS DECIMAL(18, 2)) AS avg_revenue,
  CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue
FROM labeled
GROUP BY segment_type
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd databricks/jobs/gold && uv run --no-sync python -m pytest tests/test_gold_config.py -q`
Expected: 6 passed. Also run `bash databricks/scripts/run_job_tests.sh gold --forbid-skips` — same result via the runner.

- [ ] **Step 6: Commit**

```bash
git add databricks/pyproject.toml databricks/uv.lock databricks/jobs/gold
git rm databricks/jobs/gold/src/create_gold_tables.py 2>/dev/null || true
git commit -m "feat(gold): package scaffold — four aggregation SQL files as executed source"
```

---

### Task 2: Manifest + runner (local Spark tier)

**Files:**
- Create: `databricks/jobs/gold/src/gold/manifest.py`
- Create: `databricks/jobs/gold/src/gold/runner.py`
- Create: `databricks/jobs/gold/src/gold/job_log.py` (copy `databricks/jobs/silver/src/silver/job_log.py` verbatim, module docstring adjusted to gold)
- Create: `databricks/jobs/gold/tests/conftest.py`
- Test: `databricks/jobs/gold/tests/test_runner.py`

**Interfaces:**
- Consumes: `gold.config` (Task 1).
- Produces: `run_gold(spark, catalog=DEFAULT_CATALOG, *, silver_schema: str | None = None, gold_schema: str | None = None, manifest_table: str | None = None) -> str` (returns run_id); `GoldManifestRecord` dataclass + `append_gold_manifest(spark, record, manifest_table)`; `PIPELINE_MANIFEST_SCHEMA` (copied constant — gold cannot import silver at runtime, the workspace dirs are separate). Task 3/5/6 rely on `run_gold`'s keyword overrides.

- [ ] **Step 1: Write conftest (copy silver's proven local-Spark setup)**

`databricks/jobs/gold/tests/conftest.py` — copy `databricks/jobs/silver/tests/conftest.py` verbatim (interpreter pin via `_pin_pyspark_interpreter`, module-scoped `spark` fixture with Delta + local warehouse, `create_delta_table` helper). Only change: the warehouse dir name so parallel suites never collide (`spark-warehouse` under the gold job dir — the fixture already derives it from `__file__`; verify after copying).

- [ ] **Step 2: Write the failing runner tests**

`databricks/jobs/gold/tests/test_runner.py`:

```python
"""Spark tier: runner mechanics against tiny hand-built silver tables.

Number-correctness lives in test_gold_contract.py; this file proves the
machinery — view creation, execution order, manifest row, idempotent rerun.
"""

from __future__ import annotations

import datetime as dt

import pytest
from gold.config import GOLD_TABLES
from gold.manifest import PIPELINE_MANIFEST_SCHEMA
from gold.runner import run_gold
from pyspark.sql import SparkSession

pytestmark = pytest.mark.spark

SILVER = "rt_silver"
GOLD = "rt_gold"
MANIFEST = "rt_manifest"


@pytest.fixture(scope="module")
def tiny_silver(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER}")
    spark.createDataFrame(
        [
            # customer_id, name, segment, deleted
            (1, "Ada", "Premium", False),
            (2, "Ben", "Basic", False),
            (3, "Cyd", "Basic", True),  # deleted dim row
        ],
        "customer_id INT, customer_name STRING, customer_segment STRING, _is_deleted BOOLEAN",
    ).write.mode("overwrite").saveAsTable(f"{SILVER}.customers")
    spark.createDataFrame(
        [(10, "Lamp", "Home", False), (11, "Mug", "Kitchen", False)],
        "product_id INT, product_name STRING, category STRING, _is_deleted BOOLEAN",
    ).write.mode("overwrite").saveAsTable(f"{SILVER}.products")
    spark.createDataFrame(
        [
            # order, cust, prod, date, amount, status, orphan, deleted
            (100, 1, 10, dt.date(2025, 6, 1), 50.0, "Completed", False, False),
            (101, 1, 11, dt.date(2025, 6, 2), 30.0, "Completed", False, False),
            (102, 2, 10, dt.date(2025, 6, 3), 20.0, "Pending", False, False),   # status-excluded
            (103, 2, 11, dt.date(2025, 6, 4), 40.0, "Completed", True, False),  # orphan-excluded
            (104, 2, 10, dt.date(2025, 6, 5), 60.0, "Completed", False, True),  # deleted-excluded
        ],
        "order_id INT, customer_id INT, product_id INT, order_date DATE, "
        "total_amount DECIMAL(18,2), order_status STRING, _is_orphan BOOLEAN, _is_deleted BOOLEAN",
    ).write.mode("overwrite").saveAsTable(f"{SILVER}.orders")
    spark.createDataFrame([], PIPELINE_MANIFEST_SCHEMA).write.mode(
        "overwrite"
    ).saveAsTable(MANIFEST)
    return spark


def test_run_gold_builds_all_four_tables(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    run_id = run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    assert run_id
    for table in GOLD_TABLES:
        assert spark.table(f"{GOLD}.{table}").count() > 0, table


def test_only_qualifying_orders_count(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    # Orders 102 (Pending), 103 (orphan), 104 (deleted) are excluded:
    # qualifying revenue is exactly 50 + 30, both from customer 1.
    by_customer = {
        r["customer_id"]: r
        for r in spark.table(f"{GOLD}.revenue_by_customer").collect()
    }
    assert float(by_customer[1]["total_revenue"]) == 80.0
    assert by_customer[1]["total_orders"] == 2
    assert float(by_customer[2]["total_revenue"]) == 0.0
    assert by_customer[2]["total_orders"] == 0
    assert by_customer[2]["avg_order_value"] is None
    assert 3 not in by_customer  # deleted dim row excluded


def test_manifest_row_written_with_gold_layer(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    before = spark.table(MANIFEST).count()
    run_id = run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    rows = spark.table(MANIFEST).filter(f"run_id = '{run_id}'").collect()
    assert len(rows) == 1
    row = rows[0]
    assert row["layer"] == "gold"
    assert row["status"] == "success"
    assert row["files_processed"] == 4
    assert row["rows_read"] == 5           # all silver order rows scanned
    assert row["rows_written"] > 0
    assert spark.table(MANIFEST).count() == before + 1


def test_rerun_is_idempotent(tiny_silver: SparkSession) -> None:
    spark = tiny_silver
    run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    first = {
        t: sorted(map(str, spark.table(f"{GOLD}.{t}").collect())) for t in GOLD_TABLES
    }
    run_gold(spark, silver_schema=SILVER, gold_schema=GOLD, manifest_table=MANIFEST)
    second = {
        t: sorted(map(str, spark.table(f"{GOLD}.{t}").collect())) for t in GOLD_TABLES
    }
    assert first == second
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd databricks/jobs/gold && uv run --no-sync python -m pytest tests/test_runner.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'gold.manifest'`

- [ ] **Step 4: Write manifest.py and runner.py**

`databricks/jobs/gold/src/gold/manifest.py` — mirror `databricks/jobs/silver/src/silver/manifest.py`, with `layer="gold"`, `entity_name="gold_aggregations"` conventions and the schema copied (gold cannot import silver at runtime — separate workspace upload roots):

```python
"""Gold pipeline manifest rows in ops.pipeline_manifest.

The schema is a copy of the silver package's PIPELINE_MANIFEST_SCHEMA: the
two jobs are uploaded to separate workspace directories and cannot import
each other at runtime. The reference DDL guard in database/schema.sql keeps
the copies honest.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

PIPELINE_MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("layer", StringType(), False),
        StructField("entity_name", StringType(), False),
        StructField("parent_run_id", StringType(), True),
        StructField("delivery_pattern", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("files_processed", IntegerType(), False),
        StructField("rows_read", LongType(), False),
        StructField("rows_written", LongType(), False),
        StructField("rows_quarantined", LongType(), False),
        StructField("rows_rescued", LongType(), False),
        StructField("delta_version_before", LongType(), True),
        StructField("delta_version_after", LongType(), True),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)


@dataclass(frozen=True)
class GoldManifestRecord:
    run_id: str
    files_processed: int
    rows_read: int
    rows_written: int
    started_at: datetime
    completed_at: datetime | None
    status: Literal["success", "failed"]
    error_message: str | None = None

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must not be empty")
        if self.status not in {"success", "failed"}:
            raise ValueError(f"Invalid status: {self.status}")
        if self.status == "success" and self.completed_at is None:
            raise ValueError("completed_at is required for successful runs")

    def as_row(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "layer": "gold",
            "entity_name": "gold_aggregations",
            "parent_run_id": None,
            "delivery_pattern": None,
            "source_path": None,
            "files_processed": self.files_processed,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_quarantined": 0,
            "rows_rescued": 0,
            "delta_version_before": None,
            "delta_version_after": None,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "status": self.status,
            "error_message": self.error_message,
        }


def append_gold_manifest(
    spark: SparkSession, record: GoldManifestRecord, manifest_table: str
) -> None:
    row_df = spark.createDataFrame([record.as_row()], schema=PIPELINE_MANIFEST_SCHEMA)
    row_df.write.format("delta").mode("append").saveAsTable(manifest_table)
```

`databricks/jobs/gold/src/gold/runner.py`:

```python
"""Gold runner: one qualifying_orders view, four SQL files, one manifest row.

Full recompute per run — every table is atomically replaced from current
silver, so any run from any silver state is correct and a failed run leaves
the previous version intact. The business rule (what counts as revenue)
exists exactly once: the view defined here.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from pyspark.sql import SparkSession

from gold.config import (
    DEFAULT_CATALOG,
    GOLD_SCHEMA,
    GOLD_SQL_FILES,
    GOLD_TABLES,
    SILVER_SCHEMA,
    load_sql,
    pipeline_manifest_table,
    render_sql,
)
from gold.job_log import configure_job_logger
from gold.manifest import GoldManifestRecord, append_gold_manifest

LOG = configure_job_logger("gold.runner")

QUALIFYING_ORDERS_VIEW = """
CREATE OR REPLACE TEMPORARY VIEW qualifying_orders AS
SELECT order_id, customer_id, product_id, order_date, total_amount
FROM {silver}.orders
WHERE order_status = 'Completed'
  AND NOT _is_orphan
  AND NOT _is_deleted
"""

INPUT_BREAKDOWN = """
SELECT
  COUNT(*) AS rows_total,
  COUNT_IF(order_status = 'Completed' AND NOT _is_orphan AND NOT _is_deleted) AS rows_qualifying,
  COUNT_IF(order_status = 'Pending') AS rows_pending,
  COUNT_IF(order_status = 'Cancelled') AS rows_cancelled,
  COUNT_IF(_is_orphan) AS rows_orphan,
  COUNT_IF(_is_deleted) AS rows_deleted
FROM {silver}.orders
"""


def run_gold(
    spark: SparkSession,
    catalog: str = DEFAULT_CATALOG,
    *,
    silver_schema: str | None = None,
    gold_schema: str | None = None,
    manifest_table: str | None = None,
) -> str:
    silver = silver_schema or f"{catalog}.{SILVER_SCHEMA}"
    gold = gold_schema or f"{catalog}.{GOLD_SCHEMA}"
    manifest = manifest_table or pipeline_manifest_table(catalog)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(UTC)

    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold}")
        breakdown = spark.sql(INPUT_BREAKDOWN.format(silver=silver)).collect()[0]
        LOG.info(
            "gold_input run_id=%s total=%s qualifying=%s pending=%s "
            "cancelled=%s orphan=%s deleted=%s",
            run_id,
            breakdown["rows_total"],
            breakdown["rows_qualifying"],
            breakdown["rows_pending"],
            breakdown["rows_cancelled"],
            breakdown["rows_orphan"],
            breakdown["rows_deleted"],
        )
        spark.sql(QUALIFYING_ORDERS_VIEW.format(silver=silver))
        rows_written = 0
        for filename, table in zip(GOLD_SQL_FILES, GOLD_TABLES):
            spark.sql(render_sql(load_sql(filename), silver=silver, gold=gold))
            count = spark.table(f"{gold}.{table}").count()
            rows_written += count
            LOG.info("gold_table run_id=%s table=%s rows=%s", run_id, table, count)
    except Exception as exc:
        append_gold_manifest(
            spark,
            GoldManifestRecord(
                run_id=run_id,
                files_processed=len(GOLD_SQL_FILES),
                rows_read=0,
                rows_written=0,
                started_at=started_at,
                completed_at=None,
                status="failed",
                error_message=str(exc)[:1024],
            ),
            manifest,
        )
        raise

    append_gold_manifest(
        spark,
        GoldManifestRecord(
            run_id=run_id,
            files_processed=len(GOLD_SQL_FILES),
            rows_read=int(breakdown["rows_total"]),
            rows_written=rows_written,
            started_at=started_at,
            completed_at=datetime.now(UTC),
            status="success",
        ),
        manifest,
    )
    return run_id
```

`gold/job_log.py`: copy silver's file verbatim; change only the module docstring to say gold.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd databricks/jobs/gold && uv run --no-sync python -m pytest tests/ -q`
Expected: all pass (config unit tests + 4 runner tests), 0 skipped.

- [ ] **Step 6: Commit**

```bash
git add databricks/jobs/gold
git commit -m "feat(gold): runner — shared qualifying view, ordered execution, manifest row"
```

---

### Task 3: Contract tier — gold numbers vs independent recompute

**Files:**
- Create: `databricks/jobs/gold/tests/test_gold_contract.py`
- Modify: `databricks/jobs/gold/tests/conftest.py` (add the silver-builder fixture helpers)

**Interfaces:**
- Consumes: `run_gold` (Task 2); the real silver package via path (`databricks/jobs/silver/src`); the real generator via `importlib` file loading (same pattern as `databricks/jobs/silver/tests/test_pipeline_contract.py:35-42`).
- Produces: nothing downstream — this is the correctness gate.

**Method (mirrors the silver contract tier):** build real silver tables locally by pushing generator output through the REAL silver functions (`annotate_violations` → `apply_entity_checks` → `split_validated_batch` → `merge_to_silver` → `apply_snapshot_soft_deletes` → `refresh_orphan_flags`), for BOTH the seed delivery (`generate_dataframes()`) and the delta delivery (`generate_delta_dataframes()` — updates, product soft-deletes, late parents that heal orphans), so gold's filter sees real `_is_deleted = true` rows and healed orphans. Then run the REAL gold SQL files via `run_gold` and compare every table against pandas group-bys computed from the silver tables' content — silver is gold's input contract (already proven by silver's own suite), and the pandas side shares no code with the SQL.

- [ ] **Step 1: Extend conftest with the silver-builder**

Add to `databricks/jobs/gold/tests/conftest.py` (below the existing fixtures):

```python
import importlib.util
import pathlib
import sys

_SILVER_SRC = pathlib.Path(__file__).resolve().parents[2] / "silver" / "src"
if str(_SILVER_SRC) not in sys.path:
    sys.path.insert(0, str(_SILVER_SRC))

_GEN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "data_generation" / "src" / "generate_sample_data.py"
)


def load_generator():
    spec = importlib.util.spec_from_file_location("generate_sample_data", _GEN)
    assert spec and spec.loader
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    return gen
```

Then a module-scoped `silver_tables(spark)` fixture in `test_gold_contract.py` that:
1. Creates databases `gct_silver` and (for the runner) leaves `gct_gold` creation to `run_gold`.
2. Monkeypatches exactly like `databricks/jobs/silver/tests/test_pipeline_contract.py:246-249` but with schema-qualified names: `conform_mod.silver_table = lambda entity, _c="de_assessment": f"gct_silver.{entity}"`, `quarantine_mod.quarantine_table = lambda _c="de_assessment": "gct_silver.quarantine"`, and the same `load_dq_schema` stub built from `silver.bootstrap`'s seeds (copy the `_dq_schema` helper from the silver contract test).
3. Sizes the generator so every segment is reachable: `gen.BASE_CUSTOMERS, gen.BASE_PRODUCTS, gen.BASE_ORDERS = 150, 60, 1500` (~10 orders/customer, the seed dataset's ratio, so High-Value ≥ 5000 has members at small scale — verify at red/green and adjust BASE_ORDERS upward if the High-Value guard fails).
4. Runs the seed delivery through the silver sequence above (reuse the `_conform` helper shape from `databricks/jobs/silver/tests/test_pipeline_contract.py:218-233`, then `refresh_orphan_flags(spark)` — the monkeypatched table resolver routes it to `gct_silver.orders`), then the delta delivery (`generate_delta_dataframes()`) as batch `"delta"`, then `refresh_orphan_flags(spark)` again.
5. Calls `run_gold(spark, silver_schema="gct_silver", gold_schema="gct_gold", manifest_table="gct_manifest")` (create `gct_manifest` from `PIPELINE_MANIFEST_SCHEMA` first) and returns `{"spark": spark, "silver": pandas snapshots of the three silver tables}`.

- [ ] **Step 2: Write the failing contract tests**

`databricks/jobs/gold/tests/test_gold_contract.py` — the expectations side, all pandas, no Spark SQL:

```python
def _qualifying(orders: pd.DataFrame) -> pd.DataFrame:
    return orders[
        (orders["order_status"] == "Completed")
        & ~orders["_is_orphan"]
        & ~orders["_is_deleted"]
    ]
```

Tests (each `@pytest.mark.spark`, using the module fixture):

1. `test_sales_by_product_matches_independent_recompute` — pandas: `_qualifying(orders).groupby("product_id")["total_amount"].agg(["count", "sum"])` left-joined onto non-deleted products; compare full frames (product_id, total_orders, total_revenue, avg_order_value) against `gct_gold.sales_by_product` sorted; zero-sales products expected with `total_orders == 0`, `total_revenue == 0`, `avg_order_value is None`.
2. `test_revenue_by_customer_matches_independent_recompute` — same shape per customer, plus `lifetime_value_actual == total_revenue` for every row and `last_order_date == max qualifying order_date` (NaT → NULL).
3. `test_daily_weekly_trends_matches_independent_recompute` — pandas groupby on `order_date`; also `week_start == order_date - timedelta(days=order_date.weekday())` for every row (Monday truncation, matching DATE_TRUNC('WEEK')).
4. `test_segmentation_matches_the_stated_ladder` — pandas ladder with `as_of = qualifying order_date max`, `cutoff = as_of - 90 days`, thresholds imported from `gold.config` (constants shared deliberately — the *rule* is shared, the *computation* is not); compare (segment_type, customer_count, avg_revenue, total_revenue) sets.
5. `test_gold_tables_cross_foot` — `sum(customer_count) == len(revenue_by_customer)`; `sum(segmentation.total_revenue) == sum(revenue_by_customer.total_revenue) == sum(sales_by_product.total_revenue) == sum(trends.total_revenue)` (all four tables describe the same qualifying money).
6. `test_every_segment_is_reachable` — all four segment_type values present with `customer_count > 0` (an unreachable segment is indistinguishable from a broken ladder).
7. `test_zero_activity_rows_are_kept` — at least one product and one customer with `total_orders == 0` and NULL `avg_order_value` (the delta wave's soft-deleted products must NOT appear at all — assert their ids absent).
8. `test_revenue_column_reconciles` — on the qualifying pandas frame, `total_amount == round(quantity * unit_price, 2)` for every row (needs quantity/unit_price included in the silver snapshot; select them in the fixture).
9. `test_as_of_is_data_anchored` — recompute `as_of` from qualifying rows and assert no qualifying `order_date` exceeds it and that it is `<=` today (silver's date-window check is what makes this hold — this test documents the dependency).

- [ ] **Step 3: Run to verify the suite fails only for the right reason**

Run: `cd databricks/jobs/gold && uv run --no-sync python -m pytest tests/test_gold_contract.py -q`
Expected: first run FAILS at fixture or assertion level; iterate until failures are genuine implementation-vs-expectation mismatches, then resolve each by diagnosing WHICH SIDE is wrong before changing anything (house debugging rule). Record any real defect found in `debugging-notes.md` under "Found by the contract tier".

- [ ] **Step 4: Run the full gold suite green, then the full local fleet**

Run: `bash databricks/scripts/run_job_tests.sh gold --forbid-skips` → all pass, 0 skipped.
Run: `bash databricks/scripts/run_job_tests.sh --all --forbid-skips` → data_generation, bronze, silver, gold all green (gold's conftest path-insert must not break silver's own suite).

- [ ] **Step 5: Commit**

```bash
git add databricks/jobs/gold/tests
git commit -m "test(gold): contract tier — real SQL vs independent pandas recompute"
```

---

### Task 4: Reference DDL + drift guard

**Files:**
- Modify: `database/schema.sql` (append a gold section: four `CREATE TABLE` DDLs with full column lists/types and a comment that runtime tables are CTAS-replaced — the DDL is the documented shape)
- Test: `databricks/jobs/gold/tests/test_schema_sql_drift.py`

**Interfaces:**
- Consumes: the built gold tables from the Task 3 fixture.
- Produces: schema.sql sections later docs cite.

- [ ] **Step 1: Write the failing drift test**

```python
"""The reference DDL in database/schema.sql must list exactly the columns
the executed SQL files produce. Derived from execution, not from a second
hand-maintained list — a reference doc without a guard is future drift."""

import pathlib
import re

import pytest
from gold.config import GOLD_TABLES

SCHEMA_SQL = pathlib.Path(__file__).resolve().parents[4] / "database" / "schema.sql"


@pytest.mark.spark
def test_schema_sql_matches_built_gold_tables(silver_tables) -> None:
    spark = silver_tables["spark"]
    text = SCHEMA_SQL.read_text(encoding="utf-8").lower()
    for table in GOLD_TABLES:
        match = re.search(
            rf"create table[^(]*gold\.{table}\s*\((.*?)\)\s*;", text, re.DOTALL
        )
        assert match, f"gold.{table} missing from schema.sql"
        declared = {
            line.strip().split()[0]
            for line in match.group(1).splitlines()
            if line.strip() and not line.strip().startswith("--")
        }
        built = {f.name.lower() for f in spark.table(f"gct_gold.{table}").schema.fields}
        assert declared == built, f"gold.{table}: schema.sql drift {declared ^ built}"
```

(Move the `silver_tables` fixture into `conftest.py` in Task 3 so both test files share it.)

- [ ] **Step 2: Run to verify it fails** — `gold.sales_by_product missing from schema.sql`.

- [ ] **Step 3: Append the gold DDL section to database/schema.sql**

Four `CREATE TABLE gold.<name> (...);` blocks matching the SQL files' output columns exactly (`total_orders BIGINT`, money columns `DECIMAL(18,2)`, dates `DATE`, `customer_count BIGINT`), headed by a comment: gold tables are rebuilt by `CREATE OR REPLACE TABLE … AS SELECT` each run; these DDLs document the produced shape and are guard-tested against execution.

- [ ] **Step 4: Run to verify green** (`run_job_tests.sh gold --forbid-skips`).

- [ ] **Step 5: Commit**

```bash
git add database/schema.sql databricks/jobs/gold/tests/test_schema_sql_drift.py
git commit -m "feat(gold): reference DDL for the four tables, guarded against execution"
```

---

### Task 5: Entry script, deploy wiring, CE deploy + first cluster run

**Files:**
- Create: `databricks/jobs/gold/src/run_gold.py` (entry script at src root — the workspace upload root)
- Create: `databricks/jobs/gold/src/workspace_path.py` (copy silver's `workspace_path.py`, function renamed `setup_gold_src_path`, path adjusted to the gold package dir)
- Modify: `scripts/deploy-all-ce-jobs.sh` (GOLD_WS/GOLD_SRC upload + env export)
- Modify: `scripts/ce_job_registry.py` (gold job with the table-update trigger)

**Interfaces:**
- Consumes: `run_gold` (Task 2); `base_job`/`spark_task` in `scripts/ce_job_registry.py:125-177`.
- Produces: CE job `de_assessment_gold_aggregations` (Task 6 waits on this exact name).

- [ ] **Step 1: Entry script**

`databricks/jobs/gold/src/run_gold.py` (mirror `databricks/jobs/silver/src/conform_orders.py`):

```python
from workspace_path import setup_gold_src_path

setup_gold_src_path()

import argparse

from gold.job_log import configure_job_logger, run_main
from gold.runner import run_gold

LOG = configure_job_logger("gold.run_gold")


def main() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession — run this on a Databricks cluster")
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="de_assessment")
    args = parser.parse_args()
    run_gold(spark, catalog=args.catalog)


if __name__ == "__main__":
    run_main(main, LOG)
```

- [ ] **Step 2: Deploy script + registry**

`scripts/deploy-all-ce-jobs.sh`: add `GOLD_WS="/Workspace/Users/${USER_EMAIL}/de-medallion-assessment/gold"`, `GOLD_SRC="${REPO_ROOT}/databricks/jobs/gold/src"`, an upload block identical to silver's, and `GOLD_WS` in the final `export`.

`scripts/ce_job_registry.py`: thread `gold_ws` through `all_job_settings(...)` and `main()` (env `GOLD_WS`), and append:

```python
base_job(
    gold_ws,
    catalog,
    "de_assessment_gold_aggregations",
    "aggregate",
    "run_gold.py",
    trigger={
        "pause_status": "UNPAUSED",
        "table_update": {
            "table_names": [
                f"{catalog}.silver.products",
                f"{catalog}.silver.customers",
                f"{catalog}.silver.orders",
            ],
            "condition": "ANY_UPDATED",
            "min_time_between_triggers_seconds": 120,
        },
    },
),
```

- [ ] **Step 3: Deploy and verify the upload format**

Run: `bash scripts/deploy-all-ce-jobs.sh`
Then verify the SQL files landed as workspace FILES, not notebooks (import-dir strips extensions from notebook-converted files, which would break `load_sql` at runtime):

Run: `databricks workspace get-status "/Workspace/Users/<email>/de-medallion-assessment/gold/gold/sql/01_sales_by_product.sql" --profile de-assessment-ce`
Expected: `object_type: FILE`. If it reports NOTEBOOK (or the path is missing its extension), replace the gold upload block with per-file `databricks workspace import --file <local> <ws-path> --format RAW` for the `.sql` files and record the behaviour in `debugging-notes.md`.

Also verify the trigger condition survived: `databricks jobs get --job-id <id> --profile de-assessment-ce -o json | python3 -c "import json,sys; print(json.load(sys.stdin)['settings']['trigger'])"` → three tables + `ANY_UPDATED` + 120.

- [ ] **Step 4: First manual cluster run (bootstraps the gold schema)**

Run: `databricks jobs run-now --job-id <gold-job-id> --profile de-assessment-ce` and poll to completion.
Verify with three queries via `databricks experimental aitools tools query ... --profile de-assessment-ce`:
- `SELECT COUNT(*) FROM de_assessment.gold.sales_by_product` > 0 (and the other three tables)
- `SELECT * FROM de_assessment.ops.pipeline_manifest WHERE layer='gold' ORDER BY started_at DESC LIMIT 1` → status success, rows_read = silver orders count
- `SELECT segment_type, customer_count FROM de_assessment.gold.customer_segmentation` → four segments

A green run is not evidence — these table checks are.

- [ ] **Step 5: Commit**

```bash
git add databricks/jobs/gold/src scripts/deploy-all-ce-jobs.sh scripts/ce_job_registry.py
git commit -m "feat(gold): CE job — workspace entry, any-updated trigger on the three silver tables"
```

---

### Task 6: E2E gold phase + the one full run

**Files:**
- Modify: `scripts/medallion_e2e.py` (gold phase: convergence wait + `verify_gold`)

**Interfaces:**
- Consumes: existing helpers in `scripts/medallion_e2e.py` — `MedallionResult` (line 79), the SQL statement helper `verify_silver`/`merge_metrics` use (read those functions first and use the same one, aliased locally as `q(sql)`), `wait_job_run_after` (line 155), `emit_result`.
- Produces: `verify_gold(catalog: str, result: MedallionResult) -> None` and a `gold` section in the emitted JSON report.

- [ ] **Step 1: Implement the phase**

After the existing silver verification in `_cmd_run`:

1. **Do not launch gold.** Record `gold_wait_start_ms`, then `wait_job_run_after("de_assessment_gold_aggregations", <silver-wave-completion ts>)` — the run's existence is the proof the table-update trigger fired.
2. **Converge, then assert** (the debounce means a mid-wave run may not be the last): poll up to 10 minutes until `verify_gold` passes with no pending gold run; each poll recomputes from CURRENT silver, so timing can delay convergence but never produce a false pass.
3. `verify_gold(catalog, result)` invariants (state-vs-data, all via SQL against live tables; the qualifying rule restated literally here, independent of the deployed files):
   - `sales_by_product` full-outer-join diff vs `SELECT product_id, COUNT(o.order_id), SUM(o.total_amount) FROM silver.products p LEFT JOIN (SELECT * FROM silver.orders WHERE order_status='Completed' AND NOT _is_orphan AND NOT _is_deleted) o ON ... WHERE NOT p._is_deleted GROUP BY ...` → differing-row count 0; same for `revenue_by_customer` (+ `lifetime_value_actual = total_revenue` and `last_order_date` equality).
   - trends: `SUM(total_revenue)` equals the qualifying sum; row count equals `COUNT(DISTINCT order_date)` over qualifying orders.
   - segmentation: `SUM(customer_count)` equals `COUNT(*)` of `revenue_by_customer`; per-segment recompute (same CASE ladder inlined) diff 0; all four segments present.
   - manifest: exactly ≥1 `layer='gold'` success row with `started_at` in this execution's window; `rows_read` equals current silver orders count is NOT asserted (a mid-wave run may have read fewer) — assert the LATEST gold row's `rows_read` equals the current count.
   - record into the report: gold run ids observed, per-table row counts, qualifying/pending/cancelled/orphan/deleted breakdown, and each invariant's result.

- [ ] **Step 2: Full E2E — the single paid run**

Run: `bash scripts/run-medallion-e2e-ce.sh` (background; ~25–35 min with the gold phase).
Expected: report `status: success`, silver invariants unchanged (regression check), gold section all-green with a trigger-launched run. If it aborts, the report still emits (`status: aborted`) — diagnose from the report + `ops.pipeline_manifest` before any rerun; each rerun costs real money.

- [ ] **Step 3: Commit**

```bash
git add scripts/medallion_e2e.py
git commit -m "test(e2e): gold phase — trigger-launched run, converge-then-assert against live silver"
```

---

### Task 7: Documentation truth pass + prompt history

**Files:**
- Modify: `design-notes.md` (gold section: as-built summary — semantic contract, recompute decision incl. the per-aggregation eligibility table, trigger choice; replace the "Gold obligations" section with "delivered" pointers)
- Modify: `data-model.md` (four gold tables, columns, grains, the qualifying-orders rule, the ladder with pinned constants)
- Modify: `README.md` (repo map gains `databricks/jobs/gold/`; template-mapping table gains `src/gold/01…04.sql` + `create_gold_tables.py` ↔ `databricks/jobs/gold/src/gold/sql/*` + `run_gold.py`; quick-start gains the gold job + test command)
- Modify: `acceptance-criteria.md` (gold rows → met, with evidence: contract tests, E2E report, manifest)
- Modify: `test-strategy.md` (scenario matrix gains the gold rows: status exclusion, orphan exclusion, deleted exclusion, zero-activity, segment reachability, cross-footing, idempotent rerun)
- Modify: `data-quality-strategy.md` (one short section: gold consumes flags, does not re-validate; the excluded-rows breakdown in the manifest is the observability)
- Create: entries in `ai-prompts/06-gold-aggregations.md` (see below)
- Modify: `ai-prompts/README.md` (06 row: drop "*(next phase)*")

**Prompt-history entries for 06 (8-field format per `ai-prompts/README.md`, first-person prompts, one entry per decision):**
1. Compute model — per-aggregation incremental eligibility demanded, analysis run, full recompute chosen (the append-only-contract catch killed the "cleanest" incremental case).
2. Storage & shape — tables over MVs; SQL files as executed source (the "major wins or SQL" rule).
3. Revenue semantics — the doc-silence finding, Completed-only global rule, `total_amount` as the record, the shared-view mechanism.
4. Trigger topology — staleness/cost framing, ANY_UPDATED + debounce over ALL_UPDATED/cron/most-frequent-source.
5. Segment ladder — recency-first precedence, data-anchored as_of (with the measured future-date incident: one bad row moved the anchor ten months), pinned thresholds from the measured distribution.
6. Cross-footing by construction — segmentation derived from revenue_by_customer.
Validation fields cite: contract suite, `run_job_tests.sh --all --forbid-skips` counts, the E2E report numbers.

- [ ] **Step 1: Make all documentation edits** (respect each file's existing voice/structure; no evaluator language, no history-about-the-history entries).
- [ ] **Step 2: Sweep** — `grep -ri` the diff for tool/employer references (per Global Constraints) before committing.
- [ ] **Step 3: Run the full local suite once more** (`run_job_tests.sh --all --forbid-skips`) to keep the "all green" claims honest.
- [ ] **Step 4: Commit**

```bash
git add design-notes.md data-model.md README.md acceptance-criteria.md test-strategy.md data-quality-strategy.md ai-prompts/
git commit -m "docs(gold): truth pass — as-built design, data model, prompt history 06"
```

---

## Completion

After Task 7: push the branch (`GH_TOKEN=$(gh auth token -u JGirulkar) git push -u origin cursor/gold-layer`) and STOP — the PR is raised only after review of the full diff, per the established gate.
