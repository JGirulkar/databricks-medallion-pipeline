# Bronze Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an append-only Databricks Bronze layer that bootstraps UC objects, ingests full-snapshot and incremental CSV files through Auto Loader, preserves intentional DQ issues, and records auditable batch manifests.

**Architecture:** An idempotent bootstrap job creates `de_assessment.{bronze,landing,ops}`, managed volumes, five Delta tables, and three `source_config` seed rows. Thin source entrypoints call one shared ingestion library; Auto Loader checkpoints prevent file replays, while every discovered row is appended with batch metadata and CDF enabled for downstream Silver processing.

**Tech Stack:** Python 3.11+, PySpark, Delta Lake, Databricks Auto Loader, Unity Catalog managed volumes, Declarative Automation Bundles, uv, pytest, Ruff.

## Global Constraints

- Databricks profile is **only** `de-assessment-ce`; source `scripts/env.sh` and pass `--profile de-assessment-ce` on every CLI command.
- Databricks Free Edition uses serverless tasks; omit `new_cluster`, `job_cluster_key`, and `existing_cluster_id`.
- Bronze is append-only: no business-key merge, deduplication, DQ filtering, quarantine, I/U/D stamps, or deletes.
- Source schemas match `docs/ASSESSMENT_FROM_PDF.md`; malformed values land in `_rescued_data`.
- `batch_id` is one UUID per ingest run and links every landed row to one manifest row.
- Auto Loader checkpoints provide file-level replay protection.
- CDF is enabled on `customers`, `orders`, and `products`.
- UC catalog/volume DDL and `cloudFiles` are cluster-tested because plain local Spark cannot execute them.
- No new runtime dependency is needed; use PySpark/Delta APIs already supplied locally and by Databricks Runtime.
- Use red → green → refactor for each behavior; skipped unit or Spark tests are defects.
- Do not auto-deploy on merge; CI validates, while deployment remains local or `workflow_dispatch`.

---

## File Map

### Create

- `databricks/jobs/bronze/pyproject.toml` — bronze package/test metadata.
- `databricks/jobs/bronze/src/bronze/__init__.py` — public package boundary.
- `databricks/jobs/bronze/src/bronze/config.py` — constants, `SourceConfig`, FQN builders, runtime table lookup.
- `databricks/jobs/bronze/src/bronze/schemas.py` — source and table `StructType` definitions.
- `databricks/jobs/bronze/src/bronze/metadata.py` — shared ingest metadata and customer hash.
- `databricks/jobs/bronze/src/bronze/bootstrap.py` — UC/Delta DDL, seed rows, volume directory creation.
- `databricks/jobs/bronze/src/bronze/manifest.py` — manifest model, Delta-version lookup, append.
- `databricks/jobs/bronze/src/bronze/ingest.py` — Auto Loader options, append-only `foreachBatch`, metrics, archive.
- `databricks/jobs/bronze/src/bronze/main.py` — active Spark resolution and source runner.
- `databricks/jobs/bronze/src/bootstrap_bronze.py` — bootstrap entrypoint.
- `databricks/jobs/bronze/src/ingest_customers.py` — customers entrypoint.
- `databricks/jobs/bronze/src/ingest_orders.py` — orders entrypoint.
- `databricks/jobs/bronze/src/ingest_products.py` — products entrypoint.
- `databricks/jobs/bronze/tests/test_config.py` — source config contracts and negative cases.
- `databricks/jobs/bronze/tests/test_schemas.py` — exact assessment schema contracts.
- `databricks/jobs/bronze/tests/test_metadata.py` — metadata/hash behavior and duplicate preservation.
- `databricks/jobs/bronze/tests/test_bootstrap.py` — generated DDL and non-destructive seed contract.
- `databricks/jobs/bronze/tests/test_manifest.py` — manifest validation and row shape.
- `databricks/jobs/bronze/tests/test_ingest.py` — local append-batch behavior.
- `databricks/bundle/resources/bronze.job.yml` — five serverless Bronze jobs and paused schedules.

### Modify

- `databricks/pyproject.toml` — add Bronze workspace member.
- `databricks/jobs/bronze/src/ingest_all.py` — replace placeholder with manual orchestrator.
- `databricks/bundle/databricks.yml` — include resource YAML, set catalog, remove old Bronze job.
- `database/schema.sql` — document three-level Bronze tables, metadata, config, and manifest.
- `.github/workflows/validate.yml` — execute Bronze unit tests.
- `docs/SETUP.md` — bootstrap/deploy/run instructions.
- `cursor-workflow/task-breakdown.md` — mark Bronze complete only after cluster evidence.
- `ai-prompts/04-bronze-layer.md` — assessment P-entries with Accepted/Changed/Rejected/Why.

---

### Task 1: Bronze Package and Runtime Configuration

**Files:**
- Create: `databricks/jobs/bronze/pyproject.toml`
- Create: `databricks/jobs/bronze/src/bronze/__init__.py`
- Create: `databricks/jobs/bronze/src/bronze/config.py`
- Create: `databricks/jobs/bronze/tests/test_config.py`
- Modify: `databricks/pyproject.toml`

**Interfaces:**
- Produces: `DeliveryPattern`, `SourceConfig`, `bronze_table(name, catalog)`, `source_config_table(catalog)`, `manifest_table(catalog)`, `get_source_config(spark, source_name, catalog)`.
- Consumes: a Spark table at `<catalog>.bronze.source_config`.

- [ ] **Step 1: Add Bronze as a uv workspace package**

Create `databricks/jobs/bronze/pyproject.toml`:

```toml
[project]
name = "bronze-layer"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = []

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/bronze"]
```

Change the root workspace:

```toml
[tool.uv.workspace]
members = ["jobs/data_generation", "jobs/bronze"]
```

- [ ] **Step 2: Write failing pure-Python config tests**

```python
import pytest

from bronze.config import (
    SourceConfig,
    bronze_table,
    manifest_table,
    source_config_table,
)


pytestmark = pytest.mark.unit


def test_fqn_helpers_use_three_level_uc_names() -> None:
    assert bronze_table("customers") == "de_assessment.bronze.customers"
    assert source_config_table() == "de_assessment.bronze.source_config"
    assert manifest_table() == "de_assessment.bronze.ingest_manifest"


def test_source_config_rejects_unknown_delivery_pattern() -> None:
    with pytest.raises(ValueError, match="delivery_pattern"):
        SourceConfig(
            source_name="orders",
            target_table="de_assessment.bronze.orders",
            raw_path="/Volumes/de_assessment/landing/raw/orders/incoming/",
            checkpoint_path="/Volumes/de_assessment/ops/checkpoints/orders/",
            schema_hint_path="/Volumes/de_assessment/ops/checkpoints/orders/_schema/",
            archive_path=None,
            file_format="csv",
            delivery_pattern="merge",
            cdf_enabled=True,
            schedule_hint="on_arrival",
            is_active=True,
        )
```

- [ ] **Step 3: Run tests and confirm red**

Run:

```bash
source scripts/env.sh
cd databricks
uv sync --all-packages --all-groups --no-group cluster
uv run pytest jobs/bronze/tests/test_config.py -q
```

Expected: collection fails with `ModuleNotFoundError: No module named 'bronze'`.

- [ ] **Step 4: Implement the immutable config contract**

Implement:

```python
from dataclasses import dataclass
from typing import Literal, cast

from pyspark.sql import SparkSession, functions as F

DEFAULT_CATALOG = "de_assessment"
BRONZE_SCHEMA = "bronze"
LANDING_SCHEMA = "landing"
OPS_SCHEMA = "ops"
SOURCE_CONFIG_TABLE_NAME = "source_config"
MANIFEST_TABLE_NAME = "ingest_manifest"

DeliveryPattern = Literal["full_snapshot", "incremental"]
_DELIVERY_PATTERNS = {"full_snapshot", "incremental"}


def bronze_table(name: str, catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{BRONZE_SCHEMA}.{name}"


def source_config_table(catalog: str = DEFAULT_CATALOG) -> str:
    return bronze_table(SOURCE_CONFIG_TABLE_NAME, catalog)


def manifest_table(catalog: str = DEFAULT_CATALOG) -> str:
    return bronze_table(MANIFEST_TABLE_NAME, catalog)


@dataclass(frozen=True)
class SourceConfig:
    source_name: str
    target_table: str
    raw_path: str
    checkpoint_path: str
    schema_hint_path: str
    archive_path: str | None
    file_format: str
    delivery_pattern: DeliveryPattern
    cdf_enabled: bool
    schedule_hint: str
    is_active: bool

    def __post_init__(self) -> None:
        if self.delivery_pattern not in _DELIVERY_PATTERNS:
            raise ValueError(f"Invalid delivery_pattern: {self.delivery_pattern}")
        if not self.raw_path.startswith("/Volumes/"):
            raise ValueError(f"raw_path must be a UC Volume path: {self.raw_path}")

    @classmethod
    def from_row(cls, row: object) -> "SourceConfig":
        values = row.asDict(recursive=True)  # type: ignore[attr-defined]
        return cls(
            **{
                key: cast(object, values[key])
                for key in cls.__dataclass_fields__
            }
        )


def get_source_config(
    spark: SparkSession,
    source_name: str,
    catalog: str = DEFAULT_CATALOG,
) -> SourceConfig:
    rows = (
        spark.table(source_config_table(catalog))
        .where((F.col("source_name") == source_name) & F.col("is_active"))
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            f"Expected one active source_config for {source_name!r}; found {len(rows)}"
        )
    return SourceConfig.from_row(rows[0])
```

- [ ] **Step 5: Add negative Spark lookup tests**

Use a temporary view named with a test catalog parameter or monkeypatch
`source_config_table`; verify zero and two active rows both raise the exact
`Expected one active source_config` error.

- [ ] **Step 6: Run config tests and lint**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_config.py -q
uv run ruff check jobs/bronze
```

Expected: all config tests pass and Ruff exits 0.

- [ ] **Step 7: Commit checkpoint**

```bash
git add databricks/pyproject.toml databricks/jobs/bronze
git commit -m "feat(bronze): add source configuration contracts"
```

---

### Task 2: Typed Source Schemas and Shared Metadata

**Files:**
- Create: `databricks/jobs/bronze/src/bronze/schemas.py`
- Create: `databricks/jobs/bronze/src/bronze/metadata.py`
- Create: `databricks/jobs/bronze/tests/test_schemas.py`
- Create: `databricks/jobs/bronze/tests/test_metadata.py`

**Interfaces:**
- Produces: `source_schema(source_name) -> StructType`, `table_schema(source_name) -> StructType`, `add_ingest_metadata(df, config, batch_id, ingest_timestamp) -> DataFrame`.
- Consumes: `SourceConfig` from Task 1.

- [ ] **Step 1: Write exact-schema tests first**

Assert each assessment field in order and type:

```python
@pytest.mark.unit
@pytest.mark.parametrize(
    ("source_name", "field_names"),
    [
        (
            "customers",
            [
                "customer_id",
                "customer_name",
                "email",
                "country",
                "signup_date",
                "customer_segment",
                "lifetime_value",
            ],
        ),
        (
            "orders",
            [
                "order_id",
                "customer_id",
                "order_date",
                "product_id",
                "quantity",
                "unit_price",
                "total_amount",
                "order_status",
                "payment_date",
            ],
        ),
        (
            "products",
            [
                "product_id",
                "product_name",
                "category",
                "price",
                "cost",
                "stock_quantity",
                "reorder_level",
            ],
        ),
    ],
)
def test_source_schema_matches_assessment(source_name, field_names) -> None:
    assert source_schema(source_name).fieldNames() == field_names
```

Add an unknown-source negative test expecting `ValueError("Unknown source")`.

- [ ] **Step 2: Run schema tests and confirm red**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_schemas.py -q
```

Expected: import fails because `bronze.schemas` does not exist.

- [ ] **Step 3: Implement source and table schemas**

Use explicit nullable `StructField`s, `DecimalType(18, 2)`, `DateType`, and:

```python
COMMON_METADATA_FIELDS = [
    StructField("_ingest_timestamp", TimestampType(), False),
    StructField("_source_file", StringType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("_delivery_pattern", StringType(), False),
    StructField("_rescued_data", StringType(), True),
]
```

Add `_row_hash STRING` only to customers. `source_schema()` excludes all
metadata; `table_schema()` appends metadata.

- [ ] **Step 4: Write failing metadata and duplicate-preservation tests**

Create a customers DataFrame containing null email values and two rows with the
same `customer_id`. After `add_ingest_metadata`:

```python
assert result.count() == 2
assert result.select("customer_id").distinct().count() == 1
assert result.where(F.col("email").isNull()).count() == 1
assert result.select("_batch_id").first()[0] == "batch-123"
assert result.select("_delivery_pattern").first()[0] == "full_snapshot"
assert result.where(F.col("_row_hash").isNull()).count() == 0
```

Add a stability test: reordering input columns produces the same customer hash.

- [ ] **Step 5: Implement metadata in one shared function**

Use `_metadata.file_path` when present, with a test-only `source_file` override:

```python
CUSTOMER_HASH_COLUMNS = (
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
)


def add_ingest_metadata(
    df: DataFrame,
    config: SourceConfig,
    batch_id: str,
    ingest_timestamp: datetime,
    source_file_column: Column | None = None,
) -> DataFrame:
    source_file = source_file_column or F.col("_metadata.file_path")
    result = (
        df.withColumn("_ingest_timestamp", F.lit(ingest_timestamp))
        .withColumn("_source_file", source_file)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_delivery_pattern", F.lit(config.delivery_pattern))
    )
    if config.source_name == "customers":
        canonical = [
            F.coalesce(F.col(name).cast("string"), F.lit(""))
            for name in CUSTOMER_HASH_COLUMNS
        ]
        result = result.withColumn(
            "_row_hash",
            F.sha2(F.concat_ws("||", *canonical), 256),
        )
    return result
```

- [ ] **Step 6: Run unit and Spark tests**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_schemas.py -q
uv run pytest jobs/bronze/tests/test_metadata.py -m spark -q
```

Expected: all pass; duplicate IDs remain duplicated.

- [ ] **Step 7: Commit checkpoint**

```bash
git add databricks/jobs/bronze/src/bronze databricks/jobs/bronze/tests
git commit -m "feat(bronze): define typed schemas and ingest metadata"
```

---

### Task 3: Idempotent UC Bootstrap and Source Seeds

**Files:**
- Create: `databricks/jobs/bronze/src/bronze/bootstrap.py`
- Create: `databricks/jobs/bronze/src/bootstrap_bronze.py`
- Create: `databricks/jobs/bronze/tests/test_bootstrap.py`

**Interfaces:**
- Produces: `bootstrap(spark, mkdirs, catalog) -> None`, `bootstrap_ddl(catalog) -> tuple[str, ...]`, `source_seed_rows(catalog) -> tuple[dict, ...]`.
- Consumes: Task 1 FQN helpers and Task 2 table schemas.

- [ ] **Step 1: Write failing DDL contract tests**

Verify generated statements include:

```python
assert "CREATE CATALOG IF NOT EXISTS de_assessment" in ddl
assert "CREATE VOLUME IF NOT EXISTS de_assessment.landing.raw" in ddl
assert "CREATE VOLUME IF NOT EXISTS de_assessment.ops.checkpoints" in ddl
assert "delta.enableChangeDataFeed' = 'true" in ddl
assert "MERGE INTO" not in "\n".join(bootstrap_ddl())
```

Verify seed rows are exactly three and every row has `delivery_pattern` in
`{"full_snapshot", "incremental"}`. Verify the seed merge SQL uses
`WHEN NOT MATCHED THEN INSERT` and has no `WHEN MATCHED THEN UPDATE`, preserving
manual runtime tweaks.

- [ ] **Step 2: Run bootstrap tests and confirm red**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_bootstrap.py -q
```

Expected: import fails because `bronze.bootstrap` does not exist.

- [ ] **Step 3: Implement DDL builders**

Generate idempotent DDL for:

1. catalog `de_assessment`;
2. schemas `bronze`, `landing`, `ops`;
3. managed volumes `landing.raw`, `ops.checkpoints`;
4. entity tables from Task 2 table schemas;
5. `source_config`;
6. `ingest_manifest`.

`source_config.updated_at` uses:

```sql
updated_at TIMESTAMP DEFAULT current_timestamp()
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
```

All entity tables use:

```sql
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

- [ ] **Step 4: Implement non-destructive source seeding**

Create a DataFrame from `source_seed_rows()` and a temp view, then execute:

```sql
MERGE INTO de_assessment.bronze.source_config AS target
USING bronze_source_seed AS source
ON target.source_name = source.source_name
WHEN NOT MATCHED THEN INSERT (
  source_name, target_table, raw_path, checkpoint_path,
  schema_hint_path, archive_path, file_format, delivery_pattern,
  cdf_enabled, schedule_hint, is_active
) VALUES (
  source.source_name, source.target_table, source.raw_path,
  source.checkpoint_path, source.schema_hint_path, source.archive_path,
  source.file_format, source.delivery_pattern, source.cdf_enabled,
  source.schedule_hint, source.is_active
)
```

- [ ] **Step 5: Implement directory creation injection**

`bootstrap()` calls the supplied `mkdirs(path)` for:

```python
(
    "/Volumes/de_assessment/landing/raw/products/",
    "/Volumes/de_assessment/landing/raw/customers/",
    "/Volumes/de_assessment/landing/raw/orders/incoming/",
    "/Volumes/de_assessment/landing/raw/orders/processed/",
    "/Volumes/de_assessment/ops/checkpoints/products/_schema/",
    "/Volumes/de_assessment/ops/checkpoints/customers/_schema/",
    "/Volumes/de_assessment/ops/checkpoints/orders/_schema/",
)
```

The entrypoint obtains active Spark and calls `DBUtils(spark).fs.mkdirs`.

- [ ] **Step 6: Run tests and lint**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_bootstrap.py -q
uv run ruff check jobs/bronze
```

Expected: tests pass; no UC connection is needed locally.

- [ ] **Step 7: Commit checkpoint**

```bash
git add databricks/jobs/bronze
git commit -m "feat(bronze): add idempotent unity catalog bootstrap"
```

---

### Task 4: Manifest Contract and Delta Version Bounds

**Files:**
- Create: `databricks/jobs/bronze/src/bronze/manifest.py`
- Create: `databricks/jobs/bronze/tests/test_manifest.py`

**Interfaces:**
- Produces: `ManifestRecord`, `current_delta_version(spark, table) -> int | None`, `append_manifest(spark, record, catalog) -> None`.
- Consumes: `manifest_table()` from Task 1.

- [ ] **Step 1: Write failing validation tests**

Test that an empty `batch_id`, unknown status, and completed success without
`completed_at` each raise `ValueError`. Test `as_row()` returns exactly the
manifest DDL column order.

- [ ] **Step 2: Confirm red**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_manifest.py -q
```

Expected: import fails because `bronze.manifest` does not exist.

- [ ] **Step 3: Implement manifest model**

```python
@dataclass(frozen=True)
class ManifestRecord:
    batch_id: str
    source_name: str
    delivery_pattern: str
    source_path: str
    files_processed: int
    rows_read: int
    rows_written: int
    rows_rescued: int
    delta_version_before: int | None
    delta_version_after: int | None
    started_at: datetime
    completed_at: datetime
    status: Literal["success", "failed"]
    error_message: str | None

    def __post_init__(self) -> None:
        if not self.batch_id:
            raise ValueError("batch_id must not be empty")
        if self.status not in {"success", "failed"}:
            raise ValueError(f"Invalid status: {self.status}")
```

`append_manifest()` creates one-row DataFrame with an explicit `StructType` and
uses `.mode("append").saveAsTable(manifest_table(catalog))`.

- [ ] **Step 4: Implement version lookup**

Use:

```python
rows = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1").select("version").collect()
return int(rows[0].version) if rows else None
```

Do not swallow `TABLE_OR_VIEW_NOT_FOUND`; bootstrap is a required dependency.

- [ ] **Step 5: Run tests and commit**

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_manifest.py -q
git add databricks/jobs/bronze
git commit -m "feat(bronze): add batch manifest contract"
```

---

### Task 5: Append-Only Batch Writer

**Files:**
- Create: `databricks/jobs/bronze/src/bronze/ingest.py`
- Create: `databricks/jobs/bronze/tests/test_ingest.py`

**Interfaces:**
- Produces: `BatchMetrics`, `append_batch(df, config) -> BatchMetrics`, `cloudfiles_options(config) -> dict[str, str]`.
- Consumes: Task 1 `SourceConfig`; Task 2 metadata-enriched DataFrames.

- [ ] **Step 1: Write failing local Spark behavior tests**

Test `append_batch` twice with distinct DataFrames containing duplicate
`order_id=1`. Assert all rows remain:

```python
append_batch(first_batch, orders_config)
append_batch(second_batch, orders_config)
assert spark.table(orders_config.target_table).count() == 4
assert (
    spark.table(orders_config.target_table)
    .where("order_id = 1")
    .count()
    == 4
)
```

Add an empty-batch test expecting zero metrics and no write. Add an option test:

```python
assert cloudfiles_options(config) == {
    "cloudFiles.format": "csv",
    "cloudFiles.schemaLocation": config.schema_hint_path,
    "cloudFiles.inferColumnTypes": "false",
    "rescuedDataColumn": "_rescued_data",
}
```

- [ ] **Step 2: Confirm red**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_ingest.py -m spark -q
```

Expected: import fails because `bronze.ingest` does not exist.

- [ ] **Step 3: Implement cached append and metrics**

```python
@dataclass(frozen=True)
class BatchMetrics:
    files: frozenset[str]
    rows_read: int
    rows_written: int
    rows_rescued: int


def append_batch(df: DataFrame, config: SourceConfig) -> BatchMetrics:
    cached = df.persist()
    try:
        rows_read = cached.count()
        if rows_read == 0:
            return BatchMetrics(frozenset(), 0, 0, 0)
        files = frozenset(
            row._source_file
            for row in cached.select("_source_file").distinct().collect()
        )
        rows_rescued = cached.where(F.col("_rescued_data").isNotNull()).count()
        (
            cached.write.format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .saveAsTable(config.target_table)
        )
        return BatchMetrics(files, rows_read, rows_read, rows_rescued)
    finally:
        cached.unpersist()
```

- [ ] **Step 4: Run Spark tests and commit**

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_ingest.py -m spark -q
git add databricks/jobs/bronze
git commit -m "feat(bronze): append discovered batches without deduplication"
```

---

### Task 6: Auto Loader Run Orchestration

**Files:**
- Modify: `databricks/jobs/bronze/src/bronze/ingest.py`
- Create: `databricks/jobs/bronze/src/bronze/main.py`
- Modify: `databricks/jobs/bronze/tests/test_ingest.py`

**Interfaces:**
- Produces: `run_ingest(spark, config, archive_file, batch_id_factory, clock) -> ManifestRecord`, `run_source(source_name, catalog) -> None`.
- Consumes: Tasks 1–5.

- [ ] **Step 1: Write failing orchestration tests with injected collaborators**

Avoid local `cloudFiles`. Inject a `stream_runner(callback) -> None` fake that
calls the callback with two local DataFrames. Assert:

- one `batch_id` is applied to both micro-batches;
- returned manifest aggregates files and counts;
- failure returns/writes a failed manifest and re-raises;
- no-data run returns success with zero counts;
- archive is called only after successful completion.

- [ ] **Step 2: Run and confirm red**

Run:

```bash
cd databricks
uv run pytest jobs/bronze/tests/test_ingest.py -q
```

Expected: `run_ingest` import or signature failure.

- [ ] **Step 3: Implement the real Auto Loader stream**

Construct:

```python
raw = (
    spark.readStream.format("cloudFiles")
    .options(**cloudfiles_options(config))
    .schema(source_schema(config.source_name))
    .load(config.raw_path)
)
enriched = add_ingest_metadata(
    raw,
    config,
    batch_id=batch_id,
    ingest_timestamp=started_at,
)
query = (
    enriched.writeStream.foreachBatch(write_micro_batch)
    .option("checkpointLocation", config.checkpoint_path)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()
```

The callback accumulates `BatchMetrics` on the driver. After termination,
append exactly one success manifest. In `except Exception`, append a failed
manifest with the same `batch_id`, then re-raise.

- [ ] **Step 4: Implement archive behavior**

For `archive_path is not None`, move each successfully processed source file to:

```python
destination = f"{config.archive_path.rstrip('/')}/{Path(source).name}"
archive_file(source, destination)
```

Use `DBUtils(spark).fs.mv` only in the runtime adapter. Keep all file moves out
of the core batch writer.

- [ ] **Step 5: Implement active Spark resolution**

```python
def active_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise ValueError("No active Spark session")
    return spark
```

`run_source()` loads config, constructs DBUtils adapters, and calls
`run_ingest()`. Never call `sys.exit()`.

- [ ] **Step 6: Run full Bronze tests**

```bash
source scripts/env.sh
./databricks/scripts/run_job_tests.sh bronze -m "unit or spark"
```

Expected: all tests pass; no skip.

- [ ] **Step 7: Commit checkpoint**

```bash
git add databricks/jobs/bronze
git commit -m "feat(bronze): orchestrate available-now autoloader runs"
```

---

### Task 7: Thin Entry Points and Manual Orchestrator

**Files:**
- Create: `databricks/jobs/bronze/src/ingest_customers.py`
- Create: `databricks/jobs/bronze/src/ingest_orders.py`
- Create: `databricks/jobs/bronze/src/ingest_products.py`
- Modify: `databricks/jobs/bronze/src/ingest_all.py`
- Modify: `databricks/jobs/bronze/src/bootstrap_bronze.py`

**Interfaces:**
- Produces: Databricks `spark_python_task` scripts.
- Consumes: `run_source()` and `bootstrap()`.

- [ ] **Step 1: Add a shared catalog argument parser**

In `bronze/main.py`:

```python
def parse_catalog(argv: Sequence[str] | None = None) -> str:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    return parser.parse_args(argv).catalog
```

Test default and explicit `--catalog`.

- [ ] **Step 2: Implement source entrypoints**

Each file contains only:

```python
from bronze.main import parse_catalog, run_source


def main() -> None:
    run_source("customers", parse_catalog())


if __name__ == "__main__":
    main()
```

Use the matching source name for orders/products.

- [ ] **Step 3: Replace the placeholder orchestrator**

`ingest_all.py` runs `run_source` in deterministic dependency order:

```python
for source_name in ("products", "customers", "orders"):
    run_source(source_name, catalog)
```

It does **not** call bootstrap implicitly; deployment operations run bootstrap
as an explicit job first.

- [ ] **Step 4: Run tests and Ruff**

```bash
cd databricks
uv run pytest jobs/bronze/tests -m unit -q
uv run ruff check jobs/bronze
```

Expected: pass.

- [ ] **Step 5: Commit checkpoint**

```bash
git add databricks/jobs/bronze
git commit -m "feat(bronze): add source job entrypoints"
```

---

### Task 8: Serverless Bundle Resources

**Files:**
- Create: `databricks/bundle/resources/bronze.job.yml`
- Modify: `databricks/bundle/databricks.yml`

**Interfaces:**
- Produces: `job_bronze_bootstrap`, `job_bronze_ingest_products`, `job_bronze_ingest_customers`, `job_bronze_ingest_orders`, `job_bronze_ingest_all`.
- Consumes: Task 7 scripts and `${var.catalog}`.

- [ ] **Step 1: Split Bronze resources from the root bundle**

Add to `databricks.yml`:

```yaml
include:
  - resources/*.yml

variables:
  catalog:
    default: de_assessment
```

Remove `job_bronze_ingest` and its `new_cluster` block. Leave unrelated jobs
unchanged in this task.

- [ ] **Step 2: Add serverless Bronze jobs**

Use no compute stanza:

```yaml
resources:
  jobs:
    job_bronze_bootstrap:
      name: de_assessment_bronze_bootstrap
      max_concurrent_runs: 1
      tasks:
        - task_key: bootstrap
          spark_python_task:
            python_file: ../../jobs/bronze/src/bootstrap_bronze.py
            parameters: ["--catalog", "${var.catalog}"]

    job_bronze_ingest_customers:
      name: de_assessment_bronze_customers
      max_concurrent_runs: 1
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
        pause_status: PAUSED
      tasks:
        - task_key: ingest_customers
          spark_python_task:
            python_file: ../../jobs/bronze/src/ingest_customers.py
            parameters: ["--catalog", "${var.catalog}"]
```

Add:

- products weekly: `0 0 6 ? * MON`, paused;
- orders file-arrival trigger on
  `/Volumes/de_assessment/landing/raw/orders/incoming/`, paused;
- manual `ingest_all`, no schedule/trigger.

All schedules start `PAUSED` to avoid CE quota consumption before test data and
acceptance validation are ready.

- [ ] **Step 3: Validate bundle structure**

Run:

```bash
source scripts/env.sh
cd databricks/bundle
databricks bundle validate --strict -t dev --profile de-assessment-ce
```

Expected: successful validation listing all five Bronze resources. If CLI
`v0.261.0` rejects a serverless field or strict mode, inspect
`databricks bundle schema --profile de-assessment-ce`; do not guess YAML keys.

- [ ] **Step 4: Commit checkpoint**

```bash
git add databricks/bundle
git commit -m "feat(bronze): define serverless bundle jobs"
```

---

### Task 9: Schema Documentation and CI Validation

**Files:**
- Modify: `database/schema.sql`
- Modify: `.github/workflows/validate.yml`
- Modify: `docs/SETUP.md`
- Modify: `cursor-workflow/task-breakdown.md`
- Create: `ai-prompts/04-bronze-layer.md`

**Interfaces:**
- Produces: evaluator-facing DDL, reproducible CI, setup instructions, prompt history.

- [ ] **Step 1: Expand logical schema documentation**

Document:

- all three `de_assessment.bronze.*` entity tables and metadata;
- CDF properties;
- `source_config` with `delivery_pattern`;
- `ingest_manifest` with `batch_id`;
- note that executable DDL remains in `bronze/bootstrap.py`.

- [ ] **Step 2: Add Bronze unit tests to CI**

Change the workflow test command to:

```yaml
- name: Unit tests
  run: |
    cd databricks
    uv sync --all-packages --all-groups --no-group cluster
    uv run pytest jobs/data_generation/tests/ jobs/bronze/tests/ -m unit -q
```

Do not add merge-triggered deployment.

- [ ] **Step 3: Document exact local and CE workflow**

Add:

```bash
source scripts/env.sh
./databricks/scripts/run_job_tests.sh bronze -m "unit or spark"
cd databricks/bundle
databricks bundle validate --strict -t dev --profile de-assessment-ce
databricks bundle deploy -t dev --profile de-assessment-ce
databricks bundle run job_bronze_bootstrap -t dev --profile de-assessment-ce
```

- [ ] **Step 4: Create assessment prompt history**

`ai-prompts/04-bronze-layer.md` must include P-entries for:

- append-only correction after duplicate-key conflict;
- source config table/module split;
- TDD failures and fixes;
- bundle validation/deploy;
- CE bootstrap and checkpoint replay;

Each entry includes Prompt / AI response / Accepted / Changed / Rejected / Why.

- [ ] **Step 5: Run all local verification**

```bash
source scripts/env.sh
./databricks/scripts/run_job_tests.sh bronze -m "unit or spark"
./scripts/lint.sh
cd databricks/bundle
databricks bundle validate --strict -t dev --profile de-assessment-ce
```

Expected: tests, lint, and bundle validation all pass.

- [ ] **Step 6: Commit checkpoint**

```bash
git add database/schema.sql .github/workflows/validate.yml docs/SETUP.md \
  cursor-workflow/task-breakdown.md ai-prompts/04-bronze-layer.md
git commit -m "docs(bronze): document and validate bronze delivery"
```

---

### Task 10: CE Deploy, Bootstrap, and Cluster Evidence

**Files:**
- Modify after evidence: `ai-prompts/04-bronze-layer.md`
- Modify after sign-off: `cursor-workflow/task-breakdown.md`

**Interfaces:**
- Consumes: complete bundle and `de-assessment-ce`.
- Produces: deployed jobs, UC objects, run IDs, and acceptance evidence.

- [ ] **Step 1: Load required operational skills**

Use `databricks-core` → `databricks-dabs` / `databricks-jobs` and project
`deploy-ce-job`. Use AI Dev Kit MCP `databricks-de-assessment` for post-deploy
inspection when connected; otherwise use profile-pinned CLI/SQL.

- [ ] **Step 2: Deploy the validated bundle**

```bash
source scripts/env.sh
cd databricks/bundle
databricks bundle deploy -t dev --profile de-assessment-ce
```

Expected: deployment completes and creates five Bronze jobs.

- [ ] **Step 3: Run bootstrap twice**

```bash
databricks bundle run job_bronze_bootstrap -t dev --profile de-assessment-ce
databricks bundle run job_bronze_bootstrap -t dev --profile de-assessment-ce
```

Expected: both succeed; the second run creates no duplicate source config rows.

- [ ] **Step 4: Verify UC state with AI Dev Kit MCP or SQL**

Run equivalent queries:

```sql
SELECT source_name, delivery_pattern, is_active
FROM de_assessment.bronze.source_config
ORDER BY source_name;

SHOW TABLES IN de_assessment.bronze;
DESCRIBE DETAIL de_assessment.bronze.customers;
DESCRIBE DETAIL de_assessment.bronze.orders;
DESCRIBE DETAIL de_assessment.bronze.products;
```

Expected: three config rows, five tables, and CDF enabled on all entity tables.

- [ ] **Step 5: Execute cluster ingestion smoke when fixtures exist**

Place one assessment-shaped CSV per landing path, then:

```bash
databricks bundle run job_bronze_ingest_all -t dev --profile de-assessment-ce
```

Run it a second time without adding files. Expected: no duplicate entity rows;
manifest records the no-new-data run with zero rows, demonstrating checkpoint
replay protection.

- [ ] **Step 6: Verify preservation and manifest linkage**

```sql
SELECT batch_id, source_name, rows_read, rows_written, rows_rescued, status
FROM de_assessment.bronze.ingest_manifest
ORDER BY started_at DESC;

SELECT _batch_id, COUNT(*)
FROM de_assessment.bronze.orders
GROUP BY _batch_id;
```

Expected: every landed `_batch_id` has one matching success manifest record;
intentional duplicate business keys remain in Bronze.

- [ ] **Step 7: Run layer-completion gate**

Use `.cursor/skills/layer-completion/SKILL.md`. Record run IDs, row counts,
rescued counts, and any accepted deviations. Mark Bronze complete in
`cursor-workflow/task-breakdown.md` only after this gate passes.

- [ ] **Step 8: Commit evidence only when explicitly requested**

```bash
git add ai-prompts/04-bronze-layer.md cursor-workflow/task-breakdown.md
git commit -m "test(bronze): record ce bootstrap and ingest evidence"
```

---

## Plan Self-Review

- **Spec coverage:** bootstrap, managed volumes, append-only source ingestion,
  runtime source config, typed schemas, metadata/hash, manifest/version bounds,
  CDF, five jobs, schedules, TDD, CI, manual deploy, MCP verification, and
  assessment artifacts each map to a task.
- **Feasibility correction:** local Spark does not test UC Volumes or
  `cloudFiles`; Task 10 owns those CE checks.
- **Type consistency:** `delivery_pattern` replaces every prior write-strategy
  or merge-key field. `batch_id` is used by entity rows and manifests.
- **Scope:** no data generator, Silver transformations, Gold, dashboard,
  enterprise config framework, or auto-deploy-on-merge.
- **Placeholder scan:** implementation steps contain concrete file paths,
  signatures, commands, expected results, and negative cases.

