from __future__ import annotations

import datetime as dt
import importlib.util
import os
import pathlib
import sys
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql.types import StructType

# --------------------------------------------------------------------------
# Silver-builder wiring, shared by the contract tier (test_gold_contract.py)
# and any later drift-guard test that needs the same real silver tables.
#
# The gold job is uploaded to its own workspace directory and cannot import
# the silver package at runtime, but for local tests both live in the same
# checkout: reach across via sys.path, the same trick the silver contract
# test uses for the generator (test_pipeline_contract.py:35-42).
# --------------------------------------------------------------------------
_SILVER_SRC = pathlib.Path(__file__).resolve().parents[2] / "silver" / "src"
if str(_SILVER_SRC) not in sys.path:
    sys.path.insert(0, str(_SILVER_SRC))

_GEN = (
    pathlib.Path(__file__).resolve().parents[2]
    / "data_generation" / "src" / "generate_sample_data.py"
)


def load_generator():
    """Load the real generator module by file path (not an installed package)."""
    spec = importlib.util.spec_from_file_location("generate_sample_data", _GEN)
    assert spec and spec.loader
    gen = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(gen)
    return gen


# Schema-qualified test table names, distinct from the plain "rt_*" names
# test_runner.py uses, so the two fixture families never collide.
_SILVER_TABLES = {
    "customers": "gct_silver.customers",
    "products": "gct_silver.products",
    "orders": "gct_silver.orders",
}
_QUARANTINE_TABLE = "gct_silver.quarantine"
_GOLD_SCHEMA = "gct_gold"
_MANIFEST_TABLE = "gct_manifest"

# Generator sizing for the contract tier. ~10 orders/customer (the seed
# dataset's own ratio) so every segment in the ladder has members at small
# scale, including High-Value (lifetime qualifying revenue >= 5,000).
#
# BASE_PRODUCTS started at 60 (brief's suggested starting point) but at
# 1500 orders over 60 products (~25 orders/product) every product drew at
# least one qualifying order — test_zero_activity_rows_are_kept never saw a
# zero-sales product. Raised to 150 (same ~10 orders/product ratio as
# customers, which did produce zero-activity rows) so the zero case shows up
# reliably rather than by chance (verified empirically while sizing the fixture).
_GEN_BASE_CUSTOMERS = 150
_GEN_BASE_PRODUCTS = 150
_GEN_BASE_ORDERS = 1500

# DECIMAL(18,2) columns per entity. toPandas() hands these back as python
# decimal.Decimal (object dtype), which is exact but awkward to do pandas
# arithmetic on (sum()/groupby() over an object column is not guaranteed to
# use Decimal addition). These are money values with at most 2 decimal
# places and modest magnitude, well inside float64's ~15-17 significant
# digits, so converting once here — rather than scattered across every
# test — keeps the expectation math plain float arithmetic.
_DECIMAL_COLUMNS = {
    "customers": ("lifetime_value",),
    "products": ("price", "cost"),
    "orders": ("unit_price", "total_amount"),
}


def _silver_snapshot(spark, entity: str, table: str):
    frame = spark.table(table).toPandas()
    for column in _DECIMAL_COLUMNS.get(entity, ()):
        if column in frame.columns:
            frame[column] = frame[column].apply(
                lambda v: float(v) if v is not None else None
            )
    return frame


def _dq_schema(entity: str):
    """The seeded schema, with ref_table pointed at the local test tables.

    Copied from databricks/jobs/silver/tests/test_pipeline_contract.py's
    `_dq_schema` helper, with TABLES replaced by the schema-qualified names
    above.
    """
    from silver.bootstrap import _dq_schema_seeds
    from silver.config import DqSchema

    raw = _dq_schema_seeds("de_assessment")[entity]
    for check in raw["checks"]:
        if check.get("ref_table"):
            parent = check["ref_table"].rsplit(".", 1)[-1]
            check["ref_table"] = _SILVER_TABLES[parent]
    return DqSchema.from_dict(raw)


def _as_bronze_batch(spark, frame, entity: str, batch: str):
    """Shape a generated frame like a bronze CDF micro-batch.

    Copied from test_pipeline_contract.py:185-215.
    """
    from pyspark.sql import functions as F

    columns = list(frame.columns)
    int_columns = {
        c for c in columns
        if c.endswith("_id") or c in ("quantity", "stock_quantity", "reorder_level")
    }

    def cell(column: str, value: object) -> object:
        if value is None or (isinstance(value, float) and value != value):
            return None
        # pandas widens an integer column to float as soon as it holds a NaN,
        # so 50 arrives as 50.0 and Spark rejects it for an INT field.
        return int(value) if column in int_columns else value

    records = [
        tuple(cell(c, v) for c, v in zip(columns, rec))
        for rec in frame.itertuples(index=False, name=None)
    ]
    schema = ", ".join(
        f"{c} " + ("INT" if c in int_columns else "STRING") for c in columns
    )
    df = spark.createDataFrame(records, schema=schema)
    for numeric in ("price", "cost", "unit_price", "total_amount", "lifetime_value"):
        if numeric in columns:
            df = df.withColumn(numeric, F.col(numeric).cast("decimal(18,2)"))
    return (
        df.withColumn("_batch_id", F.lit(batch))
        .withColumn("_ingest_timestamp", F.lit(dt.datetime(2026, 1, 1, tzinfo=dt.UTC)))
        .withColumn("_change_type", F.lit("insert"))
    )


def _conform(spark, frame, entity: str, batch: str, run: str) -> dict:
    """Run the real silver path for one entity and one delivery.

    Copied from test_pipeline_contract.py:218-233.
    """
    from silver.checks import apply_entity_checks
    from silver.conform import (
        apply_snapshot_soft_deletes,
        merge_to_silver,
        split_validated_batch,
    )
    from silver.quarantine import write_quarantine
    from silver.validators import annotate_violations

    schema = _dq_schema(entity)
    tagged = apply_entity_checks(
        annotate_violations(_as_bronze_batch(spark, frame, entity, batch), schema),
        schema,
        spark,
    )
    survivors, passed, failed = split_validated_batch(tagged, entity)
    written = merge_to_silver(passed, entity, spark)
    deleted = apply_snapshot_soft_deletes(spark, entity, survivors)
    quarantined = write_quarantine(
        spark, failed, entity, run, dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    )
    return {"written": written, "quarantined": quarantined, "soft_deleted": deleted}


def _pin_pyspark_interpreter() -> None:
    """Force Spark workers onto this interpreter.

    Without this, workers resolve a bare `python3` from PATH. If that is a
    different minor version than the venv running the driver, every task that
    touches Python (any UDF, any Delta write of a Python-built DataFrame) dies
    with PYTHON_VERSION_MISMATCH — which reads like a Delta/Spark failure but
    is purely an environment mismatch.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="module")
def spark() -> Iterator[pytest.Any]:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _pin_pyspark_interpreter()

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("gold-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.default", "delta")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def create_delta_table(spark, table_name: str, schema: StructType) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    tmpdir = tempfile.mkdtemp()
    uri = Path(tmpdir).as_uri()
    spark.createDataFrame([], schema=schema).write.format("delta").mode(
        "overwrite"
    ).save(uri)
    spark.sql(f"CREATE TABLE {table_name} USING delta LOCATION '{uri}'")




@pytest.fixture(scope="module")
def silver_tables(spark) -> dict:
    """Real silver tables, built from real generator output via the real
    silver pipeline, then run through the real gold runner.

    Both deliveries run: the seed delivery (`generate_dataframes()`) and the
    delta delivery (`generate_delta_dataframes()` — updates, product
    soft-deletes, late-arriving parents that heal orphans), each followed by
    `refresh_orphan_flags(spark)`. This gives gold's qualifying_orders filter
    real `_is_orphan` / `_is_deleted` rows to filter, not hand-built stand-ins.

    Returns {"spark": spark, "silver": {entity: pandas snapshot}} — the
    silver snapshot for "orders" carries every business column (including
    quantity/unit_price) so tests can recompute independently in pandas.
    """
    from gold.manifest import PIPELINE_MANIFEST_SCHEMA
    from gold.runner import run_gold
    from silver.conform import refresh_orphan_flags
    from silver.schemas import QUARANTINE_SCHEMA, silver_entity_schema

    import silver.conform as conform_mod
    import silver.quarantine as quarantine_mod

    spark.sql("DROP DATABASE IF EXISTS gct_silver CASCADE")
    spark.sql("DROP DATABASE IF EXISTS gct_gold CASCADE")
    spark.sql(f"DROP TABLE IF EXISTS {_MANIFEST_TABLE}")
    spark.sql("CREATE DATABASE IF NOT EXISTS gct_silver")

    for entity, table in _SILVER_TABLES.items():
        create_delta_table(spark, table, silver_entity_schema(entity))
    create_delta_table(spark, _QUARANTINE_TABLE, QUARANTINE_SCHEMA)

    # Same monkeypatch as test_pipeline_contract.py:246-249, but pointed at
    # schema-qualified table names instead of flat ones.
    conform_mod.silver_table = lambda entity, _c="de_assessment": _SILVER_TABLES[entity]
    conform_mod.load_dq_schema = lambda _s, entity, _c="de_assessment": _dq_schema(entity)
    quarantine_mod.quarantine_table = lambda _c="de_assessment": _QUARANTINE_TABLE

    gen = load_generator()
    gen.BASE_CUSTOMERS = _GEN_BASE_CUSTOMERS
    gen.BASE_PRODUCTS = _GEN_BASE_PRODUCTS
    gen.BASE_ORDERS = _GEN_BASE_ORDERS

    seed = dict(zip(("customers", "products", "orders"), gen.generate_dataframes()))
    for entity in ("products", "customers", "orders"):
        _conform(spark, seed[entity], entity, "seed", f"run-seed-{entity}")
    refresh_orphan_flags(spark)

    delta = dict(
        zip(("customers", "products", "orders"), gen.generate_delta_dataframes())
    )
    for entity in ("products", "customers", "orders"):
        _conform(spark, delta[entity], entity, "delta", f"run-delta-{entity}")
    refresh_orphan_flags(spark)

    spark.createDataFrame([], PIPELINE_MANIFEST_SCHEMA).write.format("delta").mode(
        "overwrite"
    ).saveAsTable(_MANIFEST_TABLE)
    run_gold(
        spark,
        silver_schema="gct_silver",
        gold_schema=_GOLD_SCHEMA,
        manifest_table=_MANIFEST_TABLE,
    )

    snapshots = {
        entity: _silver_snapshot(spark, entity, table)
        for entity, table in _SILVER_TABLES.items()
    }
    return {"spark": spark, "silver": snapshots}
