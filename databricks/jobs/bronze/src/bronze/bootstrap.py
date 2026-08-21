"""Idempotent Unity Catalog bootstrap for the Bronze layer.

All DDL uses IF NOT EXISTS — safe to re-run against a live catalog without
destroying existing data.  Source seeds use WHEN NOT MATCHED THEN INSERT
only, so manually-tweaked source_config rows are never overwritten.

Assumption: Delta DDL is executed against a UC-enabled cluster; locally the
module is import-only and the SQL strings are tested as pure strings.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from bronze.config import (
    BRONZE_SCHEMA,
    DEFAULT_CATALOG,
    LANDING_SCHEMA,
    OPS_SCHEMA,
    bronze_table,
    manifest_table,
    source_config_table,
)
from bronze.schemas import table_schema

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

_SOURCES: tuple[str, ...] = ("customers", "orders", "products")


# ---------------------------------------------------------------------------
# Type mapping: PySpark → SQL DDL
# ---------------------------------------------------------------------------


def _spark_type_to_sql(dtype: object) -> str:
    from pyspark.sql.types import DecimalType

    if isinstance(dtype, DecimalType):
        return f"DECIMAL({dtype.precision}, {dtype.scale})"
    name: str = dtype.typeName()  # type: ignore[attr-defined]
    mapping = {
        "integer": "INT",
        "string": "STRING",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "boolean": "BOOLEAN",
        "long": "BIGINT",
    }
    return mapping.get(name, name.upper())


def _field_ddl(field: object) -> str:
    sql_type = _spark_type_to_sql(field.dataType)  # type: ignore[attr-defined]
    nullable = "" if field.nullable else " NOT NULL"  # type: ignore[attr-defined]
    return f"  {field.name} {sql_type}{nullable}"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# DDL builders
# ---------------------------------------------------------------------------


def _entity_table_ddl(source_name: str, catalog: str) -> str:
    schema = table_schema(source_name)
    cols = ",\n".join(_field_ddl(f) for f in schema.fields)
    fqn = bronze_table(source_name, catalog)
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        f"{cols}\n"
        f") TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def _source_config_ddl(catalog: str) -> str:
    fqn = source_config_table(catalog)
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        "  source_name STRING NOT NULL,\n"
        "  target_table STRING NOT NULL,\n"
        "  raw_path STRING NOT NULL,\n"
        "  checkpoint_path STRING NOT NULL,\n"
        "  schema_hint_path STRING NOT NULL,\n"
        "  archive_path STRING,\n"
        "  file_format STRING NOT NULL,\n"
        "  delivery_pattern STRING NOT NULL,\n"
        "  cdf_enabled BOOLEAN NOT NULL,\n"
        "  schedule_hint STRING NOT NULL,\n"
        "  is_active BOOLEAN NOT NULL,\n"
        "  updated_at TIMESTAMP DEFAULT current_timestamp()\n"
        ") TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')"
    )


def _ingest_manifest_ddl(catalog: str) -> str:
    fqn = manifest_table(catalog)
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        "  batch_id STRING NOT NULL,\n"
        "  source_name STRING NOT NULL,\n"
        "  ingest_timestamp TIMESTAMP NOT NULL,\n"
        "  row_count BIGINT NOT NULL,\n"
        "  file_count INT NOT NULL,\n"
        "  status STRING NOT NULL,\n"
        "  created_at TIMESTAMP DEFAULT current_timestamp()\n"
        ") TBLPROPERTIES (\n"
        "  'delta.enableChangeDataFeed' = 'true',\n"
        "  'delta.feature.allowColumnDefaults' = 'supported'\n"
        ")"
    )


def bootstrap_ddl(catalog: str = DEFAULT_CATALOG) -> tuple[str, ...]:
    """Return all idempotent DDL statements needed to stand up the Bronze layer.

    No MERGE or DML — pure structural DDL only.
    """
    stmts: list[str] = [
        f"CREATE CATALOG IF NOT EXISTS {catalog}",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{BRONZE_SCHEMA}",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{LANDING_SCHEMA}",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{OPS_SCHEMA}",
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{LANDING_SCHEMA}.raw",
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{OPS_SCHEMA}.checkpoints",
    ]
    for source_name in _SOURCES:
        stmts.append(_entity_table_ddl(source_name, catalog))
    stmts.append(_source_config_ddl(catalog))
    stmts.append(_ingest_manifest_ddl(catalog))
    return tuple(stmts)


# ---------------------------------------------------------------------------
# Source seed rows
# ---------------------------------------------------------------------------


def source_seed_rows(catalog: str = DEFAULT_CATALOG) -> tuple[dict, ...]:  # type: ignore[type-arg]
    """Return seed rows for source_config — one per source entity.

    Row order is deterministic (products, customers, orders) but the MERGE
    key is source_name so order does not affect correctness.
    """
    return (
        {
            "source_name": "products",
            "target_table": bronze_table("products", catalog),
            "raw_path": f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/products/",
            "checkpoint_path": (
                f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/products/"
            ),
            "schema_hint_path": (
                f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/products/_schema/"
            ),
            "archive_path": None,
            "file_format": "csv",
            "delivery_pattern": "full_snapshot",
            "cdf_enabled": True,
            "schedule_hint": "weekly",
            "is_active": True,
        },
        {
            "source_name": "customers",
            "target_table": bronze_table("customers", catalog),
            "raw_path": f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/customers/",
            "checkpoint_path": (
                f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/customers/"
            ),
            "schema_hint_path": (
                f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/customers/_schema/"
            ),
            "archive_path": None,
            "file_format": "csv",
            "delivery_pattern": "full_snapshot",
            "cdf_enabled": True,
            "schedule_hint": "daily",
            "is_active": True,
        },
        {
            "source_name": "orders",
            "target_table": bronze_table("orders", catalog),
            "raw_path": f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/orders/incoming/",
            "checkpoint_path": (
                f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/orders/"
            ),
            "schema_hint_path": (
                f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/orders/_schema/"
            ),
            "archive_path": (
                f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/orders/processed/"
            ),
            "file_format": "csv",
            "delivery_pattern": "incremental",
            "cdf_enabled": True,
            "schedule_hint": "on_arrival",
            "is_active": True,
        },
    )


# ---------------------------------------------------------------------------
# Seed MERGE SQL — non-destructive: WHEN NOT MATCHED only
# ---------------------------------------------------------------------------

_SEED_MERGE_TARGET = source_config_table()

_SEED_MERGE_SQL = f"""\
MERGE INTO {_SEED_MERGE_TARGET} AS target
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
)"""


# ---------------------------------------------------------------------------
# Volume directories to create
# ---------------------------------------------------------------------------


def _volume_dirs(catalog: str) -> tuple[str, ...]:
    return (
        f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/products/",
        f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/customers/",
        f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/orders/incoming/",
        f"/Volumes/{catalog}/{LANDING_SCHEMA}/raw/orders/processed/",
        f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/products/_schema/",
        f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/customers/_schema/",
        f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/orders/_schema/",
    )


_DIRS: tuple[str, ...] = _volume_dirs(DEFAULT_CATALOG)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def bootstrap(
    spark: SparkSession,
    mkdirs: Callable[[str], None],
    catalog: str = DEFAULT_CATALOG,
) -> None:
    """Run idempotent Bronze-layer UC bootstrap.

    1. Execute all structural DDL (catalog, schemas, volumes, tables).
    2. Seed source_config with WHEN NOT MATCHED only — preserves manual tweaks.
    3. Create Volume sub-directories via the injected mkdirs callable.

    Args:
        spark:   Active SparkSession (from cluster context).
        mkdirs:  Callable that creates a directory path, e.g. dbutils.fs.mkdirs.
        catalog: Target UC catalog (defaults to de_assessment).
    """
    for stmt in bootstrap_ddl(catalog):
        spark.sql(stmt)

    seed_df = spark.createDataFrame(list(source_seed_rows(catalog)))
    seed_df.createOrReplaceTempView("bronze_source_seed")

    merge_sql = _SEED_MERGE_SQL
    if catalog != DEFAULT_CATALOG:
        merge_sql = merge_sql.replace(_SEED_MERGE_TARGET, source_config_table(catalog))
    spark.sql(merge_sql)

    for path in _volume_dirs(catalog):
        mkdirs(path)
