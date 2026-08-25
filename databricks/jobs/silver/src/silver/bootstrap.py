"""Idempotent Silver layer UC bootstrap — DDL, dq_schema seeds, checkpoint dirs."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import TYPE_CHECKING

from silver.config import (
    DEFAULT_CATALOG,
    ORCHESTRATION_ORDER,
    quarantine_table,
    silver_checkpoint_path,
    silver_table,
    source_config_table,
)
from silver.job_log import configure_job_logger
from silver.schemas import silver_entity_schema

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

LOG = configure_job_logger("silver.bootstrap")

_SILVER_ENTITIES = tuple(ORCHESTRATION_ORDER)


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
        "double": "DOUBLE",
    }
    return mapping.get(name, name.upper())


def _field_ddl(field: object) -> str:
    sql_type = _spark_type_to_sql(field.dataType)  # type: ignore[attr-defined]
    nullable = "" if field.nullable else " NOT NULL"  # type: ignore[attr-defined]
    return f"  {field.name} {sql_type}{nullable}"  # type: ignore[attr-defined]


def _silver_entity_ddl(entity: str, catalog: str) -> str:
    schema = silver_entity_schema(entity)
    cols = ",\n".join(_field_ddl(f) for f in schema.fields)
    fqn = silver_table(entity, catalog)
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        f"{cols}\n"
        f") TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def _quarantine_ddl(catalog: str) -> str:
    fqn = quarantine_table(catalog)
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        "  entity_name STRING NOT NULL,\n"
        "  primary_key STRING NOT NULL,\n"
        "  data STRING NOT NULL,\n"
        "  violations ARRAY<STRUCT<category:STRING, rule:STRING, column:STRING, value:STRING>> NOT NULL,\n"
        "  quarantined_at TIMESTAMP NOT NULL,\n"
        "  silver_run_id STRING NOT NULL,\n"
        "  bronze_batch_id STRING\n"
        ") TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )


def _dq_metrics_ddl(catalog: str) -> str:
    fqn = silver_table("dq_metrics", catalog)
    return (
        f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        "  silver_run_id STRING NOT NULL,\n"
        "  entity_name STRING NOT NULL,\n"
        "  check_category STRING NOT NULL,\n"
        "  rows_evaluated BIGINT NOT NULL,\n"
        "  rows_passed BIGINT NOT NULL,\n"
        "  rows_quarantined BIGINT NOT NULL,\n"
        "  pass_pct DOUBLE NOT NULL,\n"
        "  run_at TIMESTAMP NOT NULL\n"
        ")"
    )


def bootstrap_ddl(catalog: str = DEFAULT_CATALOG) -> tuple[str, ...]:
    stmts: list[str] = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver",
        f"ALTER TABLE {source_config_table(catalog)} ADD COLUMN IF NOT EXISTS dq_schema VARIANT",
    ]
    for entity in _SILVER_ENTITIES:
        stmts.append(_silver_entity_ddl(entity, catalog))
    stmts.append(_quarantine_ddl(catalog))
    stmts.append(_dq_metrics_ddl(catalog))
    return tuple(stmts)


def _dq_schema_seeds(catalog: str) -> dict[str, dict[str, object]]:
    customers_fqn = silver_table("customers", catalog)
    products_fqn = silver_table("products", catalog)
    return {
        "customers": {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [
                {
                    "name": "email",
                    "type": "string",
                    "nullable": True,
                    "validation": {"kind": "string", "format": "email"},
                },
                {
                    "name": "customer_segment",
                    "type": "string",
                    "nullable": True,
                    "validation": {
                        "kind": "string",
                        "enum": ["Premium", "Standard", "Basic"],
                    },
                },
                {
                    "name": "signup_date",
                    "type": "datetime",
                    "nullable": True,
                    "validation": {"kind": "datetime", "format": "yyyy-MM-dd", "max_date": "today"},
                },
                {
                    "name": "lifetime_value",
                    "type": "numeric",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 0},
                },
            ],
            "checks": [
                {"kind": "not_null", "column": "customer_id", "category": "completeness"},
                {"kind": "uniqueness", "column": "customer_id", "category": "uniqueness"},
            ],
        },
        "products": {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [
                {
                    "name": "price",
                    "type": "numeric",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 0},
                },
                {
                    "name": "cost",
                    "type": "numeric",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 0},
                },
            ],
            "checks": [
                {"kind": "not_null", "column": "product_id", "category": "completeness"},
                {"kind": "uniqueness", "column": "product_id", "category": "uniqueness"},
            ],
        },
        "orders": {
            "$schemaVersion": "1.0",
            "validationMode": "enforce",
            "columns": [
                {
                    "name": "quantity",
                    "type": "integer",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 1},
                },
                {
                    "name": "unit_price",
                    "type": "numeric",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 0},
                },
                {
                    "name": "total_amount",
                    "type": "numeric",
                    "nullable": True,
                    "validation": {"kind": "numeric", "minimum": 0},
                },
                {
                    "name": "order_status",
                    "type": "string",
                    "nullable": True,
                    "validation": {
                        "kind": "string",
                        "enum": ["Pending", "Completed", "Cancelled"],
                    },
                },
            ],
            "checks": [
                {"kind": "not_null", "column": "order_id", "category": "completeness"},
                {"kind": "uniqueness", "column": "order_id", "category": "uniqueness"},
                {"kind": "not_null", "column": "customer_id", "category": "completeness"},
                {"kind": "not_null", "column": "product_id", "category": "completeness"},
                {
                    "kind": "fk_exists",
                    "column": "customer_id",
                    "category": "referential",
                    "ref_table": customers_fqn,
                    "ref_column": "customer_id",
                },
                {
                    "kind": "fk_exists",
                    "column": "product_id",
                    "category": "referential",
                    "ref_table": products_fqn,
                    "ref_column": "product_id",
                },
            ],
        },
    }


def _checkpoint_dirs(catalog: str) -> tuple[str, ...]:
    return tuple(silver_checkpoint_path(entity, catalog) for entity in _SILVER_ENTITIES)


def seed_dq_schema(spark: SparkSession, catalog: str = DEFAULT_CATALOG) -> None:
    fqn = source_config_table(catalog)
    for source_name, schema_dict in _dq_schema_seeds(catalog).items():
        payload = json.dumps(schema_dict).replace("'", "\\'")
        spark.sql(
            f"""
            MERGE INTO {fqn} AS target
            USING (
              SELECT '{source_name}' AS source_name,
                     parse_json('{payload}') AS dq_schema
            ) AS source
            ON target.source_name = source.source_name
            WHEN MATCHED THEN UPDATE SET dq_schema = source.dq_schema
            """
        )


def bootstrap(
    spark: SparkSession,
    mkdirs: Callable[[str], None],
    catalog: str = DEFAULT_CATALOG,
) -> None:
    LOG.info("silver_bootstrap_start catalog=%s", catalog)
    try:
        for stmt in bootstrap_ddl(catalog):
            LOG.info("silver_bootstrap_sql %s", stmt.split("\n", maxsplit=1)[0][:120])
            spark.sql(stmt)
        seed_dq_schema(spark, catalog)
        for path in _checkpoint_dirs(catalog):
            LOG.info("silver_bootstrap_mkdirs path=%s", path)
            mkdirs(path)
        LOG.info("silver_bootstrap_complete catalog=%s", catalog)
    except Exception:
        LOG.exception("silver_bootstrap_failed catalog=%s", catalog)
        raise
