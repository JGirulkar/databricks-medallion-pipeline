from dataclasses import dataclass
import json
from typing import Any, Literal, cast

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DEFAULT_CATALOG = "de_assessment"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
CONFIG_SCHEMA = "config"
OPS_SCHEMA = "ops"
SOURCE_CONFIG_TABLE_NAME = "source_config"
QUARANTINE_TABLE_NAME = "quarantine"
DQ_METRICS_TABLE_NAME = "dq_metrics"
PIPELINE_MANIFEST_TABLE_NAME = "pipeline_manifest"

ORCHESTRATION_ORDER: tuple[str, ...] = ("products", "customers", "orders")
SNAPSHOT_ENTITIES: frozenset[str] = frozenset({"customers", "products"})
ENTITY_PK: dict[str, str] = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
}

PRODUCT_HASH_COLUMNS: tuple[str, ...] = (
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "stock_quantity",
    "reorder_level",
)

ValidationMode = Literal["enforce"]
CheckKind = Literal["not_null", "uniqueness", "fk_exists"]
_CHECK_KINDS = {"not_null", "uniqueness", "fk_exists"}


def silver_table(name: str, catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{SILVER_SCHEMA}.{name}"


def bronze_table(name: str, catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{BRONZE_SCHEMA}.{name}"


def pipeline_manifest_table(catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{OPS_SCHEMA}.{PIPELINE_MANIFEST_TABLE_NAME}"


def quarantine_table(catalog: str = DEFAULT_CATALOG) -> str:
    return silver_table(QUARANTINE_TABLE_NAME, catalog)


def dq_metrics_table(catalog: str = DEFAULT_CATALOG) -> str:
    return silver_table(DQ_METRICS_TABLE_NAME, catalog)


def source_config_table(catalog: str = DEFAULT_CATALOG) -> str:
    return f"{catalog}.{CONFIG_SCHEMA}.{SOURCE_CONFIG_TABLE_NAME}"


def silver_checkpoint_path(
    entity: str,
    catalog: str = DEFAULT_CATALOG,
    suffix: str | None = None,
) -> str:
    base = f"/Volumes/{catalog}/{OPS_SCHEMA}/checkpoints/silver/{entity}/"
    if suffix:
        return f"{base}{suffix}/"
    return base


@dataclass(frozen=True)
class ColumnRule:
    name: str
    type: str
    nullable: bool
    validation: dict[str, Any] | None = None


@dataclass(frozen=True)
class EntityCheck:
    kind: CheckKind
    column: str
    category: str
    ref_table: str | None = None
    ref_column: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in _CHECK_KINDS:
            raise ValueError(f"Invalid check kind: {self.kind}")
        if self.kind == "fk_exists" and not self.ref_table:
            raise ValueError("fk_exists check requires ref_table")


@dataclass(frozen=True)
class DqSchema:
    schema_version: str
    validation_mode: ValidationMode
    columns: tuple[ColumnRule, ...]
    checks: tuple[EntityCheck, ...]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DqSchema":
        columns = tuple(
            ColumnRule(
                name=cast(str, col["name"]),
                type=cast(str, col["type"]),
                nullable=bool(col.get("nullable", True)),
                validation=cast(dict[str, Any] | None, col.get("validation")),
            )
            for col in data.get("columns", [])
        )
        checks = tuple(
            EntityCheck(
                kind=cast(CheckKind, check["kind"]),
                column=cast(str, check["column"]),
                category=cast(str, check["category"]),
                ref_table=cast(str | None, check.get("ref_table")),
                ref_column=cast(str | None, check.get("ref_column")),
            )
            for check in data.get("checks", [])
        )
        return cls(
            schema_version=cast(str, data.get("$schemaVersion", "1.0")),
            validation_mode=cast(ValidationMode, data.get("validationMode", "enforce")),
            columns=columns,
            checks=checks,
        )


def _variant_to_dict(value: object) -> dict[str, Any]:
    if value is None:
        raise ValueError("dq_schema is null")
    if hasattr(value, "asDict"):
        return cast(dict[str, Any], value.asDict(recursive=True))
    if isinstance(value, dict):
        return value
    if type(value).__name__ == "VariantVal":
        payload = value.json() if hasattr(value, "json") else str(value)  # type: ignore[union-attr]
        return cast(dict[str, Any], json.loads(payload))
    raise TypeError(f"Unexpected dq_schema type: {type(value)}")


def load_dq_schema(
    spark: SparkSession,
    source_name: str,
    catalog: str = DEFAULT_CATALOG,
) -> DqSchema:
    fqn = source_config_table(catalog)
    # to_json normalizes UC VARIANT / Spark Connect VariantVal to a JSON string
    rows = spark.sql(
        f"""
        SELECT to_json(dq_schema) AS dq_schema_json
        FROM {fqn}
        WHERE source_name = '{source_name}'
        LIMIT 2
        """
    ).collect()
    if len(rows) != 1:
        raise ValueError(
            f"Expected one source_config row for {source_name!r}; found {len(rows)}"
        )
    payload = rows[0]["dq_schema_json"]
    if payload is None:
        raise ValueError(f"dq_schema is null for source_name={source_name!r}")
    return DqSchema.from_dict(json.loads(payload))


def get_delivery_pattern(
    spark: SparkSession,
    source_name: str,
    catalog: str = DEFAULT_CATALOG,
) -> str:
    rows = (
        spark.table(source_config_table(catalog))
        .where(F.col("source_name") == source_name)
        .limit(2)
        .collect()
    )
    if len(rows) != 1:
        raise ValueError(
            f"Expected one source_config row for {source_name!r}; found {len(rows)}"
        )
    return str(rows[0]["delivery_pattern"])
