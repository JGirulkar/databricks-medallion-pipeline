from dataclasses import dataclass
from typing import Literal, cast

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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
