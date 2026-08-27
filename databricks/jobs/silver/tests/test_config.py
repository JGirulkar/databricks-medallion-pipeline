from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import pytest
from silver.config import (
    ORCHESTRATION_ORDER,
    ColumnRule,
    DqSchema,
    EntityCheck,
    dq_metrics_table,
    load_dq_schema,
    quarantine_table,
    silver_checkpoint_path,
    silver_table,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


_SAMPLE_DQ_SCHEMA: dict[str, object] = {
    "$schemaVersion": "1.0",
    "validationMode": "enforce",
    "columns": [
        {
            "name": "email",
            "type": "string",
            "nullable": True,
            "validation": {"kind": "string", "format": "email"},
        }
    ],
    "checks": [
        {"kind": "not_null", "column": "customer_id", "category": "completeness"},
        {"kind": "uniqueness", "column": "order_id", "category": "uniqueness"},
    ],
}


@pytest.mark.unit
def test_silver_fqn_helpers() -> None:
    assert silver_table("customers") == "de_assessment.silver.customers"
    assert quarantine_table() == "de_assessment.silver.quarantine"
    assert dq_metrics_table() == "de_assessment.silver.dq_metrics"


@pytest.mark.unit
def test_silver_checkpoint_path() -> None:
    path = silver_checkpoint_path("orders")
    assert path == "/Volumes/de_assessment/ops/checkpoints/silver/orders/"


@pytest.mark.unit
def test_orchestration_order() -> None:
    assert ORCHESTRATION_ORDER == ("products", "customers", "orders")


@pytest.mark.unit
def test_dq_schema_from_dict() -> None:
    schema = DqSchema.from_dict(_SAMPLE_DQ_SCHEMA)
    assert schema.schema_version == "1.0"
    assert schema.validation_mode == "enforce"
    assert len(schema.columns) == 1
    assert schema.columns[0] == ColumnRule(
        name="email",
        type="string",
        nullable=True,
        validation={"kind": "string", "format": "email"},
    )
    assert len(schema.checks) == 2
    assert schema.checks[0] == EntityCheck(
        kind="not_null",
        column="customer_id",
        category="completeness",
        ref_table=None,
        ref_column=None,
    )
    assert schema.checks[1].kind == "uniqueness"


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("silver-config-tests")
        .getOrCreate()
    )
    yield session
    session.stop()


def _source_config_schema():
    from pyspark.sql.types import (
        ArrayType,
        BooleanType,
        StringType,
        StructField,
        StructType,
    )

    validation_struct = StructType(
        [
            StructField("kind", StringType(), False),
            StructField("format", StringType(), True),
        ]
    )
    column_struct = StructType(
        [
            StructField("name", StringType(), False),
            StructField("type", StringType(), False),
            StructField("nullable", BooleanType(), False),
            StructField("validation", validation_struct, True),
        ]
    )
    check_struct = StructType(
        [
            StructField("kind", StringType(), False),
            StructField("column", StringType(), False),
            StructField("category", StringType(), False),
        ]
    )
    dq_schema_struct = StructType(
        [
            StructField("$schemaVersion", StringType(), False),
            StructField("validationMode", StringType(), False),
            StructField("columns", ArrayType(column_struct), False),
            StructField("checks", ArrayType(check_struct), False),
        ]
    )
    return StructType(
        [
            StructField("source_name", StringType(), False),
            StructField("dq_schema", dq_schema_struct, False),
        ]
    )


@pytest.mark.unit
def test_variant_to_dict_parses_variant_val_json() -> None:
    from silver.config import _variant_to_dict

    class _VariantVal:
        def json(self) -> str:
            return '{"$schemaVersion":"1.0","validationMode":"enforce","columns":[],"checks":[]}'

    parsed = _variant_to_dict(_VariantVal())
    assert parsed["validationMode"] == "enforce"


@pytest.mark.spark
def test_load_dq_schema_reads_variant_column(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    table_name = "test_source_config_dq_schema"
    monkeypatch.setattr(
        "silver.config.source_config_table", lambda catalog="de_assessment": table_name
    )
    spark.createDataFrame(
        [
            (
                "customers",
                (
                    "1.0",
                    "enforce",
                    [
                        (
                            "email",
                            "string",
                            True,
                            ("string", "email"),
                        )
                    ],
                    [
                        ("not_null", "customer_id", "completeness"),
                        ("uniqueness", "order_id", "uniqueness"),
                    ],
                ),
            )
        ],
        schema=_source_config_schema(),
    ).createOrReplaceTempView(table_name)

    schema = load_dq_schema(spark, "customers")
    assert schema.validation_mode == "enforce"
    assert schema.checks[0].column == "customer_id"
