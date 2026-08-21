from __future__ import annotations

import pytest
from bronze.schemas import COMMON_METADATA_FIELDS, source_schema, table_schema
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    TimestampType,
)


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


@pytest.mark.unit
def test_source_schema_unknown_source_raises() -> None:
    with pytest.raises(ValueError, match="Unknown source"):
        source_schema("inventory")


@pytest.mark.unit
@pytest.mark.parametrize(
    ("source_name", "expected_types"),
    [
        (
            "customers",
            [
                IntegerType(),
                StringType(),
                StringType(),
                StringType(),
                DateType(),
                StringType(),
                DecimalType(18, 2),
            ],
        ),
        (
            "orders",
            [
                IntegerType(),
                IntegerType(),
                DateType(),
                IntegerType(),
                IntegerType(),
                DecimalType(18, 2),
                DecimalType(18, 2),
                StringType(),
                DateType(),
            ],
        ),
        (
            "products",
            [
                IntegerType(),
                StringType(),
                StringType(),
                DecimalType(18, 2),
                DecimalType(18, 2),
                IntegerType(),
                IntegerType(),
            ],
        ),
    ],
)
def test_source_schema_field_types(source_name, expected_types) -> None:
    schema = source_schema(source_name)
    assert [field.dataType for field in schema.fields] == expected_types
    assert all(field.nullable for field in schema.fields)


@pytest.mark.unit
def test_common_metadata_fields_is_immutable_tuple() -> None:
    assert isinstance(COMMON_METADATA_FIELDS, tuple)


@pytest.mark.unit
def test_table_schema_appends_common_metadata() -> None:
    schema = table_schema("orders")
    metadata_names = [field.name for field in COMMON_METADATA_FIELDS]
    assert schema.fieldNames()[-len(metadata_names) :] == metadata_names


@pytest.mark.unit
def test_table_schema_customers_includes_row_hash() -> None:
    schema = table_schema("customers")
    assert schema.fieldNames()[-1] == "_row_hash"
    assert schema.fieldNames()[-2] == "_rescued_data"


@pytest.mark.unit
def test_table_schema_orders_excludes_row_hash() -> None:
    assert "_row_hash" not in table_schema("orders").fieldNames()


@pytest.mark.unit
def test_table_schema_metadata_nullability() -> None:
    schema = table_schema("products")
    metadata_names = {field.name for field in COMMON_METADATA_FIELDS}
    metadata_fields = {
        field.name: field for field in schema.fields if field.name in metadata_names
    }
    assert set(metadata_fields) == metadata_names
    for expected in COMMON_METADATA_FIELDS:
        actual = metadata_fields[expected.name]
        assert actual.nullable == expected.nullable
        assert actual.dataType == expected.dataType
    assert metadata_fields["_ingest_timestamp"].dataType == TimestampType()
