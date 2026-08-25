"""Column predicates from dq_schema.columns — value-stage only (Silver typed Bronze)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column as SparkColumn
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    NumericType,
    StringType,
    TimestampNTZType,
    TimestampType,
)

from silver.config import ColumnRule, DqSchema

_EMAIL = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
_TEMPORAL_TYPES = (DateType, TimestampType, TimestampNTZType)
VIOLATION_ARRAY_TYPE = "array<struct<category:string,rule:string,column:string,value:string>>"


def column_predicates(
    column: ColumnRule,
    dtype: DataType | None = None,
) -> list[tuple[str, str, SparkColumn]]:
    """Return (category, rule_name, predicate) triples; True = pass."""
    c = F.col(f"`{column.name}`")
    rules: list[tuple[str, str, SparkColumn]] = []

    if not column.nullable:
        rules.append(("completeness", "not_null", c.isNotNull()))

    validation = column.validation
    if not validation:
        return rules

    kind = validation.get("kind")
    if kind == "string":
        rules.extend(_string_rules(c, validation))
    elif kind == "numeric":
        rules.extend(_numeric_rules(c, column.type, validation, dtype))
    elif kind == "datetime":
        rules.extend(_datetime_rules(c, validation, dtype))

    return rules


def _string_rules(c: SparkColumn, v: dict[str, object]) -> list[tuple[str, str, SparkColumn]]:
    rules: list[tuple[str, str, SparkColumn]] = []
    if v.get("min_length") is not None:
        rules.append(("type_logic", "min_length", F.length(c) >= int(v["min_length"])))
    if v.get("max_length") is not None:
        rules.append(("type_logic", "max_length", F.length(c) <= int(v["max_length"])))
    if v.get("pattern"):
        rules.append(("type_logic", "pattern", c.rlike(str(v["pattern"]))))
    fmt = v.get("format")
    if fmt == "email":
        rules.append(("type_logic", "format_email", c.rlike(_EMAIL)))
    enum_vals = v.get("enum")
    if enum_vals is not None:
        rules.append(("type_logic", "enum", c.isin(*enum_vals)))
    return rules


def _numeric_rules(
    c: SparkColumn,
    col_type: str,
    v: dict[str, object],
    dtype: DataType | None = None,
) -> list[tuple[str, str, SparkColumn]]:
    rules: list[tuple[str, str, SparkColumn]] = []
    if isinstance(dtype, NumericType):
        n = c
    else:
        spark_type = "long" if col_type == "integer" else "double"
        n = c.cast(spark_type)
    if v.get("minimum") is not None:
        rules.append(("type_logic", "minimum", n >= float(v["minimum"])))
    if v.get("maximum") is not None:
        rules.append(("type_logic", "maximum", n <= float(v["maximum"])))
    if v.get("exclusive_minimum") is not None:
        rules.append(
            ("type_logic", "exclusive_minimum", n > float(v["exclusive_minimum"]))
        )
    if v.get("exclusive_maximum") is not None:
        rules.append(
            ("type_logic", "exclusive_maximum", n < float(v["exclusive_maximum"]))
        )
    if v.get("multiple_of") is not None:
        rules.append(("type_logic", "multiple_of", (n % float(v["multiple_of"])) == 0))
    return rules


def _datetime_rules(
    c: SparkColumn,
    v: dict[str, object],
    dtype: DataType | None = None,
) -> list[tuple[str, str, SparkColumn]]:
    rules: list[tuple[str, str, SparkColumn]] = []
    if isinstance(dtype, _TEMPORAL_TYPES):
        parsed = c
    elif isinstance(dtype, StringType):
        fmt = str(v.get("format", "yyyy-MM-dd"))
        parsed = F.to_date(c, fmt)
    else:
        parsed = c
    parsed_date = F.to_date(parsed)
    if v.get("min_date") is not None:
        rules.append(
            (
                "type_logic",
                "min_date",
                c.isNull()
                | parsed.isNull()
                | (parsed_date >= F.lit(str(v["min_date"])).cast("date")),
            )
        )
    if v.get("max_date") is not None:
        max_date = (
            F.current_date()
            if v.get("max_date") == "today"
            else F.lit(str(v["max_date"])).cast("date")
        )
        rules.append(
            (
                "type_logic",
                "max_date",
                c.isNull()
                | parsed.isNull()
                | (parsed_date <= max_date),
            )
        )
    return rules


def annotate_violations(df: DataFrame, dq_schema: DqSchema) -> DataFrame:
    """Add `_violations` — empty array means all column rules passed."""
    df_types = {field.name: field.dataType for field in df.schema.fields}
    candidates: list[SparkColumn] = []
    for column in dq_schema.columns:
        if column.name not in df_types:
            continue
        for category, rule_name, predicate in column_predicates(
            column, df_types.get(column.name)
        ):
            candidates.append(
                F.when(
                    ~predicate,
                    F.struct(
                        F.lit(category).alias("category"),
                        F.lit(rule_name).alias("rule"),
                        F.lit(column.name).alias("column"),
                        F.col(f"`{column.name}`").cast("string").alias("value"),
                    ),
                )
            )
    if not candidates:
        return df.withColumn("_violations", F.array().cast(VIOLATION_ARRAY_TYPE))
    return df.withColumn("_violations", F.array_compact(F.array(*candidates)))
