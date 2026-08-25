"""Entity-level checks from dq_schema.checks — not_null, uniqueness, fk_exists."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.config import DqSchema, EntityCheck
from silver.validators import VIOLATION_ARRAY_TYPE


def _violation_struct(check: EntityCheck, value_col: str) -> F.Column:
    return F.struct(
        F.lit(check.category).alias("category"),
        F.lit(check.kind).alias("rule"),
        F.lit(check.column).alias("column"),
        F.col(value_col).cast("string").alias("value"),
    )


def _append_violations(df: DataFrame, violation_expr: F.Column) -> DataFrame:
    packed = F.array(F.col("_violations"), F.array(violation_expr))
    if "_violations" not in df.columns:
        return df.withColumn(
            "_violations",
            F.array_compact(F.array(violation_expr)).cast(VIOLATION_ARRAY_TYPE),
        )
    return df.withColumn(
        "_violations",
        F.array_compact(F.flatten(packed)).cast(VIOLATION_ARRAY_TYPE),
    )


def _apply_not_null(df: DataFrame, check: EntityCheck) -> DataFrame:
    predicate = F.col(check.column).isNotNull()
    violation = F.when(
        ~predicate,
        _violation_struct(check, check.column),
    )
    return _append_violations(df, violation)


def _apply_uniqueness(df: DataFrame, check: EntityCheck) -> DataFrame:
    window = Window.partitionBy(check.column)
    dup_count = F.count(F.lit(1)).over(window)
    tagged = df.withColumn("_dup_count", dup_count)
    violation = F.when(
        F.col("_dup_count") > 1,
        _violation_struct(check, check.column),
    )
    return _append_violations(tagged, violation).drop("_dup_count")


def _apply_fk_exists(
    df: DataFrame,
    check: EntityCheck,
    spark: SparkSession,
) -> DataFrame:
    ref_column = check.ref_column or check.column
    parents = (
        spark.table(check.ref_table)
        .where(~F.col("_is_deleted"))
        .select(F.col(ref_column).alias("_parent_key"))
        .distinct()
    )
    joined = df.join(
        parents,
        df[check.column] == F.col("_parent_key"),
        "left",
    )
    violation = F.when(
        F.col(check.column).isNotNull() & F.col("_parent_key").isNull(),
        _violation_struct(check, check.column),
    )
    tagged = joined.withColumn("_fk_violation", violation)
    base = tagged.drop("_parent_key")
    return _append_violations(base, F.col("_fk_violation")).drop("_fk_violation")


def apply_entity_checks(
    df: DataFrame,
    dq_schema: DqSchema,
    spark: SparkSession,
    catalog: str = "de_assessment",
) -> DataFrame:
    """Append entity check violations to `_violations`."""
    del catalog  # reserved for future catalog-scoped parent lookups
    result = df
    if "_violations" not in result.columns:
        result = result.withColumn("_violations", F.array().cast(VIOLATION_ARRAY_TYPE))
    for check in dq_schema.checks:
        if check.kind == "not_null":
            result = _apply_not_null(result, check)
        elif check.kind == "uniqueness":
            result = _apply_uniqueness(result, check)
        elif check.kind == "fk_exists":
            result = _apply_fk_exists(result, check, spark)
    return result
