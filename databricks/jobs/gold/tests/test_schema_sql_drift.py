"""The reference DDL in database/schema.sql must list exactly the columns
the executed SQL files produce. Derived from execution, not from a second
hand-maintained list — a reference doc without a guard is future drift."""

import pathlib
import re

import pytest
from gold.config import GOLD_TABLES

SCHEMA_SQL = pathlib.Path(__file__).resolve().parents[4] / "database" / "schema.sql"


@pytest.mark.spark
def test_schema_sql_matches_built_gold_tables(silver_tables) -> None:
    spark = silver_tables["spark"]
    text = SCHEMA_SQL.read_text(encoding="utf-8").lower()
    for table in GOLD_TABLES:
        match = re.search(
            rf"create table[^(]*gold\.{table}\s*\((.*?)\)\s*;", text, re.DOTALL
        )
        assert match, f"gold.{table} missing from schema.sql"
        declared = {
            line.strip().split()[0]
            for line in match.group(1).splitlines()
            if line.strip() and not line.strip().startswith("--")
        }
        built = {f.name.lower() for f in spark.table(f"gct_gold.{table}").schema.fields}
        assert declared == built, f"gold.{table}: schema.sql drift {declared ^ built}"
