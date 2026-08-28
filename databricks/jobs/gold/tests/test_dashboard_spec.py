"""Unit tier: structural guards for the committed dashboard spec.

The dashboard JSON is deployed verbatim (bare table names; catalog/schema
supplied at deploy time), so its known structural footguns are guarded here:
grid rows that do not fill the 12-column width, widget encodings referencing
fields their query never defines, missing page metadata, queryLines that
concatenate into broken SQL, and drift between the JSON and the generated
dashboard_queries.sql export.
"""

from __future__ import annotations

import importlib.util
import json
import pathlib

import pytest

pytestmark = pytest.mark.unit

ROOT = pathlib.Path(__file__).resolve().parents[4]
DASHBOARD_JSON = ROOT / "databricks" / "dashboards" / "sales_overview.lvdash.json"
QUERIES_SQL = ROOT / "databricks" / "dashboards" / "dashboard_queries.sql"
GOLD_TABLES = {"sales_by_product", "revenue_by_customer", "daily_weekly_trends", "customer_segmentation"}


def _spec() -> dict:
    return json.loads(DASHBOARD_JSON.read_text(encoding="utf-8"))


def _field_names(encodings: object) -> set[str]:
    found: set[str] = set()
    if isinstance(encodings, dict):
        for key, value in encodings.items():
            if key == "fieldName" and isinstance(value, str):
                found.add(value)
            else:
                found |= _field_names(value)
    elif isinstance(encodings, list):
        for item in encodings:
            found |= _field_names(item)
    return found


def test_pages_have_grid_metadata() -> None:
    for page in _spec()["pages"]:
        assert page.get("layoutVersion") == "GRID_V1", page["name"]
        assert page.get("pageType") in ("PAGE_TYPE_CANVAS", "PAGE_TYPE_GLOBAL_FILTERS"), page["name"]


def test_every_row_fills_the_twelve_column_grid() -> None:
    for page in _spec()["pages"]:
        rows: dict[int, int] = {}
        for item in page["layout"]:
            pos = item["position"]
            assert pos["x"] + pos["width"] <= 12, item["widget"]["name"]
            rows[pos["y"]] = rows.get(pos["y"], 0) + pos["width"]
        for y, width in rows.items():
            assert width == 12, f"{page['name']} row y={y} fills {width}/12"


def test_widget_encodings_match_their_query_fields() -> None:
    for page in _spec()["pages"]:
        for item in page["layout"]:
            widget = item["widget"]
            if "queries" not in widget:  # text widgets
                continue
            declared = {f["name"] for q in widget["queries"] for f in q["query"]["fields"]}
            used = _field_names(widget["spec"]["encodings"])
            missing = used - declared
            assert not missing, f"{widget['name']}: encodings reference undeclared fields {missing}"


def test_widgets_reference_existing_datasets() -> None:
    spec = _spec()
    datasets = {d["name"] for d in spec["datasets"]}
    for page in spec["pages"]:
        for item in page["layout"]:
            for q in item["widget"].get("queries", []):
                assert q["query"]["datasetName"] in datasets, item["widget"]["name"]


def test_query_lines_concatenate_into_clean_sql() -> None:
    for ds in _spec()["datasets"]:
        lines = ds["queryLines"]
        for line in lines[:-1]:
            assert line.endswith((" ", "\n")), f"{ds['name']}: line lacks trailing separator: {line!r}"
        sql = "".join(lines)
        assert ";" not in sql, f"{ds['name']}: datasets hold exactly one statement"
        assert "--" not in sql, f"{ds['name']}: comments can swallow following lines"


def test_queries_use_bare_gold_table_names() -> None:
    # Portability contract: catalog/schema come from deploy flags, never the SQL.
    for ds in _spec()["datasets"]:
        sql = "".join(ds["queryLines"]).lower()
        from_targets = [w.strip().split()[0] for w in sql.split("from ")[1:]]
        for target in from_targets:
            assert "." not in target, f"{ds['name']}: qualified table name {target}"
            assert target in GOLD_TABLES, f"{ds['name']}: unknown table {target}"


def test_generated_queries_file_is_in_sync() -> None:
    gen_path = ROOT / "scripts" / "gen_dashboard_queries.py"
    module_spec = importlib.util.spec_from_file_location("gen_dashboard_queries", gen_path)
    assert module_spec and module_spec.loader
    gen = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(gen)
    assert QUERIES_SQL.read_text(encoding="utf-8") == gen.render(), (
        "dashboard_queries.sql drifted from the JSON — run scripts/gen_dashboard_queries.py"
    )
