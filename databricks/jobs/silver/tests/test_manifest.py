from __future__ import annotations

from datetime import UTC, datetime

import pytest
from silver.manifest import PipelineManifestRecord, append_silver_manifest
from silver.schemas import PIPELINE_MANIFEST_SCHEMA
from conftest import create_delta_table


@pytest.mark.unit
def test_pipeline_manifest_record_maps_silver_layer() -> None:
    record = PipelineManifestRecord(
        run_id="run-1",
        entity_name="orders",
        parent_run_id="parent-1",
        delivery_pattern="incremental",
        source_path="/Volumes/de_assessment/ops/checkpoints/silver/orders/",
        files_processed=0,
        rows_read=10,
        rows_written=8,
        rows_quarantined=2,
        rows_rescued=0,
        delta_version_before=1,
        delta_version_after=2,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        completed_at=datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        status="success",
        error_message=None,
    )
    row = record.as_row()
    assert row["layer"] == "silver"
    assert row["run_id"] == "run-1"


@pytest.mark.spark
def test_append_silver_manifest_writes_row(
    spark, monkeypatch: pytest.MonkeyPatch,
) -> None:
    table_name = "test_pipeline_manifest_silver"
    monkeypatch.setattr(
        "silver.manifest.pipeline_manifest_table",
        lambda catalog="de_assessment": table_name,
    )
    create_delta_table(spark, table_name, PIPELINE_MANIFEST_SCHEMA)
    record = PipelineManifestRecord(
        run_id="run-2",
        entity_name="products",
        parent_run_id=None,
        delivery_pattern="full_snapshot",
        source_path=None,
        files_processed=0,
        rows_read=0,
        rows_written=0,
        rows_quarantined=0,
        rows_rescued=0,
        delta_version_before=None,
        delta_version_after=None,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        completed_at=datetime(2026, 1, 1, tzinfo=UTC),
        status="success",
        error_message=None,
    )
    append_silver_manifest(spark, record)
    row = spark.table(table_name).collect()[0]
    assert row["layer"] == "silver"
