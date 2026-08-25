from __future__ import annotations

from datetime import UTC, datetime

import pytest
from bronze.manifest import PIPELINE_MANIFEST_COLUMN_NAMES, ManifestRecord


def _record(**overrides: object) -> ManifestRecord:
    base = {
        "batch_id": "batch-1",
        "source_name": "orders",
        "delivery_pattern": "incremental",
        "source_path": "/Volumes/de_assessment/landing/raw/orders/incoming/",
        "files_processed": 1,
        "rows_read": 10,
        "rows_written": 10,
        "rows_rescued": 0,
        "delta_version_before": 0,
        "delta_version_after": 1,
        "started_at": datetime(2026, 1, 1, tzinfo=UTC),
        "completed_at": datetime(2026, 1, 1, 0, 1, tzinfo=UTC),
        "status": "success",
        "error_message": None,
    }
    base.update(overrides)
    return ManifestRecord(**base)  # type: ignore[arg-type]


@pytest.mark.unit
def test_manifest_rejects_empty_batch_id() -> None:
    with pytest.raises(ValueError, match="batch_id"):
        _record(batch_id="")


@pytest.mark.unit
def test_manifest_rejects_invalid_status() -> None:
    with pytest.raises(ValueError, match="Invalid status"):
        _record(status="pending")  # type: ignore[arg-type]


@pytest.mark.unit
def test_manifest_success_requires_completed_at() -> None:
    with pytest.raises(ValueError, match="completed_at"):
        _record(completed_at=None)


@pytest.mark.unit
def test_manifest_as_row_maps_to_pipeline_manifest() -> None:
    row = _record().as_row()
    assert row["run_id"] == "batch-1"
    assert row["layer"] == "bronze"
    assert row["entity_name"] == "orders"
    assert row["rows_quarantined"] == 0


@pytest.mark.unit
def test_manifest_as_row_column_order() -> None:
    row = _record().as_row()
    assert tuple(row.keys()) == PIPELINE_MANIFEST_COLUMN_NAMES
