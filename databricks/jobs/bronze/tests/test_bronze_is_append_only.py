"""Bronze must land the source as delivered — nothing rejected, nothing changed.

The layer boundary is the point: if bronze cleans, filters or de-duplicates,
there is no longer any record of what the source actually sent, and no way to
prove a downstream number came from the data rather than from the ingest. So
bronze appends every row it reads and captures unparseable content in
`_rescued_data`; all validation, quarantine, soft deletes and updates belong to
silver.

These are source-level guards. A behavioural test cannot catch a filter someone
adds tomorrow to a code path the test does not exercise, whereas this fails as
soon as a destructive operation appears anywhere in the layer.
"""

from __future__ import annotations

import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[1] / "src"

# Operation -> why it must not appear in bronze.
FORBIDDEN: dict[str, str] = {
    "whenMatched": "an update belongs to silver; bronze only appends",
    "whenNotMatched": "a merge belongs to silver; bronze only appends",
    ".merge(": "a merge belongs to silver; bronze only appends",
    "dropDuplicates": "de-duplication is survivorship, which is a silver concern",
    ".dropna(": "dropping incomplete rows destroys the evidence of a source defect",
    'mode("overwrite")': "overwrite discards previously landed rows",
    "DROPMALFORMED": "malformed rows must be rescued, not dropped",
    "FAILFAST": "one bad row must not reject the whole file",
    "badRecordsPath": "bad records belong in _rescued_data, not a side file",
    "_is_deleted": "soft deletes are a silver concern",
    "_is_orphan": "referential state is a silver concern",
    "quality_check_result": "validation outcomes belong to silver",
}


def _bronze_sources() -> list[pathlib.Path]:
    return sorted(SRC.glob("**/*.py"))


@pytest.mark.unit
def test_bronze_never_rejects_or_mutates_rows() -> None:
    offenders: list[str] = []
    for path in _bronze_sources():
        text = path.read_text()
        for operation, reason in FORBIDDEN.items():
            if operation in text:
                offenders.append(f"{path.relative_to(SRC)}: {operation} — {reason}")
    assert not offenders, "destructive or silver-layer operations in bronze:\n" + "\n".join(
        offenders
    )


@pytest.mark.unit
def test_bronze_writes_in_append_mode_only() -> None:
    """Every Delta write in the layer is an append."""
    modes: list[str] = []
    for path in _bronze_sources():
        for line in path.read_text().splitlines():
            if ".mode(" in line:
                modes.append(f"{path.relative_to(SRC)}:{line.strip()}")
    assert modes, "no Delta write found — this guard would pass vacuously"
    for entry in modes:
        assert 'mode("append")' in entry, f"non-append write in bronze: {entry}"


@pytest.mark.unit
def test_rescued_data_column_is_configured() -> None:
    """Unparseable content is captured rather than dropped."""
    import sys

    sys.path.insert(0, str(SRC))
    from bronze.ingest import cloudfiles_options
    from bronze.schemas import COMMON_METADATA_FIELDS

    class _Config:
        file_format = "csv"
        schema_hint_path = "/tmp/schema"

    options = cloudfiles_options(_Config())
    assert options.get("rescuedDataColumn") == "_rescued_data", (
        "without a rescue column, a row Auto Loader cannot parse is silently lost"
    )
    # Inferring types would let Auto Loader coerce or null a bad value in place;
    # reading everything as declared keeps the raw text recoverable.
    assert options.get("cloudFiles.inferColumnTypes") == "false"
    assert any(f.name == "_rescued_data" for f in COMMON_METADATA_FIELDS), (
        "the bronze schema must carry the rescue column"
    )
