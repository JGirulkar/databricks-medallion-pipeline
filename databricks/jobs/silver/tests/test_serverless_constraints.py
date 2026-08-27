"""Guard the serverless-compute restrictions that no local test can catch.

CE runs these jobs on serverless. Some operations local Spark accepts happily
are rejected there at runtime, so the failure only ever appears on the cluster,
minutes into a streaming query. Guarding the source is the only cheap gate.

Learned the hard way: `.cache()` on a batch inside foreachBatch passed the full
local suite and then failed every silver run with
`[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
compute`.
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Operation -> why serverless rejects it.
FORBIDDEN_ON_SERVERLESS: dict[str, str] = {
    ".cache()": "PERSIST TABLE is not supported on serverless compute",
    ".persist(": "PERSIST TABLE is not supported on serverless compute",
    ".unpersist(": "pairs with persist; unreachable if persist cannot run",
    "sparkContext": "SparkContext is not exposed on serverless / Spark Connect",
    "setLogLevel": "requires SparkContext, unavailable on serverless",
}

JOBS_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.unit
def test_job_sources_avoid_operations_serverless_rejects() -> None:
    offenders: list[str] = []
    for source in sorted(JOBS_ROOT.glob("*/src/**/*.py")):
        text = source.read_text()
        for operation, reason in FORBIDDEN_ON_SERVERLESS.items():
            if operation in text:
                offenders.append(
                    f"{source.relative_to(JOBS_ROOT)} uses {operation} — {reason}"
                )
    assert not offenders, "serverless-incompatible operations in job sources:\n" + "\n".join(
        offenders
    )
