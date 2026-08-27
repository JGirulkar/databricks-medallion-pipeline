from __future__ import annotations

import pytest
from bronze.job_log import configure_job_logger, run_main


@pytest.mark.unit
def test_run_main_logs_and_reraises() -> None:
    logger = configure_job_logger("test.job_log")

    def ok() -> None:
        return None

    run_main(ok, logger)

    with pytest.raises(RuntimeError, match="boom"):
        def fail() -> None:
            raise RuntimeError("boom")

        run_main(fail, logger)
