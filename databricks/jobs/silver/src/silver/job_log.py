"""Structured logging for Silver Databricks entrypoints."""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable


def configure_job_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        logger.addHandler(handler)
        logger.propagate = False
    return logger


def run_main(main_fn: Callable[[], None], logger: logging.Logger) -> None:
    logger.info("job_start")
    try:
        main_fn()
    except Exception:
        logger.exception("job_failed")
        raise
    logger.info("job_success")
