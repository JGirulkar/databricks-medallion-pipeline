"""Ensure silver package imports resolve on Databricks workspace uploads."""

from __future__ import annotations

import os
import sys


def setup_silver_src_path() -> None:
    src_root = os.environ.get("SILVER_SRC_ROOT")
    if src_root and src_root not in sys.path:
        sys.path.insert(0, src_root)
