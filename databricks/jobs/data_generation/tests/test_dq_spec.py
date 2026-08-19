import importlib.util
from pathlib import Path

import pytest

MODULE = Path(__file__).resolve().parents[1] / "src" / "generate_sample_data.py"
spec = importlib.util.spec_from_file_location("generate_sample_data", MODULE)
assert spec and spec.loader
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


@pytest.mark.unit
def test_dq_issue_counts_documented() -> None:
    """Sanity: intentional issue counts match spec."""
    expected = mod.DQ_ISSUE_COUNTS
    assert sum(expected.values()) == 460  # subset of ~700 total issue rows


@pytest.mark.unit
def test_generate_writes_csvs(tmp_path: Path) -> None:
    stats = mod.generate(tmp_path)
    assert (tmp_path / "customers.csv").exists()
    assert (tmp_path / "products.csv").exists()
    assert (tmp_path / "orders.csv").exists()
    assert stats["customers"] > mod.BASE_CUSTOMERS
    assert stats["orders"] > mod.BASE_ORDERS
