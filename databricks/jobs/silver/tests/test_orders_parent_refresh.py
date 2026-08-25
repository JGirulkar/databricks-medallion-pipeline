"""Unit tests for orders conform with parent dimension refresh."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.unit
def test_parent_refresh_continues_when_products_fails() -> None:
    spark = MagicMock()
    calls: list[str] = []

    def _fake_run(
        _spark: MagicMock,
        entity: str,
        catalog: str = "de_assessment",
        parent_run_id: str | None = None,
        stream_runner: object = None,
    ) -> str:
        del catalog, parent_run_id, stream_runner
        calls.append(entity)
        if entity == "products":
            raise RuntimeError("products failed")
        return f"run-{entity}"

    with patch("silver.main.run_entity_conform", side_effect=_fake_run):
        from silver.main import run_orders_conform_with_parent_refresh

        run_id = run_orders_conform_with_parent_refresh(spark)

    assert calls == ["products", "customers", "orders"]
    assert run_id == "run-orders"


@pytest.mark.unit
def test_parent_refresh_raises_when_orders_fails() -> None:
    spark = MagicMock()

    def _fake_run(
        _spark: MagicMock,
        entity: str,
        catalog: str = "de_assessment",
        parent_run_id: str | None = None,
        stream_runner: object = None,
    ) -> str:
        del catalog, parent_run_id, stream_runner
        if entity == "orders":
            raise RuntimeError("orders failed")
        return f"run-{entity}"

    with patch("silver.main.run_entity_conform", side_effect=_fake_run):
        from silver.main import run_orders_conform_with_parent_refresh

        with pytest.raises(RuntimeError, match="orders failed"):
            run_orders_conform_with_parent_refresh(spark)
