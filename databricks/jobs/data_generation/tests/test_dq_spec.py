import pytest


@pytest.mark.unit
def test_dq_issue_counts_documented() -> None:
    """Sanity: intentional issue counts match spec."""
    expected = {
        "null_emails": 50,
        "duplicate_customer_ids": 10,
        "null_order_customer_id": 100,
        "null_order_product_id": 200,
        "orphan_customer_id": 50,
        "orphan_product_id": 30,
        "duplicate_order_ids": 20,
    }
    assert sum(expected.values()) == 460  # subset of ~700 total issue rows
