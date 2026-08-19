"""Generate sample e-commerce CSVs with intentional DQ issues."""

from __future__ import annotations

import argparse
import random
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

Faker.seed(42)
random.seed(42)

# Documented intentional issue counts (subset of ~700 total flagged rows).
DQ_ISSUE_COUNTS = {
    "null_emails": 50,
    "duplicate_customer_ids": 10,
    "null_order_customer_id": 100,
    "null_order_product_id": 200,
    "orphan_customer_id": 50,
    "orphan_product_id": 30,
    "duplicate_order_ids": 20,
}

BASE_CUSTOMERS = 500
BASE_PRODUCTS = 100
BASE_ORDERS = 2000


def _customer_rows(fake: Faker) -> list[dict]:
    rows: list[dict] = []
    for i in range(1, BASE_CUSTOMERS + 1):
        rows.append(
            {
                "customer_id": f"C{i:05d}",
                "customer_name": fake.name(),
                "email": fake.email(),
                "country": fake.country_code(),
                "signup_date": fake.date_between(
                    start_date=date(2020, 1, 1), end_date=date(2025, 12, 31)
                ).isoformat(),
                "customer_segment": random.choice(
                    ["Bronze", "Silver", "Gold", "Platinum"]
                ),
                "lifetime_value": round(random.uniform(100, 5000), 2),
            }
        )
    return rows


def _product_rows(fake: Faker) -> list[dict]:
    rows: list[dict] = []
    for i in range(1, BASE_PRODUCTS + 1):
        price = round(random.uniform(10, 500), 2)
        cost = round(price * random.uniform(0.4, 0.8), 2)
        rows.append(
            {
                "product_id": f"P{i:04d}",
                "product_name": fake.catch_phrase(),
                "category": random.choice(
                    ["Electronics", "Apparel", "Home", "Sports", "Books"]
                ),
                "price": price,
                "cost": cost,
                "stock_quantity": random.randint(0, 500),
                "reorder_level": random.randint(10, 50),
            }
        )
    return rows


def _order_rows(
    fake: Faker,
    customer_ids: list[str],
    product_ids: list[str],
) -> list[dict]:
    rows: list[dict] = []
    for i in range(1, BASE_ORDERS + 1):
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)
        order_date = fake.date_between(
            start_date=date(2023, 1, 1), end_date=date(2025, 12, 31)
        )
        rows.append(
            {
                "order_id": f"O{i:06d}",
                "customer_id": random.choice(customer_ids),
                "order_date": order_date.isoformat(),
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": round(quantity * unit_price, 2),
                "order_status": random.choice(
                    ["completed", "pending", "cancelled", "returned"]
                ),
                "payment_date": (order_date + timedelta(days=random.randint(0, 3))).isoformat(),
            }
        )
    return rows


def _inject_customer_issues(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    null_idx = out.sample(n=DQ_ISSUE_COUNTS["null_emails"], random_state=1).index
    out.loc[null_idx, "email"] = None

    dup_source = out.iloc[0: DQ_ISSUE_COUNTS["duplicate_customer_ids"]].copy()
    dup_source["customer_id"] = out.iloc[0]["customer_id"]
    return pd.concat([out, dup_source], ignore_index=True)


def _inject_order_issues(
    df: pd.DataFrame,
    valid_customer_ids: set[str],
    valid_product_ids: set[str],
) -> pd.DataFrame:
    out = df.copy()
    n = len(out)

    for count, col in (
        (DQ_ISSUE_COUNTS["null_order_customer_id"], "customer_id"),
        (DQ_ISSUE_COUNTS["null_order_product_id"], "product_id"),
    ):
        idx = out.sample(n=count, random_state=col.__hash__() & 0xFFFF).index
        out.loc[idx, col] = None

    orphan_customers = [f"ORPHAN_C{i:03d}" for i in range(DQ_ISSUE_COUNTS["orphan_customer_id"])]
    orphan_products = [f"ORPHAN_P{i:03d}" for i in range(DQ_ISSUE_COUNTS["orphan_product_id"])]
    for i, cid in enumerate(orphan_customers):
        out.at[i % n, "customer_id"] = cid
    for i, pid in enumerate(orphan_products):
        out.at[(i + 17) % n, "product_id"] = pid

    dup_orders = out.iloc[: DQ_ISSUE_COUNTS["duplicate_order_ids"]].copy()
    dup_orders["order_id"] = out.iloc[0]["order_id"]
    out = pd.concat([out, dup_orders], ignore_index=True)

    # Sanity: orphans must not resolve to valid FK sets
    assert not any(c in valid_customer_ids for c in orphan_customers)
    assert not any(p in valid_product_ids for p in orphan_products)
    return out


def generate(output_dir: Path) -> dict[str, int]:
    fake = Faker()
    customers = _inject_customer_issues(pd.DataFrame(_customer_rows(fake)))
    products = pd.DataFrame(_product_rows(fake))
    customer_ids = customers["customer_id"].astype(str).tolist()
    product_ids = products["product_id"].astype(str).tolist()
    orders = _inject_order_issues(
        pd.DataFrame(_order_rows(fake, customer_ids, product_ids)),
        valid_customer_ids=set(customers["customer_id"].astype(str)),
        valid_product_ids=set(product_ids),
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    customers.to_csv(output_dir / "customers.csv", index=False)
    products.to_csv(output_dir / "products.csv", index=False)
    orders.to_csv(output_dir / "orders.csv", index=False)

    return {
        "customers": len(customers),
        "products": len(products),
        "orders": len(orders),
        **DQ_ISSUE_COUNTS,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate assessment sample CSVs")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parents[4] / "data",
    )
    args = parser.parse_args()
    stats = generate(args.output_dir)
    print(f"Wrote CSVs to {args.output_dir}: {stats}")


if __name__ == "__main__":
    main()
