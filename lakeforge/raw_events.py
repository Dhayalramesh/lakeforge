"""
Generates synthetic raw order-event data the way a real upstream system
would land it in a data lake: one small JSON file per batch, landing in
an S3 "raw/" prefix partitioned by event date -- messy on purpose, with
a few malformed records mixed in, so the ingestion job downstream has
something real to validate against rather than a suspiciously clean feed.
"""
import json
import random
from datetime import date, timedelta

RNG_SEED = 42

PRODUCTS = [
    {"product_id": f"P{100+i}", "name": name, "category": cat, "price": price}
    for i, (name, cat, price) in enumerate([
        ("Wireless Mouse", "Electronics", 799.0),
        ("Mechanical Keyboard", "Electronics", 2499.0),
        ("Yoga Mat", "Fitness", 999.0),
        ("Protein Shaker", "Fitness", 299.0),
        ("Notebook Set", "Stationery", 149.0),
        ("Desk Lamp", "Home", 1299.0),
        ("Water Bottle", "Fitness", 449.0),
        ("USB-C Cable", "Electronics", 349.0),
    ])
]

CUSTOMERS = [f"C{1000+i}" for i in range(60)]


def _make_order_event(order_id: int, event_date: date, rng: random.Random) -> dict:
    product = rng.choice(PRODUCTS)
    qty = rng.randint(1, 4)
    event = {
        "order_id": f"O{order_id}",
        "customer_id": rng.choice(CUSTOMERS),
        "product_id": product["product_id"],
        "quantity": qty,
        "unit_price": product["price"],
        "order_ts": f"{event_date.isoformat()}T{rng.randint(0, 23):02d}:{rng.randint(0, 59):02d}:00Z",
        "status": rng.choices(["completed", "cancelled", "refunded"], weights=[90, 7, 3])[0],
    }
    return event


def generate_raw_batches(
    start_date: date = date(2026, 9, 1),
    n_days: int = 7,
    orders_per_day: int = 80,
    malformed_rate: float = 0.03,
    seed: int = RNG_SEED,
) -> dict:
    """
    Returns {s3_key: json_bytes} for a batch of raw order-event files, one
    per day, each landing at raw/dt=YYYY-MM-DD/orders.json -- the same
    prefix convention a Kinesis Firehose or Glue crawler would expect.

    A small fraction of records are deliberately malformed (missing
    customer_id, negative quantity, non-numeric price) to give the
    downstream Glue-style ingestion job real validation work to do.
    """
    rng = random.Random(seed)
    batches = {}
    order_counter = 1

    for day_offset in range(n_days):
        event_date = start_date + timedelta(days=day_offset)
        records = []
        for _ in range(orders_per_day):
            event = _make_order_event(order_counter, event_date, rng)
            order_counter += 1

            if rng.random() < malformed_rate:
                defect = rng.choice(["missing_customer", "negative_qty", "bad_price"])
                if defect == "missing_customer":
                    event["customer_id"] = None
                elif defect == "negative_qty":
                    event["quantity"] = -event["quantity"]
                elif defect == "bad_price":
                    event["unit_price"] = "N/A"

            records.append(event)

        key = f"raw/dt={event_date.isoformat()}/orders.json"
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        batches[key] = body

    return batches
