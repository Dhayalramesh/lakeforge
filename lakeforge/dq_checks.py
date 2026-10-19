"""
Data-quality gate the Glue-style ingestion job runs before any record is
allowed into the processed zone -- the same discipline a real pipeline
needs, since a data lake with no validation just becomes a bigger place
to store bad data faster.
"""
from dataclasses import dataclass, field


@dataclass
class DQResult:
    total_records: int
    valid_records: int
    rejected_records: int
    rejection_reasons: dict = field(default_factory=dict)

    @property
    def rejection_rate(self) -> float:
        return self.rejected_records / self.total_records if self.total_records else 0.0

    @property
    def passed(self) -> bool:
        """Pipeline fails the DQ gate if more than 10% of a batch is bad --
        a real-world threshold: a few bad records get quarantined, a
        batch that's mostly broken signals an upstream problem worth
        stopping the pipeline for instead of silently loading garbage."""
        return self.rejection_rate <= 0.10


def validate_records(records: list[dict]) -> tuple[list[dict], DQResult]:
    """
    Returns (valid_records, DQResult). A record is rejected if:
    - customer_id is missing/null
    - quantity is not a positive integer
    - unit_price is not a positive number
    - required fields are absent entirely
    """
    valid = []
    reasons: dict = {}

    def reject(reason: str):
        reasons[reason] = reasons.get(reason, 0) + 1

    for r in records:
        required = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "order_ts", "status"]
        if any(field not in r for field in required):
            reject("missing_field")
            continue
        if r["customer_id"] in (None, ""):
            reject("missing_customer_id")
            continue
        if not isinstance(r["quantity"], int) or r["quantity"] <= 0:
            reject("invalid_quantity")
            continue
        if not isinstance(r["unit_price"], (int, float)) or r["unit_price"] <= 0:
            reject("invalid_unit_price")
            continue
        valid.append(r)

    result = DQResult(
        total_records=len(records),
        valid_records=len(valid),
        rejected_records=len(records) - len(valid),
        rejection_reasons=reasons,
    )
    return valid, result
