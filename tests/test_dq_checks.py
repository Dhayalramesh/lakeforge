"""
Validates the data-quality gate: good records pass, each specific defect
type is caught with the right reason, and the pass/fail threshold on
rejection rate behaves correctly.
"""
from lakeforge.dq_checks import validate_records


def _good_record(**overrides):
    base = {
        "order_id": "O1",
        "customer_id": "C1000",
        "product_id": "P100",
        "quantity": 2,
        "unit_price": 799.0,
        "order_ts": "2026-09-01T10:00:00Z",
        "status": "completed",
    }
    base.update(overrides)
    return base


def test_all_valid_records_pass():
    records = [_good_record(order_id=f"O{i}") for i in range(10)]
    valid, result = validate_records(records)
    assert len(valid) == 10
    assert result.rejected_records == 0
    assert result.passed


def test_missing_customer_id_is_rejected():
    records = [_good_record(customer_id=None)]
    valid, result = validate_records(records)
    assert len(valid) == 0
    assert result.rejection_reasons == {"missing_customer_id": 1}


def test_negative_quantity_is_rejected():
    records = [_good_record(quantity=-3)]
    valid, result = validate_records(records)
    assert len(valid) == 0
    assert result.rejection_reasons == {"invalid_quantity": 1}


def test_non_numeric_price_is_rejected():
    records = [_good_record(unit_price="N/A")]
    valid, result = validate_records(records)
    assert len(valid) == 0
    assert result.rejection_reasons == {"invalid_unit_price": 1}


def test_missing_required_field_is_rejected():
    record = _good_record()
    del record["order_ts"]
    valid, result = validate_records([record])
    assert len(valid) == 0
    assert result.rejection_reasons == {"missing_field": 1}


def test_dq_gate_fails_above_10_percent_rejection():
    records = [_good_record(order_id=f"O{i}") for i in range(10)]
    records += [_good_record(order_id="Obad", customer_id=None) for _ in range(2)]
    valid, result = validate_records(records)
    assert result.rejection_rate > 0.10
    assert not result.passed


def test_dq_gate_passes_at_or_below_10_percent_rejection():
    records = [_good_record(order_id=f"O{i}") for i in range(10)]
    records += [_good_record(order_id="Obad", customer_id=None)]
    valid, result = validate_records(records)
    assert result.rejection_rate == 1 / 11
    assert result.passed
