from common.features import FEATURE_NAMES, compute_features, empty_profile, to_vector, update_profile
from common.rules import rule_score
from generator.simulator import PaymentSimulator
from scorer.scoring import score_transactions

BASE_TS = 1_790_000_000_000


def txn(**overrides):
    t = {
        "txn_id": "t1", "event_time": "2026-10-22T10:00:00.000Z", "event_ts_ms": BASE_TS,
        "payer_id": "U1", "payer_vpa": "u1@ybl", "payee_vpa": "shop@ybl", "payee_type": "MERCHANT",
        "merchant_category": "grocery", "amount": 400.0, "channel": "UPI_QR", "device_id": "d1",
        "city": "Mumbai", "lat": 19.07, "lon": 72.87,
    }
    t.update(overrides)
    return t


def profile_after(*txns):
    p = empty_profile()
    for t in txns:
        p = update_profile(t, p)
    return p


def test_first_transaction_has_no_history_flags():
    f = compute_features(txn(), empty_profile())
    assert f["is_new_device"] == 0 and f["is_new_payee"] == 0 and f["km_from_last"] == 0
    assert set(f) == set(FEATURE_NAMES)
    assert len(to_vector(f)) == len(FEATURE_NAMES)


def test_new_device_high_value_triggers_rule():
    history = [txn(txn_id=f"h{i}", event_ts_ms=BASE_TS + i * 3_600_000) for i in range(5)]
    f = compute_features(
        txn(event_ts_ms=BASE_TS + 6 * 3_600_000, device_id="stolen", amount=30_000, merchant_category="electronics"),
        profile_after(*history),
    )
    assert f["is_new_device"] == 1 and f["amount_ratio"] > 8
    score, reasons = rule_score(f)
    assert score >= 0.6 and any(r.startswith("new_device_high_value") for r in reasons)


def test_impossible_travel_triggers_rule():
    p = profile_after(txn())
    f = compute_features(txn(event_ts_ms=BASE_TS + 5 * 60_000, city="Delhi", lat=28.70, lon=77.10), p)
    assert f["km_from_last"] > 1000 and f["speed_kmh"] > 1000
    score, reasons = rule_score(f)
    assert score >= 0.6 and any(r.startswith("impossible_travel") for r in reasons)


def test_velocity_counts_only_last_minute():
    burst = [txn(txn_id=f"b{i}", event_ts_ms=BASE_TS + i * 5_000, payee_vpa=f"mule{i}@ybl", payee_type="P2P") for i in range(6)]
    p = profile_after(*burst)
    f = compute_features(txn(event_ts_ms=BASE_TS + 31_000, payee_vpa="mule9@ybl", payee_type="P2P"), p)
    assert f["txns_last_1m"] == 6
    later = compute_features(txn(event_ts_ms=BASE_TS + 30 * 60_000), p)
    assert later["txns_last_1m"] == 0 and later["txns_last_10m"] == 0


def test_normal_purchase_is_not_flagged():
    history = [txn(txn_id=f"h{i}", event_ts_ms=BASE_TS + i * 3_600_000) for i in range(5)]
    f = compute_features(txn(event_ts_ms=BASE_TS + 6 * 3_600_000, amount=520), profile_after(*history))
    assert rule_score(f) == (0.0, [])


def test_profile_lists_are_bounded():
    p = empty_profile()
    for i in range(200):
        p = update_profile(txn(event_ts_ms=BASE_TS + i, device_id=f"d{i}", payee_vpa=f"p{i}@ybl"), p)
    assert len(p["devices"]) <= 10 and len(p["payees"]) <= 50 and len(p["recent_ts"]) <= 50


def test_rules_catch_most_simulated_fraud_with_reasonable_precision():
    events = PaymentSimulator(seed=3, start_ts_ms=BASE_TS).generate(30_000)
    profiles: dict = {}
    score_transactions(events[:5_000], profiles)
    results = score_transactions(events[5_000:], profiles)
    tp = sum(r.is_alert and r.txn["label_is_fraud"] for r in results)
    fp = sum(r.is_alert and not r.txn["label_is_fraud"] for r in results)
    fn = sum(not r.is_alert and r.txn["label_is_fraud"] for r in results)
    assert tp / (tp + fp) > 0.6
    assert tp / (tp + fn) > 0.5


def test_model_failure_falls_back_to_rules():
    def broken_model(rows):
        raise TimeoutError("endpoint cold start")

    [r] = score_transactions([txn()], {}, model_fn=broken_model)
    assert r.scored_by == "rules" and "model_error" in r.extra


def test_model_score_is_used_when_available():
    [r] = score_transactions([txn()], {}, model_fn=lambda rows: [0.97 for _ in rows])
    assert r.scored_by == "model" and r.score == 0.97 and r.is_alert
