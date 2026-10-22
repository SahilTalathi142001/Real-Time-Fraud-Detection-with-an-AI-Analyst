import collections

from generator.simulator import CITIES, FRAUD_PATTERNS, PaymentSimulator, haversine_km

REQUIRED_FIELDS = {
    "txn_id", "event_time", "event_ts_ms", "payer_id", "payer_vpa", "payee_vpa", "payee_type",
    "merchant_id", "merchant_category", "amount", "currency", "channel", "device_id", "device_os",
    "ip_address", "city", "lat", "lon", "label_is_fraud", "label_pattern",
}


def _events(n=20_000, seed=7, fraud_rate=0.02):
    return PaymentSimulator(num_users=300, fraud_rate=fraud_rate, seed=seed, start_ts_ms=1_700_000_000_000).generate(n)


def test_events_have_schema_and_are_time_ordered():
    events = _events(2_000)
    assert all(REQUIRED_FIELDS <= e.keys() for e in events)
    ts = [e["event_ts_ms"] for e in events]
    assert ts == sorted(ts)
    assert len({e["txn_id"] for e in events}) == len(events)
    assert all(e["amount"] >= 1 and e["currency"] == "INR" for e in events)


def test_same_seed_is_reproducible():
    assert _events(500, seed=1) == _events(500, seed=1)
    assert _events(500, seed=1) != _events(500, seed=2)


def test_all_fraud_patterns_are_injected_at_a_sensible_rate():
    events = _events()
    patterns = collections.Counter(e["label_pattern"] for e in events)
    assert set(FRAUD_PATTERNS) <= patterns.keys()
    fraud_share = sum(e["label_is_fraud"] for e in events) / len(events)
    assert 0.01 < fraud_share < 0.15
    assert all(e["label_is_fraud"] == (e["label_pattern"] != "none") for e in events)


def test_velocity_bursts_are_many_payments_within_a_minute():
    events = [e for e in _events() if e["label_pattern"] == "velocity_burst"]
    by_user = collections.defaultdict(list)
    for e in events:
        by_user[e["payer_id"]].append(e["event_ts_ms"])
    longest_burst = 0
    for ts in by_user.values():
        ts.sort()
        for i, t in enumerate(ts):
            longest_burst = max(longest_burst, sum(1 for u in ts[i:] if u - t <= 60_000))
    assert longest_burst >= 6


def test_geo_jumps_are_far_from_home():
    sim = PaymentSimulator(num_users=300, fraud_rate=0.02, seed=7, start_ts_ms=1_700_000_000_000)
    homes = {u.user_id: u.home_city for u in sim.users}
    jumps = [e for e in sim.generate(20_000) if e["label_pattern"] == "geo_jump"]
    assert jumps
    assert all(haversine_km(CITIES[homes[e["payer_id"]]], CITIES[e["city"]]) > 500 for e in jumps)


def test_new_device_high_value_uses_unseen_device_and_big_amount():
    sim = PaymentSimulator(num_users=300, fraud_rate=0.02, seed=7, start_ts_ms=1_700_000_000_000)
    events = sim.generate(20_000)
    known = {d for u in sim.users for d, _ in u.devices}
    hits = [e for e in events if e["label_pattern"] == "new_device_high_value"]
    assert hits
    assert all(e["device_id"] not in known and e["amount"] >= 20_000 for e in hits)
