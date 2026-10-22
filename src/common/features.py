"""Feature engineering shared by the real-time scorer and model training.

Each payer has a small rolling *profile* (stored in DynamoDB by the scorer).
`compute_features` turns (transaction, profile-before-transaction) into a flat
dict of numbers, and `update_profile` folds the transaction into the profile.
Training (phase 3) replays the generator through exactly the same two
functions, so the model sees the same features online and offline.
"""

from __future__ import annotations

import math

FEATURE_NAMES: list[str] = [
    "amount",
    "amount_ratio",
    "is_new_device",
    "is_new_payee",
    "secs_since_last",
    "txns_last_1m",
    "txns_last_10m",
    "km_from_last",
    "speed_kmh",
    "hour_of_day",
    "is_night",
    "is_collect",
    "is_p2p",
    "category_risk",
    "profile_txn_count",
]

CATEGORY_RISK: dict[str, float] = {
    "grocery": 0.1,
    "food_delivery": 0.1,
    "recharge": 0.2,
    "utilities": 0.1,
    "fuel": 0.2,
    "travel": 0.4,
    "electronics": 0.7,
    "gaming": 0.5,
    "jewellery": 0.8,
    "p2p": 0.5,
}

PRIOR_MEAN_AMOUNT = 500.0
PRIOR_WEIGHT = 3
MAX_DEVICES = 10
MAX_PAYEES = 50
RECENT_WINDOW_MS = 10 * 60_000
NO_HISTORY_SECS = 7 * 86_400.0
IST_OFFSET_SECS = 19_800


def empty_profile() -> dict:
    return {
        "txn_count": 0,
        "amount_mean": 0.0,
        "last_ts_ms": 0,
        "last_lat": None,
        "last_lon": None,
        "devices": [],
        "payees": [],
        "recent_ts": [],
    }


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(min(1.0, h)))


def compute_features(txn: dict, profile: dict) -> dict[str, float]:
    ts = int(txn["event_ts_ms"])
    amount = float(txn["amount"])
    n = int(profile["txn_count"])
    has_history = n > 0

    smoothed_mean = (profile["amount_mean"] * n + PRIOR_MEAN_AMOUNT * PRIOR_WEIGHT) / (n + PRIOR_WEIGHT)
    secs_since_last = (ts - profile["last_ts_ms"]) / 1000 if has_history else NO_HISTORY_SECS
    secs_since_last = max(0.0, secs_since_last)

    km = 0.0
    if has_history and profile["last_lat"] is not None:
        km = haversine_km(profile["last_lat"], profile["last_lon"], float(txn["lat"]), float(txn["lon"]))
    # Floor the elapsed time at one minute so two payments a second apart don't look like warp speed.
    speed = km / (max(secs_since_last, 60.0) / 3600)

    recent = [t for t in profile["recent_ts"] if 0 <= ts - t <= RECENT_WINDOW_MS]
    hour = int((ts / 1000 + IST_OFFSET_SECS) // 3600 % 24)
    category = txn.get("merchant_category") or "p2p"

    return {
        "amount": amount,
        "amount_ratio": amount / max(smoothed_mean, 1.0),
        "is_new_device": float(has_history and txn["device_id"] not in profile["devices"]),
        "is_new_payee": float(has_history and txn["payee_vpa"] not in profile["payees"]),
        "secs_since_last": secs_since_last,
        "txns_last_1m": float(sum(1 for t in recent if ts - t <= 60_000)),
        "txns_last_10m": float(len(recent)),
        "km_from_last": km,
        "speed_kmh": speed,
        "hour_of_day": float(hour),
        "is_night": float(hour < 6),
        "is_collect": float(txn.get("channel") == "UPI_COLLECT"),
        "is_p2p": float(txn.get("payee_type") == "P2P"),
        "category_risk": CATEGORY_RISK.get(category, 0.3),
        "profile_txn_count": float(n),
    }


def update_profile(txn: dict, profile: dict) -> dict:
    ts = int(txn["event_ts_ms"])
    n = int(profile["txn_count"])
    devices = [d for d in profile["devices"] if d != txn["device_id"]] + [txn["device_id"]]
    payees = [p for p in profile["payees"] if p != txn["payee_vpa"]] + [txn["payee_vpa"]]
    recent = [t for t in profile["recent_ts"] if 0 <= ts - t <= RECENT_WINDOW_MS] + [ts]
    return {
        "txn_count": n + 1,
        "amount_mean": (profile["amount_mean"] * n + float(txn["amount"])) / (n + 1),
        "last_ts_ms": max(ts, int(profile["last_ts_ms"])),
        "last_lat": float(txn["lat"]),
        "last_lon": float(txn["lon"]),
        "devices": devices[-MAX_DEVICES:],
        "payees": payees[-MAX_PAYEES:],
        "recent_ts": recent[-50:],
    }


def to_vector(features: dict[str, float]) -> list[float]:
    return [float(features[name]) for name in FEATURE_NAMES]
