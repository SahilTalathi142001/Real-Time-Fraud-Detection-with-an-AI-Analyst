"""Hand-written fraud rules: the phase 2 scorer, and the explanation layer after phase 3.

Rules are transparent and need no training, but they're brittle: thresholds are
guesses and they miss combinations. Phase 3 replaces the score with an ML model,
but we keep the rule *reasons* because analysts want to know why something was flagged.
"""

from __future__ import annotations

ALERT_THRESHOLD = 0.6


def rule_score(f: dict[str, float]) -> tuple[float, list[str]]:
    score = 0.0
    reasons: list[str] = []

    if f["txns_last_1m"] >= 4:
        score += 0.5 + 0.05 * min(f["txns_last_1m"] - 4, 6)
        reasons.append(f"velocity_burst: {int(f['txns_last_1m'])} payments in the previous minute")

    if f["is_new_device"] and f["amount_ratio"] >= 8:
        score += 0.7
        reasons.append(f"new_device_high_value: unseen device, amount {f['amount_ratio']:.0f}x usual")
    elif f["amount_ratio"] >= 15:
        score += 0.3
        reasons.append(f"unusually_high_amount: {f['amount_ratio']:.0f}x usual")

    if f["km_from_last"] >= 500 and f["speed_kmh"] >= 300:
        score += 0.7
        reasons.append(f"impossible_travel: {f['km_from_last']:.0f} km at {f['speed_kmh']:.0f} km/h")

    if f["is_collect"] and f["is_new_payee"]:
        score += 0.15
        reasons.append("collect_request_to_new_payee")

    if f["is_p2p"] and f["is_new_payee"] and f["txns_last_10m"] >= 3:
        score += 0.15
        reasons.append(f"p2p_fanout: new payee after {int(f['txns_last_10m'])} payments in 10 min")

    return min(score, 1.0), reasons
