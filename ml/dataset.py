"""Build labelled training data by replaying the simulator through the scorer's feature code."""

from __future__ import annotations

import csv
from pathlib import Path

from common.features import FEATURE_NAMES, compute_features, empty_profile, to_vector, update_profile
from common.rules import ALERT_THRESHOLD, rule_score
from generator.simulator import PaymentSimulator


def build(n_events: int, seed: int, start_ts_ms: int, fraud_rate: float = 0.01) -> tuple[list[list[float]], list[int], list[float]]:
    sim = PaymentSimulator(seed=seed, fraud_rate=fraud_rate, start_ts_ms=start_ts_ms)
    profiles: dict[str, dict] = {}
    rows, labels, rule_scores = [], [], []
    for txn in sim.generate(n_events):
        profile = profiles.get(txn["payer_id"]) or empty_profile()
        features = compute_features(txn, profile)
        profiles[txn["payer_id"]] = update_profile(txn, profile)
        rows.append(to_vector(features))
        labels.append(int(txn["label_is_fraud"]))
        rule_scores.append(rule_score(features)[0])
    return rows, labels, rule_scores


def write_csv(path: Path, rows: list[list[float]], labels: list[int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        for label, row in zip(labels, rows):
            w.writerow([label, *(round(v, 5) for v in row)])


def rules_baseline(labels: list[int], rule_scores: list[float]) -> dict:
    tp = sum(1 for y, s in zip(labels, rule_scores) if y and s >= ALERT_THRESHOLD)
    fp = sum(1 for y, s in zip(labels, rule_scores) if not y and s >= ALERT_THRESHOLD)
    fn = sum(1 for y, s in zip(labels, rule_scores) if y and s < ALERT_THRESHOLD)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"threshold": ALERT_THRESHOLD, "precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn}


__all__ = ["FEATURE_NAMES", "build", "write_csv", "rules_baseline"]
