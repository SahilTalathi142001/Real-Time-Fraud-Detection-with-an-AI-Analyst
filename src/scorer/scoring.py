"""Pure scoring logic (no AWS calls) so it can be unit-tested and reused offline."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Sequence

from common.features import compute_features, empty_profile, to_vector, update_profile
from common.rules import ALERT_THRESHOLD, rule_score

ModelFn = Callable[[Sequence[Sequence[float]]], list[float]]


@dataclass
class ScoredTxn:
    txn: dict
    features: dict[str, float]
    rule_score: float
    reasons: list[str]
    model_score: float | None = None
    score: float = 0.0
    is_alert: bool = False
    scored_by: str = "rules"
    extra: dict = field(default_factory=dict)


def score_transactions(
    txns: Sequence[dict],
    profiles: dict[str, dict],
    model_fn: ModelFn | None = None,
    threshold: float = ALERT_THRESHOLD,
) -> list[ScoredTxn]:
    """Score txns in order, updating `profiles` (keyed by payer_id) in place."""
    results: list[ScoredTxn] = []
    for txn in txns:
        payer = txn["payer_id"]
        profile = profiles.get(payer) or empty_profile()
        features = compute_features(txn, profile)
        r_score, reasons = rule_score(features)
        results.append(ScoredTxn(txn=txn, features=features, rule_score=r_score, reasons=reasons))
        profiles[payer] = update_profile(txn, profile)

    model_scores: list[float] | None = None
    if model_fn is not None and results:
        try:
            model_scores = model_fn([to_vector(r.features) for r in results])
        except Exception as exc:  # fall back to rules rather than drop payments
            for r in results:
                r.extra["model_error"] = str(exc)[:200]

    for i, r in enumerate(results):
        if model_scores is not None:
            r.model_score = float(model_scores[i])
            r.score = r.model_score
            r.scored_by = "model"
        else:
            r.score = r.rule_score
        r.is_alert = r.score >= threshold
    return results
