"""XGBoost training script that runs *inside* a SageMaker container.

It works both as a SageMaker Training Job (script mode: data in SM_CHANNEL_*,
model goes to SM_MODEL_DIR) and as a SageMaker Processing Job (paths passed as
arguments, model.tar.gz written to the output path). It depends only on
xgboost and numpy, which ship with the sagemaker-xgboost container.

Input CSVs: no header, label in the first column, features in FEATURE_NAMES order.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import tarfile

import numpy as np
import xgboost as xgb


def load_csv(directory: str) -> tuple[np.ndarray, np.ndarray]:
    files = sorted(glob.glob(os.path.join(directory, "*.csv")))
    if not files:
        raise FileNotFoundError(f"no CSV files in {directory}")
    data = np.vstack([np.loadtxt(f, delimiter=",", ndmin=2) for f in files])
    return data[:, 1:], data[:, 0]


def roc_auc(y: np.ndarray, p: np.ndarray) -> float:
    order = np.argsort(p)
    ranks = np.empty(len(p))
    ranks[order] = np.arange(1, len(p) + 1)
    pos = y == 1
    n_pos, n_neg = pos.sum(), (~pos).sum()
    return float((ranks[pos].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg))


def precision_recall_at(y: np.ndarray, p: np.ndarray, threshold: float) -> dict:
    pred = p >= threshold
    tp = int((pred & (y == 1)).sum())
    fp = int((pred & (y == 0)).sum())
    fn = int((~pred & (y == 1)).sum())
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"threshold": threshold, "precision": precision, "recall": recall, "f1": f1, "tp": tp, "fp": fp, "fn": fn}


def best_threshold(y: np.ndarray, p: np.ndarray) -> dict:
    candidates = [precision_recall_at(y, p, t) for t in np.round(np.arange(0.1, 0.96, 0.05), 2)]
    return max(candidates, key=lambda m: m["f1"])


def train(args: argparse.Namespace) -> dict:
    x_train, y_train = load_csv(args.train)
    x_val, y_val = load_csv(args.validation)
    pos_weight = float((y_train == 0).sum() / max((y_train == 1).sum(), 1))

    params = {
        "objective": "binary:logistic",
        "eval_metric": ["auc", "aucpr"],  # the last one drives early stopping
        "max_depth": args.max_depth,
        "eta": args.eta,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        # Partial re-weighting: helps recall on the rare class without wrecking precision.
        "scale_pos_weight": pos_weight ** 0.5,
        "tree_method": "hist",
        "seed": 42,
    }
    dtrain, dval = xgb.DMatrix(x_train, label=y_train), xgb.DMatrix(x_val, label=y_val)
    booster = xgb.train(
        params, dtrain, num_boost_round=args.num_round, evals=[(dtrain, "train"), (dval, "validation")],
        early_stopping_rounds=25, verbose_eval=25,
    )

    p_val = booster.predict(dval, iteration_range=(0, booster.best_iteration + 1))
    best = best_threshold(y_val, p_val)
    metrics = {
        "rows_train": int(len(y_train)),
        "rows_validation": int(len(y_val)),
        "fraud_rate_train": float(y_train.mean()),
        "best_iteration": int(booster.best_iteration),
        "validation_auc": roc_auc(y_val, p_val),
        "at_0.5": precision_recall_at(y_val, p_val, 0.5),
        "best_f1": best,
        "feature_importance_gain": {
            k: round(v, 2) for k, v in sorted(booster.get_score(importance_type="gain").items(), key=lambda kv: -kv[1])
        },
    }
    # SageMaker parses these lines into training-job metrics (see MetricDefinitions).
    print(f"validation-auc={metrics['validation_auc']:.5f};")
    print(f"validation-f1={best['f1']:.5f}; validation-precision={best['precision']:.5f}; validation-recall={best['recall']:.5f};")
    print(json.dumps(metrics, indent=2))

    booster = booster[: booster.best_iteration + 1]
    os.makedirs(args.model_dir, exist_ok=True)
    # The sagemaker-xgboost serving container loads a file named "xgboost-model".
    booster.save_model(os.path.join(args.model_dir, "xgboost-model"))
    with open(os.path.join(args.model_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    if args.package_tar:
        with tarfile.open(os.path.join(args.package_tar, "model.tar.gz"), "w:gz") as tar:
            tar.add(os.path.join(args.model_dir, "xgboost-model"), arcname="xgboost-model")
        with open(os.path.join(args.package_tar, "metrics.json"), "w") as f:
            json.dump(metrics, f, indent=2)
    return metrics


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--train", default=os.environ.get("SM_CHANNEL_TRAIN", "/opt/ml/processing/train"))
    p.add_argument("--validation", default=os.environ.get("SM_CHANNEL_VALIDATION", "/opt/ml/processing/validation"))
    p.add_argument("--model-dir", default=os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
    p.add_argument("--package-tar", default="", help="also write model.tar.gz here (processing-job mode)")
    p.add_argument("--num-round", "--num_round", type=int, default=300)
    p.add_argument("--max-depth", "--max_depth", type=int, default=6)
    p.add_argument("--eta", type=float, default=0.1)
    args, _ = p.parse_known_args(argv)
    return args


if __name__ == "__main__":
    train(parse_args())
