import importlib
import io
import json
import tarfile
from pathlib import Path

import pytest
from aws_cdk.assertions import Match

from common.features import FEATURE_NAMES
from ml.dataset import build, rules_baseline, write_csv


def test_dataset_rows_match_feature_order():
    rows, labels, rule_scores = build(2_000, seed=5, start_ts_ms=1_780_000_000_000, fraud_rate=0.03)
    assert len(rows) == len(labels) == len(rule_scores) == 2_000
    assert all(len(r) == len(FEATURE_NAMES) for r in rows)
    assert 0 < sum(labels) < len(labels)
    baseline = rules_baseline(labels, rule_scores)
    assert 0 <= baseline["precision"] <= 1 and baseline["tp"] + baseline["fn"] == sum(labels)


def test_training_script_produces_servable_artifact(tmp_path: Path):
    pytest.importorskip("xgboost")
    from ml import train_xgb

    for split, seed in (("train", 1), ("validation", 2)):
        rows, labels, _ = build(6_000, seed=seed, start_ts_ms=1_780_000_000_000, fraud_rate=0.03)
        write_csv(tmp_path / split / f"{split}.csv", rows, labels)
    out = tmp_path / "out"
    out.mkdir()
    metrics = train_xgb.train(
        train_xgb.parse_args([
            "--train", str(tmp_path / "train"), "--validation", str(tmp_path / "validation"),
            "--model-dir", str(tmp_path / "model"), "--package-tar", str(out), "--num-round", "30",
        ])
    )
    assert metrics["validation_auc"] > 0.9
    assert 0.1 <= metrics["best_f1"]["threshold"] <= 0.95
    with tarfile.open(out / "model.tar.gz") as tar:
        assert tar.getnames() == ["xgboost-model"]
    assert json.loads((out / "metrics.json").read_text())["rows_train"] == 6_000


def test_roc_auc_matches_known_values():
    import numpy as np

    from ml.train_xgb import roc_auc

    y = np.array([0, 0, 1, 1])
    assert roc_auc(y, np.array([0.1, 0.4, 0.35, 0.8])) == pytest.approx(0.75)
    assert roc_auc(y, np.array([0.1, 0.2, 0.8, 0.9])) == 1.0


def test_no_endpoint_without_a_model(template):
    t = template("FraudMl")
    t.resource_count_is("AWS::SageMaker::Endpoint", 0)
    t.has_resource_properties("AWS::IAM::Role", {"RoleName": "fraud-analyst-sagemaker"})


def test_serverless_endpoint_with_a_model(model_template):
    t = model_template("FraudMl")
    t.has_resource_properties(
        "AWS::SageMaker::EndpointConfig",
        {"ProductionVariants": [Match.object_like({"ServerlessConfig": {"MemorySizeInMB": 2048, "MaxConcurrency": 5}})]},
    )
    t.has_resource_properties("AWS::SageMaker::Endpoint", {"EndpointName": "fraud-analyst-xgb"})
    t.has_resource_properties(
        "AWS::SageMaker::Model",
        {"PrimaryContainer": {"Image": Match.string_like_regexp("sagemaker-xgboost:1.7-1"), "ModelDataUrl": Match.string_like_regexp("model.tar.gz")}},
    )
    # Serverless only: no instance-backed variant that would bill by the hour.
    config = next(iter(t.find_resources("AWS::SageMaker::EndpointConfig").values()))
    assert "InstanceType" not in config["Properties"]["ProductionVariants"][0]


def test_scorer_uses_model_when_deployed(model_template, template):
    def scorer_env(tpl):
        fns = tpl("FraudStreaming").find_resources("AWS::Lambda::Function", {"Properties": {"FunctionName": "fraud-analyst-scorer"}})
        return next(iter(fns.values()))["Properties"]["Environment"]["Variables"]

    assert scorer_env(model_template)["MODEL_ENDPOINT"] == "fraud-analyst-xgb"
    assert scorer_env(model_template)["ALERT_THRESHOLD"] == "0.85"
    assert scorer_env(template)["MODEL_ENDPOINT"] == ""
    model_template("FraudStreaming").has_resource_properties(
        "AWS::IAM::Policy",
        {"PolicyDocument": {"Statement": Match.array_with([Match.object_like({"Action": "sagemaker:InvokeEndpoint"})])}},
    )


class _FakeRuntime:
    def __init__(self, payload: str):
        self.payload = payload
        self.calls = []

    def invoke_endpoint(self, **kwargs):
        self.calls.append(kwargs)
        return {"Body": io.BytesIO(self.payload.encode())}


@pytest.mark.parametrize("payload", ["0.1,0.9", "0.1\n0.9\n"])
def test_scorer_parses_endpoint_csv(monkeypatch, payload):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("MODEL_ENDPOINT", "fraud-analyst-xgb")
    scorer = importlib.reload(importlib.import_module("scorer.handler"))
    fake = _FakeRuntime(payload)
    monkeypatch.setattr(scorer, "sagemaker_runtime", fake)
    assert scorer.get_model_fn()([[1.0, 2.0], [3.0, 4.0]]) == [0.1, 0.9]
    assert fake.calls[0]["ContentType"] == "text/csv"
    assert fake.calls[0]["Body"] == "1.00000,2.00000\n3.00000,4.00000"


def test_scorer_rejects_mismatched_endpoint_output(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("MODEL_ENDPOINT", "fraud-analyst-xgb")
    scorer = importlib.reload(importlib.import_module("scorer.handler"))
    monkeypatch.setattr(scorer, "sagemaker_runtime", _FakeRuntime("0.5"))
    with pytest.raises(ValueError):
        scorer.invoke_model([[1.0], [2.0]])


def test_scorer_retries_serverless_model_error(monkeypatch):
    from botocore.exceptions import ClientError

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("MODEL_ENDPOINT", "fraud-analyst-xgb")
    scorer = importlib.reload(importlib.import_module("scorer.handler"))
    monkeypatch.setattr(scorer.time, "sleep", lambda s: None)

    class Flaky(_FakeRuntime):
        def invoke_endpoint(self, **kwargs):
            self.calls.append(kwargs)
            if len(self.calls) < 3:
                raise ClientError({"Error": {"Code": "ModelError", "Message": "could not get a response"}}, "InvokeEndpoint")
            return {"Body": io.BytesIO(self.payload.encode())}

    fake = Flaky("0.2,0.8")
    monkeypatch.setattr(scorer, "sagemaker_runtime", fake)
    assert scorer.invoke_model([[1.0], [2.0]]) == [0.2, 0.8]
    assert len(fake.calls) == 3
