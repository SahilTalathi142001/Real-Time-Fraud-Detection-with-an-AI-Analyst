import json
from pathlib import Path

import aws_cdk as cdk
import pytest

from infra.app import build_app

TEST_ENV = {"CDK_DEFAULT_ACCOUNT": "123456789012", "CDK_DEFAULT_REGION": "ap-south-1"}  # pragma: allowlist secret
TEST_MODEL_URL = "s3://test-artifacts/ml/output/fraud-xgb-test/model.tar.gz"
TEST_MODEL_IMAGE = "720646828776.dkr.ecr.ap-south-1.amazonaws.com/sagemaker-xgboost:1.7-1"  # pragma: allowlist secret


def _make_app(extra_context: dict) -> cdk.App:
    mp = pytest.MonkeyPatch()
    for k, v in TEST_ENV.items():
        mp.setenv(k, v)
    context = json.loads((Path(__file__).parents[1] / "cdk.json").read_text())["context"]
    app = build_app({**context, **extra_context})
    mp.undo()
    return app


def _template_factory(app: cdk.App):
    cache = {}

    def _get(stack_name: str) -> cdk.assertions.Template:
        if stack_name not in cache:
            cache[stack_name] = cdk.assertions.Template.from_stack(app.node.find_child(stack_name))
        return cache[stack_name]

    return _get


@pytest.fixture(scope="session")
def template():
    """Templates for an app with no trained model yet (rules-only scorer)."""
    return _template_factory(_make_app({"skipModel": True}))


@pytest.fixture(scope="session")
def model_template():
    """Templates for an app with a trained model (serverless endpoint + model scorer)."""
    return _template_factory(
        _make_app({"modelDataUrl": TEST_MODEL_URL, "modelImage": TEST_MODEL_IMAGE, "modelThreshold": "0.85"})
    )
