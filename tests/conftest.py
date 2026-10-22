import json
from pathlib import Path

import aws_cdk as cdk
import pytest

from infra.app import build_app

TEST_ENV = {"CDK_DEFAULT_ACCOUNT": "123456789012", "CDK_DEFAULT_REGION": "ap-south-1"}  # pragma: allowlist secret


@pytest.fixture(scope="session")
def cdk_app() -> cdk.App:
    mp = pytest.MonkeyPatch()
    for k, v in TEST_ENV.items():
        mp.setenv(k, v)
    context = json.loads((Path(__file__).parents[1] / "cdk.json").read_text())["context"]
    app = build_app(context)
    mp.undo()
    return app


@pytest.fixture(scope="session")
def template(cdk_app):
    """Return a factory: template("FraudFoundation") -> assertions.Template."""
    cache = {}

    def _get(stack_name: str) -> cdk.assertions.Template:
        if stack_name not in cache:
            stack = cdk_app.node.find_child(stack_name)
            cache[stack_name] = cdk.assertions.Template.from_stack(stack)
        return cache[stack_name]

    return _get
