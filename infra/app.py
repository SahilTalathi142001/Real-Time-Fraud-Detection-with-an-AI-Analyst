#!/usr/bin/env python3
"""CDK entry point. Run from the repo root: `cdk synth` / `cdk deploy --all`."""

import os

import aws_cdk as cdk

from infra.config import ProjectConfig
from infra.stacks.foundation_stack import FoundationStack


def build_app(context: dict | None = None) -> cdk.App:
    app = cdk.App(context=context)
    config = ProjectConfig.from_node(app.node)
    env = cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    )

    FoundationStack(app, "FraudFoundation", config=config, env=env)

    cdk.Tags.of(app).add("project", config.project_name)
    return app


if __name__ == "__main__":
    build_app().synth()
