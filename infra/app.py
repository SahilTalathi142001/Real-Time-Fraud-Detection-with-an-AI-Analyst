#!/usr/bin/env python3
"""CDK entry point. Run from the repo root: `cdk synth` / `cdk deploy --all`."""

import json
import os
from pathlib import Path

import aws_cdk as cdk

from infra.config import ProjectConfig
from infra.stacks.analyst_stack import AnalystStack
from infra.stacks.foundation_stack import FoundationStack
from infra.stacks.lakehouse_stack import LakehouseStack
from infra.stacks.ml_stack import MlStack
from infra.stacks.monitoring_stack import MonitoringStack
from infra.stacks.streaming_stack import StreamingStack

ROOT = Path(__file__).resolve().parents[1]
LATEST_MODEL = ROOT / "ml" / "latest_model.json"
RUNTIME_ZIP = ROOT / "build" / "analyst_runtime.zip"


def load_model_info(app: cdk.App) -> dict:
    """Model location from -c modelDataUrl=... or from the file `python -m ml.pipeline train` writes."""
    if app.node.try_get_context("modelDataUrl"):
        return {
            "model_data_url": app.node.try_get_context("modelDataUrl"),
            "image": app.node.try_get_context("modelImage"),
            "threshold": float(app.node.try_get_context("modelThreshold") or 0.5),
        }
    if app.node.try_get_context("skipModel") or not LATEST_MODEL.exists():
        return {}
    return json.loads(LATEST_MODEL.read_text())


def runtime_zip_path(app: cdk.App) -> str | None:
    """The AgentCore code zip from `python -m agent package` (or -c analystRuntimeZip=...)."""
    path = Path(app.node.try_get_context("analystRuntimeZip") or RUNTIME_ZIP)
    return str(path) if path.exists() else None


def build_app(context: dict | None = None) -> cdk.App:
    app = cdk.App(context=context)
    config = ProjectConfig.from_node(app.node)
    env = cdk.Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION"),
    )

    foundation = FoundationStack(app, "FraudFoundation", config=config, env=env)
    model = load_model_info(app)
    ml = MlStack(
        app,
        "FraudMl",
        config=config,
        artifacts_bucket=foundation.artifacts_bucket,
        model_data_url=model.get("model_data_url"),
        model_image=model.get("image"),
        env=env,
    )
    lakehouse = LakehouseStack(
        app,
        "FraudLakehouse",
        config=config,
        lake_bucket=foundation.lake_bucket,
        athena_results_bucket=foundation.athena_results_bucket,
        database_name=foundation.glue_database_name,
        env=env,
    )
    streaming = StreamingStack(
        app,
        "FraudStreaming",
        config=config,
        model_endpoint_name=ml.endpoint_name if ml.has_endpoint else None,
        alert_threshold=float(model.get("threshold", 0.6)) if ml.has_endpoint else 0.6,
        lake_queue=lakehouse.lake_buffer,
        env=env,
    )
    analyst = AnalystStack(
        app,
        "FraudAnalyst",
        config=config,
        artifacts_bucket=foundation.artifacts_bucket,
        lake_bucket=foundation.lake_bucket,
        athena_results_bucket=foundation.athena_results_bucket,
        database_name=foundation.glue_database_name,
        athena_workgroup=lakehouse.workgroup_name,
        alerts_table=streaming.alerts_table,
        model_id=config.analyst_model_id,
        runtime_zip=runtime_zip_path(app),
        env=env,
    )
    MonitoringStack(
        app,
        "FraudMonitoring",
        config=config,
        api=streaming.api,
        payments_queue=streaming.payments_queue,
        payments_dlq=streaming.payments_dlq,
        lake_buffer=lakehouse.lake_buffer,
        lake_buffer_dlq=lakehouse.lake_buffer_dlq,
        ingest_fn=streaming.ingest_fn,
        scorer_fn=streaming.scorer_fn,
        loader_fn=lakehouse.loader_fn,
        endpoint_name=ml.endpoint_name if ml.has_endpoint else None,
        glue_job_name=lakehouse.glue_job_name,
        athena_workgroup=lakehouse.workgroup_name,
        runtime_name=analyst.runtime_name,
        model_id=config.analyst_model_id,
        env=env,
    )

    # ml/latest_model.json and build/analyst_runtime.zip are git-ignored, so a fresh clone has neither.
    # Deploying then would delete a live endpoint or runtime, so say so loudly.
    if not ml.has_endpoint:
        cdk.Annotations.of(ml).add_warning_v2(
            "fraud:no-model",
            "No trained model: FraudMl gets no endpoint and the scorer uses rules only. If a model was "
            "trained before, run `python -m ml.pipeline use-latest` first (or `train` on a new account).",
        )
    if not analyst.runtime_name:
        cdk.Annotations.of(analyst).add_warning_v2(
            "fraud:no-runtime-zip",
            "No build/analyst_runtime.zip: FraudAnalyst gets no AgentCore runtime. Run `python -m agent package` first.",
        )

    cdk.Tags.of(app).add("project", config.project_name)
    return app


if __name__ == "__main__":
    build_app().synth()
