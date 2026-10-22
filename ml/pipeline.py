"""Laptop-side ML pipeline: build data, train on SageMaker, smoke-test the endpoint.

    python -m ml.pipeline build-data          # simulate, featurize, upload CSVs to S3
    python -m ml.pipeline train               # SageMaker job; writes ml/latest_model.json
    cdk deploy FraudMl FraudStreaming         # creates the serverless endpoint, wires the scorer
    python -m ml.pipeline invoke              # send a few rows to the endpoint

`train --backend auto` uses a SageMaker Training Job when the account has
training-instance quota, otherwise a SageMaker Processing Job running the same
script (new accounts often start with 0 training quota).
"""

from __future__ import annotations

import argparse
import io
import json
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from common.features import FEATURE_NAMES
from ml.dataset import build, rules_baseline, write_csv

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "ml"
LATEST = ROOT / "ml" / "latest_model.json"
XGBOOST_VERSION = "1.7-1"
# Account that hosts the sagemaker-xgboost image, per region (from the SageMaker Python SDK image config).
XGBOOST_REGISTRY = {
    "ap-south-1": "720646828776",  # pragma: allowlist secret
    "us-east-1": "683313688378",
    "us-west-2": "246618743249",
    "eu-west-1": "141502667606",
    "ap-southeast-1": "121021644041",
}
TRAINING_QUOTA_CODE = "L-611FA074"  # "ml.m5.large for training job usage"


def xgboost_image(region: str) -> str:
    return f"{XGBOOST_REGISTRY[region]}.dkr.ecr.{region}.amazonaws.com/sagemaker-xgboost:{XGBOOST_VERSION}"


def stack_outputs(session: boto3.Session, stack: str) -> dict[str, str]:
    desc = session.client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in desc.get("Outputs", [])}


def cmd_build_data(args, session: boto3.Session) -> None:
    bucket = stack_outputs(session, "FraudFoundation")["ArtifactsBucketName"]
    s3 = session.client("s3")
    splits = {"train": (args.train_events, 101, 1_780_000_000_000), "validation": (args.validation_events, 202, 1_785_000_000_000)}
    for split, (n, seed, start) in splits.items():
        rows, labels, rule_scores = build(n, seed=seed, start_ts_ms=start)
        path = DATA_DIR / split / f"{split}.csv"
        write_csv(path, rows, labels)
        s3.upload_file(str(path), bucket, f"ml/data/{split}/{split}.csv")
        print(f"{split}: {n} rows, {sum(labels)} fraud ({sum(labels) / n:.2%}) -> s3://{bucket}/ml/data/{split}/")
        if split == "validation":
            baseline = rules_baseline(labels, rule_scores)
            (DATA_DIR / "rules_baseline.json").write_text(json.dumps(baseline, indent=2))
            print(f"rules baseline on validation: precision {baseline['precision']:.3f} recall {baseline['recall']:.3f} f1 {baseline['f1']:.3f}")
    s3.upload_file(str(ROOT / "ml" / "train_xgb.py"), bucket, "ml/code/train_xgb.py")
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(ROOT / "ml" / "train_xgb.py", arcname="train_xgb.py")
    s3.put_object(Bucket=bucket, Key="ml/code/sourcedir.tar.gz", Body=buf.getvalue())


def _training_quota(session: boto3.Session) -> float:
    try:
        q = session.client("service-quotas").get_service_quota(ServiceCode="sagemaker", QuotaCode=TRAINING_QUOTA_CODE)
        return float(q["Quota"]["Value"])
    except Exception:
        return 0.0


def _start_training_job(sm, name: str, role: str, bucket: str, image: str, hp: dict) -> None:
    sm.create_training_job(
        TrainingJobName=name,
        RoleArn=role,
        AlgorithmSpecification={
            "TrainingImage": image,
            "TrainingInputMode": "File",
            "MetricDefinitions": [
                {"Name": "validation:auc", "Regex": r"validation-auc=([0-9\.]+);"},
                {"Name": "validation:f1", "Regex": r"validation-f1=([0-9\.]+);"},
            ],
        },
        HyperParameters={
            "sagemaker_program": "train_xgb.py",
            "sagemaker_submit_directory": f"s3://{bucket}/ml/code/sourcedir.tar.gz",
            **{k: str(v) for k, v in hp.items()},
        },
        InputDataConfig=[
            {
                "ChannelName": ch,
                "ContentType": "text/csv",
                "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": f"s3://{bucket}/ml/data/{ch}/", "S3DataDistributionType": "FullyReplicated"}},
            }
            for ch in ("train", "validation")
        ],
        OutputDataConfig={"S3OutputPath": f"s3://{bucket}/ml/output/"},
        ResourceConfig={"InstanceType": "ml.m5.large", "InstanceCount": 1, "VolumeSizeInGB": 5},
        StoppingCondition={"MaxRuntimeInSeconds": 1800},
        Tags=[{"Key": "project", "Value": "fraud-analyst"}],
    )


def _start_processing_job(sm, name: str, role: str, bucket: str, image: str, hp: dict) -> None:
    def inp(name_: str, s3: str, local: str) -> dict:
        return {"InputName": name_, "S3Input": {"S3Uri": s3, "LocalPath": local, "S3DataType": "S3Prefix", "S3InputMode": "File"}}

    sm.create_processing_job(
        ProcessingJobName=name,
        RoleArn=role,
        AppSpecification={
            "ImageUri": image,
            "ContainerEntrypoint": [
                "python3", "/opt/ml/processing/code/train_xgb.py",
                "--model-dir", "/tmp/model", "--package-tar", "/opt/ml/processing/output",
                *[a for k, v in hp.items() for a in (f"--{k}", str(v))],
            ],
        },
        ProcessingInputs=[
            inp("code", f"s3://{bucket}/ml/code/train_xgb.py", "/opt/ml/processing/code"),
            inp("train", f"s3://{bucket}/ml/data/train/", "/opt/ml/processing/train"),
            inp("validation", f"s3://{bucket}/ml/data/validation/", "/opt/ml/processing/validation"),
        ],
        ProcessingOutputConfig={
            "Outputs": [{"OutputName": "model", "S3Output": {"S3Uri": f"s3://{bucket}/ml/output/{name}/", "LocalPath": "/opt/ml/processing/output", "S3UploadMode": "EndOfJob"}}]
        },
        ProcessingResources={"ClusterConfig": {"InstanceType": "ml.t3.large", "InstanceCount": 1, "VolumeSizeInGB": 5}},
        StoppingCondition={"MaxRuntimeInSeconds": 1800},
        Tags=[{"Key": "project", "Value": "fraud-analyst"}],
    )


def _wait(sm, backend: str, name: str) -> dict:
    describe = sm.describe_training_job if backend == "training" else sm.describe_processing_job
    key = "TrainingJobName" if backend == "training" else "ProcessingJobName"
    status_key = "TrainingJobStatus" if backend == "training" else "ProcessingJobStatus"
    started = time.time()
    while True:
        d = describe(**{key: name})
        status = d[status_key]
        print(f"  [{int(time.time() - started):>4}s] {status} {d.get('SecondaryStatus', '')}")
        if status in ("Completed", "Failed", "Stopped"):
            if status != "Completed":
                raise RuntimeError(f"{backend} job {name} {status}: {d.get('FailureReason') or d.get('ExitMessage')}")
            return d
        time.sleep(20)


def _read_metrics(s3, bucket: str, model_key: str) -> dict:
    body = s3.get_object(Bucket=bucket, Key=model_key)["Body"].read()
    with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as tar:
        member = next((m for m in tar.getmembers() if m.name.endswith("metrics.json")), None)
        if member:
            return json.load(tar.extractfile(member))
    metrics_key = model_key.rsplit("/", 1)[0] + "/metrics.json"
    return json.loads(s3.get_object(Bucket=bucket, Key=metrics_key)["Body"].read())


def cmd_train(args, session: boto3.Session) -> None:
    region = session.region_name
    bucket = stack_outputs(session, "FraudFoundation")["ArtifactsBucketName"]
    role = stack_outputs(session, "FraudMl")["SageMakerRoleArn"]
    backend = args.backend
    if backend == "auto":
        backend = "training" if _training_quota(session) >= 1 else "processing"
    name = f"fraud-xgb-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    hp = {"num_round": args.num_round, "max_depth": args.max_depth, "eta": args.eta}
    sm = session.client("sagemaker")
    print(f"Starting SageMaker {backend} job {name}")
    if backend == "training":
        _start_training_job(sm, name, role, bucket, xgboost_image(region), hp)
        d = _wait(sm, backend, name)
        model_url = d["ModelArtifacts"]["S3ModelArtifacts"]
        billable = d.get("BillableTimeInSeconds")
    else:
        _start_processing_job(sm, name, role, bucket, xgboost_image(region), hp)
        d = _wait(sm, backend, name)
        model_url = f"s3://{bucket}/ml/output/{name}/model.tar.gz"
        billable = int((d["ProcessingEndTime"] - d["ProcessingStartTime"]).total_seconds())

    metrics = _read_metrics(session.client("s3"), bucket, model_url.split(f"s3://{bucket}/", 1)[1])
    metrics["feature_importance_gain"] = {
        FEATURE_NAMES[int(k[1:])] if k.startswith("f") and k[1:].isdigit() else k: v
        for k, v in metrics.get("feature_importance_gain", {}).items()
    }
    baseline_path = DATA_DIR / "rules_baseline.json"
    info = {
        "job_name": name,
        "backend": backend,
        "model_data_url": model_url,
        "image": xgboost_image(region),
        "threshold": metrics["best_f1"]["threshold"],
        "billable_seconds": billable,
        "metrics": metrics,
        "rules_baseline": json.loads(baseline_path.read_text()) if baseline_path.exists() else None,
    }
    LATEST.write_text(json.dumps(info, indent=2))
    best = metrics["best_f1"]
    print(f"Model AUC {metrics['validation_auc']:.4f}; at threshold {best['threshold']}: precision {best['precision']:.3f} recall {best['recall']:.3f} f1 {best['f1']:.3f}")
    print(f"Wrote {LATEST.relative_to(ROOT)}. Next: cdk deploy FraudMl FraudStreaming")


def cmd_invoke(args, session: boto3.Session) -> None:
    from ml.dataset import build

    rows, labels, _ = build(3000, seed=args.seed, start_ts_ms=1_790_000_000_000, fraud_rate=0.03)
    picks = [i for i, y in enumerate(labels) if y][:5] + [i for i, y in enumerate(labels) if not y][:5]
    body = "\n".join(",".join(str(round(v, 5)) for v in rows[i]) for i in picks)
    runtime = session.client("sagemaker-runtime")
    started = time.time()
    resp = runtime.invoke_endpoint(EndpointName=args.endpoint, ContentType="text/csv", Body=body)
    scores = [float(x) for x in resp["Body"].read().decode().replace("\n", ",").split(",") if x.strip()]
    print(f"Endpoint answered in {time.time() - started:.2f}s")
    for i, s in zip(picks, scores):
        print(f"  label={'FRAUD' if labels[i] else 'ok   '}  score={s:.4f}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m ml.pipeline", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("build-data")
    p.add_argument("--train-events", type=int, default=300_000)
    p.add_argument("--validation-events", type=int, default=100_000)
    p = sub.add_parser("train")
    p.add_argument("--backend", choices=["auto", "training", "processing"], default="auto")
    p.add_argument("--num-round", type=int, default=300)
    p.add_argument("--max-depth", type=int, default=6)
    p.add_argument("--eta", type=float, default=0.1)
    p = sub.add_parser("invoke")
    p.add_argument("--endpoint", default="fraud-analyst-xgb")
    p.add_argument("--seed", type=int, default=7)
    args = parser.parse_args(argv)

    session = boto3.Session()
    {"build-data": cmd_build_data, "train": cmd_train, "invoke": cmd_invoke}[args.cmd](args, session)


if __name__ == "__main__":
    main()
