"""Delete everything this project created in the current AWS account and region.

    python scripts/teardown.py                # cdk destroy --all + leftover log groups
    python scripts/teardown.py --bootstrap    # ...and the CDKToolkit stack and its asset bucket/repo
    python scripts/teardown.py --dry-run      # only list what would be deleted

`cdk destroy` removes every resource CloudFormation created (buckets are emptied first). Some log
groups are created by AWS services on first use rather than by CloudFormation, so they survive
the destroy; this script removes them too. Shared groups such as /aws-glue/jobs/* and
/aws/sagemaker/ProcessingJobs are only deleted with --shared-logs, in case other work uses them.
"""

from __future__ import annotations

import argparse
import subprocess
import sys

import boto3

PROJECT_LOG_PREFIXES = (
    "/aws/lambda/fraud-analyst-",
    "/aws/lambda/FraudFoundation-",
    "/aws/lambda/FraudStreaming-",
    "/aws/lambda/FraudLakehouse-",
    "/aws/lambda/FraudMl-",
    "/aws/lambda/FraudAnalyst-",
    "/aws/lambda/FraudMonitoring-",
    "/aws/sagemaker/Endpoints/fraud-analyst-",
    "/aws/bedrock-agentcore/runtimes/fraud_analyst-",
)
SHARED_LOG_PREFIXES = ("/aws-glue/jobs/", "/aws/sagemaker/ProcessingJobs")
BOOTSTRAP_STACK = "CDKToolkit"


def log_groups(logs, prefixes: tuple[str, ...]) -> list[str]:
    names = []
    for prefix in prefixes:
        for page in logs.get_paginator("describe_log_groups").paginate(logGroupNamePrefix=prefix):
            names += [g["logGroupName"] for g in page["logGroups"]]
    return sorted(set(names))


def empty_versioned_bucket(s3, bucket: str) -> None:
    for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
        objects = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in page.get("Versions", []) + page.get("DeleteMarkers", [])]
        if objects:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": objects, "Quiet": True})


def delete_bootstrap(session: boto3.Session, dry_run: bool) -> None:
    cfn = session.client("cloudformation")
    try:
        resources = cfn.describe_stack_resources(StackName=BOOTSTRAP_STACK)["StackResources"]
    except cfn.exceptions.ClientError:
        print(f"{BOOTSTRAP_STACK}: not found")
        return
    by_type = {r["ResourceType"]: r["PhysicalResourceId"] for r in resources}
    bucket, repo = by_type.get("AWS::S3::Bucket"), by_type.get("AWS::ECR::Repository")
    print(f"{BOOTSTRAP_STACK}: bucket {bucket}, ECR repository {repo}")
    if dry_run:
        return
    cfn.delete_stack(StackName=BOOTSTRAP_STACK)
    cfn.get_waiter("stack_delete_complete").wait(StackName=BOOTSTRAP_STACK)
    # The bootstrap template retains its bucket and repository on stack deletion.
    if bucket:
        s3 = session.client("s3")
        empty_versioned_bucket(s3, bucket)
        s3.delete_bucket(Bucket=bucket)
    if repo:
        session.client("ecr").delete_repository(repositoryName=repo, force=True)
    print(f"{BOOTSTRAP_STACK}: deleted, with its bucket and repository")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", action="store_true", help="also delete the CDKToolkit stack")
    parser.add_argument("--shared-logs", action="store_true", help="also delete Glue job and SageMaker processing log groups")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--yes", action="store_true", help="skip the confirmation prompt")
    args = parser.parse_args()

    session = boto3.Session()
    account = session.client("sts").get_caller_identity()["Account"]
    print(f"Account {account}, region {session.region_name}")
    if not (args.dry_run or args.yes):
        if input("Type 'destroy' to delete all project stacks: ").strip() != "destroy":
            print("Cancelled.")
            return 1

    if args.dry_run:
        subprocess.run(["cdk", "list"], check=True)
    else:
        subprocess.run(["cdk", "destroy", "--all", "--force"], check=True)

    logs = session.client("logs")
    prefixes = PROJECT_LOG_PREFIXES + (SHARED_LOG_PREFIXES if args.shared_logs else ())
    for name in log_groups(logs, prefixes):
        print(f"{'would delete' if args.dry_run else 'deleting'} log group {name}")
        if not args.dry_run:
            logs.delete_log_group(logGroupName=name)

    if args.bootstrap:
        delete_bootstrap(session, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
