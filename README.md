# Real-Time Fraud Detection with an AI Analyst

A serverless, real-time UPI-style payment fraud detection pipeline on AWS, with an
Apache Iceberg lakehouse and a Bedrock AgentCore "AI fraud analyst" that answers
questions in plain English. Everything is Python, and the infrastructure is AWS CDK (Python).

> Work in progress. The project is built in six phases, one pull request per phase:
>
> 1. Foundation: the CDK app, budgets, S3 buckets, the Glue database, and the payment generator
> 2. Streaming path: API Gateway, an ingest Lambda, SQS FIFO, a scorer Lambda, DynamoDB, and SNS
> 3. ML: XGBoost trained on SageMaker and served from a serverless endpoint
> 4. Lakehouse: an SQS buffer and loader Lambda into Iceberg bronze tables (via Athena), Glue silver and gold layers, and Athena views
> 5. AI analyst: a Strands agent (Claude Haiku 4.5) on AgentCore Runtime, with Athena SQL, DynamoDB alert and Knowledge Base (S3 Vectors) tools, plus a Streamlit console
> 6. Polish: a dashboard, the README, the architecture diagram, cost notes, and teardown steps
>
> Beginner-friendly guides for each phase live in [`docs/learn/`](docs/learn/).

## About the live deployment

The reference deployment runs in an AWS account on the **AWS Free plan**, which expires on
**October 30, 2026**. The account closes then, so **the live stack won't exist after that date**.
Everything can be redeployed to any AWS account with `cdk deploy --all`.

The Free plan doesn't allow Kinesis Data Streams or Amazon Data Firehose, so this build uses
SQS FIFO and a scheduled loader Lambda in their place. See
[`docs/production-version.md`](docs/production-version.md) for where Kinesis/MSK and Firehose
would slot in for a production system, and why.

## License

MIT
