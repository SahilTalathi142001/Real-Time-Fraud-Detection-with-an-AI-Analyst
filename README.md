# Real-Time Fraud Detection with an AI Analyst

A serverless, real-time UPI-style payment fraud detection pipeline on AWS, with an
Apache Iceberg lakehouse and a Bedrock AgentCore "AI fraud analyst" that answers
questions in plain English. Everything is Python, and the infrastructure is AWS CDK (Python).

> Work in progress. The project is built in six phases, one pull request per phase:
>
> 1. Foundation: the CDK app, budgets, S3 buckets, the Glue database, and the payment generator
> 2. Streaming path: API Gateway, Lambda, Kinesis, a scorer Lambda, DynamoDB, and SNS
> 3. ML: XGBoost trained on SageMaker and served from a serverless endpoint
> 4. Lakehouse: Firehose into Iceberg, Glue silver and gold layers, and Athena
> 5. AI analyst: a Strands agent on AgentCore with a Knowledge Base on S3 Vectors, plus a Streamlit UI
> 6. Polish: a dashboard, the README, the architecture diagram, cost notes, and teardown steps
>
> Beginner-friendly guides for each phase live in [`docs/learn/`](docs/learn/).

## License

MIT
