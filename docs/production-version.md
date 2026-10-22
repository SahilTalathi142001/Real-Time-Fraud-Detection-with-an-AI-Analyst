# Production version: what would change at real scale

This project runs on an AWS account on the **Free plan**. That plan doesn't allow
**Kinesis Data Streams** or **Amazon Data Firehose** (their APIs return
`SubscriptionRequiredException`), so the build uses services the plan does allow. This page
explains each substitution, what the production-grade service would be, and why you'd switch.

| Concern | This project (Free plan) | Production choice |
| --- | --- | --- |
| Event stream | SQS FIFO, `MessageGroupId = payer_id` | Kinesis Data Streams (on-demand), or Amazon MSK (Kafka) |
| Stream → lake | Phase 4: the scorer buffers records in a standard SQS queue; a scheduled loader Lambda writes them to Iceberg with Athena `INSERT INTO` | Amazon Data Firehose with a native Apache Iceberg destination |
| Scoring compute | Lambda per batch | Lambda, or Managed Service for Apache Flink for stateful windows |
| Feature state | DynamoDB profile per payer | DynamoDB/ElastiCache, or Flink keyed state, plus SageMaker Feature Store |

## Event stream: SQS FIFO → Kinesis Data Streams or MSK

**What SQS FIFO gives us:** ordering per payer (message group), deduplication by `txn_id`,
retries, and a dead-letter queue. That's enough for a correct per-payer velocity feature.

**Why production would use Kinesis (or MSK):**
- **Replay.** A stream keeps records for 24 h to 365 days after they're read. You can re-run a fixed
  scorer, backfill a new model, or feed a second consumer from the same data. SQS deletes a
  message once it's processed.
- **Fan-out.** Many independent consumers read the same stream (scorer, lake loader, a
  real-time dashboard, an audit archive), each at its own pace. With SQS you need SNS fan-out into one
  queue per consumer.
- **Throughput.** FIFO queues are limited per message group and per queue (high-throughput mode helps,
  but payments at UPI scale, thousands of transactions per second, are a streaming workload).
  Kinesis scales by shards (on-demand mode auto-scales), and MSK scales by partitions.
- **Ecosystem.** Kafka (MSK) is the industry standard for payment event buses, with schema registry,
  exactly-once producers, Kafka Connect, and Flink/Spark integration. The Paytm PRISM design this
  project is modeled on uses Kafka.

**Cost trade-off:** Kinesis provisioned costs about $0.015 per shard-hour (about $11/month for one
shard, even when idle). On-demand costs about $0.04/hour per stream plus data. MSK Serverless starts
around $0.75/hour (about $540/month) just to exist. For a demo, SQS's pay-per-request model, about $0
when idle, is better anyway.

**What the swap would touch:** `infra/stacks/streaming_stack.py` (an `aws_kinesis.Stream`
instead of `sqs.Queue`, and a `KinesisEventSource` with `parallelization_factor=1` and
`bisect_batch_on_error=True`), `src/ingest/handler.py` (`put_records` with
`PartitionKey=payer_id`), and the record decoding in `src/scorer/handler.py`
(`base64.b64decode(record["kinesis"]["data"])` instead of `record["body"]`). The feature,
rule, and model code doesn't change. The first version of this project was written against
Kinesis; see the git history of phase 2.

## Stream → lake: SQS buffer + loader Lambda → Amazon Data Firehose

(Built in phase 4.)

**What we do:** the scorer sends every scored payment to a standard SQS "lake buffer" queue.
Every 5 minutes, a loader Lambda drains it, writes a JSON file to an S3 landing prefix, and runs
Athena `INSERT INTO bronze_* SELECT ... FROM landing_*` to append to the Iceberg tables. It deletes
the messages only after the insert succeeds. That's *at-least-once* delivery, and the silver
layer de-duplicates by `txn_id`.

**Why production would use Firehose:**
- **Fully managed.** Buffering, retries, a backup of failed records to S3, and scaling are all
  built in. There's no loader code to maintain.
- **Native Iceberg.** Firehose writes straight into Iceberg tables in the Glue catalog, and can
  even route rows to different tables and handle updates/deletes with unique keys.
- **Fewer small-commit problems.** Firehose buffers by size and time (up to 128 MB / 900 s)
  across many producers. Our 5-minute micro-batches are fine for a demo, but at scale you'd also
  schedule Iceberg compaction (`OPTIMIZE ... REWRITE DATA`) or turn on Glue's automatic table optimization.
- **Cost.** About $0.03–0.05 per GB ingested, with nothing billed at idle.

## Other production hardening (not cost-related)

- **Exactly-once-ish scoring:** make profile updates idempotent, e.g. a conditional write on
  `last_txn_id`, so retries don't double-count.
- **Feature store:** SageMaker Feature Store (online plus offline) to guarantee the same features
  in training and serving; here a shared Python module does that job.
- **Model ops:** SageMaker Pipelines plus a Model Registry with approval, shadow/canary
  deployments, and drift monitoring (SageMaker Model Monitor).
- **Security:** Lake Formation fine-grained permissions on the lake, a VPC with endpoints for Lambda,
  customer-managed KMS keys, WAF on the API, and Cognito/OIDC for the analyst UI.
- **Alerting:** an alert-case workflow (Step Functions or a ticketing integration) instead of
  email.
