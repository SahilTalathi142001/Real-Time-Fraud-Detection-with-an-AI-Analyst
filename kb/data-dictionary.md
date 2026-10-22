# Data dictionary: where fraud data lives

*Demo project, fictional company "PaySetu".*

## Real-time (seconds old)

**DynamoDB table `fraud-analyst-alerts`**: one item per alerted payment, kept 90 days.
Key `alert_id` (= `txn_id`). Indexes: `by_day` (`alert_day` + `event_ts_ms`) and `by_payer` (`payer_id` + `event_ts_ms`).
Fields: event_time, payer_id, payer_vpa, payee_vpa, payee_type, amount, merchant_category, channel, city, device_id,
score, model_score, rule_score, scored_by, reasons, features (the 15 model inputs), status (`OPEN`), sim_label_pattern.

## Lakehouse (Amazon S3 + Apache Iceberg, queried with Amazon Athena)

Bronze (raw, appended every 5 minutes): `bronze_payments`, `bronze_scores`.
Silver and gold are rebuilt by the Glue job `fraud-analyst-silver-gold`, so they lag until it runs.

- **silver_transactions**: one clean row per payment with its score. Partitioned by `event_date` (UTC).
  Notable columns: `event_ts` (UTC), `event_hour_ist` (hour in India time), `is_alert`, `scored_by`,
  `score_latency_ms` (payment to score), `sim_is_fraud`, `sim_fraud_pattern`.
- **gold_fraud_daily**: per day, city and merchant category: txn_count, total_amount, alert_count, alert_amount (money
  at risk), alert_rate, sim_fraud_count.
- **gold_payer_risk**: per payer: txn_count, total_amount, avg_amount, alert_count, max_score, last_alert_ts,
  distinct_devices, distinct_cities, home_city (most frequent city).
- **gold_detection_quality**: per day and scorer (`model` or `rules`): tp, fp, fn, tn, precision, recall,
  avg_score_latency_ms. Ground truth is the simulator label.
- Views: `v_recent_alerts` (alerted rows of silver), `v_fraud_by_city`, `v_fraud_by_pattern`.

## Common terms

- **VPA**: Virtual Payment Address, the UPI ID like `name@bank`.
- **Payer / payee**: who sends / receives the money. `payee_type` is `merchant` or `p2p` (person to person).
- **Collect request**: the payee asks for money and the payer approves it (`channel = UPI_COLLECT`).
- **Money at risk**: the total amount of alerted payments (`alert_amount`).
- **Precision**: of the payments we alerted on, the share that were really fraud. **Recall**: of the real fraud,
  the share we alerted on.
- **Reason codes**: `velocity_burst`, `new_device_high_value`, `unusually_high_amount`, `impossible_travel`,
  `collect_request_to_new_payee`, `p2p_fanout`, `model_only` (flagged by the model, no rule fired).
