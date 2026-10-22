# Alert triage runbook

*Fraud operations runbook (demo project, fictional company "PaySetu").*

## Severity from the score

| Score | Severity | Target time to first action | Who |
|---|---|---|---|
| 0.95 and above | CRITICAL | 5 minutes | On-call fraud analyst; an email alert is sent automatically |
| 0.85 to 0.95 | HIGH | 30 minutes | Fraud analyst queue |
| alert threshold to 0.85 | MEDIUM | Same business day | Fraud analyst queue |
| below the alert threshold | none | - | Not alerted; visible only in the lakehouse |

The alert threshold is chosen when the model is trained (it maximises F1 on held-out data); the current value is in
the model card. Email notifications are only sent for scores of 0.9 and above, at most 10 per hour.

## Triage steps for one alert

1. **Read the reasons.** Each alert carries rule reasons (for example `velocity_burst`, `new_device_high_value`,
   `impossible_travel`). An alert with reason `model_only` was flagged by the ML model without any hand-written rule
   firing; look at the feature values to see what is unusual.
2. **Check the payer's history.** How many alerts has this payer had today and over the last 7 days? How many
   distinct devices and cities? A payer with 5+ alerts in one day is almost always a compromised account.
3. **Check the payees.** Are the payees new to this payer? Have the same payee VPAs received money from other
   alerted payers (mule accounts)?
4. **Decide.**
   - Clear fraud pattern (burst to new payees, new device with large amount, impossible travel): block outgoing UPI on
     the payer VPA, call the customer, file mule reports for the payees.
   - Ambiguous: contact the customer before blocking.
   - Explained by the customer (travel, a large planned purchase): close the alert as a false positive and note why.
5. **Record the outcome.** Every false positive must be recorded so the model can be retrained with it.

## Customer call script (short)

"This is the PaySetu fraud team. We noticed unusual payments from your UPI ID in the last few minutes. For your
safety we have paused outgoing payments. Did you make a payment of Rs X to Y just now?" Never ask the customer for
their UPI PIN, OTP or card details.

## Escalation

- More than 20 CRITICAL alerts in 10 minutes, or one payee VPA receiving money from 3+ alerted payers: page the
  fraud lead (possible organised attack).
- Model fallbacks (alerts scored by `rules` instead of `model`) for more than 5 minutes: notify the ML on-call;
  the SageMaker endpoint may be unhealthy.
