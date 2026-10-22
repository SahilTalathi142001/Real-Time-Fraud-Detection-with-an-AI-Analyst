# Model card: real-time UPI fraud scorer

*Demo project, fictional company "PaySetu".*

## What it is

An XGBoost binary classifier (up to 300 boosting rounds, depth 6, early stopping; the current model kept 88 trees) that outputs a fraud probability between 0 and
1 for every UPI payment, about one second after the payment is made. It runs on an Amazon SageMaker serverless
endpoint (`fraud-analyst-xgb`) and is called by the scorer Lambda in batches of up to 10 payments.

## Inputs (15 features, computed from the payment and the payer's profile)

| Feature | Meaning |
|---|---|
| amount | payment amount in INR |
| amount_ratio | amount divided by the payer's running average amount |
| is_new_device | 1 if this device was never seen for the payer |
| is_new_payee | 1 if the payer never paid this payee before |
| secs_since_last | seconds since the payer's previous payment |
| txns_last_1m, txns_last_10m | payer's payments in the previous 1 and 10 minutes |
| km_from_last | distance from the previous payment's location |
| speed_kmh | km_from_last divided by the time between the two payments |
| hour_of_day, is_night | hour in IST; is_night is 1 from 00:00 to 05:59 IST |
| is_collect | 1 for collect requests (UPI_COLLECT) |
| is_p2p | 1 for person-to-person payments |
| category_risk | prior risk of the merchant category (0 to 1): jewellery 0.8, electronics 0.7, gaming and p2p 0.5, travel 0.4, grocery/food/utilities 0.1 |
| profile_txn_count | how many payments we have seen from the payer (low = new customer) |

The same feature code is used for training and in the live scorer, which avoids training/serving skew.

## Performance (trained on 300,000 simulated payments, measured on 100,000 held-out payments)

- At the chosen threshold: precision 0.95, recall 0.92, F1 0.93; ROC AUC 0.999.
- Hand-written rules alone reach F1 about 0.75 on the same data.
- Live precision and recall per day are in the lakehouse table `gold_detection_quality`.

## Threshold

The alert threshold is the value that maximises F1 on validation data (around 0.85 for the current model). Scores at
or above it create an alert in DynamoDB.

## Fallback

If the endpoint does not answer (cold start errors after retries, throttling), the scorer uses the hand-written rules
instead and marks the payment `scored_by = rules`. The CloudWatch metric `ModelFallbacks` counts these.

## Limitations

- Trained only on simulated payments from 500 synthetic payers; real fraud is more varied.
- New fraud patterns not present in training (for example slow, low-value mule activity over days) will be missed.
- Geo features rely on approximate city coordinates.
- Retrain whenever the fraud mix changes; recorded false positives should be added to the training data.
