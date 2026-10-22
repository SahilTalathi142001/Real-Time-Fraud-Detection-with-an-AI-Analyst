# UPI fraud patterns: what they look like and how we detect them

*Fraud team playbook (demo project, fictional company "PaySetu"). Applies to UPI payments scored by the real-time fraud pipeline.*

## 1. Velocity burst (account takeover / mule cash-out)

**What happens.** A fraudster gains control of a customer's UPI app (stolen phone, shared PIN, remote-access
app) and drains the account with many small-to-medium payments in a very short time, usually to several new
payees (money mules) so each transfer stays under per-transaction limits.

**Signals in our data.**
- `txns_last_1m` of 4 or more (normal customers rarely make more than 1-2 payments a minute).
- `txns_last_10m` keeps climbing; `secs_since_last` under 15 seconds.
- `is_new_payee = 1` repeatedly: rule reason `p2p_fanout`.
- Rule reason text: `velocity_burst: N payments in the previous minute`.

**Recommended action.** Severity HIGH. Temporarily block outgoing payments on the payer VPA, call the customer on
the registered number, and raise a mule-account report for every new payee that received money in the burst.

## 2. New device + high value (SIM swap / credential theft)

**What happens.** The fraudster registers the victim's UPI account on their own phone (often after a SIM swap or a
phishing call that captured the OTP) and immediately makes one or two large payments.

**Signals in our data.**
- `is_new_device = 1` (device never seen for this payer).
- `amount_ratio` of 8x or more of the payer's usual amount.
- Paid to a high-risk merchant category that resells easily: in our data `electronics` or `jewellery`.
- Rule reason text: `new_device_high_value: unseen device, amount Nx usual`.

**Recommended action.** Severity CRITICAL when score >= 0.95. De-register the new device binding, hold the payment if
it is still pending, and ask the customer to visit a branch or complete video KYC before re-enabling UPI.

## 3. Geo jump (impossible travel)

**What happens.** Two payments from the same payer come from cities far apart within minutes, which no traveller
can do. One of them is from a cloned device or a compromised session.

**Signals in our data.**
- `km_from_last` of 500 km or more together with `speed_kmh` of 300 km/h or more.
- Rule reason text: `impossible_travel: N km at N km/h`.
- Often combined with a new device.

**Recommended action.** Severity HIGH. Compare the two devices; block the one that is not the customer's usual
device and notify the customer.

## 4. Collect-request scam

**What happens.** Victims are tricked ("you have received a refund, enter your PIN to accept") into approving a UPI
collect request, which actually sends money out.

**Signals.** `channel = UPI_COLLECT`, `is_collect = 1` and `is_new_payee = 1` (rule reason `collect_request_to_new_payee`).
On its own this is a weak signal (score boost 0.15); it matters when combined with others.

**Recommended action.** Severity MEDIUM. Send the customer an in-app warning about collect-request scams; report the
payee VPA if several customers are affected.

## 5. Unusually high amount (no other signal)

A known device paying 15x or more of the usual amount (`unusually_high_amount`). Usually legitimate (rent, fees,
weddings). Severity LOW: review only if the model score is also above the alert threshold.

## Pattern labels in the data

The payment simulator labels each payment in `sim_fraud_pattern` / `sim_label_pattern` with one of
`normal`, `velocity_burst`, `new_device_high_value`, `geo_jump`. These are the demo's ground truth ("confirmed
fraud") used to measure precision and recall.
