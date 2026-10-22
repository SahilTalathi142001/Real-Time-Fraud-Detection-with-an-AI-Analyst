# How to run this project (beginner guide)

You run everything from **your own laptop**. The laptop only runs small Python programs: the payment
generator, the Streamlit console and a few command-line tools. Everything else runs in your AWS
account: the APIs, Lambdas, queues, model, lakehouse and AI agent.

Every step below has two versions:
- **macOS / Linux**: run the commands in *Terminal*.
- **Windows 10/11**: run the commands in *PowerShell*. Open it from the Start menu; you don't need
  admin rights except for installing tools.

There are two ways to use the project:

| | When | Steps |
| --- | --- | --- |
| **A. Use the stack that's already deployed** | Your account already has the `Fraud*` stacks (true until the Free plan ends on **Oct 30, 2026**) | 1 → 2 → 3 → 4, then 6 onwards |
| **B. Deploy it yourself** | A new AWS account, or after a teardown | 1 → 2 → 3 → 4 → 5 → 6 onwards |

---

## 1. Install the tools (once)

| Tool | Version | Why |
| --- | --- | --- |
| Git | any recent | to clone the repo |
| Python | **3.12** | all of the project's code |
| Node.js | 22 or newer (LTS) | the AWS CDK command-line tool runs on Node |
| AWS CDK CLI | latest v2 | `cdk deploy` / `cdk destroy` |
| AWS CLI | v2 | `aws configure` and a few checks |
| Java | 17+ (optional) | only for the local Spark test of the Glue job; it's skipped without Java |

**macOS** (with [Homebrew](https://brew.sh)):

```bash
brew install git python@3.12 node awscli libomp    # libomp is needed by xgboost on macOS
npm install -g aws-cdk
```

**Linux** (Ubuntu 24.04; on older releases, get Python 3.12 from the `deadsnakes` PPA):

```bash
sudo apt update && sudo apt install -y git python3.12 python3.12-venv unzip curl
curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash - && sudo apt install -y nodejs
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o awscliv2.zip && unzip -q awscliv2.zip && sudo ./aws/install
sudo npm install -g aws-cdk
```

**Windows** (PowerShell; `winget` comes with Windows 10/11):

```powershell
winget install --id Git.Git -e
winget install --id Python.Python.3.12 -e
winget install --id OpenJS.NodeJS.LTS -e
winget install --id Amazon.AWSCLI -e
# Close and reopen PowerShell so the new tools are on PATH, then:
npm install -g aws-cdk
```

Check that everything is installed. Each command should print a version:

```bash
git --version
python3.12 --version     # Windows: py -3.12 --version
node --version
cdk --version
aws --version
```

## 2. Give the AWS CLI your IAM keys (once)

The tools on your laptop act as **your IAM user**, using an access key.

1. In the AWS console go to **IAM → Users → (your user) → Security credentials → Create access
   key**, choose **Command Line Interface (CLI)**, and copy the *Access key ID* and *Secret access
   key*. The secret is shown only once.
2. Run `aws configure` (same command on every OS) and answer the four questions:

```text
AWS Access Key ID [None]: AKIA................
AWS Secret Access Key [None]: ........................................
Default region name [None]: ap-south-1          # Asia Pacific (Mumbai)  # pragma: allowlist secret
Default output format [None]: json
```

   Use the **Mumbai** region. The project is deployed there, and the AI analyst calls Claude through
   the `in.` (India) inference profile, which only works from Indian regions.
3. Check it: `aws sts get-caller-identity` should print your 12-digit account id.

The keys are saved in `~/.aws/credentials` (Windows: `C:\Users\<you>\.aws\credentials`). Never paste
them into code or commit them to git.

## 3. Get the code (once)

**macOS / Linux:**

```bash
git clone https://github.com/SahilTalathi142001/Real-Time-Fraud-Detection-with-an-AI-Analyst.git
cd Real-Time-Fraud-Detection-with-an-AI-Analyst
```

**Windows:** the same two commands. The folder is created under your current directory, for example
`C:\Users\<you>\Real-Time-Fraud-Detection-with-an-AI-Analyst`.

## 4. Create a virtual environment and install the Python packages (once)

A *virtual environment* (`.venv`) is a private folder of Python packages for this project only.

**macOS / Linux:**

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt        # a few minutes; pyspark is a big download
python -m pytest -q                        # should end with "82 passed" (no AWS calls)
```

**Windows (PowerShell):**

```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
# If that's blocked ("running scripts is disabled"), run this once, then activate again:
#   Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
python -m pytest -q
```

Your prompt now starts with `(.venv)`. **Every new terminal** needs `cd` into the project folder and
the activate command again (`source .venv/bin/activate` or `.venv\Scripts\Activate.ps1`). From here
on, the commands are the same on every OS unless noted.

**Path A (already deployed)?** Check the stacks are there, then skip to step 6:

```bash
aws cloudformation describe-stacks --query "Stacks[].StackName" --output table
```

You should see `FraudFoundation`, `FraudMl`, `FraudLakehouse`, `FraudStreaming`, `FraudAnalyst` and
`FraudMonitoring`.

## 5. Deploy (path B only)

Allow about 30 minutes the first time. Alert emails go to the `alertEmail` value in `cdk.json`; to
use another address, add `-c alertEmail=you@example.com` to the `cdk deploy` commands.

```bash
cdk bootstrap                                         # once per account and region (creates CDKToolkit)

# Phases 1-4: buckets, budget, streaming path, lakehouse, and the ML role
cdk deploy FraudFoundation FraudMl FraudLakehouse FraudStreaming

# Train the model (about 5 minutes on SageMaker), then deploy its serverless endpoint
python -m ml.pipeline build-data
python -m ml.pipeline train                           # writes ml/latest_model.json
cdk deploy FraudMl FraudStreaming

# Phase 5-6: the AI analyst and the dashboard
python -m agent package                               # builds build/analyst_runtime.zip (~30 MB)
cdk deploy FraudAnalyst FraudMonitoring
python -m agent ingest-kb                             # indexes the fraud playbook
```

Answer `y` when `cdk deploy` asks to approve IAM changes. Add `--require-approval never` to skip
the question.

On a **brand-new account**, Bedrock asks for a one-time Anthropic use-case form before Claude works.
Open **Amazon Bedrock → Model catalog → Claude Haiku 4.5**, click *Open in playground*, and submit the
form if asked.

> **Redeploying from a fresh clone of an account that's already deployed?** Run these two commands
> **before** any `cdk deploy`:
>
> ```bash
> python -m ml.pipeline use-latest     # rebuilds ml/latest_model.json from the model already in S3
> python -m agent package              # rebuilds build/analyst_runtime.zip
> ```
>
> Both files are git-ignored, so a new clone doesn't have them. Without them, CDK would remove the
> SageMaker endpoint (the scorer falls back to rules) and the AgentCore runtime. If they're missing,
> `cdk deploy` prints a warning that names the missing file.

**Confirm the emails.** AWS sends one "Subscription Confirmation" email per SNS topic: fraud alerts,
and ops alarms (`fraud-analyst-ops-alarms`). Click **Confirm subscription** in each, or no emails are
delivered.

## 6. Start the payment generator

```bash
python -m generator --fraud-rate 0.02 live --duration 300
```

This sends about 5 simulated UPI payments per second for 5 minutes, with about 2% fraud, to the
deployed API. Every few seconds it prints `sent N events, accepted N`. Press **Ctrl+C** to stop
early. Leave it running and open a **second terminal** for the next steps (remember to activate the
venv there).

## 7. Open the Streamlit console

```bash
streamlit run ui/app.py
```

Your browser opens <http://localhost:8501>. If it doesn't, open that address yourself. The console
has three tabs:

- **Ask the analyst**: chat with the AI agent. The sidebar has sample questions and a **New
  conversation** button. *Mode* `agentcore` uses the deployed agent; `local` runs the same agent on
  your laptop.
- **Live alerts**: refreshes every 10 s. It shows alerts today and in the last 10 minutes, critical
  alerts, money at risk, alerts by reason and by city, and the newest alerts.
- **Lakehouse**: saved Athena queries and a box for your own read-only SQL.

Stop the console with **Ctrl+C** in its terminal.

## 8. Ask the agent a question

In the **Ask the analyst** tab, type for example:

- "How many fraud alerts were raised today, and which payer had the most?"
- "For that payer, show their latest 3 alerts and tell me whether to block them."
- "Which 5 cities have the highest alert rate, and how much money is at risk there?"
- "What is a velocity burst and how do we respond to one?"

Open **How I got this** under an answer to see which tools the agent called (DynamoDB lookups,
Athena SQL, playbook search). An answer takes about 3-13 seconds and costs about $0.01.

You can also ask from the terminal:

```bash
python -m agent ask "How many fraud alerts were raised today?"
python -m agent chat          # a conversation; type exit to quit
python -m agent eval          # 5 automatic answer-quality checks (expect 5/5)
```

## 9. Check alerts, Athena and the dashboard

**Alerts.** Use the **Live alerts** tab, and check your inbox: alerts with a score of 0.9 or more
are emailed. In the console: **DynamoDB → Tables → fraud-analyst-alerts → Explore items**.

**Athena (the lakehouse).** The loader moves new payments into the lake every 5 minutes. The Glue
job builds the cleaned silver and gold tables only when you run it:

```bash
python -m lakehouse load       # optional: load now instead of waiting up to 5 minutes
python -m lakehouse glue       # ~2 minutes, about $0.02 per run
python -m lakehouse views      # only the first time after a fresh deploy
python -m lakehouse demo       # the saved queries: fraud by city, freshness, model quality, risky payers
python -m lakehouse query "SELECT * FROM v_fraud_by_pattern"
```

On macOS/Linux, put SQL that contains double quotes or `$` inside single quotes, for example
`'SELECT * FROM "bronze_payments$snapshots"'`. Windows PowerShell 5 mangles double quotes inside
arguments, so on Windows run those queries in the Streamlit **Lakehouse** tab or in the AWS console
under **Athena → Query editor** (pick workgroup `fraud-analyst`).

**Dashboard.** In the AWS console go to **CloudWatch → Dashboards → fraud-analyst**, and set the time
range to the last 15 minutes while the generator runs. Alarms are under **CloudWatch → Alarms**. To
save the dashboard as an image without the console:

```bash
python scripts/dashboard_snapshot.py --hours 3       # writes docs/images/dashboard.png
```

## 10. View costs

```bash
python scripts/cost_report.py
```

This prints costs by service and region (before credits), daily totals, and your Free plan credits
and expiry date. Cost Explorer lags by up to 24 hours, so today's usage shows up tomorrow. In the
console, see **Billing and Cost Management → Cost Explorer**, and **Budgets →
fraud-analyst-total-spend**, which emails you at $40, $80 and $110.

The whole project cost about $0.60 to build and test. Left idle it costs about $0.01/day, because
nothing is always-on.

## 11. Tear everything down

```bash
python scripts/teardown.py --dry-run      # shows what would be deleted; deletes nothing
python scripts/teardown.py                # type "destroy" to confirm; ~10-15 minutes
python scripts/teardown.py --bootstrap --shared-logs    # optional: also CDKToolkit and shared log groups
```

This runs `cdk destroy --all` (buckets are emptied first), then deletes the log groups that AWS
creates outside CloudFormation. After that, nothing from this project is left in the account.

---

## Troubleshooting

| Problem | Fix |
| --- | --- |
| `Unable to locate credentials` / `ExpiredToken` | Run `aws configure` again (step 2); check with `aws sts get-caller-identity` |
| `python` / `py` / `python3.12` not found | Reinstall Python 3.12; on Windows, tick *Add python.exe to PATH*, then reopen PowerShell |
| `cdk` not found | `npm install -g aws-cdk`, then open a new terminal |
| `.venv\Scripts\Activate.ps1 cannot be loaded` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, then activate again |
| `ModuleNotFoundError` | The venv isn't active: run the activate command from step 4 in this terminal |
| macOS: `XGBoostError ... libomp` | `brew install libomp` |
| `Stack with id FraudStreaming does not exist` | Nothing is deployed in this account and region: check the region in `aws configure get region`, or deploy (step 5) |
| Agent: `AccessDeniedException` for the model | Wrong region (it must be Mumbai), or Bedrock model access isn't enabled yet (step 5) |
| No alert emails | Click the SNS confirmation emails; check spam |
| Live alerts tab is empty | The generator isn't running, or the day changed (the tab shows today in UTC) |
| Lakehouse queries return nothing new | Run `python -m lakehouse load` and `python -m lakehouse glue` |
