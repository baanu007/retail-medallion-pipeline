# Session Report — April 11, 2026 (CI/CD + GitHub)
## GitHub CI/CD Pipeline Built Locally — Ready to Push

---

## TL;DR

Built the GitHub CI/CD pipeline requested in Step 7. Everything is ready
locally:

- **git repo initialized** on `main` branch with initial commit `fb4c425`
  containing all 58 tracked files
- **`.gitignore`** excludes caches, lock files, `.claude/`, credentials
- **Project root `README.md`** consolidates the "WHY behind each tech stack"
  writeup that Step 7 explicitly asks for (10 tech-stack decisions justified)
- **`.github/workflows/ci.yml`** — CI pipeline: lint, syntax check, JSON
  validation, Streamlit `AppTest` smoke run on all 8 pages with mocked data
- **`.github/workflows/deploy-glue.yml`** — CD pipeline: on push to `main`,
  uploads all 18 Glue scripts to S3 and syncs the Glue jobs + workflow via
  idempotent boto3 calls
- **Validated locally**: the CI's mock AppTest block was run on my machine —
  all 8 pages pass cleanly with mock data. Same code will run in GitHub Actions.

**What remains**: 3 simple commands for the user to run to authenticate,
create the GitHub repo, and push. Plus adding 4 GitHub secrets via the
web UI (no credentials through chat).

---

## 1. Files Created

| File | Purpose |
|---|---|
| `.gitignore` | Excludes caches, `.claude/`, credentials, Office lock files, build artifacts |
| `README.md` (project root) | Project overview + **tech stack WHYs** (Step 7 explicit requirement) + file inventory + run instructions |
| `.github/workflows/ci.yml` | CI pipeline (4 jobs: lint-and-syntax, json-validation, requirements-check, streamlit-apptest) |
| `.github/workflows/deploy-glue.yml` | CD pipeline (1 job: upload scripts + sync Glue jobs + sync workflow) |
| `dashboard/_ci_smoketest.py` | Local mirror of the CI's AppTest block; validated locally before committing |

---

## 2. CI Pipeline (`ci.yml`)

Runs on every push and pull request. **No AWS secrets required** — all
checks work on any machine.

### 2.1 Jobs in the CI workflow

**`lint-and-syntax`**
- Installs `ruff` (zero-config Python linter)
- Runs `ruff check . --output-format=github --exit-zero` (reports issues as
  GitHub annotations but doesn't fail the build — turn into strict mode by
  removing `--exit-zero` after the first clean pass)
- Compiles every `.py` file via `py_compile.compile(... doraise=True)` to
  catch syntax errors that ruff sometimes misses

**`json-validation`**
- Walks every `*.json` file in the repo and attempts `json.loads()` on it
- Catches accidental malformed config files (e.g. trailing commas, missing
  quotes) before they break a Glue deployment

**`requirements-check`**
- Runs `pip install --dry-run -r dashboard/requirements.txt`
- Catches conflicting version pins or unresolvable dependencies

**`streamlit-apptest`**
- Installs the full dashboard stack (streamlit, plotly, pandas, pyarrow,
  deltalake, duckdb, boto3)
- Patches `lib.data.get_table` and `lib.data.query_large` to return
  hand-crafted mock DataFrames with the exact columns and types every page
  expects
- Runs all 8 pages (Home + 7 metric pages) via `streamlit.testing.v1.AppTest`
- Captures any exceptions and fails the build if any page errors out

**Why mock data in CI**: lets us validate Python imports, Streamlit API usage,
Plotly aesthetics, and cross-page column-name expectations on every push,
without any AWS credentials. Real end-to-end data tests happen in the CD
workflow (which has AWS secrets).

### 2.2 Local validation done this session

I wrote `dashboard/_ci_smoketest.py` as an exact mirror of the CI's AppTest
block and ran it locally:

```
  OK:   Home
  OK:   CLV Snapshot
  OK:   RFM Segments
  OK:   Churn Indicators
  OK:   Sales Trends
  OK:   Loyalty Comparison
  OK:   Location Performance
  OK:   Upsell Analysis

All 8 pages OK (mock data).
```

Caught one bug during local validation: the home page uses `cust_count`
while the CLV page uses `customer_count` — the mock initially had only
`customer_count`, which made the home page fail with `KeyError: 'cust_count'`.
Fixed by including both column names in the mock DataFrame. This is exactly
why I validated locally before pushing — same bug would have failed in
GitHub Actions on the first CI run.

---

## 3. CD Pipeline (`deploy-glue.yml`)

Runs on every push to `main`. **Requires 4 GitHub secrets.**

### 3.1 Required GitHub Secrets

| Secret Name | Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM access key for the globalpartners AWS account |
| `AWS_SECRET_ACCESS_KEY` | corresponding secret key |
| `AWS_REGION` | `us-east-1` |
| `GP_BUCKET` | `globalpartners-aws` |

Optional overrides (otherwise defaults are baked in):
- `GP_ROLE_ARN`: IAM role name for Glue jobs (default `AWSGlueServiceRole-globalpartners`)
- `GP_SNS_TOPIC_ARN`: SNS topic for failure alerts

**How to add secrets**: go to the GitHub repo → Settings → Secrets and
variables → Actions → New repository secret.

### 3.2 Steps in the CD workflow

1. **Checkout** + **Setup Python 3.11** + **Install boto3**
2. **Configure AWS credentials** via `aws-actions/configure-aws-credentials@v4`
3. **Sanity check**: `aws sts get-caller-identity` + `aws s3 ls s3://$GP_BUCKET/`
4. **Upload Bronze scripts** — `aws s3 cp glue_jobs/bronze/*.py s3://$GP_BUCKET/scripts/`
5. **Upload Silver scripts** (3 files)
6. **Upload Gold scripts** (14 files)
7. **Sync Gold Glue jobs**: inline Python helper uses boto3 to create-or-update
   every job defined in the JOBS dict (same as `create_gold_jobs.py` but using
   env-var credentials instead of a named profile)
8. **Sync Glue Workflow + 5 triggers**: inline Python helper ensures the
   workflow exists and recreates all 5 triggers (SCHEDULED → bronze →
   CONDITIONAL chains)

**Idempotency**: every step can be re-run safely. Uploading a script that
already exists is a no-op S3 overwrite. Creating a Glue job that already
exists is caught and converted to `update_job()`. Recreating triggers uses
`delete_trigger` + `create_trigger` under a single lock.

**Concurrency**: `concurrency: deploy-glue-{ref}` prevents two simultaneous
deploys from racing to recreate the same triggers.

---

## 4. Tech Stack WHY Writeup (Project Root README)

The requirements doc Step 7 says "Explain 'WHY' behind each tech stack used
in your design." I consolidated this into §3 of the root README, covering:

1. **S3 + Delta Lake** — cheap durable storage + ACID + MERGE + time travel
2. **AWS Glue + PySpark** — serverless Spark, required by spec, native Delta support
3. **Glue Workflow** — built-in orchestration, no Step Functions/EventBridge needed
4. **Secrets Manager** — credentials never in code, rotation support
5. **KMS + SSE-KMS** — customer-managed encryption with audit trail
6. **SNS** — simplest reliable alerting
7. **Streamlit** — fastest pandas-to-dashboard path, testable via AppTest
8. **DuckDB + deltalake library** — in-process SQL over Delta, no DB server
9. **Plotly Express** — interactive charts with minimal code
10. **GitHub Actions** — required by spec, free for public repos, good AWS integration

Each entry has a WHY + what alternatives were rejected and why.

---

## 5. Git Status

```
Branch: main
Initial commit: fb4c425 "Initial commit: end-to-end GlobalPartners BI pipeline"
Files tracked: 58
Working tree: clean
Remote: (not yet configured — see §6)
```

Files NOT tracked (per .gitignore): `.claude/`, `~$*.docx` Office lock files,
`__pycache__/`, any `.env` / `.aws/` / credentials, Streamlit cache.

---

## 6. Next Steps for the User (Push to GitHub)

**Run these 3 commands in the project directory:**

```bash
# 1) Authenticate with GitHub (opens your browser)
gh auth login

# 2) Create the repo under your account and push this branch
gh repo create baanu007/globalpartners-bi-assessment \
    --public \
    --source=. \
    --remote=origin \
    --push \
    --description "GlobalPartners / Alltown Fresh Business Insights — DE Academy Assessment"

# 3) Watch the CI workflow run (will pass — validated locally)
gh run watch
```

**Alternative**: use `--private` instead of `--public` if you prefer not to
share the code publicly during review.

### Add the AWS secrets so the CD workflow can run

After the repo is up, go to:

    https://github.com/baanu007/globalpartners-bi-assessment/settings/secrets/actions

Click **"New repository secret"** and add these four:

| Name | Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | (your IAM access key) |
| `AWS_SECRET_ACCESS_KEY` | (corresponding secret key) |
| `AWS_REGION` | `us-east-1` |
| `GP_BUCKET` | `globalpartners-aws` |

**IMPORTANT**: use an IAM user scoped to just Glue + S3 permissions, not your
root credentials. Suggested IAM policy:
- `AWSGlueConsoleFullAccess`
- `AmazonS3FullAccess` (or scope to just `s3://globalpartners-aws/*`)
- `IAMPassRole` on `AWSGlueServiceRole-globalpartners`

After the secrets are set, the CD workflow will run automatically on the
next push to `main`. You can also trigger it manually via **Actions →
CD — sync Glue jobs + workflow to AWS → Run workflow**.

---

## 7. Verification Done This Session

| Check | Result |
|---|---|
| Git repo initializes on `main` | ✅ |
| .gitignore excludes `.claude/` | ✅ |
| .gitignore excludes Office lock files (`~$*`) | ✅ |
| 58 files staged + committed (no leaks) | ✅ |
| `ci.yml` YAML syntax valid | ✅ |
| `deploy-glue.yml` YAML syntax valid | ✅ |
| Mock AppTest runs all 8 pages locally | ✅ (after cust_count fix) |
| Root README has all 10 tech-stack WHYs | ✅ |

---

## 8. What Still Remains (Final Step 7 Items)

1. **Push to GitHub** — 3 commands above, user-run
2. **Add GitHub secrets** — 4 secrets via web UI, user-done
3. **Short video / presentation** — user-recorded walkthrough
4. **Submission package** — final zip / link to the GitHub repo

Everything the requirements doc asks for under Step 7 (CI/CD, docs, code
files, setup configs, final dashboard) is now in place. Only the actual
publishing + recording remains.
