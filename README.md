# Retail Medallion Pipeline — End-to-End AWS Data Engineering

End-to-end data engineering project for a fictional retail restaurant chain:
ingest transactional order data from RDS SQL Server, process through a
Bronze/Silver/Gold medallion architecture on AWS, compute 7 business metrics,
and expose them via an interactive Streamlit dashboard.

---

## Data

The source CSVs that originally fed this pipeline were proprietary client data
and are **not** included in this repository. The schema is a fairly generic
restaurant-chain order model:

- `order_items` — line-item-grain order data (header columns flattened in:
  customer id, restaurant id, order timestamps, totals, payment fields,
  loyalty fields, app/channel name, etc.)
- `order_item_options` — modifier / option rows that hang off `order_items`
- `date_dim` — a standard date dimension

To run this pipeline end-to-end you would provide your own retail order data
with a similar schema and load it into an RDS SQL Server instance. The Bronze
ingest job (`glue_jobs/bronze/bronze-ingest-all-tables.py`) reads the three
tables over JDBC; everything from Silver onwards is pure S3 + Delta Lake.

The Solution Design Document under `docs/design/` describes the schema and
data quality rules in more detail.

---

## 1. The 7 Business Metrics Delivered

| # | Metric | Gold Table | Dashboard Page |
|---|---|---|---|
| 1 | **CLV daily evolution** (PRIMARY) | `gold_clv_snapshot` | 1 — CLV Snapshot |
| 2 | **RFM Segmentation** (by behavior × loyalty) | `gold_rfm_segments` | 2 — RFM Segments |
| 3 | **Churn Indicators** (absolute + personal-gap tags) | `gold_churn_indicators` | 3 — Churn Indicators |
| 4 | **Sales Trends** (daily/weekly/monthly × location × category) | `gold_sales_daily/weekly/monthly` | 4 — Sales Trends |
| 5 | **Loyalty Program Impact** | `gold_loyalty_comparison` | 5 — Loyalty Comparison |
| 6 | **Location Performance** (revenue rank + retention) | `gold_location_perf` | 6 — Location Performance |
| 7 | **Upsell Analysis** (pivot from discount analysis) | `gold_upsell_summary` + `gold_upsell_top_options` | 7 — Upsell Analysis |

---

## 2. Architecture

```
RDS SQL Server (source)
        │ JDBC/SSL
        ▼
┌────────────────────┐
│  S3 Bronze (Delta) │  ← raw as-is ingestion, MERGE/upsert daily
└─────────┬──────────┘
          │ PySpark
          ▼
┌────────────────────┐
│  S3 Silver (Delta) │  ← cleaned, typed, DQ rules applied
└─────────┬──────────┘
          │ PySpark
          ▼
┌────────────────────┐
│  S3 Gold (Delta)   │  ← star schema: 4 dims + fact + 9 metric tables
└─────────┬──────────┘
          │
          ▼
┌────────────────────┐
│ Streamlit Dashboard│  ← 8 pages, reads Gold via deltalake + DuckDB
└────────────────────┘
```

**Orchestration**: AWS Glue Workflow `retail-daily-pipeline` chains
18 jobs via scheduled + conditional triggers. Fires daily at 02:00 UTC.

**Wall-clock end-to-end**: 13 min 21 sec (verified).

---

## 3. Tech Stack — Why Each Choice

### 3.1 Amazon S3 + Delta Lake
**Why**: Cheapest durable object store (~$0.023/GB); Delta Lake adds ACID
transactions, MERGE/upsert (critical for idempotent daily runs), schema
evolution, and time travel (30-day rollback without backup infrastructure).
Open format — no vendor lock-in.
**Alternatives rejected**: Parquet without Delta (no transactions, no
upsert), Iceberg (less mature tooling in AWS Glue 4.0), raw files (no
metadata).

### 3.2 AWS Glue + PySpark
**Why**: Serverless Spark — no cluster to manage, scales to data volume,
pay only for actual job runtime. Glue 4.0 includes native Delta Lake support
via the `--datalake-formats delta` parameter.
**Alternatives rejected**: EMR (requires cluster lifecycle management),
Databricks (external vendor), pure Lambda (won't scale to Spark workloads).

### 3.3 AWS Glue Workflow (native orchestration)
**Why**: Built-in scheduling, conditional triggers, and job chaining —
zero extra services. Our pipeline: SCHEDULED trigger → bronze →
conditional trigger (on bronze SUCCEEDED) → 3 silvers in parallel →
conditional trigger (on all 3 silvers SUCCEEDED) → 4 dims in parallel →
conditional trigger (on all 4 dims SUCCEEDED) → fact → conditional trigger
→ 9 metrics in parallel. 5 triggers total, 18 jobs chained.
**Alternatives rejected**: Step Functions (extra cost, extra IAM, extra
service to monitor), EventBridge + Lambda (more moving parts),
Airflow/MWAA (significant infra + cost overhead for a daily pipeline).

### 3.4 AWS Secrets Manager
**Why**: Aurora/SQL Server credentials never in code or env vars. Automatic
rotation supported. Accessed via IAM role attached to Glue job — no
hardcoded keys.
**Alternatives rejected**: Parameter Store (no rotation), env vars (leaks
in logs), hardcoding (never).

### 3.5 AWS KMS + SSE-KMS
**Why**: Customer-managed encryption keys with CloudTrail audit trail for
every decrypt. Rotation every 365 days. Meets enterprise compliance
requirements.
**Alternatives rejected**: SSE-S3 (default, no audit trail), client-side
encryption (complicates reads from Glue).

### 3.6 AWS SNS
**Why**: Simplest reliable alerting — publish on job failure, subscribers
(email, SMS, Slack webhook) receive notification. Zero operational
overhead. Free tier covers our volume.
**Alternatives rejected**: CloudWatch Alarms (harder to customize
messages), PagerDuty (external vendor, cost), custom email (SMTP
complexity).

### 3.7 Streamlit
**Why**: Fastest path from pandas DataFrame to interactive dashboard. Pure
Python — no JavaScript, no separate frontend build. Built-in caching
(`@st.cache_data`, `@st.cache_resource`), multi-page apps via filesystem
convention, programmatic testing via `streamlit.testing.v1.AppTest` (we
use this in our CI pipeline).
**Alternatives rejected**: Dash (more boilerplate, harder to test),
Tableau/PowerBI (external license), custom React (weeks of work for a
dashboard).

### 3.8 DuckDB (in-process) + `deltalake` Python library
**Why**: For querying the 10.8M-row `gold_clv_snapshot` in the dashboard
without shipping it to a database server. `deltalake` library reads Delta
tables directly from S3 (robust HTTPS stack). DuckDB runs SQL over
pyarrow-registered tables — aggregate queries over 10.8M rows complete in
< 1 second in-process. No separate database server, no Athena pay-per-query
costs for interactive use.
**Alternatives rejected**: Athena (latency + per-query cost), loading
everything into pandas (~1GB RAM, slow), DuckDB's native `delta_scan()`
extension (hit SSL cert chain issues on Windows dev env during testing).

### 3.9 Plotly Express
**Why**: Interactive charts (hover tooltips, zoom, pan) with minimal code.
Works natively with Streamlit via `st.plotly_chart`. Built-in support for
grouped bars, heatmaps, scatter with size+color aesthetics — every chart
type we need.
**Alternatives rejected**: Matplotlib (static only), Altair (less
interactive), Streamlit native charts (too limited).

### 3.10 GitHub Actions (CI/CD)
**Why**: Free for public repos, good AWS integration via
`aws-actions/configure-aws-credentials`. CI runs lint + syntax + Streamlit
import checks on every push; CD uploads Glue scripts and syncs the
workflow on merge to main.
**Alternatives rejected**: AWS CodePipeline (more setup, IAM tangle),
self-hosted Jenkins (infra overhead), bare bash scripts (no history, no
PR integration).

---

## 4. Repository Layout

```
.
├── README.md                          ← this file (tech stack + overview)
├── LICENSE                            ← MIT
├── .gitignore
├── .github/
│   └── workflows/
│       ├── ci.yml                     ← lint, syntax, Streamlit import tests
│       └── deploy-glue.yml            ← upload scripts + sync workflow on main
│
├── architecture_v3.drawio             ← architecture diagram (draw.io source)
├── architecture_v3.jpg                ← architecture diagram (image export)
├── date_dim.csv                       ← small reference date dimension
│
├── docs/
│   ├── design/                        ← Solution Design Documents (docx)
│   │   ├── Solution_Design_Document_v1.docx
│   │   ├── Solution_Design_Document_v3.docx
│   │   ├── Pipeline_Architecture_Design.docx
│   │   └── Data_Quality.docx
│   └── sessions/                      ← raw build session journals
│       ├── 01_data_exploration.md
│       ├── session_20260331_dq_report.md
│       ├── session_20260402_architecture_v3.md
│       ├── session_20260409_bronze_setup.md
│       ├── session_20260411_bronze_debug_success.md
│       ├── session_20260411_silver_etls_built.md
│       ├── session_20260411_gold_etls_built.md
│       ├── session_20260411_dashboard_built.md
│       ├── session_20260411_workflow_built.md
│       ├── session_20260411_cicd_github.md
│       └── session_20260411_spec_compliance_fixes.md
│
├── tools/
│   └── build_sdd_v3.py                ← regenerates the SDD docx from source
│
├── glue_jobs/
│   ├── bronze/
│   │   └── bronze-ingest-all-tables.py          ← 1 Bronze job
│   ├── silver/
│   │   ├── clean-order-items.py                 ← 3 Silver jobs
│   │   ├── clean-order-item-options.py
│   │   ├── clean-date-dim.py
│   │   └── _deploy/                             ← Glue job JSON configs
│   ├── gold/
│   │   ├── build-dim-date.py                    ← 4 dims
│   │   ├── build-dim-customer.py
│   │   ├── build-dim-restaurant.py
│   │   ├── build-dim-menu-item.py
│   │   ├── build-fact-orders.py                 ← 1 fact
│   │   ├── build-gold-clv-snapshot.py           ← 9 metrics (primary first)
│   │   ├── build-gold-rfm.py
│   │   ├── build-gold-churn.py
│   │   ├── build-gold-sales-daily.py
│   │   ├── build-gold-sales-weekly.py
│   │   ├── build-gold-sales-monthly.py
│   │   ├── build-gold-loyalty.py
│   │   ├── build-gold-location-perf.py
│   │   ├── build-gold-upsell.py
│   │   └── _deploy/
│   │       └── create_gold_jobs.py              ← idempotent Glue job creator
│   └── workflow/
│       └── create_workflow.py                   ← creates Glue Workflow + 5 triggers
│
└── dashboard/
    ├── app.py                          ← home page with hero CLV chart
    ├── pages/                          ← 7 metric pages
    │   ├── 1_CLV_Snapshot.py
    │   ├── 2_RFM_Segments.py
    │   ├── 3_Churn_Indicators.py
    │   ├── 4_Sales_Trends.py
    │   ├── 5_Loyalty_Comparison.py
    │   ├── 6_Location_Performance.py
    │   └── 7_Upsell_Analysis.py
    ├── lib/
    │   ├── __init__.py
    │   └── data.py                     ← cached loader (deltalake + DuckDB)
    ├── requirements.txt
    └── README.md                       ← dashboard-specific setup
```

---

## 5. How to Run Locally

### Prerequisites
- Python 3.10+
- AWS account with a named profile configured (referred to as `retail-chain`
  below — use whatever name you like)
  (`aws configure --profile retail-chain`)
- RDS SQL Server with the 3 tables (`order_items`, `order_item_options`,
  `date_dim`) loaded from your own source data
- An S3 bucket (referred to as `<BUCKET_NAME>` below)

### One-time AWS setup
```bash
# Create S3 bucket
aws s3 mb s3://<BUCKET_NAME> --profile retail-chain

# Upload scripts
for f in glue_jobs/bronze/*.py glue_jobs/silver/*.py glue_jobs/gold/*.py; do
  aws s3 cp $f s3://<BUCKET_NAME>/scripts/$(basename $f) --profile retail-chain
done

# Create Glue jobs (idempotent)
python glue_jobs/gold/_deploy/create_gold_jobs.py

# Create the Glue Workflow with daily 02:00 UTC trigger
python glue_jobs/workflow/create_workflow.py
```

### Run the pipeline manually
```bash
aws glue start-workflow-run \
    --name retail-daily-pipeline \
    --profile retail-chain
```

The workflow chains all 18 jobs in dependency order. Expect ~13-15 min
wall-clock end-to-end.

### Launch the dashboard
```bash
cd dashboard
pip install -r requirements.txt
AWS_PROFILE=retail-chain streamlit run app.py
```

Open http://localhost:8501.

---

## 6. CI/CD

### Continuous Integration (`.github/workflows/ci.yml`)
Runs on every push and pull request:

- **Python lint + format check** — ruff
- **Syntax check all .py files** — `py_compile`
- **JSON config validation** — validates Glue job deployment configs
- **Streamlit page import smoke test** — uses `streamlit.testing.v1.AppTest`
  (no real data access; verifies imports + top-level Python code compiles)
- **Requirements check** — validates `dashboard/requirements.txt` is valid

### Continuous Deployment (`.github/workflows/deploy-glue.yml`)
Runs on push to `main`:

- **Upload Glue scripts to S3** — all 18 PySpark files → `s3://{bucket}/scripts/`
- **Sync Glue jobs** — `create_gold_jobs.py` creates/updates all Gold jobs
- **Sync Glue Workflow** — `create_workflow.py` ensures workflow + 5 triggers

### Required GitHub Secrets
Set these in **Settings → Secrets and variables → Actions**:

| Secret Name | Example Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM access key with Glue/S3 permissions |
| `AWS_SECRET_ACCESS_KEY` | corresponding secret key |
| `AWS_REGION` | `us-east-1` |
| `BUCKET` | `<BUCKET_NAME>` |
| `GLUE_ROLE_ARN` | `arn:aws:iam::<AWS_ACCOUNT_ID>:role/<GLUE_ROLE>` |
| `SNS_TOPIC_ARN` | `arn:aws:sns:us-east-1:<AWS_ACCOUNT_ID>:<TOPIC_NAME>` |

The CI pipeline does NOT require any secrets — it only runs lint + syntax
+ import checks that work on any machine.

---

## 7. Pipeline Run History

Verified end-to-end workflow runs (see session reports for details):

| Date | Jobs | Status | Wall Clock |
|---|---|---|---|
| 2026-04-11 | 18/18 (Bronze→Silver→Dims→Fact→Metrics) | COMPLETED | 13:21 |

After the Bronze debug marathon on 2026-04-11 (9 errors fixed, patterns
established), we had **39 consecutive successful Glue job executions** with
zero debugging. The pattern-based approach (self-contained scripts,
DeltaTable API, defensive dedup, try/except wrapping, manifests + SNS
alerts) is fully validated.

---

## License

MIT — see [LICENSE](LICENSE).
