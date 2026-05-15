# Session Report - April 2, 2026

## Overview
Architecture review session. Analyzed SME feedback on v1 and v2, performed architect-level critique, and produced definitive v3 architecture.

---

## 1. SME Feedback History

### Feedback on v1 (`Pipeline_Architecture.drawio`)
- **Over-engineered**: EventBridge, Step Functions, Glue Data Catalog are unnecessary
- **Aurora is redundant**: SQL Server is already on RDS — connect Glue directly via JDBC
- **Use Delta Lake on S3**: Delta merge handles incremental ingestion without separate metadata tracking
- **Orchestrate within Glue**: Glue Workflows have built-in scheduling and job chaining

### Feedback on v2 (`architecture 2.drawio`)
- **Single monolithic Glue jobs per stage**: If one table fails inside a job, all tables in that job fail — no isolation
- **No failure alert pipeline**: SNS and CloudWatch are shown as disconnected boxes with no flow
- **No visible failure handling**: What happens when a job fails? Who gets told? How do you reload?

---

## 2. Architect-Level Critique (Additional Issues Found)

### 2.1 Aurora Hop Still Referenced in SDD
The Solution Design Document still describes Aurora as an intermediate database. v2 removed it from the diagram but the written document was never updated. Inconsistency between docs.

### 2.2 v1 SDD and v2 Diagram Are Contradictory
- SDD says: Parquet format, Glue Data Catalog, Step Functions, EventBridge
- v2 diagram says: Delta Lake, Glue Workflow, no Data Catalog
- These are two different architectures in the same project folder

### 2.3 CLV Daily Evolution is Incorrectly Modeled
Requirements: "show how LTV evolves for each customer daily"
Current design: `gold_clv_daily` has "one row per customer per order date" — only creates rows on days the customer ordered. On days with no orders, the customer has no CLV row. This is NOT true daily evolution — it's event-triggered snapshots.

Fix: Rename to `gold_clv_snapshot`. Cross-join active customers with `dim_date` to produce 1 row per customer per calendar day, carrying forward the cumulative CLV. This is what "daily evolution" means.

### 2.4 No Data Quality Gate Between Layers
No validation that Bronze-to-Silver didn't lose rows or that Gold tables contain sane numbers. A broken Glue job could silently write an empty table and the dashboard would show zeros with no alert.

### 2.5 Encryption is Default-Level Only
Requirements explicitly say "encryption". SSE-S3 is the default — anyone with S3 gets it. Production-level means:
- SSE-KMS (customer-managed keys) for audit trail via CloudTrail
- VPC Endpoints so Glue-to-S3 traffic stays private
- JDBC over SSL to RDS

### 2.6 Failure/Reload Mechanism is Vague
"Re-run the same day" is not a reload mechanism. Need:
- Auto-retry with backoff
- Partial failure handling (some tables succeed, some fail)
- Manual backfill capability (reload a date range)
- Rollback capability (undo a bad run)
- Idempotent design (rerun = same result)

### 2.7 Partitioning Strategy Never Defined
S3 data is written as Delta but partition keys are never specified. Without `ingestion_date` partitioning, every run does a full-table rewrite.

### 2.8 Discount Pivot Not Documented in SDD
Requirements say "Use option_price < 0 to detect discounts." Zero negative prices exist in data. The pivot to upsell analysis is correct but the SDD never documents this deviation or records SME sign-off.

---

## 3. V3 Architecture Design

### 3.1 Services (7 total, no external tools)

| # | Service | Role |
|---|---------|------|
| 1 | RDS SQL Server | Source database (existing) |
| 2 | Amazon S3 | Data lake — Bronze/Silver/Gold layers, Delta Lake format |
| 3 | AWS Glue (PySpark) | All ETL jobs — ingestion, cleaning, metric calculation |
| 4 | AWS Glue Workflow | Orchestration, scheduling, job chaining, failure triggers |
| 5 | Amazon Athena | Serverless SQL queries on Gold layer |
| 6 | Amazon SNS | Failure email alerts |
| 7 | AWS KMS | Encryption key management |

Supporting (no extra cost/license): Secrets Manager, VPC Endpoints, IAM, CloudWatch, Delta Lake (open-source on Glue).

**Removed from v1**: Aurora, EventBridge, Step Functions, Glue Data Catalog.

### 3.2 Data Flow
```
RDS SQL Server --JDBC/SSL--> S3 Bronze (Delta) --> S3 Silver (Delta) --> S3 Gold (Delta) --> Athena --> Streamlit
```

### 3.3 Why 3 Jobs (Not 15)
Research confirmed: Glue job cold start = 30-60 seconds per job. Splitting into 15 table-level jobs adds 10+ minutes of pure startup overhead on a 54 MB dataset. AWS best practice is one job per logical layer unless tables have different SLAs or resource needs. Our tables don't.

**The solution to failure isolation is not more jobs — it's try/except within each job.**

### 3.4 Glue Workflow Structure
```
Scheduled Trigger (Daily 2 AM UTC)
    │
    ▼
Glue Job 1: INGEST (Bronze)        MaxRetries=2
  - JDBC to RDS SQL Server (SSL, creds from Secrets Manager)
  - Delta merge/upsert per table (only new/changed rows via _delta_log)
  - try/except per table — partial success continues
  - Writes status manifest to S3
    │
    ├── on success ──▶ Glue Job 2: CLEAN (Silver)
    │                    - Read Bronze, apply DQ rules per table
    │                    - Row count validation (bronze vs silver)
    │                    - try/except per table
    │                    - Writes status manifest
    │                      │
    │                      ├── on success ──▶ Glue Job 3: METRICS (Gold)
    │                      │                    - Build dims, then fact, then metrics
    │                      │                    - try/except per table
    │                      │                    - Writes status manifest
    │                      │                      │
    │                      │                      ├── on success ──▶ [DONE]
    │                      │                      └── on failure ──▶ Alert Job
    │                      └── on failure ──▶ Alert Job
    └── on failure ──▶ Alert Job
```

### 3.5 Alert Job (Failure Pipeline)
A lightweight Python Shell Glue job (no Spark, no DPU cost) triggered by `on failure` conditional triggers — a native Glue Workflow feature.

```python
import boto3, json, sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
sns = boto3.client('sns')
sns.publish(
    TopicArn='arn:aws:sns:us-east-1:ACCOUNT:pipeline-failure-alerts',
    Subject=f"PIPELINE FAILED: {args['WORKFLOW_NAME']}",
    Message=json.dumps({
        "workflow": args['WORKFLOW_NAME'],
        "run_id": args['WORKFLOW_RUN_ID'],
        "action": "Check CloudWatch Logs for stack trace"
    })
)
```

No EventBridge needed. Everything stays inside Glue.

### 3.6 Failure Reload Mechanism

| Scenario | Mechanism |
|----------|-----------|
| Transient failure (network, timeout) | Auto-retry: MaxRetries=2 per job, Glue handles backoff |
| One table fails, others healthy | try/except per table: healthy tables process, manifest logs which failed |
| Need to rerun today's batch | Manual trigger via Glue Console — idempotent (Delta merge = same result) |
| Need to backfill a date range | Pass `--start-date` / `--end-date` parameters to workflow run |
| Need to undo a bad run | Delta Lake time travel: `RESTORE TABLE AS OF VERSION N` — 30-day retention |
| Dashboard shows bad data | S3 Versioning + Delta versioning — restore previous state without re-processing |

### 3.7 Encryption

| Layer | Method | Detail |
|-------|--------|--------|
| At rest (S3) | SSE-KMS | Customer-managed key, rotation every 365 days, audited via CloudTrail |
| At rest (RDS) | AES-256 | Managed by RDS |
| In transit (JDBC) | TLS/SSL | JDBC connection string with `encrypt=true;trustServerCertificate=false` |
| In transit (S3) | HTTPS | Enforced via S3 bucket policy (`aws:SecureTransport`) |
| Credentials | Secrets Manager | Auto-rotation, never hardcoded, accessed via IAM role |
| Logs | CloudWatch | Encrypted with service-managed keys |
| Network | VPC Endpoints | Glue-to-S3 and Glue-to-RDS traffic stays on AWS private network |

### 3.8 Partitioning Strategy

| Layer | Partition Key | Why |
|-------|--------------|-----|
| Bronze | `ingestion_date` (YYYY-MM-DD) | Each daily run writes to its own partition, enables date-range backfill |
| Silver | `ingestion_date` | Mirrors Bronze, DQ applied per partition |
| Gold (fact_orders) | `order_year_month` (YYYY-MM) | Optimises Athena queries that filter by date range |
| Gold (metrics) | None (small tables) | Full overwrite daily — tables are <1 MB each |

### 3.9 CLV Daily Evolution (Fixed)
Previous design: 1 row per customer per ORDER date = gaps on non-order days.

V3 design: `gold_clv_snapshot` — cross-join all active customers with all calendar dates from `dim_date`. Each row carries the customer's cumulative CLV as of that date, their CLV tier (High/Medium/Low), and running totals. This produces a complete time series for every customer, enabling true "how does CLV evolve daily" analysis on the dashboard.

### 3.10 Gold Layer Data Model (Complete)

**Dimensions:**
- `dim_customer` — USER_ID, first_order_date, last_order_date, total_orders, total_spend, is_loyalty, clv_tier
- `dim_restaurant` — RESTAURANT_ID, order_count, revenue (27 locations after dev exclusion)
- `dim_date` — Extended from 365 to ~1,402 rows (2020-04-21 to 2024-02-21), US federal holidays
- `dim_menu_item` — ITEM_NAME, cleaned ITEM_CATEGORY (40 categories after cleaning)

**Fact:**
- `fact_orders` — 1 row per ORDER_ID: customer_key, restaurant_key, date_key, hour_bucket, item_count, total_item_revenue, total_option_revenue, total_revenue, has_paid_options, is_loyalty

**Metrics (Primary):**
- `gold_clv_snapshot` — 1 row per customer per calendar day: cumulative_spend, order_count, clv_tier (High=top 20%, Medium=mid 60%, Low=bottom 20%)

**Metrics (Secondary):**
- `gold_rfm_segments` — 1 row per customer: recency_days, frequency, monetary, R/F/M scores (1-5), segment (VIP, New, Churn Risk, Regular)
- `gold_churn_indicators` — 1 row per customer: days_since_last_order, avg_gap_days, spend_change_pct, churn_tag (Active/Cooling Off/At Risk/Inactive)
- `gold_sales_daily` — daily revenue by restaurant, category, hour_bucket
- `gold_sales_weekly` — weekly aggregation
- `gold_sales_monthly` — monthly aggregation
- `gold_loyalty_comparison` — loyalty vs non-loyalty: avg_spend, repeat_rate, avg_clv
- `gold_location_perf` — 1 row per restaurant: total_revenue, avg_order_value, orders_per_week, unique_customers, rank
- `gold_upsell_analysis` — orders with/without paid options, option_revenue_pct, top paid options (replaces discount analysis since zero negative prices exist in data)

---

## 4. Files Updated This Session
| File | Description |
|------|-------------|
| `architecture_v3.drawio` | New definitive architecture diagram — open in app.diagrams.net |
| `Solution_Design_Document_v3.docx` | Comprehensive 18-section SDD with full justification for every decision |
| `build_sdd_v3.py` | Python script that generates the SDD (for reproducibility) |
| `session_20260402_architecture_v3.md` | This file |

---

## 5. What Needs Updating Next
- `Solution Design Document.docx` — must be rewritten to match v3 (currently describes v1 with Aurora, EventBridge, Step Functions)
- `Pipeline_Architecture_Design.docx` — same, needs full rewrite for v3
- SME approval on v3 before proceeding to Step 4 (build pipeline code)
