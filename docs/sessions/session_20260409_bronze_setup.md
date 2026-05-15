# Session Report - April 9, 2026

## Overview
Planning session for Bronze layer ingestion. Finalized Glue job structure (combined Bronze, per-table Silver/Gold), mapped all jobs against requirements, prepared SQL Server table creation scripts for Aurora, built `common/utils.py` and `bronze/ingest_all_tables.py`, and walked through full AWS setup steps and pipeline execution logic.

---

## 1. Glue Job Architecture Decision

### SME Feedback
> "Overall this looks good. You could also consider creating Glue jobs per table instead of combining everything as it makes debugging easier if one table fails."

### Decision Made
- **Bronze**: One combined job (`ingest_all_tables.py`) — all 3 tables in a single script with try/except per table
- **Silver**: Per-table jobs — one script per table for cleaning/validation
- **Gold**: Per-table jobs — one script per dimension, fact, and metric table

### Rationale
- Bronze is a straight JDBC copy — if RDS is down, all tables fail anyway. One job is fine.
- Silver has different DQ rules per table — separate jobs = easier debugging.
- Gold has complex business logic and table dependencies — per-table jobs allow surgical retry and clear CloudWatch logs.

---

## 2. Final Glue Job File Structure

```
glue_jobs/
  common/
    utils.py                          # Shared helper functions
  bronze/
    ingest_all_tables.py              # Single combined job (3 tables)
  silver/
    clean_order_items.py
    clean_order_item_options.py
    clean_date_dim.py
  gold/
    build_dim_customer.py
    build_dim_restaurant.py
    build_dim_date.py
    build_dim_menu_item.py
    build_fact_orders.py
    build_gold_clv_snapshot.py
    build_gold_rfm_segments.py
    build_gold_churn_indicators.py
    build_gold_sales_daily.py
    build_gold_sales_weekly.py
    build_gold_sales_monthly.py
    build_gold_loyalty_comparison.py
    build_gold_location_perf.py
    build_gold_upsell_analysis.py
  alert/
    alert_job.py
  workflow/
    workflow_definition.py
```

---

## 3. Requirements Coverage Verification

### Gold Tables vs Business Requirements
| Requirement | Gold Table | Covered |
|---|---|---|
| CLV daily evolution (Primary) | `gold_clv_snapshot` | Yes |
| CLV tiers (High/Med/Low) | `gold_clv_snapshot` | Yes |
| RFM Segmentation | `gold_rfm_segments` | Yes |
| Churn Indicators | `gold_churn_indicators` | Yes |
| Sales Trends (daily/weekly/monthly) | `gold_sales_daily/weekly/monthly` | Yes |
| Loyalty Program Impact | `gold_loyalty_comparison` | Yes |
| Top/Bottom Locations | `gold_location_perf` | Yes |
| Discount/Pricing Effectiveness | `gold_upsell_analysis` | Yes |

### Dashboard Requirements (Step 6)
| Dashboard | Gold Table(s) Feeding It |
|---|---|
| Customer Segmentation | `rfm_segments` + `dim_customer` |
| Churn Risk Indicators | `churn_indicators` |
| Sales Trends & Seasonality | `sales_daily`, `sales_weekly`, `sales_monthly` |
| Loyalty Program Impact | `loyalty_comparison` |
| Location Performance | `location_perf` |
| Pricing & Discount Effectiveness | `upsell_analysis` |

### Architecture Requirements
| Requirement | How Covered |
|---|---|
| Source = SQL Server | JDBC in Bronze job |
| AWS only, no Snowflake/DBT | Glue + S3 + Athena only |
| All logic in PySpark | Every job is PySpark |
| Daily batch schedule | Glue Workflow with daily trigger |
| Encryption | SSE-KMS, SSL/TLS, VPC Endpoints |
| Failure/reload | Per-table jobs = surgical retry + alert job via SNS |
| CI/CD via GitHub | Step 7 deliverable (later) |

---

## 4. Bronze Ingestion Job — 6-Step Anatomy

### Step 1: Initialize Glue Job
- Import Glue and Spark libraries
- Read job arguments (S3_BUCKET, SECRET_NAME, REGION, SNS_TOPIC_ARN)
- Create SparkSession with Delta Lake support

### Step 2: Get RDS Credentials from Secrets Manager
- Use boto3 to call Secrets Manager at runtime
- Parse JSON response for username/password/host/port/dbname

### Step 3: Define Tables to Ingest
| Source Table (SQL Server) | Target S3 Path | Strategy | Merge Key |
|---|---|---|---|
| `dbo.order_items` | `s3://bucket/bronze/order_items/` | Delta MERGE | ORDER_ID |
| `dbo.order_item_options` | `s3://bucket/bronze/order_item_options/` | Delta MERGE | ORDER_ID + LINEITEM_ID |
| `dbo.date_dim` | `s3://bucket/bronze/date_dim/` | Overwrite | date_key |

### Step 4: Read Each Table via JDBC
- `spark.read.format("jdbc")` with SQL Server driver
- SSL encryption enabled (`encrypt=true;trustServerCertificate=false`)
- Returns Spark DataFrame per table

### Step 5: Add Metadata Columns & Write to S3
- Add `ingestion_date` (partition key) and `ingestion_timestamp` (audit)
- Write as Delta format with `partitionBy("ingestion_date")`
- Mode = Delta MERGE/UPSERT on primary key (idempotent — rerun same day = same result)

### Step 6: Try/Except Per Table + Status Manifest
- Loop over tables with try/except — partial failure continues other tables
- Write status manifest JSON to S3 recording success/fail per table
- Exit 0 if all succeeded, exit 1 if any failed

---

## 5. Aurora SQL Server Setup

### Tables Created in `retail-chain` Database

```sql
-- Table 1: order_items (203,519 rows)
CREATE TABLE dbo.order_items (
    APP_NAME            NVARCHAR(100),
    RESTAURANT_ID       NVARCHAR(50),
    CREATION_TIME_UTC   NVARCHAR(50),
    ORDER_ID            NVARCHAR(50),
    USER_ID             NVARCHAR(50),
    PRINTED_CARD_NUMBER NVARCHAR(50),
    IS_LOYALTY          NVARCHAR(10),
    CURRENCY            NVARCHAR(10),
    LINEITEM_ID         NVARCHAR(50),
    ITEM_CATEGORY       NVARCHAR(100),
    ITEM_NAME           NVARCHAR(200),
    ITEM_PRICE          DECIMAL(10,2),
    ITEM_QUANTITY       INT
);

-- Table 2: order_item_options (193,017 rows)
CREATE TABLE dbo.order_item_options (
    ORDER_ID            NVARCHAR(50),
    LINEITEM_ID         NVARCHAR(50),
    OPTION_GROUP_NAME   NVARCHAR(200),
    OPTION_NAME         NVARCHAR(200),
    OPTION_PRICE        DECIMAL(10,2),
    OPTION_QUANTITY     INT
);

-- Table 3: date_dim (365 rows)
CREATE TABLE dbo.date_dim (
    date_key            NVARCHAR(20),
    year                INT,
    month               INT,
    week                INT,
    day_of_week         NVARCHAR(20),
    is_weekend          NVARCHAR(10),
    is_holiday          NVARCHAR(10),
    holiday_name        NVARCHAR(100)
);
```

### Data Loading
- CSV files loaded into Aurora SQL Server tables via SSMS Import Wizard
- Row counts: order_items = 203,519 | order_item_options = 193,017 | date_dim = 365

---

## 6. Code Built This Session

### `glue_jobs/common/utils.py` — COMPLETE
Shared helper functions used by all 17 Glue jobs:
| Function | Purpose |
|---|---|
| `get_logger()` | CloudWatch-compatible logger |
| `get_secret()` | Fetches DB credentials from Secrets Manager at runtime |
| `build_jdbc_url()` | Builds SSL-encrypted SQL Server JDBC URL |
| `write_manifest()` | Writes per-table status JSON to S3 after each run |
| `send_failure_alert()` | Publishes failure message to SNS topic |

### `glue_jobs/bronze/ingest_all_tables.py` — COMPLETE
Full Bronze ingestion job:
| Feature | Implementation |
|---|---|
| Source read | JDBC over SSL from RDS SQL Server |
| Write strategy | Delta MERGE/UPSERT on primary key |
| Partition | `ingestion_date` |
| Partial failure | try/except per table — other tables still process |
| Idempotency | Delta MERGE = rerun same day = no duplicates |
| Alert | SNS published if any table fails |
| Exit code | exit(1) on failure → blocks Silver/Gold in workflow |

---

## 7. AWS Infrastructure — Status

| Component | Status |
|---|---|
| S3 bucket with bronze/silver/gold folders | DONE |
| Secrets Manager secret (`retail-chain/aurora/credentials`) | DONE |
| IAM Role (`AWSGlueServiceRole-retail-chain`) | DONE |
| SNS Topic (`pipeline-failure-alerts`) | DONE |
| Scripts uploaded to S3 (`scripts/` folder) | PENDING |
| Glue job created (`bronze-ingest-all-tables`) | PENDING |
| Glue Workflow + EventBridge schedule | PENDING |
| Bronze test run verified | PENDING |

---

## 8. How the Pipeline Runs (Execution Order)

```
EventBridge (2AM daily)
        ↓
   Glue Workflow starts
        ↓
   Bronze job (ingest_all_tables.py)
        ↓ on success
   Silver jobs x3 (run in parallel)
        ↓ on success
   Gold jobs — dims first → fact → metrics (parallel where possible)
        ↓
      DONE

On any failure:
   Alert Job → SNS → email alert
   Downstream jobs blocked (exit code 1)
```

## 9. Failure & Reload Scenarios — How Each Is Handled

| Scenario | What Happens | How to Fix |
|---|---|---|
| Bronze fails midway | Delta rolls back incomplete write. Manifest records which table failed. SNS alert fires. Silver/Gold blocked. | Fix issue, rerun Bronze only. Delta MERGE handles no duplicates. |
| Silver fails, Bronze succeeded | Bronze untouched. Only failed Silver job reruns. Gold blocked. | Rerun Silver job only. |
| Source had missing rows | Pipeline runs on incomplete data. Manifest records row counts. | Roll back Bronze with Delta time travel. Rerun once source is complete. |
| Need to backfill past date | Pass `--RUN_DATE 2026-04-07` param. Delta MERGE upserts into correct partition only. | Manual trigger with date param. |
| Duplicate on rerun | Delta MERGE matches on primary key — updates existing, inserts new. Never duplicates. | No action needed — idempotent by design. |

---

## 10. Next Steps (Resume Here Tomorrow)

1. Upload `utils.py` and `ingest_all_tables.py` to `s3://your-bucket/scripts/`
2. Create Glue job `bronze-ingest-all-tables` in console
   - Point script to `s3://your-bucket/scripts/ingest_all_tables.py`
   - Add Extra Python files: `s3://your-bucket/scripts/utils.py`
   - Set 4 job parameters: S3_BUCKET, SECRET_NAME, REGION, SNS_TOPIC_ARN
3. Run the job and verify Bronze S3 output and status manifest
4. Set up Glue Workflow + EventBridge daily trigger
5. Start building Silver jobs (x3)
