"""
Build Solution Design Document v3 - Retail Restaurant Pipeline Architecture
"""
from docx import Document
from docx.shared import Inches, Pt, RGBColor, Cm, Emu
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.section import WD_ORIENT
from docx.oxml.ns import qn
from docx.oxml import OxmlElement
import os

doc = Document()

# ── Styles ──────────────────────────────────────────────────────────────
style = doc.styles['Normal']
font = style.font
font.name = 'Calibri'
font.size = Pt(11)
style.paragraph_format.space_after = Pt(6)
style.paragraph_format.line_spacing = 1.15

for level in range(1, 5):
    hs = doc.styles[f'Heading {level}']
    hs.font.name = 'Calibri'
    hs.font.color.rgb = RGBColor(0x1B, 0x3A, 0x5C)

doc.styles['Heading 1'].font.size = Pt(20)
doc.styles['Heading 2'].font.size = Pt(16)
doc.styles['Heading 3'].font.size = Pt(13)
doc.styles['Heading 4'].font.size = Pt(11)


def add_table(headers, rows, col_widths=None):
    """Add a formatted table."""
    table = doc.add_table(rows=1 + len(rows), cols=len(headers))
    table.style = 'Light Grid Accent 1'
    table.alignment = WD_TABLE_ALIGNMENT.LEFT
    # Header
    for i, h in enumerate(headers):
        cell = table.rows[0].cells[i]
        cell.text = h
        for p in cell.paragraphs:
            p.runs[0].bold = True
            p.runs[0].font.size = Pt(10)
    # Rows
    for r_idx, row in enumerate(rows):
        for c_idx, val in enumerate(row):
            cell = table.rows[r_idx + 1].cells[c_idx]
            cell.text = str(val)
            for p in cell.paragraphs:
                for run in p.runs:
                    run.font.size = Pt(10)
    if col_widths:
        for i, w in enumerate(col_widths):
            for row in table.rows:
                row.cells[i].width = Cm(w)
    doc.add_paragraph()
    return table


def add_bullet(text, level=0, bold_prefix=None):
    """Add a bullet point."""
    p = doc.add_paragraph(style='List Bullet')
    p.paragraph_format.left_indent = Cm(1.27 + level * 1.27)
    if bold_prefix:
        run = p.add_run(bold_prefix)
        run.bold = True
        p.add_run(text)
    else:
        p.add_run(text)
    return p


def add_note(text, label="Note"):
    """Add an indented note."""
    p = doc.add_paragraph()
    p.paragraph_format.left_indent = Cm(1.27)
    run = p.add_run(f"{label}: ")
    run.bold = True
    run.font.color.rgb = RGBColor(0x88, 0x44, 0x00)
    p.add_run(text)
    return p


def add_code(text):
    """Add a code block."""
    p = doc.add_paragraph()
    p.paragraph_format.left_indent = Cm(1.27)
    p.paragraph_format.space_before = Pt(4)
    p.paragraph_format.space_after = Pt(4)
    run = p.add_run(text)
    run.font.name = 'Consolas'
    run.font.size = Pt(9)
    run.font.color.rgb = RGBColor(0x33, 0x33, 0x33)
    return p


# ════════════════════════════════════════════════════════════════════════
# COVER PAGE
# ════════════════════════════════════════════════════════════════════════
for _ in range(6):
    doc.add_paragraph()

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run("Solution Design Document")
run.font.size = Pt(32)
run.bold = True
run.font.color.rgb = RGBColor(0x1B, 0x3A, 0x5C)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run("Retail Restaurant — Business Insights Pipeline")
run.font.size = Pt(18)
run.font.color.rgb = RGBColor(0x44, 0x72, 0xC4)

doc.add_paragraph()

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run("Version 3.0")
run.font.size = Pt(14)

p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
run = p.add_run("April 2, 2026")
run.font.size = Pt(12)
run.font.color.rgb = RGBColor(0x66, 0x66, 0x66)

for _ in range(4):
    doc.add_paragraph()

add_table(
    ["Field", "Value"],
    [
        ["Document", "Solution Design Document — Pipeline Architecture v3"],
        ["Project", "Retail Chain Business Insights project (Retail Restaurant)"],
        ["Version", "3.0 (supersedes v1 and v2)"],
        ["Date", "April 2, 2026"],
        ["Classification", "Internal — DE Academy Assessment"],
    ],
    col_widths=[5, 12]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# REVISION HISTORY
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("Revision History", level=1)

add_table(
    ["Version", "Date", "Change", "SME Feedback Addressed"],
    [
        ["1.0", "Mar 31, 2026", "Initial architecture: Aurora, EventBridge, Step Functions, Glue Data Catalog, Parquet, 4-step orchestration",
         "N/A — first draft"],
        ["2.0", "Apr 1, 2026", "Removed Aurora, EventBridge, Step Functions, Glue Data Catalog. Adopted Delta Lake, Glue Workflow, direct JDBC to RDS.",
         "v1 was over-engineered. Too many services. Use Delta Lake for incremental tracking. Orchestrate within Glue."],
        ["3.0", "Apr 2, 2026", "Added failure alert flow (Alert Job + SNS), try/except per table, DQ gates, SSE-KMS encryption, VPC Endpoints, partitioning strategy, CLV daily evolution fix, backfill/rollback mechanism.",
         "v2 had monolithic jobs with no failure isolation, no visible alert pipeline, no reload mechanism."],
    ],
    col_widths=[1.8, 2.5, 7, 7]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# TABLE OF CONTENTS (manual)
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("Table of Contents", level=1)

toc_items = [
    "1. Executive Summary",
    "2. Business Requirements Traceability",
    "3. Source Data Profile",
    "4. Architecture Overview",
    "5. AWS Services — What, Why, and Why Not the Alternatives",
    "6. Data Flow — Layer by Layer",
    "7. Orchestration — AWS Glue Workflow",
    "8. Failure Handling & Reload Mechanism",
    "9. Failure Alert Pipeline",
    "10. Encryption & Security",
    "11. Data Model — Gold Layer",
    "12. Business Metrics — Calculation Logic",
    "13. Data Quality Rules",
    "14. Dashboard Consumption",
    "15. Design Decisions Register",
    "16. Risk Register",
    "17. Appendix A — Glossary",
    "18. Appendix B — Rejected Alternatives",
]
for item in toc_items:
    p = doc.add_paragraph(item)
    p.paragraph_format.space_after = Pt(2)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 1. EXECUTIVE SUMMARY
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("1. Executive Summary", level=1)

doc.add_paragraph(
    "This document describes the production-grade data pipeline architecture for the Retail Restaurant "
    "Business Insights platform. The pipeline ingests transactional data from 27 restaurant locations, "
    "cleans and transforms it through a medallion architecture (Bronze/Silver/Gold), calculates seven "
    "categories of business metrics, and serves them to a Streamlit dashboard via Amazon Athena."
)

doc.add_paragraph(
    "The primary business goal is to calculate and track Customer Lifetime Value (CLV) daily, showing "
    "how each customer's value evolves over time. Secondary goals include RFM segmentation, churn "
    "indicators, sales trend monitoring, loyalty program impact analysis, location performance ranking, "
    "and pricing/upsell effectiveness."
)

doc.add_heading("Architecture at a Glance", level=2)
add_code("RDS SQL Server ──JDBC/SSL──> S3 Bronze (Delta) ──> S3 Silver (Delta) ──> S3 Gold (Delta) ──> Athena ──> Streamlit")

doc.add_heading("Key Numbers", level=2)
add_table(
    ["Metric", "Value"],
    [
        ["Source tables", "3 (order_items, order_item_options, date_dim)"],
        ["Total source rows", "396,901"],
        ["Total data volume", "~54 MB"],
        ["Clean orders", "~130,000"],
        ["Clean customers (with USER_ID)", "20,174"],
        ["Restaurant locations", "27 (after dev exclusion)"],
        ["Date range", "April 21, 2020 — February 21, 2024"],
        ["Gold tables produced", "14 (4 dims + 1 fact + 9 metrics)"],
        ["AWS services used", "7 (no external tools or licenses)"],
        ["Pipeline frequency", "Daily batch at 2:00 AM UTC"],
        ["All ETL logic", "PySpark on AWS Glue"],
    ],
    col_widths=[6, 12]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 2. BUSINESS REQUIREMENTS TRACEABILITY
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("2. Business Requirements Traceability", level=1)

doc.add_paragraph(
    "Every requirement from the Retail Chain Business Analysis Requirements document is traced to "
    "a specific component in this architecture. This section exists so the SME can verify that nothing "
    "was missed."
)

add_table(
    ["Requirement (from Brief)", "Where It Is Addressed", "Section"],
    [
        ["Design a pipeline for ingesting, transforming, and storing data",
         "Full medallion pipeline: Bronze (ingest) → Silver (transform) → Gold (store)", "§6"],
        ["Production level pipeline", "Auto-retry, failure alerting, DQ gates, idempotent Delta merge", "§7, §8, §9"],
        ["Scheduling", "Glue Workflow scheduled trigger, daily at 2 AM UTC", "§7"],
        ["Encryption", "SSE-KMS at rest, TLS 1.2 in transit, VPC Endpoints, Secrets Manager", "§10"],
        ["Failure reload mechanism", "MaxRetries=2, try/except per table, manual backfill, Delta time travel rollback", "§8"],
        ["Source is SQL Server", "RDS SQL Server — Glue connects via JDBC over SSL, no intermediate database", "§5, §6"],
        ["Entire architecture using AWS resources", "7 AWS services only. No Snowflake, DBT, or external tools", "§5"],
        ["No new licenses", "All services are pay-as-you-go AWS. Delta Lake is open-source (Apache 2.0)", "§5"],
        ["All logic in PySpark", "Every ETL job is a PySpark Glue job. Zero SQL transformations.", "§6"],
        ["Schedule pipeline once daily as batch", "Glue Workflow scheduled trigger fires at 2:00 AM UTC daily", "§7"],
        ["CLV — daily evolution per customer (Primary)", "gold_clv_snapshot: 1 row per customer per calendar day", "§11, §12.1"],
        ["RFM segmentation (Secondary)", "gold_rfm_segments: R/F/M scores + segment labels", "§12.2"],
        ["Churn indicators (Secondary)", "gold_churn_indicators: inactivity thresholds + churn tags", "§12.3"],
        ["Sales trends — daily/weekly/monthly (Secondary)", "gold_sales_daily, gold_sales_weekly, gold_sales_monthly", "§12.4"],
        ["Loyalty program impact (Secondary)", "gold_loyalty_comparison: loyalty vs non-loyalty metrics", "§12.5"],
        ["Top-performing locations (Secondary)", "gold_location_perf: ranked by revenue + operational metrics", "§12.6"],
        ["Pricing & discount effectiveness (Secondary)", "gold_upsell_analysis: free vs paid add-ons (no discount data exists — see §12.7)", "§12.7"],
        ["Data model that satisfies all above", "Star schema: 4 dims + 1 fact + 9 metric tables", "§11"],
        ["Pipeline architecture diagram", "architecture_v3.drawio (open in draw.io)", "§4"],
        ["Solution design document", "This document", "—"],
    ],
    col_widths=[5.5, 7.5, 1.5]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 3. SOURCE DATA PROFILE
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("3. Source Data Profile", level=1)

doc.add_paragraph(
    "The source data resides in a SQL Server database hosted on AWS RDS. It consists of three tables "
    "representing transactional data from the Retail Restaurant restaurant chain."
)

doc.add_heading("3.1 Tables Overview", level=2)
add_table(
    ["Table", "Rows", "Columns", "Description", "Primary Key"],
    [
        ["order_items", "203,519", "13", "Line-item transactions per customer order", "LINEITEM_ID"],
        ["order_item_options", "193,017", "6", "Add-ons and customisations per line item", "Composite (ORDER_ID + LINEITEM_ID + OPTION_GROUP_NAME + OPTION_NAME)"],
        ["date_dim", "365", "8", "Calendar dimension (2023 only — will be extended)", "date_key"],
    ],
    col_widths=[3.5, 2, 1.5, 5, 5]
)

doc.add_heading("3.2 Relationships", level=2)
add_code(
    "order_items.ORDER_ID ──1:M──> order_item_options.ORDER_ID\n"
    "order_items.LINEITEM_ID ──1:M──> order_item_options.LINEITEM_ID\n"
    "order_items.CREATION_TIME_UTC (date part) ──M:1──> date_dim.date_key"
)

doc.add_heading("3.3 Critical Data Quality Facts (Validated)", level=2)
doc.add_paragraph("These facts were verified via Python scripts against the raw CSV data during Step 1 and Step 2:")

add_table(
    ["Finding", "Impact", "Action"],
    [
        ["590 rows with ITEM_PRICE > $50 (max $5,000)", "0.3% of rows distort 79% of total revenue", "Filter: ITEM_PRICE <= $50"],
        ["84 rows with ITEM_QUANTITY > 20 (max 500)", "Overlaps with price outliers", "Filter: ITEM_QUANTITY <= 20"],
        ["826 rows from DEVELOPMENT app", "Test data from single test restaurant", "Exclude: APP_NAME = 'Retail Restaurant - DEVELOPMENT'"],
        ["17,808 rows (8.75%) with no USER_ID", "Cannot calculate per-customer metrics", "Exclude from CLV/RFM/churn; keep for aggregate analysis"],
        ["8 dirty ITEM_CATEGORY values", "Embedded URLs, typos, trailing digits", "Clean via mapping table (45 → 40 categories)"],
        ["Zero negative OPTION_PRICE values exist", "Cannot detect discounts as requirements suggest", "Pivot to upsell analysis (free vs paid add-ons)"],
        ["date_dim covers only 2023 (365 days)", "Order data spans Apr 2020 – Feb 2024 (1,402 days)", "Generate extended date_dim with US federal holidays"],
        ["OPTION_QUANTITY is always 1", "Zero variance — provides no analytical value", "Include in formula but do not analyse"],
        ["50% of customers have exactly 1 order", "Standard RFM quintiles will be skewed", "Separate one-time vs repeat for CLV tiers"],
    ],
    col_widths=[5.5, 4.5, 5]
)

add_note(
    "The discount analysis pivot is a critical deviation from the original requirements. The requirements "
    "document states: 'Use option_price < 0 to detect discounts.' After analysing all 193,017 rows in "
    "order_item_options, zero negative prices exist. The minimum OPTION_PRICE is $0.00, the maximum is $8.00, "
    "and there are only 16 distinct price points. All 133 option group names are food customisations "
    "(milk, bread, protein, toppings) — nothing discount or promo-related. Discounts were likely applied "
    "at a level not captured in this dataset. The replacement metric (upsell analysis) uses the same data "
    "to measure the revenue impact of paid vs free add-ons, which is actionable and answerable with the "
    "data we have.",
    label="SME Attention"
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 4. ARCHITECTURE OVERVIEW
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("4. Architecture Overview", level=1)

doc.add_paragraph(
    "The architecture follows a medallion pattern (Bronze → Silver → Gold) on Amazon S3 using Delta Lake "
    "format. All transformation logic runs in PySpark on AWS Glue. Orchestration, scheduling, and failure "
    "handling are managed by a single AWS Glue Workflow. The Gold layer is queried by Amazon Athena, which "
    "feeds a Streamlit dashboard."
)

doc.add_heading("4.1 High-Level Data Flow", level=2)
add_code(
    "┌─────────────────┐     JDBC/SSL      ┌──────────────┐              ┌──────────────┐              ┌──────────────┐\n"
    "│  RDS SQL Server  │ ───────────────>  │  S3 Bronze   │ ──────────> │  S3 Silver   │ ──────────> │  S3 Gold     │\n"
    "│  (Source)        │   Delta Merge     │  (Raw)       │  DQ Clean   │  (Conformed) │  Metrics    │  (Business)  │\n"
    "└─────────────────┘                    └──────────────┘              └──────────────┘              └──────┬───────┘\n"
    "                                                                                                         │\n"
    "                                                                                                         ▼\n"
    "                                                                                          ┌──────────────────────┐\n"
    "                                                                                          │  Athena → Streamlit  │\n"
    "                                                                                          └──────────────────────┘"
)

doc.add_heading("4.2 Architecture Diagram", level=2)
doc.add_paragraph(
    "The full visual diagram is in architecture_v3.drawio. Open it in draw.io (app.diagrams.net) for "
    "the interactive version. The diagram shows the data flow, Glue Workflow orchestration with failure "
    "triggers, the alert pipeline, and the security/encryption layer."
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 5. AWS SERVICES
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("5. AWS Services — What, Why, and Why Not the Alternatives", level=1)

doc.add_paragraph(
    "The architecture uses 7 AWS services. Each one was chosen because it is the simplest service that "
    "solves the specific problem, requires no external licenses, and avoids over-engineering. This section "
    "explains what each service does, why it was chosen, and why every considered alternative was rejected."
)

# Service 1: RDS
doc.add_heading("5.1 RDS SQL Server (Source Database)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["What it does", "Hosts the source transactional data (order_items, order_item_options, date_dim)"],
        ["Why this service", "The data is already in SQL Server on RDS. It is the existing source of truth. No migration or replication needed."],
        ["How Glue connects", "JDBC connection over SSL. Connection string stored in AWS Secrets Manager. Glue reads directly from RDS — no intermediate database."],
    ],
    col_widths=[3.5, 13]
)

add_note(
    "v1 of this design included AWS Aurora as an intermediate cloud database between SQL Server and S3. "
    "This was removed because: (1) SQL Server is already on RDS, which IS a cloud database — Aurora adds "
    "nothing, (2) Glue supports JDBC to SQL Server natively, (3) Aurora doubles the storage cost and adds "
    "a replication layer to maintain. The SME correctly identified this as unnecessary.",
    label="Why not Aurora?"
)

# Service 2: S3
doc.add_heading("5.2 Amazon S3 (Data Lake Storage)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["What it does", "Stores all three layers of the data lake: Bronze (raw), Silver (clean), Gold (metrics)"],
        ["Why this service", "S3 is the cheapest, most scalable storage on AWS. Native integration with Glue (PySpark reads/writes directly) and Athena (queries S3 in-place). Pay only for what you store."],
        ["Storage format", "Delta Lake on all layers. Not raw Parquet."],
        ["Bucket structure", "s3://<BUCKET_NAME>/{bronze|silver|gold}/{table_name}/"],
        ["Versioning", "Enabled. Previous versions retained 30 days for emergency rollback."],
        ["Lifecycle", "Transition Bronze data to S3 Glacier after 90 days (cost optimisation for raw data)."],
    ],
    col_widths=[3.5, 13]
)

add_note(
    "Why Delta Lake instead of plain Parquet? Four reasons: (1) ACID transactions — a failed write "
    "does not corrupt the table, (2) Delta merge (upsert) — only new/changed rows are written, not "
    "the entire table, which is how we handle incremental ingestion from RDS without a separate metadata "
    "tracker, (3) Time travel — can query or restore any previous version of the data for rollback or "
    "backfill, (4) Schema enforcement — rejects writes that don't match the expected schema, catching "
    "upstream changes early. Delta Lake is open-source (Apache 2.0 license) and runs natively on Glue "
    "PySpark. No external tool or license required.",
    label="Why Delta Lake?"
)

add_note(
    "v1 used Parquet + Glue Data Catalog. The Glue Data Catalog was needed because plain Parquet files "
    "have no built-in metadata — you need an external catalog to track schemas and partitions. Delta Lake "
    "replaces this: its _delta_log directory IS the catalog. It tracks schema, partition info, file lists, "
    "and version history. Athena can read Delta tables directly via a manifest file, so the Glue Data "
    "Catalog becomes redundant. Removing it simplifies the architecture and eliminates a service to maintain.",
    label="Why not Glue Data Catalog?"
)

# Service 3: Glue
doc.add_heading("5.3 AWS Glue — PySpark ETL Engine", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["What it does", "Runs all PySpark jobs: ingestion (Bronze), cleaning (Silver), and metric calculation (Gold)"],
        ["Why this service", "Serverless PySpark. No cluster to provision. Built-in JDBC connectors for SQL Server. Built-in Delta Lake support (Glue 4.0+). Pay per second of compute."],
        ["Job configuration", "Glue 4.0, PySpark, G.1X worker type (4 vCPU, 16 GB), 2 workers per job (sufficient for 54 MB dataset)"],
        ["Retry", "MaxRetries = 2 on each job definition. Glue retries automatically with backoff before reporting failure."],
    ],
    col_widths=[3.5, 13]
)

add_note(
    "Why not EMR? EMR requires provisioning and managing a Spark cluster (EC2 instances, scaling policies, "
    "security groups). For a 54 MB dataset with ~400K rows running once daily, EMR is massive overkill. "
    "Glue is serverless — zero infrastructure. If the dataset grows to TB scale, EMR becomes the right "
    "choice. At current scale, Glue is cheaper, simpler, and faster to develop on.",
    label="Why not EMR?"
)

add_note(
    "Why not Lambda? Lambda has a 15-minute timeout and 10 GB memory limit. PySpark jobs that join "
    "multiple tables, compute window functions for CLV/RFM, and write Delta format can exceed these "
    "limits. Lambda also cannot run PySpark natively — you would need a custom layer. The requirement "
    "states 'all logic in PySpark', which means Glue or EMR. Lambda is not an option.",
    label="Why not Lambda?"
)

# Service 4: Glue Workflow
doc.add_heading("5.4 AWS Glue Workflow (Orchestration & Scheduling)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["What it does", "Orchestrates the 3 Glue jobs + 1 alert job in a DAG. Handles scheduling (daily 2 AM UTC), job chaining (on success/on failure triggers), and parameterised runs."],
        ["Why this service", "Built into Glue — no additional service needed. Supports scheduled triggers (replaces EventBridge), conditional triggers (replaces Step Functions), and visual DAG in Console."],
        ["Trigger type", "SCHEDULED trigger: cron(0 2 * * ? *) — fires daily at 2:00 AM UTC"],
        ["Conditional triggers", "CONDITIONAL triggers with predicates: run next job on SUCCESS, run alert job on FAILED"],
        ["Parameterised runs", "Workflow run properties can pass --start-date and --end-date for backfill"],
    ],
    col_widths=[3.5, 13]
)

add_note(
    "v1 used Amazon EventBridge for scheduling and AWS Step Functions for orchestration. These are "
    "two additional services that duplicate what Glue Workflow already provides natively. "
    "EventBridge is useful when you need to trigger non-Glue targets (Lambda, SQS, etc.) from a schedule. "
    "We only trigger Glue jobs, so Glue Workflow's built-in scheduler is sufficient. "
    "Step Functions is useful for complex orchestration with branching, parallel execution, human approval "
    "steps, or cross-service workflows. Our pipeline is a simple 3-job chain. Glue Workflow handles this "
    "with less configuration, less cost, and fewer services to monitor. "
    "The SME correctly identified both as unnecessary for this use case.",
    label="Why not EventBridge + Step Functions?"
)

# Service 5: Athena
doc.add_heading("5.5 Amazon Athena (Query Engine)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["What it does", "Provides serverless SQL queries over the Gold layer in S3. The Streamlit dashboard connects to Athena via PyAthena."],
        ["Why this service", "Serverless — no database to manage. Queries S3 directly. Pay per query ($5/TB scanned). Our Gold layer is <10 MB — queries cost fractions of a cent."],
        ["Delta support", "Athena v3 reads Delta Lake tables natively via the Delta Lake manifest or through CREATE EXTERNAL TABLE with delta format."],
    ],
    col_widths=[3.5, 13]
)

add_note(
    "Why not Redshift? Redshift is a full data warehouse — provisioned clusters, nodes, concurrency scaling. "
    "Our Gold layer is under 10 MB with 14 tables. Redshift minimum cost is ~$180/month for a single dc2.large node. "
    "Athena for this workload costs <$1/month. Redshift is designed for TB/PB scale with concurrent users. We have "
    "one Streamlit dashboard. The cost difference is 180x for no benefit.",
    label="Why not Redshift?"
)

# Service 6: SNS
doc.add_heading("5.6 Amazon SNS (Failure Alerts)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["What it does", "Sends email notifications when a pipeline job fails. The Alert Job (Python shell) publishes to an SNS topic, which delivers to subscribed email addresses."],
        ["Why this service", "Simplest notification service on AWS. No server, no polling. Create topic, subscribe email, publish message. First 1,000 emails/month are free."],
        ["Topic", "pipeline-failure-alerts — subscribed by the data engineering team"],
    ],
    col_widths=[3.5, 13]
)

# Service 7: KMS
doc.add_heading("5.7 AWS KMS (Encryption Key Management)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["What it does", "Manages the encryption keys used by S3 (SSE-KMS). Provides automatic key rotation, usage audit via CloudTrail, and granular access control via key policies."],
        ["Why this service", "SSE-S3 (the default S3 encryption) uses Amazon-managed keys with no audit trail and no customer control over rotation. SSE-KMS gives us a CloudTrail record of every key usage, automatic annual rotation, and the ability to revoke access instantly by modifying the key policy. This is the production standard for regulated data."],
        ["Cost", "$1/month per key + $0.03 per 10,000 requests. For our workload: ~$1.10/month."],
    ],
    col_widths=[3.5, 13]
)

add_note(
    "v1 and v2 used SSE-S3 (the default). SSE-S3 encrypts data but with Amazon's own keys — you cannot "
    "audit who used the key, you cannot rotate on your own schedule, and you cannot revoke access without "
    "deleting the data. For a production pipeline handling customer transaction data, SSE-KMS is the correct "
    "choice. The incremental cost is negligible ($1.10/month).",
    label="Why not SSE-S3?"
)

doc.add_heading("5.8 Supporting Services (No Additional Cost/License)", level=2)
add_table(
    ["Service", "Role", "Why It's Here"],
    [
        ["AWS Secrets Manager", "Stores RDS credentials securely", "Never hardcode credentials. Supports auto-rotation. Glue reads creds at runtime via IAM role."],
        ["VPC Endpoints", "Private network path: Glue ↔ S3, Glue ↔ RDS", "Without VPC endpoints, Glue traffic to S3 traverses the public internet. VPC endpoints keep it on AWS's private backbone. Zero additional cost for Gateway endpoints (S3)."],
        ["IAM Roles", "Access control for each Glue job", "Each Glue job gets a dedicated IAM role with least-privilege permissions. Job 1 can read RDS + write Bronze. Job 2 can read Bronze + write Silver. Job 3 can read Silver + write Gold. No job has more access than it needs."],
        ["CloudWatch", "Logs and monitoring", "Every Glue job writes logs to CloudWatch automatically. Used for debugging failed runs and monitoring execution duration."],
        ["Delta Lake", "Data format on S3", "Open-source (Apache 2.0). Runs on Glue PySpark natively (Glue 4.0+). No license, no install, no external tool."],
    ],
    col_widths=[3, 3.5, 10]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 6. DATA FLOW — LAYER BY LAYER
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("6. Data Flow — Layer by Layer", level=1)

# Bronze
doc.add_heading("6.1 Bronze Layer (Raw Ingestion)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["Purpose", "Exact copy of source data in S3. No transformations. The raw audit trail."],
        ["Glue Job", "Job 1: INGEST"],
        ["Source", "RDS SQL Server (JDBC over SSL, credentials from Secrets Manager)"],
        ["Target", "s3://<BUCKET_NAME>/bronze/{table_name}/"],
        ["Format", "Delta Lake"],
        ["Partitioning", "ingestion_date (YYYY-MM-DD) — each daily run writes to its own partition"],
        ["Ingestion method", "Delta merge (upsert) on primary key. Reads source table, compares with existing Bronze data via Delta, writes only new/changed rows. This is how we handle incremental ingestion without a separate metadata tracker — Delta's _delta_log tracks what has been processed."],
        ["Tables written", "bronze.order_items, bronze.order_item_options, bronze.date_dim"],
        ["Processing", "try/except per table — if one table fails, the other two still ingest. Status manifest written to S3."],
    ],
    col_widths=[3, 13.5]
)

add_note(
    "Why Delta merge instead of full copy? A full copy rewrites the entire table every day — 203K rows of "
    "order_items even if only 100 new rows appeared. Delta merge compares on the primary key (LINEITEM_ID "
    "for order_items, composite key for options) and writes only the diff. This is faster, cheaper, and "
    "preserves history via Delta's versioning. For our current volume this saves seconds; at scale it "
    "saves hours.",
    label="Why merge, not full copy?"
)

# Silver
doc.add_heading("6.2 Silver Layer (Clean & Conformed)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["Purpose", "Validated, cleaned, business-ready data. All data quality rules applied."],
        ["Glue Job", "Job 2: CLEAN"],
        ["Source", "s3://<BUCKET_NAME>/bronze/"],
        ["Target", "s3://<BUCKET_NAME>/silver/{table_name}/"],
        ["Format", "Delta Lake"],
        ["Partitioning", "ingestion_date"],
        ["Tables written", "silver.order_items_clean, silver.order_item_options_clean, silver.date_dim_extended"],
        ["DQ Rules Applied", "See §13 for the complete rule set"],
        ["Validation", "Row count check: Silver count must be between 95% and 100% of Bronze count (after expected exclusions). If outside this range, the table is flagged in the manifest but still written (soft gate, not hard block)."],
        ["Processing", "try/except per table. Status manifest written to S3."],
    ],
    col_widths=[3, 13.5]
)

# Gold
doc.add_heading("6.3 Gold Layer (Business Metrics)", level=2)
add_table(
    ["Aspect", "Detail"],
    [
        ["Purpose", "Business-ready tables consumed by Athena and the Streamlit dashboard."],
        ["Glue Job", "Job 3: METRICS"],
        ["Source", "s3://<BUCKET_NAME>/silver/"],
        ["Target", "s3://<BUCKET_NAME>/gold/{table_name}/"],
        ["Format", "Delta Lake"],
        ["Build order", "Dimensions first (dim_customer, dim_restaurant, dim_date, dim_menu_item) → then fact_orders → then all 9 metric tables. Metrics depend on fact + dims, so the order matters."],
        ["Partitioning", "fact_orders: partitioned by order_year_month (YYYY-MM). Metric tables: no partition (all < 1 MB, full overwrite daily)."],
        ["Tables written", "4 dims + 1 fact + 9 metrics = 14 tables (see §11)"],
        ["Processing", "try/except per table. Dims are a prerequisite for fact, fact is a prerequisite for metrics. If dims fail, fact and metrics are skipped for that dim's downstream tables. Other independent metrics still attempt to build."],
    ],
    col_widths=[3, 13.5]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 7. ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("7. Orchestration — AWS Glue Workflow", level=1)

doc.add_paragraph(
    "All orchestration is handled by a single AWS Glue Workflow named retail-daily-pipeline. "
    "No external orchestrator (Step Functions, EventBridge, Airflow) is needed."
)

doc.add_heading("7.1 Workflow DAG", level=2)
add_code(
    "retail-daily-pipeline\n"
    "│\n"
    "├── Scheduled Trigger: cron(0 2 * * ? *)  [Daily 2:00 AM UTC]\n"
    "│\n"
    "├── Glue Job 1: INGEST (Bronze)           [MaxRetries=2]\n"
    "│   ├── on SUCCESS → Conditional Trigger → Glue Job 2\n"
    "│   └── on FAILURE → Conditional Trigger → Alert Job\n"
    "│\n"
    "├── Glue Job 2: CLEAN (Silver)            [MaxRetries=2]\n"
    "│   ├── on SUCCESS → Conditional Trigger → Glue Job 3\n"
    "│   └── on FAILURE → Conditional Trigger → Alert Job\n"
    "│\n"
    "├── Glue Job 3: METRICS (Gold)            [MaxRetries=2]\n"
    "│   ├── on SUCCESS → [DONE]\n"
    "│   └── on FAILURE → Conditional Trigger → Alert Job\n"
    "│\n"
    "└── Alert Job (Python Shell)              [on failure of ANY job]\n"
    "    └── Publishes to SNS → Email notification"
)

doc.add_heading("7.2 Why 3 Jobs, Not 15", level=2)

doc.add_paragraph(
    "A common question is: why not split each table into its own Glue job for maximum isolation? "
    "With 3 Bronze tables + 3 Silver tables + 14 Gold tables = 20 potential jobs. Here is why 3 is correct:"
)

add_table(
    ["Factor", "3 Jobs (per layer)", "20 Jobs (per table)"],
    [
        ["Cold start overhead", "~90 seconds total (3 × 30s)", "~600+ seconds total (20 × 30s) = 10 minutes of pure startup on a 54 MB dataset"],
        ["DPU billing", "3 job minimums", "20 job minimums (each job bills for at least 1 minute of DPU even if it runs for 5 seconds)"],
        ["Workflow complexity", "3-node DAG + 1 alert job", "20-node DAG with complex dependency wiring"],
        ["Debugging", "3 CloudWatch log groups", "20 CloudWatch log groups to search through"],
        ["Failure isolation", "try/except per table within each job — same result", "Native per-job isolation but at 7x the cost and complexity"],
        ["AWS best practice", "Yes — per-layer grouping unless tables have different SLAs or resource needs", "Only when individual tables are very large or have unique requirements"],
    ],
    col_widths=[3.5, 6, 7]
)

doc.add_paragraph(
    "The failure isolation concern is real but solved within each job via try/except blocks per table. "
    "If order_items cleaning fails inside Job 2, order_item_options and date_dim still get cleaned. "
    "The job writes a status manifest to S3 documenting which tables succeeded and which failed. "
    "Downstream Job 3 reads this manifest and only builds metrics for tables with fresh Silver data."
)

doc.add_heading("7.3 Scheduling Details", level=2)
add_table(
    ["Parameter", "Value", "Justification"],
    [
        ["Frequency", "Daily", "Requirement: 'schedule the pipeline once daily as a batch process'"],
        ["Time", "2:00 AM UTC", "Off-peak hours. Source system has lowest load. Dashboard users are not active."],
        ["Timezone", "UTC", "Avoids daylight saving time ambiguity. All AWS services use UTC internally."],
        ["Estimated runtime", "5-8 minutes", "3 jobs × ~2 min each (including cold start). 54 MB total data."],
        ["Dashboard freshness", "Data available by ~2:10 AM UTC", "Dashboard queries Gold layer — updated as soon as Job 3 completes."],
    ],
    col_widths=[3, 3.5, 10]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 8. FAILURE HANDLING & RELOAD
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("8. Failure Handling & Reload Mechanism", level=1)

doc.add_paragraph(
    "The requirement explicitly calls for a 'failure reload mechanism.' This section defines exactly "
    "what happens when things go wrong — at every level of granularity."
)

doc.add_heading("8.1 Automatic Retry", level=2)
add_table(
    ["Parameter", "Value", "Behaviour"],
    [
        ["MaxRetries", "2 per job", "If a job fails, Glue automatically retries up to 2 more times before marking it as FAILED"],
        ["Backoff", "Built-in (Glue-managed)", "Glue waits between retries. The wait period is managed by the service."],
        ["Retry scope", "Entire job restarts", "The job re-executes from the beginning. Delta merge ensures idempotency — reprocessing the same data produces the same result with no duplicates."],
        ["Total attempts", "3 (1 original + 2 retries)", "After 3 failures, the job is marked FAILED and the alert job triggers."],
    ],
    col_widths=[3, 3.5, 10]
)

doc.add_heading("8.2 Partial Failure (Table-Level Isolation)", level=2)

doc.add_paragraph("Inside each Glue job, every table is processed in a try/except block:")

add_code(
    "# Inside each Glue job\n"
    "results = {}\n"
    "for table in tables_to_process:\n"
    "    try:\n"
    "        process(table)\n"
    "        results[table] = 'SUCCESS'\n"
    "    except Exception as e:\n"
    "        results[table] = f'FAILED: {str(e)}'\n"
    "        logger.error(f'{table} failed: {e}')\n"
    "\n"
    "# Write manifest to S3\n"
    "write_json(results, f's3://bucket/manifests/{job_name}/{run_date}.json')\n"
    "\n"
    "# Job-level outcome:\n"
    "# - If ALL tables failed: raise exception → Glue marks job as FAILED → alert fires\n"
    "# - If SOME tables failed: job completes with WARNING → downstream reads manifest\n"
    "# - If NO tables failed: job completes SUCCESS"
)

doc.add_paragraph(
    "This means: if order_items ingestion fails due to a network timeout, order_item_options and date_dim "
    "still ingest successfully. The cleaning job reads the manifest and only cleans the tables that have "
    "fresh Bronze data. Metrics that depend on order_items are skipped; metrics that only need date_dim "
    "(like dim_date) still build."
)

doc.add_heading("8.3 Manual Rerun", level=2)
doc.add_paragraph(
    "Any pipeline run can be manually triggered via the AWS Glue Console or AWS CLI. Because the pipeline "
    "uses Delta merge (upsert on primary key), re-running the same day produces exactly the same result "
    "with no duplicates. This is idempotent by design."
)
add_code("aws glue start-workflow-run --name retail-daily-pipeline")

doc.add_heading("8.4 Backfill (Historical Reload)", level=2)
doc.add_paragraph(
    "To reload a date range (e.g., re-process all of January 2024), pass parameters to the workflow run:"
)
add_code(
    'aws glue start-workflow-run \\\n'
    '  --name retail-daily-pipeline \\\n'
    '  --run-properties \'{"--start_date": "2024-01-01", "--end_date": "2024-01-31"}\''
)
doc.add_paragraph(
    "Each Glue job reads these parameters and filters its source query accordingly. Delta merge "
    "ensures that the backfilled data upserts cleanly without duplicating existing rows."
)

doc.add_heading("8.5 Rollback (Undo a Bad Run)", level=2)
doc.add_paragraph(
    "Delta Lake supports time travel — every write creates a new version. To roll back a table to "
    "its previous state:"
)
add_code(
    "# In PySpark (or an ad-hoc Glue job):\n"
    "from delta.tables import DeltaTable\n"
    "\n"
    "dt = DeltaTable.forPath(spark, 's3://<BUCKET_NAME>/gold/fact_orders/')\n"
    "dt.restoreToVersion(previous_version)  # instant rollback, no reprocessing"
)
doc.add_paragraph(
    "Delta retains 30 days of version history. Combined with S3 Versioning (also enabled), "
    "we have two independent rollback mechanisms."
)

doc.add_heading("8.6 Failure Scenarios Matrix", level=2)
add_table(
    ["Scenario", "What Happens", "Recovery Action"],
    [
        ["Network timeout to RDS", "Job 1 fails → auto-retry (2x) → if still failing, alert email sent",
         "Check RDS status, Security Group rules, then manually rerun workflow"],
        ["Schema change in source", "Delta merge detects schema mismatch → job fails → alert email sent",
         "Update Glue job to handle new schema, then rerun. Delta schema enforcement catches this immediately."],
        ["One table fails in Job 2", "Other tables still clean successfully. Manifest logs the failure. Job 2 completes with WARNING.",
         "Fix the root cause, then rerun. Delta merge re-processes only the failed table."],
        ["Job 3 writes bad data to Gold", "Dashboard shows incorrect numbers",
         "Rollback via Delta time travel (dt.restoreToVersion). Instant, no reprocessing."],
        ["Entire pipeline fails", "All 3 retries exhausted → alert email with workflow run ID and link to CloudWatch logs",
         "Check CloudWatch logs for stack trace, fix root cause, manually trigger workflow."],
        ["Need to re-process last 3 months", "N/A — planned backfill, not a failure",
         "Trigger workflow with --start_date and --end_date parameters. Delta merge handles idempotent reload."],
    ],
    col_widths=[3, 6.5, 6.5]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 9. FAILURE ALERT PIPELINE
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("9. Failure Alert Pipeline", level=1)

doc.add_paragraph(
    "This was a gap in v2 — the alert infrastructure existed as boxes on the diagram but had no flow "
    "showing how failures actually trigger notifications. v3 fixes this with a dedicated Alert Job "
    "inside the Glue Workflow."
)

doc.add_heading("9.1 Alert Flow", level=2)
add_code(
    "Glue Job 1, 2, or 3 FAILS (after all retries exhausted)\n"
    "    │\n"
    "    ▼\n"
    "Glue Workflow Conditional Trigger (predicate: state = FAILED)\n"
    "    │\n"
    "    ▼\n"
    "Alert Job (Python Shell — no Spark, no DPU cost)\n"
    "    │  - Reads workflow name and run ID from Glue context\n"
    "    │  - Reads status manifests from S3 to identify which tables failed\n"
    "    │  - Constructs alert message with: which job, which tables, run ID\n"
    "    │\n"
    "    ▼\n"
    "SNS Publish → Topic: pipeline-failure-alerts\n"
    "    │\n"
    "    ▼\n"
    "Email delivered to subscribed team members\n"
    "    Subject: 'PIPELINE FAILED: retail-daily-pipeline'\n"
    "    Body: failed job name, failed tables, run ID, CloudWatch log link"
)

doc.add_heading("9.2 Why a Python Shell Job (Not EventBridge)?", level=2)
doc.add_paragraph(
    "The SME feedback on v1 was clear: do not use EventBridge. The Alert Job is a Python Shell Glue job — "
    "it runs inside the same Glue Workflow, uses zero Spark DPUs (Python Shell jobs use 1/16 DPU = "
    "minimal cost), and calls SNS via boto3. This keeps the entire alert pipeline within Glue, "
    "requiring no additional AWS services."
)

doc.add_heading("9.3 Alert Job Code", level=2)
add_code(
    "import boto3, json, sys\n"
    "from awsglue.utils import getResolvedOptions\n"
    "\n"
    "args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])\n"
    "\n"
    "# Read manifest to find which tables failed\n"
    "s3 = boto3.client('s3')\n"
    "# ... read manifests from s3://bucket/manifests/ ...\n"
    "\n"
    "sns = boto3.client('sns')\n"
    "sns.publish(\n"
    "    TopicArn='arn:aws:sns:us-east-1:ACCOUNT:pipeline-failure-alerts',\n"
    "    Subject=f'PIPELINE FAILED: {args[\"WORKFLOW_NAME\"]}',\n"
    "    Message=json.dumps({\n"
    "        'workflow': args['WORKFLOW_NAME'],\n"
    "        'run_id': args['WORKFLOW_RUN_ID'],\n"
    "        'failed_tables': failed_tables,  # from manifest\n"
    "        'action': 'Check CloudWatch Logs for stack trace'\n"
    "    }, indent=2)\n"
    ")"
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 10. ENCRYPTION & SECURITY
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("10. Encryption & Security", level=1)

doc.add_paragraph(
    "The requirement calls for encryption as part of a production-level pipeline. This section "
    "defines encryption at every layer and every transit path."
)

doc.add_heading("10.1 Encryption Matrix", level=2)
add_table(
    ["Layer", "Mechanism", "Detail"],
    [
        ["S3 (at rest)", "SSE-KMS", "Customer-managed KMS key. Auto-rotation every 365 days. Every key usage logged in CloudTrail. S3 bucket policy enforces: deny any PutObject without SSE-KMS header."],
        ["RDS (at rest)", "AES-256", "Managed by RDS. Enabled at instance creation. Transparent to applications."],
        ["JDBC (in transit)", "TLS/SSL", "Connection string: jdbc:sqlserver://host:1433;encrypt=true;trustServerCertificate=false. Glue job enforces encrypted connections."],
        ["S3 (in transit)", "HTTPS (TLS 1.2)", "S3 bucket policy: deny any request where aws:SecureTransport = false. All Glue ↔ S3 traffic uses HTTPS."],
        ["Credentials", "Secrets Manager", "RDS username/password stored in Secrets Manager. Auto-rotation enabled. Glue jobs retrieve credentials at runtime via IAM role — never hardcoded in scripts."],
        ["Logs", "CloudWatch encryption", "CloudWatch log groups encrypted with service-managed keys by default."],
        ["Network", "VPC Endpoints", "S3 Gateway Endpoint: Glue ↔ S3 traffic stays on AWS private network, never touches the public internet. RDS is in a private subnet accessible only from Glue's VPC."],
    ],
    col_widths=[2.5, 3, 11]
)

doc.add_heading("10.2 IAM — Least-Privilege Access", level=2)
add_table(
    ["IAM Role", "Permissions"],
    [
        ["glue-job-ingest-role", "s3:PutObject on bronze/*, s3:GetObject on bronze/* (for Delta merge comparison), secretsmanager:GetSecretValue, rds-data:ExecuteStatement"],
        ["glue-job-clean-role", "s3:GetObject on bronze/*, s3:PutObject on silver/*, s3:GetObject on silver/*"],
        ["glue-job-metrics-role", "s3:GetObject on silver/*, s3:PutObject on gold/*, s3:GetObject on gold/*"],
        ["glue-job-alert-role", "sns:Publish on pipeline-failure-alerts topic, s3:GetObject on manifests/*"],
    ],
    col_widths=[4, 12.5]
)

doc.add_paragraph(
    "Each Glue job has its own IAM role. No job has access to more data than it needs. "
    "The ingest job cannot read Gold data. The metrics job cannot write to Bronze. "
    "The alert job can only publish to SNS and read manifests."
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 11. DATA MODEL — GOLD LAYER
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("11. Data Model — Gold Layer", level=1)

doc.add_paragraph(
    "The Gold layer uses a star schema: 4 dimension tables, 1 fact table, and 9 metric tables. "
    "The star schema was chosen because it is the standard for analytical workloads — Athena "
    "and dashboard tools are optimised for star-schema queries (filter on dims, aggregate on facts)."
)

doc.add_heading("11.1 Star Schema Diagram", level=2)
add_code(
    "                    ┌──────────────┐\n"
    "                    │  dim_date    │\n"
    "                    │  (PK: date_  │\n"
    "                    │   key)       │\n"
    "                    └──────┬───────┘\n"
    "                           │\n"
    "┌──────────────┐   ┌──────┴───────┐   ┌──────────────┐\n"
    "│ dim_customer │───│ fact_orders  │───│dim_restaurant│\n"
    "│ (PK: user_id)│   │              │   │(PK: rest_id) │\n"
    "└──────────────┘   │ (PK: order_  │   └──────────────┘\n"
    "                    │  id)         │\n"
    "┌──────────────┐   │              │\n"
    "│dim_menu_item │───│              │\n"
    "│(PK: item_key)│   └──────────────┘\n"
    "└──────────────┘"
)

doc.add_heading("11.2 Dimension Tables", level=2)

# dim_customer
doc.add_heading("dim_customer", level=3)
add_table(
    ["Column", "Type", "Description"],
    [
        ["user_id", "STRING", "Primary key. MongoDB ObjectId from source."],
        ["first_order_date", "DATE", "Date of customer's first order"],
        ["last_order_date", "DATE", "Date of customer's most recent order"],
        ["total_orders", "INT", "Lifetime count of orders"],
        ["total_spend", "DECIMAL(10,2)", "Lifetime revenue (item + option revenue)"],
        ["avg_order_value", "DECIMAL(10,2)", "total_spend / total_orders"],
        ["is_loyalty", "BOOLEAN", "TRUE if customer has ever used loyalty card"],
        ["clv_tier", "STRING", "High (top 20%), Medium (mid 60%), Low (bottom 20%)"],
        ["customer_tenure_days", "INT", "Days between first order and last order"],
    ],
    col_widths=[4, 3, 9.5]
)

# dim_restaurant
doc.add_heading("dim_restaurant", level=3)
add_table(
    ["Column", "Type", "Description"],
    [
        ["restaurant_id", "STRING", "Primary key. 27 locations (dev restaurant excluded)."],
        ["total_orders", "INT", "Lifetime count of orders at this location"],
        ["total_revenue", "DECIMAL(12,2)", "Lifetime revenue at this location"],
        ["first_order_date", "DATE", "Earliest order at this location"],
        ["last_order_date", "DATE", "Most recent order at this location"],
    ],
    col_widths=[4, 3, 9.5]
)

# dim_date
doc.add_heading("dim_date", level=3)
add_table(
    ["Column", "Type", "Description"],
    [
        ["date_key", "DATE", "Primary key. YYYY-MM-DD format (standardised from DD-MM-YYYY source)."],
        ["year", "INT", "Calendar year"],
        ["month", "INT", "Month number (1-12)"],
        ["month_name", "STRING", "Full month name (January, February, etc.)"],
        ["week", "INT", "ISO week number"],
        ["day_of_week", "STRING", "Full day name (Monday, Tuesday, etc.)"],
        ["day_of_week_num", "INT", "1=Monday through 7=Sunday"],
        ["is_weekend", "BOOLEAN", "TRUE for Saturday and Sunday"],
        ["is_holiday", "BOOLEAN", "TRUE for US federal holidays"],
        ["holiday_name", "STRING", "Holiday name (NULL for non-holidays)"],
        ["quarter", "INT", "Calendar quarter (1-4)"],
    ],
    col_widths=[4, 3, 9.5]
)
doc.add_paragraph(
    "Extended from the original 365-row (2023-only) date_dim to cover the full data range: "
    "April 21, 2020 through February 21, 2024 (~1,402 rows). US federal holidays are populated "
    "for all years."
)

# dim_menu_item
doc.add_heading("dim_menu_item", level=3)
add_table(
    ["Column", "Type", "Description"],
    [
        ["item_key", "STRING", "Surrogate key (hash of item_name + item_category)"],
        ["item_name", "STRING", "Menu item name from source"],
        ["item_category", "STRING", "Cleaned category (40 categories after DQ rules)"],
        ["avg_price", "DECIMAL(10,2)", "Average unit price across all orders"],
        ["total_quantity_sold", "INT", "Lifetime units sold"],
    ],
    col_widths=[4, 3, 9.5]
)

doc.add_heading("11.3 Fact Table", level=2)
doc.add_heading("fact_orders", level=3)
doc.add_paragraph("Grain: one row per ORDER_ID.")
add_table(
    ["Column", "Type", "Description"],
    [
        ["order_id", "STRING", "Primary key. Unique order identifier."],
        ["user_id", "STRING", "FK → dim_customer. NULL for anonymous orders."],
        ["restaurant_id", "STRING", "FK → dim_restaurant"],
        ["order_date", "DATE", "FK → dim_date (date part of CREATION_TIME_UTC)"],
        ["order_hour_utc", "INT", "Hour of order (0-23 UTC)"],
        ["hour_bucket", "STRING", "Morning (6-11), Afternoon (12-17), Evening (18-23), Night (0-5)"],
        ["item_count", "INT", "Number of line items in this order"],
        ["total_item_revenue", "DECIMAL(10,2)", "SUM(ITEM_PRICE × ITEM_QUANTITY) for this order"],
        ["total_option_revenue", "DECIMAL(10,2)", "SUM(OPTION_PRICE × OPTION_QUANTITY) for this order's options"],
        ["total_revenue", "DECIMAL(10,2)", "total_item_revenue + total_option_revenue"],
        ["has_paid_options", "BOOLEAN", "TRUE if any option has OPTION_PRICE > 0"],
        ["paid_option_count", "INT", "Count of paid add-ons (OPTION_PRICE > 0)"],
        ["is_loyalty", "BOOLEAN", "TRUE if IS_LOYALTY = TRUE on any line item"],
        ["app_name", "STRING", "Ordering platform (Retail Restaurant, Neighborhood Perks)"],
    ],
    col_widths=[4, 3, 9.5]
)

doc.add_paragraph(
    "Revenue formula: total_item_revenue = SUM(ITEM_PRICE × ITEM_QUANTITY) across all line items in "
    "the order. total_option_revenue = SUM(OPTION_PRICE × OPTION_QUANTITY) across all options linked "
    "to those line items. total_revenue = total_item_revenue + total_option_revenue."
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 12. BUSINESS METRICS
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("12. Business Metrics — Calculation Logic", level=1)

doc.add_paragraph(
    "This section defines the exact calculation logic for each metric table. Every formula, threshold, "
    "and grouping rule is specified so that the PySpark implementation can be written directly from this "
    "document with no ambiguity."
)

# 12.1 CLV
doc.add_heading("12.1 Customer Lifetime Value (PRIMARY METRIC)", level=2)

doc.add_heading("Table: gold_clv_snapshot", level=3)
doc.add_paragraph(
    "The primary business requirement is: 'Calculate CLV and show how LTV evolves for each customer daily.' "
    "This means we need one row per customer per calendar day — not just on days they ordered, but every "
    "day — showing their cumulative CLV as of that date."
)

add_table(
    ["Column", "Type", "Description"],
    [
        ["user_id", "STRING", "FK → dim_customer"],
        ["snapshot_date", "DATE", "Calendar date (every day from customer's first order to pipeline run date)"],
        ["cumulative_orders", "INT", "Total orders placed by this customer up to and including this date"],
        ["cumulative_spend", "DECIMAL(10,2)", "Total revenue from this customer up to and including this date"],
        ["clv_tier", "STRING", "High / Medium / Low (recalculated daily based on current percentile position)"],
        ["days_as_customer", "INT", "Days since first order"],
        ["is_active", "BOOLEAN", "TRUE if customer has ordered in the last 90 days relative to snapshot_date"],
    ],
    col_widths=[3.5, 3, 10]
)

doc.add_heading("CLV Calculation Logic", level=4)
add_code(
    "Step 1: Calculate cumulative spend per customer per order date\n"
    "  - For each customer, running SUM of total_revenue from fact_orders\n"
    "    ordered by order_date\n"
    "  - Window function: SUM(total_revenue) OVER (PARTITION BY user_id ORDER BY order_date)\n"
    "\n"
    "Step 2: Expand to every calendar day\n"
    "  - Cross-join each customer with dim_date (from first_order_date to current_date)\n"
    "  - Forward-fill the cumulative_spend: on non-order days, carry forward the\n"
    "    last known cumulative value using last() OVER (PARTITION BY user_id ORDER BY date\n"
    "    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) with IGNORE NULLS\n"
    "\n"
    "Step 3: Assign CLV tiers (recalculated daily)\n"
    "  - For each snapshot_date, rank all customers by cumulative_spend\n"
    "  - Top 20% = High, Middle 60% = Medium, Bottom 20% = Low\n"
    "  - Window function: PERCENT_RANK() OVER (PARTITION BY snapshot_date ORDER BY cumulative_spend)"
)

add_note(
    "Why cross-join with dim_date instead of only keeping order dates? Because the requirement says "
    "'show how LTV evolves daily.' If a customer ordered on Jan 1 and Jan 15, we need rows for Jan 2-14 "
    "showing the same CLV (no change). Without these rows, a time-series chart in the dashboard would "
    "show gaps or straight lines jumping between data points. With the cross-join, the dashboard can "
    "plot a smooth daily evolution curve for any customer. v2's design only had rows on order dates — "
    "that was a gap.",
    label="Why this approach?"
)

add_note(
    "For 20,174 customers × ~1,402 days = ~28.3 million rows. At ~50 bytes per row, this is ~1.4 GB. "
    "Delta Lake with Parquet columnar compression will reduce this to ~100-200 MB on S3. Athena can "
    "query this efficiently with partition pruning on snapshot_date.",
    label="Scale consideration"
)

# 12.2 RFM
doc.add_heading("12.2 RFM Segmentation", level=2)
doc.add_heading("Table: gold_rfm_segments", level=3)
doc.add_paragraph("One row per customer. Recalculated daily.")

add_table(
    ["Column", "Type", "Description"],
    [
        ["user_id", "STRING", "FK → dim_customer"],
        ["last_order_date", "DATE", "Date of most recent order"],
        ["recency_days", "INT", "Days since last order (relative to pipeline run date)"],
        ["frequency", "INT", "Total number of orders (lifetime)"],
        ["monetary", "DECIMAL(10,2)", "Total spend (lifetime)"],
        ["r_score", "INT", "Recency score (1-5, where 5 = most recent)"],
        ["f_score", "INT", "Frequency score (1-5, where 5 = most frequent)"],
        ["m_score", "INT", "Monetary score (1-5, where 5 = highest spend)"],
        ["rfm_segment", "STRING", "VIP / Loyal / New Customer / Promising / At Risk / Lost"],
    ],
    col_widths=[3.5, 3, 10]
)

doc.add_heading("RFM Scoring Logic", level=4)
add_code(
    "Scoring: Quintile-based (NTILE(5)) for each dimension\n"
    "  R score: NTILE(5) OVER (ORDER BY recency_days DESC)  -- lower recency = higher score\n"
    "  F score: NTILE(5) OVER (ORDER BY frequency ASC)\n"
    "  M score: NTILE(5) OVER (ORDER BY monetary ASC)\n"
    "\n"
    "Segmentation rules:\n"
    "  VIP:           R >= 4 AND F >= 4 AND M >= 4\n"
    "  Loyal:         F >= 4 AND M >= 3\n"
    "  New Customer:  R >= 4 AND F <= 2\n"
    "  Promising:     R >= 3 AND F >= 2 AND M >= 2\n"
    "  At Risk:       R <= 2 AND F >= 3  (were frequent, now gone quiet)\n"
    "  Lost:          R <= 2 AND F <= 2 AND M <= 2\n"
    "  Regular:       everything else"
)

add_note(
    "50% of customers have exactly 1 order (frequency = 1). Standard quintiles will put the majority "
    "in the lowest F bucket. This is expected behaviour — it reflects the reality of the business. "
    "The segmentation rules account for this by not requiring high F for 'New Customer' (which correctly "
    "captures recent first-time buyers).",
    label="Handling the 50% one-time buyer skew"
)

# 12.3 Churn
doc.add_heading("12.3 Churn Indicators", level=2)
doc.add_heading("Table: gold_churn_indicators", level=3)
doc.add_paragraph("One row per customer. Recalculated daily.")

add_table(
    ["Column", "Type", "Description"],
    [
        ["user_id", "STRING", "FK → dim_customer"],
        ["total_orders", "INT", "Lifetime order count"],
        ["days_since_last_order", "INT", "Recency (days since most recent order)"],
        ["avg_days_between_orders", "DECIMAL(10,2)", "Average gap between consecutive orders (NULL for single-order customers)"],
        ["spend_trend_pct", "DECIMAL(10,2)", "% change in spend: (last 90 days spend - prior 90 days spend) / prior 90 days spend × 100"],
        ["churn_tag", "STRING", "Active / Cooling Off / At Risk / Inactive"],
    ],
    col_widths=[4, 3, 9.5]
)

doc.add_heading("Churn Tagging Logic", level=4)
add_code(
    "Active:      days_since_last_order <= 30\n"
    "Cooling Off: days_since_last_order BETWEEN 31 AND 45\n"
    "At Risk:     days_since_last_order BETWEEN 46 AND 90\n"
    "Inactive:    days_since_last_order > 90\n"
    "\n"
    "Note: These thresholds are configurable. The 45-day 'At Risk' threshold was\n"
    "chosen based on the data: the median gap between repeat orders is ~28 days.\n"
    "45 days represents approximately 1.5x the median gap."
)

add_note(
    "The requirement says 'Build a customer activity profile to help marketing identify at-risk "
    "customers (no predictions).' This is explicitly NOT a churn prediction model — it is a "
    "threshold-based tagging system. Marketing uses the tags to filter customer lists and trigger "
    "re-engagement campaigns. No ML involved.",
    label="Not a prediction model"
)

# 12.4 Sales Trends
doc.add_heading("12.4 Sales Trends", level=2)

for grain, tbl in [("Daily", "gold_sales_daily"), ("Weekly", "gold_sales_weekly"), ("Monthly", "gold_sales_monthly")]:
    doc.add_heading(f"Table: {tbl}", level=3)

cols = [
    ["date / week / month", "DATE/INT", "Time grain"],
    ["restaurant_id", "STRING", "FK → dim_restaurant"],
    ["item_category", "STRING", "Cleaned menu category"],
    ["hour_bucket", "STRING", "Morning / Afternoon / Evening / Night (daily only)"],
    ["total_revenue", "DECIMAL(12,2)", "Sum of total_revenue from fact_orders"],
    ["order_count", "INT", "Count of orders"],
    ["avg_order_value", "DECIMAL(10,2)", "total_revenue / order_count"],
    ["unique_customers", "INT", "Count of distinct user_ids (excluding anonymous)"],
]
add_table(["Column", "Type", "Description"], cols, col_widths=[4, 3, 9.5])

doc.add_paragraph(
    "All three tables are aggregations of fact_orders joined with dim_date, dim_restaurant, "
    "and dim_menu_item. Weekly and monthly tables aggregate from fact_orders directly (not from daily) "
    "to avoid rounding errors from pre-aggregation."
)

# 12.5 Loyalty
doc.add_heading("12.5 Loyalty Program Impact", level=2)
doc.add_heading("Table: gold_loyalty_comparison", level=3)
doc.add_paragraph("Two rows: one for loyalty members, one for non-members.")

add_table(
    ["Column", "Type", "Description"],
    [
        ["is_loyalty", "BOOLEAN", "TRUE = loyalty members, FALSE = non-members"],
        ["customer_count", "INT", "Number of unique customers in this group"],
        ["total_orders", "INT", "Total orders placed by this group"],
        ["total_revenue", "DECIMAL(12,2)", "Total revenue from this group"],
        ["avg_spend_per_customer", "DECIMAL(10,2)", "total_revenue / customer_count"],
        ["avg_order_value", "DECIMAL(10,2)", "total_revenue / total_orders"],
        ["repeat_order_rate", "DECIMAL(5,2)", "% of customers with more than 1 order"],
        ["avg_clv", "DECIMAL(10,2)", "Average CLV across customers in this group"],
        ["avg_orders_per_customer", "DECIMAL(10,2)", "total_orders / customer_count"],
    ],
    col_widths=[4, 3, 9.5]
)

# 12.6 Location
doc.add_heading("12.6 Location Performance", level=2)
doc.add_heading("Table: gold_location_perf", level=3)
doc.add_paragraph("One row per restaurant (27 rows). Ranked by revenue.")

add_table(
    ["Column", "Type", "Description"],
    [
        ["restaurant_id", "STRING", "FK → dim_restaurant"],
        ["total_revenue", "DECIMAL(12,2)", "Lifetime revenue"],
        ["avg_order_value", "DECIMAL(10,2)", "Average revenue per order"],
        ["total_orders", "INT", "Lifetime order count"],
        ["orders_per_week", "DECIMAL(10,2)", "Total orders / weeks active"],
        ["unique_customers", "INT", "Distinct customer count"],
        ["repeat_customer_rate", "DECIMAL(5,2)", "% of customers with 2+ orders at this location"],
        ["revenue_rank", "INT", "1 = highest revenue, 27 = lowest"],
    ],
    col_widths=[4, 3, 9.5]
)

# 12.7 Upsell
doc.add_heading("12.7 Pricing & Upsell Analysis", level=2)

add_note(
    "The original requirement specifies 'Pricing & Discount Effectiveness' using option_price < 0 to "
    "detect discounts. After thorough analysis of all 193,017 rows in order_item_options, zero negative "
    "prices exist. The price range is $0.00 to $8.00 with exactly 16 distinct values. 66.3% of options "
    "are free ($0) and 33.7% are paid add-ons. All 133 option group names are food customisations — "
    "nothing discount-related. Discounts were likely applied at a transaction level not captured in this "
    "dataset. This metric has been replaced with Upsell Analysis, which measures the revenue impact of "
    "paid add-ons vs free options — an actionable insight from the data we actually have.",
    label="DEVIATION FROM REQUIREMENTS"
)

doc.add_heading("Table: gold_upsell_analysis", level=3)

add_table(
    ["Column", "Type", "Description"],
    [
        ["has_paid_options", "BOOLEAN", "TRUE = order includes at least one paid add-on"],
        ["order_count", "INT", "Number of orders in this group"],
        ["total_revenue", "DECIMAL(12,2)", "Total order revenue (items + options)"],
        ["avg_order_value", "DECIMAL(10,2)", "Average revenue per order"],
        ["total_option_revenue", "DECIMAL(10,2)", "Revenue from add-ons only"],
        ["option_revenue_pct", "DECIMAL(5,2)", "option_revenue / total_revenue × 100"],
        ["avg_paid_options_per_order", "DECIMAL(10,2)", "Average count of paid add-ons per order (for paid group only)"],
    ],
    col_widths=[4, 3, 9.5]
)

doc.add_paragraph(
    "Additionally, a supplementary table gold_top_paid_options lists the top 20 paid option names "
    "by frequency, with their revenue contribution. This supports the dashboard's pricing view."
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 13. DATA QUALITY RULES
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("13. Data Quality Rules (Silver Layer)", level=1)

doc.add_paragraph(
    "These rules are applied in Glue Job 2 (CLEAN) when transforming Bronze to Silver. "
    "Every rule was defined based on findings from Step 1 (Data Exploration) and Step 2 (Data Analysis)."
)

add_table(
    ["#", "Rule", "Logic", "Rows Affected", "Rationale"],
    [
        ["DQ-01", "Remove price outliers", "ITEM_PRICE <= 50", "590 (0.29%)", "Prices up to $5,000 on items that normally cost $8-15. Data entry errors. <1% of rows distort 79% of revenue."],
        ["DQ-02", "Remove quantity outliers", "ITEM_QUANTITY <= 20", "84 (0.04%)", "Quantities up to 500. No restaurant serves 500 of one item in a single order."],
        ["DQ-03", "Exclude DEVELOPMENT app", "APP_NAME != 'Retail Restaurant - DEVELOPMENT'", "826 (0.41%)", "Test data from a single test restaurant. Not real transactions."],
        ["DQ-04", "Exclude Test Items category", "ITEM_CATEGORY != 'Test Items'", "1 (0.00%)", "Single test row."],
        ["DQ-05", "Exclude zero-quantity row", "ITEM_QUANTITY > 0", "1 (0.00%)", "One row with qty=0, empty name/category. Contributes nothing."],
        ["DQ-06", "Clean dirty categories", "Mapping table (see below)", "~1,347 (0.66%)", "Embedded URLs, typos, trailing digits. Maps 45 dirty values to 40 clean values."],
        ["DQ-07", "Standardise dates", "Convert DD-MM-YYYY to YYYY-MM-DD", "365 (date_dim)", "Source date_dim uses DD-MM-YYYY. All dates standardised to ISO format."],
        ["DQ-08", "Extend date_dim", "Generate dates 2020-04-21 to 2024-02-21", "365 → ~1,402", "Source covers 2023 only. Order data spans 2020-2024. Extended with US holidays."],
        ["DQ-09", "Flag anonymous orders", "Add is_anonymous = (USER_ID IS NULL)", "17,808 (8.75%)", "Not removed — flagged. Included in aggregate metrics, excluded from per-customer metrics."],
        ["DQ-10", "Flag zero-price items", "Add is_zero_price = (ITEM_PRICE = 0)", "156 (0.08%)", "Likely promos/comps. Not removed — flagged for the dashboard."],
    ],
    col_widths=[1, 3, 3.5, 2.5, 6.5]
)

doc.add_heading("Category Cleaning Map", level=2)
add_table(
    ["Dirty Value", "Clean Value", "Count"],
    [
        ["Kids", "Kid's", "1,158"],
        ["BBQ Plateshttps://order.pxsweb.com/...", "BBQ Plates", "70"],
        ["Bowls0", "Bowls", "52"],
        ["Sandwiches`1", "Sandwiches", "30"],
        ["Drip Chttps://www.opendining.net/...offee", "Drip Coffee", "26"],
        ["Sqalads", "Salads", "8"],
        ["Kid'shttps://www.opendining.net/...", "Kid's", "2"],
        ["(empty string)", "Unknown", "1"],
        ["Test Items", "EXCLUDE (DQ-04)", "1"],
    ],
    col_widths=[6, 4, 2]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 14. DASHBOARD CONSUMPTION
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("14. Dashboard Consumption", level=1)

doc.add_paragraph(
    "The Streamlit dashboard connects to Athena via PyAthena and queries the Gold layer tables. "
    "Each of the 6 required dashboard views maps directly to specific Gold tables."
)

add_table(
    ["Dashboard View", "Primary Gold Table(s)", "Key Question Answered"],
    [
        ["1. Customer Segmentation", "gold_rfm_segments, gold_clv_snapshot, dim_customer",
         "What distinct customer segments emerge when grouping by purchase behavior and loyalty status?"],
        ["2. Churn Risk Indicators", "gold_churn_indicators, dim_customer",
         "Which metrics correlate with higher churn risk?"],
        ["3. Sales Trends & Seasonality", "gold_sales_daily, gold_sales_weekly, gold_sales_monthly, dim_date",
         "What are the monthly and seasonal trends, and how do they vary by category or location?"],
        ["4. Loyalty Program Impact", "gold_loyalty_comparison, dim_customer",
         "How does loyalty membership affect spending and repeat order rates?"],
        ["5. Location Performance", "gold_location_perf, dim_restaurant",
         "Which locations generate highest revenue and what distinguishes top performers?"],
        ["6. Pricing & Upsell", "gold_upsell_analysis",
         "How do paid add-ons affect overall sales volume and order value?"],
    ],
    col_widths=[3.5, 5, 8]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 15. DESIGN DECISIONS REGISTER
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("15. Design Decisions Register", level=1)

doc.add_paragraph(
    "Every non-obvious architectural decision is recorded here with its context, alternatives considered, "
    "and rationale. This register exists so that any future question of 'why did you do X instead of Y?' "
    "has an immediate, documented answer."
)

decisions = [
    ["DD-01", "Connect Glue directly to RDS SQL Server, no Aurora intermediate",
     "Aurora adds cost, latency, and replication complexity for zero benefit. SQL Server is already on RDS = already a cloud database. Glue JDBC works with SQL Server natively.",
     "Aurora, DMS to S3"],
    ["DD-02", "Delta Lake on S3, not plain Parquet",
     "Delta provides ACID transactions, merge/upsert (incremental ingestion), time travel (rollback), and schema enforcement. Replaces the need for Glue Data Catalog as a metadata store.",
     "Parquet + Glue Data Catalog"],
    ["DD-03", "Glue Workflow for orchestration, not Step Functions + EventBridge",
     "Glue Workflow has native scheduled triggers (replaces EventBridge) and conditional triggers with on-success/on-failure predicates (replaces Step Functions). Fewer services = less to maintain.",
     "Step Functions + EventBridge, Apache Airflow on MWAA"],
    ["DD-04", "3 Glue jobs (per layer), not per-table jobs",
     "30-60s cold start per job. 20 table-level jobs = 10+ min startup on 54 MB. AWS best practice: group by layer. Failure isolation solved via try/except per table within each job.",
     "1 job per table (15-20 jobs)"],
    ["DD-05", "Python Shell Alert Job inside Glue Workflow, not EventBridge rule",
     "SME feedback: no EventBridge. Glue Workflow on-failure conditional triggers natively launch the alert job. Python Shell uses 1/16 DPU = negligible cost.",
     "EventBridge rule → SNS, CloudWatch Alarm → SNS"],
    ["DD-06", "SSE-KMS encryption, not SSE-S3",
     "SSE-S3 is Amazon-managed with no audit trail. SSE-KMS gives CloudTrail audit log of every key usage, customer-controlled rotation, and ability to revoke access. $1.10/month incremental cost.",
     "SSE-S3 (default)"],
    ["DD-07", "CLV as daily snapshot (cross-join with dim_date), not order-date-only",
     "Requirement says 'show how LTV evolves daily.' Order-date-only creates gaps. Cross-join with dim_date + forward-fill produces continuous time series for every customer.",
     "One row per customer per order date only"],
    ["DD-08", "Upsell analysis instead of discount analysis",
     "Zero negative option_price values exist in the dataset (verified: all 193,017 rows, range $0-$8, 16 distinct values). Discounts not captured in this data. Upsell analysis uses the same table to measure paid vs free add-on impact.",
     "Discount analysis per requirements (impossible with available data)"],
    ["DD-09", "RFM quintile scoring with segment rules, not k-means clustering",
     "Requirement says 'segment using RFM metrics' — this is a deterministic, interpretable method. K-means would add ML complexity with no requirement for it. Analysts can understand and explain quintile-based segments.",
     "K-means, DBSCAN, or other ML clustering"],
    ["DD-10", "Churn thresholds (30/45/90 days), not ML prediction",
     "Requirement explicitly says 'no predictions.' Threshold-based tagging is deterministic, explainable, and sufficient for marketing to identify at-risk customers.",
     "Logistic regression, survival analysis"],
    ["DD-11", "Athena for dashboard queries, not Redshift",
     "Gold layer < 10 MB. Athena cost: < $1/month. Redshift minimum: ~$180/month. One Streamlit dashboard, not concurrent BI users. Athena is the right tool at this scale.",
     "Redshift, Redshift Serverless"],
    ["DD-12", "VPC Endpoints for Glue-S3 traffic",
     "Without endpoints, Glue traffic to S3 traverses the public internet. Gateway VPC Endpoint for S3 has zero additional cost and keeps traffic on AWS private backbone.",
     "Public internet path (default)"],
]

add_table(
    ["ID", "Decision", "Rationale", "Alternatives Rejected"],
    decisions,
    col_widths=[1.3, 4.5, 6, 4.5]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 16. RISK REGISTER
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("16. Risk Register", level=1)

add_table(
    ["ID", "Risk", "Likelihood", "Impact", "Mitigation"],
    [
        ["R-01", "Source schema change (new/renamed columns in SQL Server)", "Medium", "High",
         "Delta Lake schema enforcement rejects mismatched writes → job fails → alert fires → team investigates. Schema changes require a conscious Glue job update."],
        ["R-02", "gold_clv_snapshot grows large (~28M rows at scale)", "High", "Medium",
         "Partition by snapshot_month. Athena queries filter by date range. Consider lifecycle policy to archive snapshots older than 12 months to Glacier."],
        ["R-03", "RDS SQL Server unavailable during batch window", "Low", "High",
         "MaxRetries=2 handles transient failures. If RDS is down for extended period, alert fires and team manually reruns when restored. Bronze/Silver/Gold from previous day remain available."],
        ["R-04", "Data volume grows 10x (from 54 MB to 540 MB)", "Low", "Low",
         "Glue auto-scales. Increase worker count from 2 to 10. Delta merge becomes more valuable at higher volume. Architecture remains the same."],
        ["R-05", "Discount data becomes available in future", "Medium", "Low",
         "Add gold_discount_analysis table to Job 3. No architecture change needed — just an additional metric table in the existing Gold layer."],
        ["R-06", "Team member leaves — no one understands the pipeline", "Medium", "High",
         "This document + code comments + CloudWatch logs + Glue Console visual DAG. Pipeline is 3 jobs with clear names, not a complex distributed system."],
    ],
    col_widths=[1, 5, 2, 1.5, 7]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 17. GLOSSARY
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("17. Appendix A — Glossary", level=1)

add_table(
    ["Term", "Definition"],
    [
        ["Bronze Layer", "Raw data ingested from source with no transformations. The audit trail."],
        ["Silver Layer", "Cleaned, validated, conformed data. Data quality rules applied."],
        ["Gold Layer", "Business-ready tables: dimensions, facts, and calculated metrics."],
        ["Delta Lake", "Open-source storage layer (Apache 2.0) that adds ACID transactions, schema enforcement, time travel, and merge/upsert to Parquet files on S3."],
        ["Delta Merge", "Upsert operation: INSERT new rows, UPDATE changed rows, leave untouched rows as-is. Used for incremental ingestion."],
        ["CLV", "Customer Lifetime Value — total revenue attributed to a customer over their entire relationship."],
        ["RFM", "Recency, Frequency, Monetary — a customer segmentation framework."],
        ["Idempotent", "A process that produces the same result if run multiple times with the same input. Critical for safe reruns and backfill."],
        ["Star Schema", "A data modelling pattern with a central fact table surrounded by dimension tables. Optimised for analytical queries."],
        ["SSE-KMS", "Server-Side Encryption with AWS Key Management Service — customer-managed encryption keys with full audit trail."],
        ["VPC Endpoint", "A private connection between a VPC and an AWS service that does not traverse the public internet."],
        ["DPU", "Data Processing Unit — Glue's compute unit (4 vCPU, 16 GB memory). Billing is per-DPU-second."],
        ["Manifest", "A JSON file written by each Glue job recording which tables succeeded/failed. Used for downstream coordination."],
        ["Time Travel", "Delta Lake feature: query or restore any previous version of a table. Retention: 30 days."],
    ],
    col_widths=[3.5, 13]
)

doc.add_page_break()

# ════════════════════════════════════════════════════════════════════════
# 18. REJECTED ALTERNATIVES
# ════════════════════════════════════════════════════════════════════════
doc.add_heading("18. Appendix B — Rejected Alternatives (Full Analysis)", level=1)

doc.add_paragraph(
    "This appendix provides detailed analysis of every alternative that was considered and rejected. "
    "It exists as a reference for SME review and future architecture discussions."
)

alternatives = [
    ["AWS Aurora (intermediate database)",
     "Mirror SQL Server data in Aurora before S3 ingestion",
     "SQL Server is already on RDS — it IS a cloud database. Aurora adds: (1) replication lag between SQL Server and Aurora, "
     "(2) double storage cost (~$0.10/GB/month × data size), (3) a replication pipeline to monitor and maintain, "
     "(4) another database to secure and encrypt. For the benefit of... nothing. Glue connects to SQL Server via JDBC identically "
     "to how it would connect to Aurora. Zero functional difference. The SME correctly identified this as unnecessary in v1 review."],

    ["Amazon EventBridge (scheduler)",
     "Trigger the pipeline on a cron schedule",
     "EventBridge is designed for event routing across multiple services. We only trigger Glue jobs. "
     "Glue Workflow has a built-in SCHEDULED trigger type that accepts cron expressions. Using EventBridge adds a service "
     "that does nothing Glue Workflow cannot do alone. The only case for EventBridge would be if we needed to trigger "
     "non-Glue targets (Lambda, SQS, etc.) from the same schedule — we do not."],

    ["AWS Step Functions (orchestrator)",
     "Orchestrate the job chain with branching and error handling",
     "Step Functions excels at complex workflows: parallel branches, human approval steps, cross-service orchestration, "
     "and long-running processes. Our pipeline is a linear 3-job chain. Glue Workflow handles this natively with "
     "conditional triggers (on success → next job, on failure → alert job). Step Functions would add: "
     "(1) a state machine definition to maintain, (2) IAM roles for Step Functions itself, (3) CloudWatch logs for "
     "Step Functions in addition to Glue logs, (4) cost ($0.025 per 1,000 state transitions). "
     "For a 3-node linear DAG, this is over-engineering."],

    ["Glue Data Catalog (metadata store)",
     "Register all tables so Athena can discover and query them",
     "The Glue Data Catalog is necessary when using plain Parquet — Parquet files have no built-in metadata for schema, "
     "partition tracking, or file lists. Delta Lake replaces all of this: its _delta_log directory tracks the schema, "
     "partition info, file manifest, and version history. Athena v3 reads Delta tables directly. "
     "Adding the Glue Data Catalog would create two sources of truth for metadata — one in _delta_log and one in the catalog. "
     "This leads to drift and confusion."],

    ["Amazon Redshift (query engine)",
     "Use a data warehouse for dashboard queries instead of Athena",
     "Our Gold layer is under 10 MB across 14 tables. Redshift minimum cost: ~$180/month (dc2.large). "
     "Athena cost for this workload: < $1/month ($5 per TB scanned, our queries scan KB not TB). "
     "Redshift is designed for TB/PB scale with concurrent BI users. We have one Streamlit dashboard. "
     "The cost difference is 180x for no performance benefit at this scale. If the workload grows to multiple "
     "concurrent dashboards or TB-scale data, Redshift Serverless becomes worth revisiting."],

    ["Amazon EMR (Spark engine)",
     "Run PySpark on a managed Spark cluster instead of Glue",
     "EMR requires: (1) cluster provisioning (instance types, count, scaling policies), (2) security group configuration, "
     "(3) bootstrap scripts for Delta Lake libraries, (4) cluster lifecycle management (start/stop/terminate). "
     "For a 54 MB dataset running once daily, this is like hiring a moving truck to deliver a letter. "
     "Glue is serverless — zero infrastructure. Pay-per-second. Native Delta Lake support in Glue 4.0. "
     "At TB scale with complex multi-hour Spark jobs, EMR becomes cost-effective. Not here."],

    ["Apache Airflow on MWAA (orchestrator)",
     "Use Airflow DAGs for pipeline orchestration",
     "MWAA (Managed Workflows for Apache Airflow) minimum cost: ~$50/month for the smallest environment. "
     "It requires DAG file management in S3, Python environment configuration, and Airflow-specific expertise. "
     "Our pipeline is 3 jobs in a chain. Glue Workflow does this for free (no separate billing). "
     "Airflow is the right choice for complex multi-pipeline environments with 50+ DAGs. Not for a single 3-job workflow."],

    ["AWS DMS (data ingestion)",
     "Use Database Migration Service for CDC from SQL Server to S3",
     "DMS is designed for continuous CDC (change data capture) — it maintains a replication instance that streams "
     "changes in real-time or near-real-time. Our requirement is a once-daily batch. DMS adds: (1) a replication instance "
     "to manage and pay for (~$30+/month), (2) task configuration and monitoring, (3) another service to secure. "
     "Glue JDBC + Delta merge achieves the same incremental result in a simpler, batch-oriented way."],

    ["Snowflake / DBT (data warehouse + transformation)",
     "Use Snowflake for storage/compute and DBT for transformations",
     "Explicitly prohibited by the requirements: 'The company does not want to get new licenses. "
     "The company does not want to use Snowflake, DBT, etc external tools.' Additionally, all logic "
     "must be in PySpark, which rules out DBT (SQL-based)."],
]

for alt_name, alt_purpose, alt_reason in alternatives:
    doc.add_heading(alt_name, level=2)
    p = doc.add_paragraph()
    run = p.add_run("Proposed for: ")
    run.bold = True
    p.add_run(alt_purpose)
    p = doc.add_paragraph()
    run = p.add_run("Why rejected: ")
    run.bold = True
    p.add_run(alt_reason)
    doc.add_paragraph()  # spacer

# ════════════════════════════════════════════════════════════════════════
# SAVE
# ════════════════════════════════════════════════════════════════════════
output_path = os.path.join(
    r"C:\Users\12145\Documents\DE Academy\Business Insights Assessment",
    "Solution_Design_Document_v3.docx"
)
doc.save(output_path)
print(f"Document saved to: {output_path}")
print(f"File size: {os.path.getsize(output_path):,} bytes")
