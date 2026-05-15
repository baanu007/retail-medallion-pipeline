# Session Report - March 31 to April 1, 2026

## Overview
Full data quality analysis and pipeline architecture design session for the GlobalPartners Alltown Fresh DE Academy assessment.

---

## 1. Data Quality Analysis

### Schema Summary
- **order_items.csv**: 203,519 rows, 13 columns - main transaction table (line items per order)
- **order_item_options.csv**: 193,017 rows, 6 columns - add-ons/customisations per line item
- **date_dim.csv**: 365 rows, 8 columns - calendar dimension (2023 only)

### Data Relationships Validated
- LINEITEM_ID is the PK in order_items (203,518 unique, 1 null)
- ORDER_ID links order_items to order_item_options (1-to-many)
- 0 orders with multiple USER_IDs, RESTAURANT_IDs, or timestamps (all consistent)
- 14 orphan ORDER_IDs in options table (28 rows, 0.01%) - not in items table
- 15 orphan LINEITEM_IDs in options table
- 59.9% of orders have options, 40.1% have none
- IS_LOYALTY and PRINTED_CARD_NUMBER are perfectly correlated (no inconsistencies)
- 7 loyalty cards map to multiple USER_IDs
- Only 28 of 17,808 anonymous rows have a card number (card NOT viable for USER_ID recovery)
- Items per order: mean=1.55, median=1, max=61
- Options per line item: mean=1.88, median=2, max=152

### Data Quality Issues Found (9 total)
1. **ITEM_PRICE > $100** - 144 rows - REMOVING (prices up to $5,000, clearly data entry errors, distorts 79% of revenue)
2. **ITEM_QUANTITY > 50** - 27 rows - REMOVING (quantities up to 500, overlaps with price outliers)
3. **Test/Development data** - 827 rows - REMOVING (APP_NAME="DEVELOPMENT" = 826 rows from 1 test restaurant + 1 "Test Items" category row)
4. **Missing USER_ID** - 17,808 rows (8.75%), 12,669 orders - REMOVING from customer-level metrics (CLV, RFM, churn), keeping for aggregate analysis
5. **No discount data** - zero negative OPTION_PRICE values exist in entire dataset (range $0-$8, 16 distinct values, 66.3% are $0 free options) - PIVOTING to upsell/add-on analysis instead of discount analysis
6. **Dirty ITEM_CATEGORY names** - 8 bad categories, ~1,347 rows - CLEANING (embedded URLs, typos like "Sqalads", inconsistent "Kids" vs "Kid's", trailing digits like "Bowls0")
7. **Orphan option records** - 14 orders, 28 rows in options with no match in items - DOCUMENTING (excluded via left join)
8. **Zero-price items** - 156 rows with ITEM_PRICE=$0 - KEEPING but flagging (likely promos/comps)
9. **Loyalty cards with multiple USER_IDs** - 7 cards - DOCUMENTING (USER_ID remains primary customer key, not card number)

### Key Data Stats for Reference
- 131,328 unique orders
- 20,174 unique customers (with USER_ID)
- 28 restaurant locations (27 after removing dev)
- 5,866 unique loyalty cards
- 22.64% loyalty rate
- 50% of customers are one-time buyers
- Clean avg order value: ~$15-17
- Order date range: 2020-04-21 to 2024-02-21
- date_dim only covers 2023 (60.4% of order rows are outside 2023)
- 3 APP_NAMEs: Alltown Fresh (98.97%), Neighborhood Perks (0.62%), DEVELOPMENT (0.41%)
- CURRENCY is always "USD" (constant column)
- OPTION_QUANTITY is always 1 (zero variance column)
- 203,332 timestamps have milliseconds, 187 do not (mixed format)

---

## 2. Pipeline Architecture Design

### Data Flow
SQL Server → AWS Aurora (already done) → S3 Bronze (raw) → S3 Silver (clean) → S3 Gold (metrics) → Athena → Streamlit Dashboard

### AWS Services Used
| Service | Role |
|---------|------|
| AWS Aurora | Cloud source database (replicated from SQL Server) |
| Amazon S3 | Data lake storage (Bronze/Silver/Gold layers, Parquet format) |
| AWS Glue | Runs all PySpark jobs (ingestion, cleaning, metric calculation) |
| Glue Data Catalog | Metadata store so Athena can discover tables |
| Amazon Athena | Serverless SQL queries on Gold layer |
| AWS Step Functions | Orchestrates pipeline jobs in sequence |
| Amazon EventBridge | Daily cron trigger at 2 AM UTC |
| Amazon SNS | Email alerts on job failure |
| AWS Secrets Manager | Stores Aurora DB credentials securely |
| Amazon CloudWatch | Logs and monitoring |

### Medallion Layers
- **Bronze**: Raw copy from Aurora as Parquet, partitioned by ingestion_date, no transformations
- **Silver**: Data quality rules applied (remove outliers, dev data, clean categories, extend date_dim, standardise dates)
- **Gold**: Business-ready tables (dimensions, facts, metrics)

### Gold Layer Data Model

**Dimension Tables:**
- `dim_customer` - one row per USER_ID (first/last order date, total orders, total spend, loyalty flag)
- `dim_restaurant` - one row per RESTAURANT_ID (27 locations)
- `dim_date` - extended date dim covering 2020-04 to 2024-02 (replaces original 365-row 2023-only table)
- `dim_menu_item` - one row per item (ITEM_NAME + cleaned ITEM_CATEGORY)

**Fact Table:**
- `fact_orders` - one row per ORDER_ID (customer, restaurant, date, hour, total_item_revenue, total_option_revenue, total_revenue, item_count, has_paid_options, is_loyalty, is_zero_price)
- Revenue formula: total_item_revenue = SUM(ITEM_PRICE x ITEM_QUANTITY), total_option_revenue = SUM(OPTION_PRICE), total = both combined

**Metric Tables (7 metrics):**

1. `gold_clv_daily` - PRIMARY METRIC - one row per customer per order date showing cumulative CLV over time, with CLV tier (High=top 20%, Medium=mid 60%, Low=bottom 20%)
2. `gold_rfm_segments` - one row per customer with recency_days, frequency, monetary, R/F/M scores (1-5), segment label (VIP, New Customer, Churn Risk, Regular)
3. `gold_churn_indicators` - one row per customer with days_since_last_order, avg_days_between_orders, spend_change_pct, churn_tag (Active <30d, Cooling Off 31-45d, At Risk 46-90d, Inactive >90d)
4. `gold_sales_daily` - daily revenue by restaurant, category, time-of-day bucket
5. `gold_sales_weekly` - weekly aggregation of same
6. `gold_sales_monthly` - monthly aggregation of same
7. `gold_loyalty_comparison` - loyalty vs non-loyalty comparison (avg spend, repeat rate, CLV)
8. `gold_location_performance` - one row per restaurant ranked by revenue, with avg order value, orders/week, unique customers
9. `gold_upsell_analysis` - replaces discount analysis (orders with/without paid options, option revenue %, top paid options)

### Scheduling
- EventBridge triggers Step Functions daily at 2 AM UTC
- 4 sequential steps: Ingest → Clean → Build Metrics → Refresh Catalog
- Each step waits for previous to complete before starting

### Encryption & Security
- S3: SSE-S3 encryption at rest
- Aurora: AES-256 encryption at rest
- All connections: TLS/SSL in transit
- Credentials: AWS Secrets Manager (never hardcoded)
- Access: IAM roles with least-privilege permissions

### Failure & Reload Mechanism
- Auto-retry: 3 attempts per step with increasing wait (30s, 60s, 120s)
- SNS email alerts on final failure
- Idempotent design: re-running same day overwrites same partition, no duplicates
- Glue Job Bookmarks: tracks processed data for incremental loads
- S3 Versioning: previous file versions kept for 30 days for rollback
- Manual reload: can re-trigger any date via Step Functions console

---

## 3. Discount Data Discussion
- Requirements doc says to use option_price < 0 for discounts
- After checking all 193,017 option rows: ZERO negative prices exist
- All 133 option group names are food customisations (milk, bread, protein, etc.) - nothing discount/promo related
- Discounts were likely applied at a different level not captured in this dataset
- Decision: Replace discount analysis with upsell/add-on analysis (free vs paid options)

---

## 4. Files Generated This Session
| File | Description |
|------|-------------|
| `Data_Quality_Assessment.docx` | Word doc covering all 9 DQ issues, schema, relationships - ready for submission |
| `Pipeline_Architecture_Design.docx` | Word doc with full pipeline architecture, data model, scheduling, encryption, failure handling |
| `Pipeline_Architecture.drawio` | Visual diagram of the architecture (open in app.diagrams.net) |
| `session_20260331_dq_report.md` | This file - session context and reference |

---

## 5. Next Steps
- Step 4: Build PySpark ETL pipeline code (Bronze/Silver/Gold Glue jobs)
- Step 5: Implement all 7 business metric calculations in PySpark
- Step 6: Build Streamlit dashboard (6+ views)
- Step 7: CI/CD, documentation, final submission package
