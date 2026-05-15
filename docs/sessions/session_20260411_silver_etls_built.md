# Session Report — April 11, 2026 (continuation)
## Silver Layer ETLs: Three Jobs Built, Deployed, and SUCCEEDED

---

## TL;DR

Built all 3 Silver layer Glue ETL jobs — one per source table — following the
project-wide standards locked in during the Bronze debug marathon. Each job is
self-contained (no `common/utils.py` imports), uses `DeltaTable.forPath().merge()`,
dedupes on merge keys, follows the `job.commit()` + natural-end / `raise` exit
pattern, and writes a manifest + SNS alert on failure.

**DEPLOYED AND RUN. All 3 jobs SUCCEEDED on first run — zero debugging needed.**
Data verified in S3 Silver, row counts match data exploration predictions.

### Final Results
| Table | Bronze | Silver | Removed | Retained | Runtime |
|---|---|---|---|---|---|
| `order_items` | 203,520 | 202,105 | 1,415 | 99.30% | 108s |
| `order_item_options` | 193,017 | 190,718 | 2,299 | 98.81% | 101s |
| `date_dim` | 365 | 365 | 0 | 100.00% | ~60s |

---

## 1. Why Silver? (The Grounding We Started With)

Before writing any code we re-read the full project context to make sure Silver
actually serves the Gold / dashboard requirements:

### The 7 business metrics the dashboard must show
| # | Metric | Gold Table | Priority |
|---|---|---|---|
| 1 | Customer Lifetime Value — daily evolution | `gold_clv_snapshot` | PRIMARY |
| 2 | RFM Segmentation | `gold_rfm_segments` | Secondary |
| 3 | Churn Indicators | `gold_churn_indicators` | Secondary |
| 4 | Sales Trends (daily/weekly/monthly) | `gold_sales_*` | Secondary |
| 5 | Loyalty vs non-loyalty comparison | `gold_loyalty_comparison` | Secondary |
| 6 | Location/restaurant performance | `gold_location_perf` | Secondary |
| 7 | **Upsell analysis** (pivoted from discount) | `gold_upsell_analysis` | Secondary |

### The critical DQ rules that drove Silver logic
From `01_data_exploration.md`:
- Outliers: 590 rows with ITEM_PRICE > $50, 84 rows with ITEM_QUANTITY > 20 →
  **distort 79% of raw revenue** (the Korean Kimchi @ $5,000 × qty 500 disaster)
- 826 DEVELOPMENT rows on a single dev-only restaurant ID
- 8.75% anonymous orders (no USER_ID) — keep for sales trends, exclude from CLV/RFM
- Dirty category names: URL-embedded, typos, trailing digits
- **Zero negative option prices** → pivot from discount analysis to upsell analysis
- date_dim uses `DD-MM-YYYY` format (European), not US `MM-DD-YYYY`

---

## 2. What Silver Does (Operations Performed)

Silver is the curated, conformed, trustworthy layer. All Gold metrics join
against Silver — so Silver must be **clean, typed, and analytics-ready**.

### Common operations in every Silver job
1. **Read from Bronze Delta** (not from RDS — Bronze is the immutable source of truth)
2. **Data type casting** — strict enforcement: `DECIMAL(10,2)`, `INT`, `TIMESTAMP`, `DATE`, `BOOLEAN`
3. **NULL primary key rejection** — unjoinable rows dropped
4. **Outlier filtering** — apply business DQ rules
5. **Value standardization** — clean dirty strings
6. **Derived columns** — pre-compute useful fields so Gold doesn't have to
7. **Bronze → Silver row count logging** — detect silent data loss
8. **Dedupe safety net** on merge keys
9. **Delta MERGE** into Silver (or full overwrite for small tables)
10. **Manifest + SNS alert** on failure

---

## 3. Job 1: `clean-order-items.py`

**Source**: `s3://globalpartners-aws/bronze/order_items/`
**Target**: `s3://globalpartners-aws/silver/order_items/`
**Merge key**: `ORDER_ID + LINEITEM_ID`
**Strategy**: Delta MERGE (upsert)

### Specific operations
| Step | Operation | Rows Removed (est.) |
|---|---|---|
| 1 | Read Bronze Delta (203,519 rows) | — |
| 2 | Filter `APP_NAME != "Alltown Fresh - DEVELOPMENT"` | ~826 |
| 3 | Filter `RESTAURANT_ID != "6050e76361e498ca740bba6f"` | (overlaps with #2) |
| 4 | Filter `ITEM_CATEGORY != "Test Items"` | 1 |
| 5 | Drop NULL `ORDER_ID` / `LINEITEM_ID` | ≤1 |
| 6 | Cast `CREATION_TIME_UTC` → TIMESTAMP (ISO 8601 parse) | — |
| 7 | Cast `ITEM_PRICE` → DECIMAL(10,2) | — |
| 8 | Cast `ITEM_QUANTITY` → INT | — |
| 9 | Cast `IS_LOYALTY` → BOOLEAN (from "TRUE"/"FALSE" string) | — |
| 10 | Filter `0 <= ITEM_PRICE <= 50` | ~590 |
| 11 | Filter `1 <= ITEM_QUANTITY <= 20` | ~85 |
| 12 | Clean `ITEM_CATEGORY` (URL-embedded, typos, trailing digits) | — |
| 13 | Derive `order_date`, `order_hour`, `line_item_revenue`, `is_anonymous`, `is_zero_price_flag` | — |
| 14 | Dedupe on merge keys | ≤0 (Bronze already deduped) |
| 15 | Delta MERGE into Silver | — |

**Expected output**: ~202,000 rows retained (~99.3% of Bronze). Clean revenue
~$2,062,659 (versus $9,933,754 raw, per Section 5.1 of data exploration).

### Category cleaning rules implemented
| Dirty pattern | Clean value |
|---|---|
| Contains `BBQ Plates` | `BBQ Plates` |
| `Bowls0` | `Bowls` |
| Regex `^\s*Sandwiches.*$` | `Sandwiches` |
| Contains `Drip C` AND `offee` | `Drip Coffee` |
| `Sqalads` | `Salads` |
| Starts with `Kid's` OR equals `Kids` | `Kid's` |
| NULL or empty | `Unknown` |

### Derived columns (why each one)
- `order_date` — groups for daily sales / CLV snapshot joins
- `order_hour` — for hourly sales heat map (peak at 15:00-16:00 UTC)
- `line_item_revenue` = `ITEM_PRICE × ITEM_QUANTITY` — used in fact_orders
- `is_anonymous` = `USER_ID IS NULL` — CLV/RFM eligibility flag
- `is_zero_price_flag` = `ITEM_PRICE == 0` — 156 freebies/promos to flag

---

## 4. Job 2: `clean-order-item-options.py`

**Source**: `s3://globalpartners-aws/bronze/order_item_options/`
**Target**: `s3://globalpartners-aws/silver/order_item_options/`
**Merge key**: `ORDER_ID + LINEITEM_ID + OPTION_GROUP_NAME + OPTION_NAME`
**Strategy**: Delta MERGE (upsert)

### Specific operations
| Step | Operation |
|---|---|
| 1 | Read Bronze Delta (193,017 rows) |
| 2 | Drop NULL `ORDER_ID`, `LINEITEM_ID`, `OPTION_GROUP_NAME`, `OPTION_NAME` |
| 3 | Cast `OPTION_PRICE` → DECIMAL(10,2) |
| 4 | Cast `OPTION_QUANTITY` → INT |
| 5 | Standardize `OPTION_GROUP_NAME`: trim → strip `CT ` prefix → `initcap` |
| 6 | Trim `OPTION_NAME` |
| 7 | Filter `OPTION_PRICE >= 0` AND `OPTION_QUANTITY >= 1` (defensive) |
| 8 | Derive `is_paid_option` (`OPTION_PRICE > 0`) and `option_revenue` |
| 9 | Dedupe on merge keys |
| 10 | Delta MERGE into Silver |

### Why the OPTION_GROUP_NAME cleaning matters
Data exploration found 133 distinct option groups. After trim + `CT ` strip +
`initcap`, these collapse to ~100 by merging case/prefix duplicates:
- `BOTTLE DEPOSIT` (17) + `Bottle Deposit` (181) → `Bottle Deposit` (198)
- `CT Bacon Egg And Cheese Options` (53) + `Bacon Egg And Cheese Options` (14,760) → `Bacon Egg And Cheese Options` (14,813)
- `add` (144) + `Add` (16) → `Add` (160)

This directly improves the upsell analysis metric — without standardization,
the same logical option group would appear as multiple rows in the dashboard.

### Orphan options decision (KEEP, don't filter)
28 option rows (0.01%) reference ORDER_IDs not present in order_items.
**Kept in Silver** — filtering here would hide a source-system bug. They will
be naturally excluded when Gold JOINs options to line items for fact_orders.

### Why this feeds upsell analysis (the pivot)
Requirements originally said "use option_price < 0 for discounts." Data
exploration confirmed ZERO negative prices. `is_paid_option` flag allows Gold
to compute:
- % of options that are free ($0) vs paid  (baseline: 66.3% free, 33.7% paid)
- Average upsell revenue per order
- Top-grossing paid options by count and revenue
- Which items have the highest paid-option attach rate

---

## 5. Job 3: `clean-date-dim.py`

**Source**: `s3://globalpartners-aws/bronze/date_dim/`
**Target**: `s3://globalpartners-aws/silver/date_dim/`
**Strategy**: Full OVERWRITE (365 rows, tiny table)

### Specific operations
| Step | Operation |
|---|---|
| 1 | Read Bronze Delta (365 rows) |
| 2 | Drop NULL `date_key` |
| 3 | Rename raw `date_key` → `date_key_raw`, parse `DD-MM-YYYY` → `date_key` (DATE) |
| 4 | Cast `year` / `month` / `week` → INT |
| 5 | Cast `is_weekend` / `is_holiday` → BOOLEAN (via helper handling TRUE/T/1/YES) |
| 6 | Trim `holiday_name`; empty string → NULL |
| 7 | Reject rows where DATE parse returned NULL (malformed source) |
| 8 | Full overwrite to Silver Delta |

### Why overwrite (not merge)
365 rows, ~5 KB total. Full overwrite is faster, simpler, and matches the
Bronze strategy for this table. Merge adds cost with no benefit at this size.

### Why DD-MM-YYYY (not US format)
Confirmed via data exploration section 2.3: source dates are European
`DD-MM-YYYY` (e.g. `01-01-2023` = Jan 1). `to_date(col, "dd-MM-yyyy")` enforces
this parse.

### Gold responsibility (NOT Silver)
date_dim only covers 2023. Order data spans Apr 2020 – Feb 2024 (~1,402 days).
**Gold** will extend `dim_date` to the full range and add US federal holidays
for 2020, 2021, 2022, 2024. Silver cleans what Bronze gives us — nothing more.

---

## 6. Project Standards Applied (From Bronze Debug Marathon)

Every Silver script adheres to the 7 standards established on 2026-04-11:

| # | Standard | Applied? |
|---|---|---|
| 1 | Self-contained scripts (no `common/utils.py` imports) | YES — all helpers inlined |
| 2 | `DeltaTable.forPath().merge()` — never `spark.sql("MERGE INTO delta.")` | YES |
| 3 | `df.dropDuplicates(merge_keys)` safety net before merge | YES |
| 4 | Required params: `S3_BUCKET`, `REGION`, `SNS_TOPIC_ARN` + `--datalake-formats delta` | YES |
| 5 | No `sys.exit()` — `job.commit()` + natural end on success, `raise Exception()` on fail | YES |
| 6 | Per-table try/except (one table's failure doesn't block others) | YES (single-table jobs, but try/except still wraps main logic) |
| 7 | Manifest written to S3 + SNS alert on failure | YES |

### Differences from Bronze
- **No `SECRET_NAME` parameter** — Silver reads from S3 Delta, not from RDS, so
  no Aurora credentials are needed.
- **No JDBC code** — input is `spark.read.format("delta").load(...)`.
- **Single-table scripts** — unlike Bronze (which was one job for all 3 tables),
  Silver has three separate jobs so failures are fully isolated at the job level
  (not just at the try/except level).

---

## 7. Files Produced

| File | Description |
|---|---|
| `glue_jobs/silver/clean-order-items.py` | Silver ETL for order_items (most complex) |
| `glue_jobs/silver/clean-order-item-options.py` | Silver ETL for order_item_options (upsell feeder) |
| `glue_jobs/silver/clean-date-dim.py` | Silver ETL for date_dim (overwrite strategy) |
| `session_20260411_silver_etls_built.md` | This report |

---

## 8. Deployment + Run Results (DONE)

### Deployment steps executed
1. **Uploaded all 3 scripts** to `s3://globalpartners-aws/scripts/` via `aws s3 cp`.
2. **Created 3 Glue jobs** via `aws glue create-job` with `--cli-input-json` payloads
   (avoided the hidden-tab bug by using file-based JSON instead of the console).
   Job configs saved in `glue_jobs/silver/_deploy/*.json` for reproducibility.
   - GlueVersion 4.0, G.1X × 2 workers, Timeout 15-30 min, MaxRetries 0
   - `--datalake-formats delta` set
3. **Verified job params** via `aws glue get-job --query Job.DefaultArguments` —
   clean, no hidden characters.
4. **Ran `clean-date-dim` first** (smallest, fastest feedback) → SUCCEEDED in ~60s.
5. **Ran `clean-order-items` + `clean-order-item-options` in parallel** → both SUCCEEDED.

### Final verified results
| Table | Bronze | Silver | Removed | % Retained | Exec Time | Silver Size |
|---|---|---|---|---|---|---|
| `order_items` | 203,520 | 202,105 | 1,415 | 99.30% | 108s | 12.3 MiB |
| `order_item_options` | 193,017 | 190,718 | 2,299 | 98.81% | 101s | 7.0 MiB |
| `date_dim` | 365 | 365 | 0 | 100.00% | ~60s | 7 KiB |

### order_items — breakdown matches data exploration predictions
| Filter Stage | Rows | Removed | Prediction |
|---|---|---|---|
| Bronze initial | 203,520 | — | 203,519 (off by 1 — live DB) |
| After business filters (DEV app) | 202,693 | 827 | ~826 ✓ |
| After PK null filter | 202,691 | 2 | ~1 ✓ |
| After outlier filter (price, qty) | 202,105 | 586 | ~590 ✓ |
| Final Silver | 202,105 | 1,415 | ~1,412 ✓ |

**The DQ rules performed exactly as designed.** 99.30% retention matches the
99.31% forecast from the data exploration doc.

### order_item_options — 2,299 "duplicates" explained (important finding)
The 2,299 rows removed were NOT duplicates in the source data. They were
**created by Silver's OPTION_GROUP_NAME standardization** and then collapsed
by the `dropDuplicates(MERGE_KEYS)` safety net:

- Bronze has 193,017 distinct rows on `(ORDER_ID, LINEITEM_ID, OPTION_GROUP_NAME, OPTION_NAME)`.
- Silver's pipeline (`trim → strip "CT " → initcap`) made pairs like `"BOTTLE DEPOSIT"`
  and `"Bottle Deposit"` identical on all 4 PK columns.
- `dropDuplicates` collapsed them.

**This is exactly what we wanted** — it's the case-merge + CT-prefix collapse we
designed for. Without the `dropDuplicates` safety net, the Delta MERGE would
have hit the same `multipleSourceRowMatchingTargetRowInMergeException` as
Bronze Error #9. The Bronze debug marathon's "defensive dedup" lesson paid off
immediately.

**Caveat**: `dropDuplicates` picks an arbitrary row when duplicates exist.
For the collapsed pairs, if two rows had different `OPTION_PRICE` values, one
is silently dropped. Spot checks confirm they had matching prices (they were
truly the same option group), so no data loss — but worth remembering for Gold.

### Data landed in S3 (verified)
```
s3://globalpartners-aws/silver/
├── order_items/
│   ├── _delta_log/00000000000000000000.json       (10 KB)
│   └── ingestion_date=2026-04-11/
│       ├── part-00000-*.snappy.parquet            (3.1 MiB)
│       ├── part-00001-*.snappy.parquet            (3.1 MiB)
│       ├── part-00002-*.snappy.parquet            (3.1 MiB)
│       └── part-00003-*.snappy.parquet            (3.0 MiB)
├── order_item_options/
│   ├── _delta_log/00000000000000000000.json       (6 KB)
│   └── ingestion_date=2026-04-11/
│       ├── part-00000-*.snappy.parquet            (1.8 MiB)
│       ├── part-00001-*.snappy.parquet            (1.8 MiB)
│       ├── part-00002-*.snappy.parquet            (1.8 MiB)
│       └── part-00003-*.snappy.parquet            (1.8 MiB)
└── date_dim/
    ├── _delta_log/00000000000000000000.json       (2.6 KB)
    └── ingestion_date=2026-04-11/
        └── part-00000-*.snappy.parquet            (7 KB)

s3://globalpartners-aws/manifests/
├── clean-date-dim/2026-04-11/status.json          (overall_status: SUCCESS)
├── clean-order-items/2026-04-11/status.json       (overall_status: SUCCESS)
└── clean-order-item-options/2026-04-11/status.json (overall_status: SUCCESS)
```

---

## 9. Next Steps (Resume Here Next Session)

### Gold layer (13 jobs planned)
1. **Dimensions** (4 jobs, parallel): `dim_customer`, `dim_restaurant`, `dim_date` (extended from 365 → ~1,402 rows), `dim_menu_item`
2. **Fact table** (1 job): `fact_orders` — 1 row per ORDER_ID, joins line items + options
3. **Metrics** (8 jobs): `gold_clv_snapshot` (primary), `gold_rfm_segments`, `gold_churn_indicators`, `gold_sales_daily/weekly/monthly`, `gold_loyalty_comparison`, `gold_location_perf`, `gold_upsell_analysis`

### Orchestration
4. Build Glue Workflow chaining: Bronze → {Silver × 3 parallel} → {Gold dims parallel} → Gold fact → Gold metrics parallel → Alert-on-failure
5. EventBridge daily 2AM UTC schedule

### Finalization
6. Streamlit dashboard (Step 6) — 6 views, one per metric area
7. CI/CD + submission (Step 7)

---

## 9. Risks / Things to Watch For

1. **Cast failures on malformed timestamps**: If any `CREATION_TIME_UTC` string
   doesn't match ISO 8601, `to_timestamp` returns NULL. These rows will be
   dropped by downstream null-key filters in Gold — verify in smoke test.
2. **Category cleaning regex coverage**: Our explicit map covers the 8 dirty
   values found during exploration. If SQL Server has accumulated new dirty
   values since March, they'll pass through untouched. Monitor distinct count
   of `ITEM_CATEGORY` in Silver after first run.
3. **`CT ` prefix collision**: `initcap` on `"CT Breakfast"` already-prefixed
   items is fine (they become `"Breakfast"` which merges into the canonical
   group). But if `CT` is a legitimate brand prefix somewhere, it'd be lost.
   Data exploration showed it was always a duplicate-category artifact.
4. **Bronze row count assumption**: Silver row-count logging assumes Bronze has
   data. If Bronze is empty (which would be a Bronze failure), Silver will
   succeed with zero rows — not ideal. Consider adding a "Silver empty = alert"
   check later if needed.

---

## 10. Meta: How This Went

Writing Silver was significantly faster than Bronze because all the debugging
was already done on Bronze. The scripts basically wrote themselves by following
the established patterns. This validates the Bronze debug lesson: **"Every
error today = permanent knowledge. Silver and Gold will go WAY faster."**

Gold will go faster still. Three self-contained silver jobs = about 900 lines
of code total, and most of it is logging, comments, and the repeated exit
pattern. The actual business logic per job is ~30 lines of PySpark.
