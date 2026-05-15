# Session Report — April 11, 2026 (Spec Compliance Fixes)
## Cross-Verified Step 5 & 6 Against Requirements Doc — Fixed 3 Critical Gaps + 1 Medium Gap

---

## TL;DR

Cross-verified every bullet in `Business_Analysis_Requirements.docx`
(Step 5 metrics and Step 6 dashboards) against our implementation. Found 3
critical gaps + 1 medium gap + noted 3 low/optional items. Fixed all 4
non-cosmetic gaps at the Gold layer and in the dashboard. Metric 6 (Pricing
& Discount Effectiveness / Upsell pivot) was explicitly left as-is per user
instruction.

All 8 dashboard pages re-verified via `AppTest` after the changes.

---

## 1. Gaps Identified from Cross-Verification

### Fixed (this session)

| # | Gap | Severity | Fix |
|---|---|---|---|
| 1 | **RFM windowing**: spec says "Frequency/Monetary in last N months", we had lifetime | CRITICAL | 12-month rolling window applied; Recency stays lifetime |
| 2 | **RFM × loyalty cross-section**: Step 6 Q1 explicitly asks to segment by behavior AND loyalty status | CRITICAL | Joined dim_customer for is_loyalty; dashboard shows cross-tab |
| 3 | **Customer retention per location**: Step 6 Q5 asks for "customer retention" as distinguishing metric | CRITICAL | Added `retention_rate_pct` and `repeat_customer_count` to gold_location_perf |
| 4 | **Absolute churn threshold**: spec example ">45 days = at risk" (absolute), we had personal-gap multiplier | MEDIUM | Added absolute-threshold `churn_tag` (Active ≤30, Cooling Off 30-45, At Risk 45-60, Inactive >60); kept personal-gap version as `churn_tag_personal` |

### Deliberately skipped (per user)

| # | Gap | Why skipped |
|---|---|---|
| 5 | Metric 6 Pricing/Discount dashboard naming + pivot clarity | User said to leave metric 6 alone |
| 6 | Time-of-day in sales trends | Marked "optional" in spec |
| 7 | Holiday annotations in sales charts | Not explicitly required, seasonality is already visible via monthly trends |

---

## 2. Gold Job Changes

### 2.1 `glue_jobs/gold/build-gold-rfm.py`

**Before:**
- F = countDistinct(order_id) over LIFETIME
- M = sum(total_revenue) over LIFETIME
- No loyalty flag in output

**After:**
- F = countDistinct(order_id) **over last 365 days before ref_date**
- M = sum(total_revenue) **over last 365 days before ref_date**
- R = datediff(ref_date, last_order_date) — lifetime (unchanged)
- **Joined dim_customer for `is_loyalty` flag**
- Customers with no orders in the window get F=0, M=0 (they naturally fall into low-score tiers and most become "Churn Risk")

**Implementation**:
```python
WINDOW_DAYS = 365  # "last N months" from the spec

# Recency: lifetime
recency_df = fact.groupBy("user_id").agg(
    F.datediff(F.lit(ref_date), F.max("order_date")).alias("recency_days")
)

# Frequency + Monetary: rolling 12-month window
window_fact = fact.filter(
    F.col("order_date") >= F.date_sub(F.lit(ref_date), WINDOW_DAYS)
)
fm_df = window_fact.groupBy("user_id").agg(
    F.countDistinct("order_id").alias("frequency"),
    F.round(F.sum("total_revenue"), 2).alias("monetary"),
)

# Join dim_customer to get is_loyalty
customers_df = spark.read.format("delta").load(DIM_CUST).select("user_id", "is_loyalty")

rfm = (
    customers_df
    .join(recency_df, on="user_id", how="left")
    .join(fm_df,      on="user_id", how="left")
    .na.fill({"frequency": 0, "monetary": 0.0})
)
```

**Verified output** (20,044 rows):
- `is_loyalty` column present: 14,301 False / 5,743 True
- 9,616 customers have frequency=0 (no orders in last 12 months) — these are the customers who haven't ordered within the window
- 10,428 customers have frequency ≥ 1 — these are actively segmentable

### 2.2 `glue_jobs/gold/build-gold-churn.py`

**Before:** Single `churn_tag` based on personal-gap multipliers.

**After:** TWO churn tags:
1. `churn_tag` — **absolute threshold matching spec example**:
   - Active ≤ 30 days
   - Cooling Off 30–45 days
   - At Risk 45–60 days
   - Inactive > 60 days
2. `churn_tag_personal` — personal-gap multiplier (the old logic, kept as the alternative view)

**Verified output** (10,017 repeat customers):
```
churn_tag (absolute):         Inactive 7,809 | Active 1,527 | Cooling Off 378 | At Risk 303
churn_tag_personal:           Inactive 6,094 | Active 2,686 | Cooling Off 475 | At Risk 762
```

The two methods disagree on ~1,700 customers — the personal method classifies more as Active because their typical inter-order gaps are longer (weekly vs monthly regulars). Both views are useful depending on the business question.

### 2.3 `glue_jobs/gold/build-gold-location-perf.py`

**Before:** 14 columns; customer count but no retention.

**After:** Added 2 new columns:
- `repeat_customer_count` — raw count of customers at this restaurant with ≥2 orders here
- `retention_rate_pct` — `repeat_customer_count / known_customers * 100`

**Implementation**:
```python
customer_visit_counts = (
    fact
    .filter(~F.col("is_anonymous"))
    .groupBy("restaurant_id", "user_id")
    .agg(F.countDistinct("order_id").alias("visit_count"))
)
retention = (
    customer_visit_counts
    .groupBy("restaurant_id")
    .agg(
        F.count("user_id").alias("known_customers"),
        F.sum(F.when(F.col("visit_count") >= 2, 1).otherwise(0)).alias("repeat_customer_count"),
    )
    .withColumn("retention_rate_pct",
                F.round((F.col("repeat_customer_count") / F.col("known_customers")) * 100, 1))
)
loc = loc.join(retention, on="restaurant_id", how="left")
```

**Verified output** (27 locations):
- Retention range: **35.7% – 100.0%**
- Mean retention: **54.1%**

---

## 3. Dashboard Page Changes

### 3.1 `dashboard/pages/2_RFM_Segments.py`

**New structure:**
1. **Loyalty filter radio** (All / Loyalty only / Non-Loyalty only)
2. **Segment × Loyalty cross-tab bar chart** (the chart Step 6 Q1 explicitly asks for)
3. Filtered segment distribution
4. R×M scatter with loyalty shown as point symbol
5. R×F heatmap
6. Segment × loyalty leaderboard table

Directly answers the requirement question: *"What distinct customer segments emerge when grouping customers by purchase behavior (total spend, frequency, recency) AND loyalty status?"*

### 3.2 `dashboard/pages/3_Churn_Indicators.py`

**New structure:**
1. Radio picker to select which churn methodology to use for KPIs/charts (absolute vs personal)
2. **Side-by-side comparison of both distributions** in two columns
3. Recency-vs-spend-change scatter using the selected method
4. Breakdown table for the selected method
5. **Threshold alert list** — customers with `days_since_last_order > 45` (directly matches spec example), sorted by most-recent-first, top 200 shown

The alert list is the "threshold-based alerts" that Step 6 Q2 explicitly asks for.

### 3.3 `dashboard/pages/6_Location_Performance.py`

**New structure:**
1. Added **retention rate** to the KPI band (avg across all locations)
2. Revenue ranking bar chart now **colored by retention rate** (RdYlGn) — instantly shows which high-revenue restaurants are sticky vs which rely on one-time visits
3. **New scatter**: Revenue vs Retention (directly visualizes the "which top performers have high retention" question)
4. AOV vs orders-per-week scatter now colored by retention
5. Leaderboard table now includes `repeat_customer_count` and `retention_rate_pct` columns

Directly answers Step 6 Q5: *"Which restaurant locations generate the highest revenue, and what operational metrics (e.g., average order size, customer retention) distinguish top performers?"*

---

## 4. Verification

### 4.1 Schema + data checks (via deltalake + pandas)

```
gold_rfm_segments:         20,044 rows
  cols include: user_id, is_loyalty, recency_days, frequency, monetary, ...
  is_loyalty: {False: 14301, True: 5743}
  F=0 (no orders in window): 9,616

gold_churn_indicators:     10,017 rows
  cols include: churn_tag, churn_tag_personal, ...
  churn_tag:          {Inactive 7809, Active 1527, Cooling Off 378, At Risk 303}
  churn_tag_personal: {Inactive 6094, Active 2686, At Risk 762,    Cooling Off 475}

gold_location_perf:        27 rows
  cols include: retention_rate_pct, repeat_customer_count, ...
  retention range: 35.7% - 100.0%
  mean retention: 54.1%
```

### 4.2 Streamlit AppTest on all 8 pages

```
Home                   [20.0s]  OK    (first-load materializes CLV)
CLV Snapshot           [ 3.0s]  OK
RFM Segments           [ 1.5s]  OK    ← updated
Churn Indicators       [ 0.0s]  OK    ← updated
Sales Trends           [ 4.5s]  OK
Loyalty Comparison     [ 0.1s]  OK
Location Performance   [ 1.0s]  OK    ← updated
Upsell Analysis        [ 1.1s]  OK    (unchanged — metric 6 left alone)
```

**All 8 pages OK. No regressions.**

---

## 5. Cross-Verification Results (after fixes)

### Step 5 Primary Metric

| Requirement | Status |
|---|---|
| Use order_items AND order_item_options to compute revenue per order | ✅ |
| Aggregate total spend per customer_id | ✅ |
| High CLV: top 20% | ✅ |
| Medium CLV: mid 60% | ✅ |
| Low CLV: bottom 20% | ✅ |
| Show how LTV evolves for each customer daily | ✅ (gold_clv_snapshot) |

### Step 5 Secondary Metrics

| # | Metric | Requirement Match |
|---|---|---|
| 1 | **RFM Segmentation** | ✅ All items met, **including "last N months" window** |
| 2 | **Churn Indicators** | ✅ All items met, **including absolute threshold matching spec example** |
| 3 | **Sales Trends (daily/weekly/monthly × location × category)** | ✅ Required items met (time-of-day was optional) |
| 4 | **Loyalty Program Impact** | ✅ All items met |
| 5 | **Top-Performing Locations** | ✅ All items met, **including customer retention** |
| 6 | **Pricing & Discount Effectiveness** | ⚠️ Pivoted to upsell (data has zero negative prices, documented) — **left as-is per user instruction** |

### Step 6 Dashboards

| # | Required Dashboard | Status |
|---|---|---|
| 1 | **Customer Segmentation** (by behavior AND loyalty status) | ✅ RFM page now has loyalty cross-tab |
| 2 | **Churn Risk Indicators** (metrics correlating with churn + threshold alerts) | ✅ Both methodologies + threshold alert list |
| 3 | **Sales Trends and Seasonality** (monthly/seasonal, by category and location) | ✅ |
| 4 | **Loyalty Program Impact** (CLV, AOV, repeat rate) | ✅ |
| 5 | **Location Performance** (revenue rank + operational metrics incl. retention) | ✅ Retention now shown |
| 6 | **Pricing and Discount Effectiveness** | ⚠️ Pivoted (left as-is) |

**Score: 5/6 dashboards fully spec-compliant. 1/6 pivoted-and-documented per earlier architectural decision.**

---

## 6. What's Left — Step 7 Only

- Update SDD.docx to reflect v3 architecture (remove remaining v1 references)
- Video / presentation walk-through of the complete pipeline + dashboard
- Git commit history + project root README
- CI/CD pipeline using GitHub Actions
- Final submission package

---

## 7. Running Job Success Total

Previous: 36 consecutive successful Glue job executions since the Bronze
debug marathon. This session re-ran 3 jobs (RFM, Churn, Location) with
non-trivial business-logic changes. All 3 succeeded first try.

**New total: 39 consecutive successful Glue job executions** with zero
debugging. Every change has been pattern-based (inherit from the
established Bronze → Silver → Gold template, insert new SQL, done).
