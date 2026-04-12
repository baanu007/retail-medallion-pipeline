# Step 1: Source Data Exploration Report

**Date**: 2026-03-30
**Project**: GlobalPartners Business Insights Assessment (Alltown Fresh)
**Status**: COMPLETE
**Method**: All statistics validated via Python scripts against raw CSV data

---

## 1. Files Overview

| File | Rows | Size | Description |
|------|------|------|-------------|
| `order_items.csv` | 203,519 | 37 MB | Transactional line-item data for customer orders |
| `order_item_options.csv` | 193,017 | 17 MB | Add-ons, customizations, modifiers per order line item |
| `date_dim.csv` | 365 | 15 KB | Calendar dimension table (2023 only) |

**Total data volume**: ~54 MB, ~396,901 rows across all files.

---

## 2. Schema & Column-Level Profiling

### 2.1 order_items.csv (203,519 rows, 13 columns)

| # | Column | Data Type | Non-null % | Null/Empty | Distinct | Notes |
|---|--------|-----------|------------|------------|----------|-------|
| 1 | APP_NAME | string | 100.00% | 0 | 3 | See breakdown below |
| 2 | RESTAURANT_ID | string (MongoDB ObjectId) | 100.00% | 0 | 28 | 28 restaurant locations |
| 3 | CREATION_TIME_UTC | datetime (ISO 8601) | 100.00% | 0 | 131,328 | Range: 2020-04-21 to 2024-02-21 |
| 4 | ORDER_ID | string (MongoDB ObjectId) | 100.00% | 0 | 131,328 | Same cardinality as timestamps |
| 5 | USER_ID | string (MongoDB ObjectId) | **91.25%** | **17,808** | 20,174 | Anonymous orders = CLV gap |
| 6 | PRINTED_CARD_NUMBER | string | 22.64% | 157,435 | 5,866 | Only for loyalty members |
| 7 | IS_LOYALTY | boolean (string) | 100.00% | 0 | 2 | FALSE: 77.36%, TRUE: 22.64% |
| 8 | CURRENCY | string | 100.00% | 0 | 1 | Always "USD" |
| 9 | LINEITEM_ID | string (MongoDB ObjectId) | ~100% | 1 | 203,518 | Unique per row (PK candidate) |
| 10 | ITEM_CATEGORY | string | ~100% | 1 | 45 | Has quality issues (see Section 5) |
| 11 | ITEM_NAME | string | ~100% | 1 | 431 | Menu item names |
| 12 | ITEM_PRICE | decimal | 100.00% | 0 | 636 | Min: $0, Max: $5,000 |
| 13 | ITEM_QUANTITY | integer | 100.00% | 0 | 39 | Min: 0, Max: 500 |

#### APP_NAME Distribution
| Value | Count | % |
|-------|-------|---|
| Alltown Fresh | 201,423 | 98.97% |
| Alltown Neighborhood Perks | 1,270 | 0.62% |
| Alltown Fresh - DEVELOPMENT | 826 | 0.41% |

#### Top 10 RESTAURANT_ID Distribution
| RESTAURANT_ID | Orders | % |
|---------------|--------|---|
| 5fece84aadb0d51509c36f22 | 32,459 | 15.95% |
| 6054db3495b70198148b456f | 23,135 | 11.37% |
| 5e7e35ec902ad5ac017b242a | 21,416 | 10.52% |
| 5f6a6c1537ab46bd38e9df75 | 18,888 | 9.28% |
| 62f2ce9824813746ce6f5140 | 12,571 | 6.18% |
| 6054db3395b70198148b456e | 11,776 | 5.79% |
| 622107b40ac81503e0369ca6 | 11,594 | 5.70% |
| 5edff313902ad5580e9c3f30 | 10,569 | 5.19% |
| 63bc98a7519adc105105a990 | 10,313 | 5.07% |
| 6351b7d1a036663f05239b60 | 8,897 | 4.37% |

*Top 4 locations account for 47.1% of all orders.*

#### ITEM_PRICE Percentiles
| P1 | P5 | P10 | P25 | P50 | P75 | P90 | P95 | P99 |
|----|-----|-----|-----|-----|-----|-----|-----|-----|
| $1.59 | $2.39 | $3.89 | $5.89 | $8.00 | $10.00 | $11.99 | $13.98 | $26.67 |

- **Zeros**: 156 rows ($0 items - likely freebies/promos)
- **Negatives**: 0
- **Extreme outliers**: 590 rows with price > $50 (see Section 5.1)

#### ITEM_QUANTITY Distribution
| Qty | Count | % |
|-----|-------|---|
| 1 | 189,868 | 93.29% |
| 2 | 10,335 | 5.08% |
| 3 | 1,747 | 0.86% |
| 4 | 728 | 0.36% |
| 5 | 457 | 0.22% |
| 6-10 | 299 | 0.15% |
| 11-50 | 68 | 0.03% |
| 51-500 | 16 | 0.01% |
| 0 | 1 | 0.00% |

---

### 2.2 order_item_options.csv (193,017 rows, 6 columns)

| # | Column | Data Type | Non-null % | Null/Empty | Distinct | Notes |
|---|--------|-----------|------------|------------|----------|-------|
| 1 | ORDER_ID | string (ObjectId) | 100.00% | 0 | 78,614 | FK to order_items |
| 2 | LINEITEM_ID | string (ObjectId) | 100.00% | 0 | 102,712 | FK to order_items |
| 3 | OPTION_GROUP_NAME | string | 100.00% | 0 | 133 | Option category |
| 4 | OPTION_NAME | string | 100.00% | 0 | 637 | Specific choice |
| 5 | OPTION_PRICE | decimal | 100.00% | 0 | 16 | Min: $0, Max: $8 |
| 6 | OPTION_QUANTITY | integer | 100.00% | 0 | 1 | **ALWAYS 1 - zero variance** |

#### OPTION_PRICE Distribution (16 distinct values)
| Price | Count | % | Type |
|-------|-------|---|------|
| $0.00 | 127,980 | 66.31% | Free/included |
| $1.00 | 37,988 | 19.68% | Paid add-on |
| $2.00 | 19,619 | 10.16% | Paid add-on |
| $0.60 | 2,670 | 1.38% | Paid add-on |
| $0.75 | 1,565 | 0.81% | Paid add-on |
| $0.50 | 1,414 | 0.73% | Paid add-on |
| $3.00 | 1,368 | 0.71% | Paid add-on |
| Other ($0.05-$8) | 413 | 0.21% | Misc |

**Key finding**: 66.3% of all options are FREE ($0). 33.7% are paid add-ons.
**Zero negative prices** - discount detection via negative prices is NOT possible.

#### Top 15 OPTION_GROUP_NAME
| Option Group | Count | % |
|-------------|-------|---|
| Milk Options | 25,841 | 13.39% |
| Bacon Egg And Cheese Options | 14,760 | 7.65% |
| Breakfast Burrito Options | 12,219 | 6.33% |
| Chipotle Turkey Options | 9,813 | 5.08% |
| Smoothie Add-ons | 6,815 | 3.53% |
| Drip Coffee Options | 6,555 | 3.40% |
| BLAT Options | 5,928 | 3.07% |
| Chop Salad Options | 5,926 | 3.07% |
| Syrup | 5,426 | 2.81% |
| Bagel Choice | 5,417 | 2.81% |
| Vegetables | 5,225 | 2.71% |
| Espresso Options | 5,197 | 2.69% |
| SW Sausage Egg & Cheese Options | 5,009 | 2.60% |
| Cracked Oats Oatmeal Options | 4,991 | 2.59% |
| Bowl Protein Option | 4,680 | 2.42% |

---

### 2.3 date_dim.csv (365 rows, 8 columns)

| # | Column | Data Type | Non-null % | Distinct | Notes |
|---|--------|-----------|------------|----------|-------|
| 1 | date_key | string (DD-MM-YYYY) | 100% | 365 | Jan 1 - Dec 31, 2023 |
| 2 | year | integer | 100% | 1 | Always 2023 |
| 3 | month | integer | 100% | 12 | 1-12 |
| 4 | week | integer | 100% | 52 | 1-52 |
| 5 | day_of_week | string | 100% | 7 | Full names (Monday-Sunday) |
| 6 | is_weekend | boolean | 100% | 2 | 260 weekdays, 105 weekend days |
| 7 | is_holiday | boolean | 100% | 2 | 12 holidays |
| 8 | holiday_name | string | 3.3% | 12 | Empty for non-holidays |

#### Holidays in date_dim
| Date | Holiday |
|------|---------|
| 01-01-2023 | New Year's Day |
| 02-01-2023 | New Year's Day Observed |
| 16-01-2023 | Martin Luther King Jr. |
| 20-02-2023 | Washington's Birthday |
| 29-05-2023 | Memorial Day |
| 19-06-2023 | Juneteenth |
| 04-07-2023 | Indep. Day |
| 04-09-2023 | Labor Day |
| 09-10-2023 | Columbus Day |
| 11-11-2023 | Veterans Day |
| 23-11-2023 | Thanksgiving Day |
| 25-12-2023 | Christmas |

---

## 3. Entity Relationships & Join Integrity

### 3.1 Entity Relationship Diagram (Text)

```
┌──────────────────────┐         ┌──────────────────────────┐
│    order_items        │         │   order_item_options      │
│                      │         │                          │
│  PK: LINEITEM_ID     │ 1───M   │  FK: ORDER_ID            │
│  FK: ORDER_ID        │────────>│  FK: LINEITEM_ID         │
│  FK: USER_ID         │         │  OPTION_GROUP_NAME       │
│  FK: RESTAURANT_ID   │         │  OPTION_NAME             │
│  CREATION_TIME_UTC   │         │  OPTION_PRICE            │
│  ...                 │         │  OPTION_QUANTITY (=1)    │
└──────────┬───────────┘         └──────────────────────────┘
           │
           │ (date extracted)
           │ M───1
           ▼
┌──────────────────────┐
│    date_dim           │
│                      │
│  PK: date_key        │
│  (DD-MM-YYYY)        │
│  year, month, week   │
│  day_of_week         │
│  is_weekend          │
│  is_holiday          │
│  holiday_name        │
└──────────────────────┘
```

### 3.2 Join Integrity Validation

| Check | Result | Status |
|-------|--------|--------|
| **LINEITEM_ID uniqueness** (order_items) | 203,519 unique / 203,519 rows | PASS |
| **Exact duplicate rows** (order_items) | 0 duplicates | PASS |
| **ORDER_ID consistency** - same user per order | 0 orders with multiple USER_IDs | PASS |
| **ORDER_ID consistency** - same restaurant per order | 0 orders with multiple RESTAURANT_IDs | PASS |
| **ORDER_ID consistency** - same timestamp per order | 0 orders with multiple timestamps | PASS |
| **Orphan options** (in options, not in items) | 28 rows (0.01%) across 14 ORDER_IDs | WARNING |
| **Orders with no options** (in items, not in options) | 52,728 orders (40.1%) | INFO |
| **Orders in both tables** | 78,600 orders (59.9%) | INFO |

### 3.3 Cardinality Analysis

**Orders to Line Items** (order_items):
| Items/Order | Orders | % |
|-------------|--------|---|
| 1 item | 84,619 | 64.4% |
| 2 items | 31,613 | 24.1% |
| 3 items | 9,239 | 7.0% |
| 4-5 items | 4,846 | 3.7% |
| 6-10 items | 950 | 0.7% |
| 11-61 items | 61 | 0.0% |

Average: **1.55 items/order**. Median: **1**. Max: **61**.

**Options per Line Item** (order_item_options):
| Options/Item | Line Items | % |
|-------------|------------|---|
| 1 option | 51,139 | 49.8% |
| 2 options | 32,239 | 31.4% |
| 3 options | 11,544 | 11.2% |
| 4 options | 4,092 | 4.0% |
| 5 options | 1,946 | 1.9% |
| 6-7 options | 891 | 0.9% |
| 8+ options | 861 | 0.8% |

Average: **1.88 options/item**. Median: **2**. Max: **152** (outlier).

---

## 4. Customer & Business Analysis

### 4.1 Customer Distribution

| Metric | Value |
|--------|-------|
| Unique customers (with USER_ID) | **20,174** |
| Anonymous orders (no USER_ID) | **17,808 rows (8.75%)** |
| Loyalty card holders | 5,866 unique cards |
| Loyalty order rate | 22.64% of all rows |

**Orders Per Customer:**
| Orders | Customers | % | Cumulative |
|--------|-----------|---|------------|
| 1 (one-time) | 10,093 | **50.0%** | 50.0% |
| 2 | 2,978 | 14.8% | 64.8% |
| 3-5 | 3,196 | 15.8% | 80.6% |
| 6-10 | 1,735 | 8.6% | 89.2% |
| 11-20 | 1,125 | 5.6% | 94.8% |
| 21-50 | 751 | 3.7% | 98.5% |
| 51-100 | 207 | 1.0% | 99.6% |
| 101+ | 89 | 0.4% | 100.0% |

**Mean**: 5.9 orders/customer. **Median**: 1. **Max**: 2,124.

### 4.2 Loyalty Data Consistency (VALIDATED)

| Combination | Count | Status |
|-------------|-------|--------|
| IS_LOYALTY=TRUE + has PRINTED_CARD | 46,084 | Consistent |
| IS_LOYALTY=TRUE + no card | 0 | Consistent |
| IS_LOYALTY=FALSE + has card | 0 | Consistent |
| IS_LOYALTY=FALSE + no card | 157,435 | Consistent |

**IS_LOYALTY and PRINTED_CARD_NUMBER are perfectly correlated.** No inconsistencies.

### 4.3 Anonymous Order Recovery via Card Number

| Scenario | Count |
|----------|-------|
| Anonymous rows WITH card number | 28 (0.2%) |
| Anonymous rows WITHOUT card number | 17,780 (99.8%) |

**Card number is NOT a viable fallback for missing USER_IDs.** Only 28 of 17,808 anonymous rows have a card.

Additionally, 7 card numbers map to multiple USER_IDs - cards are NOT unique per customer.

### 4.4 DEVELOPMENT App Deep Dive

| Metric | Value |
|--------|-------|
| Total rows | 826 |
| Restaurant IDs | 1 (6050e76361e498ca740bba6f) |
| Date range | 2021-03-16 to 2024-02-12 |
| Top category | Breakfast (519 rows, 62.8%) |
| Loyalty orders | Not tracked |

Single test restaurant used across 3 years. **Must filter out entirely.**

### 4.5 Neighborhood Perks App Deep Dive

| Metric | Value |
|--------|-------|
| Total rows | 1,270 |
| Restaurant IDs | 3 locations |
| Date range | 2021-07-06 to 2023-03-13 |
| Loyalty rate | **0%** (none are loyalty) |

Separate program from main Alltown Fresh. Zero loyalty overlap.

### 4.6 Time-of-Day Pattern (UTC)

```
Hour    Orders     %   Distribution
00:00      691   0.3%  |
01:00      684   0.3%  |
02:00      616   0.3%  |
03:00      440   0.2%  |
04:00      424   0.2%  |
05:00    1,345   0.7%  ||
06:00    1,334   0.7%  ||
07:00    2,003   1.0%  ||||
08:00    1,030   0.5%  ||
09:00    2,387   1.2%  ||||
10:00    8,237   4.0%  ||||||||||||||||
11:00   15,306   7.5%  ||||||||||||||||||||||||||||||
12:00   19,671   9.7%  ||||||||||||||||||||||||||||||||||||||
13:00   21,387  10.5%  ||||||||||||||||||||||||||||||||||||||||||
14:00   21,635  10.6%  ||||||||||||||||||||||||||||||||||||||||||
15:00   28,951  14.2%  ||||||||||||||||||||||||||||||||||||||||||||||||||||||||
16:00   30,714  15.1%  ||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
17:00   20,819  10.2%  ||||||||||||||||||||||||||||||||||||||||||||
18:00   11,656   5.7%  ||||||||||||||||||||||||||
19:00    6,533   3.2%  ||||||||||||
20:00    3,559   1.7%  |||||||
21:00    2,190   1.1%  ||||
22:00    1,188   0.6%  ||
23:00      719   0.4%  |
```

**Peak hours (UTC)**: 15:00-16:00 (3-4 PM). Note: UTC times - actual local peak depends on timezone (likely EST, so 10-11 AM local = lunch rush).

### 4.7 Order Volume Over Time

| Year | Orders | % | YoY Growth |
|------|--------|---|------------|
| 2020 | 10,226 | 5.0% | - (launch Apr) |
| 2021 | 43,478 | 21.4% | +325% |
| 2022 | 60,066 | 29.5% | +38% |
| 2023 | 80,664 | 39.6% | +34% |
| 2024 | 9,085 | 4.5% | Partial (Jan-Feb) |

---

## 5. Data Quality Issues (Comprehensive)

### 5.1 CRITICAL: Price & Quantity Outliers

**590 rows** with ITEM_PRICE > $50. **84 rows** with ITEM_QUANTITY > 10.

**Top outliers by extended price:**

| Price | Qty | Extended ($) | Item | Category | App |
|-------|-----|-------------|------|----------|-----|
| $5,000 | 500 | $2,500,000 | Korean Kimchi | Bowls | Alltown Fresh |
| $3,567 | 300 | $1,070,100 | Chili Chicken Bowl | Bowls | Alltown Fresh |
| $3,500 | 500 | $1,750,000 | Meet Your Matcha 12oz | Smoothies | Alltown Fresh |
| $2,967 | 300 | $890,100 | B.L.A.T | Sandwiches | Alltown Fresh |
| $1,917 | 213 | $408,321 | Tuscan Sun | Salads | Alltown Fresh |
| $1,403 | 118 | $165,556 | Carolina Pulled Pork | Sandwiches | Alltown Fresh |
| $1,125 | 500 | $562,500 | Espresso Double | Espresso | Alltown Fresh |
| $1,001 | 1 | $1,001 | Dora's Chicken Picatta | Bowls | DEVELOPMENT |

**Revenue Impact:**

| Metric | Value |
|--------|-------|
| Total raw revenue (all rows) | **$9,933,754.69** |
| Clean revenue (price<=50, qty<=20, no dev) | **$2,062,659.41** |
| Outlier revenue removed | **$7,871,095.28 (79.2%)** |
| Rows removed | 1,412 (0.69%) |
| Rows kept | 202,107 (99.31%) |

**Less than 1% of rows account for 79% of raw revenue.** These outliers will destroy every metric if not filtered.

**Clean revenue by year:**
| Year | Revenue | Orders | Avg $/Order |
|------|---------|--------|-------------|
| 2020 | $113,572 | 8,065 | $14.08 |
| 2021 | $449,850 | 26,634 | $16.89 |
| 2022 | $571,406 | 37,447 | $15.26 |
| 2023 | $834,808 | 51,988 | $16.06 |
| 2024 | $93,023 | 6,157 | $15.11 |

Avg order value is consistently **$14-17** after cleaning.

### 5.2 CRITICAL: No Discount Data

The requirements document states: "Use option_price < 0 to detect discounts."

**VERIFIED FACT: Zero negative option prices exist in the entire dataset.**
- All 193,017 OPTION_PRICE values are >= $0
- Range: $0.00 to $8.00
- 16 distinct price points only

**Decision**: Pivot to **Pricing & Upsell Analysis** - compare free ($0, 66.3%) vs paid add-on options (33.7%).

### 5.3 CRITICAL: Anonymous Orders (8.75%)

- 17,808 rows across unknown number of orders have no USER_ID
- Card number fallback recovers only 28 rows (0.2%) - NOT viable
- **Cannot be used for**: CLV, RFM segmentation, churn analysis
- **Can be used for**: Sales trends, location performance, category analysis

### 5.4 MODERATE: Dirty Category Names

| Dirty Value | Clean Value | Count | Issue Type |
|-------------|-------------|-------|------------|
| `Kids` | `Kid's` (or keep separate?) | 1,158 | Inconsistency |
| `BBQ Plateshttps://order.pxsweb.com/...` | `BBQ Plates` | 70 | URL embedded |
| `Bowls0` | `Bowls` | 52 | Trailing digit |
| `` Sandwiches`1 `` | `Sandwiches` | 30 | Backtick + digit |
| `Drip Chttps://www.opendining.net/...offee` | `Drip Coffee` | 26 | URL mid-word |
| `Sqalads` | `Salads` | 8 | Typo |
| `Kid'shttps://www.opendining.net/...` | `Kid's` | 2 | URL appended |
| `` (empty) | Unknown | 1 | Missing |
| `Test Items` | EXCLUDE | 1 | Test data |

After cleaning: **40 categories** (down from 45).

### 5.5 MODERATE: Test/Development Data

| Filter | Rows Affected | % |
|--------|---------------|---|
| APP_NAME = "Alltown Fresh - DEVELOPMENT" | 826 | 0.41% |
| ITEM_CATEGORY = "Test Items" | 1 | 0.00% |
| RESTAURANT_ID = 6050e76361e498ca740bba6f (dev only) | 826 | 0.41% |

All dev data maps to a single restaurant ID, active from 2021-2024.

### 5.6 MODERATE: Date Dimension Gap

- **date_dim covers**: Jan 1 - Dec 31, 2023 (365 rows)
- **Order data spans**: Apr 21, 2020 - Feb 21, 2024 (~1,402 days)
- **Decision**: Generate extended date_dim for full range with US federal holidays
- **Format note**: date_dim uses DD-MM-YYYY; orders use ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)

### 5.7 LOW: Option Group Name Inconsistencies

| Issue | Examples |
|-------|---------|
| Case inconsistency | "BOTTLE DEPOSIT" (17) vs "Bottle Deposit" (181) |
| CT-prefix duplicates | "Bacon Egg And Cheese Options" (14,760) vs "CT Bacon Egg And Cheese Options" (53) |
| Lowercase variants | "add" (144) vs "Add" (16) |
| Redundant grouping | "Shakshuka Options" (311) vs "Shakshuka" (70) |

133 option groups could likely be consolidated to ~100 after standardization.

### 5.8 LOW: Constant Column

- `OPTION_QUANTITY` = 1 for all 193,017 rows (zero variance)
- Include in revenue formula but provides no analytical insight

### 5.9 LOW: Zero-Price Items (156 rows)

By category: Breakfast (45), Sandwiches (30), Salads (19), Meal Prep Kits (15), etc.
Likely freebies, promotions, or comp items. Keep in dataset but flag.

### 5.10 LOW: Zero-Quantity Row (1 row)

Single row with ITEM_QUANTITY=0, ITEM_PRICE=$4.39, empty ITEM_NAME and ITEM_CATEGORY.
Contributes $0 revenue. Should be excluded.

---

## 6. Design Implications

### 6.1 For Revenue Calculation
```
Line Item Revenue = ITEM_PRICE * ITEM_QUANTITY
Option Revenue    = OPTION_PRICE * OPTION_QUANTITY  (but OPTION_QUANTITY is always 1)
Order Revenue     = SUM(all line item revenues) + SUM(all option revenues for those line items)
```
**Must apply**: price <= $50 filter, qty <= 20 filter, exclude DEVELOPMENT app, exclude qty=0 row.

### 6.2 For CLV (Primary Metric)
- Usable customer base: **20,174 unique USER_IDs**
- 50% are one-time buyers (CLV = single order value)
- Right-skewed distribution requires percentile-based tiers, not mean-based
- Clean avg order value: **~$15-17**

### 6.3 For RFM Segmentation
- Median frequency = 1 order - standard RFM quintile scoring will put 50% in lowest bucket
- Consider: log-transform frequency, or separate one-time vs repeat customers first

### 6.4 For Churn Detection
- With 50% one-time buyers, "churn" definition needs careful thought
- Suggestion: Only apply churn analysis to customers with >= 2 orders

### 6.5 For Pipeline Architecture
- 28 restaurant locations (small dimension table)
- ~131K unique orders, ~203K line items (manageable for PySpark)
- Data grows ~35% annually
- Need to extend date_dim from 365 to ~1,402 rows
- Date format conversion needed (DD-MM-YYYY vs ISO 8601)

---

## 7. Summary of Actions Required

| Priority | Action | Impact |
|----------|--------|--------|
| CRITICAL | Filter price outliers (>$50) and quantity outliers (>20) | 79% revenue distortion |
| CRITICAL | Exclude DEVELOPMENT app data (826 rows) | Clean analysis |
| CRITICAL | Handle anonymous orders (17,808 rows) in customer metrics | CLV accuracy |
| HIGH | Clean dirty category names (8 categories, ~1,347 rows) | Category analysis accuracy |
| HIGH | Generate extended date_dim (2020-04 to 2024-02) | Full date coverage |
| HIGH | Pivot discount analysis to upsell analysis ($0 vs paid options) | Dashboard #6 |
| MEDIUM | Standardize option group names (case, CT-prefix) | Option analysis |
| LOW | Flag zero-price items (156 rows) | Revenue completeness |
| LOW | Exclude zero-quantity row (1 row) | Data hygiene |

---

## 8. Next Steps

| Step | Task | Deliverable |
|------|------|-------------|
| **Step 2** | Deep analysis: define outlier thresholds, clean category mapping, revenue/customer/loyalty deep dive | `02_data_analysis.md` + scripts |
| **Step 3** | AWS pipeline architecture: SQL Server -> S3 medallion -> PySpark -> Athena | Architecture diagram + SDD |
| **Step 4** | Build PySpark ETL pipeline (bronze/silver/gold layers) | Pipeline code |
| **Step 5** | Calculate CLV, RFM, churn, sales, loyalty, location, upsell metrics | PySpark jobs |
| **Step 6** | Streamlit dashboard (6 views) | Dashboard app |
| **Step 7** | CI/CD, documentation, submission | Final package |
