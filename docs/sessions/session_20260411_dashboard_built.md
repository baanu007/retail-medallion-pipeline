# Session Report — April 11, 2026 (Dashboard)
## Streamlit Dashboard: Built, Deps Installed, All 8 Pages Verified

---

## TL;DR

Built a 8-page Streamlit dashboard (1 home + 7 metric pages) that reads Gold
Delta Lake tables directly from S3. Installed deps, smoke-tested the data
loader, then used Streamlit's `AppTest` framework to programmatically exercise
every page with real data. **All 8 pages render without runtime errors.**

### Pages implemented

| # | Page | Business Metric | Gold Table(s) |
|---|---|---|---|
| 0 | **Home** | KPI overview + hero CLV chart | multiple |
| 1 | **CLV Snapshot** (PRIMARY) | Daily CLV evolution | `gold_clv_snapshot` |
| 2 | **RFM Segments** | Recency/Frequency/Monetary segmentation | `gold_rfm_segments` |
| 3 | **Churn Indicators** | Repeat-customer churn risk | `gold_churn_indicators` |
| 4 | **Sales Trends** | Daily/weekly/monthly revenue rollups | `gold_sales_daily/weekly/monthly` |
| 5 | **Loyalty Comparison** | Loyalty vs non-loyalty vs anonymous | `gold_loyalty_comparison` |
| 6 | **Location Performance** | 27 restaurants ranked | `gold_location_perf` |
| 7 | **Upsell Analysis** | Paid vs free options (discount pivot) | `gold_upsell_summary`, `gold_upsell_top_options` |

---

## 1. Architecture

```
┌──────────────────────┐
│   Streamlit app      │
│   (8 pages)          │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐       ┌──────────────────────┐
│  lib/data.py         │       │  DuckDB (in-memory)  │
│  @st.cache_data      │◀─────▶│  Runs SQL over       │
│  @st.cache_resource  │       │  pyarrow-registered  │
└──────────┬───────────┘       │  tables              │
           │                    └──────────────────────┘
           ▼
┌──────────────────────┐
│  deltalake lib       │
│  (Rust-backed        │
│   DeltaTable reader) │
└──────────┬───────────┘
           │ S3 API (AWS SDK)
           ▼
┌──────────────────────────────┐
│  s3://<BUCKET_NAME>/    │
│  gold/ — 15 Delta tables     │
└──────────────────────────────┘
```

### Two-tier data access strategy

**Tier 1 — Small tables via `deltalake` → pandas**:
Loaded full-table as pandas DataFrame, cached for 1 hour via
`@st.cache_data(ttl=3600)`. Used for all dims, RFM, churn, loyalty,
location_perf, sales (daily/weekly/monthly), upsell summary/top_options.

**Tier 2 — Large tables via `deltalake` → pyarrow → DuckDB**:
The two heavyweight tables — `gold_clv_snapshot` (~10.8M rows, ~1 GB in memory)
and `fact_orders` (~130K rows) — are loaded as pyarrow Tables via the
`deltalake` library, then registered with a cached DuckDB in-memory connection
(`@st.cache_resource`). Pages query them via `query_large(sql)` — SQL
execution is purely in-memory (no S3 traffic), so aggregate queries over
10.8M rows complete in ~0.5s after the one-time load.

**Why not DuckDB's native `delta_scan()` extension?**
Tried it first — it works against the CLV table in ~4s with pushdown — but
the DuckDB delta extension hit an SSL error on the dev machine because of a
stale `SSL_CERT_FILE` env var left over from another project. The `deltalake`
Python library uses a different HTTP stack and isn't affected. Swapped to the
deltalake-lib approach for robustness; defensive code in `lib/data.py` also
unsets any broken CA-bundle env vars at import time for anyone else with the
same config.

---

## 2. Files Created

```
dashboard/
├── app.py                          190 lines — Home/overview
├── pages/
│   ├── 1_CLV_Snapshot.py          135 lines — PRIMARY metric: daily CLV + individual trajectories
│   ├── 2_RFM_Segments.py           90 lines — segment distribution + R×F heatmap + scatter
│   ├── 3_Churn_Indicators.py      105 lines — churn tag distribution + recency/spend scatter
│   ├── 4_Sales_Trends.py          105 lines — daily/weekly/monthly trend with restaurant filter
│   ├── 5_Loyalty_Comparison.py     95 lines — 3-segment comparison + repeat rate chart
│   ├── 6_Location_Performance.py  115 lines — 27-restaurant leaderboard + AOV scatter
│   └── 7_Upsell_Analysis.py       105 lines — free vs paid donut + top-options bar + scatter
├── lib/
│   ├── __init__.py
│   └── data.py                    125 lines — cached loader + defensive CA-bundle cleanup
├── requirements.txt                 7 lines
└── README.md                       60 lines — setup + run + troubleshooting
```

---

## 3. Key Design Decisions

### 3.1 Plotly for charts
Used `plotly.express` and `plotly.graph_objects` for interactive charts
(hover tooltips, zoom, pan). Streamlit's built-in `st.line_chart` was too
limited for the metric pages.

### 3.2 Caching strategy
- `@st.cache_data(ttl=3600)`: for pandas DataFrames returned by `get_table()`.
  Cached per function-call signature, expires after 1 hour.
- `@st.cache_resource`: for the heavy pyarrow tables and DuckDB connection.
  Cached for the process lifetime (never expires until restart).

### 3.3 Hero chart on home page
The home page exercises `query_large()` once for the CLV aggregate by tier
over time. This pre-warms the CLV cache so that clicking through to the
detailed CLV page is instant.

### 3.4 Decimal column coercion
Delta Lake's `DECIMAL(10,2)` type maps to pandas `object` columns containing
Python `Decimal` instances. Plotly's chart builders don't coerce these to
float for numeric properties (e.g. `size=`), throwing `ValueError: Input
value is not numeric`. Fix: pages that use these columns in chart aesthetics
cast them explicitly to float.

### 3.5 Customer picker on CLV page
The CLV "individual trajectory" view offers a dropdown of the top-N customers
ranked by total spend (user can slide N from 10 to 200). Selected customer's
user_id is fed into a filtered DuckDB query that returns only that customer's
~1,400-day trajectory. This works because `gold_clv_snapshot` is partitioned
by `snapshot_year` and the filter pushes down.

### 3.6 Navigation
Streamlit auto-discovers files in `pages/` and orders them by the numeric
prefix. No routing code needed.

---

## 4. Smoke Testing Methodology

Four progressively deeper tests:

### 4.1 Dependency import check
```python
python -c "import streamlit, plotly, pandas, pyarrow, deltalake, duckdb, boto3"
```
Confirmed all 7 deps importable. Had to `pip install deltalake duckdb` (the
other 5 were already present).

### 4.2 deltalake → pandas on a small table
```python
dt = DeltaTable('s3://<BUCKET_NAME>/gold/dim_restaurant', storage_options=...)
df = dt.to_pandas()  # 27 rows — works
```

### 4.3 deltalake → pyarrow on the CLV table + DuckDB SQL
```python
tbl = DeltaTable(f'{GOLD}/gold_clv_snapshot').to_pyarrow_table()
# 10,797,593 rows, 1089 MB, loaded in 7.5s
con.register('gold_clv_snapshot', tbl)
con.execute('SELECT ... GROUP BY date_key, clv_tier ...').df()
# 4,187 rows in 0.46s
```

### 4.4 Full page rendering via Streamlit `AppTest`
```python
from streamlit.testing.v1 import AppTest
at = AppTest.from_file('pages/1_CLV_Snapshot.py', default_timeout=120)
at.run()
errors = list(at.exception)
```
This is the real test — it runs each page top-to-bottom with real data.

**Results (first run)**: 6/8 pages OK, 2 errored on Plotly's `size=` param
rejecting pandas Decimal objects. Fixed by adding `astype(float)` on the
relevant columns in those pages.

**Results (second run, after fix)**: 8/8 pages OK.

| Page | First-run status | Runtime |
|---|---|---|
| Home | OK | 20.4s (first load, includes CLV materialization) |
| CLV Snapshot | OK | 3.7s |
| RFM Segments | OK | 1.6s |
| Churn Indicators | OK | 1.3s |
| Sales Trends | OK | 4.1s |
| Loyalty Comparison | OK | 0.1s |
| Location Performance | ERROR → fixed | 1.3s → 0.9s |
| Upsell Analysis | ERROR → fixed | 0.9s → 1.1s |

Home page first-load is 20s because it materializes the 10.8M-row CLV table.
Subsequent page loads reuse the cached pyarrow table and are sub-second.

---

## 5. How to Run

From the project root:

```bash
cd dashboard
pip install -r requirements.txt
AWS_PROFILE=retail-chain streamlit run app.py
```

Open <http://localhost:8501> in your browser.

First page load takes ~20 seconds (materializing the CLV snapshot from S3).
After that, every page is sub-second.

---

## 6. Known Quirks (Documented in README)

1. **Stale `SSL_CERT_FILE` env var**: If you have an `SSL_CERT_FILE`,
   `REQUESTS_CA_BUNDLE`, or `CURL_CA_BUNDLE` env var pointing to a
   nonexistent file (from some earlier Python project), it can break
   HTTPS for boto3. `lib/data.py` defensively unsets these at import time
   if they reference missing files. The Rust-backed deltalake client still
   logs a warning about "no native root CA certificates found" but the
   actual S3 requests succeed (it falls back to WebPKI).

2. **CLV first load is heavy**: 10.8M rows materialized into ~1 GB pyarrow.
   Laptop needs ~2 GB free RAM. Pre-aggregating into a smaller
   `gold_clv_daily_stats` Gold job would cut this to ~4K rows — left for
   future enhancement, not blocking for the assessment.

3. **Plotly Decimal coercion**: Delta Lake DECIMAL columns must be cast to
   float before being used as Plotly size/color aesthetics. Two pages
   (Location, Upsell) apply this cast explicitly after loading. Kept as
   per-page fix rather than global because only these two pages hit it.

---

## 7. Next Steps

### Immediate
- (Optional) Add a `gold_clv_daily_stats` pre-aggregation Glue job to avoid
  the 20s first-load — not required for assessment submission but would
  improve UX significantly.

### Step 7 — CI/CD + Submission
1. Build Glue Workflow chaining all 18 pipeline jobs:
   `bronze-ingest-all-tables → [3 silver jobs parallel] → [4 dim jobs parallel] → fact_orders → [9 metric jobs parallel] → alert-on-fail`
2. EventBridge daily 2 AM UTC trigger on the workflow
3. Final documentation polish
4. Video/presentation walk-through of the full pipeline + dashboard
5. Git commit + submission

### Streamlit deployment (if needed)
- Current setup is local-run only. Options for hosting:
  - Streamlit Community Cloud (free, public)
  - AWS App Runner / ECS (private)
  - Keep local-only (run from laptop during demo)
- For the assessment, local-only is fine.

---

## 8. Meta-lessons

1. **`streamlit.testing.v1.AppTest` is the right way to smoke-test pages
   without a browser.** Runs the page top-to-bottom with real data,
   captures exceptions. Infinitely better than manually clicking through
   or using Selenium.

2. **The `deltalake` Python library is more reliable than DuckDB's delta
   extension** on non-standard dev environments. deltalake has been
   maintained longer, has a simpler I/O model, and its S3 client is more
   forgiving of weird env vars.

3. **Pre-warming caches on the home page matters**: by doing the CLV
   aggregate on the home page, the 20-second materialization happens once
   when the user naturally enters the app, not when they click "CLV" and
   wonder why it's slow.

4. **Delta Lake DECIMAL → pandas Decimal → Plotly incompatibility is a
   common trap.** Any dashboard consuming Delta tables should cast decimal
   columns to float before charting.

5. **The 17-jobs-with-zero-debugging pattern continued here**: the dashboard
   needed two small fixes (Decimal cast), but both were caught by AppTest
   before any manual testing. The tight loop of `write code → AppTest →
   fix → AppTest → ship` is very fast.
