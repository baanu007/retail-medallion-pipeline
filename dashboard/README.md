# Retail Restaurant Business Insights Dashboard

Streamlit dashboard for the 7 business metrics produced by the Glue pipeline.
Reads Gold Delta Lake tables directly from `s3://<BUCKET_NAME>/gold/`.

## Pages

| Page | Metric | Source Gold Table(s) |
|---|---|---|
| Home | KPI overview + hero CLV chart | multiple |
| 1 — CLV Snapshot | Daily CLV evolution (PRIMARY) | `gold_clv_snapshot` |
| 2 — RFM Segments | Recency/Frequency/Monetary segmentation | `gold_rfm_segments` |
| 3 — Churn Indicators | Repeat-customer churn risk | `gold_churn_indicators` |
| 4 — Sales Trends | Daily/weekly/monthly revenue rollups | `gold_sales_daily/weekly/monthly` |
| 5 — Loyalty Comparison | Loyalty vs non-loyalty vs anonymous | `gold_loyalty_comparison` |
| 6 — Location Performance | 27 restaurants ranked | `gold_location_perf` |
| 7 — Upsell Analysis | Paid vs free options (discount pivot) | `gold_upsell_summary`, `gold_upsell_top_options` |

## Prerequisites

- Python 3.10+
- AWS credentials for the `retail-chain` profile (`~/.aws/credentials`)
- The Gold layer must exist in S3 (run the Gold Glue jobs first — see
  `docs/sessions/session_20260411_gold_etls_built.md`)

## Install

```bash
cd dashboard
pip install -r requirements.txt
```

## Run

```bash
AWS_PROFILE=retail-chain streamlit run app.py
```

On Windows Git Bash:
```bash
AWS_PROFILE=retail-chain PYTHONIOENCODING=utf-8 streamlit run app.py
```

On Windows CMD:
```cmd
set AWS_PROFILE=retail-chain
streamlit run app.py
```

Streamlit will open http://localhost:8501 in your browser.

## How it works

- **Small tables** (dims, segment metrics) are loaded via the `deltalake`
  Python library directly as pandas DataFrames. Cached for 1 hour via
  `@st.cache_data`.
- **Large tables** (`gold_clv_snapshot` ~20M rows, `fact_orders` ~130K rows)
  are queried via DuckDB's `delta_scan()` extension — DuckDB pushes down
  filters and aggregations to the underlying parquet files, so even the
  20M-row CLV table responds in seconds.

Credentials are resolved via the standard boto3 / AWS CLI credential chain
(`AWS_PROFILE` → `~/.aws/credentials` → env vars → instance metadata).

## Troubleshooting

**"No credentials found"** — make sure `AWS_PROFILE=retail-chain` is set,
or that `aws sts get-caller-identity --profile retail-chain` returns an
identity.

**"Cannot load delta extension"** — DuckDB needs network access to download
the Delta extension on first run. Run `duckdb` interactively once and
execute `INSTALL delta; LOAD delta;` to cache it.

**CLV page is slow** — first load queries ~20M rows from S3. After the first
query, results are cached per session. Subsequent page refreshes are instant.

**"Failed to read parquet"** — if any Delta table has missing `_delta_log/`
files, re-run the relevant Glue job. Delta is idempotent so it's safe.
