"""
Data access layer for the GlobalPartners BI dashboard.

Reads Delta Lake tables from s3://globalpartners-aws/gold/ using two strategies:

1. `get_table(name)` — loads a full Gold Delta table as a pandas DataFrame
   via the `deltalake` Python library. Cached per session.  Appropriate for
   small tables (dims, segment/summary metrics).

2. `query_large(sql)` — runs a DuckDB SQL query against Gold tables using
   `delta_scan()` with predicate/aggregate pushdown. Appropriate for the
   two large tables (`gold_clv_snapshot` ≈ 20M rows, `fact_orders` ≈ 130K rows).
   In the SQL, reference tables by plain name — this function auto-rewrites
   them to delta_scan() paths.

AWS credentials are resolved via the standard boto3 credential chain:
  AWS_PROFILE env var → ~/.aws/credentials → env vars → instance metadata.
"""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Dict, Iterable

# Defensive: if SSL_CERT_FILE / REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE are set but
# point to nonexistent files, they break DuckDB's HTTP stack with an unhelpful
# error. Unset any that don't point to real files BEFORE importing duckdb.
for _ca_var in ("SSL_CERT_FILE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE"):
    _p = os.environ.get(_ca_var)
    if _p and not os.path.isfile(_p.strip("'\"")):
        os.environ.pop(_ca_var, None)

import duckdb
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

BUCKET = os.environ.get("GP_BUCKET", "globalpartners-aws")
REGION = os.environ.get("AWS_REGION", "us-east-1")
GOLD = f"s3://{BUCKET}/gold"

# Tables small enough to fully materialize as pandas DataFrames
SMALL_TABLES = (
    "dim_date",
    "dim_customer",
    "dim_restaurant",
    "dim_menu_item",
    "gold_rfm_segments",
    "gold_churn_indicators",
    "gold_sales_daily",
    "gold_sales_weekly",
    "gold_sales_monthly",
    "gold_loyalty_comparison",
    "gold_location_perf",
    "gold_upsell_summary",
    "gold_upsell_top_options",
)

# Tables large enough that we query via DuckDB delta_scan() pushdown
LARGE_TABLES = (
    "fact_orders",
    "gold_clv_snapshot",
)

ALL_TABLES = SMALL_TABLES + LARGE_TABLES


def _storage_options() -> Dict[str, str]:
    opts = {
        "AWS_REGION": REGION,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }
    if os.environ.get("AWS_PROFILE"):
        opts["AWS_PROFILE"] = os.environ["AWS_PROFILE"]
    return opts


# ─────────────────────────────────────────────
# Small-table loader (deltalake → pandas)
# ─────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner="Loading Gold table from S3…")
def get_table(name: str) -> pd.DataFrame:
    """Load a Gold Delta table as a full pandas DataFrame."""
    if name not in ALL_TABLES:
        raise ValueError(f"Unknown table: {name}. Known: {ALL_TABLES}")
    dt = DeltaTable(f"{GOLD}/{name}", storage_options=_storage_options())
    return dt.to_pandas()


# ─────────────────────────────────────────────
# Large-table query helper (DuckDB delta_scan)
# ─────────────────────────────────────────────

@st.cache_resource(show_spinner="Loading gold_clv_snapshot from S3 (one-time ~60s, then cached)…")
def _load_clv_pyarrow():
    """Load the full CLV snapshot as a pyarrow Table.
    Cached for the lifetime of the Streamlit process (cache_resource)."""
    dt = DeltaTable(f"{GOLD}/gold_clv_snapshot", storage_options=_storage_options())
    return dt.to_pyarrow_table()


@st.cache_resource(show_spinner="Loading fact_orders from S3…")
def _load_fact_pyarrow():
    """Load the full fact_orders as a pyarrow Table."""
    dt = DeltaTable(f"{GOLD}/fact_orders", storage_options=_storage_options())
    return dt.to_pyarrow_table()


@st.cache_resource
def _duckdb_con():
    """Cached in-process DuckDB connection with the large Gold tables registered
    as pyarrow-backed views. All S3 I/O is handled by the `deltalake` library
    (which works reliably with AWS SDK); DuckDB only does in-memory SQL."""
    con = duckdb.connect(":memory:")
    con.register("gold_clv_snapshot", _load_clv_pyarrow())
    con.register("fact_orders", _load_fact_pyarrow())
    return con


@st.cache_data(ttl=3600, show_spinner="Running query…")
def query_large(sql: str) -> pd.DataFrame:
    """Run a SQL query against the registered large Gold tables
    (gold_clv_snapshot, fact_orders). In-memory execution, no S3 traffic."""
    con = _duckdb_con()
    return con.execute(sql).df()
