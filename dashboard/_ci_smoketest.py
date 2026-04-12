"""Local mirror of the CI workflow's AppTest block.

Runs all 8 Streamlit pages with a mocked data loader (no AWS access).
Used to verify the CI workflow logic BEFORE pushing to GitHub.

This file is tracked by git so it stays in sync with the CI workflow —
if you change the mock data here, update .github/workflows/ci.yml to match.
"""

import sys, os, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, ".")

import pandas as pd
from unittest.mock import patch

SAMPLE_TABLES = {
    "dim_customer": pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "first_order_date": pd.to_datetime(["2023-01-01"] * 3),
        "last_order_date":  pd.to_datetime(["2023-06-01"] * 3),
        "tenure_days": [150, 100, 50],
        "total_orders": [5, 3, 1],
        "total_spend": [250.0, 120.0, 30.0],
        "avg_order_value": [50.0, 40.0, 30.0],
        "is_loyalty": [True, False, False],
        "primary_restaurant_id": ["r1"] * 3,
        "clv_tier": ["High", "Medium", "Low"],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 3,
    }),
    "gold_rfm_segments": pd.DataFrame({
        "user_id": ["u1", "u2", "u3"],
        "is_loyalty": [True, False, False],
        "recency_days": [10, 50, 200],
        "frequency": [10, 3, 1],
        "monetary": [500.0, 120.0, 30.0],
        "r_score": [5, 3, 1],
        "f_score": [5, 3, 1],
        "m_score": [5, 3, 1],
        "rfm_score": ["555", "333", "111"],
        "segment": ["VIP", "Regular", "Churn Risk"],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 3,
    }),
    "gold_churn_indicators": pd.DataFrame({
        "user_id": ["u1", "u2"],
        "total_orders": [5, 3],
        "first_order_date": pd.to_datetime(["2023-01-01"] * 2),
        "last_order_date":  pd.to_datetime(["2023-12-01"] * 2),
        "recent_spend": [50.0, 30.0],
        "prior_avg_spend": [45.0, 40.0],
        "avg_gap_days": [30.0, 20.0],
        "days_since_last_order": [15, 60],
        "churn_tag": ["Active", "At Risk"],
        "churn_tag_personal": ["Active", "Inactive"],
        "spend_change_pct": [11.1, -25.0],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 2,
    }),
    "gold_loyalty_comparison": pd.DataFrame({
        "segment": ["Loyalty", "Non-Loyalty", "Anonymous"],
        "customer_count": [5000, 15000, 0],
        "order_count": [30000, 90000, 12000],
        "total_revenue": [450000.0, 1400000.0, 180000.0],
        "avg_spend_per_order": [15.0, 15.5, 15.2],
        "avg_orders_per_customer": [6.0, 6.0, None],
        "avg_customer_spend": [90.0, 93.0, None],
        "repeat_customer_rate_pct": [60.0, 47.0, None],
        "option_attach_rate_pct": [32.0, 33.0, 30.0],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 3,
    }),
    "gold_location_perf": pd.DataFrame({
        "restaurant_id": ["r1", "r2"],
        "total_orders": [20000, 10000],
        "unique_customers": [5000, 2500],
        "total_revenue": [300000.0, 150000.0],
        "avg_order_value": [15.0, 15.0],
        "active_weeks": [200, 150],
        "first_order_date": pd.to_datetime(["2020-01-01"] * 2),
        "last_order_date":  pd.to_datetime(["2024-01-01"] * 2),
        "orders_per_week": [100.0, 66.7],
        "revenue_per_week": [1500.0, 1000.0],
        "loyalty_order_pct": [25.0, 20.0],
        "repeat_customer_count": [2500, 1200],
        "retention_rate_pct": [50.0, 48.0],
        "revenue_rank": [1, 2],
        "orders_rank": [1, 2],
        "customers_rank": [1, 2],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 2,
    }),
    "gold_upsell_summary": pd.DataFrame([{
        "total_option_rows": 190000,
        "free_option_rows": 125000,
        "paid_option_rows": 65000,
        "free_option_pct": 65.9,
        "paid_option_pct": 34.1,
        "total_option_revenue": 85000.0,
        "total_orders": 130000,
        "orders_with_paid_options": 42000,
        "order_upsell_rate_pct": 32.3,
        "total_order_revenue": 2146000.0,
        "upsell_share_of_revenue_pct": 3.97,
        "avg_option_revenue_per_upsell_order": 2.0,
        "gold_ingestion_ts": "2026-04-11T00:00:00Z",
    }]),
    "gold_upsell_top_options": pd.DataFrame({
        "option_group_name": ["Breakfast Burrito Options", "Smoothie Add-ons"],
        "option_name": ["Add Smokehouse Bacon", "Add Protein"],
        "attach_count": [1900, 2700],
        "total_revenue": [3400.0, 2700.0],
        "avg_price": [1.8, 1.0],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 2,
    }),
    "gold_sales_daily": pd.DataFrame({
        "order_date": pd.to_datetime(["2023-01-01", "2023-01-02"]),
        "restaurant_id": ["r1", "r1"],
        "item_category": ["Breakfast", "Lunch"],
        "order_count": [100, 80],
        "unique_customers": [90, 75],
        "total_item_revenue": [1500.0, 1200.0],
        "loyalty_line_items": [20, 15],
        "avg_order_revenue": [15.0, 15.0],
        "snapshot_year": [2023, 2023],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 2,
    }),
    "gold_sales_weekly": pd.DataFrame({
        "order_year": [2023, 2023],
        "order_week": [1, 2],
        "restaurant_id": ["r1", "r1"],
        "order_count": [500, 550],
        "unique_customers": [450, 500],
        "total_revenue": [7500.0, 8250.0],
        "avg_order_value": [15.0, 15.0],
        "loyalty_orders": [100, 120],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 2,
    }),
    "gold_sales_monthly": pd.DataFrame({
        "order_year": [2023, 2023],
        "order_month": [1, 2],
        "order_year_month": ["2023-01", "2023-02"],
        "restaurant_id": ["r1", "r1"],
        "order_count": [2000, 2200],
        "unique_customers": [1800, 2000],
        "total_revenue": [30000.0, 33000.0],
        "avg_order_value": [15.0, 15.0],
        "loyalty_orders": [400, 440],
        "total_option_revenue": [1000.0, 1100.0],
        "gold_ingestion_ts": ["2026-04-11T00:00:00Z"] * 2,
    }),
}


def _fake_get_table(name):
    if name in SAMPLE_TABLES:
        return SAMPLE_TABLES[name].copy()
    raise KeyError("No mock for " + name)


def _fake_query_large(sql):
    # Dashboard pages call query_large with different SQL. Return a union
    # of every column any page's query expects — extra columns are harmless.
    if "customers_scored" in sql or "MAX(cumulative_spend)" in sql:
        return pd.DataFrame([{
            "customers_scored": 20000,
            "earliest_date": pd.to_datetime("2020-04-21").date(),
            "latest_date":   pd.to_datetime("2024-02-21").date(),
            "max_clv": 5000.0,
            "median_clv_latest": 85.0,
        }])
    return pd.DataFrame({
        "date_key": pd.to_datetime(["2023-01-01", "2023-01-02", "2023-01-03"] * 3),
        "clv_tier": ["High", "Medium", "Low"] * 3,
        "median_spend": [100.0, 50.0, 20.0] * 3,
        "p75_spend":    [150.0, 75.0, 30.0] * 3,
        "p25_spend":    [ 80.0, 40.0, 15.0] * 3,
        # Home page uses cust_count; CLV page uses customer_count — provide both
        "cust_count":     [1000, 3000, 1000] * 3,
        "customer_count": [1000, 3000, 1000] * 3,
        "daily_revenue": [10.0, 5.0, 2.0] * 3,
        "daily_order_count": [1, 1, 1] * 3,
        "cumulative_spend": [100.0, 50.0, 20.0] * 3,
        "cumulative_orders": [1, 1, 1] * 3,
        "days_since_first_order": [0, 1, 2] * 3,
        "is_active_day": [True, False, False] * 3,
        "snapshot_year": [2023] * 9,
    })


def main():
    patcher1 = patch("lib.data.get_table", side_effect=_fake_get_table)
    patcher2 = patch("lib.data.query_large", side_effect=_fake_query_large)
    patcher1.start()
    patcher2.start()

    from streamlit.testing.v1 import AppTest
    pages = [
        ("app.py", "Home"),
        ("pages/1_CLV_Snapshot.py", "CLV Snapshot"),
        ("pages/2_RFM_Segments.py", "RFM Segments"),
        ("pages/3_Churn_Indicators.py", "Churn Indicators"),
        ("pages/4_Sales_Trends.py", "Sales Trends"),
        ("pages/5_Loyalty_Comparison.py", "Loyalty Comparison"),
        ("pages/6_Location_Performance.py", "Location Performance"),
        ("pages/7_Upsell_Analysis.py", "Upsell Analysis"),
    ]
    failed = []
    for path, label in pages:
        try:
            at = AppTest.from_file(path, default_timeout=60)
            at.run()
            errs = list(at.exception)
            if errs:
                failed.append((label, errs[0].message[:200]))
                print("  FAIL: " + label + " -- " + errs[0].message[:180])
            else:
                print("  OK:   " + label)
        except Exception as e:
            failed.append((label, str(e)[:200]))
            print("  EXC:  " + label + " -- " + str(e)[:200])
    patcher1.stop()
    patcher2.stop()

    print()
    if failed:
        print("FAILED: " + str(len(failed)) + "/" + str(len(pages)))
        sys.exit(1)
    print("All " + str(len(pages)) + " pages OK (mock data).")


if __name__ == "__main__":
    main()
