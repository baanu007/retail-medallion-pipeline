"""
Sales Trends — daily / weekly / monthly revenue rollups.
"""

import streamlit as st
import plotly.express as px

from lib.data import get_table

st.set_page_config(page_title="Sales Trends", page_icon="📈", layout="wide")

st.title("Sales Trends")
st.caption("Revenue rollups at daily, weekly, and monthly grain. Filter by restaurant and category.")

monthly = get_table("gold_sales_monthly")
weekly = get_table("gold_sales_weekly")
daily = get_table("gold_sales_daily")

# ─── KPIs ───
c1, c2, c3 = st.columns(3)
c1.metric("Total revenue",         f"${monthly['total_revenue'].sum():,.0f}")
c2.metric("Total orders",          f"{int(monthly['order_count'].sum()):,}")
c3.metric("Months observed",       f"{monthly[['order_year', 'order_month']].drop_duplicates().shape[0]}")

st.divider()

# ─── Filters ───
col_f1, col_f2 = st.columns(2)
with col_f1:
    restaurants = ["All"] + sorted(monthly["restaurant_id"].unique().tolist())
    restaurant_filter = st.selectbox("Restaurant", restaurants)
with col_f2:
    grain = st.radio("Grain", ["Monthly", "Weekly", "Daily"], horizontal=True)

if restaurant_filter == "All":
    m = monthly.copy()
    w = weekly.copy()
    d = daily.copy()
else:
    m = monthly[monthly["restaurant_id"] == restaurant_filter].copy()
    w = weekly[weekly["restaurant_id"] == restaurant_filter].copy()
    d = daily[daily["restaurant_id"] == restaurant_filter].copy()

# ─── Trend chart ───
if grain == "Monthly":
    chart_df = m.groupby("order_year_month", as_index=False).agg(
        total_revenue=("total_revenue", "sum"),
        order_count=("order_count", "sum"),
    ).sort_values("order_year_month")
    x = "order_year_month"
elif grain == "Weekly":
    chart_df = w.groupby(["order_year", "order_week"], as_index=False).agg(
        total_revenue=("total_revenue", "sum"),
        order_count=("order_count", "sum"),
    ).sort_values(["order_year", "order_week"])
    chart_df["label"] = chart_df["order_year"].astype(str) + "-W" + chart_df["order_week"].astype(str).str.zfill(2)
    x = "label"
else:
    chart_df = d.groupby("order_date", as_index=False).agg(
        total_revenue=("total_item_revenue", "sum"),
        order_count=("order_count", "sum"),
    ).sort_values("order_date")
    x = "order_date"

fig = px.line(
    chart_df,
    x=x,
    y="total_revenue",
    labels={x: grain, "total_revenue": "Revenue ($)"},
    title=f"{grain} revenue" + (f" — {restaurant_filter[:10]}…" if restaurant_filter != "All" else " — all restaurants"),
)
fig.update_layout(height=420, hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─── Category breakdown (daily data has category) ───
st.subheader("Revenue by item category")
cat_totals = (
    d.groupby("item_category", as_index=False)
     .agg(total_revenue=("total_item_revenue", "sum"),
          order_count=("order_count", "sum"))
     .sort_values("total_revenue", ascending=False)
     .head(20)
)
fig_cat = px.bar(
    cat_totals,
    x="total_revenue",
    y="item_category",
    orientation="h",
    labels={"total_revenue": "Revenue ($)", "item_category": "Category"},
    title="Top 20 item categories by revenue",
)
fig_cat.update_layout(height=520, yaxis=dict(autorange="reversed"))
st.plotly_chart(fig_cat, use_container_width=True)

st.caption("Sources: `gold_sales_daily`, `gold_sales_weekly`, `gold_sales_monthly`")
