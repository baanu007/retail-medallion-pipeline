"""
Location (restaurant) Performance.
"""

import streamlit as st
import plotly.express as px

from lib.data import get_table

st.set_page_config(page_title="Location Performance", page_icon="📍", layout="wide")

st.title("Location Performance")
st.caption(
    "27 Retail Restaurant locations ranked by revenue, order volume, unique customer "
    "base, and **customer retention rate** (% of customers who ordered ≥2 times). "
    "Retention distinguishes top performers — a high-revenue restaurant with low "
    "retention is relying on one-time visits; a high-retention restaurant has "
    "sticky regulars."
)

loc = get_table("gold_location_perf").sort_values("revenue_rank")
# Short IDs for readability
loc["restaurant_short"] = loc["restaurant_id"].str[-8:]
# Delta DECIMAL → pandas Decimal objects; Plotly needs float for sizes/colors.
for c in ("total_revenue", "avg_order_value", "orders_per_week",
          "revenue_per_week", "loyalty_order_pct", "retention_rate_pct"):
    loc[c] = loc[c].astype(float)

# ─── KPIs ───
c1, c2, c3, c4 = st.columns(4)
c1.metric("Restaurants",        f"{len(loc):,}")
c2.metric("Total revenue",      f"${loc['total_revenue'].sum():,.0f}")
c3.metric("Top location share", f"{loc.iloc[0]['total_revenue'] / loc['total_revenue'].sum() * 100:.1f}%",
          help="Revenue share of the #1 restaurant")
c4.metric("Avg retention rate", f"{loc['retention_rate_pct'].mean():.1f}%",
          help="Average % of customers with ≥2 orders across all locations")

st.divider()

# ─── Revenue ranking bar chart — colored by retention rate ───
st.subheader("Revenue ranking (bar color = customer retention rate)")
fig1 = px.bar(
    loc,
    x="total_revenue",
    y="restaurant_short",
    orientation="h",
    color="retention_rate_pct",
    color_continuous_scale="RdYlGn",
    range_color=[0, max(loc["retention_rate_pct"].max(), 60)],
    labels={"total_revenue": "Revenue ($)", "restaurant_short": "Restaurant",
            "retention_rate_pct": "Retention %"},
    hover_data=["restaurant_id", "total_orders", "unique_customers",
                "avg_order_value", "retention_rate_pct", "repeat_customer_count"],
)
fig1.update_layout(
    height=700,
    yaxis=dict(
        autorange="reversed",
        tickvals=loc["restaurant_short"],
        ticktext=loc["restaurant_short"],
    ),
)
st.plotly_chart(fig1, use_container_width=True)

st.divider()

# ─── Revenue vs retention scatter (the key "distinguishing metric" chart) ───
col1, col2 = st.columns(2)
with col1:
    st.subheader("Revenue vs retention rate")
    st.caption("The requirement's core question: which top-performing locations also keep customers coming back?")
    fig2 = px.scatter(
        loc,
        x="retention_rate_pct",
        y="total_revenue",
        size="unique_customers",
        color="avg_order_value",
        color_continuous_scale="Viridis",
        hover_name="restaurant_short",
        labels={
            "retention_rate_pct": "Retention rate (%)",
            "total_revenue": "Total revenue ($)",
            "avg_order_value": "AOV ($)",
        },
        size_max=45,
    )
    fig2.update_layout(height=450)
    st.plotly_chart(fig2, use_container_width=True)

with col2:
    st.subheader("AOV vs orders per week")
    fig3 = px.scatter(
        loc,
        x="orders_per_week",
        y="avg_order_value",
        size="total_revenue",
        color="retention_rate_pct",
        color_continuous_scale="RdYlGn",
        hover_name="restaurant_short",
        labels={
            "orders_per_week": "Orders per week",
            "avg_order_value": "Average order value ($)",
            "retention_rate_pct": "Retention %",
        },
        size_max=45,
    )
    fig3.update_layout(height=450)
    st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ─── Ranked table ───
st.subheader("Full leaderboard")
display = loc[[
    "revenue_rank", "restaurant_short", "restaurant_id",
    "total_revenue", "total_orders", "unique_customers", "repeat_customer_count",
    "retention_rate_pct", "avg_order_value", "orders_per_week", "loyalty_order_pct",
]].copy()
display.columns = [
    "Rank", "Short ID", "Full ID",
    "Revenue", "Orders", "Customers", "Repeat Cust.",
    "Retention %", "AOV", "Orders/wk", "Loyalty %",
]
st.dataframe(display, use_container_width=True, hide_index=True)

st.caption("Source: `s3://<BUCKET_NAME>/gold/gold_location_perf/` — now includes retention_rate_pct")
