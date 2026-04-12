"""
CLV Snapshot — the PRIMARY metric.
Shows how customer lifetime value evolves daily.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from lib.data import get_table, query_large

st.set_page_config(page_title="CLV Snapshot", page_icon="💰", layout="wide")

st.title("Customer Lifetime Value — Daily Evolution")
st.caption(
    "Primary metric per the architecture. Each customer has one row per calendar day "
    "from their first order onward, carrying forward cumulative spend. Tiers are "
    "recomputed every day via ntile(5) on cumulative spend: top 20% → High, middle "
    "60% → Medium, bottom 20% → Low."
)

# ─── KPI band ───
kpi = query_large("""
    SELECT
        COUNT(DISTINCT user_id)          AS customers_scored,
        MIN(date_key)                    AS earliest_date,
        MAX(date_key)                    AS latest_date,
        MAX(cumulative_spend)            AS max_clv,
        approx_quantile(
            CASE WHEN date_key = (SELECT MAX(date_key) FROM gold_clv_snapshot)
                 THEN cumulative_spend END,
            0.5
        )                                AS median_clv_latest
    FROM gold_clv_snapshot
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Customers scored",   f"{int(kpi['customers_scored']):,}")
c2.metric("Time range",         f"{kpi['earliest_date']} → {kpi['latest_date']}")
c3.metric("Max CLV observed",   f"${float(kpi['max_clv']):,.2f}")
c4.metric("Median CLV (latest)", f"${float(kpi['median_clv_latest']):,.2f}")

st.divider()

# ─── View 1: tier-level median spend over time ───
st.subheader("1. Median cumulative spend per customer, by tier")
st.caption(
    "How the median customer in each tier grows their lifetime spend over time. "
    "Diverging lines mean the top-spending cohort is pulling away from the rest."
)

tier_over_time = query_large("""
    SELECT
        date_key,
        clv_tier,
        approx_quantile(cumulative_spend, 0.5) AS median_spend,
        approx_quantile(cumulative_spend, 0.75) AS p75_spend,
        approx_quantile(cumulative_spend, 0.25) AS p25_spend,
        COUNT(*) AS customer_count
    FROM gold_clv_snapshot
    WHERE clv_tier IN ('High', 'Medium', 'Low')
    GROUP BY date_key, clv_tier
    ORDER BY date_key
""")

fig1 = px.line(
    tier_over_time,
    x="date_key",
    y="median_spend",
    color="clv_tier",
    color_discrete_map={"High": "#2ca02c", "Medium": "#1f77b4", "Low": "#d62728"},
    labels={"date_key": "Date", "median_spend": "Median Cumulative Spend ($)", "clv_tier": "Tier"},
)
fig1.update_layout(hovermode="x unified", height=450)
st.plotly_chart(fig1, use_container_width=True)

# ─── View 2: tier membership over time (stacked area) ───
st.subheader("2. Customer tier membership over time")
st.caption(
    "How many customers fall into each tier on each day. Note: Pre-Activity "
    "customers (no orders yet) are excluded — only active customers are tiered."
)

fig2 = px.area(
    tier_over_time,
    x="date_key",
    y="customer_count",
    color="clv_tier",
    color_discrete_map={"High": "#2ca02c", "Medium": "#1f77b4", "Low": "#d62728"},
    labels={"date_key": "Date", "customer_count": "Customer Count", "clv_tier": "Tier"},
)
fig2.update_layout(hovermode="x unified", height=400)
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ─── View 3: individual customer trajectory ───
st.subheader("3. Individual customer CLV trajectory")
st.caption("Pick a customer to see their personal cumulative spend curve over time.")

# Top-50 customers for the dropdown (by total_spend from dim_customer)
customers = get_table("dim_customer").sort_values("total_spend", ascending=False)

col1, col2 = st.columns([1, 3])
with col1:
    topn = st.slider("Top N customers in dropdown", 10, 200, 50, 10)
    chosen_id = st.selectbox(
        "Customer (ranked by total spend)",
        options=customers.head(topn)["user_id"].tolist(),
        format_func=lambda uid: f"{uid[:10]}… — ${float(customers.loc[customers['user_id'] == uid, 'total_spend'].iloc[0]):,.2f}",
    )
    # Look up this customer's tier + stats
    row = customers[customers["user_id"] == chosen_id].iloc[0]
    st.metric("Total spend",   f"${float(row['total_spend']):,.2f}")
    st.metric("Total orders",  f"{int(row['total_orders'])}")
    st.metric("CLV tier",      row["clv_tier"])
    st.metric("Tenure (days)", f"{int(row['tenure_days']):,}")

with col2:
    traj = query_large(f"""
        SELECT date_key, cumulative_spend, daily_revenue, clv_tier
        FROM gold_clv_snapshot
        WHERE user_id = '{chosen_id}'
        ORDER BY date_key
    """)
    if len(traj) > 0:
        fig3 = go.Figure()
        fig3.add_trace(go.Scatter(
            x=traj["date_key"], y=traj["cumulative_spend"],
            mode="lines", name="Cumulative spend",
            line=dict(color="#1f77b4", width=3),
        ))
        # Highlight actual order days
        orders = traj[traj["daily_revenue"] > 0]
        fig3.add_trace(go.Scatter(
            x=orders["date_key"], y=orders["cumulative_spend"],
            mode="markers", name="Order day",
            marker=dict(color="#ff7f0e", size=6),
        ))
        fig3.update_layout(
            title="Cumulative CLV over time (orange dots = order days)",
            xaxis_title="Date",
            yaxis_title="Cumulative Spend ($)",
            height=450,
            hovermode="x unified",
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.warning("No CLV snapshot rows for this customer.")

st.divider()
st.caption("Source: `s3://globalpartners-aws/gold/gold_clv_snapshot/` — built by `build-gold-clv-snapshot.py`")
