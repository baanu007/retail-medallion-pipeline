"""
RFM Segments — Recency / Frequency / Monetary customer segmentation
cross-tabulated with LOYALTY STATUS.

Matches Step 6 dashboard Q1: "What distinct customer segments emerge when
grouping customers by purchase behavior (total spend, frequency, recency)
AND loyalty status?"
"""

import streamlit as st
import plotly.express as px

from lib.data import get_table

st.set_page_config(page_title="RFM Segments", page_icon="🎯", layout="wide")

st.title("RFM Segmentation (by purchase behavior × loyalty status)")
st.caption(
    "Each non-anonymous customer scored 1-5 on Recency (days since last order), "
    "Frequency (orders in last 12 months), and Monetary (spend in last 12 months), "
    "using ntile(5). Frequency and Monetary use a rolling 12-month window per the "
    "Step 5 business requirement. Segments are cross-tabulated with loyalty status."
)

rfm = get_table("gold_rfm_segments")
# Ensure loyalty label for readability
rfm["loyalty_label"] = rfm["is_loyalty"].map({True: "Loyalty", False: "Non-Loyalty"}).fillna("Unknown")

# ─── Loyalty filter ───
loyalty_filter = st.radio(
    "Filter by loyalty status",
    options=["All", "Loyalty only", "Non-Loyalty only"],
    horizontal=True,
)
if loyalty_filter == "Loyalty only":
    view = rfm[rfm["is_loyalty"] == True].copy()
elif loyalty_filter == "Non-Loyalty only":
    view = rfm[rfm["is_loyalty"] == False].copy()
else:
    view = rfm.copy()

# ─── KPIs ───
c1, c2, c3, c4 = st.columns(4)
c1.metric("Customers in view", f"{len(view):,}")
c2.metric("Median frequency",  f"{int(view['frequency'].median())}")
c3.metric("Median monetary",   f"${view['monetary'].median():,.2f}")
c4.metric("Median recency",    f"{int(view['recency_days'].median())}d")

st.divider()

# ─── Segment × Loyalty cross-tab bar chart ───
st.subheader("Segment distribution × loyalty status")
st.caption(
    "Cross-tab of RFM segment × loyalty status. Use this to spot whether "
    "loyalty members are disproportionately represented in any segment."
)

cross = (
    rfm.groupby(["segment", "loyalty_label"], as_index=False)
       .agg(customer_count=("user_id", "count"))
)
fig_cross = px.bar(
    cross,
    x="segment",
    y="customer_count",
    color="loyalty_label",
    barmode="group",
    color_discrete_map={"Loyalty": "#2ca02c", "Non-Loyalty": "#1f77b4"},
    labels={"customer_count": "Customer Count", "segment": "Segment", "loyalty_label": "Loyalty"},
    text="customer_count",
)
fig_cross.update_traces(textposition="outside")
fig_cross.update_layout(height=440)
st.plotly_chart(fig_cross, use_container_width=True)

st.divider()

# ─── Filtered segment distribution + scatter ───
col1, col2 = st.columns(2)
with col1:
    st.subheader(f"Segment distribution ({loyalty_filter.lower()})")
    seg_counts = view["segment"].value_counts().reset_index()
    seg_counts.columns = ["segment", "count"]
    seg_counts["pct"] = (seg_counts["count"] / seg_counts["count"].sum() * 100).round(1)
    fig = px.bar(
        seg_counts,
        x="segment", y="count",
        color="segment",
        text="pct",
        labels={"count": "Customer Count", "segment": "Segment"},
    )
    fig.update_traces(texttemplate="%{text}%", textposition="outside")
    fig.update_layout(showlegend=False, height=420)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("R × M scatter (sample of 5000 customers)")
    sample = view.sample(min(5000, len(view)), random_state=42).copy()
    # Cast Decimal → float for Plotly size
    sample["monetary"] = sample["monetary"].astype(float)
    sample["frequency_plot"] = sample["frequency"].astype(float) + 0.1  # size can't be 0
    fig2 = px.scatter(
        sample,
        x="recency_days",
        y="monetary",
        color="segment",
        size="frequency_plot",
        size_max=15,
        symbol="loyalty_label",
        labels={"recency_days": "Recency (days)", "monetary": "Monetary ($, last 12mo)"},
        hover_data=["frequency", "r_score", "f_score", "m_score", "loyalty_label"],
    )
    fig2.update_yaxes(type="log")
    fig2.update_layout(height=420)
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ─── R × F heatmap ───
st.subheader("R × F heatmap")
st.caption("Where the customer base clusters in recency/frequency space.")
heat = (
    view.groupby(["r_score", "f_score"])
        .agg(customer_count=("user_id", "count"))
        .reset_index()
)
fig3 = px.density_heatmap(
    heat,
    x="r_score", y="f_score",
    z="customer_count",
    labels={"r_score": "R score", "f_score": "F score", "customer_count": "# customers"},
    color_continuous_scale="Blues",
)
fig3.update_layout(height=450)
st.plotly_chart(fig3, use_container_width=True)

st.divider()

# ─── Full segment leaderboard (with loyalty breakdown) ───
st.subheader("Segment × loyalty leaderboard")
seg_stats = (
    rfm.groupby(["segment", "loyalty_label"], as_index=False)
       .agg(
           customer_count=("user_id", "count"),
           median_recency=("recency_days", "median"),
           median_frequency=("frequency", "median"),
           median_monetary=("monetary", "median"),
           total_monetary=("monetary", "sum"),
       )
       .sort_values(["segment", "loyalty_label"])
)
seg_stats["median_monetary"] = seg_stats["median_monetary"].astype(float).round(2)
seg_stats["total_monetary"]  = seg_stats["total_monetary"].astype(float).round(2)
st.dataframe(seg_stats, use_container_width=True, hide_index=True)

st.caption("Source: `s3://globalpartners-aws/gold/gold_rfm_segments/` — F and M use a rolling 12-month window.")
