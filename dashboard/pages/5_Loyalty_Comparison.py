"""
Loyalty vs Non-Loyalty vs Anonymous comparison.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from lib.data import get_table

st.set_page_config(page_title="Loyalty Comparison", page_icon="⭐", layout="wide")

st.title("Loyalty Program Comparison")
st.caption(
    "Non-loyalty vs loyalty vs anonymous customers, compared on volume, average "
    "order value, repeat rate, and upsell attach rate."
)

loy = get_table("gold_loyalty_comparison")

# Order rows Loyalty > Non-Loyalty > Anonymous
order_map = {"Loyalty": 0, "Non-Loyalty": 1, "Anonymous": 2}
loy["sort"] = loy["segment"].map(order_map)
loy = loy.sort_values("sort").drop(columns="sort").reset_index(drop=True)

# ─── KPI comparison band ───
segments = loy["segment"].tolist()
for i, seg in enumerate(segments):
    row = loy[loy["segment"] == seg].iloc[0]
    cols = st.columns([1, 1, 1, 1, 1])
    with cols[0]:
        st.markdown(f"### {seg}")
    cols[1].metric("Customers",     f"{int(row['customer_count']):,}" if row['customer_count'] else "—")
    cols[2].metric("Orders",        f"{int(row['order_count']):,}")
    cols[3].metric("Revenue",       f"${float(row['total_revenue']):,.0f}")
    cols[4].metric("AOV",           f"${float(row['avg_spend_per_order']):,.2f}")

st.divider()

# ─── Repeat-rate side-by-side ───
known = loy[loy["segment"] != "Anonymous"].copy()

col1, col2 = st.columns(2)
with col1:
    st.subheader("Repeat customer rate (%)")
    fig1 = px.bar(
        known,
        x="segment", y="repeat_customer_rate_pct",
        color="segment",
        color_discrete_map={"Loyalty": "#2ca02c", "Non-Loyalty": "#1f77b4"},
        text="repeat_customer_rate_pct",
        labels={"repeat_customer_rate_pct": "Repeat rate (%)", "segment": ""},
    )
    fig1.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig1.update_layout(showlegend=False, height=360, yaxis_range=[0, 80])
    st.plotly_chart(fig1, use_container_width=True)

    delta = known.loc[known["segment"] == "Loyalty", "repeat_customer_rate_pct"].iloc[0] - \
            known.loc[known["segment"] == "Non-Loyalty", "repeat_customer_rate_pct"].iloc[0]
    st.info(f"**Loyalty members repeat {delta:.1f} percentage points more often** — the program works.")

with col2:
    st.subheader("Upsell attach rate (%)")
    fig2 = px.bar(
        loy,
        x="segment", y="option_attach_rate_pct",
        color="segment",
        text="option_attach_rate_pct",
        labels={"option_attach_rate_pct": "Attach rate (%)", "segment": ""},
    )
    fig2.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig2.update_layout(showlegend=False, height=360, yaxis_range=[0, 50])
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("Upsell attach rate: % of orders with ≥ 1 paid option.")

st.divider()

# ─── Revenue share ───
st.subheader("Revenue share")
rev_fig = go.Figure(data=[
    go.Pie(
        labels=loy["segment"],
        values=loy["total_revenue"],
        hole=0.5,
        marker=dict(colors=["#2ca02c", "#1f77b4", "#bcbcbc"]),
        textinfo="label+percent",
    )
])
rev_fig.update_layout(height=420, title="Share of total revenue by segment")
st.plotly_chart(rev_fig, use_container_width=True)

st.dataframe(loy, use_container_width=True, hide_index=True)
st.caption("Source: `s3://<BUCKET_NAME>/gold/gold_loyalty_comparison/`")
