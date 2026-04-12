"""
Upsell Analysis — the discount-to-upsell pivot.

Requirements originally said "use option_price < 0 to detect discounts." Data
exploration confirmed ZERO negative prices exist. We pivoted to upsell analysis:
compare free ($0, 66% of options) vs paid add-ons (34%).
"""

import streamlit as st
import plotly.express as px

from lib.data import get_table

st.set_page_config(page_title="Upsell Analysis", page_icon="➕", layout="wide")

st.title("Upsell Analysis")
st.caption(
    "Discount analysis was impossible (zero negative option prices in the data). "
    "We pivoted to upsell analysis: how effective is the business at attaching "
    "paid options to orders?"
)

summary = get_table("gold_upsell_summary").iloc[0]
top = get_table("gold_upsell_top_options").copy()
# Delta DECIMAL → pandas Decimal; Plotly needs float for sizes/colors.
for c in ("total_revenue", "avg_price", "attach_count"):
    top[c] = top[c].astype(float)

# ─── KPIs ───
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total option rows",      f"{int(summary['total_option_rows']):,}")
c2.metric("Free option %",          f"{float(summary['free_option_pct']):.1f}%")
c3.metric("Paid option %",          f"{float(summary['paid_option_pct']):.1f}%")
c4.metric("Orders with paid opts",  f"{int(summary['orders_with_paid_options']):,}",
          f"{float(summary['order_upsell_rate_pct']):.1f}% attach")
c5.metric("Upsell share of revenue", f"{float(summary['upsell_share_of_revenue_pct']):.2f}%",
          help="Paid option revenue ÷ total order revenue")

st.divider()

# ─── Split donut: free vs paid options ───
col1, col2 = st.columns(2)
with col1:
    st.subheader("Free vs paid options")
    fig_split = px.pie(
        names=["Free ($0)", "Paid"],
        values=[int(summary['free_option_rows']), int(summary['paid_option_rows'])],
        color_discrete_sequence=["#bcbcbc", "#2ca02c"],
        hole=0.55,
    )
    fig_split.update_layout(height=360)
    st.plotly_chart(fig_split, use_container_width=True)

    st.info(
        f"**${float(summary['total_option_revenue']):,.0f}** in paid option "
        f"revenue across **{int(summary['paid_option_rows']):,}** upsells, "
        f"averaging **${float(summary['avg_option_revenue_per_upsell_order']):.2f}** "
        f"per upsell order."
    )

with col2:
    st.subheader("Top 15 paid options by revenue")
    top15 = top.head(15).copy()
    top15["option_display"] = top15["option_group_name"].str.slice(0, 30) + " / " + top15["option_name"]
    fig_top = px.bar(
        top15,
        x="total_revenue",
        y="option_display",
        orientation="h",
        color="attach_count",
        color_continuous_scale="Greens",
        labels={"total_revenue": "Revenue ($)", "option_display": "Option"},
        hover_data=["avg_price", "attach_count"],
    )
    fig_top.update_layout(height=520, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig_top, use_container_width=True)

st.divider()

# ─── Attach count vs revenue scatter (all top options) ───
st.subheader("Attach count vs revenue (top 100 paid options)")
st.caption("High-attach-count / high-revenue options (top-right) are the biggest upsell winners.")

top100 = top.head(100).copy()
fig_scat = px.scatter(
    top100,
    x="attach_count",
    y="total_revenue",
    size="avg_price",
    color="option_group_name",
    hover_name="option_name",
    labels={
        "attach_count": "Attach count",
        "total_revenue": "Total revenue ($)",
        "avg_price": "Avg price ($)",
    },
)
fig_scat.update_layout(height=520, showlegend=False)
st.plotly_chart(fig_scat, use_container_width=True)

st.divider()
st.subheader("Full ranked list")
st.dataframe(top, use_container_width=True, hide_index=True)

st.caption("Sources: `gold_upsell_summary` and `gold_upsell_top_options`")
