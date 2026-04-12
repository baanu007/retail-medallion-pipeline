"""
Churn Indicators — at-risk / inactive customer detection.

Shows BOTH churn tag methodologies side by side:
  * churn_tag            — absolute threshold (matches spec example ">45 days = at risk")
  * churn_tag_personal   — personal-gap multiplier (sophisticated alternative)
"""

import streamlit as st
import plotly.express as px

from lib.data import get_table

st.set_page_config(page_title="Churn Indicators", page_icon="⚠️", layout="wide")

TAG_ORDER = ["Active", "Cooling Off", "At Risk", "Inactive"]
TAG_COLORS = {
    "Active": "#2ca02c",
    "Cooling Off": "#ffbb00",
    "At Risk": "#ff7f0e",
    "Inactive": "#d62728",
}

st.title("Churn Indicators")
st.caption(
    "Repeat customers (≥2 orders) only. One-time buyers are excluded because "
    '"churn" has no meaning for them.  TWO churn tagging methodologies are '
    "provided:\n\n"
    "* **Absolute** — `churn_tag` — matches the business spec example: "
    "Active ≤30d, Cooling Off 30-45d, **At Risk 45-60d**, Inactive >60d.\n"
    "* **Personal-gap** — `churn_tag_personal` — relative to each customer's own "
    "typical inter-order gap (1.5× / 2× / 3×). Better for weekly vs monthly "
    "regulars since their "normal" differs."
)

churn = get_table("gold_churn_indicators")

# ─── Which tagging methodology to show in the KPI band + charts ───
tag_col = st.radio(
    "Churn tagging method",
    options=[
        ("churn_tag",           "Absolute threshold (matches spec: >45d = at risk)"),
        ("churn_tag_personal",  "Personal-gap multiplier (sophisticated alt)"),
    ],
    format_func=lambda x: x[1],
    horizontal=False,
)[0]

# KPI band
c1, c2, c3, c4 = st.columns(4)
c1.metric("Repeat customers", f"{len(churn):,}")
c2.metric("Active",    f"{(churn[tag_col] == 'Active').sum():,}")
c3.metric("Cooling Off / At Risk",
          f"{churn[tag_col].isin(['Cooling Off', 'At Risk']).sum():,}")
c4.metric("Inactive",  f"{(churn[tag_col] == 'Inactive').sum():,}")

st.divider()

# ─── Side-by-side comparison of the two methods ───
st.subheader("Both methods — side-by-side distribution")
st.caption("See how many customers land in each bucket under the two methods.")

col1, col2 = st.columns(2)
for col, method_col, title in [
    (col1, "churn_tag", "Absolute threshold"),
    (col2, "churn_tag_personal", "Personal-gap multiplier"),
]:
    with col:
        counts = churn[method_col].value_counts().reset_index()
        counts.columns = ["tag", "count"]
        counts["sort"] = counts["tag"].map({t: i for i, t in enumerate(TAG_ORDER)})
        counts = counts.sort_values("sort").drop(columns=["sort"])
        fig = px.bar(
            counts,
            x="tag", y="count",
            color="tag",
            color_discrete_map=TAG_COLORS,
            labels={"count": "Customer Count", "tag": "Tag"},
            title=title,
            text="count",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, height=380)
        st.plotly_chart(fig, use_container_width=True)

st.divider()

# ─── Recency vs spend change scatter (current method) ───
st.subheader(f"Recency vs spend change — colored by `{tag_col}`")
st.caption(
    "Customers in the top-right corner are actively buying AND spending more. "
    "Bottom-left = inactive and declining — strong churn candidates."
)
sample = churn.dropna(subset=["spend_change_pct"]).sample(
    min(3000, len(churn)), random_state=42
).copy()
# Cast Decimal → float for plotting
sample["total_orders_plot"] = sample["total_orders"].astype(float)
fig2 = px.scatter(
    sample,
    x="days_since_last_order",
    y="spend_change_pct",
    color=tag_col,
    color_discrete_map=TAG_COLORS,
    size="total_orders_plot",
    size_max=15,
    labels={
        "days_since_last_order": "Days since last order",
        "spend_change_pct": "Spend change vs prior average (%)",
    },
    hover_data=["total_orders", "avg_gap_days"],
)
fig2.update_layout(height=450)
fig2.update_yaxes(range=[-100, 200])
st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ─── Breakdown table for current method ───
st.subheader(f"Breakdown — {tag_col}")
summary = (
    churn.groupby(tag_col, as_index=False)
         .agg(
             customer_count=("user_id", "count"),
             median_days_since_last=("days_since_last_order", "median"),
             median_avg_gap=("avg_gap_days", "median"),
             median_orders=("total_orders", "median"),
             median_spend_change_pct=("spend_change_pct", "median"),
         )
)
summary["sort"] = summary[tag_col].map({t: i for i, t in enumerate(TAG_ORDER)})
summary = summary.sort_values("sort").drop(columns=["sort"])
st.dataframe(summary, use_container_width=True, hide_index=True)

# ─── Threshold-based alert list (absolute method — matches spec) ───
st.divider()
st.subheader("Threshold alert list — customers with days_since_last_order > 45")
st.caption(
    "Matches the business spec example: customers whose last order was "
    "more than 45 days ago are flagged for analyst outreach."
)
alerts = (
    churn[churn["days_since_last_order"] > 45]
    .sort_values("days_since_last_order", ascending=False)
    [["user_id", "total_orders", "last_order_date", "days_since_last_order",
      "avg_gap_days", "recent_spend", "spend_change_pct", "churn_tag"]]
    .head(200)
)
st.write(f"{(churn['days_since_last_order'] > 45).sum():,} customers exceed the 45-day threshold. Top 200 shown.")
st.dataframe(alerts, use_container_width=True, hide_index=True)

st.caption("Source: `s3://globalpartners-aws/gold/gold_churn_indicators/`")
