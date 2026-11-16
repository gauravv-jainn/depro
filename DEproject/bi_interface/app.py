"""
Modern Conversational BI Interface & Analytics Portal
Empowered by Modern Data Stack: Airflow | Airbyte | BigQuery | dbt
"""

import os
import sys
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import importlib
import bi_interface.db_connector as db_connector
import bi_interface.conversational_engine as conv_engine

importlib.reload(db_connector)
importlib.reload(conv_engine)

from bi_interface.db_connector import execute_query, is_bigquery_connected, get_table_counts
from bi_interface.conversational_engine import parse_natural_language_query

# Page config
st.set_page_config(
    page_title="Conversational BI | Modern Data Stack",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.1rem;
        font-weight: 700;
        color: #1E293B;
        margin-bottom: 0.2rem;
    }
    .sub-header {
        font-size: 1rem;
        color: #64748B;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
    }
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #0F172A;
    }
    .metric-label {
        font-size: 0.85rem;
        color: #64748B;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 1.05rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


# Sidebar
with st.sidebar:
    st.image("https://img.icons8.com/isometric/100/database.png", width=64)
    st.title("Data Pipeline BI")
    st.caption("E-Commerce Modern Data Stack")

    st.markdown("---")
    st.subheader("Warehouse Status")
    if is_bigquery_connected():
        st.success("🟢 Connected to BigQuery")
        st.caption(f"Project: `{os.getenv('GCP_PROJECT')}`")
    else:
        st.info("🔵 Active: Local Warehouse Cache (DuckDB)")
        st.caption("Set GCP_PROJECT to use BigQuery")

    st.markdown("---")
    st.subheader("Data Warehouse Layers")
    counts = get_table_counts()
    
    with st.expander("Raw Layer (Airbyte / Bronze)", expanded=False):
        st.write(f"- `raw.customers`: **{counts.get('raw.customers', 0)}** rows")
        st.write(f"- `raw.products`: **{counts.get('raw.products', 0)}** rows")
        st.write(f"- `raw.orders`: **{counts.get('raw.orders', 0)}** rows")
        st.write(f"- `raw.order_items`: **{counts.get('raw.order_items', 0)}** rows")
        st.write(f"- `raw.payments`: **{counts.get('raw.payments', 0)}** rows")

    with st.expander("Staging Layer (dbt / Silver)", expanded=False):
        st.write(f"- `stg_customers`: **{counts.get('staging.stg_customers', 0)}**")
        st.write(f"- `stg_products`: **{counts.get('staging.stg_products', 0)}**")
        st.write(f"- `stg_orders`: **{counts.get('staging.stg_orders', 0)}**")
        st.write(f"- `int_order_items_detailed`: **{counts.get('staging.int_order_items_detailed', 0)}**")

    with st.expander("Marts Layer (dbt / Gold)", expanded=True):
        st.write(f"- `dim_customers`: **{counts.get('marts.dim_customers', 0)}**")
        st.write(f"- `dim_products`: **{counts.get('marts.dim_products', 0)}**")
        st.write(f"- `fact_orders`: **{counts.get('marts.fact_orders', 0)}**")
        st.write(f"- `mart_daily_sales`: **{counts.get('marts.mart_daily_sales', 0)}**")
        st.write(f"- `mart_delivery_sla`: **{counts.get('marts.mart_delivery_sla', 0)}**")

    st.markdown("---")
    st.subheader("Pipeline Orchestration")
    if st.button("Run pipeline (local runner)", use_container_width=True):
        with st.spinner("Running ingest, dbt run, dbt test..."):
            from orchestration.local_runner import run_pipeline
            success = run_pipeline()
            if success:
                st.success("DAG Executed Successfully!")
                st.rerun()
            else:
                st.error("Pipeline failed. Check terminal logs.")


# Header
st.markdown('<div class="main-header">Modern E-Commerce Data Pipeline & Conversational BI</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Automated Ingestion (Airbyte) | Cloud Data Warehouse (BigQuery) | Transformations (dbt) | Orchestration (Airflow)</div>', unsafe_allow_html=True)

# Tabs
tab_chat, tab_dashboard, tab_architecture = st.tabs([
    "Conversational AI Assistant",
    "Executive KPI Dashboard",
    "Pipeline Architecture & Lineage"
])

# -----------------------------------------------------------------------------
# TAB 1: Conversational BI Assistant
# -----------------------------------------------------------------------------
def render_result(parsed, df_result, key):
    """Render the chart + data table for one assistant answer."""
    chart_type, x_col, y_col = parsed["chart_type"], parsed["x"], parsed["y"]
    if df_result.empty:
        st.warning("Query returned zero rows.")
        return
    chart_col, data_col = st.columns([3, 2])
    with chart_col:
        has_xy = x_col in df_result.columns and y_col in df_result.columns
        if chart_type == "bar" and has_xy:
            fig = px.bar(df_result, x=x_col, y=y_col, color=y_col, template="plotly_white",
                         title=f"{y_col.replace('_', ' ').title()} by {x_col.replace('_', ' ').title()}")
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{key}")
        elif chart_type == "line" and has_xy:
            fig = px.line(df_result, x=x_col, y=y_col, markers=True, template="plotly_white",
                          title=f"{y_col.replace('_', ' ').title()} Trend")
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{key}")
        elif chart_type == "pie" and has_xy:
            fig = px.pie(df_result, names=x_col, values=y_col, hole=0.4, template="plotly_white",
                         title=f"Distribution of {y_col.replace('_', ' ').title()}")
            st.plotly_chart(fig, use_container_width=True, key=f"chart_{key}")
        else:
            num_cols = df_result.select_dtypes(include=["number"]).columns.tolist()
            cat_cols = df_result.select_dtypes(include=["object", "string"]).columns.tolist()
            if num_cols and cat_cols and len(df_result) > 1:
                fig = px.bar(df_result, x=cat_cols[0], y=num_cols[0], template="plotly_white",
                             title=f"{num_cols[0].title()} by {cat_cols[0].title()}")
                st.plotly_chart(fig, use_container_width=True, key=f"chart_{key}")
            else:
                st.dataframe(df_result, use_container_width=True)
    with data_col:
        st.markdown(f"**Result Data ({len(df_result)} rows):**")
        st.dataframe(df_result, use_container_width=True, height=350)
        st.download_button("Download CSV", df_result.to_csv(index=False).encode("utf-8"),
                           file_name="query_results.csv", mime="text/csv", key=f"dl_{key}")


with tab_chat:
    st.subheader("Chat with your data")
    st.write("Ask in plain English. The assistant writes SQL against the dbt Gold marts, runs it on the warehouse, and charts the answer.")

    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    def ask(text):
        st.session_state["pending_question"] = text

    st.markdown("**Try one:**")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.button("Top selling products", on_click=ask, args=("what is the top selling product",), use_container_width=True)
        st.button("Delivery SLA by city", on_click=ask, args=("Show me delivery SLA and delayed orders by city",), use_container_width=True)
    with col2:
        st.button("Revenue and margin by category", on_click=ask, args=("What is our category revenue and profit margins?",), use_container_width=True)
        st.button("Payment method split", on_click=ask, args=("What is the breakdown of payment methods?",), use_container_width=True)
    with col3:
        st.button("Customer RFM segments", on_click=ask, args=("Show customer count and spend by RFM customer segment",), use_container_width=True)
        st.button("Daily revenue trend", on_click=ask, args=("What is our daily revenue trend over time?",), use_container_width=True)
    if st.session_state["messages"] and st.button("Clear chat"):
        st.session_state["messages"] = []
        st.rerun()

    typed = st.chat_input("Ask a question, e.g. 'top 5 customers by spend', or paste a SELECT")
    question = typed or st.session_state.pop("pending_question", None)

    if question:
        st.session_state["messages"].append({"role": "user", "content": question})
        try:
            parsed = parse_natural_language_query(question)
            df_result = execute_query(parsed["sql"])
            st.session_state["messages"].append(
                {"role": "assistant", "content": parsed["summary"], "parsed": parsed, "df": df_result})
        except Exception as e:
            st.session_state["messages"].append({"role": "assistant", "content": f"Sorry, that failed: {e}", "error": True})

    for i, msg in enumerate(st.session_state["messages"]):
        with st.chat_message(msg["role"]):
            if msg.get("error"):
                st.error(msg["content"])
                continue
            st.markdown(msg["content"])
            if msg["role"] == "assistant" and "parsed" in msg:
                with st.expander("View generated SQL", expanded=False):
                    st.code(msg["parsed"]["sql"], language="sql")
                render_result(msg["parsed"], msg["df"], key=i)


# -----------------------------------------------------------------------------
# TAB 2: Executive KPI Dashboard
# -----------------------------------------------------------------------------
with tab_dashboard:
    st.subheader("Enterprise Operational & Financial Metrics")

    # Fetch Fact & Dimension Summaries
    try:
        kpi_df = execute_query("""
            SELECT 
                COUNT(order_id) AS total_orders,
                ROUND(SUM(order_total_amount), 2) AS gmv_inr,
                ROUND(AVG(order_total_amount), 2) AS aov_inr,
                ROUND(SUM(net_gross_profit_amount), 2) AS total_profit_inr,
                ROUND(AVG(fulfillment_duration_days), 1) AS avg_delivery_days,
                ROUND(
                    (SUM(CASE WHEN order_status = 'Delivered' AND is_delayed_flag = 0 THEN 1 ELSE 0 END) * 100.0) / 
                    NULLIF(SUM(CASE WHEN order_status = 'Delivered' THEN 1 ELSE 0 END), 0), 1
                ) AS on_time_rate_pct
            FROM marts.fact_orders;
        """)

        if not kpi_df.empty:
            kpi = kpi_df.iloc[0]
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Gross Revenue (GMV)", f"INR {kpi['gmv_inr']:,.0f}")
            m2.metric("Total Orders", f"{int(kpi['total_orders']):,}")
            m3.metric("Avg Order Value (AOV)", f"INR {kpi['aov_inr']:,.0f}")
            m4.metric("Gross Profit", f"INR {kpi['total_profit_inr']:,.0f}")
            m5.metric("On-Time Delivery", f"{kpi['on_time_rate_pct']}%")

        st.markdown("---")

        # Row 1 Charts
        r1_c1, r1_c2 = st.columns(2)

        with r1_c1:
            st.markdown("#### Category Revenue & Profitability")
            cat_df = execute_query("""
                SELECT 
                    category,
                    ROUND(SUM(total_gross_revenue), 2) AS gross_revenue,
                    ROUND(SUM(total_gross_profit), 2) AS gross_profit,
                    ROUND(AVG(gross_margin_percentage), 1) AS margin_pct
                FROM marts.dim_products
                GROUP BY category
                ORDER BY gross_revenue DESC;
            """)
            fig_cat = px.bar(
                cat_df,
                x="category",
                y=["gross_revenue", "gross_profit"],
                barmode="group",
                title="Revenue vs Profit by Category (INR)",
                labels={"value": "Amount (INR)", "variable": "Metric"},
                template="plotly_white"
            )
            st.plotly_chart(fig_cat, use_container_width=True)

        with r1_c2:
            st.markdown("#### Customer RFM Segments (LTV)")
            rfm_df = execute_query("""
                SELECT 
                    customer_segment,
                    COUNT(customer_id) AS customer_count,
                    ROUND(SUM(lifetime_spend), 2) AS total_segment_spend
                FROM marts.dim_customers
                GROUP BY customer_segment
                ORDER BY total_segment_spend DESC;
            """)
            fig_rfm = px.pie(
                rfm_df,
                names="customer_segment",
                values="total_segment_spend",
                hole=0.45,
                title="Revenue Contribution by Customer Tier",
                template="plotly_white"
            )
            st.plotly_chart(fig_rfm, use_container_width=True)

        # Row 2 Charts
        r2_c1, r2_c2 = st.columns(2)

        with r2_c1:
            st.markdown("#### Delivery SLA & Fulfillment Performance by City")
            sla_df = execute_query("""
                SELECT 
                    shipping_city,
                    total_orders,
                    delivered_orders,
                    delayed_orders,
                    on_time_delivery_rate_pct
                FROM marts.mart_delivery_sla
                ORDER BY total_orders DESC;
            """)
            fig_sla = px.bar(
                sla_df,
                x="shipping_city",
                y="on_time_delivery_rate_pct",
                color="on_time_delivery_rate_pct",
                color_continuous_scale="RdYlGn",
                title="On-Time Delivery Rate % by City",
                labels={"on_time_delivery_rate_pct": "On-Time %"},
                template="plotly_white"
            )
            st.plotly_chart(fig_sla, use_container_width=True)

        with r2_c2:
            st.markdown("#### Daily Revenue Trend & Run-rate")
            sales_df = execute_query("""
                SELECT 
                    order_date,
                    gross_revenue,
                    net_revenue,
                    total_profit
                FROM marts.mart_daily_sales
                ORDER BY order_date ASC;
            """)
            fig_sales = px.line(
                sales_df,
                x="order_date",
                y=["gross_revenue", "total_profit"],
                title="Daily Sales & Profit Timeline",
                labels={"value": "Amount (INR)", "variable": "Metric"},
                template="plotly_white"
            )
            st.plotly_chart(fig_sales, use_container_width=True)

    except Exception as e:
        st.error(f"Dashboard load error: {e}")


# -----------------------------------------------------------------------------
# TAB 3: Pipeline Architecture & Lineage
# -----------------------------------------------------------------------------
with tab_architecture:
    st.subheader("Modern Data Stack (MDS) Architecture & Lineage")

    st.markdown("""
    ```
    ┌──────────────────────┐
    │  E-Commerce Source   │ (Customers, Products, Orders, Items, Payments)
    └──────────┬───────────┘
               │
               ▼  [EXTRACT & LOAD]
    ┌──────────────────────┐
    │   Airbyte / Ingest   │ Managed EL connector loading raw CSV/APIs
    └──────────┬───────────┘
               │
               ▼  [DATA WAREHOUSE LANDING]
    ┌──────────────────────┐
    │ Warehouse: RAW Layer │ BigQuery dataset raw (source-of-truth)
    └──────────┬───────────┘
               │
               ▼  [TRANSFORMATION: BRONZE]
    ┌──────────────────────┐
    │ dbt: STAGING Layer   │ Type-casting, column standardization, deduplication
    └──────────┬───────────┘
               │
               ▼  [TRANSFORMATION: SILVER]
    ┌──────────────────────┐
    │ dbt: INTERMEDIATE    │ Business logic joins, margin calculations, customer metrics
    └──────────┬───────────┘
               │
               ▼  [TRANSFORMATION: GOLD]
    ┌──────────────────────┐
    │ dbt: MARTS (Star)    │ dim_customers, dim_products, fact_orders, mart_daily_sales
    └──────────┬───────────┘
               │
               ▼  [ORCHESTRATION & SERVING]
    ┌──────────────────────┐
    │ Apache Airflow DAG   │ Scheduled workflow, data quality test gates, automated alerts
    └──────────┬───────────┘
               │
               ▼  [CONSUMPTION]
    ┌──────────────────────┐
    │ Conversational BI    │ Natural Language to SQL, Plotly charts, executive KPIs
    └──────────────────────┘
    ```
    """)

    st.markdown("---")
    st.subheader("Data Catalog & Schema Browser")

    table_options = [
        "marts.fact_orders",
        "marts.dim_customers",
        "marts.dim_products",
        "marts.mart_daily_sales",
        "marts.mart_delivery_sla",
        "staging.stg_orders",
        "staging.stg_customers",
        "staging.int_order_items_detailed",
        "raw.orders",
        "raw.customers"
    ]

    selected_table = st.selectbox("Select Table to Inspect:", table_options)
    if selected_table:
        sample_df = execute_query(f"SELECT * FROM {selected_table} LIMIT 10;")
        col_info = execute_query(f"DESCRIBE {selected_table};")
        
        c1, c2 = st.columns([1, 2])
        with c1:
            st.markdown(f"**Schema Definition (`{selected_table}`):**")
            st.dataframe(col_info, use_container_width=True)
        with c2:
            st.markdown(f"**Data Preview (Top 10 Rows):**")
            st.dataframe(sample_df, use_container_width=True)
