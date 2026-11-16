"""
Conversational Engine: Translates Natural Language Queries into Warehouse SQL
Features:
- Multi-token semantic intent parsing with fuzzy & concept matching
- Dynamic LIMIT and metric sorting extraction
- Fallback to OpenAI / LLM if API key is provided
"""

import os
import re

WAREHOUSE_SCHEMA_PROMPT = """
Target Tables in Schema 'marts':
1. dim_customers: customer_id, full_name, email, city, state, signup_at, total_orders_placed, lifetime_spend, average_order_value, customer_segment ('Platinum VIP', 'Gold Loyal', 'Silver Regular', 'Bronze New', 'Prospect')
2. dim_products: product_id, product_name, category, retail_price_inr, unit_cost_inr, unit_profit_inr, current_inventory_stock, total_quantity_sold, total_gross_revenue, total_gross_profit, gross_margin_percentage
3. fact_orders: order_id, customer_id, ordered_at, order_date, order_status ('Delivered', 'Shipped', 'Processing', 'Cancelled'), payment_method, shipping_city, shipping_fee_inr, order_total_amount, total_cogs_amount, net_gross_profit_amount, is_delayed_flag (0 or 1), fulfillment_duration_days, delivery_delay_days
4. mart_daily_sales: order_date, total_orders, unique_customers_ordering, delivered_orders, cancelled_orders, gross_revenue, net_revenue, total_profit, average_order_value
5. mart_delivery_sla: shipping_city, total_orders, delivered_orders, delayed_orders, on_time_delivery_rate_pct, avg_delivery_days, avg_delay_when_late_days
"""


def extract_limit(query: str, default: int = 10) -> int:
    """
    Extracts limit dynamically based on numeric mention or singular vs plural phrasing.
    Examples:
    - 'what is the top selling product' -> 1 (Singular)
    - 'what are the top selling products' -> 10 (Plural default)
    - 'top 5 products' -> 5
    """
    q_lower = query.lower()

    # 1. Explicit numeric limit: 'top 5', 'first 3', 'limit 8'
    match = re.search(r"\b(?:top|first|best|limit)\s*(\d+)\b", q_lower)
    if match:
        val = int(match.group(1))
        return max(1, min(val, 50))

    # 2. Plural indicators: 'what are', 'which are', 'who are', 'show all', 'list'
    if re.search(r"\b(what are|which are|who are|show all|list the)\b", q_lower):
        return default

    # 3. Explicit singular indicators: 'what is', 'which is', 'who is', 'number 1', '#1'
    if re.search(r"\b(what is the|which is the|who is the|what's the|who's the|number 1|#1|single)\b", q_lower):
        return 1

    # 4. Singular noun phrasing without 's' (e.g. 'top product' vs 'top products')
    if re.search(r"\b(product|customer|item|client)\b", q_lower) and not re.search(r"\b(products|customers|items|clients)\b", q_lower):
        if re.search(r"\b(is|the single|best single)\b", q_lower):
            return 1

    return default


def parse_natural_language_query(user_query: str) -> dict:
    """
    Translates natural language questions to SQL using smart semantic intent matching.
    """
    q = user_query.lower().strip()

    # Direct SQL passthrough
    if q.startswith("select "):
        return {
            "sql": user_query,
            "summary": "Executing your custom SQL query directly against the warehouse:",
            "chart_type": "table",
            "x": None,
            "y": None
        }

    # Extract dynamic limit (e.g. top 5, top 3, or default 10)
    limit_val = extract_limit(q, default=10)

    # -------------------------------------------------------------------------
    # INTENT 1: Top / Best Selling Products (Quantity or Revenue)
    # -------------------------------------------------------------------------
    has_superlative = bool(re.search(r"\b(top|best|highest|most|popular|leading|max|rank|favorite)\b", q))
    has_product = bool(re.search(r"\b(product|products|item|items|sku|merchandise|selling|sold)\b", q))
    has_customer = bool(re.search(r"\b(customer|customers|client|clients|buyer|buyers|spender|spenders)\b", q))

    # Match Top Products (ensure it's not top customers)
    if (has_superlative and has_product and not has_customer) or re.search(r"\btop\s*selling\b", q) or re.search(r"\bbest\s*seller\b", q):
        # Check if quantity or volume is specifically asked
        if re.search(r"\b(quantity|units|volume|sold|count)\b", q):
            sort_col = "total_quantity_sold"
            summary_metric = "units sold"
        else:
            sort_col = "total_gross_revenue"
            summary_metric = "gross revenue"

        sql = f"""SELECT product_name, category, total_quantity_sold, total_gross_revenue, gross_margin_percentage
FROM marts.dim_products
ORDER BY {sort_col} DESC
LIMIT {limit_val};"""

        if limit_val == 1:
            summary_msg = f"The #1 top-performing product by {summary_metric} is:"
        else:
            summary_msg = f"Here are the top {limit_val} products ranked by {summary_metric}:"

        return {
            "sql": sql,
            "summary": summary_msg,
            "chart_type": "bar",
            "x": "product_name",
            "y": sort_col
        }

    # -------------------------------------------------------------------------
    # INTENT 2: Top / High-Value Customers (LTV / Big Spenders)
    # -------------------------------------------------------------------------
    if has_customer and (has_superlative or re.search(r"\b(spend|spender|valuable|rich|loyal|ltv|revenue)\b", q)):
        sql = f"""SELECT full_name, city, customer_segment, total_orders_placed, lifetime_spend, average_order_value
FROM marts.dim_customers
ORDER BY lifetime_spend DESC
LIMIT {limit_val};"""
        if limit_val == 1:
            summary_msg = "The #1 highest-spending customer by Lifetime Value (LTV) is:"
        else:
            summary_msg = f"Here are the top {limit_val} highest-spending customers by Lifetime Value (LTV):"

        return {
            "sql": sql,
            "summary": summary_msg,
            "chart_type": "bar",
            "x": "full_name",
            "y": "lifetime_spend"
        }

    # -------------------------------------------------------------------------
    # INTENT 3: Customer RFM Segmentation / Tiers
    # -------------------------------------------------------------------------
    if re.search(r"\b(rfm|segment|segments|segmentation|tier|tiers|vip|platinum|gold loyal|silver regular|bronze new)\b", q) or (has_customer and re.search(r"\b(group|groups|breakdown|distribution|count)\b", q)):
        sql = """SELECT 
    customer_segment,
    COUNT(customer_id) AS total_customers,
    ROUND(SUM(lifetime_spend), 2) AS total_spend,
    ROUND(AVG(lifetime_spend), 2) AS avg_customer_spend,
    ROUND(AVG(total_orders_placed), 1) AS avg_orders
FROM marts.dim_customers
GROUP BY customer_segment
ORDER BY total_spend DESC;"""
        return {
            "sql": sql,
            "summary": "Here is the customer distribution across RFM tiers (Platinum VIP, Gold Loyal, Silver Regular, Bronze New):",
            "chart_type": "pie",
            "x": "customer_segment",
            "y": "total_spend"
        }

    # -------------------------------------------------------------------------
    # INTENT 4: Product Category Performance & Margins
    # -------------------------------------------------------------------------
    if re.search(r"\b(category|categories|department|departments|genre|profit margin|margin|margins|profitability)\b", q):
        sql = """SELECT 
    category,
    COUNT(product_id) AS total_products,
    SUM(total_quantity_sold) AS units_sold,
    ROUND(SUM(total_gross_revenue), 2) AS category_revenue,
    ROUND(SUM(total_gross_profit), 2) AS category_profit,
    ROUND(AVG(gross_margin_percentage), 2) AS avg_margin_pct
FROM marts.dim_products
GROUP BY category
ORDER BY category_revenue DESC;"""
        return {
            "sql": sql,
            "summary": "Here is the performance breakdown across all product categories including gross profit and margins:",
            "chart_type": "bar",
            "x": "category",
            "y": "category_revenue"
        }

    # -------------------------------------------------------------------------
    # INTENT 5: Delivery SLA, Shipping Delays & City Logistics
    # -------------------------------------------------------------------------
    if re.search(r"\b(delivery|deliveries|delay|delays|delayed|late|sla|shipping|shipment|courier|on time|ontime|city|cities|fulfillment)\b", q):
        sql = """SELECT 
    shipping_city,
    total_orders,
    delivered_orders,
    delayed_orders,
    on_time_delivery_rate_pct,
    avg_delivery_days
FROM marts.mart_delivery_sla
ORDER BY total_orders DESC;"""
        return {
            "sql": sql,
            "summary": "Here is the logistics fulfillment performance, SLA compliance, and on-time delivery rate by city:",
            "chart_type": "bar",
            "x": "shipping_city",
            "y": "on_time_delivery_rate_pct"
        }

    # -------------------------------------------------------------------------
    # INTENT 6: Daily Sales Trends / Revenue Over Time
    # -------------------------------------------------------------------------
    if re.search(r"\b(daily|trend|trends|timeline|sales over time|revenue trend|day|date|month|growth|timeline|run rate)\b", q):
        sql = """SELECT 
    order_date,
    total_orders,
    gross_revenue,
    net_revenue,
    total_profit
FROM marts.mart_daily_sales
ORDER BY order_date ASC;"""
        return {
            "sql": sql,
            "summary": "Here is the daily sales, net revenue, and gross profit trend over time:",
            "chart_type": "line",
            "x": "order_date",
            "y": "gross_revenue"
        }

    # -------------------------------------------------------------------------
    # INTENT 7: Payment Methods & Gateways
    # -------------------------------------------------------------------------
    if re.search(r"\b(payment|payments|pay|method|methods|mode|modes|upi|card|credit|debit|cod|cash|net banking)\b", q):
        sql = """SELECT 
    payment_method,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(order_total_amount), 2) AS total_collected_inr,
    ROUND(AVG(order_total_amount), 2) AS avg_order_value
FROM marts.fact_orders
WHERE order_status != 'Cancelled'
GROUP BY payment_method
ORDER BY total_collected_inr DESC;"""
        return {
            "sql": sql,
            "summary": "Here is the payment method distribution across settled orders:",
            "chart_type": "pie",
            "x": "payment_method",
            "y": "total_collected_inr"
        }

    # -------------------------------------------------------------------------
    # INTENT 8: Cancelled Orders / Lost Revenue
    # -------------------------------------------------------------------------
    if re.search(r"\b(cancel|cancelled|cancellations|canceling|failed|loss|lost)\b", q):
        sql = """SELECT 
    shipping_city,
    COUNT(order_id) AS cancelled_orders,
    ROUND(SUM(order_total_amount), 2) AS lost_revenue_inr
FROM marts.fact_orders
WHERE order_status = 'Cancelled'
GROUP BY shipping_city
ORDER BY cancelled_orders DESC;"""
        return {
            "sql": sql,
            "summary": "Here is the breakdown of cancelled orders and lost revenue by destination city:",
            "chart_type": "bar",
            "x": "shipping_city",
            "y": "cancelled_orders"
        }

    # -------------------------------------------------------------------------
    # INTENT 9: Overall Business KPIs (GMV, AOV, Totals)
    # -------------------------------------------------------------------------
    if re.search(r"\b(aov|average order|order value|basket size|gmv|total revenue|total sales|kpi|overview|summary|stats)\b", q):
        sql = """SELECT 
    COUNT(DISTINCT order_id) AS total_orders,
    ROUND(SUM(order_total_amount), 2) AS gross_revenue_inr,
    ROUND(AVG(order_total_amount), 2) AS average_order_value_inr,
    ROUND(SUM(net_gross_profit_amount), 2) AS total_gross_profit_inr,
    ROUND(AVG(fulfillment_duration_days), 1) AS avg_delivery_days
FROM marts.fact_orders;"""
        return {
            "sql": sql,
            "summary": "Here is the high-level business KPI summary (GMV, Total Orders, AOV, Gross Profit):",
            "chart_type": "table",
            "x": None,
            "y": None
        }

    # -------------------------------------------------------------------------
    # INTENT 10: Optional LLM Integration if OpenAI Key is provided
    # -------------------------------------------------------------------------
    if os.getenv("ANTHROPIC_API_KEY"):
        try:
            import anthropic
            client = anthropic.Anthropic()
            msg = client.messages.create(
                model="claude-haiku-5-5",
                max_tokens=600,
                system="You are a SQL expert. Reply with exactly one read-only SELECT statement and nothing else.\n" + WAREHOUSE_SCHEMA_PROMPT,
                messages=[{"role": "user", "content": user_query}],
            )
            generated_sql = msg.content[0].text.strip().replace("```sql", "").replace("```", "").strip()
            if generated_sql.lower().startswith(("select", "with")):
                return {
                    "sql": generated_sql,
                    "summary": f"LLM-generated SQL for: '{user_query}'",
                    "chart_type": "table",
                    "x": None,
                    "y": None
                }
        except Exception as e:
            print(f"[!] Anthropic SQL generation error: {e}")

    if os.getenv("OPENAI_API_KEY"):
        try:
            from openai import OpenAI
            client = OpenAI()
            prompt = f"""
            You are a BigQuery/DuckDB SQL expert. Generate a single valid SQL query for this user question.
            Only return the SQL statement, no markdown, no explanations.
            {WAREHOUSE_SCHEMA_PROMPT}
            Question: {user_query}
            """
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0
            )
            generated_sql = response.choices[0].message.content.strip().replace("```sql", "").replace("```", "")
            return {
                "sql": generated_sql,
                "summary": f"Generated SQL query for: '{user_query}'",
                "chart_type": "table",
                "x": None,
                "y": None
            }
        except Exception as e:
            print(f"[!] LLM SQL generation error: {e}")

    # -------------------------------------------------------------------------
    # Fallback: Product & Category Overview
    # -------------------------------------------------------------------------
    sql = """SELECT 
    category,
    COUNT(product_id) AS total_products,
    ROUND(SUM(total_gross_revenue), 2) AS category_revenue,
    ROUND(AVG(gross_margin_percentage), 2) AS avg_profit_margin_pct
FROM marts.dim_products
GROUP BY category
ORDER BY category_revenue DESC;"""

    return {
        "sql": sql,
        "summary": f"Displaying general category performance & margin analysis for: '{user_query}'",
        "chart_type": "bar",
        "x": "category",
        "y": "category_revenue"
    }
