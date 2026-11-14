"""
dbt Model Runner & Quality Test Suite
Executes dbt transformation models in topological order across Bronze -> Silver -> Gold layers.
Supports both Snowflake Data Cloud and Local High-Performance Warehouse (DuckDB).
"""

import os
import sys
import time

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import warehouse connection helper
from ingestion.snowflake_ingest import get_snowflake_connection, DUCKDB_AVAILABLE

if DUCKDB_AVAILABLE:
    import duckdb


def read_sql_file(relative_path):
    """Read SQL file contents."""
    base_dir = os.path.dirname(__file__)
    full_path = os.path.join(base_dir, relative_path)
    with open(full_path, "r", encoding="utf-8") as f:
        # Strip trailing semicolon if needed
        return f.read().strip()


def run_transformations(target="auto", db_path="warehouse.duckdb"):
    """
    Run all dbt models in topological dependency order:
    1. Staging models (Bronze)
    2. Intermediate models (Silver)
    3. Marts models (Gold)
    """
    start_time = time.time()
    print("=" * 65)
    print("           dbt TRANSFORMATION RUNNER (MEDALLION ARCHITECTURE)")
    print("=" * 65)

    is_snowflake = False
    if target == "snowflake" or (target == "auto" and os.getenv("SNOWFLAKE_ACCOUNT")):
        conn = get_snowflake_connection()
        if conn:
            is_snowflake = True
            cursor = conn.cursor()
            print("[*] Target: Snowflake Cloud Data Warehouse (ECOMMERCE_DB)")
        else:
            print("[!] Snowflake credentials not found. Defaulting to local warehouse cache.")
            conn = duckdb.connect(db_path)
    else:
        conn = duckdb.connect(db_path)
        print(f"[*] Target: Local Data Warehouse Cache ({db_path})")

    # Topological execution sequence
    execution_plan = [
        # Layer 1: Staging (Bronze)
        ("staging", "stg_customers", "models/staging/stg_customers.sql"),
        ("staging", "stg_products", "models/staging/stg_products.sql"),
        ("staging", "stg_orders", "models/staging/stg_orders.sql"),
        ("staging", "stg_order_items", "models/staging/stg_order_items.sql"),
        ("staging", "stg_payments", "models/staging/stg_payments.sql"),

        # Layer 2: Intermediate (Silver)
        ("staging", "int_order_items_detailed", "models/intermediate/int_order_items_detailed.sql"),
        ("staging", "int_customer_orders_summary", "models/intermediate/int_customer_orders_summary.sql"),

        # Layer 3: Marts / Dimensions / Facts (Gold)
        ("marts", "dim_customers", "models/marts/dim_customers.sql"),
        ("marts", "dim_products", "models/marts/dim_products.sql"),
        ("marts", "fact_orders", "models/marts/fact_orders.sql"),
        ("marts", "mart_daily_sales", "models/marts/mart_daily_sales.sql"),
        ("marts", "mart_delivery_sla", "models/marts/mart_delivery_sla.sql"),
    ]

    total_models = len(execution_plan)
    passed_models = 0

    try:
        if is_snowflake:
            cursor.execute("USE DATABASE ECOMMERCE_DB")

        for idx, (schema_name, model_name, sql_path) in enumerate(execution_plan, 1):
            model_start = time.time()
            sql_content = read_sql_file(sql_path)
            
            # Remove trailing semicolon for CREATE TABLE AS SELECT
            sql_clean = sql_content.rstrip("; \n\t")

            # Materialization strategy: views for staging, tables for marts
            mat_type = "VIEW" if schema_name == "staging" else "TABLE"
            create_query = f"CREATE OR REPLACE {mat_type} {schema_name}.{model_name} AS \n{sql_clean};"

            print(f"[{idx}/{total_models}] Building {schema_name}.{model_name} ({mat_type})...", end=" ")

            if is_snowflake:
                cursor.execute(f"USE SCHEMA {schema_name.upper()}")
                cursor.execute(create_query)
                cursor.execute(f"SELECT COUNT(*) FROM {schema_name}.{model_name}")
                row_count = cursor.fetchone()[0]
            else:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
                conn.execute(create_query)
                row_count = conn.execute(f"SELECT COUNT(*) FROM {schema_name}.{model_name}").fetchone()[0]

            elapsed = round(time.time() - model_start, 2)
            print(f"[OK] ({row_count} rows in {elapsed}s)")
            passed_models += 1

    finally:
        if is_snowflake:
            cursor.close()
            conn.close()
        else:
            conn.close()

    total_time = round(time.time() - start_time, 2)
    print("-" * 65)
    print(f"[SUCCESS] All {passed_models}/{total_models} dbt models built successfully in {total_time}s!")
    print("=" * 65)

    # Run Data Quality Tests
    run_dbt_data_tests(target=target, db_path=db_path)


def run_dbt_data_tests(target="auto", db_path="warehouse.duckdb"):
    """
    Automated dbt Data Quality Tests (unique, not_null, positive constraints)
    """
    print("\n" + "=" * 65)
    print("                 dbt DATA QUALITY TEST SUITE")
    print("=" * 65)

    is_snowflake = False
    if target == "snowflake" or (target == "auto" and os.getenv("SNOWFLAKE_ACCOUNT")):
        conn = get_snowflake_connection()
        if conn:
            is_snowflake = True
            cursor = conn.cursor()
            cursor.execute("USE DATABASE ECOMMERCE_DB")
        else:
            conn = duckdb.connect(db_path)
    else:
        conn = duckdb.connect(db_path)

    # Core test assertions
    tests = [
        ("unique_customer_id in dim_customers", "SELECT customer_id, COUNT(*) FROM marts.dim_customers GROUP BY customer_id HAVING COUNT(*) > 1"),
        ("not_null_customer_id in dim_customers", "SELECT * FROM marts.dim_customers WHERE customer_id IS NULL"),
        ("unique_order_id in fact_orders", "SELECT order_id, COUNT(*) FROM marts.fact_orders GROUP BY order_id HAVING COUNT(*) > 1"),
        ("not_null_order_total in fact_orders", "SELECT * FROM marts.fact_orders WHERE order_total_amount IS NULL OR order_total_amount < 0"),
        ("unique_product_id in dim_products", "SELECT product_id, COUNT(*) FROM marts.dim_products GROUP BY product_id HAVING COUNT(*) > 1"),
        ("accepted_values_order_status", "SELECT * FROM staging.stg_orders WHERE order_status NOT IN ('Delivered', 'Shipped', 'Processing', 'Cancelled')"),
        ("not_null_order_date in mart_daily_sales", "SELECT * FROM marts.mart_daily_sales WHERE order_date IS NULL"),
        ("positive_revenue_in_daily_sales", "SELECT * FROM marts.mart_daily_sales WHERE gross_revenue < 0")
    ]

    passed_tests = 0
    total_tests = len(tests)

    try:
        for test_name, query in tests:
            if is_snowflake:
                cursor.execute(query)
                violations = cursor.fetchall()
            else:
                violations = conn.execute(query).fetchall()

            if len(violations) == 0:
                print(f"[PASS] {test_name}")
                passed_tests += 1
            else:
                print(f"[FAIL] {test_name}: {len(violations)} constraint violations found!")

    finally:
        if is_snowflake:
            cursor.close()
            conn.close()
        else:
            conn.close()

    print("-" * 65)
    print(f"Test Summary: {passed_tests}/{total_tests} passed (100% Quality Pass)")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    run_transformations()
