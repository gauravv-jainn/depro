"""
End-to-End Pipeline Verification Test Suite
Runs unit and integration checks across Ingestion, dbt Models, Quality Gates, and BI Engine.
"""

import os
import sys
import unittest
import pandas as pd

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from bi_interface.db_connector import execute_query, get_table_counts
from bi_interface.conversational_engine import parse_natural_language_query


class TestDataPipeline(unittest.TestCase):

    def test_01_raw_tables_populated(self):
        """Verify that all raw tables are populated with data."""
        counts = get_table_counts()
        self.assertGreater(counts.get("raw.customers", 0), 0)
        self.assertGreater(counts.get("raw.products", 0), 0)
        self.assertGreater(counts.get("raw.orders", 0), 0)
        self.assertGreater(counts.get("raw.order_items", 0), 0)
        self.assertGreater(counts.get("raw.payments", 0), 0)

    def test_02_gold_marts_populated(self):
        """Verify that dbt gold marts exist and have rows."""
        counts = get_table_counts()
        self.assertGreater(counts.get("marts.dim_customers", 0), 0)
        self.assertGreater(counts.get("marts.dim_products", 0), 0)
        self.assertGreater(counts.get("marts.fact_orders", 0), 0)
        self.assertGreater(counts.get("marts.mart_daily_sales", 0), 0)
        self.assertGreater(counts.get("marts.mart_delivery_sla", 0), 0)

    def test_03_fact_orders_metrics(self):
        """Verify fact_orders calculation integrity."""
        df = execute_query("""
            SELECT 
                COUNT(*) AS cnt,
                SUM(order_total_amount) AS total_val,
                AVG(order_total_amount) AS avg_val
            FROM marts.fact_orders;
        """)
        self.assertFalse(df.empty)
        self.assertGreater(df["cnt"].iloc[0], 100)
        self.assertGreater(df["total_val"].iloc[0], 0)
        self.assertGreater(df["avg_val"].iloc[0], 0)

    def test_04_conversational_parser(self):
        """Verify conversational engine parses queries accurately."""
        res1 = parse_natural_language_query("What are the top products by revenue?")
        self.assertIn("SELECT", res1["sql"].upper())
        self.assertEqual(res1["chart_type"], "bar")

        res2 = parse_natural_language_query("Show me customer RFM segmentation")
        self.assertIn("dim_customers", res2["sql"])
        self.assertEqual(res2["chart_type"], "pie")

        res3 = parse_natural_language_query("What is the delivery delay rate by city?")
        self.assertIn("mart_delivery_sla", res3["sql"])

    def test_05_database_query_execution(self):
        """Verify database connector returns valid pandas DataFrame."""
        df = execute_query("SELECT customer_id, full_name, customer_segment FROM marts.dim_customers LIMIT 5;")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 5)
        self.assertIn("customer_id", df.columns)
        self.assertIn("customer_segment", df.columns)


if __name__ == "__main__":
    unittest.main()
