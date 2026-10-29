"""Đường dẫn trong container và số đối chiếu handover. Repo Analytics tự chứa gold."""
import os

GOLD_DIR = "/data/gold"
OUT_DIR = "/data/analytics"
SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
REPORT_DIR = "/opt/reports"

EXPECTED_REVENUE = 13494400.74
EXPECTED_KPI_MONTHS = 20
EXPECTED_RFM_CUSTOMERS = 94983

GOLD_TABLES = [
    "fact_orders",
    "fact_order_items",
    "agg_sales_monthly",
    "agg_category_monthly",
    "agg_customer_state",
    "agg_seller_performance",
    "agg_customer_rfm",
]