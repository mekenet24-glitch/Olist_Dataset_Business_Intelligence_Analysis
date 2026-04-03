import sys
from pathlib import Path

# Add parent directory to path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from src.db_connection import get_connection


# -----------------------------
# 1. CSV-BASED ANALYTICS
# -----------------------------
def run_kpis_csv(path):
    df = pd.read_csv(path)

    print("\n--- CSV BUSINESS KPIs ---")

    print("Total Revenue:", df["payment_value"].sum())
    print("Average Order Value:", df["payment_value"].mean())
    print("Total Orders:", df["order_id"].nunique())
    print(
        "Avg Revenue per Customer:",
        df.groupby("customer_id")["payment_value"].sum().mean()
    )


# -----------------------------
# 2. MYSQL-BASED ANALYTICS
# -----------------------------
def run_kpis_sql():
    conn = get_connection()
    cursor = conn.cursor()

    print("\n--- MYSQL BUSINESS KPIs ---")

    queries = {
        "Total Revenue": """
            SELECT SUM(payment_value) FROM olist_sales
        """,
        "Average Order Value": """
            SELECT AVG(payment_value) FROM olist_sales
        """,
        "Total Orders": """
            SELECT COUNT(DISTINCT order_id) FROM olist_sales
        """,
        "Top 5 States by Revenue": """
            SELECT customer_state, SUM(payment_value)
            FROM olist_sales
            GROUP BY customer_state
            ORDER BY SUM(payment_value) DESC
            LIMIT 5
        """,
        "Top 5 Customers": """
            SELECT customer_id, SUM(payment_value)
            FROM olist_sales
            GROUP BY customer_id
            ORDER BY SUM(payment_value) DESC
            LIMIT 5
        """
    }

    for name, query in queries.items():
        cursor.execute(query)
        result = cursor.fetchall()

        print(f"\n{name}:")
        for row in result:
            print(row)

    cursor.close()
    conn.close()


# -----------------------------
# MAIN ENTRY POINT
# -----------------------------
if __name__ == "__main__":
    run_kpis_csv("data/processed/final_dataset.csv")
    run_kpis_sql()