import sys
from pathlib import Path
import os

# Add parent directory to path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db_connection import get_connection
import pandas as pd
from sqlalchemy import create_engine, text


def load_to_mysql(csv_path):

    # -----------------------
    # 1. LOAD DATA
    # -----------------------
    # Load orders from processed data
    orders = pd.read_csv(csv_path)
    orders = orders[["order_id", "customer_id", "payment_value", "customer_state"]]
    
    # Load customers from raw data
    customers = pd.read_csv("data/raw/olist_customers_dataset.csv")
    
    # Load payments from raw data
    payments = pd.read_csv("data/raw/olist_order_payments_dataset.csv")

    # -----------------------
    # 2. DB CONNECTION & SETUP
    # -----------------------
    # Get credentials from environment variables or use defaults
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "password")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = os.getenv("DB_PORT", "3306")
    db_name = os.getenv("DB_NAME", "olist_intelligence")
    
    # First, create engine without database to create the database if it doesn't exist
    engine_base = create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}"
    )
    
    # Create database if it doesn't exist
    with engine_base.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
        connection.commit()
    
    # Now create engine for the specific database
    engine = create_engine(
        f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    )

    # -----------------------
    # 3. LOAD DATA TO SQL
    # -----------------------
    orders.to_sql("orders", engine, if_exists="replace", index=False)
    customers.to_sql("customers", engine, if_exists="replace", index=False)
    payments.to_sql("order_payments", engine, if_exists="replace", index=False)

    print("SUCCESS: Data loaded into MySQL")


if __name__ == "__main__":
    load_to_mysql("data/processed/final_dataset.csv")