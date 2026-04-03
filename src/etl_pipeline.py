import sys
from pathlib import Path
from datetime import datetime
import os

# Add parent directory to path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.extract import extract_all
from src.transform import transform_orders, transform_order_items, transform_customers
import pandas as pd


def main():
    """
    ETL Pipeline with timestamp tracking for Power BI analysis
    """
    # Timestamp for this ETL run
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_path = "data/raw"
    output_path = "data/processed"
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    print(f"🚀 Starting ETL Pipeline at {run_timestamp}")
    
    # Extract all datasets
    customers, orders, order_items, payments, products = extract_all(data_path)
    
    # Transform orders
    orders_clean = transform_orders(orders)
    
    # Transform order items
    order_items_clean = transform_order_items(order_items)
    
    # Transform customers
    customers_clean = transform_customers(customers)
    
    # Add ETL timestamp to each dataset for Power BI tracking
    orders_clean["etl_timestamp"] = run_timestamp
    order_items_clean["etl_timestamp"] = run_timestamp
    customers_clean["etl_timestamp"] = run_timestamp
    
    # Save transformed datasets with timestamp
    timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    orders_clean.to_csv(f"{output_path}/orders_clean_{timestamp_suffix}.csv", index=False)
    order_items_clean.to_csv(f"{output_path}/order_items_clean_{timestamp_suffix}.csv", index=False)
    customers_clean.to_csv(f"{output_path}/customers_clean_{timestamp_suffix}.csv", index=False)
    
    # Merge for BI analysis
    bi_dataset = orders_clean.merge(customers_clean, on="customer_id", how="left")
    bi_dataset = bi_dataset.merge(order_items_clean, on="order_id", how="left")
    bi_dataset["etl_timestamp"] = run_timestamp
    
    bi_dataset.to_csv(f"{output_path}/bi_dataset_{timestamp_suffix}.csv", index=False)
    
    print(f"✅ ETL Pipeline completed successfully!")
    print(f"📊 Processed {len(orders_clean)} orders")
    print(f"💾 Saved datasets to {output_path}")
    print(f"⏰ ETL Timestamp: {run_timestamp}")


if __name__ == "__main__":
    main()