#!/usr/bin/env python
"""
Script to correct CSV formatting inconsistencies and load corrected data to MySQL
"""

import sys
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transform import transform_orders

def correct_and_load_orders():
    """
    1. Load and correct orders CSV (fix quoting inconsistencies)
    2. Transform the data
    3. Load to MySQL
    """
    
    print("\n" + "="*60)
    print("CORRECTING CSV AND LOADING TO MYSQL")
    print("="*60)
    
    # =========================================================
    # 1. READ AND CORRECT CSV
    # =========================================================
    print("\n[STEP 1] Reading and correcting orders CSV...")
    
    try:
        # Read raw CSV with all columns as strings initially
        orders = pd.read_csv(
            "data/raw/olist_orders_dataset.csv",
            dtype=str,
            quotechar='"',
            skipinitialspace=True
        )
        
        # Strip whitespace from all string columns
        orders = orders.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        print(f"✓ Loaded {len(orders)} records")
        print(f"✓ Columns: {list(orders.columns)}")
        
        # Write corrected CSV back to raw folder
        corrected_path = "data/raw/olist_orders_dataset_corrected.csv"
        orders.to_csv(corrected_path, index=False, quoting=1)  # quoting=1 = QUOTE_ALL
        print(f"✓ Saved corrected CSV to {corrected_path}")
        
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")
        return False
    
    # =========================================================
    # 2. TRANSFORM DATA
    # =========================================================
    print("\n[STEP 2] Transforming orders data...")
    
    try:
        orders_transformed = transform_orders(orders)
        print(f"✓ Transformed {len(orders_transformed)} records")
        print(f"✓ Shape: {orders_transformed.shape}")
        
        # Save transformed data
        processed_path = "data/processed/orders_transformed.csv"
        orders_transformed.to_csv(processed_path, index=False)
        print(f"✓ Saved to {processed_path}")
        
    except Exception as e:
        print(f"✗ Error transforming data: {e}")
        return False
    
    # =========================================================
    # 3. CONNECT TO MYSQL AND LOAD DATA
    # =========================================================
    print("\n[STEP 3] Loading to MySQL...")
    
    try:
        # Get DB credentials
        db_user = os.getenv("DB_USER", "root")
        db_password = os.getenv("DB_PASSWORD", "password")
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "3306")
        db_name = os.getenv("DB_NAME", "olist_intelligence")
        
        # Create engine without database first
        engine_base = create_engine(
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}"
        )
        
        # Create database if it doesn't exist
        with engine_base.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            conn.commit()
        print(f"✓ Database created/verified: {db_name}")
        
        # Create engine for the specific database
        engine = create_engine(
            f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        )
        
        # Load transformed orders to MySQL
        table_name = "orders_transformed"
        orders_transformed.to_sql(
            table_name,
            con=engine,
            if_exists="replace",
            index=False
        )
        print(f"✓ Loaded {len(orders_transformed)} records to table: {table_name}")
        
        # Verify load
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table_name}"))
            row_count = result.fetchone()[0]
            print(f"✓ Verified: {row_count} records in database")
        
        engine.dispose()
        
    except Exception as e:
        print(f"✗ Error loading to MySQL: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "="*60)
    print("✓ SUCCESS: Data corrected and loaded to MySQL!")
    print("="*60 + "\n")
    
    return True


if __name__ == "__main__":
    success = correct_and_load_orders()
    sys.exit(0 if success else 1)
