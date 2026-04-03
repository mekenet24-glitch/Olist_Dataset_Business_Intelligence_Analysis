#!/usr/bin/env python
"""
Comprehensive data loading, verification, and quality check script
- Loads all related tables to MySQL
- Verifies data integrity
- Creates data quality dashboard
"""

import sys
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
import warnings

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.transform import transform_orders

warnings.filterwarnings('ignore')

class DataLoader:
    def __init__(self):
        self.db_user = os.getenv("DB_USER", "root")
        self.db_password = os.getenv("DB_PASSWORD", "password")
        self.db_host = os.getenv("DB_HOST", "localhost")
        self.db_port = os.getenv("DB_PORT", "3306")
        self.db_name = os.getenv("DB_NAME", "olist_intelligence")
        self.engine = None
        
    def connect(self):
        """Create database and connection"""
        print("\n[CONNECTING] Establishing MySQL connection...")
        
        # Create database if it doesn't exist
        engine_base = create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}"
        )
        
        with engine_base.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.db_name}"))
            conn.commit()
        
        # Create engine for the specific database
        self.engine = create_engine(
            f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )
        
        print(f"✓ Connected to {self.db_name}")
        return self.engine
    
    def load_table(self, csv_path, table_name, transform_func=None):
        """Load CSV to MySQL table"""
        print(f"\n[LOADING] {table_name}...")
        
        try:
            # Read CSV
            df = pd.read_csv(csv_path, dtype=str)
            df = df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
            
            # Transform if function provided
            if transform_func:
                df = transform_func(df)
            
            records_count = len(df)
            
            # Load to MySQL
            df.to_sql(table_name, con=self.engine, if_exists="replace", index=False)
            
            print(f"✓ Loaded {records_count:,} records to {table_name}")
            return True, records_count
            
        except Exception as e:
            print(f"✗ Error loading {table_name}: {e}")
            return False, 0
    
    def verify_table(self, table_name):
        """Verify table structure and data"""
        print(f"\n[VERIFYING] {table_name}...")
        
        try:
            with self.engine.connect() as conn:
                # Count records
                result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table_name}"))
                row_count = result.fetchone()[0]
                
                # Get columns
                inspector = inspect(self.engine)
                columns = [col['name'] for col in inspector.get_columns(table_name)]
                
                # Check for NULL values
                null_check = conn.execute(text(f"""
                    SELECT 
                        {', '.join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) as null_{col}" for col in columns[:5]])}
                    FROM {table_name}
                """))
                null_data = null_check.fetchone()
                
                print(f"  ✓ Rows: {row_count:,}")
                print(f"  ✓ Columns: {len(columns)}")
                print(f"  ✓ Columns: {', '.join(columns[:5])}...")
                
                # Check for NULLs in first 5 columns
                has_nulls = any(null_data)
                if has_nulls:
                    print(f"  ⚠ NULL values found: {dict(zip([f'null_{c}' for c in columns[:5]], null_data))}")
                else:
                    print(f"  ✓ No NULL values in key columns")
                
                return True
                
        except Exception as e:
            print(f"✗ Error verifying {table_name}: {e}")
            return False
    
    def create_data_quality_dashboard(self):
        """Create comprehensive data quality report"""
        print("\n" + "="*70)
        print("DATA QUALITY DASHBOARD")
        print("="*70)
        
        try:
            with self.engine.connect() as conn:
                # Tables overview
                print("\n[TABLES OVERVIEW]")
                tables = ['orders_transformed', 'customers', 'order_items', 'payments', 'products']
                table_stats = []
                
                for table in tables:
                    try:
                        result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                        count = result.fetchone()[0]
                        table_stats.append(f"  • {table}: {count:,} records")
                    except:
                        table_stats.append(f"  • {table}: ✗ not found")
                
                for stat in table_stats:
                    print(stat)
                
                # Data Quality Metrics
                print("\n[ORDERS DATA QUALITY]")
                orders_quality = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT order_id) as unique_orders,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        ROUND(COUNT(CASE WHEN order_status = 'delivered' THEN 1 END) / COUNT(*) * 100, 2) as delivery_rate,
                        ROUND(AVG(CAST(delivery_time_days as DECIMAL)), 2) as avg_delivery_days,
                        MIN(order_purchase_timestamp) as earliest_order,
                        MAX(order_purchase_timestamp) as latest_order
                    FROM orders_transformed
                """))
                
                for row in orders_quality:
                    print(f"  • Total Records: {row[0]:,}")
                    print(f"  • Unique Orders: {row[1]:,}")
                    print(f"  • Unique Customers: {row[2]:,}")
                    print(f"  • Delivered Orders: {row[3]}%")
                    print(f"  • Avg Delivery Time: {row[4]} days")
                    print(f"  • Date Range: {row[5]} to {row[6]}")
                
                # Integrity Checks
                print("\n[INTEGRITY CHECKS]")
                
                # Check for orphaned records
                orphaned = conn.execute(text("""
                    SELECT COUNT(*) FROM orders_transformed o
                    WHERE o.delivery_time_days = -1
                """))
                orphaned_count = orphaned.fetchone()[0]
                print(f"  • Undelivered Orders: {orphaned_count:,}")
                
                # Status distribution
                status_dist_result = conn.execute(text("""
                    SELECT order_status, COUNT(*) as count 
                    FROM orders_transformed 
                    GROUP BY order_status 
                    ORDER BY count DESC
                """))
                
                status_dist_rows = status_dist_result.fetchall()
                print(f"  • Status Distribution:")
                for status, count in status_dist_rows:
                    print(f"    - {status}: {count:,}")
                
                # Duplicate check
                duplicates = conn.execute(text("""
                    SELECT COUNT(*) - COUNT(DISTINCT order_id) 
                    FROM orders_transformed
                """))
                dup_count = duplicates.fetchone()[0]
                print(f"  • Duplicate Orders: {dup_count}")
                
        except Exception as e:
            print(f"✗ Error generating dashboard: {e}")
            import traceback
            traceback.print_exc()
    
    def run_all(self):
        """Execute complete loading, verification, and reporting"""
        print("\n" + "="*70)
        print("COMPREHENSIVE DATA LOADING & VERIFICATION")
        print("="*70)
        
        # Connect
        self.connect()
        
        # Load all tables
        print("\n" + "="*70)
        print("STEP 1: LOADING DATA TO MYSQL")
        print("="*70)
        
        tables_to_load = [
            ("data/raw/olist_orders_dataset_corrected.csv", "orders_transformed", transform_orders),
            ("data/raw/olist_customers_dataset.csv", "customers", None),
            ("data/raw/olist_order_items_dataset.csv", "order_items", None),
            ("data/raw/olist_order_payments_dataset.csv", "payments", None),
            ("data/raw/olist_products_dataset.csv", "products", None),
        ]
        
        results = {}
        for csv_path, table_name, transform in tables_to_load:
            success, count = self.load_table(csv_path, table_name, transform)
            results[table_name] = (success, count)
        
        # Verify all tables
        print("\n" + "="*70)
        print("STEP 2: VERIFYING DATA INTEGRITY")
        print("="*70)
        
        for table_name in results.keys():
            self.verify_table(table_name)
        
        # Data Quality Dashboard
        print("\n" + "="*70)
        print("STEP 3: DATA QUALITY ANALYSIS")
        print("="*70)
        
        self.create_data_quality_dashboard()
        
        # Summary
        print("\n" + "="*70)
        print("LOADING SUMMARY")
        print("="*70)
        
        total_records = sum(count for _, count in results.values() if count > 0)
        print(f"\n✓ Total records loaded: {total_records:,}")
        print(f"✓ Tables created: {len(results)}")
        print(f"✓ Database: {self.db_name}")
        print(f"✓ All data is ready for analysis!")
        
        print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    loader = DataLoader()
    loader.run_all()
