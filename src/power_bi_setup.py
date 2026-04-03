#!/usr/bin/env python
"""
Power BI Connection Verification & Configuration Script
- Verifies MySQL connection
- Checks Power BI prerequisites
- Exports metadata for Power BI
- Creates sample DAX reference file
"""

import sys
from pathlib import Path
import os
import subprocess
import json
import pandas as pd
from sqlalchemy import create_engine, text, inspect

sys.path.insert(0, str(Path(__file__).parent.parent))

class PowerBISetup:
    def __init__(self):
        self.db_user = "root"
        self.db_password = "password"
        self.db_host = "localhost"
        self.db_port = "3306"
        self.db_name = "olist_intelligence"
        self.engine = None
        
    def verify_mysql_connection(self):
        """Verify MySQL connection is working"""
        print("\n[CHECKING] MySQL Connection...")
        
        try:
            self.engine = create_engine(
                f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
            )
            
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                print("✓ MySQL connection successful!")
                return True
                
        except Exception as e:
            print(f"✗ MySQL connection failed: {e}")
            print("   Make sure MySQL service is running")
            return False
    
    def get_table_metadata(self):
        """Extract table metadata for Power BI"""
        print("\n[EXTRACTING] Table Metadata...")
        
        metadata = {}
        
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            
            for table in tables:
                columns = inspector.get_columns(table)
                metadata[table] = {
                    "columns": [col['name'] for col in columns],
                    "column_types": {col['name']: str(col['type']) for col in columns}
                }
                print(f"✓ {table}: {len(columns)} columns")
            
            return metadata
            
        except Exception as e:
            print(f"✗ Error extracting metadata: {e}")
            return {}
    
    def get_table_statistics(self):
        """Get row counts and basic statistics"""
        print("\n[GATHERING] Table Statistics...")
        
        stats = {}
        
        try:
            with self.engine.connect() as conn:
                tables = ['orders_transformed', 'customers', 'order_items', 'payments', 'products']
                
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) as count FROM {table}"))
                    count = result.fetchone()[0]
                    stats[table] = {
                        "row_count": count,
                        "last_updated": "2026-04-02"
                    }
                    print(f"✓ {table}: {count:,} rows")
            
            return stats
            
        except Exception as e:
            print(f"✗ Error getting statistics: {e}")
            return {}
    
    def export_connection_config(self, metadata, stats):
        """Export connection configuration for Power BI"""
        print("\n[EXPORTING] Connection Configuration...")
        
        config = {
            "connection": {
                "type": "MySQL",
                "host": self.db_host,
                "port": int(self.db_port),
                "database": self.db_name,
                "username": self.db_user,
                "connection_string": f"Driver={{MySQL ODBC 8.0 Driver}};SERVER={self.db_host};PORT={self.db_port};DATABASE={self.db_name};UID={self.db_user};PWD={self.db_password};"
            },
            "tables": {
                table: {
                    "row_count": stats.get(table, {}).get("row_count", 0),
                    "columns": metadata.get(table, {}).get("columns", [])
                }
                for table in stats.keys()
            },
            "relationships": [
                {
                    "name": "Orders to Customers",
                    "from_table": "orders_transformed",
                    "from_column": "customer_id",
                    "to_table": "customers",
                    "to_column": "customer_id"
                },
                {
                    "name": "Orders to Order Items",
                    "from_table": "orders_transformed",
                    "from_column": "order_id",
                    "to_table": "order_items",
                    "to_column": "order_id"
                },
                {
                    "name": "Orders to Payments",
                    "from_table": "orders_transformed",
                    "from_column": "order_id",
                    "to_table": "payments",
                    "to_column": "order_id"
                },
                {
                    "name": "Order Items to Products",
                    "from_table": "order_items",
                    "from_column": "product_id",
                    "to_table": "products",
                    "to_column": "product_id"
                }
            ]
        }
        
        config_path = "power_bi_config.json"
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✓ Exported to {config_path}")
        return config
    
    def check_power_bi_driver(self):
        """Check if MySQL ODBC driver is installed"""
        print("\n[CHECKING] MySQL ODBC Driver...")
        
        try:
            import pyodbc
            drivers = pyodbc.drivers()
            mysql_drivers = [d for d in drivers if 'mysql' in d.lower()]
            
            if mysql_drivers:
                print(f"✓ MySQL ODBC drivers found: {mysql_drivers}")
                return True
            else:
                print("✗ MySQL ODBC driver not found")
                print("   Download from: https://dev.mysql.com/downloads/connector/odbc/")
                return False
                
        except ImportError:
            print("⚠ pyodbc not installed (optional - Power BI has built-in MySQL support)")
            return None
    
    def create_dax_reference(self):
        """Create DAX reference file for common measures"""
        print("\n[CREATING] DAX Reference File...")
        
        dax_content = """// DAX MEASURES FOR POWER BI - Olist Intelligence

// ============================================================
// REVENUE MEASURES
// ============================================================

Total Revenue = SUM('payments'[payment_value])

Revenue by Status = 
    SUMIF('orders_transformed'[order_status], SELECTEDVALUE('orders_transformed'[order_status]), 'payments'[payment_value])

Revenue Growth MoM = 
    DIVIDE(
        [Total Revenue] - CALCULATE([Total Revenue], PREVIOUSMONTH('date_table'[Date])),
        CALCULATE([Total Revenue], PREVIOUSMONTH('date_table'[Date]))
    )


// ============================================================
// ORDER MEASURES
// ============================================================

Total Orders = COUNTA('orders_transformed'[order_id])

Unique Customers = DISTINCT('customers'[customer_id])

Orders by Status = 
    COUNTIF('orders_transformed'[order_status], SELECTEDVALUE('orders_transformed'[order_status]))

Average Order Value = 
    DIVIDE([Total Revenue], [Total Orders])


// ============================================================
// DELIVERY PERFORMANCE
// ============================================================

Delivery Rate = 
    DIVIDE(
        COUNTIF('orders_transformed'[order_status], "delivered"),
        [Total Orders]
    ) * 100

On-Time Delivery % = 
    DIVIDE(
        COUNTIF('orders_transformed'[delivered_on_time], 1),
        [Total Orders]
    ) * 100

Average Delivery Days = AVERAGE('orders_transformed'[delivery_time_days])

Delayed Orders = COUNTIF('orders_transformed'[is_delayed], 1)


// ============================================================
// GEOGRAPHIC MEASURES
// ============================================================

Total States = DISTINCT('customers'[customer_state])

Revenue by State = 
    SUMIF('customers'[customer_state], 
          SELECTEDVALUE('customers'[customer_state]),
          RELATED('payments'[payment_value]))


// ============================================================
// PRODUCT MEASURES
// ============================================================

Total Products = DISTINCT('products'[product_id])

Revenue by Category = 
    SUMIF('products'[product_category_name],
          SELECTEDVALUE('products'[product_category_name]),
          RELATED('payments'[payment_value]))

Top Category = 
    MAXX(
        TOPN(1, VALUES('products'[product_category_name]), [Revenue by Category]),
        'products'[product_category_name]
    )


// ============================================================
// PAYMENT ANALYSIS
// ============================================================

Payment Method Distribution = 
    COUNTIF('payments'[payment_type], 
            SELECTEDVALUE('payments'[payment_type]))

Average Installments = AVERAGE('payments'[payment_installments])

Credit Card Orders = 
    COUNTIF('payments'[payment_type], "credit_card")


// ============================================================
// TIME-BASED MEASURES
// ============================================================

Orders This Month = 
    CALCULATE(
        [Total Orders],
        DATESMTD(EOMONTH(TODAY(), 0))
    )

Orders Last Month = 
    CALCULATE(
        [Total Orders],
        DATESMTD(EOMONTH(TODAY(), -1))
    )

MoM Growth = 
    DIVIDE(
        [Orders This Month] - [Orders Last Month],
        [Orders Last Month]
    ) * 100
"""
        
        dax_path = "DAX_REFERENCE.txt"
        with open(dax_path, 'w') as f:
            f.write(dax_content)
        
        print(f"✓ Created DAX reference: {dax_path}")
    
    def run_all(self):
        """Execute all setup checks and exports"""
        print("\n" + "="*70)
        print("POWER BI SETUP VERIFICATION")
        print("="*70)
        
        # Verify MySQL
        if not self.verify_mysql_connection():
            print("\n✗ Cannot proceed without MySQL connection")
            return False
        
        # Get metadata
        metadata = self.get_table_metadata()
        if not metadata:
            return False
        
        # Get statistics
        stats = self.get_table_statistics()
        if not stats:
            return False
        
        # Export config
        config = self.export_connection_config(metadata, stats)
        
        # Check driver
        self.check_power_bi_driver()
        
        # Create DAX reference
        self.create_dax_reference()
        
        # Summary
        print("\n" + "="*70)
        print("SETUP COMPLETE")
        print("="*70)
        print("\n✓ MySQL Connection: VERIFIED")
        print(f"✓ Tables Ready: {len(stats)}")
        print(f"✓ Total Records: {sum(s['row_count'] for s in stats.values()):,}")
        print("✓ Configuration Exported: power_bi_config.json")
        print("✓ DAX Reference Created: DAX_REFERENCE.txt")
        print("\n📊 Next Steps:")
        print("   1. Open Power BI Desktop")
        print("   2. Get Data → MySQL Database")
        print("   3. Enter: localhost, olist_intelligence")
        print("   4. Username: root, Password: password")
        print("   5. Select all tables and Load")
        print("   6. Use DAX_REFERENCE.txt for measure creation")
        print("\n" + "="*70 + "\n")
        
        return True


if __name__ == "__main__":
    setup = PowerBISetup()
    success = setup.run_all()
    sys.exit(0 if success else 1)
