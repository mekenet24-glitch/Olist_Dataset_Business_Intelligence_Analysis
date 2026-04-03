# Power BI Connection Guide - Olist Intelligence

## Quick Connection Details

**Database Server Details:**
- **Host**: localhost
- **Port**: 3306
- **Database Name**: olist_intelligence
- **Username**: root
- **Password**: password

**Tables Available:**
- `orders_transformed` (99,441 records) - Main orders with derived metrics
- `customers` (99,441 records) - Customer information
- `order_items` (112,650 records) - Items per order
- `payments` (103,886 records) - Payment information
- `products` (32,951 records) - Product catalog

---

## Method 1: Direct Connection (Recommended)

### Step 1: Open Power BI Desktop
1. Launch **Power BI Desktop**
2. Click **Get Data** → **Database** → **MySQL Database**

### Step 2: Enter Connection Details
1. **Server**: `localhost`
2. **Database**: `olist_intelligence`
3. Click **OK**

### Step 3: Authentication
- **Username**: `root`
- **Password**: `password`
- Select "Connect"

### Step 4: Select Tables
Check these tables:
- ☐ orders_transformed
- ☐ customers
- ☐ order_items
- ☐ payments
- ☐ products

Click **Load**

---

## Method 2: Using Connection String

If Method 1 doesn't work, use this connection string:

```
Driver={MySQL ODBC 8.0 Driver};SERVER=localhost;PORT=3306;DATABASE=olist_intelligence;UID=root;PWD=password;
```

---

## Setting Up Table Relationships

After loading all tables, create these relationships in Power BI:

### Primary Relationships:
1. **orders_transformed ↔ customers**
   - From: `orders_transformed.customer_id`
   - To: `customers.customer_id`
   - Direction: Single (One-to-Many)

2. **orders_transformed ↔ order_items**
   - From: `orders_transformed.order_id`
   - To: `order_items.order_id`
   - Direction: Single (One-to-Many)

3. **orders_transformed ↔ payments**
   - From: `orders_transformed.order_id`
   - To: `payments.order_id`
   - Direction: Single (One-to-Many)

4. **order_items ↔ products**
   - From: `order_items.product_id`
   - To: `products.product_id`
   - Direction: Single (One-to-Many)

---

## Recommended Measures to Create

### In the Model Tab, create these calculated measures:

**Total Revenue:**
```DAX
Total Revenue = SUM('payments'[payment_value])
```

**Total Orders:**
```DAX
Total Orders = COUNTA('orders_transformed'[order_id])
```

**Delivery Rate:**
```DAX
Delivery Rate = 
    DIVIDE(
        COUNTIF('orders_transformed'[order_status], "delivered"),
        COUNTA('orders_transformed'[order_id]),
        0
    ) * 100
```

**Average Delivery Days:**
```DAX
Avg Delivery Days = AVERAGE('orders_transformed'[delivery_time_days])
```

**On-Time Delivery:**
```DAX
On-Time Delivery % = 
    DIVIDE(
        COUNTIF('orders_transformed'[delivered_on_time], 1),
        COUNTA('orders_transformed'[order_id]),
        0
    ) * 100
```

---

## Troubleshooting

### Issue: "MySQL driver not found"
**Solution**: 
1. Download MySQL ODBC Connector: https://dev.mysql.com/downloads/connector/odbc/
2. Run installer
3. Restart Power BI Desktop

### Issue: "Cannot connect to localhost"
**Solution**:
1. Verify MySQL is running: `mysql -u root -p`
2. Verify database exists: `SHOW DATABASES;`
3. If using remote connection, replace "localhost" with IP address

### Issue: "Access denied for user 'root'@'localhost'"
**Solution**:
1. Verify password is correct
2. Try connecting with MySQL Workbench first to confirm credentials

---

## Recommended Visualizations

### Dashboard 1: Orders Overview
- Total Orders (Card)
- Total Revenue (Card)
- Delivery Rate % (Card)
- Orders by Status (Pie Chart)
- Orders by Month (Line Chart)
- Top 10 States (Bar Chart)

### Dashboard 2: Delivery Performance
- Average Delivery Days (Card)
- On-Time Delivery % (Card)
- Delivery Days Distribution (Histogram)
- Delay Analysis by State (Map)
- Orders vs Delivery Time (Scatter)

### Dashboard 3: Customer & Products
- Total Customers (Card)
- Total Products (Card)
- Top 10 Categories (Bar)
- Customers by State (Map)
- Revenue by Product Category (Pie)

---

## Connection Security Note

⚠️ **For production use**, consider:
- Using environment variables for credentials
- Using Azure SQL Database instead of local MySQL
- Enabling SSL/TLS encryption
- Using separate read-only database users

---

## PowerShell Script to Export Connection Info

Save credentials securely in a JSON file for reference:

```powershell
$connectionInfo = @{
    Host = "localhost"
    Port = 3306
    Database = "olist_intelligence"
    Username = "root"
    Tables = @("orders_transformed", "customers", "order_items", "payments", "products")
    ConnectionString = "Driver={MySQL ODBC 8.0 Driver};SERVER=localhost;PORT=3306;DATABASE=olist_intelligence;UID=root;PWD=password;"
}

$connectionInfo | ConvertTo-Json | Out-File "power_bi_connection.json"
```

---

**For help, refer to:** https://learn.microsoft.com/en-us/power-bi/connect-data/desktop-connect-mysql
