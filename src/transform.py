import pandas as pd
import numpy as np


def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Olist orders dataset:
    - Parse timestamps
    - Create time-based features
    - Generate delivery metrics
    - Basic cleanup
    """

    # =========================================================
    # 1. TIMESTAMP CONVERSION
    # =========================================================
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # =========================================================
    # 2. BASIC DATA CLEANING
    # =========================================================

    # Remove duplicate orders
    df = df.drop_duplicates(subset=["order_id"])

    # Optional: ensure valid purchase timestamps
    df = df.dropna(subset=["order_purchase_timestamp"])

    # =========================================================
    # 3. TIME FEATURE ENGINEERING
    # =========================================================

    df["purchase_year"] = df["order_purchase_timestamp"].dt.year
    df["purchase_month"] = df["order_purchase_timestamp"].dt.month
    df["purchase_day"] = df["order_purchase_timestamp"].dt.day
    df["purchase_week"] = df["order_purchase_timestamp"].dt.isocalendar().week
    df["purchase_dow"] = df["order_purchase_timestamp"].dt.day_name()

    df["purchase_date"] = df["order_purchase_timestamp"].dt.date

    # =========================================================
    # 4. DELIVERY METRICS
    # =========================================================

    df["delivery_time_days"] = (
        df["order_delivered_customer_date"] -
        df["order_purchase_timestamp"]
    ).dt.days

    df["estimated_vs_actual_days"] = (
        df["order_estimated_delivery_date"] -
        df["order_delivered_customer_date"]
    ).dt.days

    df["is_delayed"] = np.where(
        df["delivery_time_days"] >
        (df["order_estimated_delivery_date"] - df["order_purchase_timestamp"]).dt.days,
        1,
        0
    )

    # =========================================================
    # 5. BUSINESS FLAGS
    # =========================================================

    df["delivered_on_time"] = np.where(df["is_delayed"] == 0, 1, 0)

    df["has_delivery"] = np.where(
        df["order_delivered_customer_date"].notnull(),
        1,
        0
    )

    # =========================================================
    # 6. NULL HANDLING FOR ANALYTICS
    # =========================================================

    # Fill missing numeric metrics with -1 (common BI practice)
    df["delivery_time_days"] = df["delivery_time_days"].fillna(-1)
    df["estimated_vs_actual_days"] = df["estimated_vs_actual_days"].fillna(-1)

    # =========================================================
    # 7. FINAL OUTPUT SANITIZATION
    # =========================================================

    df = df.reset_index(drop=True)

    return df


def transform_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic cleanup for order items dataset
    """

    df = df.drop_duplicates()

    # Ensure numeric consistency
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")

    if "freight_value" in df.columns:
        df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce")

    df = df.fillna(0)

    return df


def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean customer dataset
    """

    df = df.drop_duplicates(subset=["customer_id"])

    # Standardize location fields
    if "customer_city" in df.columns:
        df["customer_city"] = df["customer_city"].str.lower().str.strip()

    if "customer_state" in df.columns:
        df["customer_state"] = df["customer_state"].str.upper().str.strip()

    return df


def transform_orders_with_payments(orders: pd.DataFrame, payments: pd.DataFrame, customers: pd.DataFrame) -> pd.DataFrame:
    """
    Transform orders with payment and customer data merged in.
    
    Steps:
    - Convert timestamp columns
    - Create time features
    - Create delivery metrics
    - Merge payments data
    - Merge customer geography
    - Create business KPIs
    """

    # STEP 1 — Convert timestamp columns
    timestamp_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]

    for col in timestamp_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col], errors="coerce")

    # STEP 2 — Create time features (for Power BI analysis)
    orders["purchase_year"] = orders["order_purchase_timestamp"].dt.year
    orders["purchase_month"] = orders["order_purchase_timestamp"].dt.month
    orders["purchase_day"] = orders["order_purchase_timestamp"].dt.day
    orders["purchase_week"] = orders["order_purchase_timestamp"].dt.isocalendar().week
    orders["purchase_dow"] = orders["order_purchase_timestamp"].dt.day_name()
    orders["purchase_date"] = orders["order_purchase_timestamp"].dt.date

    # STEP 3 — Create delivery_time_days (CORE KPI)
    orders["delivery_time_days"] = (
        orders["order_delivered_customer_date"] -
        orders["order_purchase_timestamp"]
    ).dt.days

    # STEP 4 — Create estimated delivery difference
    orders["estimated_delivery_days"] = (
        orders["order_estimated_delivery_date"] -
        orders["order_purchase_timestamp"]
    ).dt.days

    # STEP 5 — Create delay flag (is_delayed)
    orders["is_delayed"] = (
        orders["delivery_time_days"] > orders["estimated_delivery_days"]
    ).astype(int)

    # STEP 6 — Handle missing delivery values (some orders are not delivered yet)
    orders["delivery_time_days"] = orders["delivery_time_days"].fillna(-1)
    orders["estimated_delivery_days"] = orders["estimated_delivery_days"].fillna(-1)
    orders["is_delayed"] = orders["is_delayed"].fillna(0)

    # STEP 7 — Merge payments (to get revenue)
    payments_agg = payments.groupby("order_id", as_index=False)["payment_value"].sum()
    orders = orders.merge(payments_agg, on="order_id", how="left")

    # STEP 8 — Merge customers (for geography)
    orders = orders.merge(
        customers[["customer_id", "customer_state"]],
        on="customer_id",
        how="left"
    )

    # STEP 9 — Create business KPIs
    orders["order_count"] = 1

    # STEP 10 — Final cleanup
    orders = orders.drop_duplicates(subset=["order_id"])
    orders = orders.reset_index(drop=True)

    return orders
