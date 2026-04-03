import pandas as pd

def extract_all(data_path):

    customers = pd.read_csv(f"{data_path}/olist_customers_dataset.csv")
    orders = pd.read_csv(f"{data_path}/olist_orders_dataset.csv")
    order_items = pd.read_csv(f"{data_path}/olist_order_items_dataset.csv")
    payments = pd.read_csv(f"{data_path}/olist_order_payments_dataset.csv")
    products = pd.read_csv(f"{data_path}/olist_products_dataset.csv")

    print("All datasets extracted successfully")

    return customers, orders, order_items, payments, products