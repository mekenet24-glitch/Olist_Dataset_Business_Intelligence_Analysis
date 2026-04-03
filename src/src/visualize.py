import pandas as pd
import matplotlib.pyplot as plt


def load_data(path):
    return pd.read_csv(path)


# -----------------------------
# 1. Revenue by State
# -----------------------------
def revenue_by_state(df):
    grouped = df.groupby("customer_state")["payment_value"].sum().sort_values(ascending=False)

    plt.figure()
    grouped.head(10).plot(kind="bar")
    plt.title("Top 10 States by Revenue")
    plt.xlabel("State")
    plt.ylabel("Revenue")
    plt.xticks(rotation=45)

    plt.show()


# -----------------------------
# 2. Top Customers
# -----------------------------
def top_customers(df):
    grouped = df.groupby("customer_id")["payment_value"].sum().sort_values(ascending=False)

    plt.figure()
    grouped.head(10).plot(kind="bar")
    plt.title("Top 10 Customers by Revenue")
    plt.xlabel("Customer ID")
    plt.ylabel("Revenue")

    plt.show()


# -----------------------------
# 3. Payment Distribution
# -----------------------------
def payment_distribution(df):
    plt.figure()
    df["payment_value"].plot(kind="hist", bins=30)
    plt.title("Payment Value Distribution")
    plt.xlabel("Payment Value")

    plt.show()


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    df = load_data("data/processed/final_dataset.csv")

    revenue_by_state(df)
    top_customers(df)
    payment_distribution(df)