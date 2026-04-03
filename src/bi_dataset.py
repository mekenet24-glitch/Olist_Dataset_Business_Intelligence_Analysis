import pandas as pd

def create_bi_dataset(path, output_path):
    df = pd.read_csv(path)

    bi_df = df.groupby("customer_state").agg(
        total_revenue=("payment_value", "sum"),
        avg_order_value=("payment_value", "mean"),
        total_orders=("order_id", "nunique")
    ).reset_index()

    bi_df.to_csv(output_path, index=False)

    print("BI dataset created at:", output_path)

if __name__ == "__main__":
    create_bi_dataset(
        "data/processed/final_dataset.csv",
        "data/processed/bi_dataset.csv"
    )