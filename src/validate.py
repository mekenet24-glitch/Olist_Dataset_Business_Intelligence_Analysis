import pandas as pd

def validate(path):
    df = pd.read_csv(path)

    print("\n--- DATA VALIDATION ---")
    print("Shape:", df.shape)
    print("\nColumns:", df.columns.tolist())
    print("\nMissing values:\n", df.isnull().sum())
    print("\nSample rows:\n", df.head())

if __name__ == "__main__":
    validate("data/processed/final_dataset.csv")