import os


def load_data(df, output_path):

    # ensure folder exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # save final dataset
    df.to_csv(output_path, index=False)

    print(f"Data loaded successfully to: {output_path}")