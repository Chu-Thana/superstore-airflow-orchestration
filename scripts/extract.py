from pathlib import Path
import pandas as pd


def extract_sales_data():
    data_dir = Path("/opt/airflow/data")
    input_file = data_dir / "Superstore.csv"

    try:
        df = pd.read_csv(input_file)
    except UnicodeDecodeError:
        df = pd.read_csv(input_file, encoding="latin1")

    output_file = data_dir / "sales_data.parquet"
    df.to_parquet(output_file, index=False)

    print(f"Extracted {len(df)} rows")
    print(f"Saved to {output_file}")