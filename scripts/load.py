import pandas as pd
from pathlib import Path


def load_sales_summary():
    data_dir = Path("/opt/airflow/data")

    input_file = data_dir / "sales_summary.parquet"
    df = pd.read_parquet(input_file)

    output_file = data_dir / "final_output.csv"
    df.to_csv(output_file, index=False)

    print("Load completed")
    print(df.head())