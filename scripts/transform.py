import pandas as pd
from pathlib import Path


def transform_sales_data():
    data_dir = Path("/opt/airflow/data")

    input_file = data_dir / "raw_sales.parquet"
    df = pd.read_parquet(input_file)

    # ตัวอย่าง transform
    df["Order Date"] = pd.to_datetime(df["Order Date"])

    summary = (
        df.groupby("Category")
        .agg(
            total_sales=("Sales", "sum"),
            total_orders=("Order ID", "count"),
        )
        .reset_index()
    )

    output_file = data_dir / "sales_summary.parquet"
    summary.to_parquet(output_file, index=False)

    print("Transform completed")