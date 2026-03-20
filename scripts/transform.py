from pathlib import Path


def transform_sales_data() -> None:
    data_dir = Path("/opt/airflow/data")
    raw_file = data_dir / "raw_sales.txt"
    transformed_file = data_dir / "transformed_sales.txt"

    if not raw_file.exists():
        raise FileNotFoundError(f"Missing input file: {raw_file}")

    content = raw_file.read_text(encoding="utf-8")
    transformed_file.write_text(content.upper(), encoding="utf-8")

    print("Transform step completed.")
    print(f"Created file: {transformed_file}")