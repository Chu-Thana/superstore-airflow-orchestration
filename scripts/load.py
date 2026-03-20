from pathlib import Path


def load_sales_summary() -> None:
    data_dir = Path("/opt/airflow/data")
    transformed_file = data_dir / "transformed_sales.txt"
    final_file = data_dir / "sales_summary.txt"

    if not transformed_file.exists():
        raise FileNotFoundError(f"Missing input file: {transformed_file}")

    content = transformed_file.read_text(encoding="utf-8")
    final_file.write_text(f"FINAL OUTPUT:\n{content}", encoding="utf-8")

    print("Load step completed.")
    print(f"Created file: {final_file}")