from pathlib import Path


def extract_sales_data() -> None:
    output_dir = Path("/opt/airflow/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_file = output_dir / "raw_sales.txt"
    raw_file.write_text("raw sales data extracted\n", encoding="utf-8")

    print("Extract step completed.")
    print(f"Created file: {raw_file}")