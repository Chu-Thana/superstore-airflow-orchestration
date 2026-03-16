from pathlib import Path
from datetime import datetime

def main():
    output_dir = Path("/opt/airflow/data")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "etl_output.txt"

    with output_file.open("w", encoding="utf-8") as f:
        f.write(f"ETL completed at {datetime.now().isoformat()}\n")

    print("ETL script executed successfully.")
    print(f"Output written to: {output_file}")


if __name__ == "__main__":
    main()