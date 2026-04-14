from __future__ import annotations

from pathlib import Path
import pandas as pd
import os

BASE_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow")

INPUT_FILE = Path(BASE_PATH) / "data/processed/sales_events_extracted.csv"
OUTPUT_FILE = Path(BASE_PATH) / "data/processed/sales_events_cleaned.csv"

def transform_staging_sales() -> str:
    """
    Clean staging data:
    - remove duplicates flagged by consumer
    - validate required columns
    - cast data types
    - keep only valid sales rows
    """
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    required_cols = ["event_id", "order_id", "region", "sales", "event_time", "is_duplicate"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df["is_duplicate"] = pd.to_numeric(df["is_duplicate"], errors="coerce").fillna(1).astype(int)
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    df = df.dropna(subset=["event_id", "order_id", "region", "sales", "event_time"])
    df = df[df["sales"] >= 0]
    df = df[df["is_duplicate"] == 0]

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Transformed {len(df)} clean rows to {OUTPUT_FILE}")
    return str(OUTPUT_FILE)


if __name__ == "__main__":
    transform_staging_sales()