from __future__ import annotations

from pathlib import Path
import pandas as pd
import os
import logging

BASE_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow")

INPUT_FILE = Path(BASE_PATH) / "data/processed/sales_events_cleaned.csv"
OUTPUT_FILE = Path(BASE_PATH) / "data/warehouse/sales_summary_by_region.csv"

logger = logging.getLogger(__name__)

def load_staging_sales_summary() -> str:
    """
    Aggregate clean sales events into warehouse-ready regional summary.
    """

    logger.info("Start load_staging_sales_summary")

    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)

    expected_columns = {"region", "sales", "order_id", "event_id"}
    missing_expected = expected_columns - set(df.columns)
    if missing_expected:
        raise ValueError(f"Schema mismatch. Missing columns: {sorted(missing_expected)}")

    if df.empty:
        raise ValueError("No clean rows available for summary.")

    if (df["sales"] < 0).any():
        raise ValueError("Negative sales detected in cleaned input")

    if df["region"].isna().any():
        raise ValueError("Missing region detected in cleaned input")

    summary = (
        df.groupby("region", as_index=False)
        .agg(
            total_sales=("sales", "sum"),
            total_orders=("order_id", "nunique"),
            total_events=("event_id", "count"),
        )
        .sort_values(by="total_sales", ascending=False)
    )

    logger.info(f"Input clean rows: {len(df)}")
    logger.info(f"Output summary rows: {len(summary)}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUTPUT_FILE, index=False)

    logger.info(f"Loaded {len(df)} rows into warehouse")

    return str(OUTPUT_FILE)


if __name__ == "__main__":
    load_staging_sales_summary()
