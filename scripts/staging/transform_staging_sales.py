from __future__ import annotations
from pathlib import Path
import pandas as pd
import os
import logging
import boto3

BASE_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow")

INPUT_FILE = Path(BASE_PATH) / "data/processed/sales_events_extracted.csv"
OUTPUT_FILE = Path(BASE_PATH) / "data/processed/sales_events_cleaned.csv"

logger = logging.getLogger(__name__)

def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    s3 = boto3.client("s3")
    s3.upload_file(local_path, bucket, key)
    logger.info(f"Uploaded {local_path} to s3://{bucket}/{key}")

def transform_staging_sales() -> str:
    """
    Clean staging data:
    - remove duplicates flagged by consumer
    - validate required columns
    - cast data types
    - keep only valid sales rows
    """

    logger.info("Start transform_staging_sales")

    if not INPUT_FILE.exists():
        logger.error(f"Input file not found: {INPUT_FILE}")
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Loaded {len(df)} rows")

    expected_columns = {
        "event_id",
        "order_id",
        "region",
        "sales",
        "event_time",
        "is_duplicate",
    }

    missing_expected = expected_columns - set(df.columns)
    if missing_expected:
        raise ValueError(f"Schema mismatch. Missing columns: {sorted(missing_expected)}")

    required_cols = ["event_id", "order_id", "region", "sales", "event_time", "is_duplicate"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    df["sales"] = pd.to_numeric(df["sales"], errors="coerce")
    df["is_duplicate"] = pd.to_numeric(df["is_duplicate"], errors="coerce").fillna(1).astype(int)
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    if (df["sales"] < 0).any():
        raise ValueError("Negative sales detected")

    if df["region"].isnull().any():
        raise ValueError("Missing region detected")

    raw_count = len(df)

    df = df.dropna(subset=["event_id", "order_id", "region", "sales", "event_time"])
    df = df[df["sales"] >= 0]
    df = df[df["is_duplicate"] == 0]

    clean_count = len(df)
    dropped_count = raw_count - clean_count

    logger.info(f"Raw rows before cleaning: {raw_count}")
    logger.info(f"Clean rows after cleaning: {clean_count}")
    logger.info(f"Dropped rows during cleaning: {dropped_count}")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"After cleaning: {len(df)} rows")

    df.to_csv(OUTPUT_FILE, index=False)

    SILVER_S3_BUCKET = "sales-analytics-lakehouse-thana"
    SILVER_S3_KEY = "silver/sales_cleaned.csv"

    upload_to_s3(
        str(OUTPUT_FILE),
        SILVER_S3_BUCKET,
        SILVER_S3_KEY
    )

    logger.info(f"Uploaded silver dataset to s3://{SILVER_S3_BUCKET}/{SILVER_S3_KEY}")

    logger.info("Transform completed successfully")
    return str(OUTPUT_FILE)




if __name__ == "__main__":
    transform_staging_sales()