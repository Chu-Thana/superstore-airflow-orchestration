from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import os

BASE_PATH = os.getenv("AIRFLOW_DATA_PATH", "/opt/airflow")
STAGING_FILE = Path(BASE_PATH) / "staging/staging_sales_events.jsonl"
OUTPUT_FILE = Path(BASE_PATH) / "data/processed/sales_events_extracted.csv"

def extract_staging_sales() -> str:
    """
    Read raw JSONL events from staging and convert them to CSV for downstream tasks.
    Returns output file path.
    """
    if not STAGING_FILE.exists():
        raise FileNotFoundError(f"Staging file not found: {STAGING_FILE}")

    rows = []
    with open(STAGING_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))

    if not rows:
        raise ValueError("Staging file exists but contains no records.")

    df = pd.DataFrame(rows)
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Extracted {len(df)} rows to {OUTPUT_FILE}")
    return str(OUTPUT_FILE)


if __name__ == "__main__":
    extract_staging_sales()