"""Four small, beginner-friendly Data Engineering functions."""

import os
import shutil
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parents[1]))
DATA = ROOT / "data"

FEATURES = ["items_count", "total_price", "distance_km", "is_express", "warehouse_load"]
TARGET = "late_delivery"
SOURCE_DIR_CANDIDATES = ("source", "raw")


def file_for(layer: str, run_date: str) -> Path:
    date.fromisoformat(run_date)  # Reject invalid dates before using the file path.
    if layer == "source":
        preferred = DATA / "source" / f"orders_{run_date}.csv"
        if preferred.exists():
            return preferred
        return DATA / "raw" / f"orders_{run_date}.csv"
    return DATA / layer / f"orders_{run_date}.csv"


def extract_to_bronze(run_date: str):
    source = file_for("source", run_date)
    target = file_for("bronze", run_date)
    if not source.exists():
        raise FileNotFoundError(f"Missing source: {source}. Run generate_data.py to add dates.")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)  # Bronze preserves the raw CSV exactly.
    print(f"Bronze: copied raw data for {run_date}")


def transform_to_silver(run_date: str):
    df = pd.read_csv(file_for("bronze", run_date))
    before = len(df)
    required_source_columns = {
        "order_id",
        "order_date",
        "items_count",
        "order_value_egp",
        "distance_km",
        "shipping_method",
        "warehouse_load_pct",
        TARGET,
    }
    missing = required_source_columns.difference(df.columns)
    if missing:
        raise ValueError(f"Missing source columns: {', '.join(sorted(missing))}")

    df["total_price"] = df["order_value_egp"]
    df["is_express"] = df["shipping_method"].isin(["Express", "Same_Day"]).astype(int)
    df["warehouse_load"] = df["warehouse_load_pct"]
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=FEATURES + [TARGET])
    df = df[(df["distance_km"] > 0) & (df["total_price"] > 0)]
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date.astype(str)
    target = file_for("silver", run_date)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False)  # Overwrite this day's partition: idempotent.
    print(f"Silver: {before} raw rows -> {len(df)} clean rows")


def prepare_gold(run_date: str):
    df = pd.read_csv(file_for("silver", run_date))
    columns = ["order_id", "order_date"] + FEATURES + [TARGET]
    gold = df[columns].copy()
    target = file_for("gold", run_date)
    target.parent.mkdir(parents=True, exist_ok=True)
    gold.to_csv(target, index=False)
    print(f"Gold: saved {len(gold)} rows with ML features and target")


def validate_gold(run_date: str):
    df = pd.read_csv(file_for("gold", run_date))
    assert len(df) >= 20, "Not enough clean orders"
    assert df["order_id"].is_unique, "Duplicate order IDs found"
    assert df[FEATURES + [TARGET]].notna().all().all(), "Missing values found"
    assert df[TARGET].isin([0, 1]).all(), "Unexpected target labels"
    print(f"Validation passed: {run_date}, {len(df)} orders")
