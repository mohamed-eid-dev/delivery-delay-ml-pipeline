from pathlib import Path

import pandas as pd

from src import etl


RUN_DATE = "2026-10-20"


def test_file_for_accepts_packaged_raw_date(tmp_path, monkeypatch):
    monkeypatch.setattr(etl, "DATA", tmp_path)
    (tmp_path / "raw").mkdir()
    source = tmp_path / "raw" / f"orders_{RUN_DATE}.csv"
    source.write_text("order_id\n1\n", encoding="utf-8")

    assert etl.file_for("source", RUN_DATE) == source


def test_etl_medallion_flow_creates_ml_columns(tmp_path, monkeypatch):
    monkeypatch.setattr(etl, "DATA", tmp_path)
    source_dir = tmp_path / "raw"
    source_dir.mkdir()
    source = source_dir / f"orders_{RUN_DATE}.csv"
    pd.DataFrame(
        {
            "order_id": ["A1", "A1", "A2"],
            "order_date": [RUN_DATE] * 3,
            "items_count": [1, 1, 3],
            "order_value_egp": [100.0, 100.0, 250.0],
            "distance_km": [10.0, 10.0, 25.0],
            "shipping_method": ["Express", "Express", "Standard"],
            "warehouse_load_pct": [50.0, 50.0, 70.0],
            "late_delivery": [0, 0, 1],
        }
    ).to_csv(source, index=False)

    etl.extract_to_bronze(RUN_DATE)
    etl.transform_to_silver(RUN_DATE)
    etl.prepare_gold(RUN_DATE)

    gold = pd.read_csv(tmp_path / "gold" / f"orders_{RUN_DATE}.csv")
    assert gold["order_id"].is_unique
    assert gold["is_express"].tolist() == [1, 0]
    assert gold["total_price"].tolist() == [100.0, 250.0]
