"""A tiny ML pipeline: prepare, train, evaluate, save."""

import json
import os
import shutil
from pathlib import Path
 
import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from src.etl import FEATURES, TARGET

ROOT = Path(os.environ.get("PROJECT_DIR", Path(__file__).resolve().parents[1]))


def prepare_features():
    files = sorted((ROOT / "data" / "gold").glob("orders_*.csv"))
    if not files:
        raise ValueError("No Gold files yet. Run the Data DAG first!")
    df = pd.concat([pd.read_csv(file) for file in files], ignore_index=True)
    df = df.drop_duplicates(subset=["order_id"])
    if len(df) < 50:
        raise ValueError("Please process at least 2 daily batches before training.")
    target = ROOT / "data" / "ml" / "training.csv"
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False)
    print(f"ML data: {len(df)} orders from {len(files)} Gold files")


def train_model():
    df = pd.read_csv(ROOT / "data" / "ml" / "training.csv")
    X_train, X_test, y_train, y_test = train_test_split(
        df[FEATURES], df[TARGET], test_size=0.25,
        random_state=42, stratify=df[TARGET],
    )
    model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=300))
    model.fit(X_train, y_train)

    (ROOT / "models").mkdir(parents=True, exist_ok=True)
    joblib.dump(model, ROOT / "models" / "candidate_model.joblib")

    test = X_test.copy()
    test[TARGET] = y_test
    test.to_csv(ROOT / "data" / "ml" / "test.csv", index=False)
    print(f"Model trained on {len(X_train)} rows; {len(X_test)} reserved for testing")


def evaluate_model():
    model = joblib.load(ROOT / "models" / "candidate_model.joblib")
    test = pd.read_csv(ROOT / "data" / "ml" / "test.csv")
    predictions = model.predict(test[FEATURES])
    actual = test[TARGET]
    metrics = {
        "accuracy": round(float(accuracy_score(actual, predictions)), 3),
        "precision": round(float(precision_score(actual, predictions, zero_division=0)), 3),
        "recall": round(float(recall_score(actual, predictions, zero_division=0)), 3),
        "f1": round(float(f1_score(actual, predictions, zero_division=0)), 3),
    }
    (ROOT / "metrics").mkdir(parents=True, exist_ok=True)
    (ROOT / "metrics" / "metrics.json").write_text(
        json.dumps(metrics, indent=2), encoding="utf-8"
    )
    print(f"Evaluation: {metrics}")


def save_model():
    source = ROOT / "models" / "candidate_model.joblib"
    target = ROOT / "models" / "late_delivery_model.joblib"
    shutil.copyfile(source, target)
    print(f"Model saved: {target}")
