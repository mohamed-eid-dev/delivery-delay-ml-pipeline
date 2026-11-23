"""Compatibility entry point for the ML training pipeline."""

from src.train import (
    evaluate_model,
    prepare_features,
    save_model,
    train_model,
)

__all__ = ["prepare_features", "train_model", "evaluate_model", "save_model"]
