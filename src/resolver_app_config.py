#!/usr/bin/env python3
"""Configuration constants for the local resolver app."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any


def find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "datasets").exists() and (parent / "models").exists():
            return parent
    return current.parents[1]


PROJECT_ROOT = find_project_root()
DEFAULT_DATASET_DIR = PROJECT_ROOT / "datasets" / "ms_full_reference"
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "maris_parcels"
DEFAULT_POINT_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "maris_point_addresses"
DEFAULT_OPENADDRESSES_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "openaddresses_ms"
DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "openaddresses_ms_direct"
DEFAULT_VERIFIED_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "manual_verified_ms"
DEFAULT_ACTIVE_LEARNING_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "active_learning"
DEMO_DATASET_DIR = PROJECT_ROOT / "examples" / "demo_reference"
DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "stage2_model.json"
DEFAULT_TRAIN_DATASET_DIR = PROJECT_ROOT / "datasets" / "fresh_60k_active_v2" / "train_dataset"
DEFAULT_EVAL_DATASET_DIR = PROJECT_ROOT / "datasets" / "fresh_60k_active_v2" / "eval_dataset"
DEFAULT_TRAINING_OUTPUT_DIR = PROJECT_ROOT / "runs" / "app_training"
ZIP_CITY_ENRICHMENT_MIN_RECORDS = 25
ZIP_CITY_ENRICHMENT_MIN_SHARE = 0.98
REFERENCE_FIELDNAMES = [
    "address_id",
    "house_number",
    "predir",
    "street_name",
    "street_type",
    "suffixdir",
    "unit_type",
    "unit_value",
    "city",
    "state",
    "zip_code",
    "canonical_address",
    "source_quality",
]
MANUAL_FIELDNAMES = [
    "address_id",
    "house_number",
    "predir",
    "street_name",
    "street_type",
    "suffixdir",
    "unit_type",
    "unit_value",
    "city",
    "state",
    "zip_code",
    "source_note",
]
FEEDBACK_FIELDNAMES = [
    "created_at",
    "feedback_type",
    "input_address",
    "standardized_address",
    "predicted_match_id",
    "predicted_canonical_address",
    "confidence",
    "stage",
    "correct_address",
    "correct_reference_id",
    "correct_canonical_address",
    "top_candidates",
]


def runtime_config_value(name: str) -> Any:
    """Read facade-overridden config values for legacy tests and scripts."""
    app_module = sys.modules.get("resolver_app")
    if app_module is not None and hasattr(app_module, name):
        return getattr(app_module, name)
    return globals()[name]


def reference_csv_path(dataset_dir: Path) -> Path:
    return dataset_dir / "reference_addresses.csv"
