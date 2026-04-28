#!/usr/bin/env python3
"""Persistence helpers for app feedback and manually verified addresses."""
from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List, Optional

from address_dataset_generator import AddressRecord, query_text_key
from address_resolver import ReferenceAddress, Resolver
from resolver_app_config import (
    FEEDBACK_FIELDNAMES,
    MANUAL_FIELDNAMES,
    REFERENCE_FIELDNAMES,
    reference_csv_path,
    runtime_config_value,
)

def manual_verified_csv_path() -> Path:
    return runtime_config_value("DEFAULT_VERIFIED_SOURCE_DIR") / "verified_addresses.csv"


def active_learning_feedback_csv_path() -> Path:
    return runtime_config_value("DEFAULT_ACTIVE_LEARNING_DIR") / "resolver_feedback.csv"


def append_active_learning_feedback(row: Dict[str, object], path: Optional[Path] = None) -> None:
    feedback_path = path or active_learning_feedback_csv_path()
    feedback_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not feedback_path.exists() or feedback_path.stat().st_size == 0
    normalized = {
        name: json.dumps(value, separators=(",", ":"), sort_keys=True) if isinstance(value, (list, dict)) else value
        for name, value in row.items()
    }
    with feedback_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FEEDBACK_FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(normalized)


def feedback_override_keys(input_address: str, standardized_address: str = "") -> List[str]:
    keys = []
    for value in (input_address, standardized_address):
        key = query_text_key(value)
        if key and key not in keys:
            keys.append(key)
    return keys


def load_feedback_overrides(resolver: Resolver, path: Optional[Path] = None) -> Dict[str, str]:
    feedback_path = path or active_learning_feedback_csv_path()
    if not feedback_path.exists():
        return {}
    overrides: Dict[str, str] = {}
    missing_id_corrections: List[Dict[str, str]] = []
    with feedback_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            feedback_type = row.get("feedback_type", "").strip()
            if feedback_type == "correct":
                reference_id = row.get("predicted_match_id", "").strip()
            elif feedback_type == "correction":
                reference_id = row.get("correct_reference_id", "").strip()
                if not reference_id:
                    missing_id_corrections.append(row)
                    continue
            else:
                continue
            if reference_id not in resolver.reference_by_id:
                continue
            for key in feedback_override_keys(row.get("input_address", ""), row.get("standardized_address", "")):
                overrides[key] = reference_id
    if missing_id_corrections:
        needed_canonicals = {
            query_text_key(row.get("correct_canonical_address", ""))
            for row in missing_id_corrections
            if row.get("correct_canonical_address", "")
        }
        canonical_to_id = {
            query_text_key(reference.canonical_address): reference_id
            for reference_id, reference in resolver.reference_by_id.items()
            if query_text_key(reference.canonical_address) in needed_canonicals
        }
        for row in missing_id_corrections:
            reference_id = canonical_to_id.get(query_text_key(row.get("correct_canonical_address", "")), "")
            if reference_id:
                for key in feedback_override_keys(row.get("input_address", ""), row.get("standardized_address", "")):
                    overrides[key] = reference_id
    return overrides


def address_record_csv_row(address_id: str, record: AddressRecord, source_note: str = "") -> Dict[str, str]:
    return {
        "address_id": address_id,
        "house_number": record.house_number,
        "predir": record.predir,
        "street_name": record.street_name.title(),
        "street_type": record.street_type,
        "suffixdir": record.suffixdir,
        "unit_type": record.unit_type,
        "unit_value": record.unit_value,
        "city": record.city.title(),
        "state": record.state,
        "zip_code": record.zip_code,
        "source_note": " ".join(source_note.split())[:240],
    }


def append_manual_verified_record(address_id: str, record: AddressRecord, source_note: str) -> None:
    path = manual_verified_csv_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(address_record_csv_row(address_id, record, source_note))


def reference_csv_row(reference: ReferenceAddress) -> Dict[str, str]:
    return {
        "address_id": reference.address_id,
        "house_number": reference.house_number,
        "predir": reference.predir,
        "street_name": reference.street_name.title(),
        "street_type": reference.street_type,
        "suffixdir": reference.suffixdir,
        "unit_type": reference.unit_type,
        "unit_value": reference.unit_value,
        "city": reference.city.title(),
        "state": reference.state,
        "zip_code": reference.zip_code,
        "canonical_address": reference.canonical_address,
        "source_quality": f"{reference.source_quality:.3f}",
    }


def reference_fieldnames_for_path(path: Path) -> List[str]:
    if path.exists() and path.stat().st_size > 0:
        with path.open(newline="", encoding="utf-8") as handle:
            header = next(csv.reader(handle), None)
        if header:
            return header
    return REFERENCE_FIELDNAMES


def append_reference_record(dataset_dir: Path, reference: ReferenceAddress) -> None:
    path = reference_csv_path(dataset_dir)
    fieldnames = reference_fieldnames_for_path(path)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writerow(reference_csv_row(reference))


def update_reference_metadata(dataset_dir: Path, increment: int = 1) -> None:
    if increment <= 0:
        return
    path = dataset_dir / "metadata.json"
    if not path.exists():
        return
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return
    for key in ("rows_seen", "rows_loaded", "reference_records"):
        metadata[key] = int(metadata.get(key, 0)) + increment
    sources = metadata.setdefault("sources", [])
    manual_source = None
    for source in sources:
        if source.get("source_format") == "address_record":
            manual_source = source
            break
    if manual_source is None:
        manual_source = {
            "source_format": "address_record",
            "state": "MS",
            "rows_seen": 0,
            "rows_loaded": 0,
            "rows_skipped": 0,
            "input_paths": [str(manual_verified_csv_path())],
        }
        sources.append(manual_source)
    manual_source["rows_seen"] = int(manual_source.get("rows_seen", 0)) + increment
    manual_source["rows_loaded"] = int(manual_source.get("rows_loaded", 0)) + increment
    if "source_format" in metadata and "address_record" not in str(metadata["source_format"]).split("+"):
        metadata["source_format"] = f"{metadata['source_format']}+address_record"
    input_paths = metadata.setdefault("input_paths", [])
    manual_path = str(manual_verified_csv_path())
    if manual_path not in input_paths:
        input_paths.append(manual_path)
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
