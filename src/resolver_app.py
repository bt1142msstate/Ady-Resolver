#!/usr/bin/env python3
"""Local web UI for resolving a typed address against the trained reference set."""
from __future__ import annotations

import argparse
import json
import sys
import csv
import re
import subprocess
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from address_dataset_generator import (
    AddressRecord,
    MISSISSIPPI_COUNTIES,
    canonical_address,
    discover_input_files,
    load_real_addresses,
    mississippi_counties_in_paths,
    query_text_key,
    zip_code_matches_state,
)
from address_resolver import (
    ReferenceAddress,
    Resolver,
    Stage2Model,
    build_city_lookup,
    choose_combined_resolution,
    load_model,
    load_reference,
    normalize_text,
    standardize_parts,
)


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


from resolver_app_ui import HTML



def manual_verified_csv_path() -> Path:
    return DEFAULT_VERIFIED_SOURCE_DIR / "verified_addresses.csv"


def active_learning_feedback_csv_path() -> Path:
    return DEFAULT_ACTIVE_LEARNING_DIR / "resolver_feedback.csv"


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


from resolver_batch_io import (
    ADDRESS_COLUMN_NAMES,
    ID_COLUMN_NAMES,
    MAX_BATCH_ADDRESSES,
    batch_report_filename,
    column_letters_from_cell_ref,
    column_letters_to_index,
    column_matches_name,
    detect_address_column,
    detect_data_column,
    detect_id_column,
    excel_column_name,
    extract_batch_addresses,
    inspect_batch_columns,
    looks_like_header_row,
    normalize_header,
    read_batch_upload,
    read_csv_upload,
    read_shared_strings,
    read_upload_rows,
    read_xlsx_upload,
    spreadsheet_column_index,
    worksheet_path_from_workbook,
    write_xlsx_report,
    xml_local_name,
    xlsx_cell,
)



class ResolverService:
    def __init__(
        self,
        dataset_dir: Path,
        model_path: Path,
        train_dataset_dir: Path = DEFAULT_TRAIN_DATASET_DIR,
        eval_dataset_dir: Path = DEFAULT_EVAL_DATASET_DIR,
        training_output_dir: Path = DEFAULT_TRAINING_OUTPUT_DIR,
        training_jobs: int = 4,
    ) -> None:
        self.dataset_dir = dataset_dir
        self.model_path = model_path
        self.train_dataset_dir = train_dataset_dir
        self.eval_dataset_dir = eval_dataset_dir
        self.training_output_dir = training_output_dir
        self.training_jobs = max(1, training_jobs)
        self.model_lock = threading.RLock()
        self.training_lock = threading.RLock()
        self.training_job: Dict[str, object] = {
            "state": "idle",
            "message": "",
            "queued": False,
            "queued_at": "",
            "queue_reason": "",
            "log_tail": [],
            "progress_pct": 0,
            "phase": "Idle",
        }
        reference_rows, _ = load_reference(dataset_dir / "reference_addresses.csv")
        city_lookup = build_city_lookup(reference_rows)
        self.resolver = Resolver(reference_rows, city_lookup)
        self.model, self.accept_threshold, self.review_threshold, self.model_metadata = load_model(model_path, self.resolver)
        self.feedback_overrides = load_feedback_overrides(self.resolver)
        self.reference_count = len(reference_rows)
        self.examples = [row.canonical_address for row in reference_rows[:5]]
        self.next_reference_index = self.next_reference_number(reference_rows)

    @property
    def dataset_name(self) -> str:
        try:
            return str(self.dataset_dir.relative_to(PROJECT_ROOT))
        except ValueError:
            return str(self.dataset_dir)

    def resolve(self, raw_address: str) -> Dict[str, object]:
        parsed = self.resolver.parse(raw_address)
        override = self.feedback_override(raw_address, parsed.standardized_address)
        if override:
            return override
        with self.model_lock:
            model = self.model
            accept_threshold = self.accept_threshold
            review_threshold = self.review_threshold
        stage1 = self.resolver.resolve_stage1(parsed, review_threshold=review_threshold)
        stage2 = model.resolve(parsed, accept_threshold=accept_threshold, review_threshold=review_threshold)
        combined = choose_combined_resolution(stage1, stage2)
        top_candidates = self.top_candidate_payload(combined)
        return {
            "input_address": raw_address,
            "standardized_address": combined.standardized_query_address,
            "predicted_match_id": combined.predicted_match_id,
            "predicted_canonical_address": combined.predicted_canonical_address,
            "confidence": combined.confidence,
            "needs_review": combined.needs_review,
            "stage": combined.stage,
            "top_candidates": top_candidates,
            "stage1": self.resolution_summary(stage1),
            "stage2": self.resolution_summary(stage2),
        }

    def resolve_batch(
        self,
        filename: str,
        content: bytes,
        address_column: str = "",
        id_column: str = "",
        has_header: Optional[bool] = None,
    ) -> Tuple[str, bytes, int]:
        addresses = read_batch_upload(filename, content, address_column, id_column, has_header)
        headers = [
            "source_row",
            "source_id",
            "original_address",
            "standardized_address",
            "resolved_address",
            "confidence",
            "needs_review",
            "match_id",
            "stage",
            "top_candidate_1",
            "top_candidate_1_score",
            "top_candidate_2",
            "top_candidate_2_score",
            "top_candidate_3",
            "top_candidate_3_score",
        ]
        report_rows: List[List[object]] = []
        for source_row, source_id, raw_address in addresses:
            resolution = self.resolve(raw_address)
            candidates = list(resolution.get("top_candidates") or [])
            row: List[object] = [
                source_row,
                source_id,
                raw_address,
                resolution.get("standardized_address", ""),
                resolution.get("predicted_canonical_address", ""),
                f"{float(resolution.get('confidence') or 0.0):.4f}",
                "yes" if resolution.get("needs_review") else "no",
                resolution.get("predicted_match_id", ""),
                resolution.get("stage", ""),
            ]
            for index in range(3):
                if index < len(candidates):
                    candidate = candidates[index]
                    row.extend(
                        [
                            candidate.get("canonical_address", ""),
                            f"{float(candidate.get('score') or 0.0):.4f}",
                        ]
                    )
                else:
                    row.extend(["", ""])
            report_rows.append(row)
        return batch_report_filename(filename), write_xlsx_report(headers, report_rows), len(report_rows)

    def feedback_override(self, raw_address: str, standardized_address: str) -> Optional[Dict[str, object]]:
        reference_id = ""
        for key in feedback_override_keys(raw_address, standardized_address):
            reference_id = self.feedback_overrides.get(key, "")
            if reference_id:
                break
        reference = self.resolver.reference_by_id.get(reference_id)
        if reference is None:
            return None
        top_candidates = [
            {
                "reference_id": reference.address_id,
                "score": 1.0,
                "canonical_address": reference.canonical_address,
            }
        ]
        summary = {
            "predicted_match_id": reference.address_id,
            "predicted_canonical_address": reference.canonical_address,
            "confidence": 1.0,
            "needs_review": False,
            "stage": "feedback_override",
            "standardized_address": standardized_address,
        }
        return {
            "input_address": raw_address,
            "standardized_address": standardized_address,
            "predicted_match_id": reference.address_id,
            "predicted_canonical_address": reference.canonical_address,
            "confidence": 1.0,
            "needs_review": False,
            "stage": "feedback_override",
            "top_candidates": top_candidates,
            "stage1": summary,
            "stage2": summary,
        }

    def top_candidate_payload(self, resolution) -> List[Dict[str, object]]:
        candidates = []
        for candidate in resolution.top_candidates[:5]:
            reference = self.resolver.reference_by_id.get(candidate.reference_id)
            candidates.append(
                {
                    "reference_id": candidate.reference_id,
                    "score": candidate.score,
                    "canonical_address": reference.canonical_address if reference else "",
                }
            )
        return candidates

    def resolution_summary(self, resolution) -> Dict[str, object]:
        return {
            "predicted_match_id": resolution.predicted_match_id,
            "predicted_canonical_address": resolution.predicted_canonical_address,
            "confidence": resolution.confidence,
            "needs_review": resolution.needs_review,
            "stage": resolution.stage,
            "standardized_address": resolution.standardized_query_address,
        }

    def health(self) -> Dict[str, object]:
        training_status = self.training_status()
        return {
            "dataset_name": self.dataset_name,
            "dataset_dir": str(self.dataset_dir),
            "model_path": str(self.model_path),
            "reference_count": self.reference_count,
            "accept_threshold": self.accept_threshold,
            "review_threshold": self.review_threshold,
            "feedback_override_count": len(self.feedback_overrides),
            "training_state": training_status["state"],
            "examples": self.examples,
        }

    def next_reference_number(self, reference_rows: List[ReferenceAddress]) -> int:
        highest = 0
        for row in reference_rows:
            if not row.address_id.startswith("REF_"):
                continue
            try:
                highest = max(highest, int(row.address_id.removeprefix("REF_")))
            except ValueError:
                continue
        return highest + 1

    def infer_zip(self, parsed) -> str:
        if parsed.zip_code:
            return parsed.zip_code
        if not parsed.city or not parsed.state:
            return ""
        candidate_ids = self.resolver.by_house_city_street.get(
            (parsed.house_number, parsed.city, parsed.state, parsed.street_signature),
            [],
        )
        if not candidate_ids:
            candidate_ids = self.resolver.by_house_city_street_name.get(
                (parsed.house_number, parsed.city, parsed.state, parsed.street_name),
                [],
            )
        if not candidate_ids:
            candidate_ids = self.resolver.by_city_state.get((parsed.city, parsed.state), [])
            candidate_ids = [
                candidate_id
                for candidate_id in candidate_ids
                if self.resolver.reference_by_id[candidate_id].street_name == parsed.street_name
                and self.resolver.reference_by_id[candidate_id].street_type == parsed.street_type
            ]
        zip_codes = {
            self.resolver.reference_by_id[candidate_id].zip_code
            for candidate_id in candidate_ids
            if self.resolver.reference_by_id[candidate_id].zip_code
        }
        return next(iter(zip_codes)) if len(zip_codes) == 1 else ""

    def record_from_manual_input(self, raw_address: str) -> AddressRecord:
        parsed = self.resolver.parse(raw_address)
        state = parsed.state or "MS"
        zip_code = self.infer_zip(parsed)
        if state != "MS":
            raise ValueError("Only Mississippi addresses can be added to this resolver.")
        if not parsed.house_number or not parsed.street_name:
            raise ValueError("Address must include a house number and street.")
        if not parsed.city:
            raise ValueError("Address must include a city.")
        if not zip_code:
            raise ValueError("Address must include a ZIP, or the ZIP must be inferable from existing nearby references.")
        if not zip_code_matches_state(zip_code, state):
            raise ValueError("ZIP code does not look like a Mississippi ZIP.")
        return AddressRecord(
            address_id="",
            house_number=parsed.house_number,
            predir=parsed.predir,
            street_name=parsed.street_name,
            street_type=parsed.street_type,
            suffixdir=parsed.suffixdir,
            unit_type=parsed.unit_type,
            unit_value=parsed.unit_value,
            city=parsed.city,
            state=state,
            zip_code=zip_code,
        )

    def reference_from_record(self, record: AddressRecord, address_id: str) -> ReferenceAddress:
        standardized = standardize_parts(
            record.house_number.upper(),
            record.predir.upper(),
            normalize_text(record.street_name),
            record.street_type.upper(),
            record.suffixdir.upper(),
            record.unit_type.upper(),
            record.unit_value.upper(),
            normalize_text(record.city),
            record.state.upper(),
            record.zip_code,
        )
        street_signature = " ".join(
            bit
            for bit in [
                record.predir.upper(),
                normalize_text(record.street_name),
                record.street_type.upper(),
                record.suffixdir.upper(),
            ]
            if bit
        )
        return ReferenceAddress(
            address_id=address_id,
            canonical_address=canonical_address(record),
            house_number=record.house_number.upper(),
            predir=record.predir.upper(),
            street_name=normalize_text(record.street_name),
            street_type=record.street_type.upper(),
            suffixdir=record.suffixdir.upper(),
            unit_type=record.unit_type.upper(),
            unit_value=record.unit_value.upper(),
            city=normalize_text(record.city),
            state=record.state.upper(),
            zip_code=record.zip_code,
            standardized_address=standardized,
            street_signature=street_signature,
            source_quality=1.0,
        )

    def next_manual_id(self) -> str:
        path = manual_verified_csv_path()
        highest = 0
        if path.exists():
            with path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    value = row.get("address_id", "")
                    if not value.startswith("MANUAL_MS_"):
                        continue
                    try:
                        highest = max(highest, int(value.removeprefix("MANUAL_MS_")))
                    except ValueError:
                        continue
        return f"MANUAL_MS_{highest + 1:06d}"

    def add_verified_address(self, raw_address: str, source_note: str) -> Dict[str, object]:
        record = self.record_from_manual_input(raw_address)
        reference_id = f"REF_{self.next_reference_index:07d}"
        reference = self.reference_from_record(record, reference_id)
        existing_ids = self.resolver.by_exact.get(reference.standardized_address, [])
        if existing_ids:
            existing = self.resolver.reference_by_id[existing_ids[0]]
            return {
                "already_exists": True,
                "reference_id": existing.address_id,
                "canonical_address": existing.canonical_address,
                "reference_count": self.reference_count,
            }

        manual_id = self.next_manual_id()
        append_manual_verified_record(manual_id, record, source_note)
        append_reference_record(self.dataset_dir, reference)
        update_reference_metadata(self.dataset_dir)
        self.resolver.add_reference(reference)
        self.reference_count += 1
        self.next_reference_index += 1
        return {
            "already_exists": False,
            "reference_id": reference.address_id,
            "canonical_address": reference.canonical_address,
            "reference_count": self.reference_count,
        }

    def import_verified_addresses(
        self,
        filename: str,
        content: bytes,
        address_column: str = "",
        source_note: str = "",
        has_header: Optional[bool] = None,
    ) -> Dict[str, object]:
        addresses = read_batch_upload(filename, content, address_column, "", has_header)
        added: List[Dict[str, object]] = []
        existing: List[Dict[str, object]] = []
        failures: List[Dict[str, object]] = []
        manual_number = int(self.next_manual_id().removeprefix("MANUAL_MS_"))
        file_note = Path(filename).name or "uploaded file"

        for source_row, _source_id, raw_address in addresses:
            row_note = " ".join(part for part in [source_note, f"import:{file_note}", f"row:{source_row}"] if part)
            try:
                record = self.record_from_manual_input(raw_address)
                reference_id = f"REF_{self.next_reference_index:07d}"
                reference = self.reference_from_record(record, reference_id)
                existing_ids = self.resolver.by_exact.get(reference.standardized_address, [])
                if existing_ids:
                    existing_reference = self.resolver.reference_by_id[existing_ids[0]]
                    existing.append(
                        {
                            "source_row": source_row,
                            "input_address": raw_address,
                            "reference_id": existing_reference.address_id,
                            "canonical_address": existing_reference.canonical_address,
                        }
                    )
                    continue

                manual_id = f"MANUAL_MS_{manual_number:06d}"
                manual_number += 1
                append_manual_verified_record(manual_id, record, row_note)
                append_reference_record(self.dataset_dir, reference)
                self.resolver.add_reference(reference)
                self.reference_count += 1
                self.next_reference_index += 1
                added.append(
                    {
                        "source_row": source_row,
                        "input_address": raw_address,
                        "reference_id": reference.address_id,
                        "canonical_address": reference.canonical_address,
                    }
                )
            except ValueError as exc:
                failures.append(
                    {
                        "source_row": source_row,
                        "input_address": raw_address,
                        "error": str(exc),
                    }
                )

        if added:
            update_reference_metadata(self.dataset_dir, len(added))
        return {
            "imported": True,
            "row_count": len(addresses),
            "added_count": len(added),
            "existing_count": len(existing),
            "failed_count": len(failures),
            "added": added[:10],
            "existing": existing[:10],
            "failures": failures[:10],
            "reference_count": self.reference_count,
        }

    def record_feedback(self, raw_address: str, feedback_type: str, correct_address: str = "") -> Dict[str, object]:
        if feedback_type not in {"correct", "wrong", "correction"}:
            raise ValueError("Feedback type must be correct, wrong, or correction.")
        if not raw_address:
            raise ValueError("Address is required.")
        if feedback_type == "correction" and not correct_address:
            raise ValueError("Correction address is required.")

        resolution = self.resolve(raw_address)
        correct_reference_id = ""
        correct_canonical_address = ""
        if feedback_type == "correction":
            correction = self.add_verified_address(correct_address, f"active learning correction for: {raw_address}")
            correct_reference_id = str(correction["reference_id"])
            correct_canonical_address = str(correction["canonical_address"])

        row = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "feedback_type": feedback_type,
            "input_address": raw_address,
            "standardized_address": resolution["standardized_address"],
            "predicted_match_id": resolution["predicted_match_id"],
            "predicted_canonical_address": resolution["predicted_canonical_address"],
            "confidence": resolution["confidence"],
            "stage": resolution["stage"],
            "correct_address": correct_address,
            "correct_reference_id": correct_reference_id,
            "correct_canonical_address": correct_canonical_address,
            "top_candidates": resolution["top_candidates"],
        }
        append_active_learning_feedback(row)
        override_reference_id = ""
        if feedback_type == "correct":
            override_reference_id = str(resolution["predicted_match_id"] or "")
        elif feedback_type == "correction":
            override_reference_id = correct_reference_id
        if override_reference_id in self.resolver.reference_by_id:
            for key in feedback_override_keys(raw_address, str(resolution["standardized_address"])):
                self.feedback_overrides[key] = override_reference_id
        training_status = self.queue_training(f"feedback:{feedback_type}")
        return {
            "saved": True,
            "feedback_path": str(active_learning_feedback_csv_path()),
            "correct_reference_id": correct_reference_id,
            "correct_canonical_address": correct_canonical_address,
            "override_applied": bool(override_reference_id),
            "training": training_status,
            "reference_count": self.reference_count,
        }

    def training_dataset_ready(self) -> bool:
        return (
            (self.train_dataset_dir / "reference_addresses.csv").exists()
            and (self.train_dataset_dir / "queries.csv").exists()
            and (self.eval_dataset_dir / "reference_addresses.csv").exists()
            and (self.eval_dataset_dir / "queries.csv").exists()
        )

    def feedback_row_count(self) -> int:
        path = active_learning_feedback_csv_path()
        if not path.exists():
            return 0
        with path.open(newline="", encoding="utf-8") as handle:
            return sum(1 for _row in csv.DictReader(handle))

    def training_status(self) -> Dict[str, object]:
        with self.training_lock:
            status = dict(self.training_job)
            status["log_tail"] = list(status.get("log_tail", []))
            if status.get("state") == "running":
                started_at = str(status.get("started_at") or "")
                elapsed_seconds = 0.0
                if started_at:
                    try:
                        elapsed_seconds = max(
                            0.0,
                            (
                                datetime.now(timezone.utc)
                                - datetime.fromisoformat(started_at)
                            ).total_seconds(),
                        )
                    except ValueError:
                        elapsed_seconds = 0.0
                estimated = min(92, 6 + int(elapsed_seconds / 3))
                status["progress_pct"] = max(int(status.get("progress_pct") or 0), estimated)
        status["train_dataset_dir"] = str(self.train_dataset_dir)
        status["eval_dataset_dir"] = str(self.eval_dataset_dir)
        status["feedback_path"] = str(active_learning_feedback_csv_path())
        status["feedback_rows"] = self.feedback_row_count()
        status["training_dataset_ready"] = self.training_dataset_ready()
        return status

    def start_training(self, trigger: str = "manual", reason: str = "manual") -> Dict[str, object]:
        if not self.training_dataset_ready():
            raise ValueError(
                "Training datasets are missing. Generate datasets/fresh_60k_active_v2 first, "
                "or start the app with --train-dataset-dir and --eval-dataset-dir."
            )
        feedback_rows = self.feedback_row_count()
        if feedback_rows <= 0:
            raise ValueError("No feedback rows found yet. Mark results Correct/Wrong or Save Correction before updating training.")
        with self.training_lock:
            if self.training_job.get("state") == "running":
                return self.training_status()
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            run_dir = self.training_output_dir / timestamp
            temp_model_path = self.model_path.with_name(f"{self.model_path.stem}.training-{timestamp}{self.model_path.suffix}")
            log_path = run_dir / "training.log"
            command = [
                sys.executable,
                str(PROJECT_ROOT / "src" / "address_resolver.py"),
                "--mode",
                "fit-predict",
                "--train-dataset-dir",
                str(self.train_dataset_dir),
                "--eval-dataset-dir",
                str(self.eval_dataset_dir),
                "--active-learning-feedback-csv",
                str(active_learning_feedback_csv_path()),
                "--model-path",
                str(temp_model_path),
                "--output-dir",
                str(run_dir),
                "--compare-variants",
                "--jobs",
                str(self.training_jobs),
            ]
            self.training_job = {
                "state": "running",
                "message": "Training started",
                "trigger": trigger,
                "reason": reason,
                "queued": False,
                "queued_at": "",
                "queue_reason": "",
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "finished_at": "",
                "command": command,
                "run_dir": str(run_dir),
                "log_path": str(log_path),
                "temp_model_path": str(temp_model_path),
                "returncode": None,
                "log_tail": [],
                "evaluation": {},
                "progress_pct": 4,
                "phase": "Starting",
            }
            thread = threading.Thread(
                target=self.run_training_job,
                args=(command, run_dir, temp_model_path, log_path),
                daemon=True,
            )
            thread.start()
            return self.training_status()

    def queue_training(self, reason: str) -> Dict[str, object]:
        try:
            if not self.training_dataset_ready():
                status = self.training_status()
                status["auto_training_error"] = (
                    "Training datasets are missing. Generate datasets/fresh_60k_active_v2 first, "
                    "or start the app with --train-dataset-dir and --eval-dataset-dir."
                )
                return status
            if self.feedback_row_count() <= 0:
                status = self.training_status()
                status["auto_training_error"] = "No feedback rows found yet."
                return status
            with self.training_lock:
                if self.training_job.get("state") == "running":
                    self.training_job["queued"] = True
                    self.training_job["queued_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    self.training_job["queue_reason"] = reason
                    self.training_job["message"] = "Training running; another run is queued."
                    return self.training_status()
            return self.start_training(trigger="feedback", reason=reason)
        except ValueError as exc:
            status = self.training_status()
            status["auto_training_error"] = str(exc)
            return status

    def append_training_log_line(self, line: str) -> None:
        with self.training_lock:
            log_tail = list(self.training_job.get("log_tail", []))
            log_tail.append(line.rstrip())
            self.training_job["log_tail"] = log_tail[-40:]
            lowered = line.lower()
            progress = int(self.training_job.get("progress_pct") or 0)
            if "loaded" in lowered and "feedback" in lowered:
                progress = max(progress, 18)
                self.training_job["phase"] = "Loading feedback"
            elif "saved stage 2 model" in lowered:
                progress = max(progress, 70)
                self.training_job["phase"] = "Evaluating"
            elif "address resolver finished" in lowered:
                progress = max(progress, 94)
                self.training_job["phase"] = "Finalizing"
            self.training_job["progress_pct"] = progress

    def reload_model(self) -> None:
        model, accept_threshold, review_threshold, metadata = load_model(self.model_path, self.resolver)
        with self.model_lock:
            self.model = model
            self.accept_threshold = accept_threshold
            self.review_threshold = review_threshold
            self.model_metadata = metadata

    def run_training_job(self, command: List[str], run_dir: Path, temp_model_path: Path, log_path: Path) -> None:
        run_dir.mkdir(parents=True, exist_ok=True)
        returncode = 1
        try:
            with log_path.open("w", encoding="utf-8") as log_handle:
                process = subprocess.Popen(
                    command,
                    cwd=PROJECT_ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                with self.training_lock:
                    self.training_job["pid"] = process.pid
                assert process.stdout is not None
                for line in process.stdout:
                    log_handle.write(line)
                    log_handle.flush()
                    self.append_training_log_line(line)
                returncode = process.wait()
            evaluation = {}
            evaluation_path = run_dir / "evaluation.json"
            if evaluation_path.exists():
                try:
                    evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    evaluation = {}
            if returncode == 0 and temp_model_path.exists():
                temp_model_path.replace(self.model_path)
                self.reload_model()
                state = "succeeded"
                message = "Training complete; model reloaded."
            else:
                state = "failed"
                message = f"Training failed with return code {returncode}."
        except Exception as exc:  # pragma: no cover - surfaced through app status
            state = "failed"
            message = str(exc)
            evaluation = {}
        finally:
            if temp_model_path.exists() and state != "succeeded":
                temp_model_path.unlink()
            queued_reason = ""
            with self.training_lock:
                if self.training_job.get("queued"):
                    queued_reason = str(self.training_job.get("queue_reason") or "queued_feedback")
                self.training_job["state"] = state
                self.training_job["message"] = message
                self.training_job["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self.training_job["returncode"] = returncode
                self.training_job["evaluation"] = evaluation
                self.training_job["queued"] = False
                self.training_job["queued_at"] = ""
                self.training_job["queue_reason"] = ""
                self.training_job["progress_pct"] = 100 if state == "succeeded" else int(self.training_job.get("progress_pct") or 0)
                self.training_job["phase"] = "Complete" if state == "succeeded" else "Failed"
            if queued_reason:
                self.queue_training(queued_reason)


def reference_csv_path(dataset_dir: Path) -> Path:
    return dataset_dir / "reference_addresses.csv"


def reference_cache_ready(dataset_dir: Path) -> bool:
    path = reference_csv_path(dataset_dir)
    return path.exists() and path.stat().st_size > 0


def default_source_specs() -> List[Tuple[Path, str]]:
    specs = []
    if DEFAULT_SOURCE_DIR.exists():
        specs.append((DEFAULT_SOURCE_DIR, "maris_parcels"))
    if DEFAULT_POINT_SOURCE_DIR.exists():
        specs.append((DEFAULT_POINT_SOURCE_DIR, "maris"))
    if DEFAULT_OPENADDRESSES_SOURCE_DIR.exists():
        specs.append((DEFAULT_OPENADDRESSES_SOURCE_DIR, "auto"))
    if DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR.exists():
        specs.append((DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR, "openaddresses"))
    if DEFAULT_VERIFIED_SOURCE_DIR.exists():
        specs.append((DEFAULT_VERIFIED_SOURCE_DIR, "address_record"))
    return specs


def zip_city_consensus(
    records: List[AddressRecord],
    min_records: int = ZIP_CITY_ENRICHMENT_MIN_RECORDS,
    min_share: float = ZIP_CITY_ENRICHMENT_MIN_SHARE,
) -> Dict[str, str]:
    zip_city_counts: Dict[str, Dict[str, int]] = {}
    city_display: Dict[str, str] = {}
    for record in records:
        if not record.zip_code or not record.city:
            continue
        city_key = normalize_text(record.city)
        if not city_key:
            continue
        zip_city_counts.setdefault(record.zip_code, {})
        zip_city_counts[record.zip_code][city_key] = zip_city_counts[record.zip_code].get(city_key, 0) + 1
        city_display.setdefault(city_key, record.city)

    consensus: Dict[str, str] = {}
    for zip_code, counts in zip_city_counts.items():
        total = sum(counts.values())
        if total < min_records:
            continue
        city_key, count = max(counts.items(), key=lambda item: item[1])
        if count / total >= min_share:
            consensus[zip_code] = city_display[city_key]
    return consensus


def add_zip_city_enrichment(
    records: List[AddressRecord],
    seen_keys: set[str],
    min_records: int = ZIP_CITY_ENRICHMENT_MIN_RECORDS,
    min_share: float = ZIP_CITY_ENRICHMENT_MIN_SHARE,
) -> Dict[str, object]:
    consensus = zip_city_consensus(records, min_records=min_records, min_share=min_share)
    added_records: List[AddressRecord] = []
    duplicate_count = 0
    eligible_blank_city_records = 0

    for record in records:
        if record.city or not record.zip_code:
            continue
        inferred_city = consensus.get(record.zip_code)
        if not inferred_city:
            continue
        eligible_blank_city_records += 1
        enriched = AddressRecord(
            address_id=record.address_id,
            house_number=record.house_number,
            predir=record.predir,
            street_name=record.street_name,
            street_type=record.street_type,
            suffixdir=record.suffixdir,
            unit_type=record.unit_type,
            unit_value=record.unit_value,
            city=inferred_city,
            state=record.state,
            zip_code=record.zip_code,
            source_quality=record.source_quality,
        )
        key = query_text_key(canonical_address(enriched))
        if key in seen_keys:
            duplicate_count += 1
            continue
        seen_keys.add(key)
        added_records.append(enriched)

    records.extend(added_records)
    return {
        "enabled": True,
        "min_records": min_records,
        "min_share": min_share,
        "consensus_zip_count": len(consensus),
        "eligible_blank_city_records": eligible_blank_city_records,
        "records_added": len(added_records),
        "duplicates_skipped": duplicate_count,
    }


def build_reference_cache(dataset_dir: Path, source_specs: List[Tuple[Path, str]]) -> None:
    if not source_specs:
        raise SystemExit("At least one real address source is required to build the app reference cache.")

    source_files: List[Path] = []
    for source_dir, _source_format in source_specs:
        if not source_dir.exists():
            raise SystemExit(f"Real address source cache not found: {source_dir}")
        source_files.extend(discover_input_files([source_dir]))

    covered_counties = mississippi_counties_in_paths(source_files)
    missing_counties = sorted(set(MISSISSIPPI_COUNTIES) - set(covered_counties))
    if missing_counties:
        raise SystemExit(
            "Cannot build full app reference cache; source is missing counties: "
            + ", ".join(missing_counties)
        )

    print("Building full Mississippi app reference cache from:")
    for source_dir, source_format in source_specs:
        print(f"  - {source_dir} ({source_format})")

    records = []
    seen_keys = set()
    source_results = []
    for source_dir, source_format in source_specs:
        load_result = load_real_addresses([source_dir], source_format=source_format, state_filter="MS")
        source_results.append(load_result)
        for record in load_result.records:
            key = query_text_key(canonical_address(record))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            records.append(record)

    if not records:
        raise SystemExit("No usable real addresses were loaded for the app reference cache.")

    zip_city_enrichment = add_zip_city_enrichment(records, seen_keys)

    dataset_dir.mkdir(parents=True, exist_ok=True)
    temporary_reference = reference_csv_path(dataset_dir).with_suffix(".csv.part")
    with temporary_reference.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REFERENCE_FIELDNAMES)
        writer.writeheader()
        for index, record in enumerate(records, 1):
            record.address_id = f"REF_{index:07d}"
            writer.writerow(
                {
                    "address_id": record.address_id,
                    "house_number": record.house_number,
                    "predir": record.predir,
                    "street_name": record.street_name,
                    "street_type": record.street_type,
                    "suffixdir": record.suffixdir,
                    "unit_type": record.unit_type,
                    "unit_value": record.unit_value,
                    "city": record.city,
                    "state": record.state,
                    "zip_code": record.zip_code,
                    "canonical_address": canonical_address(record),
                    "source_quality": f"{record.source_quality:.3f}",
                }
            )
    temporary_reference.replace(reference_csv_path(dataset_dir))

    source_unique_count = len(records) - int(zip_city_enrichment["records_added"])
    metadata = {
        "address_source": "real",
        "source_format": "+".join(result.source_format for result in source_results),
        "state": "MS",
        "rows_seen": sum(result.rows_seen for result in source_results),
        "rows_loaded": sum(result.rows_loaded for result in source_results),
        "rows_skipped": sum(result.rows_skipped for result in source_results),
        "reference_records": len(records),
        "deduplicated_records": sum(result.rows_loaded for result in source_results) - source_unique_count,
        "source_records_after_deduplication": source_unique_count,
        "derived_records_added": int(zip_city_enrichment["records_added"]),
        "zip_city_enrichment": zip_city_enrichment,
        "sources": [
            {
                "source_format": result.source_format,
                "state": result.state,
                "rows_seen": result.rows_seen,
                "rows_loaded": result.rows_loaded,
                "rows_skipped": result.rows_skipped,
                "input_paths": result.input_paths,
            }
            for result in source_results
        ],
        "input_paths": [path for result in source_results for path in result.input_paths],
        "mississippi_county_coverage": {
            "covered_counties": covered_counties,
            "covered_county_count": len(covered_counties),
            "expected_county_count": len(MISSISSIPPI_COUNTIES),
            "missing_counties": missing_counties,
        },
    }
    (dataset_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"Wrote {len(records):,} app reference records to {reference_csv_path(dataset_dir)}.")


class ResolverRequestHandler(BaseHTTPRequestHandler):
    service: ResolverService

    def log_message(self, format: str, *args) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        route = urlparse(self.path).path
        if route == "/":
            self.send_html(HTML)
            return
        if route == "/api/health":
            self.send_json(self.service.health())
            return
        if route == "/api/training":
            self.send_json(self.service.training_status())
            return
        self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        route = urlparse(self.path).path
        if route == "/api/add-address":
            self.add_verified_address()
            return
        if route == "/api/add-addresses":
            self.import_verified_addresses()
            return
        if route == "/api/feedback":
            self.record_feedback()
            return
        if route == "/api/training/start":
            self.start_training()
            return
        if route == "/api/batch-columns":
            self.batch_columns()
            return
        if route == "/api/batch-resolve":
            self.batch_resolve()
            return
        if route != "/api/resolve":
            self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            if not raw_address:
                self.send_json({"error": "Address is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(self.service.resolve(raw_address))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def parse_multipart_form(self) -> Dict[str, object]:
        content_type = self.headers.get("Content-Type", "")
        marker = "boundary="
        if marker not in content_type:
            raise ValueError("Expected multipart form data.")
        boundary = content_type.split(marker, 1)[1].split(";", 1)[0].strip().strip('"')
        if not boundary:
            raise ValueError("Multipart boundary is missing.")
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            raise ValueError("Request body is empty.")
        if length > 32 * 1024 * 1024:
            raise ValueError("Upload is too large. Use a file under 32 MB.")

        body = self.rfile.read(length)
        delimiter = b"--" + boundary.encode("utf-8")
        form: Dict[str, object] = {}
        for raw_part in body.split(delimiter):
            part = raw_part
            if part.startswith(b"\r\n"):
                part = part[2:]
            if part.endswith(b"--"):
                part = part[:-2]
            if part.endswith(b"\r\n"):
                part = part[:-2]
            if not part or part == b"--":
                continue
            if b"\r\n\r\n" not in part:
                continue
            raw_headers, content = part.split(b"\r\n\r\n", 1)
            headers = raw_headers.decode("utf-8", errors="replace").split("\r\n")
            disposition = ""
            for header in headers:
                name, _, value = header.partition(":")
                if name.lower() == "content-disposition":
                    disposition = value.strip()
                    break
            if not disposition:
                continue
            fields: Dict[str, str] = {}
            for segment in disposition.split(";"):
                key, separator, value = segment.strip().partition("=")
                if separator:
                    fields[key] = value.strip().strip('"')
            name = fields.get("name", "")
            filename = fields.get("filename")
            if not name:
                continue
            if filename is not None:
                form[name] = {"filename": filename, "content": content}
            else:
                form[name] = content.decode("utf-8", errors="replace").strip()
        return form

    def start_training(self) -> None:
        try:
            self.read_json_body()
            self.send_json(self.service.start_training())
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def batch_columns(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(inspect_batch_columns(filename, content))
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def batch_resolve(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            address_column = str(form.get("address_column") or "")
            id_column = str(form.get("id_column") or "")
            has_header_value = str(form.get("has_header") or "").strip().lower()
            has_header = None
            if has_header_value in {"1", "true", "yes"}:
                has_header = True
            elif has_header_value in {"0", "false", "no"}:
                has_header = False
            output_filename, workbook, row_count = self.service.resolve_batch(
                filename,
                content,
                address_column,
                id_column,
                has_header,
            )
            self.send_bytes(
                workbook,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=output_filename,
                extra_headers={"X-Ady-Resolved-Rows": str(row_count)},
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def import_verified_addresses(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "verified_addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            address_column = str(form.get("address_column") or "")
            source_note = str(form.get("source_note") or "")
            has_header_value = str(form.get("has_header") or "").strip().lower()
            has_header = None
            if has_header_value in {"1", "true", "yes"}:
                has_header = True
            elif has_header_value in {"0", "false", "no"}:
                has_header = False
            self.send_json(
                self.service.import_verified_addresses(
                    filename,
                    content,
                    address_column,
                    source_note,
                    has_header,
                )
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def record_feedback(self) -> None:
        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            feedback_type = str(payload.get("feedback_type", "")).strip()
            correct_address = str(payload.get("correct_address", "")).strip()
            self.send_json(self.service.record_feedback(raw_address, feedback_type, correct_address))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def add_verified_address(self) -> None:
        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            source_note = str(payload.get("source_note", "")).strip()
            if not raw_address:
                self.send_json({"error": "Address is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(self.service.add_verified_address(raw_address, source_note))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def read_json_body(self) -> Dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def send_html(self, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_json(self, payload: Dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_bytes(
        self,
        payload: bytes,
        content_type: str,
        filename: str,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local web UI for resolving typed addresses.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind.")
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR, help="Dataset directory with reference_addresses.csv.")
    parser.add_argument("--real-address-input", type=Path, action="append", help="Real address cache used to build the app reference cache. May be repeated. Defaults to cached MARIS parcels plus cached MARIS point addresses when available.")
    parser.add_argument("--real-address-format", default="auto", choices=["auto", "maris", "maris_parcels", "nad", "openaddresses", "address_record", "generic"], help="Input schema for custom --real-address-input values.")
    parser.add_argument("--rebuild-reference-cache", action="store_true", help="Rebuild the app reference cache from --real-address-input before starting.")
    parser.add_argument("--model-path", type=Path, default=DEFAULT_MODEL_PATH, help="Saved Stage 2 model JSON.")
    parser.add_argument("--train-dataset-dir", type=Path, default=DEFAULT_TRAIN_DATASET_DIR, help="Dataset directory used by automatic app retraining.")
    parser.add_argument("--eval-dataset-dir", type=Path, default=DEFAULT_EVAL_DATASET_DIR, help="Evaluation dataset directory used by automatic app retraining.")
    parser.add_argument("--training-output-dir", type=Path, default=DEFAULT_TRAINING_OUTPUT_DIR, help="Run output directory used by automatic app retraining.")
    parser.add_argument("--training-jobs", type=int, default=4, help="Worker count used by automatic app retraining.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_dir = args.dataset_dir.expanduser().resolve()
    if args.real_address_input:
        source_specs = [(path.expanduser().resolve(), args.real_address_format) for path in args.real_address_input]
    else:
        source_specs = [(path.expanduser().resolve(), source_format) for path, source_format in default_source_specs()]
    model_path = args.model_path.expanduser().resolve()
    train_dataset_dir = args.train_dataset_dir.expanduser().resolve()
    eval_dataset_dir = args.eval_dataset_dir.expanduser().resolve()
    training_output_dir = args.training_output_dir.expanduser().resolve()
    if (
        dataset_dir == DEFAULT_DATASET_DIR.resolve()
        and not reference_cache_ready(dataset_dir)
        and not source_specs
        and reference_cache_ready(DEMO_DATASET_DIR)
    ):
        print(f"Full reference cache not found; using demo dataset at {DEMO_DATASET_DIR}.")
        dataset_dir = DEMO_DATASET_DIR.resolve()
    if args.rebuild_reference_cache or not reference_cache_ready(dataset_dir):
        build_reference_cache(dataset_dir, source_specs)
    if not reference_cache_ready(dataset_dir):
        raise SystemExit(f"Reference CSV not found: {reference_csv_path(dataset_dir)}")
    if not model_path.exists():
        raise SystemExit(f"Model JSON not found: {model_path}")

    print(f"Loading resolver reference cache from {reference_csv_path(dataset_dir)}...")
    service = ResolverService(
        dataset_dir,
        model_path,
        train_dataset_dir=train_dataset_dir,
        eval_dataset_dir=eval_dataset_dir,
        training_output_dir=training_output_dir,
        training_jobs=args.training_jobs,
    )
    ResolverRequestHandler.service = service
    server = ThreadingHTTPServer((args.host, args.port), ResolverRequestHandler)
    print(f"Ady Resolver app running at http://{args.host}:{args.port}")
    print(f"Dataset: {service.dataset_name} ({service.reference_count:,} references)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
