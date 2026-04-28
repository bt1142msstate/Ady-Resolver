#!/usr/bin/env python3
"""Resolver app service layer."""
from __future__ import annotations

import csv
import json
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from address_dataset_generator import AddressRecord, canonical_address, query_text_key, zip_code_matches_state
from address_resolver import (
    ReferenceAddress,
    Resolver,
    build_city_lookup,
    choose_combined_resolution,
    load_model,
    load_reference,
    normalize_text,
    standardize_parts,
)
from resolver_app_config import (
    DEFAULT_EVAL_DATASET_DIR,
    DEFAULT_TRAIN_DATASET_DIR,
    DEFAULT_TRAINING_OUTPUT_DIR,
    PROJECT_ROOT,
)
from resolver_app_storage import (
    active_learning_feedback_csv_path,
    append_active_learning_feedback,
    append_manual_verified_record,
    append_reference_record,
    feedback_override_keys,
    load_feedback_overrides,
    manual_verified_csv_path,
    update_reference_metadata,
)
from resolver_batch_io import batch_report_filename, read_batch_upload, write_xlsx_report

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
