#!/usr/bin/env python3
"""Audit helpers for measuring source extraction completeness."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Union

from address_source_common import *  # noqa: F401,F403
from address_source_manifest import SourceSpec, coerce_source_spec, source_status_summary_for_path
from address_source_parsers import _canonical_address_for_dedupe, load_real_addresses


def _record_summary(records: Sequence[AddressRecord]) -> Dict[str, int]:
    cities = {normalize_spaces(record.city).upper() for record in records if record.city}
    zips = {record.zip_code for record in records if record.zip_code}
    blank_city_records = sum(1 for record in records if not record.city)
    blank_zip_records = sum(1 for record in records if not record.zip_code)
    return {
        "city_count": len(cities),
        "zip_count": len(zips),
        "blank_city_records": blank_city_records,
        "blank_zip_records": blank_zip_records,
    }


def audit_source_specs(
    source_specs: Sequence[Union[SourceSpec, Tuple[Path, str]]],
    state_filter: str = "MS",
    baseline_reference_count: Optional[int] = None,
) -> Dict[str, object]:
    specs: List[SourceSpec] = []
    for source_spec in source_specs:
        coerced = coerce_source_spec(source_spec)
        if coerced.enabled:
            specs.append(coerced)
    seen_keys: set[str] = set()
    source_audits: List[Dict[str, object]] = []
    total_skip_reasons: Counter[str] = Counter()
    all_input_files: List[Path] = []
    total_rows_seen = 0
    total_rows_loaded = 0
    total_rows_skipped = 0
    total_net_new = 0
    total_duplicate_against_prior = 0

    for spec in specs:
        source_audit: Dict[str, object] = {
            "name": spec.name,
            "path": str(spec.path),
            "source_format": spec.source_format,
            "enabled": spec.enabled,
            "notes": spec.notes,
        }
        source_audit.update(source_status_summary_for_path(spec.path))
        if not spec.path.exists():
            source_audit.update(
                {
                    "status": "missing",
                    "rows_seen": 0,
                    "rows_loaded": 0,
                    "rows_skipped": 0,
                    "skip_reasons": {"missing_source_path": 1},
                    "net_new_records": 0,
                    "duplicate_against_prior_sources": 0,
                }
            )
            source_audits.append(source_audit)
            total_skip_reasons["missing_source_path"] += 1
            continue

        result = load_real_addresses([spec.path], spec.source_format, state_filter)
        input_files = [Path(path) for path in result.input_paths]
        all_input_files.extend(input_files)
        net_new_records = 0
        duplicate_against_prior = 0
        for record in result.records:
            key = query_text_key(_canonical_address_for_dedupe(record))
            if key in seen_keys:
                duplicate_against_prior += 1
                continue
            seen_keys.add(key)
            net_new_records += 1

        record_summary = _record_summary(result.records)
        covered_counties = mississippi_counties_in_paths(input_files)
        source_audit.update(
            {
                "status": "loaded",
                "input_file_count": len(input_files),
                "rows_seen": result.rows_seen,
                "rows_loaded": result.rows_loaded,
                "rows_skipped": result.rows_skipped,
                "skip_reasons": result.skip_reasons,
                "duplicate_rows": result.duplicate_rows,
                "duplicate_against_prior_sources": duplicate_against_prior,
                "net_new_records": net_new_records,
                "source_quality": source_quality_for_format(result.source_format),
                "detected_source_format": result.source_format,
                "covered_counties": covered_counties,
                "covered_county_count": len(covered_counties),
                **record_summary,
            }
        )
        source_audits.append(source_audit)
        total_skip_reasons.update(result.skip_reasons)
        total_rows_seen += result.rows_seen
        total_rows_loaded += result.rows_loaded
        total_rows_skipped += result.rows_skipped
        total_net_new += net_new_records
        total_duplicate_against_prior += duplicate_against_prior

    covered_counties = mississippi_counties_in_paths(all_input_files)
    summary: Dict[str, object] = {
        "source_count": len(source_audits),
        "rows_seen": total_rows_seen,
        "rows_loaded": total_rows_loaded,
        "rows_skipped": total_rows_skipped,
        "skip_reasons": dict(sorted(total_skip_reasons.items())),
        "net_new_records": total_net_new,
        "duplicate_against_prior_sources": total_duplicate_against_prior,
        "mississippi_county_coverage": {
            "covered_counties": covered_counties,
            "covered_county_count": len(covered_counties),
            "expected_county_count": len(MISSISSIPPI_COUNTIES),
            "missing_counties": sorted(set(MISSISSIPPI_COUNTIES) - set(covered_counties)),
        },
    }
    if baseline_reference_count is not None:
        summary["baseline_reference_count"] = baseline_reference_count
        summary["beats_baseline"] = total_net_new > baseline_reference_count

    return {
        "summary": summary,
        "sources": source_audits,
    }


def write_source_audit(audit: Dict[str, object], output_path: Path) -> None:
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(audit, indent=2, sort_keys=True), encoding="utf-8")
