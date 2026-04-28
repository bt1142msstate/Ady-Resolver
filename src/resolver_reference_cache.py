#!/usr/bin/env python3
"""Reference-cache construction helpers for the resolver app."""
from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Sequence, Tuple, Union

from address_dataset_generator import (
    AddressRecord,
    MISSISSIPPI_COUNTIES,
    SourceSpec,
    canonical_address,
    coerce_source_spec,
    discover_input_files,
    load_real_addresses,
    mississippi_counties_in_paths,
    query_text_key,
    source_status_summary_for_path,
)
from address_resolver import normalize_text
from resolver_app_config import (
    REFERENCE_FIELDNAMES,
    ZIP_CITY_ENRICHMENT_MIN_RECORDS,
    ZIP_CITY_ENRICHMENT_MIN_SHARE,
    reference_csv_path,
    runtime_config_value,
)

def reference_cache_ready(dataset_dir: Path) -> bool:
    path = reference_csv_path(dataset_dir)
    return path.exists() and path.stat().st_size > 0


def default_source_specs() -> List[Tuple[Path, str]]:
    specs = []
    if runtime_config_value("DEFAULT_SOURCE_DIR").exists():
        specs.append((runtime_config_value("DEFAULT_SOURCE_DIR"), "maris_parcels"))
    if runtime_config_value("DEFAULT_POINT_SOURCE_DIR").exists():
        specs.append((runtime_config_value("DEFAULT_POINT_SOURCE_DIR"), "maris"))
    if runtime_config_value("DEFAULT_OPENADDRESSES_SOURCE_DIR").exists():
        specs.append((runtime_config_value("DEFAULT_OPENADDRESSES_SOURCE_DIR"), "auto"))
    if runtime_config_value("DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR").exists():
        specs.append((runtime_config_value("DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR"), "openaddresses"))
    if runtime_config_value("DEFAULT_VERIFIED_SOURCE_DIR").exists():
        specs.append((runtime_config_value("DEFAULT_VERIFIED_SOURCE_DIR"), "address_record"))
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


def build_reference_cache(dataset_dir: Path, source_specs: Sequence[Union[SourceSpec, Tuple[Path, str]]]) -> None:
    normalized_specs = [coerce_source_spec(spec) for spec in source_specs]
    normalized_specs = [spec for spec in normalized_specs if spec.enabled]
    if not normalized_specs:
        raise SystemExit("At least one real address source is required to build the app reference cache.")

    source_files: List[Path] = []
    for spec in normalized_specs:
        if not spec.path.exists():
            raise SystemExit(f"Real address source cache not found: {spec.path}")
        source_files.extend(discover_input_files([spec.path]))

    covered_counties = mississippi_counties_in_paths(source_files)
    missing_counties = sorted(set(MISSISSIPPI_COUNTIES) - set(covered_counties))
    if missing_counties:
        raise SystemExit(
            "Cannot build full app reference cache; source is missing counties: "
            + ", ".join(missing_counties)
        )

    print("Building full Mississippi app reference cache from:")
    for spec in normalized_specs:
        print(f"  - {spec.path} ({spec.source_format})")

    records = []
    seen_keys = set()
    source_results = []
    duplicate_across_sources = 0
    skip_reasons: Counter[str] = Counter()
    for spec in normalized_specs:
        load_result = load_real_addresses([spec.path], source_format=spec.source_format, state_filter="MS")
        source_results.append(load_result)
        skip_reasons.update(load_result.skip_reasons)
        for record in load_result.records:
            key = query_text_key(canonical_address(record))
            if key in seen_keys:
                duplicate_across_sources += 1
                skip_reasons["duplicate_across_sources"] += 1
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
        "skip_reasons": dict(sorted(skip_reasons.items())),
        "reference_records": len(records),
        "deduplicated_records": sum(result.rows_loaded for result in source_results) - source_unique_count,
        "duplicate_across_sources": duplicate_across_sources,
        "source_records_after_deduplication": source_unique_count,
        "derived_records_added": int(zip_city_enrichment["records_added"]),
        "zip_city_enrichment": zip_city_enrichment,
        "sources": [
            {
                "name": spec.name,
                "path": str(spec.path),
                "source_format": result.source_format,
                "configured_source_format": spec.source_format,
                "state": result.state,
                "rows_seen": result.rows_seen,
                "rows_loaded": result.rows_loaded,
                "rows_skipped": result.rows_skipped,
                "skip_reasons": result.skip_reasons,
                "duplicate_rows": result.duplicate_rows,
                "source_status": source_status_summary_for_path(spec.path),
                "notes": spec.notes,
                "input_paths": result.input_paths,
            }
            for spec, result in zip(normalized_specs, source_results)
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
