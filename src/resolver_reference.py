#!/usr/bin/env python3
"""Reference/query CSV loading and active-learning data helpers."""
from __future__ import annotations

import csv
from dataclasses import replace
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from resolver_models import QueryAddress, ReferenceAddress
from resolver_parsing import normalize_text, standardize_parts


def load_reference(path: Path) -> Tuple[List[ReferenceAddress], Dict[str, ReferenceAddress]]:
    rows: List[ReferenceAddress] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                source_quality = float(row.get("source_quality", "") or 0.5)
            except ValueError:
                source_quality = 0.5
            standardized = standardize_parts(
                row["house_number"].upper(),
                row["predir"].upper(),
                normalize_text(row["street_name"]),
                row["street_type"].upper(),
                row["suffixdir"].upper(),
                row["unit_type"].upper(),
                row["unit_value"].upper(),
                normalize_text(row["city"]),
                row["state"].upper(),
                row["zip_code"],
            )
            record = ReferenceAddress(
                address_id=row["address_id"],
                canonical_address=row["canonical_address"],
                house_number=row["house_number"].upper(),
                predir=row["predir"].upper(),
                street_name=normalize_text(row["street_name"]),
                street_type=row["street_type"].upper(),
                suffixdir=row["suffixdir"].upper(),
                unit_type=row["unit_type"].upper(),
                unit_value=row["unit_value"].upper(),
                city=normalize_text(row["city"]),
                state=row["state"].upper(),
                zip_code=row["zip_code"],
                standardized_address=standardized,
                street_signature=" ".join(
                    bit for bit in [row["predir"].upper(), normalize_text(row["street_name"]), row["street_type"].upper(), row["suffixdir"].upper()] if bit
                ),
                source_quality=max(0.0, min(1.0, source_quality)),
            )
            rows.append(record)
    return rows, {row.address_id: row for row in rows}


def reference_dedupe_key(row: ReferenceAddress) -> Tuple[str, ...]:
    return (
        row.house_number,
        row.predir,
        row.street_name,
        row.street_type,
        row.suffixdir,
        row.unit_type,
        row.unit_value,
        row.city,
        row.state,
        row.zip_code,
    )


def augment_reference_rows(
    primary_rows: Sequence[ReferenceAddress],
    extra_rows: Sequence[ReferenceAddress],
    id_prefix: str = "AUG",
) -> Tuple[List[ReferenceAddress], Dict[str, int]]:
    combined: List[ReferenceAddress] = []
    seen_keys = set()
    seen_ids = set()
    duplicate_address_count = 0
    renamed_reference_count = 0

    for row in primary_rows:
        combined.append(row)
        seen_keys.add(reference_dedupe_key(row))
        seen_ids.add(row.address_id)

    for row in extra_rows:
        key = reference_dedupe_key(row)
        if key in seen_keys:
            duplicate_address_count += 1
            continue

        address_id = row.address_id
        if not address_id or address_id in seen_ids:
            renamed_reference_count += 1
            address_id = f"{id_prefix}_{renamed_reference_count:07d}"
            while address_id in seen_ids:
                renamed_reference_count += 1
                address_id = f"{id_prefix}_{renamed_reference_count:07d}"

        combined.append(replace(row, address_id=address_id))
        seen_keys.add(key)
        seen_ids.add(address_id)

    return combined, {
        "base_reference_count": len(primary_rows),
        "extra_reference_count": len(extra_rows),
        "added_reference_count": len(combined) - len(primary_rows),
        "duplicate_address_count": duplicate_address_count,
        "renamed_reference_count": renamed_reference_count,
        "combined_reference_count": len(combined),
    }


def load_queries(path: Path) -> List[QueryAddress]:
    rows: List[QueryAddress] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                QueryAddress(
                    query_id=row["query_id"],
                    split=row["split"],
                    label=int(row["label"]),
                    true_match_id=row["true_match_id"],
                    query_address=row["query_address"],
                    canonical_address=row["canonical_address"],
                )
            )
    return rows


def reference_canonical_index(reference_rows: Sequence[ReferenceAddress]) -> Dict[str, ReferenceAddress]:
    return {normalize_text(row.canonical_address): row for row in reference_rows}


def load_active_learning_feedback_queries(
    path: Path,
    reference_rows: Sequence[ReferenceAddress],
) -> Tuple[List[QueryAddress], Dict[str, int]]:
    stats = {
        "rows_seen": 0,
        "queries_added": 0,
        "positive_queries_added": 0,
        "negative_queries_added": 0,
        "rows_skipped": 0,
        "missing_reference_rows": 0,
    }
    if not path.exists():
        return [], stats

    by_id = {row.address_id: row for row in reference_rows}
    by_canonical = reference_canonical_index(reference_rows)
    queries: List[QueryAddress] = []

    def lookup_reference(address_id: str, canonical: str) -> Optional[ReferenceAddress]:
        if address_id and address_id in by_id:
            return by_id[address_id]
        return by_canonical.get(normalize_text(canonical)) if canonical else None

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows_seen"] += 1
            feedback_type = row.get("feedback_type", "").strip()
            query_address = row.get("input_address", "").strip()
            if not query_address:
                stats["rows_skipped"] += 1
                continue

            label = 0
            true_match_id = ""
            canonical = "NO_MATCH"
            if feedback_type == "correction":
                reference = lookup_reference(
                    row.get("correct_reference_id", "").strip(),
                    row.get("correct_canonical_address", "").strip() or row.get("correct_address", "").strip(),
                )
                if reference is None:
                    stats["missing_reference_rows"] += 1
                    continue
                label = 1
                true_match_id = reference.address_id
                canonical = reference.canonical_address
            elif feedback_type == "correct":
                reference = lookup_reference(
                    row.get("predicted_match_id", "").strip(),
                    row.get("predicted_canonical_address", "").strip(),
                )
                if reference is not None:
                    label = 1
                    true_match_id = reference.address_id
                    canonical = reference.canonical_address
            elif feedback_type == "wrong":
                pass
            else:
                stats["rows_skipped"] += 1
                continue

            queries.append(
                QueryAddress(
                    query_id=f"AL_{len(queries) + 1:07d}",
                    split="train",
                    label=label,
                    true_match_id=true_match_id,
                    query_address=query_address,
                    canonical_address=canonical,
                )
            )
            stats["queries_added"] += 1
            if label:
                stats["positive_queries_added"] += 1
            else:
                stats["negative_queries_added"] += 1

    return queries, stats


def build_city_lookup(reference_rows: Sequence[ReferenceAddress]) -> Dict[Tuple[str, ...], str]:
    lookup: Dict[Tuple[str, ...], str] = {}
    for row in reference_rows:
        tokens = tuple(normalize_text(row.city).split())
        lookup[tokens] = row.city
    return lookup
