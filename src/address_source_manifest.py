#!/usr/bin/env python3
"""Source manifest helpers for public and bring-your-own address inputs."""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple, Union

from address_source_common import *  # noqa: F401,F403
from address_source_parsers import _canonical_address_for_dedupe, load_real_addresses


PROJECT_ROOT = Path(__file__).resolve().parents[1]
VALID_SOURCE_FORMATS = {"auto", "maris", "maris_parcels", "nad", "openaddresses", "address_record", "generic"}


@dataclass(frozen=True)
class SourceSpec:
    name: str
    path: Path
    source_format: str = "auto"
    enabled: bool = True
    notes: str = ""


@dataclass
class CombinedRealAddressLoadResult:
    records: List[AddressRecord]
    source_results: List[RealAddressLoadResult]
    source_specs: List[SourceSpec]
    input_paths: List[str]
    source_format: str
    state: str
    rows_seen: int
    rows_loaded: int
    rows_skipped: int
    rows_loaded_after_cross_source_deduplication: int
    duplicate_across_sources: int
    skip_reasons: Dict[str, int]


def coerce_source_spec(spec: Union[SourceSpec, Tuple[Path, str], Tuple[str, str], Dict[str, object]]) -> SourceSpec:
    if isinstance(spec, SourceSpec):
        return spec
    if isinstance(spec, tuple):
        path, source_format = spec
        source_path = Path(path).expanduser().resolve()
        return SourceSpec(name=source_path.name or "source", path=source_path, source_format=str(source_format or "auto"))
    if isinstance(spec, dict):
        path_value = spec.get("path") or spec.get("cache_dir")
        if not path_value:
            raise ValueError("Source manifest entries require a path or cache_dir.")
        source_path = Path(str(path_value)).expanduser().resolve()
        source_format = str(spec.get("format") or spec.get("source_format") or "auto")
        return SourceSpec(
            name=str(spec.get("name") or source_path.name or "source"),
            path=source_path,
            source_format=source_format,
            enabled=bool(spec.get("enabled", True)),
            notes=str(spec.get("notes") or spec.get("note") or ""),
        )
    raise TypeError(f"Unsupported source spec: {spec!r}")


def _resolve_manifest_path(value: object, manifest_path: Path) -> Path:
    if value is None:
        raise ValueError("Source manifest entries require a path or cache_dir.")
    source_path = Path(str(value)).expanduser()
    if not source_path.is_absolute():
        source_path = manifest_path.parent / source_path
    return source_path.resolve()


def source_specs_from_manifest(manifest_path: Path, enabled_only: bool = True) -> List[SourceSpec]:
    manifest_path = manifest_path.expanduser().resolve()
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read source manifest {manifest_path}: {exc}") from exc

    entries = payload.get("sources") if isinstance(payload, dict) else payload
    if not isinstance(entries, list):
        raise ValueError("Source manifest must be a list or an object with a sources list.")

    specs: List[SourceSpec] = []
    for index, entry in enumerate(entries, 1):
        if not isinstance(entry, dict):
            raise ValueError(f"Source manifest entry {index} must be an object.")
        source_format = str(entry.get("format") or entry.get("source_format") or "auto")
        if source_format not in VALID_SOURCE_FORMATS:
            raise ValueError(f"Source manifest entry {index} has unsupported format: {source_format}")
        enabled = bool(entry.get("enabled", True))
        if enabled_only and not enabled:
            continue
        source_path = _resolve_manifest_path(entry.get("path") or entry.get("cache_dir"), manifest_path)
        specs.append(
            SourceSpec(
                name=str(entry.get("name") or source_path.name or f"source_{index}"),
                path=source_path,
                source_format=source_format,
                enabled=enabled,
                notes=str(entry.get("notes") or entry.get("note") or ""),
            )
        )
    return specs


def source_spec_tuples(specs: Iterable[Union[SourceSpec, Tuple[Path, str]]]) -> List[Tuple[Path, str]]:
    coerced = [coerce_source_spec(spec) for spec in specs]
    return [(spec.path, spec.source_format) for spec in coerced]


def default_public_source_specs(root: Optional[Path] = None) -> List[SourceSpec]:
    root = (root or PROJECT_ROOT).expanduser().resolve()
    candidates = [
        SourceSpec("maris_parcels", root / "datasets" / "source_cache" / "maris_parcels", "maris_parcels"),
        SourceSpec("maris_point_addresses", root / "datasets" / "source_cache" / "maris_point_addresses", "maris"),
        SourceSpec("openaddresses_processed", root / "datasets" / "source_cache" / "openaddresses_ms", "auto"),
        SourceSpec("openaddresses_direct", root / "datasets" / "source_cache" / "openaddresses_ms_direct", "openaddresses"),
        SourceSpec("manual_verified_ms", root / "datasets" / "source_cache" / "manual_verified_ms", "address_record"),
    ]
    return [spec for spec in candidates if spec.path.exists()]


def source_status_summary_for_path(path: Path) -> Dict[str, object]:
    path = path.expanduser().resolve()
    if not path.exists() or not path.is_dir():
        return {}

    summary: Dict[str, object] = {}
    openaddresses_manifest = path / OPENADDRESSES_DIRECT_MANIFEST_FILENAME
    if openaddresses_manifest.exists():
        try:
            payload = json.loads(openaddresses_manifest.read_text(encoding="utf-8"))
            sources = payload.get("sources", [])
        except (OSError, json.JSONDecodeError):
            sources = []
        if isinstance(sources, list):
            status_counts = Counter(
                str(item.get("status") or "unknown")
                for item in sources
                if isinstance(item, dict)
            )
            summary["openaddresses_direct_status_counts"] = dict(sorted(status_counts.items()))
            summary["openaddresses_direct_source_count"] = sum(status_counts.values())

    maris_manifest = path / MARIS_PARCEL_MANIFEST_FILENAME
    if maris_manifest.exists():
        try:
            payload = json.loads(maris_manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        layers = payload.get("layers") if isinstance(payload, dict) else None
        if isinstance(layers, list):
            summary["maris_parcel_layer_count"] = len(layers)
            summary["maris_parcel_expected_layer_count"] = payload.get("expected_layer_count")
    return summary


def load_real_addresses_from_source_specs(
    source_specs: Sequence[Union[SourceSpec, Tuple[Path, str]]],
    state_filter: str,
    limit: Optional[int] = None,
) -> CombinedRealAddressLoadResult:
    specs = []
    for source_spec in source_specs:
        coerced = coerce_source_spec(source_spec)
        if coerced.enabled:
            specs.append(coerced)
    records: List[AddressRecord] = []
    source_results: List[RealAddressLoadResult] = []
    seen_keys: set[str] = set()
    duplicate_across_sources = 0
    skip_reasons: Counter[str] = Counter()

    for spec in specs:
        remaining_limit = None if limit is None else max(limit - len(records), 0)
        if remaining_limit == 0:
            break
        load_result = load_real_addresses([spec.path], spec.source_format, state_filter, limit=remaining_limit)
        source_results.append(load_result)
        skip_reasons.update(load_result.skip_reasons)
        for record in load_result.records:
            key = query_text_key(_canonical_address_for_dedupe(record))
            if key in seen_keys:
                duplicate_across_sources += 1
                skip_reasons["duplicate_across_sources"] += 1
                continue
            seen_keys.add(key)
            records.append(record)
            if limit is not None and len(records) >= limit:
                break

    return CombinedRealAddressLoadResult(
        records=records,
        source_results=source_results,
        source_specs=specs,
        input_paths=[path for result in source_results for path in result.input_paths],
        source_format="+".join(result.source_format for result in source_results) if source_results else "",
        state=state_filter,
        rows_seen=sum(result.rows_seen for result in source_results),
        rows_loaded=sum(result.rows_loaded for result in source_results),
        rows_skipped=sum(result.rows_skipped for result in source_results),
        rows_loaded_after_cross_source_deduplication=len(records),
        duplicate_across_sources=duplicate_across_sources,
        skip_reasons=dict(sorted(skip_reasons.items())),
    )
