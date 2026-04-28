#!/usr/bin/env python3
"""OpenAddresses download and normalization helpers."""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from address_source_common import *  # noqa: F401,F403
from address_source_downloads import open_url, read_json_url

def download_openaddresses_ms(cache_dir: Path) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open_url(OPENADDRESSES_RESULTS_URL, timeout=60) as response:
        html = response.read().decode("utf-8", errors="replace")
    urls = sorted(set(OPENADDRESSES_MS_ZIP_RE.findall(html)))
    if not urls:
        raise RuntimeError("No Mississippi OpenAddresses ZIP links were found in the OpenAddresses results index.")

    downloaded: List[Path] = []
    for url in urls:
        filename = url.rsplit("/", 1)[-1]
        source_name = url.rsplit("/us/ms/", 1)[-1].replace("/", "_")
        target = cache_dir / f"{source_name or filename}"
        if target.exists() and target.stat().st_size > 0:
            downloaded.append(target)
            continue
        with open_url(url, timeout=120) as response, target.open("wb") as handle:
            handle.write(response.read())
        downloaded.append(target)
    return downloaded


def download_openaddresses_ms_source_configs(config_dir: Path, refresh: bool = False) -> List[Path]:
    config_dir.mkdir(parents=True, exist_ok=True)
    cached_configs = sorted(path for path in config_dir.glob("*.json") if path.stat().st_size > 0)
    if cached_configs and not refresh:
        print(f"Using cached OpenAddresses Mississippi source configs from {config_dir} ({len(cached_configs)} file(s)).")
        return cached_configs

    with open_url(OPENADDRESSES_MS_SOURCES_API_URL, timeout=60) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    if not isinstance(payload, list):
        raise RuntimeError("OpenAddresses Mississippi source catalog did not return a file list.")

    downloaded: List[Path] = []
    for entry in payload:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", ""))
        download_url = str(entry.get("download_url", ""))
        if not name.endswith(".json") or not download_url:
            continue
        target = config_dir / name
        with open_url(download_url, timeout=60) as response:
            target.write_bytes(response.read())
        downloaded.append(target)

    if not downloaded:
        raise RuntimeError("No OpenAddresses Mississippi source configs were downloaded.")
    return sorted(downloaded)


def openaddresses_direct_manifest_path(cache_dir: Path) -> Path:
    return cache_dir / OPENADDRESSES_DIRECT_MANIFEST_FILENAME


def openaddresses_source_name(config_path: Path) -> str:
    return re.sub(r"[^A-Za-z0-9_-]+", "_", config_path.stem).strip("_") or "source"


def openaddresses_direct_target_path(cache_dir: Path, source_name: str, layer_index: int, layer_count: int) -> Path:
    suffix = f"_{layer_index + 1}" if layer_count > 1 else ""
    return cache_dir / f"{source_name}{suffix}.csv"


def dict_get_conform_value(row: Dict[str, object], field_name: object) -> str:
    field = str(field_name or "")
    if not field:
        return ""
    if field in row:
        return clean_real_value(row[field])
    lower = field.lower()
    for key, value in row.items():
        if str(key).lower().replace("\ufeff", "") == lower:
            return clean_real_value(value)
    normalized = clean_real_token(field)
    for key, value in row.items():
        if clean_real_token(key) == normalized:
            return clean_real_value(value)
    return ""


def openaddresses_regexp_replace(value: str, pattern: str, replace: str) -> str:
    match = re.search(pattern, value)
    if not match:
        return ""
    result = replace
    if not result and match.groups():
        return clean_real_value(match.group(1) or "")
    for index, group in enumerate(match.groups(), 1):
        result = result.replace(f"${index}", group or "")
    return clean_real_value(result)


def openaddresses_conform_value(row: Dict[str, object], spec: object) -> str:
    if spec is None:
        return ""
    if isinstance(spec, str):
        return dict_get_conform_value(row, spec)
    if isinstance(spec, list):
        return normalize_spaces(" ".join(openaddresses_conform_value(row, item) for item in spec)).strip()
    if not isinstance(spec, dict):
        return clean_real_value(spec)

    function = str(spec.get("function", "")).lower()
    field_value = dict_get_conform_value(row, spec.get("field"))
    if function == "prefixed_number":
        house_number, _street = split_house_number_from_street(field_value)
        return house_number
    if function == "postfixed_street":
        _house_number, street = split_house_number_from_street(field_value)
        return clean_street_name(street)
    if function == "regexp":
        return openaddresses_regexp_replace(field_value, str(spec.get("pattern", "")), str(spec.get("replace", "$1")))
    return ""


def openaddresses_direct_normalized_row(
    attributes: Dict[str, object],
    conform: Dict[str, object],
    source_name: str,
    source_id: object,
    coverage: Dict[str, object],
) -> Optional[Dict[str, str]]:
    house_number = clean_house_number(openaddresses_conform_value(attributes, conform.get("number")))
    street = clean_real_value(openaddresses_conform_value(attributes, conform.get("street")))
    if not house_number or not street:
        for field in OPENADDRESSES_DIRECT_FULL_ADDRESS_FALLBACK_FIELDS:
            full_address = dict_get_conform_value(attributes, field)
            if not full_address:
                continue
            fallback_number, fallback_street = split_house_number_from_street(full_address)
            house_number = house_number or fallback_number
            street = street or fallback_street
            if house_number and street:
                break

    house_number = clean_house_number(house_number)
    street = clean_street_name(street)
    if not house_number or house_number == "0" or not street:
        return None

    city = clean_city_candidate(openaddresses_conform_value(attributes, conform.get("city")))
    if not city and isinstance(coverage, dict) and coverage.get("city"):
        city = clean_city_candidate(coverage.get("city"))
    region = canonical_state(openaddresses_conform_value(attributes, conform.get("region")))
    if not region and isinstance(coverage, dict):
        region = canonical_state(coverage.get("state"))
    region = region or "MS"
    postcode = clean_zip_code(openaddresses_conform_value(attributes, conform.get("postcode")))
    if not city and not postcode:
        return None
    if not zip_code_matches_state(postcode, region):
        return None

    unit = clean_real_value(openaddresses_conform_value(attributes, conform.get("unit"))).upper()[:24]
    return {
        "NUMBER": house_number,
        "STREET": street,
        "UNIT": unit,
        "CITY": city,
        "REGION": region,
        "POSTCODE": postcode,
        "SOURCE": source_name,
        "SOURCE_ID": clean_real_value(source_id),
    }


def arcgis_object_id_field(layer_metadata: Dict[str, object]) -> str:
    object_id_field = str(layer_metadata.get("objectIdField", "") or layer_metadata.get("objectIdFieldName", ""))
    if object_id_field:
        return object_id_field
    for field in layer_metadata.get("fields", []) or []:
        if isinstance(field, dict) and str(field.get("type", "")).lower() == "esrifieldtypeoid":
            return str(field.get("name", "OBJECTID"))
    return "OBJECTID"


def read_arcgis_features_for_object_ids(query_url: str, object_ids: Sequence[int], timeout: int = 45) -> Tuple[List[Dict[str, object]], int, bool]:
    if not object_ids:
        return [], 0, False
    try:
        feature_payload = read_json_url(
            query_url,
            {
                "objectIds": ",".join(str(value) for value in object_ids),
                "outFields": "*",
                "returnGeometry": "false",
                "f": "json",
            },
            timeout=timeout,
        )
        features = [
            feature
            for feature in feature_payload.get("features", []) or []
            if isinstance(feature, dict)
        ]
        return features, 0, False
    except Exception:
        return [], len(object_ids), True


def openaddresses_conform_has_situs_locality(conform: Dict[str, object], coverage: Dict[str, object]) -> bool:
    return bool(conform.get("city") or conform.get("postcode") or (isinstance(coverage, dict) and coverage.get("city")))


def download_openaddresses_ms_direct(
    cache_dir: Path,
    config_dir: Path,
    batch_size: int = 250,
    refresh: bool = False,
    refresh_configs: bool = False,
    include_statewide: bool = False,
) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    config_paths = download_openaddresses_ms_source_configs(config_dir, refresh=refresh_configs)
    downloaded: List[Path] = []
    manifest_sources: List[Dict[str, object]] = []
    cached_count = 0
    downloaded_count = 0

    for config_path in config_paths:
        source_name = openaddresses_source_name(config_path)
        if source_name in OPENADDRESSES_DIRECT_SKIP_SOURCE_NAMES and not include_statewide:
            manifest_sources.append({"source": source_name, "status": "skipped_statewide_duplicate"})
            continue

        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            manifest_sources.append({"source": source_name, "status": "failed", "error": str(exc)})
            continue

        coverage = config.get("coverage", {}) if isinstance(config.get("coverage"), dict) else {}
        address_layers = [
            layer
            for layer in (config.get("layers", {}).get("addresses", []) if isinstance(config.get("layers"), dict) else [])
            if isinstance(layer, dict)
        ]
        for layer_index, layer in enumerate(address_layers):
            target = openaddresses_direct_target_path(cache_dir, source_name, layer_index, len(address_layers))
            protocol = str(layer.get("protocol", "")).upper()
            if protocol != "ESRI":
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "skipped_protocol",
                        "protocol": layer.get("protocol", ""),
                    }
                )
                continue
            conform = layer.get("conform", {}) if isinstance(layer.get("conform"), dict) else {}
            if not openaddresses_conform_has_situs_locality(conform, coverage):
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "skipped_no_situs_locality",
                    }
                )
                continue
            if not refresh and target.exists() and target.stat().st_size > 0:
                downloaded.append(target)
                cached_count += 1
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "cached",
                        "output": str(target),
                    }
                )
                continue

            layer_url = str(layer.get("data", "")).rstrip("/")
            if not layer_url or not conform:
                manifest_sources.append({"source": source_name, "layer": layer.get("name", ""), "status": "skipped_missing_layer"})
                continue

            temporary_target = target.with_suffix(target.suffix + ".part")
            try:
                metadata = read_json_url(layer_url, {"f": "json"}, timeout=90)
                object_id_field = arcgis_object_id_field(metadata)
                query_url = f"{layer_url}/query"
                id_payload = read_json_url(
                    query_url,
                    {"where": "1=1", "returnIdsOnly": "true", "f": "json"},
                    timeout=120,
                )
                object_ids = sorted(int(value) for value in id_payload.get("objectIds", []) or [])
                rows_written = 0
                object_ids_skipped = 0
                consecutive_failed_batches = 0
                with temporary_target.open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=list(OPENADDRESSES_DIRECT_FIELDNAMES))
                    writer.writeheader()
                    for start in range(0, len(object_ids), batch_size):
                        batch = object_ids[start:start + batch_size]
                        features, skipped, failed = read_arcgis_features_for_object_ids(query_url, batch)
                        object_ids_skipped += skipped
                        if failed:
                            consecutive_failed_batches += 1
                            if consecutive_failed_batches >= 3:
                                raise RuntimeError(
                                    f"ArcGIS feature batch failed {consecutive_failed_batches} consecutive times; "
                                    f"skipped {object_ids_skipped} object id(s)."
                                )
                            continue
                        consecutive_failed_batches = 0
                        for feature in features:
                            attributes = feature.get("attributes", {}) if isinstance(feature, dict) else {}
                            if not isinstance(attributes, dict):
                                continue
                            source_id = attributes.get(object_id_field, "")
                            normalized = openaddresses_direct_normalized_row(
                                attributes,
                                conform,
                                source_name,
                                source_id,
                                coverage,
                            )
                            if normalized is None:
                                continue
                            writer.writerow(normalized)
                            rows_written += 1
                temporary_target.replace(target)
                downloaded.append(target)
                downloaded_count += 1
                manifest_sources.append(
                    {
                        "source": source_name,
                        "layer": layer.get("name", ""),
                        "status": "downloaded",
                        "url": layer_url,
                        "object_id_count": len(object_ids),
                        "object_ids_skipped": object_ids_skipped,
                        "rows_written": rows_written,
                        "output": str(target),
                    }
                )
                print(f"Cached OpenAddresses direct source {source_name}: {rows_written:,} usable row(s).")
            except Exception as exc:
                if temporary_target.exists():
                    temporary_target.unlink()
                if target.exists() and target.stat().st_size > 0:
                    downloaded.append(target)
                    manifest_sources.append(
                        {
                            "source": source_name,
                            "layer": layer.get("name", ""),
                            "status": "failed_reused_cache",
                            "error": str(exc),
                            "output": str(target),
                        }
                    )
                else:
                    manifest_sources.append(
                        {
                            "source": source_name,
                            "layer": layer.get("name", ""),
                            "status": "failed",
                            "error": str(exc),
                        }
                    )

    manifest = {
        "source_catalog_url": OPENADDRESSES_MS_SOURCES_API_URL,
        "config_dir": str(config_dir),
        "cache_dir": str(cache_dir),
        "include_statewide": include_statewide,
        "sources": manifest_sources,
    }
    openaddresses_direct_manifest_path(cache_dir).write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    if cached_count:
        print(f"Reused {cached_count} cached OpenAddresses direct CSV(s) from {cache_dir}.")
    if downloaded_count:
        print(f"Downloaded {downloaded_count} OpenAddresses direct CSV(s) into {cache_dir}.")
    return downloaded
