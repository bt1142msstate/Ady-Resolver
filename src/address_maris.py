#!/usr/bin/env python3
"""MARIS point-address and parcel download helpers."""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from address_source_common import *  # noqa: F401,F403
from address_source_downloads import download_file, open_url, read_json_url

def download_maris_point_addresses(cache_dir: Path) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    with open_url(MARIS_POINT_ADDRESSES_URL, timeout=60) as response:
        html = response.read().decode("utf-8", errors="replace")
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    urls = sorted(set(MARIS_POINT_ADDRESS_ZIP_RE.findall(html)))
    if not urls:
        raise RuntimeError("No MARIS Mississippi point-address ZIP links were found.")

    downloaded: List[Path] = []
    for url in urls:
        target = cache_dir / url.rsplit("/", 1)[-1]
        downloaded.append(download_file(url, target, timeout=300))
    return downloaded


def maris_parcel_layer_filename(layer_name: str) -> str:
    upper_name = layer_name.upper()
    if "TALLA_WALTH" in upper_name or "TALLAHATCHIE" in upper_name and "WALTHALL" in upper_name:
        return "Tallahatchie_Walthall_maris_parcels.csv"
    county_name = re.sub(r"_?PARCELS.*$", "", layer_name, flags=re.IGNORECASE)
    county_name = county_name.replace("PEARLRIVER", "PearlRiver")
    county_name = re.sub(r"[^A-Za-z0-9]+", "_", county_name).strip("_")
    if not county_name:
        county_name = re.sub(r"[^A-Za-z0-9]+", "_", layer_name).strip("_") or "maris_parcels"
    return f"{county_name}_maris_parcels.csv"
def maris_parcel_layers(layer_limit: Optional[int] = None) -> List[Dict[str, object]]:
    service = read_json_url(MARIS_PARCELS_SERVICE_URL, {"f": "json"}, timeout=60)
    layers = [
        layer
        for layer in service.get("layers", [])
        if isinstance(layer, dict)
        and str(layer.get("type", "")).lower() == "feature layer"
        and "PARCEL" in str(layer.get("name", "")).upper()
        and int(layer.get("id", -1)) not in {0, 82, 83}
    ]
    layers.sort(key=lambda layer: int(layer.get("id", 0)))
    return layers[:layer_limit] if layer_limit else layers


def maris_parcel_manifest_path(cache_dir: Path) -> Path:
    return cache_dir / MARIS_PARCEL_MANIFEST_FILENAME


def maris_parcel_manifest_entries(layers: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
    return [
        {
            "id": int(layer["id"]),
            "name": str(layer["name"]),
            "filename": maris_parcel_layer_filename(str(layer["name"])),
        }
        for layer in layers
    ]


def read_maris_parcel_manifest(cache_dir: Path) -> Optional[List[Dict[str, object]]]:
    manifest_path = maris_parcel_manifest_path(cache_dir)
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if manifest.get("service_url") != MARIS_PARCELS_SERVICE_URL:
        return None
    layers = manifest.get("layers")
    if not isinstance(layers, list):
        return None
    entries = []
    for layer in layers:
        if not isinstance(layer, dict) or not layer.get("filename"):
            return None
        entries.append(layer)
    return entries


def write_maris_parcel_manifest(cache_dir: Path, layers: Sequence[Dict[str, object]]) -> None:
    entries = maris_parcel_manifest_entries(layers)
    manifest = {
        "service_url": MARIS_PARCELS_SERVICE_URL,
        "expected_layer_count": MARIS_PARCEL_EXPECTED_LAYER_COUNT,
        "layers": entries,
    }
    maris_parcel_manifest_path(cache_dir).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def write_maris_parcel_manifest_from_paths(cache_dir: Path, paths: Sequence[Path]) -> None:
    manifest = {
        "service_url": MARIS_PARCELS_SERVICE_URL,
        "expected_layer_count": MARIS_PARCEL_EXPECTED_LAYER_COUNT,
        "layers": [{"filename": path.name} for path in paths],
    }
    maris_parcel_manifest_path(cache_dir).write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def cached_maris_parcel_paths(cache_dir: Path, layer_limit: Optional[int] = None) -> Optional[List[Path]]:
    entries = read_maris_parcel_manifest(cache_dir)
    if entries:
        selected_entries = entries[:layer_limit] if layer_limit else entries
        paths = [cache_dir / str(entry["filename"]) for entry in selected_entries]
        if paths and all(path.exists() and path.stat().st_size > 0 for path in paths):
            return paths

    if layer_limit:
        return None

    csv_paths = sorted(
        path
        for path in cache_dir.glob("*.csv")
        if path.name != MARIS_PARCEL_MANIFEST_FILENAME and path.stat().st_size > 0
    )
    if len(csv_paths) >= MARIS_PARCEL_EXPECTED_LAYER_COUNT:
        covered_counties = mississippi_counties_in_paths(csv_paths)
        if len(covered_counties) == len(MISSISSIPPI_COUNTIES):
            return csv_paths
    return None


def download_maris_parcels(
    cache_dir: Path,
    layer_limit: Optional[int] = None,
    batch_size: int = 500,
    refresh: bool = False,
) -> List[Path]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    if not refresh:
        cached_paths = cached_maris_parcel_paths(cache_dir, layer_limit=layer_limit)
        if cached_paths:
            if not maris_parcel_manifest_path(cache_dir).exists() and not layer_limit:
                write_maris_parcel_manifest_from_paths(cache_dir, cached_paths)
            print(f"Using cached MARIS parcel CSVs from {cache_dir} ({len(cached_paths)} file(s)).")
            return cached_paths

    layers = maris_parcel_layers(layer_limit=layer_limit)
    downloaded: List[Path] = []
    cached_count = 0
    downloaded_count = 0
    for layer in layers:
        layer_id = int(layer["id"])
        layer_name = str(layer["name"])
        target = cache_dir / maris_parcel_layer_filename(layer_name)
        if not refresh and target.exists() and target.stat().st_size > 0:
            downloaded.append(target)
            cached_count += 1
            continue

        query_url = f"{MARIS_PARCELS_SERVICE_URL}/{layer_id}/query"
        id_payload = read_json_url(
            query_url,
            {
                "where": "SITEADD IS NOT NULL AND SITEADD <> ''",
                "returnIdsOnly": "true",
                "f": "json",
            },
            timeout=120,
        )
        object_ids = sorted(int(value) for value in id_payload.get("objectIds", []) or [])
        temporary_target = target.with_suffix(target.suffix + ".part")
        with temporary_target.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(MARIS_PARCEL_OUT_FIELDS))
            writer.writeheader()
            for start in range(0, len(object_ids), batch_size):
                batch = object_ids[start:start + batch_size]
                feature_payload = read_json_url(
                    query_url,
                    {
                        "objectIds": ",".join(str(value) for value in batch),
                        "outFields": ",".join(MARIS_PARCEL_OUT_FIELDS),
                        "returnGeometry": "false",
                        "f": "json",
                    },
                    timeout=120,
                )
                for feature in feature_payload.get("features", []) or []:
                    attributes = feature.get("attributes", {}) if isinstance(feature, dict) else {}
                    writer.writerow({field: attributes.get(field, "") for field in MARIS_PARCEL_OUT_FIELDS})
        temporary_target.replace(target)
        downloaded.append(target)
        downloaded_count += 1
    write_maris_parcel_manifest(cache_dir, layers)
    if cached_count:
        print(f"Reused {cached_count} cached MARIS parcel CSV(s) from {cache_dir}.")
    if downloaded_count:
        print(f"Downloaded {downloaded_count} MARIS parcel CSV(s) into {cache_dir}.")
    return downloaded
