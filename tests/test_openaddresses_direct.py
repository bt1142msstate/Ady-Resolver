import csv
import io
import json
import struct
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

import address_dataset_generator as generator  # noqa: E402


class BytesResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return self.payload


def make_dbf(rows):
    fields = [(name, 32) for name in rows[0]]
    header_length = 32 + len(fields) * 32 + 1
    record_length = 1 + sum(length for _name, length in fields)
    header = bytearray(32)
    header[0] = 3
    header[1:4] = b"\x1a\x01\x01"
    header[4:8] = struct.pack("<I", len(rows))
    header[8:10] = struct.pack("<H", header_length)
    header[10:12] = struct.pack("<H", record_length)
    descriptors = bytearray()
    for name, length in fields:
        descriptor = bytearray(32)
        encoded_name = name.encode("ascii")[:11]
        descriptor[: len(encoded_name)] = encoded_name
        descriptor[11] = ord("C")
        descriptor[16] = length
        descriptors.extend(descriptor)
    body = bytearray()
    for row in rows:
        record = bytearray(b" ")
        for name, length in fields:
            record.extend(str(row.get(name, "")).encode("ascii")[:length].ljust(length))
        body.extend(record)
    return bytes(header + descriptors + b"\r" + body + b"\x1a")


def make_shapefile_zip(dbf_name: str, rows) -> bytes:
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as archive:
        archive.writestr(dbf_name, make_dbf(rows))
    return raw.getvalue()


class OpenAddressesDirectTests(unittest.TestCase):
    def test_downloads_esri_source_to_cached_openaddresses_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_dir = root / "configs"
            cache_dir = root / "cache"
            config_dir.mkdir()
            (config_dir / "clay.json").write_text(
                json.dumps(
                    {
                        "coverage": {"state": "ms", "county": "Clay"},
                        "layers": {
                            "addresses": [
                                {
                                    "name": "county",
                                    "protocol": "ESRI",
                                    "data": "https://example.test/FeatureServer/5",
                                    "conform": {
                                        "format": "geojson",
                                        "number": "PHYNUM",
                                        "street": "RD_LABEL",
                                        "city": {
                                            "function": "regexp",
                                            "field": "CITYSTATEZ",
                                            "pattern": "^(.+)(?:, MS)(?: \\d{5})?$",
                                            "replace": "$1",
                                        },
                                        "region": {
                                            "function": "regexp",
                                            "field": "CITYSTATEZ",
                                            "pattern": "^(?:.+, )(MS)(?: \\d{5})?$",
                                            "replace": "$1",
                                        },
                                        "postcode": {
                                            "function": "regexp",
                                            "field": "CITYSTATEZ",
                                            "pattern": "^(?:.+, MS)(?: )?(\\d{5})?$",
                                            "replace": "$1",
                                        },
                                    },
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )

            original_read_json_url = generator.read_json_url

            def fake_read_json_url(url, params=None, timeout=120):
                params = params or {}
                if url == "https://example.test/FeatureServer/5":
                    return {"objectIdField": "OBJECTID"}
                if params.get("returnIdsOnly") == "true":
                    return {"objectIds": [1, 2]}
                if params.get("objectIds") == "1,2":
                    return {
                        "features": [
                            {
                                "attributes": {
                                    "OBJECTID": 1,
                                    "PHY_N": 476,
                                    "RD_LABEL": "HILLCREST RD",
                                    "ADDR": "476 HILLCREST RD",
                                    "CITYSTATEZ": "WEST POINT, MS 39773",
                                }
                            },
                            {
                                "attributes": {
                                    "OBJECTID": 2,
                                    "PHY_N": 0,
                                    "RD_LABEL": "UNASSIGNED RD",
                                    "CITYSTATEZ": "WEST POINT, MS 39773",
                                }
                            },
                        ]
                    }
                raise AssertionError(f"Unexpected request: {url} {params}")

            try:
                generator.read_json_url = fake_read_json_url
                paths = generator.download_openaddresses_ms_direct(cache_dir, config_dir)
                self.assertEqual([cache_dir / "clay.csv"], paths)

                with paths[0].open(newline="", encoding="utf-8") as handle:
                    rows = list(csv.DictReader(handle))
                self.assertEqual(1, len(rows))
                self.assertEqual("476", rows[0]["NUMBER"])
                self.assertEqual("Hillcrest Rd", rows[0]["STREET"])
                self.assertEqual("West Point", rows[0]["CITY"])

                def fail_read_json_url(url, params=None, timeout=120):
                    raise AssertionError("cached direct source should not be re-downloaded")

                generator.read_json_url = fail_read_json_url
                cached_paths = generator.download_openaddresses_ms_direct(cache_dir, config_dir)
                self.assertEqual(paths, cached_paths)
            finally:
                generator.read_json_url = original_read_json_url

    def test_direct_normalization_does_not_use_owner_mailing_city_zip(self) -> None:
        normalized = generator.openaddresses_direct_normalized_row(
            {
                "OBJECTID": 1,
                "STREET_NUM": 25,
                "STREET": "PINE RD",
                "CITY": "JACKSON",
                "ZIP": "39236",
            },
            {"number": "STREET_NUM", "street": "STREET"},
            "amite",
            1,
            {"state": "ms", "county": "Amite"},
        )

        self.assertIsNone(normalized)

    def test_arcgis_batch_fallback_recovers_individual_features(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_dir = root / "configs"
            cache_dir = root / "cache"
            config_dir.mkdir()
            (config_dir / "fallback.json").write_text(
                json.dumps(
                    {
                        "coverage": {"state": "ms", "county": "Newton"},
                        "layers": {
                            "addresses": [
                                {
                                    "name": "county",
                                    "protocol": "ESRI",
                                    "data": "https://example.test/FeatureServer/0",
                                    "conform": {
                                        "format": "geojson",
                                        "number": "NUM",
                                        "street": "STREET",
                                        "city": "CITY",
                                        "postcode": "ZIP",
                                    },
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )

            original_read_json_url = generator.read_json_url

            def fake_read_json_url(url, params=None, timeout=120):
                params = params or {}
                if url == "https://example.test/FeatureServer/0":
                    return {"objectIdField": "OBJECTID"}
                if params.get("returnIdsOnly") == "true":
                    return {"objectIds": [1, 2, 3, 4]}
                object_ids = str(params.get("objectIds", ""))
                if "," in object_ids:
                    raise RuntimeError("batch too large")
                object_id = int(object_ids)
                return {
                    "features": [
                        {
                            "attributes": {
                                "OBJECTID": object_id,
                                "NUM": object_id + 100,
                                "STREET": "PINE RD",
                                "CITY": "NEWTON",
                                "ZIP": "39345",
                            }
                        }
                    ]
                }

            try:
                generator.read_json_url = fake_read_json_url
                paths = generator.download_openaddresses_ms_direct(cache_dir, config_dir, batch_size=4)
            finally:
                generator.read_json_url = original_read_json_url

            self.assertEqual([cache_dir / "fallback.csv"], paths)
            with paths[0].open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(4, len(rows))
            self.assertEqual({"101", "102", "103", "104"}, {row["NUMBER"] for row in rows})
            manifest = json.loads((cache_dir / generator.OPENADDRESSES_DIRECT_MANIFEST_FILENAME).read_text())
            self.assertEqual(0, manifest["sources"][0]["object_ids_skipped"])

    def test_http_shapefile_zip_source_is_normalized_and_cached(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config_dir = root / "configs"
            cache_dir = root / "cache"
            config_dir.mkdir()
            (config_dir / "city_of_hattiesburg.json").write_text(
                json.dumps(
                    {
                        "coverage": {"state": "ms", "city": "Hattiesburg"},
                        "layers": {
                            "addresses": [
                                {
                                    "name": "city",
                                    "protocol": "http",
                                    "data": "https://example.test/hattiesburg.zip",
                                    "conform": {
                                        "format": "shapefile",
                                        "file": "Address_Points.shp",
                                        "id": "AddrID",
                                        "number": "AddrNum",
                                        "street": ["PreDir", "Streetname", "Suffix", "PostDir"],
                                        "unit": "Suite",
                                    },
                                }
                            ]
                        },
                    }
                ),
                encoding="utf-8",
            )
            payload = make_shapefile_zip(
                "Address_Points.dbf",
                [
                    {
                        "AddrID": "A1",
                        "AddrNum": "42",
                        "PreDir": "W",
                        "Streetname": "PINE",
                        "Suffix": "ST",
                        "PostDir": "",
                        "Suite": "",
                    }
                ],
            )
            original_open_url = generator.open_url

            def fake_open_url(url, timeout=60):
                self.assertEqual("https://example.test/hattiesburg.zip", url)
                return BytesResponse(payload)

            try:
                generator.open_url = fake_open_url
                paths = generator.download_openaddresses_ms_direct(cache_dir, config_dir)
            finally:
                generator.open_url = original_open_url

            self.assertEqual([cache_dir / "city_of_hattiesburg.csv"], paths)
            with paths[0].open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(1, len(rows))
            self.assertEqual("42", rows[0]["NUMBER"])
            self.assertEqual("W Pine St", rows[0]["STREET"])
            self.assertEqual("Hattiesburg", rows[0]["CITY"])

    def test_county_only_situs_rows_require_explicit_opt_in(self) -> None:
        attributes = {
            "OBJECTID": 1,
            "STREET_NUM": 25,
            "STREET": "PINE RD",
        }
        conform = {"number": "STREET_NUM", "street": "STREET"}
        coverage = {"state": "ms", "county": "Amite"}

        self.assertIsNone(
            generator.openaddresses_direct_normalized_row(attributes, conform, "amite", 1, coverage)
        )

        normalized = generator.openaddresses_direct_normalized_row(
            attributes,
            conform,
            "amite",
            1,
            coverage,
            allow_county_only=True,
        )

        self.assertIsNotNone(normalized)
        self.assertEqual("county_only_situs", normalized["LOCALITY_STATUS"])
        record = generator.real_row_to_record(normalized, "openaddresses", "REAL_TEST", "MS")
        self.assertIsNotNone(record)
        self.assertEqual("", record.city)
        self.assertEqual("", record.zip_code)
        self.assertEqual(generator.OPENADDRESSES_COUNTY_ONLY_SOURCE_QUALITY, record.source_quality)


if __name__ == "__main__":
    unittest.main()
