import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

import address_dataset_generator as generator  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
