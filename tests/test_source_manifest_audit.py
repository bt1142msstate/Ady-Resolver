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


class SourceManifestAuditTests(unittest.TestCase):
    def write_csv(self, path: Path, rows) -> None:
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)

    def test_source_manifest_parses_enabled_relative_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path = root / "addresses.csv"
            source_path.write_text("full_address\n1 Pine Rd, Newton MS 39345\n", encoding="utf-8")
            manifest_path = root / "sources.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "sources": [
                            {
                                "name": "mine",
                                "path": "addresses.csv",
                                "format": "generic",
                                "enabled": True,
                                "notes": "test source",
                            },
                            {
                                "name": "disabled",
                                "path": "missing.csv",
                                "format": "generic",
                                "enabled": False,
                            },
                        ]
                    }
                ),
                encoding="utf-8",
            )

            specs = generator.source_specs_from_manifest(manifest_path)

            self.assertEqual(1, len(specs))
            self.assertEqual("mine", specs[0].name)
            self.assertEqual(source_path.resolve(), specs[0].path)
            self.assertEqual("generic", specs[0].source_format)
            self.assertEqual("test source", specs[0].notes)

    def test_source_manifest_rejects_invalid_format(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "sources.json"
            manifest_path.write_text(
                json.dumps({"sources": [{"path": "addresses.csv", "format": "madeup"}]}),
                encoding="utf-8",
            )

            with self.assertRaises(ValueError):
                generator.source_specs_from_manifest(manifest_path)

    def test_source_audit_reports_skip_reasons_and_net_new_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_one = root / "one.csv"
            source_two = root / "two.csv"
            self.write_csv(
                source_one,
                [
                    {"NUMBER": "1", "STREET": "Pine Rd", "CITY": "Newton", "REGION": "MS", "POSTCODE": "39345"},
                    {"NUMBER": "1", "STREET": "Pine Rd", "CITY": "Newton", "REGION": "MS", "POSTCODE": "39345"},
                    {"NUMBER": "", "STREET": "Pine Rd", "CITY": "Newton", "REGION": "MS", "POSTCODE": "39345"},
                    {"NUMBER": "2", "STREET": "Oak Rd", "CITY": "", "REGION": "MS", "POSTCODE": ""},
                ],
            )
            self.write_csv(
                source_two,
                [
                    {"NUMBER": "1", "STREET": "Pine Rd", "CITY": "Newton", "REGION": "MS", "POSTCODE": "39345"},
                    {"NUMBER": "3", "STREET": "Cedar Rd", "CITY": "Newton", "REGION": "MS", "POSTCODE": "39345"},
                ],
            )
            specs = [
                generator.SourceSpec("one", source_one, "openaddresses"),
                generator.SourceSpec("two", source_two, "openaddresses"),
            ]

            audit = generator.audit_source_specs(specs, state_filter="MS")

            self.assertEqual(2, audit["summary"]["source_count"])
            self.assertEqual(2, audit["summary"]["net_new_records"])
            self.assertEqual(1, audit["summary"]["duplicate_against_prior_sources"])
            self.assertEqual(1, audit["sources"][0]["skip_reasons"]["duplicate_canonical"])
            self.assertIn("missing_or_invalid_house_number", audit["summary"]["skip_reasons"])
            self.assertIn("missing_locality", audit["summary"]["skip_reasons"])
            self.assertEqual(1, audit["sources"][0]["city_count"])
            self.assertEqual(1, audit["sources"][0]["zip_count"])


if __name__ == "__main__":
    unittest.main()
