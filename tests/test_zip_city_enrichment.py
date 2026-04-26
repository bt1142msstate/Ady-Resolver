import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_dataset_generator import AddressRecord, canonical_address, query_text_key  # noqa: E402
from resolver_app import add_zip_city_enrichment, zip_city_consensus  # noqa: E402


def record(address_id: str, house: str, street: str, city: str, zip_code: str) -> AddressRecord:
    return AddressRecord(
        address_id=address_id,
        house_number=house,
        predir="",
        street_name=street,
        street_type="RD",
        suffixdir="",
        unit_type="",
        unit_value="",
        city=city,
        state="MS",
        zip_code=zip_code,
    )


class ZipCityEnrichmentTests(unittest.TestCase):
    def test_consensus_requires_dominant_zip_city(self) -> None:
        records = [
            record("A1", "1", "PINE", "Brandon", "39047"),
            record("A2", "2", "PINE", "Brandon", "39047"),
            record("A3", "3", "PINE", "Pearl", "39047"),
            record("B1", "1", "OAK", "Newton", "39345"),
            record("B2", "2", "OAK", "Newton", "39345"),
            record("B3", "3", "OAK", "Newton", "39345"),
        ]

        consensus = zip_city_consensus(records, min_records=3, min_share=0.75)

        self.assertNotIn("39047", consensus)
        self.assertEqual("Newton", consensus["39345"])

    def test_adds_city_variant_without_removing_blank_city_source(self) -> None:
        records = [
            record("A1", "1", "OAK", "Newton", "39345"),
            record("A2", "2", "OAK", "Newton", "39345"),
            record("A3", "3", "OAK", "Newton", "39345"),
            record("BLANK", "10", "CEDAR", "", "39345"),
        ]
        seen = {query_text_key(canonical_address(item)) for item in records}

        stats = add_zip_city_enrichment(records, seen, min_records=3, min_share=0.95)

        self.assertEqual(1, stats["records_added"])
        self.assertEqual("", records[3].city)
        self.assertEqual("Newton", records[4].city)
        self.assertEqual("10 CEDAR RD, NEWTON MS 39345", canonical_address(records[4]))

    def test_skips_duplicate_inferred_variant(self) -> None:
        records = [
            record("A1", "1", "OAK", "Newton", "39345"),
            record("A2", "2", "OAK", "Newton", "39345"),
            record("A3", "3", "OAK", "Newton", "39345"),
            record("BLANK", "10", "CEDAR", "", "39345"),
            record("EXISTING", "10", "CEDAR", "Newton", "39345"),
        ]
        seen = {query_text_key(canonical_address(item)) for item in records}

        stats = add_zip_city_enrichment(records, seen, min_records=3, min_share=0.95)

        self.assertEqual(0, stats["records_added"])
        self.assertEqual(1, stats["duplicates_skipped"])


if __name__ == "__main__":
    unittest.main()
