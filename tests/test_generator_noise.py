import random
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_dataset_generator import (  # noqa: E402
    AddressRecord,
    DatasetBuilder,
    RenderStyle,
    canonical_address,
    op_compound_local_typo,
    render_address,
)


class GeneratorNoiseTests(unittest.TestCase):
    def test_compound_local_typo_changes_street_type_city_and_street(self) -> None:
        record = AddressRecord(
            address_id="REF_TEST",
            house_number="101",
            predir="",
            street_name="Candace",
            street_type="ST",
            suffixdir="",
            unit_type="",
            unit_value="",
            city="Newton",
            state="MS",
            zip_code="39345",
        )
        style = RenderStyle()

        tags = op_compound_local_typo(record, style, random.Random(8))

        self.assertIsNotNone(tags)
        tag_parts = set(tags.split("|"))
        self.assertIn("street_typo", tag_parts)
        self.assertIn("street_type_typo", tag_parts)
        self.assertIn("city_typo", tag_parts)
        self.assertNotEqual("Candace", record.street_name)
        self.assertNotEqual("ST", record.street_type)
        self.assertNotEqual("Newton", record.city)
        self.assertNotEqual("101 Candace ST, Newton MS 39345", render_address(record, style))

    def test_adversarial_no_match_prefers_near_neighbor_holdouts(self) -> None:
        target = AddressRecord(
            address_id="REF_TEST",
            house_number="101",
            predir="",
            street_name="Candace",
            street_type="ST",
            suffixdir="",
            unit_type="",
            unit_value="",
            city="Newton",
            state="MS",
            zip_code="39345",
        )
        near_neighbor = AddressRecord(
            address_id="SRC_NEAR",
            house_number="101",
            predir="",
            street_name="Candoose",
            street_type="ST",
            suffixdir="",
            unit_type="",
            unit_value="",
            city="Newton",
            state="MS",
            zip_code="39345",
        )
        far_neighbor = AddressRecord(
            address_id="SRC_FAR",
            house_number="999",
            predir="",
            street_name="Oak",
            street_type="RD",
            suffixdir="",
            unit_type="",
            unit_value="",
            city="Jackson",
            state="MS",
            zip_code="39201",
        )
        builder = DatasetBuilder(seed=11, real_address_pool=[far_neighbor, near_neighbor])
        builder.factory._seen_canonical.add(canonical_address(target))

        results = builder.build_adversarial_negative_bases([target], 1)

        self.assertEqual(1, len(results))
        self.assertEqual("Candoose", results[0].street_name)
        self.assertEqual("near_neighbor_same_house_city", builder.adversarial_reasons[results[0].address_id])


if __name__ == "__main__":
    unittest.main()
