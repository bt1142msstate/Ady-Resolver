import random
import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_dataset_generator import (  # noqa: E402
    AddressRecord,
    RenderStyle,
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


if __name__ == "__main__":
    unittest.main()
