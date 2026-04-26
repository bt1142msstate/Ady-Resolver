import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_resolver import ReferenceAddress, Resolver, build_city_lookup, standardize_parts  # noqa: E402
import resolver_app as resolver_app_module  # noqa: E402
from resolver_app import (  # noqa: E402
    REFERENCE_FIELDNAMES,
    ResolverService,
    extract_batch_addresses,
    inspect_batch_columns,
    read_batch_upload,
    read_xlsx_upload,
    write_xlsx_report,
)


def reference(address_id: str, house_number: str, street_name: str, street_type: str) -> ReferenceAddress:
    standardized = standardize_parts(
        house_number,
        "",
        street_name,
        street_type,
        "",
        "",
        "",
        "NEWTON",
        "MS",
        "39345",
    )
    return ReferenceAddress(
        address_id=address_id,
        canonical_address=standardized,
        house_number=house_number,
        predir="",
        street_name=street_name,
        street_type=street_type,
        suffixdir="",
        unit_type="",
        unit_value="",
        city="NEWTON",
        state="MS",
        zip_code="39345",
        standardized_address=standardized,
        street_signature=" ".join(part for part in [street_name, street_type] if part),
    )


class BatchResolveTests(unittest.TestCase):
    def test_extracts_address_column_from_csv_upload(self) -> None:
        content = b"name,address\nA,newton ms candace st 101\nB,st candace 101 newton ms\n"

        addresses = read_batch_upload("addresses.csv", content)

        self.assertEqual(
            [(2, "", "newton ms candace st 101"), (3, "", "st candace 101 newton ms")],
            addresses,
        )

    def test_requested_address_header_is_not_treated_as_excel_column_letters(self) -> None:
        content = b"name,address\nA,newton ms candace st 101\n"

        addresses = read_batch_upload("addresses.csv", content, "address")

        self.assertEqual([(2, "", "newton ms candace st 101")], addresses)

    def test_requested_spreadsheet_column_skips_matching_header(self) -> None:
        rows = [["name", "address"], ["A", "101 Candace St Newton MS"]]

        addresses = extract_batch_addresses(rows, "B")

        self.assertEqual([(2, "", "101 Candace St Newton MS")], addresses)

    def test_retains_selected_source_id_column(self) -> None:
        content = b"record_id,address\nABC123,newton ms candace st 101\n"

        addresses = read_batch_upload("addresses.csv", content, "B", "A")

        self.assertEqual([(2, "ABC123", "newton ms candace st 101")], addresses)

    def test_inspects_batch_columns_for_dropdowns(self) -> None:
        content = b"record_id,address,note\nABC123,newton ms candace st 101,manual\n"

        inspected = inspect_batch_columns("addresses.csv", content)

        self.assertTrue(inspected["has_header"])
        self.assertEqual("B", inspected["guessed_address_column"])
        self.assertEqual("A", inspected["guessed_id_column"])
        self.assertEqual(
            ["A", "B", "C"],
            [column["value"] for column in inspected["columns"]],
        )

    def test_xlsx_writer_round_trips_rows_for_upload_reader(self) -> None:
        workbook = write_xlsx_report(
            ["address", "note"],
            [["Netooailn Missppi candece se 101", "messy"], ["new1on candace se 101 mississipi", "ocr"]],
        )

        rows = read_xlsx_upload(workbook)

        self.assertEqual("address", rows[0][0])
        self.assertEqual("Netooailn Missppi candece se 101", rows[1][0])
        self.assertEqual("new1on candace se 101 mississipi", rows[2][0])

    def test_import_verified_addresses_from_selected_column(self) -> None:
        existing = reference("REF_0000001", "101", "CANDACE", "ST")
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            dataset_dir = root / "dataset"
            dataset_dir.mkdir()
            (dataset_dir / "reference_addresses.csv").write_text(",".join(REFERENCE_FIELDNAMES) + "\n", encoding="utf-8")
            original_verified_dir = resolver_app_module.DEFAULT_VERIFIED_SOURCE_DIR
            resolver_app_module.DEFAULT_VERIFIED_SOURCE_DIR = root / "manual"
            try:
                service = ResolverService.__new__(ResolverService)
                service.dataset_dir = dataset_dir
                service.resolver = Resolver([existing], build_city_lookup([existing]))
                service.reference_count = 1
                service.next_reference_index = 2

                content = (
                    b"record_id,address\n"
                    b"A1,101 Candace St Newton MS 39345\n"
                    b"A2,102 Candace St Newton MS 39345\n"
                    b"A3,No City Here\n"
                )
                result = service.import_verified_addresses("verified.csv", content, "B", "county upload", True)
            finally:
                resolver_app_module.DEFAULT_VERIFIED_SOURCE_DIR = original_verified_dir

        self.assertEqual(3, result["row_count"])
        self.assertEqual(1, result["added_count"])
        self.assertEqual(1, result["existing_count"])
        self.assertEqual(1, result["failed_count"])
        self.assertEqual(2, result["reference_count"])


if __name__ == "__main__":
    unittest.main()
