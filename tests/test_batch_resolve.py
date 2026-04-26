import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from resolver_app import extract_batch_addresses, read_batch_upload, read_xlsx_upload, write_xlsx_report  # noqa: E402


class BatchResolveTests(unittest.TestCase):
    def test_extracts_address_column_from_csv_upload(self) -> None:
        content = b"name,address\nA,newton ms candace st 101\nB,st candace 101 newton ms\n"

        addresses = read_batch_upload("addresses.csv", content)

        self.assertEqual(
            [(2, "newton ms candace st 101"), (3, "st candace 101 newton ms")],
            addresses,
        )

    def test_requested_address_header_is_not_treated_as_excel_column_letters(self) -> None:
        content = b"name,address\nA,newton ms candace st 101\n"

        addresses = read_batch_upload("addresses.csv", content, "address")

        self.assertEqual([(2, "newton ms candace st 101")], addresses)

    def test_requested_spreadsheet_column_skips_matching_header(self) -> None:
        rows = [["name", "address"], ["A", "101 Candace St Newton MS"]]

        addresses = extract_batch_addresses(rows, "B")

        self.assertEqual([(2, "101 Candace St Newton MS")], addresses)

    def test_xlsx_writer_round_trips_rows_for_upload_reader(self) -> None:
        workbook = write_xlsx_report(
            ["address", "note"],
            [["Netooailn Missppi candece se 101", "messy"], ["new1on candace se 101 mississipi", "ocr"]],
        )

        rows = read_xlsx_upload(workbook)

        self.assertEqual("address", rows[0][0])
        self.assertEqual("Netooailn Missppi candece se 101", rows[1][0])
        self.assertEqual("new1on candace se 101 mississipi", rows[2][0])


if __name__ == "__main__":
    unittest.main()
