import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_dataset_generator import clean_house_number, canonical_address, maris_row_to_record, parse_street_line  # noqa: E402


class SourceParsingTests(unittest.TestCase):
    def test_maris_skips_placeholder_city_for_post_community(self) -> None:
        row = {
            "STNUM": "51",
            "NAME": "MOUNT PLEASANT",
            "TYPE": "RD",
            "City": "COUNTY",
            "Post_Comm": "FULTON",
            "Zipcode": "38843",
        }

        record = maris_row_to_record(row, "REAL_TEST", "MS")

        self.assertIsNotNone(record)
        self.assertEqual("Fulton", record.city)
        self.assertEqual("51 MOUNT PLEASANT RD, FULTON MS 38843", canonical_address(record))

    def test_maris_keeps_zip_when_only_placeholder_city_exists(self) -> None:
        row = {
            "STNUM": "1725",
            "NAME": "COUNTY ROAD 121",
            "Post_Comm": "RURAL",
            "Zipcode": "39153",
        }

        record = maris_row_to_record(row, "REAL_TEST", "MS")

        self.assertIsNotNone(record)
        self.assertEqual("", record.city)
        self.assertEqual("39153", record.zip_code)

    def test_maris_uses_later_valid_street_type_field(self) -> None:
        row = {
            "STNUM": "268",
            "NAME": "MAXINE",
            "TYPE": "Residential",
            "ST_TYPE": "DR",
            "Post_Comm": "PEARL",
            "Zipcode": "39208",
        }

        record = maris_row_to_record(row, "REAL_TEST", "MS")

        self.assertIsNotNone(record)
        self.assertEqual("DR", record.street_type)
        self.assertEqual("268 MAXINE DR, PEARL MS 39208", canonical_address(record))

    def test_maris_recovers_common_type_embedded_in_name(self) -> None:
        row = {
            "STNUM": "821",
            "NAME": "MYRTLE AVE",
            "Post_Comm": "NATCHEZ",
            "Zipcode": "39120",
        }

        record = maris_row_to_record(row, "REAL_TEST", "MS")

        self.assertIsNotNone(record)
        self.assertEqual("Myrtle", record.street_name)
        self.assertEqual("AVE", record.street_type)
        self.assertEqual("821 MYRTLE AVE, NATCHEZ MS 39120", canonical_address(record))

    def test_maris_does_not_split_uncommon_name_as_type_fallback(self) -> None:
        row = {
            "Address": "101",
            "FullName": "MAISON DE VILLE",
            "Street": "MAISON DE VILLE",
            "StreetType": "",
            "Post_Comm": "STARKVILLE",
            "Zipcode": "39759",
        }

        record = maris_row_to_record(row, "REAL_TEST", "MS")

        self.assertIsNotNone(record)
        self.assertEqual("Maison De Ville", record.street_name)
        self.assertEqual("", record.street_type)
        self.assertEqual("101 MAISON DE VILLE, STARKVILLE MS 39759", canonical_address(record))

    def test_known_newton_clarke_source_typo_is_corrected(self) -> None:
        row = {
            "SITEADD": "306 Clarke Ave",
            "SCITY": "Newton",
            "SSTATE": "MS",
            "SZIP": "39345",
        }

        from address_dataset_generator import maris_parcel_row_to_record, real_row_to_record

        direct_record = maris_parcel_row_to_record(row, "REAL_TEST", "MS")
        corrected_record = real_row_to_record(row, "maris_parcels", "REAL_TEST", "MS")

        self.assertIsNotNone(direct_record)
        self.assertEqual("Clarke", direct_record.street_name)
        self.assertIsNotNone(corrected_record)
        self.assertEqual("Clark", corrected_record.street_name)
        self.assertEqual("306 CLARK AVE, NEWTON MS 39345", canonical_address(corrected_record))

    def test_west_place_is_not_misread_as_predirection_only(self) -> None:
        predir, street_name, street_type, suffixdir = parse_street_line("WEST PL")

        self.assertEqual("", predir)
        self.assertEqual("West", street_name)
        self.assertEqual("PL", street_type)
        self.assertEqual("", suffixdir)

    def test_non_numeric_and_zero_house_numbers_are_rejected(self) -> None:
        self.assertEqual("", clean_house_number("CLOVERHILLRD"))
        self.assertEqual("", clean_house_number("0"))
        self.assertEqual("", clean_house_number("000A"))
        self.assertEqual("3C", clean_house_number("3C"))

    def test_maris_parcel_skips_side_of_road_descriptor_without_house_number(self) -> None:
        from address_dataset_generator import maris_parcel_row_to_record

        row = {
            "SITEADD": "0 S/S OF MAGNOLIA LANE",
            "SCITY": "",
            "SSTATE": "MS",
            "SZIP": "39470",
        }

        self.assertIsNone(maris_parcel_row_to_record(row, "REAL_TEST", "MS"))

    def test_maris_parcel_strips_side_of_road_marker_from_addressed_row(self) -> None:
        from address_dataset_generator import maris_parcel_row_to_record

        row = {
            "SITEADD": "1410 S/S HOWARD ST.",
            "SCITY": "CENTERVILLE",
            "SSTATE": "MS",
            "SZIP": "39631",
        }

        record = maris_parcel_row_to_record(row, "REAL_TEST", "MS")

        self.assertIsNotNone(record)
        self.assertEqual("Howard", record.street_name)
        self.assertEqual("ST", record.street_type)
        self.assertEqual("1410 HOWARD ST, CENTERVILLE MS 39631", canonical_address(record))

    def test_maris_parcel_skips_dod_note_rows(self) -> None:
        from address_dataset_generator import maris_parcel_row_to_record

        row = {
            "SITEADD": "279 MARY ETHEL DOD 11/11/15",
            "SCITY": "",
            "SSTATE": "MS",
            "SZIP": "39735",
        }

        self.assertIsNone(maris_parcel_row_to_record(row, "REAL_TEST", "MS"))

    def test_maris_parcel_skips_compact_dod_note_rows(self) -> None:
        from address_dataset_generator import maris_parcel_row_to_record

        row = {
            "SITEADD": "29095 JOHN DOD 11272020",
            "SCITY": "",
            "SSTATE": "MS",
            "SZIP": "38930",
        }

        self.assertIsNone(maris_parcel_row_to_record(row, "REAL_TEST", "MS"))

    def test_generic_loader_skips_direction_of_location_descriptors(self) -> None:
        from address_dataset_generator import real_row_to_record

        row = {
            "NUMBER": "2",
            "STREET": "N OF KAHNVILLE RD",
            "CITY": "GLOSTER",
            "REGION": "MS",
            "POSTCODE": "39638",
        }

        self.assertIsNone(real_row_to_record(row, "openaddresses", "REAL_TEST", "MS"))

    def test_duplicate_terminal_street_type_is_removed_when_name_has_real_token(self) -> None:
        predir, street_name, street_type, suffixdir = parse_street_line("CHERRY CV CV")

        self.assertEqual("", predir)
        self.assertEqual("Cherry", street_name)
        self.assertEqual("CV", street_type)
        self.assertEqual("", suffixdir)


if __name__ == "__main__":
    unittest.main()
