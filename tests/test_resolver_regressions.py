import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_resolver import ReferenceAddress, Resolver, build_city_lookup, standardize_parts  # noqa: E402


def reference(
    address_id: str,
    house_number: str,
    street_name: str,
    street_type: str,
    city: str,
    state: str = "MS",
    zip_code: str = "39345",
) -> ReferenceAddress:
    standardized = standardize_parts(
        house_number,
        "",
        street_name,
        street_type,
        "",
        "",
        "",
        city,
        state,
        zip_code,
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
        city=city,
        state=state,
        zip_code=zip_code,
        standardized_address=standardized,
        street_signature=" ".join(part for part in [street_name, street_type] if part),
    )


class ResolverRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        rows = [
            reference("TARGET", "101", "CANDACE", "ST", "NEWTON"),
            reference("NEWTON_AVE", "101", "NEWTON", "AVE", "NEWTON"),
            reference("FOREST_AVE", "101", "FOREST", "AVE", "NEWTON"),
            reference("PARKER_ST", "101", "PARKER", "ST", "NEWTON"),
            reference("NATCHEZ", "101", "PECANWOOD", "DR", "NATCHEZ", zip_code="39120"),
        ]
        self.resolver = Resolver(rows, build_city_lookup(rows))

    def test_house_city_state_typo_candidates_include_local_street(self) -> None:
        parsed = self.resolver.parse("101 candoose st newton ms")
        candidate_ids = self.resolver.candidate_ids(parsed, limit=10)
        self.assertIn("TARGET", candidate_ids)

    def test_house_city_state_typo_resolves_to_local_street(self) -> None:
        parsed = self.resolver.parse("101 candoose st newton ms")
        resolution = self.resolver.resolve_stage1(parsed, review_threshold=0.8)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_compounded_street_type_and_city_typos_resolve(self) -> None:
        parsed = self.resolver.parse("101 candoowse sr newtooon MS")
        self.assertEqual("101 CANDOOWSE ST, NEWTON MS", parsed.standardized_address)
        resolution = self.resolver.resolve_stage1(parsed, review_threshold=0.8)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_heavy_city_and_ambiguous_type_typo_resolves(self) -> None:
        parsed = self.resolver.parse("101 candece se Netooailn MS")
        self.assertEqual("101 CANDECE ST, NEWTON MS", parsed.standardized_address)
        resolution = self.resolver.resolve_stage1(parsed, review_threshold=0.8)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_exact_city_keeps_southeast_suffix_direction(self) -> None:
        parsed = self.resolver.parse("101 candace se newton ms")
        self.assertEqual("101 CANDACE SE, NEWTON MS", parsed.standardized_address)

    def test_street_typo_without_locality_stays_unresolved(self) -> None:
        parsed = self.resolver.parse("101 candoose st")
        resolution = self.resolver.resolve_stage1(parsed, review_threshold=0.8)
        self.assertEqual("", resolution.predicted_match_id)

    def test_rare_city_typo_source_value_does_not_block_common_city_match(self) -> None:
        rows = [
            reference("TARGET", "34", "JOHN HENRY", "LN", "STARKVILLE", zip_code="39759"),
            reference("TYPO_CITY", "99", "OTHER", "RD", "STARKVILEE", zip_code="39759"),
            *[
                reference(f"STARKVILLE_{index}", str(100 + index), "CEDAR GROVE", "RD", "STARKVILLE", zip_code="39759")
                for index in range(25)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("34 john henry ln starkvilee ms")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("TARGET", resolution.predicted_match_id)
        self.assertIn(
            "34 JOHN HENRY LN, STARKVILLE MS",
            [variant.standardized_address for _, variant in resolver.locality_variants(parsed)],
        )

    def test_trailing_city_without_state_is_parsed_as_locality(self) -> None:
        rows = [
            reference("TARGET", "306", "CLARK", "AVE", "NEWTON"),
            reference("NEARBY", "301", "CLARK", "AVE", "NEWTON"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("306 clarke ave newton")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("306 CLARKE AVE, NEWTON", parsed.standardized_address)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_blank_zip_reference_does_not_win_zip_rule_when_query_has_no_zip(self) -> None:
        rows = [
            reference("TARGET", "301", "CLARK", "AVE", "NEWTON", zip_code="39345"),
            reference("BLANK_ZIP_WRONG_CITY", "301", "CLARK", "ST", "PICAYUNE", zip_code=""),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("301 clark ave newton ms")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_fuzzy_city_suffix_without_state_preserves_street_type(self) -> None:
        rows = [
            reference("TARGET", "385", "COLLEGE VIEW", "ST", "STARKVILLE", zip_code="39759"),
            reference("CITY_COUNT", "101", "MAIN", "ST", "STARKVILLE", zip_code="39759"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("385 colage view drive starkvile")
        candidate_ids = resolver.candidate_ids(parsed, limit=10)

        self.assertEqual("385 COLAGE VIEW DR, STARKVILLE MS", parsed.standardized_address)
        self.assertIn("TARGET", candidate_ids)

    def test_west_place_is_parsed_as_street_name_not_empty_directional(self) -> None:
        rows = [
            reference("TARGET", "419", "WEST", "PL", "MADISON", zip_code="39110"),
            reference("NEARBY", "419", "WARREN", "PL", "MADISON", zip_code="39110"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("419 west pl madison ms")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("419 WEST PL, MADISON MS", parsed.standardized_address)
        self.assertEqual("TARGET", resolution.predicted_match_id)


if __name__ == "__main__":
    unittest.main()
