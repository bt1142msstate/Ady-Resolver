import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_resolver import ReferenceAddress, Resolver, Stage2Model, build_city_lookup, standardize_parts  # noqa: E402


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

    def test_reordered_house_city_and_state_typos_resolve(self) -> None:
        parsed = self.resolver.parse("candece 101 se Netooailn Missppi")
        self.assertEqual("101 CANDECE ST, NEWTON MS", parsed.standardized_address)
        resolution = self.resolver.resolve_stage1(parsed, review_threshold=0.8)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_reordered_missing_digit_candidate_retrieves_same_street_city(self) -> None:
        parsed = self.resolver.parse("candece 10 se Netooailn Missppi")
        self.assertEqual("10 CANDECE ST, NEWTON MS", parsed.standardized_address)
        candidate_ids = self.resolver.candidate_ids(parsed, limit=10)
        self.assertIn("TARGET", candidate_ids)

    def test_scrambled_component_order_resolves(self) -> None:
        examples = [
            "newton ms candace st 101",
            "st candace 101 newton ms",
            "candace newton 101 st ms",
            "ms 101 newton candace st",
            "Netooailn Missppi candece se 101",
            "new1on candace se 101 mississipi",
        ]

        for raw in examples:
            with self.subTest(raw=raw):
                parsed = self.resolver.parse(raw)
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

        self.assertEqual("STARKVILLE", parsed.city)
        self.assertEqual("TARGET", resolution.predicted_match_id)

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

    def test_truncated_city_street_typos_and_type_confusion_resolve(self) -> None:
        rows = [
            reference("TARGET", "385", "COLLEGE VIEW", "ST", "STARKVILLE", zip_code="39759"),
            reference("CITY_COUNT", "101", "MAIN", "ST", "STARKVILLE", zip_code="39759"),
            reference("STAR_CITY", "385", "MAIN", "DR", "STAR", zip_code="39167"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        examples = [
            "385 collagr vieww dr stark MS",
            "385 collage view dr stark MS",
            "385 college vieww dr stark MS",
            "stark ms 385 collagr vieww dr",
            "collagr vieww 385 dr stark ms",
            "385 colage view dr starkvile ms",
            "385 collge viw dr stark ms",
            "385 colage view drive starkvile",
        ]

        for raw in examples:
            with self.subTest(raw=raw):
                parsed = resolver.parse(raw)
                resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

                self.assertEqual("STARKVILLE", parsed.city)
                self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_city_context_keeps_street_token_from_becoming_prefix_city(self) -> None:
        rows = [
            reference("TARGET", "306", "CLARK", "AVE", "NEWTON"),
            *[
                reference(f"CLARKSDALE_{index}", str(100 + index), "DELTA", "ST", "CLARKSDALE", zip_code="38614")
                for index in range(25)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        examples = [
            "306 clark avenue netwon missppi",
            "clark av 306 newto ms",
        ]

        for raw in examples:
            with self.subTest(raw=raw):
                parsed = resolver.parse(raw)
                resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

                self.assertEqual("NEWTON", parsed.city)
                self.assertEqual("CLARK", parsed.street_name)
                self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_fuzzy_city_variants_chain_after_inferring_state(self) -> None:
        rows = [
            reference("TARGET", "385", "COLLEGE VIEW", "ST", "STARKVILLE", zip_code="39759"),
            reference("TYPO_CITY", "99", "OTHER", "RD", "STARKVILEE", zip_code="39759"),
            *[
                reference(f"STARKVILLE_{index}", str(100 + index), "MAIN", "ST", "STARKVILLE", zip_code="39759")
                for index in range(25)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("starkvilee 385 collagr vieww street")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("STARKVILEE", parsed.city)
        self.assertIn(
            "385 COLLAGR VIEWW ST, STARKVILLE MS",
            [variant.standardized_address for _, variant in resolver.locality_variants(parsed)],
        )
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_five_digit_house_number_is_not_stolen_as_zip(self) -> None:
        rows = [
            reference("TARGET", "14400", "WILLIAMSBURG", "DR", "GULFPORT", zip_code="39503"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "GULFPORT", zip_code="39503"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("14400 willibsburg dr gulfpott mississipi")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("14400", parsed.house_number)
        self.assertEqual("", parsed.zip_code)
        self.assertEqual("GULFPORT", parsed.city)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_trailing_house_number_with_type_typo_and_city_typo_resolves(self) -> None:
        rows = [
            reference("TARGET", "1553", "TORRENCE", "DR", "BYRAM", zip_code="39272"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "BYRAM", zip_code="39272"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("missppi byrmm driev tojrenec 1553")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("1553", parsed.house_number)
        self.assertEqual("DR", parsed.street_type)
        self.assertEqual("BYRAM", parsed.city)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_global_city_typo_uses_house_street_context_without_state(self) -> None:
        rows = [
            reference("TARGET", "212", "PORTER", "ST", "SENATOBIA", zip_code="38668"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "SENATOBIA", zip_code="38668"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("portree 212 street sneattobia")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("SENATOBIA", parsed.city)
        self.assertEqual("PORTREE", parsed.street_name)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_contextual_city_reassignment_recovers_street_token_city_confusion(self) -> None:
        rows = [
            reference("TARGET", "2685", "PRAIRIE VIEW", "CIR", "TUPELO", zip_code="38826"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "TUPELO", zip_code="38826"),
            reference("PRAIRIE_CITY", "2685", "OTHER", "RD", "PRAIRIE", zip_code="39756"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("prairie vimw 2685 circde tupeplo")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("TUPELO", parsed.city)
        self.assertEqual("PRAIRIE VIMW", parsed.street_name)
        self.assertEqual("CIR", parsed.street_type)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_contextual_state_city_fuzzy_handles_heavy_city_typo(self) -> None:
        rows = [
            reference("TARGET", "160", "JOHNSTONE", "DR", "MADISON", zip_code="39110"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "MADISON", zip_code="39110"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("maiosn 160 johnstoone st missppi")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("MADISON", parsed.city)
        self.assertEqual("JOHNSTOONE", parsed.street_name)
        self.assertEqual("TARGET", resolution.predicted_match_id)

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

    def test_same_house_candidates_do_not_crowd_out_strong_local_street_match(self) -> None:
        rows = [
            reference("TARGET", "385", "COLLEGE VIEW", "ST", "STARKVILLE", zip_code="39759"),
            *[
                reference(f"SAME_HOUSE_{index}", "102", f"RANDOM {index}", "RD", "STARKVILLE", zip_code="39759")
                for index in range(90)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        parsed = resolver.parse("102 colage view starkville ms")
        candidate_ids = resolver.candidate_ids(parsed, limit=10)

        self.assertIn("TARGET", candidate_ids)

        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)
        ranked = model.rank_candidates(parsed, limit=5)
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertEqual("TARGET", ranked[0].reference_id)
        self.assertEqual("", resolution.predicted_match_id)
        self.assertEqual("stage2_no_match", resolution.stage)

    def test_view_token_can_remain_part_of_street_name(self) -> None:
        rows = [
            reference("TARGET", "102", "COLLEGE VIEW", "ST", "STARKVILLE", zip_code="39759"),
            reference("COTTAGE", "102", "COTTAGE", "LN", "STARKVILLE", zip_code="39759"),
            reference("CITY_COUNT", "101", "MAIN", "ST", "STARKVILLE", zip_code="39759"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)

        parsed = resolver.parse("102 colage view starkvile ms")
        ranked = model.rank_candidates(parsed, limit=3)
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertIn(
            "102 COLAGE VIEW, STARKVILLE MS",
            [variant.standardized_address for _, variant in resolver.stage2_variants(parsed)],
        )
        self.assertEqual("TARGET", ranked[0].reference_id)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_house_only_query_returns_no_candidate_suggestions(self) -> None:
        parsed = self.resolver.parse("102 ....")

        self.assertEqual([], self.resolver.candidate_ids(parsed, limit=10))

    def test_house_plus_city_typo_without_street_returns_no_candidate_suggestions(self) -> None:
        rows = [
            reference("STARKVILLE_ADDRESS", "102", "COLLEGE VIEW", "ST", "STARKVILLE", zip_code="39759"),
            reference("NATCHEZ_ADDRESS", "500", "CANAL", "ST", "NATCHEZ", zip_code="39120"),
            reference("NATCHEZ_STREET", "500", "NATCHEZ", "ST", "DURANT", zip_code="39090"),
            reference("GAUTIER_ADDRESS", "2620", "SOUTHERN", "DR", "GAUTIER", zip_code="39553"),
            reference("GAUTIER_COUNT", "100", "MAIN", "ST", "GAUTIER", zip_code="39553"),
            reference("UTICA_ADDRESS", "1102", "ROSS", "LN", "UTICA", zip_code="39175"),
            reference("UTICA_COUNT", "100", "MAIN", "ST", "UTICA", zip_code="39175"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        examples = [
            ("102 .... starkvile ms", "STARKVILLE"),
            ("500 .... nachez Missppi", "NATCHEZ"),
            ("2620 .... gaootier", "GAUTIER"),
            ("1102 .... utiac", "UTICA"),
        ]

        for raw, city in examples:
            with self.subTest(raw=raw):
                parsed = resolver.parse(raw)
                self.assertEqual(city, parsed.city)
                self.assertEqual("", parsed.street_name)
                self.assertEqual([], resolver.candidate_ids(parsed, limit=10))

    def test_trailing_city_typo_without_state_can_be_recovered_from_street_context(self) -> None:
        rows = [
            reference("TARGET", "124", "MARTIN LUTHER KING", "DR", "PURVIS", zip_code="39475"),
            *[
                reference(f"PURVIS_{index}", str(100 + index), "MAIN", "ST", "PURVIS", zip_code="39475")
                for index in range(5)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("125 marttin luther king drivee purvsi")

        self.assertEqual("PURVIS", parsed.city)
        self.assertEqual("MARTTIN LUTHER KING", parsed.street_name)

    def test_short_city_typo_prefers_common_city_over_source_typo(self) -> None:
        rows = [
            reference("TARGET", "500", "MEADE", "CT", "PEARL", zip_code="39208"),
            reference("TYPO_CITY", "900", "OTHER", "RD", "PERAL", zip_code="39208"),
            *[
                reference(f"PEARL_{index}", str(100 + index), "MAIN", "ST", "PEARL", zip_code="39208")
                for index in range(20)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))

        parsed = resolver.parse("500 meade ct perl ms")
        resolution = resolver.resolve_stage1(parsed, review_threshold=0.8)

        self.assertEqual("PEARL", parsed.city)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_non_ms_five_digit_house_before_highway_is_not_stolen_as_zip(self) -> None:
        rows = [
            reference("TARGET", "12331", "HWY 330", "", "COFFEEVILLE", zip_code="38922"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "COFFEEVILLE", zip_code="38922"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)

        parsed = resolver.parse("12331 hwy 330 coffeville ms")
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertEqual("12331", parsed.house_number)
        self.assertEqual("", parsed.zip_code)
        self.assertEqual("COFFEEVILLE", parsed.city)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_unit_typo_can_match_unit_embedded_in_source_street(self) -> None:
        rows = [
            reference("TARGET", "300", "MASON ST UNIT 218", "", "LAUREL", zip_code="39440"),
            reference("NEAR_UNIT", "300", "MASON ST UNIT 216", "", "LAUREL", zip_code="39440"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "LAUREL", zip_code="39440"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)

        parsed = resolver.parse("mason st uniit 218 300 laur ms")
        ranked = model.rank_candidates(parsed, limit=3)
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertEqual("300 MASON ST UNIT 218, LAUREL MS", parsed.standardized_address)
        self.assertEqual("TARGET", ranked[0].reference_id)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_reordered_ordinal_street_keeps_later_house_number(self) -> None:
        rows = [
            reference("TARGET", "527", "8TH", "AVE", "LAUREL", zip_code="39440"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "LAUREL", zip_code="39440"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)

        parsed = resolver.parse("8th ave 527 laur ms")
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertEqual("527", parsed.house_number)
        self.assertEqual("8TH", parsed.street_name)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_blvd_typo_bld_resolves_as_boulevard(self) -> None:
        rows = [
            reference("TARGET", "146", "ASHTON PARK", "BLVD", "MADISON", zip_code="39110"),
            reference("CITY_COUNT", "100", "MAIN", "ST", "MADISON", zip_code="39110"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)

        parsed = resolver.parse("146 ashon park bld madison ms")
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertEqual("BLVD", parsed.street_type)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_non_ms_state_name_in_middle_can_be_street_name(self) -> None:
        rows = [
            reference("TARGET", "1223", "TEXAS", "ST", "NATCHEZ", zip_code="39120"),
            reference("WRONG_CITY", "1223", "TATE", "ST", "CORINTH", zip_code="38834"),
            *[
                reference(f"NATCHEZ_{index}", str(100 + index), "MAIN", "ST", "NATCHEZ", zip_code="39120")
                for index in range(20)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)

        parsed = resolver.parse("natchze teexas sr 1223")
        ranked = model.rank_candidates(parsed, limit=3)
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertEqual("1223 TEEXAS ST, NATCHEZ MS", parsed.standardized_address)
        self.assertEqual("TARGET", ranked[0].reference_id)
        self.assertEqual("TARGET", resolution.predicted_match_id)

    def test_rare_city_typo_is_corrected_before_contextual_reassignment(self) -> None:
        rows = [
            reference("TARGET", "1313", "DIVISION", "ST", "WEST POINT", zip_code="39773"),
            reference("WRONG_CITY", "1313", "MADISON", "ST", "CORINTH", zip_code="38834"),
            reference("TYPO_CITY", "1", "OTHER", "ST", "WEST OINT", zip_code="39773"),
            *[
                reference(f"WEST_POINT_{index}", str(100 + index), "MAIN", "ST", "WEST POINT", zip_code="39773")
                for index in range(20)
            ],
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        model = Stage2Model(resolver=resolver, weights=[0.0] * 28)

        parsed = resolver.parse("divisioon 1313 sr west oint Missppi")
        ranked = model.rank_candidates(parsed, limit=3)
        resolution = model.resolve(parsed, accept_threshold=0.42, review_threshold=0.8)

        self.assertEqual("1313 DIVISIOON ST, WEST POINT MS", parsed.standardized_address)
        self.assertEqual("TARGET", ranked[0].reference_id)
        self.assertEqual("TARGET", resolution.predicted_match_id)


if __name__ == "__main__":
    unittest.main()
