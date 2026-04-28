import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_resolver import (  # noqa: E402
    Resolver,
    build_city_lookup,
    choose_combined_resolution,
    load_model,
    load_queries,
    load_reference,
)


class DemoAccuracySmokeTests(unittest.TestCase):
    def test_demo_hard_cases_resolve_as_expected(self) -> None:
        dataset_dir = PROJECT_ROOT / "examples" / "demo_reference"
        reference_rows, _reference_by_id = load_reference(dataset_dir / "reference_addresses.csv")
        resolver = Resolver(reference_rows, build_city_lookup(reference_rows))
        model, accept_threshold, review_threshold, _metadata = load_model(
            PROJECT_ROOT / "models" / "stage2_model.json",
            resolver,
        )

        for query in load_queries(dataset_dir / "queries.csv"):
            with self.subTest(query_id=query.query_id, query_address=query.query_address):
                parsed = resolver.parse(query.query_address)
                stage1 = resolver.resolve_stage1(parsed, review_threshold=review_threshold)
                stage2 = model.resolve(
                    parsed,
                    accept_threshold=accept_threshold,
                    review_threshold=review_threshold,
                )
                combined = choose_combined_resolution(stage1, stage2)

                if query.label:
                    self.assertEqual(query.true_match_id, combined.predicted_match_id)
                    self.assertEqual(query.canonical_address, combined.predicted_canonical_address)
                else:
                    self.assertEqual("", combined.predicted_match_id)


if __name__ == "__main__":
    unittest.main()
