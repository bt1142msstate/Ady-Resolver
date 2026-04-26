import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_resolver import CandidateScore, Resolution, choose_combined_resolution, compare_variant_metrics  # noqa: E402


class EvaluationMetricTests(unittest.TestCase):
    def test_combiner_prefers_stronger_stage2_same_match(self) -> None:
        stage1 = Resolution(
            predicted_match_id="REF_TARGET",
            predicted_canonical_address="101 CANDACE ST, NEWTON MS 39345",
            standardized_query_address="101 CANDECE ST, NEWTON MS",
            confidence=0.93,
            needs_review=True,
            stage="stage1_original_house_locality_fuzzy_street",
            top_candidates=(CandidateScore("REF_TARGET", 0.93),),
        )
        stage2 = Resolution(
            predicted_match_id="REF_TARGET",
            predicted_canonical_address="101 CANDACE ST, NEWTON MS 39345",
            standardized_query_address="101 CANDECE ST, NEWTON MS",
            confidence=0.99,
            needs_review=False,
            stage="stage2_model_original",
            top_candidates=(CandidateScore("REF_TARGET", 0.99),),
        )

        combined = choose_combined_resolution(stage1, stage2)

        self.assertEqual("stage2_model_original", combined.stage)
        self.assertFalse(combined.needs_review)

    def test_compare_variant_metrics_reports_deltas_and_pass_flags(self) -> None:
        baseline = {
            "accuracy": 0.70,
            "top3_accuracy": 0.55,
            "precision": 0.99,
            "recall": 0.55,
            "resolved_rate": 0.37,
            "coverage": 0.36,
            "accepted_accuracy": 0.99,
        }
        challenger = {
            "accuracy": 0.95,
            "top3_accuracy": 0.98,
            "precision": 0.98,
            "recall": 0.94,
            "resolved_rate": 0.64,
            "coverage": 0.89,
            "accepted_accuracy": 0.97,
        }

        comparison = compare_variant_metrics("stage2_vs_stage1", challenger, baseline)

        self.assertEqual("stage2_vs_stage1", comparison["name"])
        self.assertEqual(0.25, comparison["accuracy_delta"])
        self.assertEqual(0.39, comparison["recall_delta"])
        self.assertTrue(comparison["accuracy_gte_baseline"])
        self.assertTrue(comparison["recall_gte_baseline"])
        self.assertFalse(comparison["precision_gte_baseline"])
        self.assertTrue(comparison["overall_better_or_equal"])


if __name__ == "__main__":
    unittest.main()
