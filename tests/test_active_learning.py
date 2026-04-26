import csv
import sys
import threading
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_resolver import (  # noqa: E402
    ReferenceAddress,
    Resolver,
    build_city_lookup,
    load_active_learning_feedback_queries,
    standardize_parts,
)
from resolver_app import (  # noqa: E402
    FEEDBACK_FIELDNAMES,
    ResolverService,
    append_active_learning_feedback,
    feedback_override_keys,
    load_feedback_overrides,
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


class ActiveLearningTests(unittest.TestCase):
    def test_feedback_writer_appends_header_and_json_candidates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "feedback.csv"

            append_active_learning_feedback(
                {
                    "created_at": "2026-04-25T00:00:00+00:00",
                    "feedback_type": "wrong",
                    "input_address": "101 candoowse sr newtooon MS",
                    "top_candidates": [{"reference_id": "REF_1", "score": 0.72}],
                },
                path,
            )

            with path.open(newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            self.assertEqual(FEEDBACK_FIELDNAMES, list(rows[0].keys()))
            self.assertIn('"REF_1"', rows[0]["top_candidates"])

    def test_feedback_queries_turn_corrections_and_wrongs_into_training_rows(self) -> None:
        target = reference("REF_TARGET", "101", "CANDACE", "ST")
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "feedback.csv"
            append_active_learning_feedback(
                {
                    "created_at": "2026-04-25T00:00:00+00:00",
                    "feedback_type": "correction",
                    "input_address": "101 candoowse sr newtooon MS",
                    "correct_canonical_address": target.canonical_address,
                },
                path,
            )
            append_active_learning_feedback(
                {
                    "created_at": "2026-04-25T00:01:00+00:00",
                    "feedback_type": "wrong",
                    "input_address": "301 clark ave newton ms",
                    "predicted_match_id": "REF_TARGET",
                    "predicted_canonical_address": target.canonical_address,
                },
                path,
            )

            queries, stats = load_active_learning_feedback_queries(path, [target])

        self.assertEqual(2, stats["queries_added"])
        self.assertEqual(1, stats["positive_queries_added"])
        self.assertEqual(1, stats["negative_queries_added"])
        self.assertEqual("REF_TARGET", queries[0].true_match_id)
        self.assertEqual(1, queries[0].label)
        self.assertEqual(0, queries[1].label)
        self.assertEqual("", queries[1].true_match_id)

    def test_feedback_overrides_trust_repeated_correct_inputs(self) -> None:
        target = reference("REF_TARGET", "101", "CANDACE", "ST")
        resolver = Resolver([target], build_city_lookup([target]))
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "feedback.csv"
            append_active_learning_feedback(
                {
                    "created_at": "2026-04-25T00:00:00+00:00",
                    "feedback_type": "correct",
                    "input_address": "101 candoowse sr newtooon MS",
                    "standardized_address": "101 CANDOOWSE ST, NEWTON MS",
                    "predicted_match_id": "REF_TARGET",
                    "predicted_canonical_address": target.canonical_address,
                },
                path,
            )

            overrides = load_feedback_overrides(resolver, path)

        for key in feedback_override_keys("101 candoowse sr newtooon MS", "101 CANDOOWSE ST, NEWTON MS"):
            self.assertEqual("REF_TARGET", overrides[key])

    def test_queue_training_coalesces_when_training_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            train_dir = root / "train"
            eval_dir = root / "eval"
            for dataset_dir in (train_dir, eval_dir):
                dataset_dir.mkdir()
                (dataset_dir / "reference_addresses.csv").write_text("address_id\n", encoding="utf-8")
                (dataset_dir / "queries.csv").write_text("query_id\n", encoding="utf-8")

            service = ResolverService.__new__(ResolverService)
            service.training_lock = threading.RLock()
            service.training_job = {
                "state": "running",
                "message": "Training started",
                "queued": False,
                "queued_at": "",
                "queue_reason": "",
                "log_tail": [],
            }
            service.train_dataset_dir = train_dir
            service.eval_dataset_dir = eval_dir
            service.feedback_row_count = lambda: 1

            status = service.queue_training("feedback:wrong")

        self.assertTrue(status["queued"])
        self.assertEqual("feedback:wrong", status["queue_reason"])


if __name__ == "__main__":
    unittest.main()
