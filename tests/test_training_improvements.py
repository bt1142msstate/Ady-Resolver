import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

from address_resolver import (  # noqa: E402
    QueryAddress,
    ReferenceAddress,
    Resolver,
    STAGE2_FEATURE_NAMES,
    Stage2Model,
    augment_reference_rows,
    build_city_lookup,
    build_stage2_training_rows,
    mine_stage2_hard_negatives,
    standardize_parts,
    train_stage2_weights,
)


def reference(
    address_id: str,
    house_number: str,
    street_name: str,
    street_type: str,
    city: str = "NEWTON",
    state: str = "MS",
    zip_code: str = "39345",
    source_quality: float = 0.5,
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
        source_quality=source_quality,
    )


class TrainingImprovementTests(unittest.TestCase):
    def test_reference_augmentation_preserves_labels_and_renames_collisions(self) -> None:
        primary = [reference("REF_1", "101", "CANDACE", "ST")]
        extra = [
            reference("REF_1", "102", "CANDACE", "ST"),
            reference("REF_2", "101", "CANDACE", "ST"),
        ]

        augmented, stats = augment_reference_rows(primary, extra)

        self.assertEqual(2, len(augmented))
        self.assertEqual("REF_1", augmented[0].address_id)
        self.assertEqual("AUG_0000001", augmented[1].address_id)
        self.assertEqual("102", augmented[1].house_number)
        self.assertEqual(1, stats["added_reference_count"])
        self.assertEqual(1, stats["duplicate_address_count"])
        self.assertEqual(1, stats["renamed_reference_count"])

    def test_stage2_mines_model_scored_hard_negatives(self) -> None:
        rows = [
            reference("TARGET", "101", "CANDACE", "ST"),
            reference("CONFUSER", "101", "CANDOOSE", "ST"),
            reference("NEARBY", "101", "CANDACE", "AVE"),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        target = rows[0]
        query = QueryAddress(
            query_id="Q1",
            split="train",
            label=1,
            true_match_id="TARGET",
            query_address="101 candoose st newton ms",
            canonical_address=target.canonical_address,
        )

        base_rows = build_stage2_training_rows(resolver, [query])
        weights = train_stage2_weights(
            base_rows.pair_rows,
            base_rows.calibration_rows,
            base_rows.feature_length,
        )
        model = Stage2Model(resolver=resolver, weights=weights)
        mined = mine_stage2_hard_negatives(resolver, [query], model)

        self.assertGreater(mined.stats["mined_pair_rows"], 0)
        self.assertGreater(mined.stats["mined_positive_hard_negatives"], 0)
        self.assertGreater(len(mined.calibration_rows), 0)

    def test_stage2_features_capture_phonetics_house_mismatch_and_source_quality(self) -> None:
        rows = [
            reference("TARGET", "306", "CLARK", "AVE", source_quality=1.0),
            reference("NEARBY", "301", "CLARK", "AVE", source_quality=0.62),
        ]
        resolver = Resolver(rows, build_city_lookup(rows))
        parsed = resolver.parse("306 clarke ave newton ms")

        features = resolver.candidate_features(parsed, "NEARBY")
        values = features.values
        index = {name: offset for offset, name in enumerate(STAGE2_FEATURE_NAMES)}

        self.assertEqual(len(STAGE2_FEATURE_NAMES), len(values))
        self.assertGreaterEqual(values[index["street_phonetic_similarity"]], 0.9)
        self.assertEqual(1.0, values[index["zip_city_consistency"]])
        self.assertEqual(1.0, values[index["house_mismatch_strong_context"]])
        self.assertAlmostEqual(0.62, values[index["source_quality"]])

        model = Stage2Model(resolver=resolver, weights=[0.0] * len(STAGE2_FEATURE_NAMES))
        accept_values = model.accept_feature_values(features, best_score=0.7, margin=0.2)
        self.assertEqual(0.7, accept_values[13])
        self.assertEqual(0.2, accept_values[14])
        self.assertGreaterEqual(accept_values[15], 0.9)


if __name__ == "__main__":
    unittest.main()
