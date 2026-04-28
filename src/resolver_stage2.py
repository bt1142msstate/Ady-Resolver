#!/usr/bin/env python3
"""Stage 2 candidate ranking, training, evaluation, and model persistence."""
from __future__ import annotations

import json
import math
import random
from collections import Counter, OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from resolver_models import (
    CalibrationTrainingRow,
    CandidateFeatures,
    CandidateScore,
    FeatureVector,
    PairTrainingRow,
    QueryAddress,
    Resolution,
    Stage2TrainingRows,
)


def sigmoid(value: float) -> float:
    if value >= 0:
        exp_value = math.exp(-value)
        return 1.0 / (1.0 + exp_value)
    exp_value = math.exp(value)
    return exp_value / (1.0 + exp_value)


def gini_impurity(labels: Sequence[int]) -> float:
    if not labels:
        return 0.0
    positives = sum(labels)
    negatives = len(labels) - positives
    p_pos = positives / len(labels)
    p_neg = negatives / len(labels)
    return 1.0 - p_pos * p_pos - p_neg * p_neg


def tree_leaf(labels: Sequence[int]) -> Dict[str, object]:
    probability = sum(labels) / len(labels) if labels else 0.0
    return {"leaf": True, "probability": probability, "count": len(labels)}


def fit_probability_tree(rows: Sequence[Tuple[Tuple[float, ...], int]], max_depth: int = 4, min_leaf: int = 30) -> Dict[str, object]:
    def build(subset: Sequence[Tuple[Tuple[float, ...], int]], depth: int) -> Dict[str, object]:
        labels = [label for _, label in subset]
        if depth >= max_depth or len(subset) <= 2 * min_leaf or len(set(labels)) <= 1:
            return tree_leaf(labels)

        feature_count = len(subset[0][0])
        best_split = None
        parent_impurity = gini_impurity(labels)
        if parent_impurity == 0.0:
            return tree_leaf(labels)

        for feature_idx in range(feature_count):
            values = sorted({features[feature_idx] for features, _ in subset})
            if len(values) <= 1:
                continue
            step = max(1, len(values) // 12)
            candidates = [values[idx] for idx in range(step, len(values), step)]
            if candidates and candidates[-1] == values[-1]:
                candidates.pop()
            for threshold in candidates:
                left = [item for item in subset if item[0][feature_idx] <= threshold]
                right = [item for item in subset if item[0][feature_idx] > threshold]
                if len(left) < min_leaf or len(right) < min_leaf:
                    continue
                left_labels = [label for _, label in left]
                right_labels = [label for _, label in right]
                split_impurity = (len(left) / len(subset)) * gini_impurity(left_labels) + (len(right) / len(subset)) * gini_impurity(right_labels)
                gain = parent_impurity - split_impurity
                if best_split is None or gain > best_split[0]:
                    best_split = (gain, feature_idx, threshold, left, right)

        if best_split is None or best_split[0] <= 0.0:
            return tree_leaf(labels)

        _, feature_idx, threshold, left, right = best_split
        return {
            "leaf": False,
            "feature_idx": feature_idx,
            "threshold": threshold,
            "left": build(left, depth + 1),
            "right": build(right, depth + 1),
        }

    return build(list(rows), depth=0)


def predict_tree_probability(tree: Dict[str, object], features: Sequence[float]) -> float:
    node = tree
    while not node.get("leaf", False):
        if features[int(node["feature_idx"])] <= float(node["threshold"]):
            node = node["left"]
        else:
            node = node["right"]
    return float(node["probability"])


class Stage2Model:
    ambiguous_margin_threshold = 0.01

    def __init__(
        self,
        resolver: Resolver,
        weights: Sequence[float],
        accept_tree: Optional[Dict[str, object]] = None,
        training_metadata: Optional[Dict[str, object]] = None,
    ) -> None:
        self.resolver = resolver
        self.weights = tuple(weights)
        self.accept_tree = accept_tree
        self.training_metadata = dict(training_metadata or {})
        self.rank_cache_limit = 4096
        self._rank_cache: OrderedDict[ParsedAddress, Tuple[Tuple[CandidateScore, ...], Dict[str, CandidateFeatures]]] = OrderedDict()

    def get_cached_rank(self, parsed: ParsedAddress) -> Optional[Tuple[Tuple[CandidateScore, ...], Dict[str, CandidateFeatures]]]:
        cached = self._rank_cache.get(parsed)
        if cached is not None:
            self._rank_cache.move_to_end(parsed)
        return cached

    def store_cached_rank(self, parsed: ParsedAddress, cached: Tuple[Tuple[CandidateScore, ...], Dict[str, CandidateFeatures]]) -> None:
        self._rank_cache[parsed] = cached
        self._rank_cache.move_to_end(parsed)
        if len(self._rank_cache) > self.rank_cache_limit:
            self._rank_cache.popitem(last=False)

    def clear_rank_cache(self) -> None:
        self._rank_cache.clear()

    def predict_probability(self, features: CandidateFeatures) -> float:
        score = sum(weight * value for weight, value in zip(self.weights, features.values))
        return sigmoid(score)

    def rank_candidates(
        self,
        parsed: ParsedAddress,
        limit: int = 5,
        candidate_limit: Optional[int] = None,
    ) -> Tuple[CandidateScore, ...]:
        cached = self.get_cached_rank(parsed) if candidate_limit is None else None
        if cached is None:
            variants = self.resolver.stage2_variants(parsed)
            candidate_ids = self.resolver.candidate_ids_for_variants(variants, limit=candidate_limit)
            scored = []
            features_by_id: Dict[str, CandidateFeatures] = {}
            for candidate_id in candidate_ids:
                features = self.resolver.candidate_features_for_variants(variants, candidate_id)
                features_by_id[candidate_id] = features
                probability = self.predict_probability(features)
                scored.append(
                    CandidateScore(reference_id=candidate_id, score=self.rank_probability(features, probability))
                )
            scored.sort(key=lambda item: item.score, reverse=True)
            cached = (tuple(scored), features_by_id)
            if candidate_limit is None:
                self.store_cached_rank(parsed, cached)
        ranked, _ = cached
        return ranked[:limit]

    def best_features(self, parsed: ParsedAddress, candidate_id: str) -> CandidateFeatures:
        variants = self.resolver.stage2_variants(parsed)
        cached = self.get_cached_rank(parsed)
        if cached is None:
            self.rank_candidates(parsed, limit=5)
            cached = self.get_cached_rank(parsed)
            if cached is None:
                raise RuntimeError("Rank cache missing after candidate ranking.")
        _, features_by_id = cached
        if candidate_id in features_by_id:
            return features_by_id[candidate_id]
        return self.resolver.candidate_features_for_variants(variants, candidate_id)

    def rank_probability(self, features: CandidateFeatures, probability: float) -> float:
        values = features.values
        if not features.variant.street_name:
            return probability

        street_text_evidence = max(values[2], values[3])
        street_evidence = max(street_text_evidence, 0.90 * values[23])
        locality_evidence = max(values[4], values[6], values[7], values[14], values[15])
        adjusted = probability

        if locality_evidence >= 0.86 and street_evidence >= 0.66 and street_text_evidence >= 0.58:
            adjusted = max(
                adjusted,
                min(
                    0.93,
                    0.42 * street_evidence
                    + 0.18 * values[4]
                    + 0.08 * values[14]
                    + 0.08 * values[25]
                    + 0.08 * values[27]
                    + 0.12 * values[5]
                    + 0.04 * values[8],
                ),
            )
        elif locality_evidence >= 0.86 and street_text_evidence < 0.58:
            adjusted = min(
                adjusted,
                0.20 + 0.30 * street_text_evidence + 0.08 * locality_evidence + 0.12 * values[5],
            )
        return max(0.0, min(1.0, adjusted))

    def resolve(self, parsed: ParsedAddress, accept_threshold: float, review_threshold: float) -> Resolution:
        ranked = self.rank_candidates(parsed)
        if not ranked:
            return Resolution(
                predicted_match_id="",
                predicted_canonical_address="",
                standardized_query_address=parsed.standardized_address,
                confidence=1.0,
                needs_review=False,
                stage="stage2_no_candidates",
                top_candidates=(),
            )

        best = ranked[0]
        margin = best.score - ranked[1].score if len(ranked) > 1 else best.score
        best_features = self.best_features(parsed, best.reference_id)
        decision_score = self.decision_score(best_features, best.score, margin)
        decision_score = self.guard_decision_score(best_features, decision_score)
        exact_full_match = self.is_exact_full_match(best_features)
        standardized_query = best_features.variant.standardized_address

        if decision_score >= accept_threshold and len(ranked) > 1 and margin < self.ambiguous_margin_threshold and not exact_full_match:
            return Resolution(
                predicted_match_id="",
                predicted_canonical_address="",
                standardized_query_address=standardized_query,
                confidence=max(0.0, min(1.0, decision_score)),
                needs_review=True,
                stage="stage2_ambiguous_margin",
                top_candidates=ranked,
            )

        if decision_score >= accept_threshold:
            candidate = self.resolver.reference_by_id[best.reference_id]
            return Resolution(
                predicted_match_id=candidate.address_id,
                predicted_canonical_address=candidate.canonical_address,
                standardized_query_address=standardized_query,
                confidence=decision_score,
                needs_review=decision_score < review_threshold,
                stage=f"stage2_model_{best_features.variant_reason}",
                top_candidates=ranked,
            )

        return Resolution(
            predicted_match_id="",
            predicted_canonical_address="",
            standardized_query_address=standardized_query,
            confidence=max(0.0, min(1.0, 1.0 - decision_score)),
            needs_review=max(0.0, min(1.0, 1.0 - decision_score)) < review_threshold,
            stage="stage2_no_match",
            top_candidates=ranked,
        )

    def is_exact_full_match(self, features: CandidateFeatures) -> bool:
        values = features.values
        return bool(
            values[16]
            and values[17]
            and (values[15] or values[6])
            and values[25] >= 0.5
        )

    def guard_decision_score(self, features: CandidateFeatures, decision_score: float) -> float:
        values = features.values
        if (
            features.variant.house_number
            and values[5] < 0.45
            and max(values[2], 0.90 * values[23], values[3]) >= 0.66
            and max(values[4], values[6], values[7], values[14], values[15]) >= 0.86
        ):
            return min(decision_score, 0.39)
        if (
            features.variant.street_name
            and values[16]
            and max(values[2], values[3]) < 0.55
            and max(values[4], values[6], values[7], values[14], values[15]) >= 0.86
        ):
            return min(decision_score, 0.39)
        return decision_score

    def decision_score(self, features: CandidateFeatures, best_score: float, margin: float) -> float:
        if self.accept_tree:
            return predict_tree_probability(self.accept_tree, self.accept_feature_values(features, best_score, margin))

        values = features.values
        return max(
            0.0,
            min(
                1.0,
                0.34 * values[22]
                + 0.15 * values[1]
                + 0.12 * values[2]
                + 0.08 * values[3]
                + 0.11 * values[5]
                + 0.07 * values[4]
                + 0.04 * values[6]
                + 0.02 * values[7]
                + 0.03 * values[14]
                + 0.02 * values[15]
                + 0.01 * best_score
                + 0.01 * max(0.0, margin)
                + 0.04 * values[23]
                + 0.02 * values[24]
                + 0.03 * values[25]
                + 0.02 * values[27]
                - 0.12 * values[26]
            ),
        )

    def accept_feature_values(self, features: CandidateFeatures, best_score: float, margin: float) -> Tuple[float, ...]:
        values = features.values
        return (
            values[1],
            values[2],
            values[3],
            values[4],
            values[5],
            values[6],
            values[7],
            values[14],
            values[15],
            values[16],
            values[17],
            values[18],
            values[22],
            best_score,
            max(0.0, margin),
            values[23],
            values[24],
            values[25],
            values[26],
            values[27],
        )

    def to_dict(self) -> Dict[str, object]:
        return {
            "weights": list(self.weights),
            "accept_tree": self.accept_tree,
            "training_metadata": self.training_metadata,
        }

    @classmethod
    def from_dict(cls, resolver: Resolver, payload: Dict[str, object]) -> "Stage2Model":
        weights = payload.get("weights")
        if not isinstance(weights, list) or not weights:
            raise ValueError("Saved model is missing weights.")
        return cls(
            resolver=resolver,
            weights=[float(value) for value in weights],
            accept_tree=payload.get("accept_tree"),
            training_metadata=payload.get("training_metadata")
            if isinstance(payload.get("training_metadata"), dict)
            else None,
        )


def build_stage2_training_rows(
    resolver: Resolver,
    train_queries: Sequence[QueryAddress],
    candidate_limit: int = 40,
) -> Stage2TrainingRows:
    pair_rows: List[PairTrainingRow] = []
    calibration_rows: List[CalibrationTrainingRow] = []
    feature_length = 0
    stats: Counter[str] = Counter()

    for query in train_queries:
        stats["training_queries"] += 1
        if query.label == 1:
            stats["positive_queries"] += 1
        else:
            stats["negative_queries"] += 1

        parsed = resolver.parse(query.query_address)
        variants = resolver.stage2_variants(parsed)
        candidate_ids = resolver.candidate_ids_for_variants(variants, limit=candidate_limit)
        if query.label == 1 and query.true_match_id and query.true_match_id not in candidate_ids:
            candidate_ids.append(query.true_match_id)
            stats["forced_positive_candidates"] += 1

        seen = set()
        negatives: List[Tuple[FeatureVector, float]] = []
        positive_values: Optional[FeatureVector] = None
        for candidate_id in candidate_ids:
            if candidate_id in seen:
                continue
            if candidate_id not in resolver.reference_by_id:
                stats["missing_reference_candidates"] += 1
                continue
            seen.add(candidate_id)
            features = resolver.candidate_features_for_variants(variants, candidate_id)
            feature_length = len(features.values)
            label = 1 if query.label == 1 and candidate_id == query.true_match_id else 0
            if label == 1:
                positive_values = features.values
            else:
                negatives.append((features.values, resolver.rough_score(parsed, resolver.reference_by_id[candidate_id])))

        negatives.sort(key=lambda item: item[1], reverse=True)
        if query.label == 1 and positive_values is None and query.true_match_id in resolver.reference_by_id:
            features = resolver.candidate_features(parsed, query.true_match_id)
            feature_length = len(features.values)
            positive_values = features.values

        if query.label == 1 and positive_values is not None:
            hard_negatives = [values for values, _ in negatives[:4]]
            if hard_negatives:
                pair_rows.append((positive_values, hard_negatives))
                stats["rough_hard_negative_rows"] += 1
                stats["rough_hard_negatives"] += len(hard_negatives)
            calibration_rows.append((positive_values, 1, 1.0))
            for values, _ in negatives[:2]:
                calibration_rows.append((values, 0, 0.5))
        elif query.label == 0:
            for values, _ in negatives[:2]:
                calibration_rows.append((values, 0, 0.35))

    stats["pair_rows"] = len(pair_rows)
    stats["calibration_rows"] = len(calibration_rows)
    return Stage2TrainingRows(pair_rows, calibration_rows, feature_length, dict(stats))


def train_stage2_weights(
    pair_rows: Sequence[PairTrainingRow],
    calibration_rows: Sequence[CalibrationTrainingRow],
    feature_length: int,
    pair_epochs: int = 10,
    calibration_epochs: int = 6,
) -> List[float]:
    weights = [0.0] * feature_length
    if feature_length <= 0:
        return weights

    pair_rows = list(pair_rows)
    calibration_rows = list(calibration_rows)

    pair_learning_rate = 0.08
    pair_l2 = 0.0001
    pair_rng = random.Random(17)
    for epoch in range(pair_epochs):
        pair_rng.shuffle(pair_rows)
        margin_target = 0.35
        for positive_values, negative_values_list in pair_rows:
            for negative_values in negative_values_list:
                margin = sum(
                    weight * (positive - negative)
                    for weight, positive, negative in zip(weights, positive_values, negative_values)
                )
                if margin >= margin_target:
                    continue
                for idx, (positive, negative) in enumerate(zip(positive_values, negative_values)):
                    regularization = 0.0 if idx == 0 else pair_l2 * weights[idx]
                    weights[idx] += pair_learning_rate * ((positive - negative) - regularization)
        pair_learning_rate *= 0.92

    calibration_learning_rate = 0.04
    calibration_l2 = 0.0002
    calibration_rng = random.Random(29)
    if calibration_rows:
        positives = sum(1 for _, label, _ in calibration_rows if label == 1)
        negatives = len(calibration_rows) - positives
        positive_scale = negatives / positives if positives and negatives else 1.0
        negative_scale = 1.0

        for epoch in range(calibration_epochs):
            calibration_rng.shuffle(calibration_rows)
            for values, label, row_weight in calibration_rows:
                margin = sum(weight * value for weight, value in zip(weights, values))
                prediction = sigmoid(margin)
                class_scale = positive_scale if label == 1 else negative_scale
                error = (label - prediction) * row_weight * class_scale
                for idx, value in enumerate(values):
                    regularization = 0.0 if idx == 0 else calibration_l2 * weights[idx]
                    weights[idx] += calibration_learning_rate * (error * value - regularization)
            calibration_learning_rate *= 0.9

    return weights


def mine_stage2_hard_negatives(
    resolver: Resolver,
    train_queries: Sequence[QueryAddress],
    model: Stage2Model,
    candidate_limit: int = 80,
    ranked_limit: int = 8,
) -> Stage2TrainingRows:
    pair_rows: List[PairTrainingRow] = []
    calibration_rows: List[CalibrationTrainingRow] = []
    feature_length = len(model.weights)
    stats: Counter[str] = Counter()

    for query in train_queries:
        parsed = resolver.parse(query.query_address)
        variants = resolver.stage2_variants(parsed)
        ranked = model.rank_candidates(parsed, limit=ranked_limit, candidate_limit=candidate_limit)
        if not ranked:
            continue
        stats["mined_queries_with_candidates"] += 1

        if query.label == 1:
            if not query.true_match_id or query.true_match_id not in resolver.reference_by_id:
                continue

            positive_features = resolver.candidate_features_for_variants(variants, query.true_match_id)
            positive_values = positive_features.values
            feature_length = len(positive_values)
            true_rank = next(
                (idx for idx, candidate in enumerate(ranked) if candidate.reference_id == query.true_match_id),
                None,
            )
            true_score = (
                ranked[true_rank].score
                if true_rank is not None
                else model.predict_probability(positive_features)
            )
            wrong_candidates: List[FeatureVector] = []
            for candidate in ranked:
                if candidate.reference_id == query.true_match_id:
                    continue
                features = resolver.candidate_features_for_variants(variants, candidate.reference_id)
                competitive = true_rank != 0 or candidate.score >= 0.40 or true_score - candidate.score < 0.20
                if not competitive:
                    continue
                wrong_candidates.append(features.values)
                calibration_rows.append((features.values, 0, 0.85))
                stats["mined_positive_negative_calibration_rows"] += 1
                if len(wrong_candidates) >= 3:
                    break

            if wrong_candidates:
                repeat_count = 2 if true_rank != 0 else 1
                for _ in range(repeat_count):
                    pair_rows.append((positive_values, wrong_candidates))
                stats["mined_pair_rows"] += repeat_count
                stats["mined_positive_hard_negatives"] += len(wrong_candidates) * repeat_count
                if true_rank != 0:
                    stats["first_pass_positive_rank_errors"] += 1
        else:
            for candidate in ranked[:3]:
                if candidate.score < 0.35:
                    continue
                features = resolver.candidate_features_for_variants(variants, candidate.reference_id)
                feature_length = len(features.values)
                calibration_rows.append((features.values, 0, 0.9))
                stats["mined_no_match_calibration_rows"] += 1

    stats["pair_rows"] = len(pair_rows)
    stats["calibration_rows"] = len(calibration_rows)
    return Stage2TrainingRows(pair_rows, calibration_rows, feature_length, dict(stats))


def fit_stage2_model(resolver: Resolver, train_queries: Sequence[QueryAddress]) -> Stage2Model:
    base_rows = build_stage2_training_rows(resolver, train_queries)
    first_pass_weights = train_stage2_weights(
        base_rows.pair_rows,
        base_rows.calibration_rows,
        base_rows.feature_length,
    )
    first_pass_model = Stage2Model(resolver=resolver, weights=first_pass_weights)
    mined_rows = mine_stage2_hard_negatives(resolver, train_queries, first_pass_model)

    pair_rows = [*base_rows.pair_rows, *mined_rows.pair_rows]
    calibration_rows = [*base_rows.calibration_rows, *mined_rows.calibration_rows]
    feature_length = max(base_rows.feature_length, mined_rows.feature_length)
    if mined_rows.pair_rows or mined_rows.calibration_rows:
        weights = train_stage2_weights(pair_rows, calibration_rows, feature_length)
    else:
        weights = first_pass_weights

    training_metadata: Dict[str, object] = {
        "hard_negative_mining": True,
        "base": base_rows.stats,
        "mined": mined_rows.stats,
        "final_pair_rows": len(pair_rows),
        "final_calibration_rows": len(calibration_rows),
    }

    rank_model = Stage2Model(resolver=resolver, weights=weights, training_metadata=training_metadata)

    accept_rows: List[Tuple[FeatureVector, int]] = []
    for query in train_queries:
        parsed = resolver.parse(query.query_address)
        ranked = rank_model.rank_candidates(parsed)
        if not ranked:
            continue
        best = ranked[0]
        margin = best.score - ranked[1].score if len(ranked) > 1 else best.score
        best_features = rank_model.best_features(parsed, best.reference_id)
        accept_rows.append(
            (
                rank_model.accept_feature_values(best_features, best.score, margin),
                1 if (query.label == 1 and best.reference_id == query.true_match_id) else 0,
            )
        )

    accept_tree = fit_probability_tree(accept_rows, max_depth=4, min_leaf=40)
    training_metadata["accept_rows"] = len(accept_rows)
    return Stage2Model(
        resolver=resolver,
        weights=weights,
        accept_tree=accept_tree,
        training_metadata=training_metadata,
    )


def is_correct(query: QueryAddress, resolution: Resolution) -> bool:
    if query.label == 1:
        return resolution.predicted_match_id == query.true_match_id
    return resolution.predicted_match_id == ""


def choose_combined_resolution(stage1: Resolution, stage2: Resolution) -> Resolution:
    if not stage1.predicted_match_id:
        return stage2
    if not stage2.predicted_match_id:
        return stage1
    if stage1.predicted_match_id == stage2.predicted_match_id and stage2.confidence > stage1.confidence:
        return stage2
    if stage1.needs_review and not stage2.needs_review and stage2.confidence >= stage1.confidence:
        return stage2
    return stage1


def evaluate_variant(name: str, queries: Sequence[QueryAddress], resolutions: Dict[str, Resolution]) -> Dict[str, object]:
    correct = sum(1 for query in queries if is_correct(query, resolutions[query.query_id]))
    accepted = sum(1 for query in queries if not resolutions[query.query_id].needs_review)
    accepted_correct = sum(1 for query in queries if not resolutions[query.query_id].needs_review and is_correct(query, resolutions[query.query_id]))

    predicted_positive = sum(1 for query in queries if resolutions[query.query_id].predicted_match_id)
    predicted_negative = len(queries) - predicted_positive
    true_positive = sum(1 for query in queries if query.label == 1 and resolutions[query.query_id].predicted_match_id == query.true_match_id)
    actual_positive = sum(1 for query in queries if query.label == 1)

    top3_hits = 0
    positive_count = 0
    stage1_count = 0
    stage2_count = 0
    for query in queries:
        resolution = resolutions[query.query_id]
        if resolution.stage.startswith("stage1"):
            stage1_count += 1
        else:
            stage2_count += 1
        if query.label == 1:
            positive_count += 1
            top_candidate_ids = [candidate.reference_id for candidate in resolution.top_candidates[:3]]
            if query.true_match_id in top_candidate_ids:
                top3_hits += 1

    precision = true_positive / predicted_positive if predicted_positive else 0.0
    recall = true_positive / actual_positive if actual_positive else 0.0
    coverage = accepted / len(queries) if queries else 0.0
    accepted_accuracy = accepted_correct / accepted if accepted else 0.0

    return {
        "name": name,
        "accuracy": round(correct / len(queries), 4) if queries else 0.0,
        "top3_accuracy": round(top3_hits / positive_count, 4) if positive_count else 0.0,
        "precision": round(precision, 4),
        "recall": round(recall, 4),
        "resolved_count": predicted_positive,
        "unresolved_count": predicted_negative,
        "resolved_rate": round(predicted_positive / len(queries), 4) if queries else 0.0,
        "coverage": round(coverage, 4),
        "accepted_accuracy": round(accepted_accuracy, 4),
        "stage1_share": round(stage1_count / len(queries), 4) if queries else 0.0,
        "stage2_share": round(stage2_count / len(queries), 4) if queries else 0.0,
    }


def compare_variant_metrics(name: str, challenger: Dict[str, object], baseline: Dict[str, object]) -> Dict[str, object]:
    numeric_keys = (
        "accuracy",
        "top3_accuracy",
        "precision",
        "recall",
        "resolved_rate",
        "coverage",
        "accepted_accuracy",
    )
    comparison: Dict[str, object] = {"name": name}
    for key in numeric_keys:
        comparison[f"{key}_delta"] = round(float(challenger.get(key, 0.0)) - float(baseline.get(key, 0.0)), 4)
    comparison["accuracy_gte_baseline"] = float(challenger.get("accuracy", 0.0)) >= float(baseline.get("accuracy", 0.0))
    comparison["recall_gte_baseline"] = float(challenger.get("recall", 0.0)) >= float(baseline.get("recall", 0.0))
    comparison["precision_gte_baseline"] = float(challenger.get("precision", 0.0)) >= float(baseline.get("precision", 0.0))
    comparison["accepted_accuracy_gte_baseline"] = float(challenger.get("accepted_accuracy", 0.0)) >= float(baseline.get("accepted_accuracy", 0.0))
    comparison["overall_better_or_equal"] = bool(comparison["accuracy_gte_baseline"] and comparison["recall_gte_baseline"])
    return comparison


def choose_accept_threshold(model: Stage2Model, resolver: Resolver, train_queries: Sequence[QueryAddress]) -> float:
    scored: List[Tuple[QueryAddress, float, str]] = []
    for query in train_queries:
        parsed = resolver.parse(query.query_address)
        ranked = model.rank_candidates(parsed)
        if ranked:
            best = ranked[0]
            margin = best.score - ranked[1].score if len(ranked) > 1 else best.score
            features = model.best_features(parsed, best.reference_id)
            decision_score = model.decision_score(features, best.score, margin)
            scored.append((query, decision_score, best.reference_id))
        else:
            scored.append((query, 0.0, ""))

    best_threshold = 0.70
    best_accuracy = -1.0
    for threshold_step in range(25, 98):
        threshold = threshold_step / 100.0
        correct = 0
        for query, score, candidate_id in scored:
            predicted = candidate_id if score >= threshold else ""
            if query.label == 1 and predicted == query.true_match_id:
                correct += 1
            elif query.label == 0 and predicted == "":
                correct += 1
        accuracy = correct / len(scored) if scored else 0.0
        if accuracy > best_accuracy:
            best_accuracy = accuracy
            best_threshold = threshold
    return best_threshold


def choose_review_threshold(resolutions: Iterable[Tuple[QueryAddress, Resolution]], target_accuracy: float = 0.97) -> float:
    candidates = [round(step / 100.0, 2) for step in range(60, 100)]
    best_threshold = 0.90
    best_coverage = -1.0
    paired = list(resolutions)
    for threshold in candidates:
        accepted = [(query, resolution) for query, resolution in paired if resolution.confidence >= threshold]
        if not accepted:
            continue
        accuracy = sum(1 for query, resolution in accepted if is_correct(query, resolution)) / len(accepted)
        coverage = len(accepted) / len(paired) if paired else 0.0
        if accuracy >= target_accuracy and coverage > best_coverage:
            best_threshold = threshold
            best_coverage = coverage
    return best_threshold


def save_model(path: Path, model: Stage2Model, accept_threshold: float, review_threshold: float, metadata: Dict[str, object]) -> None:
    metadata = dict(metadata)
    if model.training_metadata:
        metadata["stage2_training"] = model.training_metadata
    payload = {
        "model": model.to_dict(),
        "accept_threshold": accept_threshold,
        "review_threshold": review_threshold,
        "metadata": metadata,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_model(path: Path, resolver: Resolver) -> Tuple[Stage2Model, float, float, Dict[str, object]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    model = Stage2Model.from_dict(resolver, payload["model"])
    accept_threshold = float(payload["accept_threshold"])
    review_threshold = float(payload["review_threshold"])
    metadata = payload.get("metadata", {})
    return model, accept_threshold, review_threshold, metadata


_WORKER_RESOLVER: Optional[Resolver] = None
_WORKER_MODEL: Optional[Stage2Model] = None
_WORKER_ACCEPT_THRESHOLD: float = 0.0
