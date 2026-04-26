#!/usr/bin/env python3
"""Resolve messy addresses against a canonical real-address reference set.

The pipeline is intentionally hybrid:
1. Stage 1 uses deterministic normalization and exact/near-exact lookups.
2. Stage 2 uses blocked candidate generation plus a nearest-neighbor style score.

Outputs:
- predictions.csv with standardized addresses, predicted matches, confidence, and review flags
- evaluation.json with metrics for stage1, stage2, and the combined pipeline
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import re
import time
from collections import Counter, OrderedDict, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path
from typing import Dict, Hashable, Iterable, List, Optional, Sequence, Tuple

from address_dataset_generator import (
    DIRECTION_TO_FULL,
    STATE_ABBREV_TO_NAME,
    STREET_TYPE_ALIASES,
    STREET_TYPE_CONFUSIONS,
    STREET_TYPE_TO_FULL,
    UNIT_TYPE_TO_FULL,
)


FULL_TO_DIRECTION = {value: key for key, value in DIRECTION_TO_FULL.items()}
FULL_TO_STREET_TYPE = {value: key for key, value in STREET_TYPE_TO_FULL.items()}
FULL_TO_UNIT_TYPE = {value: key for key, value in UNIT_TYPE_TO_FULL.items()}
STATE_NAME_TO_ABBREV = {name: abbr for abbr, name in STATE_ABBREV_TO_NAME.items()}
STATE_TYPO_ALIASES = {
    "MISSIPPI": "MS",
    "MISSPPI": "MS",
    "MISSPI": "MS",
    "MISSISSPI": "MS",
    "MISSISSIPI": "MS",
    "MISSISIPPI": "MS",
    "MISISIPPI": "MS",
    "MISISSIPPI": "MS",
    "MISSISSPPI": "MS",
}
STREET_TYPE_TYPO_ALIASES = {
    "SR": "ST",
    "SY": "ST",
    "DT": "ST",
    "XR": "DR",
    "FR": "DR",
    "DN": "DR",
    "RN": "RD",
    "RF": "RD",
    "LF": "LN",
    "KN": "LN",
    "AB": "AVE",
    "AC": "AVE",
    "SV": "AVE",
}
CONTEXTUAL_STREET_TYPE_TYPO_ALIASES = {
    "SE": "ST",
}
PUNCT_RE = re.compile(r"[.,]")
SPACE_RE = re.compile(r"\s+")
TOKEN_RE = re.compile(r"[A-Z0-9#]+")


VALID_STATES = set(STATE_ABBREV_TO_NAME)


@dataclass(frozen=True, slots=True)
class ReferenceAddress:
    address_id: str
    canonical_address: str
    house_number: str
    predir: str
    street_name: str
    street_type: str
    suffixdir: str
    unit_type: str
    unit_value: str
    city: str
    state: str
    zip_code: str
    standardized_address: str
    street_signature: str
    source_quality: float = 0.5


@dataclass(frozen=True, slots=True)
class QueryAddress:
    query_id: str
    split: str
    label: int
    true_match_id: str
    query_address: str
    canonical_address: str


@dataclass(frozen=True, slots=True)
class ParsedAddress:
    raw: str
    standardized_address: str
    house_number: str
    predir: str
    street_name: str
    street_type: str
    suffixdir: str
    unit_type: str
    unit_value: str
    city: str
    state: str
    zip_code: str

    @property
    def street_signature(self) -> str:
        return " ".join(part for part in [self.predir, self.street_name, self.street_type, self.suffixdir] if part)

    @property
    def street_core_signature(self) -> str:
        return " ".join(part for part in [self.predir, self.street_name, self.suffixdir] if part)


@dataclass(frozen=True, slots=True)
class CandidateScore:
    reference_id: str
    score: float


@dataclass(frozen=True, slots=True)
class CandidateFeatures:
    reference_id: str
    variant_reason: str
    variant: ParsedAddress
    values: Tuple[float, ...]


@dataclass(frozen=True, slots=True)
class Resolution:
    predicted_match_id: str
    predicted_canonical_address: str
    standardized_query_address: str
    confidence: float
    needs_review: bool
    stage: str
    top_candidates: Tuple[CandidateScore, ...]


FeatureVector = Tuple[float, ...]
PairTrainingRow = Tuple[FeatureVector, List[FeatureVector]]
CalibrationTrainingRow = Tuple[FeatureVector, int, float]


@dataclass(slots=True)
class Stage2TrainingRows:
    pair_rows: List[PairTrainingRow]
    calibration_rows: List[CalibrationTrainingRow]
    feature_length: int
    stats: Dict[str, int]


STAGE2_FEATURE_NAMES = (
    "bias",
    "full_similarity",
    "street_name_similarity",
    "street_signature_overlap",
    "city_similarity",
    "house_similarity",
    "zip_exact",
    "zip_prefix",
    "type_exact",
    "type_missing",
    "predir_exact",
    "suffixdir_exact",
    "unit_exact",
    "unit_missing",
    "state_exact",
    "city_state_exact",
    "house_exact",
    "street_exact",
    "locality_corrected",
    "missing_city",
    "missing_state",
    "missing_zip",
    "rough",
    "street_phonetic_similarity",
    "city_phonetic_similarity",
    "zip_city_consistency",
    "house_mismatch_strong_context",
    "source_quality",
)


def normalize_text(text: str) -> str:
    text = text.upper().replace("/", " ").replace("-", " ")
    text = PUNCT_RE.sub(" ", text)
    text = text.replace("'", "")
    return SPACE_RE.sub(" ", text).strip()


@lru_cache(maxsize=200_000)
def token_set(text: str) -> frozenset[str]:
    return frozenset(TOKEN_RE.findall(text))


@lru_cache(maxsize=200_000)
def sequence_similarity(left: str, right: str) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def cheap_similarity(left: str, right: str) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0

    left_tokens = token_set(left)
    right_tokens = token_set(right)
    overlap = len(left_tokens & right_tokens) / len(left_tokens | right_tokens) if left_tokens and right_tokens else 0.0

    # SequenceMatcher is the expensive fallback. Use it only when token overlap
    # leaves the pair ambiguous or when the strings are short enough that token
    # overlap loses too much signal.
    if overlap >= 0.92:
        return overlap
    if len(left) <= 8 or len(right) <= 8 or overlap >= 0.45:
        return max(overlap, sequence_similarity(left, right))
    return overlap


def closest_choice(value: str, choices: Sequence[str], minimum_score: float = 0.0) -> Tuple[str, float, float]:
    if not value or not choices:
        return "", 0.0, 0.0
    scored = sorted(
        ((choice, sequence_similarity(value, choice)) for choice in choices),
        key=lambda item: item[1],
        reverse=True,
    )
    best_choice, best_score = scored[0]
    second_score = scored[1][1] if len(scored) > 1 else 0.0
    if best_score < minimum_score:
        return "", best_score, second_score
    return best_choice, best_score, second_score


def token_overlap(left: str, right: str) -> float:
    left_tokens = token_set(left)
    right_tokens = token_set(right)
    if not left_tokens and not right_tokens:
        return 1.0
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def soundex_token(token: str) -> str:
    token = re.sub(r"[^A-Z]+", "", normalize_text(token))
    if not token:
        return ""
    groups = {
        **dict.fromkeys("BFPV", "1"),
        **dict.fromkeys("CGJKQSXZ", "2"),
        **dict.fromkeys("DT", "3"),
        "L": "4",
        **dict.fromkeys("MN", "5"),
        "R": "6",
    }
    first = token[0]
    encoded = []
    previous = groups.get(first, "")
    for char in token[1:]:
        code = groups.get(char, "")
        if code and code != previous:
            encoded.append(code)
        previous = code
    return (first + "".join(encoded) + "000")[:4]


@lru_cache(maxsize=100_000)
def phonetic_signature(text: str) -> str:
    return " ".join(code for code in (soundex_token(token) for token in TOKEN_RE.findall(text)) if code)


def phonetic_similarity(left: str, right: str) -> float:
    left_signature = phonetic_signature(left)
    right_signature = phonetic_signature(right)
    if not left_signature and not right_signature:
        return 1.0
    if not left_signature or not right_signature:
        return 0.0
    return sequence_similarity(left_signature, right_signature)


def numeric_similarity(left: str, right: str) -> float:
    if not left and not right:
        return 1.0
    if not left or not right:
        return 0.0
    if left == right:
        return 1.0
    if left.isdigit() and right.isdigit():
        gap = abs(int(left) - int(right))
        return max(0.0, 1.0 - min(gap, 25) / 25.0)
    return cheap_similarity(left, right)


def standardize_parts(
    house_number: str,
    predir: str,
    street_name: str,
    street_type: str,
    suffixdir: str,
    unit_type: str,
    unit_value: str,
    city: str,
    state: str,
    zip_code: str,
) -> str:
    street_bits = [house_number, predir, street_name, street_type, suffixdir]
    street_part = " ".join(bit for bit in street_bits if bit)
    unit_part = " ".join(bit for bit in [unit_type, unit_value] if bit)
    left = street_part if not unit_part else f"{street_part} {unit_part}"
    locality = " ".join(bit for bit in [city, state, zip_code] if bit)
    if locality:
        return f"{left}, {locality}".strip(", ")
    return left


def rebuild_parsed(parsed: ParsedAddress, **updates: str) -> ParsedAddress:
    values = {
        "house_number": parsed.house_number,
        "predir": parsed.predir,
        "street_name": parsed.street_name,
        "street_type": parsed.street_type,
        "suffixdir": parsed.suffixdir,
        "unit_type": parsed.unit_type,
        "unit_value": parsed.unit_value,
        "city": parsed.city,
        "state": parsed.state,
        "zip_code": parsed.zip_code,
    }
    values.update(updates)
    values["standardized_address"] = standardize_parts(
        values["house_number"],
        values["predir"],
        values["street_name"],
        values["street_type"],
        values["suffixdir"],
        values["unit_type"],
        values["unit_value"],
        values["city"],
        values["state"],
        values["zip_code"],
    )
    return replace(parsed, **values)


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


def load_reference(path: Path) -> Tuple[List[ReferenceAddress], Dict[str, ReferenceAddress]]:
    rows: List[ReferenceAddress] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                source_quality = float(row.get("source_quality", "") or 0.5)
            except ValueError:
                source_quality = 0.5
            standardized = standardize_parts(
                row["house_number"].upper(),
                row["predir"].upper(),
                normalize_text(row["street_name"]),
                row["street_type"].upper(),
                row["suffixdir"].upper(),
                row["unit_type"].upper(),
                row["unit_value"].upper(),
                normalize_text(row["city"]),
                row["state"].upper(),
                row["zip_code"],
            )
            record = ReferenceAddress(
                address_id=row["address_id"],
                canonical_address=row["canonical_address"],
                house_number=row["house_number"].upper(),
                predir=row["predir"].upper(),
                street_name=normalize_text(row["street_name"]),
                street_type=row["street_type"].upper(),
                suffixdir=row["suffixdir"].upper(),
                unit_type=row["unit_type"].upper(),
                unit_value=row["unit_value"].upper(),
                city=normalize_text(row["city"]),
                state=row["state"].upper(),
                zip_code=row["zip_code"],
                standardized_address=standardized,
                street_signature=" ".join(
                    bit for bit in [row["predir"].upper(), normalize_text(row["street_name"]), row["street_type"].upper(), row["suffixdir"].upper()] if bit
                ),
                source_quality=max(0.0, min(1.0, source_quality)),
            )
            rows.append(record)
    return rows, {row.address_id: row for row in rows}


def reference_dedupe_key(row: ReferenceAddress) -> Tuple[str, ...]:
    return (
        row.house_number,
        row.predir,
        row.street_name,
        row.street_type,
        row.suffixdir,
        row.unit_type,
        row.unit_value,
        row.city,
        row.state,
        row.zip_code,
    )


def augment_reference_rows(
    primary_rows: Sequence[ReferenceAddress],
    extra_rows: Sequence[ReferenceAddress],
    id_prefix: str = "AUG",
) -> Tuple[List[ReferenceAddress], Dict[str, int]]:
    combined: List[ReferenceAddress] = []
    seen_keys = set()
    seen_ids = set()
    duplicate_address_count = 0
    renamed_reference_count = 0

    for row in primary_rows:
        combined.append(row)
        seen_keys.add(reference_dedupe_key(row))
        seen_ids.add(row.address_id)

    for row in extra_rows:
        key = reference_dedupe_key(row)
        if key in seen_keys:
            duplicate_address_count += 1
            continue

        address_id = row.address_id
        if not address_id or address_id in seen_ids:
            renamed_reference_count += 1
            address_id = f"{id_prefix}_{renamed_reference_count:07d}"
            while address_id in seen_ids:
                renamed_reference_count += 1
                address_id = f"{id_prefix}_{renamed_reference_count:07d}"

        combined.append(replace(row, address_id=address_id))
        seen_keys.add(key)
        seen_ids.add(address_id)

    return combined, {
        "base_reference_count": len(primary_rows),
        "extra_reference_count": len(extra_rows),
        "added_reference_count": len(combined) - len(primary_rows),
        "duplicate_address_count": duplicate_address_count,
        "renamed_reference_count": renamed_reference_count,
        "combined_reference_count": len(combined),
    }


def load_queries(path: Path) -> List[QueryAddress]:
    rows: List[QueryAddress] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rows.append(
                QueryAddress(
                    query_id=row["query_id"],
                    split=row["split"],
                    label=int(row["label"]),
                    true_match_id=row["true_match_id"],
                    query_address=row["query_address"],
                    canonical_address=row["canonical_address"],
                )
            )
    return rows


def reference_canonical_index(reference_rows: Sequence[ReferenceAddress]) -> Dict[str, ReferenceAddress]:
    return {normalize_text(row.canonical_address): row for row in reference_rows}


def load_active_learning_feedback_queries(
    path: Path,
    reference_rows: Sequence[ReferenceAddress],
) -> Tuple[List[QueryAddress], Dict[str, int]]:
    stats = {
        "rows_seen": 0,
        "queries_added": 0,
        "positive_queries_added": 0,
        "negative_queries_added": 0,
        "rows_skipped": 0,
        "missing_reference_rows": 0,
    }
    if not path.exists():
        return [], stats

    by_id = {row.address_id: row for row in reference_rows}
    by_canonical = reference_canonical_index(reference_rows)
    queries: List[QueryAddress] = []

    def lookup_reference(address_id: str, canonical: str) -> Optional[ReferenceAddress]:
        if address_id and address_id in by_id:
            return by_id[address_id]
        return by_canonical.get(normalize_text(canonical)) if canonical else None

    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows_seen"] += 1
            feedback_type = row.get("feedback_type", "").strip()
            query_address = row.get("input_address", "").strip()
            if not query_address:
                stats["rows_skipped"] += 1
                continue

            label = 0
            true_match_id = ""
            canonical = "NO_MATCH"
            if feedback_type == "correction":
                reference = lookup_reference(
                    row.get("correct_reference_id", "").strip(),
                    row.get("correct_canonical_address", "").strip() or row.get("correct_address", "").strip(),
                )
                if reference is None:
                    stats["missing_reference_rows"] += 1
                    continue
                label = 1
                true_match_id = reference.address_id
                canonical = reference.canonical_address
            elif feedback_type == "correct":
                reference = lookup_reference(
                    row.get("predicted_match_id", "").strip(),
                    row.get("predicted_canonical_address", "").strip(),
                )
                if reference is not None:
                    label = 1
                    true_match_id = reference.address_id
                    canonical = reference.canonical_address
            elif feedback_type == "wrong":
                pass
            else:
                stats["rows_skipped"] += 1
                continue

            queries.append(
                QueryAddress(
                    query_id=f"AL_{len(queries) + 1:07d}",
                    split="train",
                    label=label,
                    true_match_id=true_match_id,
                    query_address=query_address,
                    canonical_address=canonical,
                )
            )
            stats["queries_added"] += 1
            if label:
                stats["positive_queries_added"] += 1
            else:
                stats["negative_queries_added"] += 1

    return queries, stats


def build_city_lookup(reference_rows: Sequence[ReferenceAddress]) -> Dict[Tuple[str, ...], str]:
    lookup: Dict[Tuple[str, ...], str] = {}
    for row in reference_rows:
        tokens = tuple(normalize_text(row.city).split())
        lookup[tokens] = row.city
    return lookup


def canonical_abbrev(token: str, short_to_full: Dict[str, str], full_to_short: Dict[str, str]) -> str:
    if token in short_to_full:
        return token
    return full_to_short.get(token, token)


def canonical_street_type_token(token: str, allow_typos: bool = True) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "", normalize_text(token))
    if not normalized:
        return ""
    canonical = STREET_TYPE_ALIASES.get(normalized)
    if canonical:
        return canonical
    if allow_typos:
        return STREET_TYPE_TYPO_ALIASES.get(normalized, "")
    return ""


def looks_like_street_descriptor(token: str) -> bool:
    return bool(
        canonical_abbrev(token, DIRECTION_TO_FULL, FULL_TO_DIRECTION) in DIRECTION_TO_FULL
        or canonical_street_type_token(token, allow_typos=False)
        or contextual_street_type_token(token)
    )


def extract_zip_code(tokens: List[str]) -> str:
    for idx in range(len(tokens) - 1, -1, -1):
        token = tokens[idx]
        if re.fullmatch(r"\d{5}", token):
            del tokens[idx]
            return token
    if tokens and re.fullmatch(r"\d{4}", tokens[-1]):
        return tokens.pop()
    return ""


def house_number_score(tokens: Sequence[str], idx: int) -> Tuple[int, int, int]:
    token = tokens[idx]
    digit_count = sum(1 for char in token if char.isdigit())
    street_context = 0
    if idx > 0 and not looks_like_street_descriptor(tokens[idx - 1]):
        street_context += 1
    if idx + 1 < len(tokens) and looks_like_street_descriptor(tokens[idx + 1]):
        street_context += 1
    return digit_count, street_context, -idx


def looks_like_house_number_token(token: str) -> bool:
    if token.startswith("#") or not any(ch.isdigit() for ch in token):
        return False
    digit_count = sum(1 for char in token if char.isdigit())
    alpha_count = sum(1 for char in token if char.isalpha())
    return token[0].isdigit() or digit_count >= alpha_count


def extract_house_number(tokens: List[str]) -> str:
    if tokens and looks_like_house_number_token(tokens[0]):
        return tokens.pop(0)

    candidates: List[Tuple[Tuple[int, int, int], int]] = []
    for idx, token in enumerate(tokens):
        if not looks_like_house_number_token(token):
            continue
        previous = tokens[idx - 1]
        previous_is_unit = canonical_abbrev(previous.lstrip("#"), UNIT_TYPE_TO_FULL, FULL_TO_UNIT_TYPE) in UNIT_TYPE_TO_FULL
        if previous_is_unit:
            continue
        candidates.append((house_number_score(tokens, idx), idx))
    if candidates:
        _, idx = max(candidates)
        return tokens.pop(idx)
    return ""


def contextual_street_type_token(token: str) -> str:
    normalized = re.sub(r"[^A-Z0-9]+", "", normalize_text(token))
    if not normalized:
        return ""
    return canonical_street_type_token(normalized) or CONTEXTUAL_STREET_TYPE_TYPO_ALIASES.get(normalized, "")


def state_from_candidate(candidate_tokens: Sequence[str]) -> str:
    candidate = " ".join(candidate_tokens)
    if candidate in VALID_STATES:
        return candidate
    if candidate in STATE_NAME_TO_ABBREV:
        return STATE_NAME_TO_ABBREV[candidate]
    if candidate in STATE_TYPO_ALIASES:
        return STATE_TYPO_ALIASES[candidate]
    return ""


def extract_state(tokens: List[str], city_lookup: Optional[Dict[Tuple[str, ...], str]] = None) -> str:
    if not tokens:
        return ""
    city_lookup = city_lookup or {}

    max_width = min(3, len(tokens))
    for width in range(max_width, 0, -1):
        for start in range(len(tokens) - width, -1, -1):
            state = state_from_candidate(tokens[start:start + width])
            if state:
                del tokens[start:start + width]
                return state

    fuzzy_widths = (3, 2, 1)
    for width in fuzzy_widths:
        if len(tokens) < width:
            continue
        for start in range(len(tokens) - width, -1, -1):
            candidate_tokens = tuple(tokens[start:start + width])
            if candidate_tokens in city_lookup:
                continue
            if any(canonical_street_type_token(token) in STREET_TYPE_TO_FULL for token in candidate_tokens):
                continue
            candidate = " ".join(candidate_tokens)
            minimum_score = 0.88 if width == 1 else 0.84
            match, best_score, second_score = closest_choice(candidate, tuple(STATE_NAME_TO_ABBREV), minimum_score=minimum_score)
            if match and best_score - second_score >= 0.08:
                del tokens[start:start + width]
                return STATE_NAME_TO_ABBREV[match]
    return ""


def city_candidate_looks_like_street(tokens: Sequence[str], start: int, width: int) -> bool:
    after = start + width
    if after < len(tokens) and canonical_street_type_token(tokens[after], allow_typos=False):
        return True
    return False


def extract_city(tokens: List[str], city_lookup: Dict[Tuple[str, ...], str], state: str, zip_code: str) -> str:
    if not tokens:
        return ""
    for width in range(min(3, len(tokens)), 0, -1):
        candidate = tuple(tokens[-width:])
        remaining_required = 1 if state or zip_code else 2
        if candidate in city_lookup and len(tokens) - width >= remaining_required:
            city = city_lookup[candidate]
            del tokens[-width:]
            return city

    for width in range(min(3, len(tokens)), 0, -1):
        for start in range(len(tokens) - width, -1, -1):
            candidate = tuple(tokens[start:start + width])
            if candidate not in city_lookup:
                continue
            if any(any(ch.isdigit() for ch in token) for token in candidate):
                continue
            if len(tokens) - width < 1:
                continue
            if city_candidate_looks_like_street(tokens, start, width):
                continue
            city = city_lookup[candidate]
            del tokens[start:start + width]
            return city
    return ""


def parse_street_tokens(tokens: List[str], allow_contextual_type: bool = False) -> Tuple[str, str, str, str]:
    tokens = list(tokens)
    predir = ""
    suffixdir = ""
    street_type = ""

    def type_from_token(token: str) -> str:
        return contextual_street_type_token(token) if allow_contextual_type else canonical_street_type_token(token)

    if tokens:
        maybe_street_type = type_from_token(tokens[-1])
        if maybe_street_type:
            street_type = maybe_street_type
            tokens.pop()
    if tokens and not street_type:
        maybe_street_type = type_from_token(tokens[0])
        if maybe_street_type and len(tokens) > 1:
            street_type = maybe_street_type
            tokens.pop(0)
    if tokens and not street_type:
        for idx in range(len(tokens) - 1, -1, -1):
            maybe_street_type = type_from_token(tokens[idx])
            if maybe_street_type and len(tokens) > 1:
                street_type = maybe_street_type
                del tokens[idx]
                break

    if tokens and canonical_abbrev(tokens[0], DIRECTION_TO_FULL, FULL_TO_DIRECTION) in DIRECTION_TO_FULL and len(tokens) > 1:
        predir = canonical_abbrev(tokens.pop(0), DIRECTION_TO_FULL, FULL_TO_DIRECTION)

    if tokens and canonical_abbrev(tokens[-1], DIRECTION_TO_FULL, FULL_TO_DIRECTION) in DIRECTION_TO_FULL and len(tokens) > 1:
        suffixdir = canonical_abbrev(tokens.pop(), DIRECTION_TO_FULL, FULL_TO_DIRECTION)

    if not tokens and predir and street_type:
        street_name = DIRECTION_TO_FULL.get(predir, predir)
        predir = ""
    else:
        street_name = " ".join(tokens)
    return predir, street_name, street_type, suffixdir


def parse_query_address(raw: str, city_lookup: Dict[Tuple[str, ...], str]) -> ParsedAddress:
    normalized = normalize_text(raw)
    tokens = normalized.split()

    zip_code = extract_zip_code(tokens)

    state = extract_state(tokens, city_lookup)

    city = extract_city(tokens, city_lookup, state, zip_code)

    unit_type = ""
    unit_value = ""
    if len(tokens) >= 2 and canonical_abbrev(tokens[0].lstrip("#"), UNIT_TYPE_TO_FULL, FULL_TO_UNIT_TYPE) in UNIT_TYPE_TO_FULL:
        unit_type = canonical_abbrev(tokens.pop(0).lstrip("#"), UNIT_TYPE_TO_FULL, FULL_TO_UNIT_TYPE)
        unit_value = tokens.pop(0)

    house_number = extract_house_number(tokens)

    if len(tokens) >= 2:
        unit_pos = None
        for idx in range(len(tokens) - 1):
            alias = canonical_abbrev(tokens[idx].lstrip("#"), UNIT_TYPE_TO_FULL, FULL_TO_UNIT_TYPE)
            if alias in UNIT_TYPE_TO_FULL:
                unit_pos = idx
        if unit_pos is not None:
            unit_type = canonical_abbrev(tokens[unit_pos].lstrip("#"), UNIT_TYPE_TO_FULL, FULL_TO_UNIT_TYPE)
            unit_value = tokens[unit_pos + 1]
            del tokens[unit_pos:unit_pos + 2]

    if not unit_value:
        for idx, token in enumerate(tokens):
            if token.startswith("#") and len(token) > 1:
                unit_type = "APT"
                unit_value = token[1:]
                del tokens[idx]
                break

    predir, street_name, street_type, suffixdir = parse_street_tokens(tokens)

    standardized = standardize_parts(
        house_number,
        predir,
        street_name,
        street_type,
        suffixdir,
        unit_type,
        unit_value,
        city,
        state,
        zip_code,
    )
    return ParsedAddress(
        raw=raw,
        standardized_address=standardized,
        house_number=house_number,
        predir=predir,
        street_name=street_name,
        street_type=street_type,
        suffixdir=suffixdir,
        unit_type=unit_type,
        unit_value=unit_value,
        city=city,
        state=state,
        zip_code=zip_code,
    )


class Resolver:
    def __init__(self, reference_rows: Sequence[ReferenceAddress], city_lookup: Dict[Tuple[str, ...], str]) -> None:
        self.reference_rows = list(reference_rows)
        self.reference_by_id = {row.address_id: row for row in reference_rows}
        self.city_lookup = city_lookup
        self.by_exact = defaultdict(list)
        self.by_house_zip_street = defaultdict(list)
        self.by_house_zip_street_name = defaultdict(list)
        self.by_house_zip_core = defaultdict(list)
        self.by_house_zip = defaultdict(list)
        self.by_house_zip_prefix_street = defaultdict(list)
        self.by_house_zip_prefix_street_name = defaultdict(list)
        self.by_house_zip_prefix = defaultdict(list)
        self.by_house_city_street = defaultdict(list)
        self.by_house_city_street_name = defaultdict(list)
        self.by_house_city_core = defaultdict(list)
        self.by_house_city_state = defaultdict(list)
        self.by_city_street = defaultdict(list)
        self.by_city_street_name = defaultdict(list)
        self.by_zip = defaultdict(list)
        self.by_city_state = defaultdict(list)
        self.by_state = defaultdict(list)
        self.by_house = defaultdict(list)
        self.by_zip_prefix = defaultdict(list)
        self.zip_to_localities = defaultdict(set)
        self.cities_by_state = defaultdict(set)
        self.states_by_city = defaultdict(set)
        self.city_counts_by_state = defaultdict(Counter)
        self.street_token_index = defaultdict(set)
        self.city_token_index = defaultdict(set)
        self.default_candidate_limit = 28
        self.blocking_prefilter_limit = 90
        self._parse_cache: Dict[str, ParsedAddress] = {}
        self._stage1_cache: Dict[Tuple[str, float], Resolution] = {}

        for row in self.reference_rows:
            self._index_reference(row)

    def _index_reference(self, row: ReferenceAddress) -> None:
        self.by_exact[row.standardized_address].append(row.address_id)
        self.by_house_zip_street[(row.house_number, row.zip_code, row.street_signature)].append(row.address_id)
        self.by_house_zip_street_name[(row.house_number, row.zip_code, row.street_name)].append(row.address_id)
        self.by_house_zip_core[(row.house_number, row.zip_code, " ".join(bit for bit in [row.predir, row.street_name, row.suffixdir] if bit))].append(row.address_id)
        self.by_house_zip[(row.house_number, row.zip_code)].append(row.address_id)
        self.by_house_zip_prefix_street[(row.house_number, row.zip_code[:3], row.street_signature)].append(row.address_id)
        self.by_house_zip_prefix_street_name[(row.house_number, row.zip_code[:3], row.street_name)].append(row.address_id)
        self.by_house_zip_prefix[(row.house_number, row.zip_code[:3])].append(row.address_id)
        self.by_house_city_street[(row.house_number, row.city, row.state, row.street_signature)].append(row.address_id)
        self.by_house_city_street_name[(row.house_number, row.city, row.state, row.street_name)].append(row.address_id)
        self.by_house_city_core[(row.house_number, row.city, row.state, " ".join(bit for bit in [row.predir, row.street_name, row.suffixdir] if bit))].append(row.address_id)
        self.by_house_city_state[(row.house_number, row.city, row.state)].append(row.address_id)
        self.by_city_street[(row.city, row.state, row.street_signature)].append(row.address_id)
        self.by_city_street_name[(row.city, row.state, row.street_name)].append(row.address_id)
        self.by_zip[row.zip_code].append(row.address_id)
        self.by_city_state[(row.city, row.state)].append(row.address_id)
        self.by_state[row.state].append(row.address_id)
        self.by_house[row.house_number].append(row.address_id)
        self.by_zip_prefix[row.zip_code[:3]].append(row.address_id)
        self.zip_to_localities[row.zip_code].add((row.city, row.state))
        self.cities_by_state[row.state].add(row.city)
        self.states_by_city[row.city].add(row.state)
        self.city_counts_by_state[row.state][row.city] += 1
        for token in TOKEN_RE.findall(row.street_name):
            if len(token) >= 3:
                self.street_token_index[token].add(row.address_id)
        for token in TOKEN_RE.findall(row.city):
            if len(token) >= 3:
                self.city_token_index[token].add(row.address_id)

    def add_reference(self, row: ReferenceAddress) -> None:
        if row.address_id in self.reference_by_id:
            raise ValueError(f"Reference address already exists: {row.address_id}")
        self.reference_rows.append(row)
        self.reference_by_id[row.address_id] = row
        self._index_reference(row)
        self.city_lookup[tuple(row.city.split())] = row.city
        self._parse_cache.clear()
        self._stage1_cache.clear()

    def parse(self, raw: str) -> ParsedAddress:
        cached = self._parse_cache.get(raw)
        if cached is None:
            cached = parse_query_address(raw, self.city_lookup)
            cached = self.apply_fuzzy_city_anywhere(cached)
            cached = self.apply_fuzzy_city_suffix(cached)
            self._parse_cache[raw] = cached
        return cached

    def locality_city_choices(self, parsed: ParsedAddress) -> Tuple[str, ...]:
        choices = set()
        if parsed.state:
            choices.update(city for city in self.cities_by_state.get(parsed.state, ()) if city)
        if parsed.zip_code:
            for city, state in self.zip_to_localities.get(parsed.zip_code, ()):
                if city and (not parsed.state or state == parsed.state):
                    choices.add(city)
        return tuple(sorted(choices))

    def fuzzy_city_choices(self, parsed: ParsedAddress) -> Tuple[Tuple[str, ...], bool]:
        city_choices = self.locality_city_choices(parsed)
        if city_choices:
            return city_choices, False
        if parsed.state or parsed.zip_code or not parsed.house_number:
            return (), False
        return (
            tuple(
                sorted(
                    {
                        city
                        for state_counts in self.city_counts_by_state.values()
                        for city, count in state_counts.items()
                        if city and count >= 2
                    }
                )
            ),
            True,
        )

    def street_tokens_for_reparse(self, parsed: ParsedAddress) -> List[str]:
        tokens: List[str] = []
        if parsed.predir:
            tokens.append(parsed.predir)
        if parsed.street_name:
            tokens.extend(parsed.street_name.split())
        if parsed.street_type:
            tokens.append(parsed.street_type)
        if parsed.suffixdir:
            tokens.append(parsed.suffixdir)
        return tokens

    def apply_fuzzy_city_anywhere(self, parsed: ParsedAddress) -> ParsedAddress:
        if parsed.city or not parsed.street_name:
            return parsed

        street_tokens = self.street_tokens_for_reparse(parsed)
        if len(street_tokens) < 2:
            return parsed

        city_choices, allow_global_city_match = self.fuzzy_city_choices(parsed)
        if not city_choices:
            return parsed

        best: Tuple[float, int, int, str] = (0.0, 0, 0, "")
        best_start = -1
        best_width = 0
        max_width = min(3, len(street_tokens) - 1)
        for width in range(max_width, 0, -1):
            for start in range(0, len(street_tokens) - width + 1):
                candidate_tokens = street_tokens[start:start + width]
                if all(looks_like_street_descriptor(token) for token in candidate_tokens):
                    continue
                candidate = " ".join(candidate_tokens)
                if allow_global_city_match:
                    minimum_score = 0.86 if width == 1 else 0.82
                elif parsed.house_number and parsed.state:
                    minimum_score = 0.62 if width == 1 else 0.60
                else:
                    minimum_score = 0.80 if width == 1 else 0.76
                city_match, best_score, second_score = closest_choice(candidate, city_choices, minimum_score=minimum_score)
                required_margin = 0.08 if allow_global_city_match else 0.05
                if not city_match or (best_score - second_score < required_margin and best_score < 0.90):
                    continue
                position_score = 1 if start in {0, len(street_tokens) - width} else 0
                candidate_score = (best_score, position_score, width, city_match)
                if candidate_score > best:
                    best = candidate_score
                    best_start = start
                    best_width = width

        if not best[3]:
            return parsed

        remaining_tokens = street_tokens[:best_start] + street_tokens[best_start + best_width:]
        predir, street_name, street_type, suffixdir = parse_street_tokens(remaining_tokens, allow_contextual_type=True)
        if not street_name:
            return parsed

        state = parsed.state
        if allow_global_city_match and not state:
            states = {candidate_state for candidate_state in self.states_by_city.get(best[3], ()) if candidate_state}
            if len(states) == 1:
                state = next(iter(states))

        return rebuild_parsed(
            parsed,
            predir=predir,
            street_name=street_name,
            street_type=street_type,
            suffixdir=suffixdir,
            city=best[3],
            state=state,
        )

    def apply_fuzzy_city_suffix(self, parsed: ParsedAddress) -> ParsedAddress:
        if parsed.city or not parsed.street_name:
            return parsed

        street_tokens = parsed.street_name.split()
        if len(street_tokens) < 2:
            return parsed

        city_choices, allow_global_city_match = self.fuzzy_city_choices(parsed)
        if not city_choices:
            return parsed

        max_width = min(3, len(street_tokens) - 1)
        for width in range(max_width, 0, -1):
            candidate_tokens = street_tokens[-width:]
            if width > 1:
                leading_tokens = candidate_tokens[:-1]
                if any(
                    token in STREET_TYPE_TYPO_ALIASES
                    or token in CONTEXTUAL_STREET_TYPE_TYPO_ALIASES
                    or (canonical_street_type_token(token, allow_typos=False) and token != "ST")
                    for token in leading_tokens
                ):
                    continue
            candidate = " ".join(candidate_tokens)
            street_context = bool(parsed.house_number and parsed.state and len(street_tokens) - width >= 1)
            if allow_global_city_match:
                minimum_score = 0.86 if width == 1 else 0.82
            elif street_context:
                minimum_score = 0.62 if width == 1 else 0.60
            else:
                minimum_score = 0.80 if width == 1 else 0.76
            city_match, best_score, second_score = closest_choice(candidate, city_choices, minimum_score=minimum_score)
            required_margin = 0.08 if allow_global_city_match else 0.05 if street_context else 0.04
            if not city_match or (best_score - second_score < required_margin and best_score < 0.90):
                continue
            if street_context and best_score < 0.74:
                city_count = self.city_counts_by_state[parsed.state][city_match]
                if len(city_choices) > 20 and city_count < 20:
                    continue

            remaining_tokens = street_tokens[:-width]
            predir = parsed.predir
            suffixdir = parsed.suffixdir
            street_type = parsed.street_type

            if remaining_tokens and not predir:
                maybe_predir = canonical_abbrev(remaining_tokens[0], DIRECTION_TO_FULL, FULL_TO_DIRECTION)
                if maybe_predir in DIRECTION_TO_FULL:
                    predir = maybe_predir
                    remaining_tokens = remaining_tokens[1:]

            if remaining_tokens and not street_type:
                maybe_street_type = contextual_street_type_token(remaining_tokens[-1])
                if maybe_street_type:
                    street_type = maybe_street_type
                    remaining_tokens = remaining_tokens[:-1]

            if remaining_tokens and not suffixdir:
                maybe_suffixdir = canonical_abbrev(remaining_tokens[-1], DIRECTION_TO_FULL, FULL_TO_DIRECTION)
                if maybe_suffixdir in DIRECTION_TO_FULL:
                    suffixdir = maybe_suffixdir
                    remaining_tokens = remaining_tokens[:-1]

            if remaining_tokens and not street_type:
                maybe_street_type = canonical_street_type_token(remaining_tokens[-1])
                if maybe_street_type:
                    street_type = maybe_street_type
                    remaining_tokens = remaining_tokens[:-1]

            street_name = " ".join(remaining_tokens).strip()
            if not street_name:
                continue
            state = parsed.state
            if allow_global_city_match and not state:
                states = {candidate_state for candidate_state in self.states_by_city.get(city_match, ()) if candidate_state}
                if len(states) == 1:
                    state = next(iter(states))

            return rebuild_parsed(
                parsed,
                predir=predir,
                street_name=street_name,
                street_type=street_type,
                suffixdir=suffixdir,
                city=city_match,
                state=state,
            )

        return parsed

    def locality_variants(self, parsed: ParsedAddress) -> List[Tuple[str, ParsedAddress]]:
        variants: List[Tuple[str, ParsedAddress]] = []

        if parsed.zip_code:
            localities = self.zip_to_localities.get(parsed.zip_code)
            if localities and len(localities) == 1:
                city, state = next(iter(localities))
                city_ok = not parsed.city or cheap_similarity(parsed.city, city) >= 0.62
                state_ok = not parsed.state or parsed.state == state
                if city_ok or state_ok:
                    corrected = rebuild_parsed(parsed, city=city, state=state)
                    if corrected != parsed:
                        variants.append(("zip_locality", corrected))

        if parsed.city and not parsed.state:
            states = {state for state in self.states_by_city.get(parsed.city, ()) if state}
            if len(states) == 1:
                state = next(iter(states))
                corrected = rebuild_parsed(parsed, state=state)
                if corrected != parsed:
                    variants.append(("city_state", corrected))

        if parsed.city and parsed.state in self.cities_by_state:
            city_choices = tuple(self.cities_by_state[parsed.state])
            city_match, best_score, second_score = closest_choice(
                parsed.city,
                city_choices,
                minimum_score=0.82,
            )
            if city_match and city_match != parsed.city and best_score - second_score >= 0.05:
                corrected = rebuild_parsed(parsed, city=city_match)
                variants.append(("state_city_fuzzy", corrected))
            alternate_choices = tuple(city for city in city_choices if city != parsed.city)
            if alternate_choices:
                alt_match, alt_score, alt_second_score = closest_choice(parsed.city, alternate_choices, minimum_score=0.86)
                source_count = self.city_counts_by_state[parsed.state][parsed.city]
                alt_count = self.city_counts_by_state[parsed.state][alt_match] if alt_match else 0
                count_ratio_ok = source_count <= 3 and alt_count >= max(20, source_count * 10)
                margin_ok = alt_score - alt_second_score >= 0.03
                if alt_match and alt_count > source_count and (margin_ok or count_ratio_ok):
                    corrected = rebuild_parsed(parsed, city=alt_match)
                    if corrected != parsed:
                        variants.append(("state_city_fuzzy_alt", corrected))

        unique_variants: List[Tuple[str, ParsedAddress]] = []
        seen = set()
        for reason, variant in variants:
            key = (
                variant.house_number,
                variant.predir,
                variant.street_name,
                variant.street_type,
                variant.suffixdir,
                variant.unit_type,
                variant.unit_value,
                variant.city,
                variant.state,
                variant.zip_code,
            )
            if key not in seen:
                seen.add(key)
                unique_variants.append((reason, variant))
        return unique_variants

    def unique_match(self, index: Dict[Hashable, List[str]], key: Hashable) -> str:
        matches = index.get(key, [])
        return matches[0] if len(matches) == 1 else ""

    def fuzzy_house_locality_match(self, parsed: ParsedAddress) -> str:
        if not parsed.house_number or not parsed.street_name:
            return ""
        strong_locality = bool(parsed.city or parsed.zip_code)

        candidate_ids = list(self.by_house.get(parsed.house_number, []))
        if parsed.city and parsed.state:
            localized = [cid for cid in candidate_ids if self.reference_by_id[cid].city == parsed.city and self.reference_by_id[cid].state == parsed.state]
            if localized:
                candidate_ids = localized
        elif parsed.city:
            localized = [cid for cid in candidate_ids if self.reference_by_id[cid].city == parsed.city]
            if localized:
                candidate_ids = localized
        elif parsed.zip_code:
            localized = [cid for cid in candidate_ids if self.reference_by_id[cid].zip_code[:3] == parsed.zip_code[:3]]
            if localized:
                candidate_ids = localized
        elif parsed.state:
            localized = [cid for cid in candidate_ids if self.reference_by_id[cid].state == parsed.state]
            if localized:
                candidate_ids = localized

        max_candidates = 80 if strong_locality else 12
        if not candidate_ids or len(candidate_ids) > max_candidates:
            return ""

        scored: List[Tuple[float, str]] = []
        parsed_core = parsed.street_core_signature or parsed.street_name
        for candidate_id in candidate_ids:
            candidate = self.reference_by_id[candidate_id]
            street_sim = cheap_similarity(parsed.street_name, candidate.street_name)
            core_overlap = token_overlap(parsed_core, " ".join(bit for bit in [candidate.predir, candidate.street_name, candidate.suffixdir] if bit))
            type_score = 1.0 if not parsed.street_type or parsed.street_type == candidate.street_type else 0.0
            locality_score = 1.0
            if parsed.city and parsed.state:
                locality_score = 1.0 if candidate.city == parsed.city and candidate.state == parsed.state else 0.0
            elif parsed.city:
                locality_score = 1.0 if candidate.city == parsed.city else 0.0
            elif parsed.zip_code:
                locality_score = 1.0 if candidate.zip_code[:3] == parsed.zip_code[:3] else 0.0
            score = 0.58 * street_sim + 0.22 * core_overlap + 0.10 * type_score + 0.10 * locality_score
            scored.append((score, candidate_id))

        scored.sort(reverse=True)
        best_score, best_id = scored[0]
        second_score = scored[1][0] if len(scored) > 1 else 0.0
        if strong_locality:
            best = self.reference_by_id[best_id]
            street_sim = cheap_similarity(parsed.street_name, best.street_name)
            type_ok = not parsed.street_type or parsed.street_type == best.street_type
            if street_sim >= 0.62 and type_ok and best_score - second_score >= 0.16:
                return best_id
        if best_score >= 0.88 and best_score - second_score >= 0.08:
            return best_id
        return ""

    def stage2_variants(self, parsed: ParsedAddress) -> List[Tuple[str, ParsedAddress]]:
        return [("original", parsed), *self.locality_variants(parsed)]

    def stage1_variants(self, parsed: ParsedAddress) -> List[Tuple[str, ParsedAddress]]:
        variants: List[Tuple[str, ParsedAddress]] = [("original", parsed), *self.locality_variants(parsed)]

        if parsed.unit_type or parsed.unit_value:
            variants.append(("drop_unit", rebuild_parsed(parsed, unit_type="", unit_value="")))

        if parsed.street_type:
            variants.append(("drop_type", rebuild_parsed(parsed, street_type="")))
            for candidate_type in STREET_TYPE_CONFUSIONS.get(parsed.street_type, ()):
                variants.append(("type_confusion", rebuild_parsed(parsed, street_type=candidate_type)))

        if parsed.zip_code and len(parsed.zip_code) >= 4:
            variants.append(("zip_prefix", rebuild_parsed(parsed, zip_code=parsed.zip_code[:3])))

        unique_variants: List[Tuple[str, ParsedAddress]] = []
        seen = set()
        for reason, variant in variants:
            key = (
                variant.house_number,
                variant.predir,
                variant.street_name,
                variant.street_type,
                variant.suffixdir,
                variant.unit_type,
                variant.unit_value,
                variant.city,
                variant.state,
                variant.zip_code,
            )
            if key not in seen:
                seen.add(key)
                unique_variants.append((reason, variant))
        return unique_variants

    def rough_score(self, parsed: ParsedAddress, candidate: ReferenceAddress) -> float:
        street_overlap = token_overlap(parsed.street_signature, candidate.street_signature)
        street_name_similarity = cheap_similarity(parsed.street_name, candidate.street_name)
        house_similarity = numeric_similarity(parsed.house_number, candidate.house_number)
        city_similarity = cheap_similarity(parsed.city, candidate.city)
        zip_exact = 1.0 if parsed.zip_code and parsed.zip_code == candidate.zip_code else 0.0
        zip_prefix = 1.0 if parsed.zip_code[:3] and parsed.zip_code[:3] == candidate.zip_code[:3] else 0.0
        state_exact = 1.0 if parsed.state and parsed.state == candidate.state else 0.0
        return (
            0.28 * house_similarity
            + 0.24 * street_name_similarity
            + 0.14 * street_overlap
            + 0.12 * city_similarity
            + 0.10 * zip_exact
            + 0.06 * zip_prefix
            + 0.06 * state_exact
        )

    def add_blocking_candidates(
        self,
        scores: Dict[str, float],
        candidate_ids: Iterable[str],
        weight: float,
        limit: Optional[int] = None,
    ) -> None:
        added = 0
        for candidate_id in candidate_ids:
            scores[candidate_id] += weight
            added += 1
            if limit is not None and added >= limit:
                break

    def adaptive_candidate_limit(self, variants: Sequence[Tuple[str, ParsedAddress]], base_limit: int) -> int:
        best = variants[0][1]
        missing = sum(1 for value in (best.house_number, best.street_name, best.city, best.state, best.zip_code) if not value)
        limit = base_limit + missing * 4
        if not best.zip_code:
            limit += 4
        if not best.city or not best.house_number:
            limit += 2
        return min(limit, 40)

    def candidate_ids_for_variants(self, variants: Sequence[Tuple[str, ParsedAddress]], limit: Optional[int] = None) -> List[str]:
        base_limit = self.default_candidate_limit if limit is None else limit
        candidate_limit = self.adaptive_candidate_limit(variants, base_limit)
        blocking_scores: Dict[str, float] = defaultdict(float)

        for _, variant in variants:
            if variant.house_number and variant.zip_code and variant.street_signature:
                self.add_blocking_candidates(
                    blocking_scores,
                    self.by_house_zip_street.get((variant.house_number, variant.zip_code, variant.street_signature), []),
                    12.0,
                    limit=10,
                )
            if variant.house_number and variant.zip_code and variant.street_name:
                self.add_blocking_candidates(
                    blocking_scores,
                    self.by_house_zip_street_name.get((variant.house_number, variant.zip_code, variant.street_name), []),
                    10.0,
                    limit=12,
                )
            if variant.house_number and variant.zip_code:
                self.add_blocking_candidates(
                    blocking_scores,
                    self.by_house_zip.get((variant.house_number, variant.zip_code), []),
                    11.0,
                    limit=120,
                )
            if variant.house_number and variant.city and variant.state and variant.street_signature:
                self.add_blocking_candidates(
                    blocking_scores,
                    self.by_house_city_street.get((variant.house_number, variant.city, variant.state, variant.street_signature), []),
                    9.0,
                    limit=12,
                )
            if variant.house_number and variant.city and variant.state and variant.street_name:
                self.add_blocking_candidates(
                    blocking_scores,
                    self.by_house_city_street_name.get((variant.house_number, variant.city, variant.state, variant.street_name), []),
                    8.0,
                    limit=15,
                )
            if variant.house_number and variant.city and variant.state:
                self.add_blocking_candidates(
                    blocking_scores,
                    self.by_house_city_state.get((variant.house_number, variant.city, variant.state), []),
                    11.0,
                    limit=160,
                )
            if variant.zip_code:
                self.add_blocking_candidates(blocking_scores, self.by_zip.get(variant.zip_code, []), 7.0, limit=50)
                if variant.house_number and variant.zip_code[:3]:
                    self.add_blocking_candidates(
                        blocking_scores,
                        self.by_house_zip_prefix.get((variant.house_number, variant.zip_code[:3]), []),
                        8.5,
                        limit=160,
                    )
                self.add_blocking_candidates(blocking_scores, self.by_zip_prefix.get(variant.zip_code[:3], []), 2.5, limit=70)
            if variant.city and variant.state:
                self.add_blocking_candidates(blocking_scores, self.by_city_state.get((variant.city, variant.state), []), 5.5, limit=60)
                if variant.street_signature:
                    self.add_blocking_candidates(
                        blocking_scores,
                        self.by_city_street.get((variant.city, variant.state, variant.street_signature), []),
                        7.5,
                        limit=80,
                    )
                if variant.street_name:
                    self.add_blocking_candidates(
                        blocking_scores,
                        self.by_city_street_name.get((variant.city, variant.state, variant.street_name), []),
                        7.0,
                        limit=100,
                    )
            if variant.state:
                self.add_blocking_candidates(blocking_scores, self.by_state.get(variant.state, []), 1.0, limit=90)
            if variant.house_number:
                self.add_blocking_candidates(blocking_scores, self.by_house.get(variant.house_number, []), 6.0, limit=60)

            street_tokens = [token for token in token_set(variant.street_name) if len(token) >= 3]
            for token in street_tokens:
                self.add_blocking_candidates(blocking_scores, self.street_token_index.get(token, ()), 1.8, limit=35)
            city_tokens = [token for token in token_set(variant.city) if len(token) >= 3]
            for token in city_tokens:
                self.add_blocking_candidates(blocking_scores, self.city_token_index.get(token, ()), 0.8, limit=25)

        if not blocking_scores:
            blocking_pool = [row.address_id for row in self.reference_rows]
        else:
            prioritized = sorted(blocking_scores.items(), key=lambda item: item[1], reverse=True)
            prefilter_limit = max(self.blocking_prefilter_limit, candidate_limit * 3)
            blocking_pool = [candidate_id for candidate_id, _ in prioritized[: prefilter_limit]]

        scored = []
        for candidate_id in blocking_pool:
            candidate = self.reference_by_id[candidate_id]
            best = max(self.rough_score(variant, candidate) for _, variant in variants)
            blocking_bonus = blocking_scores.get(candidate_id, 0.0)
            scored.append((candidate_id, best, blocking_bonus))
        scored.sort(key=lambda item: (item[1], item[2]), reverse=True)
        return [candidate_id for candidate_id, _, _ in scored[:candidate_limit]]

    def candidate_ids(self, parsed: ParsedAddress, limit: Optional[int] = None) -> List[str]:
        return self.candidate_ids_for_variants(self.stage2_variants(parsed), limit=limit)

    def candidate_features_for_variants(self, variants: Sequence[Tuple[str, ParsedAddress]], candidate_id: str) -> CandidateFeatures:
        candidate = self.reference_by_id[candidate_id]
        best_reason = variants[0][0]
        best_variant = variants[0][1]
        best_values: Tuple[float, ...] = ()
        best_key = (-1.0, -1.0)

        for variant_reason, variant in variants:
            full_similarity = sequence_similarity(variant.standardized_address, candidate.standardized_address)
            street_name_similarity = sequence_similarity(variant.street_name, candidate.street_name)
            street_signature_overlap = token_overlap(variant.street_signature, candidate.street_signature)
            city_similarity = sequence_similarity(variant.city, candidate.city)
            house_similarity = numeric_similarity(variant.house_number, candidate.house_number)
            street_phonetic_similarity = phonetic_similarity(variant.street_name, candidate.street_name)
            city_phonetic_similarity = phonetic_similarity(variant.city, candidate.city)
            zip_exact = 1.0 if variant.zip_code and variant.zip_code == candidate.zip_code else 0.0
            zip_prefix = 1.0 if variant.zip_code[:3] and variant.zip_code[:3] == candidate.zip_code[:3] else 0.0
            type_exact = 1.0 if variant.street_type and variant.street_type == candidate.street_type else 0.0
            type_missing = 1.0 if not variant.street_type else 0.0
            predir_exact = 1.0 if variant.predir == candidate.predir else 0.0
            suffixdir_exact = 1.0 if variant.suffixdir == candidate.suffixdir else 0.0
            unit_exact = 1.0 if variant.unit_type == candidate.unit_type and variant.unit_value == candidate.unit_value and variant.unit_type else 0.0
            unit_missing = 1.0 if not variant.unit_type and not variant.unit_value else 0.0
            state_exact = 1.0 if variant.state == candidate.state and variant.state else 0.0
            city_state_exact = 1.0 if city_similarity == 1.0 and state_exact else 0.0
            house_exact = 1.0 if variant.house_number == candidate.house_number and variant.house_number else 0.0
            street_exact = 1.0 if variant.street_name == candidate.street_name and variant.street_name else 0.0
            locality_corrected = 0.0 if variant_reason == "original" else 1.0
            missing_city = 1.0 if not variant.city else 0.0
            missing_state = 1.0 if not variant.state else 0.0
            missing_zip = 1.0 if not variant.zip_code else 0.0
            rough = self.rough_score(variant, candidate)
            localities_for_query_zip = self.zip_to_localities.get(variant.zip_code, set()) if variant.zip_code else set()
            if localities_for_query_zip:
                zip_city_consistency = 1.0 if (candidate.city, candidate.state) in localities_for_query_zip else 0.0
            elif candidate.zip_code and variant.city and variant.state:
                zip_city_consistency = 1.0 if (variant.city, variant.state) in self.zip_to_localities.get(candidate.zip_code, set()) else 0.0
            else:
                zip_city_consistency = 0.5
            house_mismatch_strong_context = 1.0 if (
                variant.house_number
                and candidate.house_number
                and variant.house_number != candidate.house_number
                and max(street_name_similarity, street_phonetic_similarity) >= 0.88
                and (city_similarity >= 0.86 or zip_exact)
            ) else 0.0

            values = (
                1.0,
                full_similarity,
                street_name_similarity,
                street_signature_overlap,
                city_similarity,
                house_similarity,
                zip_exact,
                zip_prefix,
                type_exact,
                type_missing,
                predir_exact,
                suffixdir_exact,
                unit_exact,
                unit_missing,
                state_exact,
                city_state_exact,
                house_exact,
                street_exact,
                locality_corrected,
                missing_city,
                missing_state,
                missing_zip,
                rough,
                street_phonetic_similarity,
                city_phonetic_similarity,
                zip_city_consistency,
                house_mismatch_strong_context,
                candidate.source_quality,
            )
            key = (
                full_similarity
                + max(street_name_similarity, 0.92 * street_phonetic_similarity)
                + house_similarity
                + 0.5 * max(city_similarity, 0.9 * city_phonetic_similarity)
                - 0.2 * house_mismatch_strong_context,
                rough,
            )
            if key > best_key:
                best_key = key
                best_reason = variant_reason
                best_variant = variant
                best_values = values

        return CandidateFeatures(
            reference_id=candidate_id,
            variant_reason=best_reason,
            variant=best_variant,
            values=best_values,
        )

    def candidate_features(self, parsed: ParsedAddress, candidate_id: str) -> CandidateFeatures:
        return self.candidate_features_for_variants(self.stage2_variants(parsed), candidate_id)

    def resolve_stage1(self, parsed: ParsedAddress, review_threshold: float = 0.97) -> Resolution:
        cache_key = (parsed.standardized_address, review_threshold)
        cached = self._stage1_cache.get(cache_key)
        if cached is not None:
            return cached
        variants = self.stage1_variants(parsed)

        stage1_rules = (
            ("exact", 0.99, lambda item: self.unique_match(self.by_exact, item.standardized_address)),
            ("house_zip_street", 0.97, lambda item: self.unique_match(self.by_house_zip_street, (item.house_number, item.zip_code, item.street_signature)) if item.zip_code else ""),
            ("house_zip_street_name", 0.95, lambda item: self.unique_match(self.by_house_zip_street_name, (item.house_number, item.zip_code, item.street_name)) if item.zip_code else ""),
            ("house_zip_core", 0.945, lambda item: self.unique_match(self.by_house_zip_core, (item.house_number, item.zip_code, item.street_core_signature)) if item.zip_code else ""),
            ("house_zip_prefix_street", 0.93, lambda item: self.unique_match(self.by_house_zip_prefix_street, (item.house_number, item.zip_code[:3], item.street_signature)) if item.zip_code[:3] else ""),
            ("house_zip_prefix_street_name", 0.92, lambda item: self.unique_match(self.by_house_zip_prefix_street_name, (item.house_number, item.zip_code[:3], item.street_name)) if item.zip_code[:3] else ""),
            ("house_city_street", 0.93, lambda item: self.unique_match(self.by_house_city_street, (item.house_number, item.city, item.state, item.street_signature))),
            ("house_city_street_name", 0.91, lambda item: self.unique_match(self.by_house_city_street_name, (item.house_number, item.city, item.state, item.street_name))),
            ("house_city_core", 0.905, lambda item: self.unique_match(self.by_house_city_core, (item.house_number, item.city, item.state, item.street_core_signature))),
            ("house_locality_fuzzy_street", 0.93, self.fuzzy_house_locality_match),
        )

        for variant_reason, variant in variants:
            for rule_name, confidence, finder in stage1_rules:
                match_id = finder(variant)
                if not match_id:
                    continue
                matched = self.reference_by_id[match_id]
                final_confidence = confidence if variant_reason == "original" else max(0.88, confidence - 0.03)
                resolution = Resolution(
                    predicted_match_id=matched.address_id,
                    predicted_canonical_address=matched.canonical_address,
                    standardized_query_address=variant.standardized_address,
                    confidence=final_confidence,
                    needs_review=final_confidence < review_threshold,
                    stage=f"stage1_{variant_reason}_{rule_name}",
                    top_candidates=(CandidateScore(matched.address_id, final_confidence),),
                )
                self._stage1_cache[cache_key] = resolution
                return resolution

        resolution = Resolution(
            predicted_match_id="",
            predicted_canonical_address="",
            standardized_query_address=parsed.standardized_address,
            confidence=0.0,
            needs_review=True,
            stage="stage1_unresolved",
            top_candidates=(),
        )
        self._stage1_cache[cache_key] = resolution
        return resolution


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
                scored.append(
                    CandidateScore(reference_id=candidate_id, score=self.predict_probability(features))
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
        standardized_query = best_features.variant.standardized_address

        if decision_score >= accept_threshold and len(ranked) > 1 and margin < self.ambiguous_margin_threshold:
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
_WORKER_REVIEW_THRESHOLD: float = 0.0
_WORKER_COMPUTE_STAGE2: bool = True


def init_resolution_worker(
    dataset_dir_str: str,
    model_path_str: str,
    accept_threshold: float,
    review_threshold: float,
    compute_stage2_for_all: bool,
) -> None:
    global _WORKER_RESOLVER, _WORKER_MODEL, _WORKER_ACCEPT_THRESHOLD, _WORKER_REVIEW_THRESHOLD, _WORKER_COMPUTE_STAGE2
    dataset_dir = Path(dataset_dir_str)
    reference_rows, _ = load_reference(dataset_dir / "reference_addresses.csv")
    city_lookup = build_city_lookup(reference_rows)
    resolver = Resolver(reference_rows, city_lookup)
    model, _, _, _ = load_model(Path(model_path_str), resolver)
    _WORKER_RESOLVER = resolver
    _WORKER_MODEL = model
    _WORKER_ACCEPT_THRESHOLD = accept_threshold
    _WORKER_REVIEW_THRESHOLD = review_threshold
    _WORKER_COMPUTE_STAGE2 = compute_stage2_for_all


def resolve_single_query_worker(query: QueryAddress) -> Tuple[str, Resolution, Optional[Resolution], Resolution]:
    if _WORKER_RESOLVER is None or _WORKER_MODEL is None:
        raise RuntimeError("Resolution worker was not initialized.")

    parsed = _WORKER_RESOLVER.parse(query.query_address)
    stage1 = _WORKER_RESOLVER.resolve_stage1(parsed, review_threshold=_WORKER_REVIEW_THRESHOLD)
    if stage1.predicted_match_id and not _WORKER_COMPUTE_STAGE2:
        combined = stage1
        stage2 = None
    else:
        stage2 = _WORKER_MODEL.resolve(parsed, accept_threshold=_WORKER_ACCEPT_THRESHOLD, review_threshold=_WORKER_REVIEW_THRESHOLD)
        combined = choose_combined_resolution(stage1, stage2)
    return query.query_id, stage1, stage2, combined


def resolve_queries(
    resolver: Resolver,
    stage2_model: Stage2Model,
    queries: Sequence[QueryAddress],
    accept_threshold: float,
    review_threshold: float,
    compute_stage2_for_all: bool = True,
    jobs: int = 1,
    eval_dataset_dir: Optional[Path] = None,
    model_path: Optional[Path] = None,
) -> Tuple[Dict[str, Resolution], Optional[Dict[str, Resolution]], Dict[str, Resolution]]:
    if jobs > 1:
        if eval_dataset_dir is None or model_path is None:
            raise ValueError("Parallel query resolution requires eval_dataset_dir and model_path.")
        stage1_resolutions: Dict[str, Resolution] = {}
        stage2_resolutions: Optional[Dict[str, Resolution]] = {} if compute_stage2_for_all else None
        combined_resolutions: Dict[str, Resolution] = {}

        with ProcessPoolExecutor(
            max_workers=jobs,
            initializer=init_resolution_worker,
            initargs=(
                str(eval_dataset_dir),
                str(model_path),
                accept_threshold,
                review_threshold,
                compute_stage2_for_all,
            ),
        ) as executor:
            for query_id, stage1, stage2, combined in executor.map(resolve_single_query_worker, queries, chunksize=64):
                stage1_resolutions[query_id] = stage1
                if stage2_resolutions is not None and stage2 is not None:
                    stage2_resolutions[query_id] = stage2
                combined_resolutions[query_id] = combined
        return stage1_resolutions, stage2_resolutions, combined_resolutions

    stage1_resolutions: Dict[str, Resolution] = {}
    stage2_resolutions: Optional[Dict[str, Resolution]] = {} if compute_stage2_for_all else None
    combined_resolutions: Dict[str, Resolution] = {}

    for query in queries:
        parsed = resolver.parse(query.query_address)
        stage1 = resolver.resolve_stage1(parsed, review_threshold=review_threshold)
        if stage1.predicted_match_id and not compute_stage2_for_all:
            combined = stage1
            stage2 = None
        else:
            stage2 = stage2_model.resolve(parsed, accept_threshold=accept_threshold, review_threshold=review_threshold)
            combined = choose_combined_resolution(stage1, stage2)

        stage1_resolutions[query.query_id] = stage1
        if stage2_resolutions is not None and stage2 is not None:
            stage2_resolutions[query.query_id] = stage2
        combined_resolutions[query.query_id] = combined

    return stage1_resolutions, stage2_resolutions, combined_resolutions


def write_predictions(
    output_path: Path,
    queries: Sequence[QueryAddress],
    combined_resolutions: Dict[str, Resolution],
) -> None:
    fieldnames = [
        "query_id",
        "split",
        "input_address",
        "standardized_address",
        "predicted_match_id",
        "predicted_canonical_address",
        "confidence",
        "needs_review",
        "resolution_stage",
        "true_label",
        "true_match_id",
        "correct",
        "top_candidates",
    ]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for query in queries:
            resolution = combined_resolutions[query.query_id]
            writer.writerow(
                {
                    "query_id": query.query_id,
                    "split": query.split,
                    "input_address": query.query_address,
                    "standardized_address": resolution.standardized_query_address,
                    "predicted_match_id": resolution.predicted_match_id or "NO_MATCH",
                    "predicted_canonical_address": resolution.predicted_canonical_address or "NO_MATCH",
                    "confidence": f"{resolution.confidence:.4f}",
                    "needs_review": str(resolution.needs_review),
                    "resolution_stage": resolution.stage,
                    "true_label": query.label,
                    "true_match_id": query.true_match_id or "NO_MATCH",
                    "correct": str(is_correct(query, resolution)),
                    "top_candidates": "; ".join(f"{candidate.reference_id}:{candidate.score:.3f}" for candidate in resolution.top_candidates),
                }
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve generated messy addresses against the reference dataset.")
    parser.add_argument(
        "--mode",
        choices=("fit-predict", "train", "predict"),
        default="fit-predict",
        help="`train` saves a model, `predict` loads a saved model, and `fit-predict` does both in one run.",
    )
    parser.add_argument("--dataset-dir", type=Path, default=Path.cwd() / "datasets" / "address_dataset", help="Directory containing reference_addresses.csv and queries.csv.")
    parser.add_argument("--train-dataset-dir", type=Path, help="Optional dataset directory used only for model fitting.")
    parser.add_argument("--eval-dataset-dir", type=Path, help="Optional dataset directory used only for prediction/evaluation.")
    parser.add_argument(
        "--augment-eval-reference-csv",
        type=Path,
        help="Optional full reference CSV to append to the eval reference set as live-scale distractors while preserving eval labels.",
    )
    parser.add_argument("--query-limit", type=int, help="Optional cap on eval queries for fast smoke runs.")
    parser.add_argument(
        "--active-learning-feedback-csv",
        type=Path,
        action="append",
        default=[],
        help="Append app feedback rows to Stage 2 training. May be repeated.",
    )
    parser.add_argument("--output-dir", type=Path, default=Path.cwd() / "runs" / "resolver_output", help="Directory for predictions and evaluation reports.")
    parser.add_argument("--model-path", type=Path, default=Path.cwd() / "models" / "stage2_model.json", help="Path to saved Stage 2 model JSON.")
    parser.add_argument("--compare-variants", action="store_true", help="In predict mode, also compute stage2-only comparison metrics instead of only the combined pipeline.")
    parser.add_argument("--fast-predict", action="store_true", help="In predict mode, skip evaluation-only reporting work and emit only combined predictions plus runtime metadata.")
    parser.add_argument("--jobs", type=int, default=max(1, min(8, os.cpu_count() or 1)), help="Number of worker processes for query resolution/evaluation.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_dir = args.dataset_dir.expanduser().resolve()
    train_dataset_dir = (args.train_dataset_dir.expanduser().resolve() if args.train_dataset_dir else dataset_dir)
    eval_dataset_dir = (args.eval_dataset_dir.expanduser().resolve() if args.eval_dataset_dir else dataset_dir)
    augment_eval_reference_csv = (
        args.augment_eval_reference_csv.expanduser().resolve()
        if args.augment_eval_reference_csv
        else None
    )
    output_dir = args.output_dir.expanduser().resolve()
    model_path = args.model_path.expanduser().resolve()
    active_learning_feedback_paths = [path.expanduser().resolve() for path in args.active_learning_feedback_csv]
    output_dir.mkdir(parents=True, exist_ok=True)
    if args.query_limit is not None and args.query_limit <= 0:
        raise SystemExit("--query-limit must be greater than zero.")

    if args.mode in {"train", "fit-predict"}:
        train_reference_rows, _ = load_reference(train_dataset_dir / "reference_addresses.csv")
        train_queries_all = load_queries(train_dataset_dir / "queries.csv")
        active_learning_stats = []
        for feedback_path in active_learning_feedback_paths:
            feedback_queries, feedback_stats = load_active_learning_feedback_queries(feedback_path, train_reference_rows)
            feedback_stats["path"] = str(feedback_path)
            active_learning_stats.append(feedback_stats)
            train_queries_all.extend(feedback_queries)
        train_city_lookup = build_city_lookup(train_reference_rows)
        train_resolver = Resolver(train_reference_rows, train_city_lookup)
        train_queries = [query for query in train_queries_all if query.split == "train"]
        calibration_queries = [query for query in train_queries_all if query.split == "validation"]
        if not calibration_queries:
            calibration_queries = train_queries
        stage2_model = fit_stage2_model(train_resolver, train_queries)
        accept_threshold = choose_accept_threshold(stage2_model, train_resolver, calibration_queries)

        bootstrap_stage2 = []
        for query in calibration_queries:
            parsed = train_resolver.parse(query.query_address)
            bootstrap_stage2.append((query, stage2_model.resolve(parsed, accept_threshold=accept_threshold, review_threshold=0.90)))
        review_threshold = choose_review_threshold(bootstrap_stage2)

        save_model(
            model_path,
            stage2_model,
            accept_threshold,
            review_threshold,
            metadata={
                "train_dataset_dir": str(train_dataset_dir),
                "eval_dataset_dir": str(eval_dataset_dir),
                "query_count": len(train_queries_all),
                "reference_count": len(train_reference_rows),
                "train_query_count": len(train_queries),
                "calibration_query_count": len(calibration_queries),
                "active_learning_feedback": active_learning_stats,
            },
        )
        print(f"Saved Stage 2 model to: {model_path}")
        if args.mode == "train":
            return

    eval_reference_rows, _ = load_reference(eval_dataset_dir / "reference_addresses.csv")
    reference_augmentation: Dict[str, int] = {
        "base_reference_count": len(eval_reference_rows),
        "extra_reference_count": 0,
        "added_reference_count": 0,
        "duplicate_address_count": 0,
        "renamed_reference_count": 0,
        "combined_reference_count": len(eval_reference_rows),
    }
    if augment_eval_reference_csv:
        extra_reference_rows, _ = load_reference(augment_eval_reference_csv)
        eval_reference_rows, reference_augmentation = augment_reference_rows(
            eval_reference_rows,
            extra_reference_rows,
        )
    eval_queries = load_queries(eval_dataset_dir / "queries.csv")
    if args.query_limit is not None:
        eval_queries = eval_queries[: args.query_limit]
    eval_city_lookup = build_city_lookup(eval_reference_rows)
    eval_resolver = Resolver(eval_reference_rows, eval_city_lookup)

    if args.mode == "predict":
        if not model_path.exists():
            raise SystemExit(f"Saved model not found: {model_path}")
        stage2_model, accept_threshold, review_threshold, _ = load_model(model_path, eval_resolver)
    else:
        stage2_model = Stage2Model.from_dict(eval_resolver, stage2_model.to_dict())

    fast_predict = args.fast_predict and args.mode == "predict"
    compare_variants = (args.compare_variants or args.mode != "predict") and not fast_predict
    effective_jobs = max(1, args.jobs)
    if augment_eval_reference_csv and effective_jobs > 1:
        print("Augmented eval references are resolved in-process; using --jobs 1 for this run.")
        effective_jobs = 1
    started = time.perf_counter()
    stage1_resolutions, stage2_resolutions, combined_resolutions = resolve_queries(
        resolver=eval_resolver,
        stage2_model=stage2_model,
        queries=eval_queries,
        accept_threshold=accept_threshold,
        review_threshold=review_threshold,
        compute_stage2_for_all=compare_variants,
        jobs=effective_jobs,
        eval_dataset_dir=eval_dataset_dir,
        model_path=model_path,
    )
    runtime_seconds = time.perf_counter() - started

    write_predictions(output_dir / "predictions.csv", eval_queries, combined_resolutions)
    evaluation = {
        "mode": args.mode,
        "dataset_dir": str(dataset_dir),
        "train_dataset_dir": str(train_dataset_dir),
        "eval_dataset_dir": str(eval_dataset_dir),
        "augment_eval_reference_csv": str(augment_eval_reference_csv) if augment_eval_reference_csv else "",
        "output_dir": str(output_dir),
        "model_path": str(model_path),
        "query_count": len(eval_queries),
        "reference_count": len(eval_reference_rows),
        "reference_augmentation": reference_augmentation,
        "jobs": effective_jobs,
        "query_limit": args.query_limit,
        "accept_threshold": accept_threshold,
        "review_threshold": review_threshold,
        "runtime_seconds": round(runtime_seconds, 4),
        "variants": {},
    }
    if not fast_predict:
        evaluation["variants"]["stage1_only"] = evaluate_variant("stage1_only", eval_queries, stage1_resolutions)
        if stage2_resolutions is not None:
            evaluation["variants"]["stage2_only"] = evaluate_variant("stage2_only", eval_queries, stage2_resolutions)
        evaluation["variants"]["combined"] = evaluate_variant("combined", eval_queries, combined_resolutions)
        if "stage2_only" in evaluation["variants"]:
            evaluation["comparisons"] = {
                "stage2_vs_stage1": compare_variant_metrics(
                    "stage2_vs_stage1",
                    evaluation["variants"]["stage2_only"],
                    evaluation["variants"]["stage1_only"],
                ),
                "combined_vs_stage1": compare_variant_metrics(
                    "combined_vs_stage1",
                    evaluation["variants"]["combined"],
                    evaluation["variants"]["stage1_only"],
                ),
                "combined_vs_stage2": compare_variant_metrics(
                    "combined_vs_stage2",
                    evaluation["variants"]["combined"],
                    evaluation["variants"]["stage2_only"],
                ),
            }
    (output_dir / "evaluation.json").write_text(json.dumps(evaluation, indent=2), encoding="utf-8")

    print("Address resolver finished.")
    print(json.dumps(evaluation, indent=2))
    print(f"\nPredictions written to: {output_dir / 'predictions.csv'}")
    print(f"Evaluation written to: {output_dir / 'evaluation.json'}")


if __name__ == "__main__":
    main()
