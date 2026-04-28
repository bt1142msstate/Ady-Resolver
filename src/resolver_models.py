#!/usr/bin/env python3
"""Shared data models and feature metadata for address resolution."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple


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
