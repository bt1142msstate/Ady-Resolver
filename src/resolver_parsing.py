#!/usr/bin/env python3
"""Address normalization, similarity scoring, and query parsing helpers."""
from __future__ import annotations

import re
from dataclasses import replace
from difflib import SequenceMatcher
from functools import lru_cache
from typing import Dict, List, Optional, Sequence, Tuple

from address_dataset_generator import (
    DIRECTION_TO_FULL,
    STATE_ABBREV_TO_NAME,
    STREET_TYPE_ALIASES,
    STREET_TYPE_TO_FULL,
    UNIT_TYPE_TO_FULL,
)
from resolver_models import ParsedAddress


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
    "MISISSIPI": "MS",
    "MISISIPI": "MS",
    "MISSISIPPI": "MS",
    "MISISIPPI": "MS",
    "MISISSIPPI": "MS",
    "MISSISSPPI": "MS",
}
STREET_TYPE_TYPO_ALIASES = {
    "SR": "ST",
    "SY": "ST",
    "DT": "ST",
    "STRET": "ST",
    "STREE": "ST",
    "STRETT": "ST",
    "XR": "DR",
    "FR": "DR",
    "DN": "DR",
    "DRIE": "DR",
    "DRIEV": "DR",
    "DRIVEE": "DR",
    "RN": "RD",
    "RF": "RD",
    "RAOD": "RD",
    "RODA": "RD",
    "LF": "LN",
    "KN": "LN",
    "AB": "AVE",
    "AC": "AVE",
    "SV": "AVE",
    "AVNE": "AVE",
    "AVNEU": "AVE",
    "CIRCDE": "CIR",
}
CONTEXTUAL_STREET_TYPE_TYPO_ALIASES = {
    "SE": "ST",
}
PUNCT_RE = re.compile(r"[.,]")
SPACE_RE = re.compile(r"\s+")
TOKEN_RE = re.compile(r"[A-Z0-9#]+")


VALID_STATES = set(STATE_ABBREV_TO_NAME)

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
        typo_match = STREET_TYPE_TYPO_ALIASES.get(normalized, "")
        if typo_match:
            return typo_match
        if len(normalized) >= 5:
            full_match, best_score, second_score = closest_choice(
                normalized,
                tuple(FULL_TO_STREET_TYPE),
                minimum_score=0.80,
            )
            if full_match and best_score - second_score >= 0.04:
                return FULL_TO_STREET_TYPE[full_match]
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
            other_house_number = any(
                other_idx != idx and looks_like_house_number_token(other_token)
                for other_idx, other_token in enumerate(tokens)
            )
            if not other_house_number:
                continue
            del tokens[idx]
            return token
    if tokens and re.fullmatch(r"\d{4}", tokens[-1]):
        other_house_number = any(looks_like_house_number_token(token) for token in tokens[:-1])
        if not other_house_number:
            return ""
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


def city_candidate_too_short(candidate: Sequence[str]) -> bool:
    return sum(len(token) for token in candidate) < 3


def extract_city(tokens: List[str], city_lookup: Dict[Tuple[str, ...], str], state: str, zip_code: str) -> str:
    if not tokens:
        return ""
    for width in range(min(3, len(tokens)), 0, -1):
        candidate = tuple(tokens[-width:])
        if city_candidate_too_short(candidate):
            continue
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
            if city_candidate_too_short(candidate):
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
