#!/usr/bin/env python3
"""Address rendering and typo/noise generation helpers."""
from __future__ import annotations

import random
import string
from copy import deepcopy
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from address_source_loading import (
    AddressRecord,
    DIRECTION_TO_FULL,
    KEYBOARD_NEIGHBORS,
    OCR_ALPHA_CONFUSIONS,
    OCR_DIGIT_CONFUSIONS,
    PHONETIC_REPLACEMENTS,
    STATE_ABBREV_TO_NAME,
    STREET_TYPE_CONFUSIONS,
    STREET_TYPE_KEYBOARD_TYPOS,
    STREET_TYPE_TO_FULL,
    UNIT_TYPE_TO_FULL,
    normalize_spaces,
    titleish,
)


@dataclass
class RenderStyle:
    uppercase: bool = False
    lowercase: bool = False
    use_full_directional: bool = False
    use_full_street_type: bool = False
    use_full_unit_type: bool = False
    include_unit: bool = True
    include_zip: bool = True
    include_commas: bool = True
    unit_first: bool = False
    house_after_street_name: bool = False
    component_order: Tuple[str, ...] = ()
    slash_unit: bool = False
    extra_spaces: bool = False



def shift_house_number(value: str, rng: random.Random) -> str:
    match = re.search(r"\d+", value)
    if not match:
        return mutate_numeric_token(value, rng) if value else str(rng.randint(1, 9999))
    current = int(match.group(0))
    step = rng.choice([2, 4, 6, 8, 10, 12])
    sign = -1 if rng.random() < 0.35 else 1
    shifted = str(max(1, current + sign * step))
    return f"{value[:match.start()]}{shifted}{value[match.end():]}"


def choose_weighted(rng: random.Random, items: Sequence[Tuple[str, float]]) -> str:
    total = sum(weight for _, weight in items)
    point = rng.random() * total
    upto = 0.0
    for item, weight in items:
        upto += weight
        if point <= upto:
            return item
    return items[-1][0]


def mutate_alpha_token(token: str, rng: random.Random) -> str:
    letters = [i for i, ch in enumerate(token) if ch.isalpha()]
    if not letters:
        return token
    mode = choose_weighted(rng, [("substitute", 0.5), ("delete", 0.25), ("transpose", 0.25)])
    idx = rng.choice(letters)
    chars = list(token)

    if mode == "substitute":
        lower = chars[idx].lower()
        replacement_pool = KEYBOARD_NEIGHBORS.get(lower, string.ascii_lowercase)
        replacement = rng.choice(replacement_pool)
        chars[idx] = replacement.upper() if chars[idx].isupper() else replacement
        return "".join(chars)

    if mode == "delete" and len(token) > 4:
        del chars[idx]
        return "".join(chars)

    if mode == "transpose" and len(token) > 3:
        swap_idx = idx + 1 if idx < len(token) - 1 else idx - 1
        if 0 <= swap_idx < len(token):
            chars[idx], chars[swap_idx] = chars[swap_idx], chars[idx]
            return "".join(chars)

    # fallback if transpose/delete are not feasible
    chars[idx] = rng.choice(string.ascii_lowercase)
    return "".join(chars)


def mutate_extra_or_missing_letter(token: str, rng: random.Random) -> str:
    letters = [idx for idx, ch in enumerate(token) if ch.isalpha()]
    if not letters:
        return token
    idx = rng.choice(letters)
    if len(token) > 4 and rng.random() < 0.55:
        return token[:idx] + token[idx + 1:]
    return token[:idx] + token[idx] + token[idx:]


def mutate_street_type_token(token: str, rng: random.Random) -> str:
    token = token.upper()
    choices = [choice for choice in STREET_TYPE_KEYBOARD_TYPOS.get(token, ()) if choice != token]
    if choices and rng.random() < 0.85:
        return rng.choice(choices)
    mutated = mutate_alpha_token(token, rng).upper()
    return mutated if mutated != token else (choices[0] if choices else token)


def mutate_numeric_token(token: str, rng: random.Random) -> str:
    digits = [i for i, ch in enumerate(token) if ch.isdigit()]
    if not digits:
        return token
    idx = rng.choice(digits)
    chars = list(token)
    replacement_choices = [d for d in string.digits if d != chars[idx]]
    chars[idx] = rng.choice(replacement_choices)
    return "".join(chars)


def mutate_ocr_token(token: str, rng: random.Random) -> str:
    if not token:
        return token

    positions = [idx for idx, ch in enumerate(token) if ch.isalnum()]
    if not positions:
        return token

    idx = rng.choice(positions)
    chars = list(token)
    current = chars[idx]
    if current.isdigit():
        replacement = OCR_DIGIT_CONFUSIONS.get(current)
        if not replacement:
            return token
        chars[idx] = replacement
        return "".join(chars)

    replacement = OCR_ALPHA_CONFUSIONS.get(current.lower())
    if not replacement:
        return token
    if len(replacement) == 1:
        chars[idx] = replacement.upper() if current.isupper() else replacement
        return "".join(chars)

    rendered = replacement.upper() if current.isupper() else replacement
    return token[:idx] + rendered + token[idx + 1:]


def mutate_locality_token(token: str, rng: random.Random) -> str:
    """Produce more human-looking misspellings for cities/states than a single typo."""
    if len(token) < 4:
        return token

    mutated = token
    edit_count = 1 if len(token) <= 6 else rng.choice([1, 2, 2, 3])
    for _ in range(edit_count):
        mode = choose_weighted(
            rng,
            [
                ("alpha", 0.34),
                ("duplicate_vowel", 0.16),
                ("drop_vowel", 0.16),
                ("swap_pair", 0.12),
                ("soft_replace", 0.12),
                ("ocr", 0.10),
            ],
        )

        if mode == "alpha":
            mutated = mutate_alpha_token(mutated, rng)
            continue

        if mode == "ocr":
            mutated = mutate_ocr_token(mutated, rng)
            continue

        letters = [i for i, ch in enumerate(mutated) if ch.isalpha()]
        if not letters:
            continue

        if mode == "duplicate_vowel":
            vowels = [i for i in letters if mutated[i].lower() in "aeiou"]
            if vowels:
                idx = rng.choice(vowels)
                mutated = mutated[:idx + 1] + mutated[idx] + mutated[idx + 1:]
                continue

        if mode == "drop_vowel" and len(mutated) > 5:
            vowels = [i for i in letters if mutated[i].lower() in "aeiou"]
            if vowels:
                idx = rng.choice(vowels)
                mutated = mutated[:idx] + mutated[idx + 1:]
                continue

        if mode == "swap_pair" and len(mutated) > 4:
            idx = rng.choice(letters[:-1]) if len(letters) > 1 else letters[0]
            swap_idx = idx + 1
            if swap_idx < len(mutated):
                chars = list(mutated)
                chars[idx], chars[swap_idx] = chars[swap_idx], chars[idx]
                mutated = "".join(chars)
                continue

        if mode == "soft_replace":
            idx = rng.choice(letters)
            ch = mutated[idx].lower()
            replacements = {
                "i": "e",
                "e": "i",
                "o": "u",
                "u": "o",
                "s": "ss",
                "c": "s",
                "p": "pp",
                "l": "ll",
            }.get(ch)
            if replacements:
                rendered = replacements.upper() if mutated[idx].isupper() else replacements
                mutated = mutated[:idx] + rendered + mutated[idx + 1:]
                continue

        mutated = mutate_alpha_token(mutated, rng)

    return mutated


def mutate_phonetic_token(token: str, rng: random.Random) -> str:
    if len(token) < 4:
        return token

    upper = token.upper()
    candidates = [(src, dst) for src, dst in PHONETIC_REPLACEMENTS if src in upper]
    if candidates:
        src, dst = rng.choice(candidates)
        idx = upper.index(src)
        rendered = dst if token.isupper() else dst.lower()
        return token[:idx] + rendered + token[idx + len(src):]

    # fallback: vowel drift tends to create phonetic-looking misspellings
    for src, dst in (("A", "E"), ("E", "I"), ("I", "Y"), ("O", "U"), ("U", "O")):
        if src in upper:
            idx = upper.index(src)
            rendered = dst if token.isupper() else dst.lower()
            return token[:idx] + rendered + token[idx + 1:]
    return token


def render_component_direction(code: str, style: RenderStyle) -> str:
    if not code:
        return ""
    return DIRECTION_TO_FULL[code] if style.use_full_directional else code


def render_component_street_type(code: str, style: RenderStyle) -> str:
    if not code:
        return ""
    return STREET_TYPE_TO_FULL.get(code, code) if style.use_full_street_type else code


def render_component_unit_type(code: str, style: RenderStyle) -> str:
    if not code:
        return ""
    return UNIT_TYPE_TO_FULL.get(code, code) if style.use_full_unit_type else code


def render_address(record: AddressRecord, style: Optional[RenderStyle] = None) -> str:
    style = style or RenderStyle()
    pieces: List[str] = []

    predir = render_component_direction(record.predir, style)
    street_type = render_component_street_type(record.street_type, style)
    suffixdir = render_component_direction(record.suffixdir, style)
    street_bits: List[str] = []
    if not style.house_after_street_name:
        street_bits.append(record.house_number)
    if predir:
        street_bits.append(predir)
    street_bits.append(record.street_name)
    if style.house_after_street_name:
        street_bits.append(record.house_number)
    if street_type:
        street_bits.append(street_type)
    if suffixdir:
        street_bits.append(suffixdir)
    street_part = " ".join(bit for bit in street_bits if bit)

    unit_part = ""
    if style.include_unit and record.unit_type and record.unit_value:
        if style.slash_unit and record.unit_type == "APT":
            unit_part = f"#{record.unit_value}"
        else:
            unit_part = f"{render_component_unit_type(record.unit_type, style)} {record.unit_value}"

    locality_bits = [record.city, record.state]
    if style.include_zip:
        locality_bits.append(record.zip_code)
    locality_part = " ".join(bit for bit in locality_bits if bit)

    if style.component_order:
        component_values = {
            "house": record.house_number,
            "predir": predir,
            "street_name": record.street_name,
            "street_type": street_type,
            "suffixdir": suffixdir,
            "unit": unit_part,
            "city": record.city,
            "state": record.state,
            "zip": record.zip_code if style.include_zip else "",
        }
        ordered_parts = [component_values[key] for key in style.component_order if component_values.get(key)]
        included = set(style.component_order)
        ordered_parts.extend(value for key, value in component_values.items() if key not in included and value)
        address = " ".join(ordered_parts)
    else:
        if style.unit_first and unit_part:
            pieces.extend([unit_part, street_part])
        else:
            pieces.append(street_part)
            if unit_part:
                pieces.append(unit_part)

        if style.include_commas:
            address = f"{', '.join(pieces)}, {locality_part}"
        else:
            address = f"{' '.join(pieces)} {locality_part}"

    if style.extra_spaces:
        address = address.replace(",", " , ")
        address = "  ".join(address.split(" "))

    address = normalize_spaces(address)

    if style.uppercase:
        address = address.upper()
    elif style.lowercase:
        address = address.lower()
    else:
        address = titleish(address)

    return normalize_spaces(address)


def canonical_address(record: AddressRecord) -> str:
    return render_address(
        record,
        RenderStyle(
            uppercase=True,
            use_full_directional=False,
            use_full_street_type=False,
            use_full_unit_type=False,
            include_unit=True,
            include_zip=True,
            include_commas=True,
            unit_first=False,
            slash_unit=False,
        ),
    )


# ---------------------------------------------------------------------------
# Address uniqueness tracking
# ---------------------------------------------------------------------------

class AddressFactory:
    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self._seen_canonical: set[str] = set()


# ---------------------------------------------------------------------------
# Corruption engine
# ---------------------------------------------------------------------------

Operation = Callable[[AddressRecord, RenderStyle, random.Random], Optional[str]]


def op_expand_street_type(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.use_full_street_type = True
    return "expand_street_type"


def op_expand_directionals(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.predir and not record.suffixdir:
        return None
    style.use_full_directional = True
    return "expand_directional"


def op_expand_unit_type(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_type:
        return None
    style.use_full_unit_type = True
    return "expand_unit_type"


def op_unit_hash(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if record.unit_type != "APT" or not record.unit_value:
        return None
    style.slash_unit = True
    return "hash_unit"


def op_unit_first(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_type:
        return None
    style.unit_first = True
    return "unit_first"


def op_remove_zip(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.include_zip = False
    return "drop_zip"


def op_remove_unit(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_type:
        return None
    style.include_unit = False
    return "drop_unit"


def op_remove_commas(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.include_commas = False
    return "remove_commas"


def op_lowercase(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.lowercase = True
    style.uppercase = False
    return "lowercase"


def op_extra_spaces(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    style.extra_spaces = True
    return "extra_spaces"


def op_typo_street(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [i for i, tok in enumerate(tokens) if len(tok) >= 4]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    tokens[idx] = mutate_alpha_token(tokens[idx], rng)
    record.street_name = " ".join(tokens)
    return "street_typo"


def op_phonetic_street(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [i for i, tok in enumerate(tokens) if len(tok) >= 4]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    mutated = mutate_phonetic_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.street_name = " ".join(tokens)
    return "street_phonetic"


def op_typo_city(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 4:
        return None
    tokens = record.city.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    tokens[idx] = mutate_locality_token(tokens[idx], rng)
    record.city = " ".join(tokens)
    return "city_typo"


def op_heavy_city_typo_no_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 5:
        return None
    original = record.city
    for _ in range(rng.choice([2, 2, 3])):
        op_typo_city(record, style, rng)
    if record.city == original:
        return None
    record.state = ""
    if rng.random() < 0.65:
        record.zip_code = ""
    style.include_commas = False
    return "heavy_city_typo_no_state"


def op_heavy_city_typo_with_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 5 or not record.state:
        return None
    original = record.city
    for _ in range(rng.choice([2, 2, 3])):
        op_typo_city(record, style, rng)
    if record.city == original:
        return None
    if rng.random() < 0.70:
        record.zip_code = ""
    style.include_commas = False
    return "heavy_city_typo_with_state"


def op_house_after_street_name(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.house_number or not record.street_name:
        return None
    style.house_after_street_name = True
    style.include_commas = False
    return "house_after_street_name"


def op_phonetic_city(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.city.replace(" ", "")) < 4:
        return None
    tokens = record.city.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    mutated = mutate_phonetic_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.city = " ".join(tokens)
    return "city_phonetic"


def op_wrong_city(
    record: AddressRecord,
    style: RenderStyle,
    rng: random.Random,
    state_to_cities: Optional[Dict[str, Sequence[str]]] = None,
) -> Optional[str]:
    city_index = state_to_cities or {}
    alternatives = [city for city in city_index.get(record.state, ()) if city != record.city]
    if not alternatives:
        return None
    record.city = rng.choice(alternatives)
    return "wrong_city_same_state" if len(city_index.get(record.state, ())) > 1 else "wrong_city"


def op_typo_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    state_name = STATE_ABBREV_TO_NAME.get(record.state, record.state)
    if len(state_name.replace(" ", "")) < 4:
        return None
    tokens = state_name.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    tokens[idx] = mutate_locality_token(tokens[idx], rng)
    record.state = " ".join(tokens)
    return "state_typo"


def op_common_state_typo(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if record.state != "MS":
        return op_typo_state(record, style, rng)
    record.state = rng.choice(
        [
            "Missppi",
            "Missippi",
            "Mississipi",
            "Mississppi",
            "Misissippi",
            "Misisippi",
        ]
    )
    return "common_state_typo"


def op_phonetic_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    state_name = STATE_ABBREV_TO_NAME.get(record.state, record.state)
    if len(state_name.replace(" ", "")) < 4:
        return None
    tokens = state_name.split()
    idx = rng.randrange(len(tokens))
    if len(tokens[idx]) < 4:
        return None
    mutated = mutate_phonetic_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.state = " ".join(tokens)
    return "state_phonetic"


def op_drop_city(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.city:
        return None
    record.city = ""
    return "drop_city"


def op_drop_state(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.state:
        return None
    record.state = ""
    return "drop_state"


def op_zip_truncate(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if len(record.zip_code) != 5:
        return None
    record.zip_code = record.zip_code[:4]
    return "zip_truncate"


def op_street_type_confusion(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    alternatives = STREET_TYPE_CONFUSIONS.get(record.street_type, ())
    if not alternatives:
        return None
    record.street_type = rng.choice(list(alternatives))
    return "street_type_confusion"


def op_typo_street_type(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.street_type:
        return None
    mutated = mutate_street_type_token(record.street_type, rng)
    if mutated == record.street_type:
        return None
    record.street_type = mutated
    return "street_type_typo"


def op_extra_or_missing_street_letter(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [idx for idx, token in enumerate(tokens) if len(token) >= 5]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    mutated = mutate_extra_or_missing_letter(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.street_name = " ".join(tokens)
    return "street_letter_extra_missing"


def op_compound_local_typo(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tags: List[str] = []
    for op in (op_typo_street, op_typo_street_type, op_typo_city):
        tag = op(record, style, rng)
        if tag:
            tags.append(tag)

    if len(tags) < 2:
        return None

    if rng.random() < 0.65:
        style.include_zip = False
    if rng.random() < 0.50:
        style.include_commas = False
    if rng.random() < 0.35:
        style.lowercase = True
        style.uppercase = False

    return "|".join(tags)


def op_reordered_locality_typo(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tags: List[str] = []
    for op in (op_house_after_street_name, op_typo_street_type, op_heavy_city_typo_with_state, op_common_state_typo):
        tag = op(record, style, rng)
        if tag:
            tags.append(tag)
    if rng.random() < 0.45:
        tag = op_house_drop_digit(record, style, rng)
        if tag:
            tags.append(tag)

    if len(tags) < 3:
        return None

    if rng.random() < 0.55:
        style.lowercase = True
        style.uppercase = False
    style.include_zip = False
    style.include_commas = False
    return "|".join(tags)


def op_scrambled_component_order(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.house_number or not record.street_name or not record.city or not record.state:
        return None
    tags: List[str] = []
    for op in (op_typo_street_type, op_heavy_city_typo_with_state, op_common_state_typo):
        tag = op(record, style, rng)
        if tag:
            tags.append(tag)
    if rng.random() < 0.45:
        tag = op_extra_or_missing_street_letter(record, style, rng)
        if tag:
            tags.append(tag)
    if rng.random() < 0.30:
        tag = op_house_drop_digit(record, style, rng)
        if tag:
            tags.append(tag)

    style.component_order = rng.choice(
        (
            ("city", "state", "street_name", "street_type", "house", "zip"),
            ("state", "house", "city", "street_name", "street_type", "zip"),
            ("street_type", "street_name", "house", "city", "state", "zip"),
            ("street_name", "city", "house", "street_type", "state", "zip"),
            ("city", "street_name", "street_type", "house", "state", "zip"),
            ("street_name", "street_type", "city", "house", "state", "zip"),
        )
    )
    style.include_zip = rng.random() < 0.35
    style.include_commas = False
    if rng.random() < 0.60:
        style.lowercase = True
        style.uppercase = False
    tags.append("component_order_shuffle")
    return "|".join(tags)


def op_ocr_street(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    tokens = record.street_name.split()
    candidates = [idx for idx, token in enumerate(tokens) if len(token) >= 4]
    if not candidates:
        return None
    idx = rng.choice(candidates)
    mutated = mutate_ocr_token(tokens[idx], rng)
    if mutated == tokens[idx]:
        return None
    tokens[idx] = mutated
    record.street_name = " ".join(tokens)
    return "street_ocr"


def op_ocr_locality(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    choices: List[Tuple[str, str]] = []
    if record.city:
        choices.append(("city", record.city))
    if record.state:
        choices.append(("state", STATE_ABBREV_TO_NAME.get(record.state, record.state)))
    if not choices:
        return None
    field, value = rng.choice(choices)
    mutated = mutate_ocr_token(value, rng)
    if mutated == value:
        return None
    if field == "city":
        record.city = mutated
        return "city_ocr"
    record.state = mutated
    return "state_ocr"


def op_merge_tokens(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    candidates = []
    if " " in record.street_name:
        candidates.append("street")
    if " " in record.city:
        candidates.append("city")
    if not candidates:
        return None
    field = rng.choice(candidates)
    if field == "street":
        record.street_name = record.street_name.replace(" ", "")
        return "merge_street_tokens"
    record.city = record.city.replace(" ", "")
    return "merge_city_tokens"


def op_house_digit_error(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    record.house_number = mutate_numeric_token(record.house_number, rng)
    return "house_digit_error"


def op_house_drop_digit(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    digit_positions = [idx for idx, char in enumerate(record.house_number) if char.isdigit()]
    if len(digit_positions) < 2:
        return None
    idx = rng.choice(digit_positions)
    record.house_number = record.house_number[:idx] + record.house_number[idx + 1:]
    return "house_drop_digit"


def op_house_near_miss(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    shifted = shift_house_number(record.house_number, rng)
    if shifted == record.house_number:
        return None
    record.house_number = shifted
    return "house_near_miss"


def op_zip_digit_error(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    record.zip_code = mutate_numeric_token(record.zip_code, rng)
    return "zip_digit_error"


def op_drop_directional(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.predir and not record.suffixdir:
        return None
    if record.predir and record.suffixdir:
        if rng.random() < 0.5:
            record.predir = ""
        else:
            record.suffixdir = ""
    elif record.predir:
        record.predir = ""
    else:
        record.suffixdir = ""
    return "drop_directional"


def op_strip_leading_zero_unit(record: AddressRecord, style: RenderStyle, rng: random.Random) -> Optional[str]:
    if not record.unit_value or not record.unit_value[0].isdigit():
        return None
    record.unit_value = record.unit_value.lstrip("0") or record.unit_value
    return "strip_unit_zero"


EASY_OPS: Sequence[Operation] = (
    op_expand_street_type,
    op_expand_directionals,
    op_expand_unit_type,
    op_unit_hash,
    op_unit_first,
    op_remove_zip,
    op_remove_unit,
    op_remove_commas,
    op_lowercase,
    op_extra_spaces,
    op_merge_tokens,
)

MEDIUM_OPS: Sequence[Operation] = EASY_OPS + (
    op_typo_street,
    op_phonetic_street,
    op_extra_or_missing_street_letter,
    op_house_after_street_name,
    op_typo_city,
    op_heavy_city_typo_no_state,
    op_heavy_city_typo_with_state,
    op_phonetic_city,
    op_ocr_street,
    op_ocr_locality,
    op_wrong_city,
    op_typo_state,
    op_common_state_typo,
    op_phonetic_state,
    op_drop_city,
    op_drop_state,
    op_zip_truncate,
    op_street_type_confusion,
    op_typo_street_type,
    op_drop_directional,
    op_strip_leading_zero_unit,
)

HARD_OPS: Sequence[Operation] = MEDIUM_OPS + (
    op_compound_local_typo,
    op_reordered_locality_typo,
    op_scrambled_component_order,
    op_house_near_miss,
    op_house_drop_digit,
    op_house_digit_error,
    op_zip_digit_error,
)

LOCALITY_OPS: Sequence[Operation] = (
    op_typo_city,
    op_phonetic_city,
    op_heavy_city_typo_with_state,
    op_ocr_locality,
    op_wrong_city,
    op_typo_state,
    op_common_state_typo,
    op_phonetic_state,
    op_drop_city,
    op_drop_state,
)


class Corruptor:
    def __init__(self, rng: random.Random, state_to_cities: Optional[Dict[str, Sequence[str]]] = None) -> None:
        self.rng = rng
        self.state_to_cities = state_to_cities or {}

    def apply_operation(self, op: Operation, record: AddressRecord, style: RenderStyle) -> Optional[str]:
        if op is op_wrong_city:
            return op_wrong_city(record, style, self.rng, self.state_to_cities)
        return op(record, style, self.rng)

    def choose_difficulty(self) -> str:
        return choose_weighted(self.rng, [("easy", 0.35), ("medium", 0.45), ("hard", 0.20)])

    def difficulty_operation_count(self, difficulty: str) -> int:
        if difficulty == "easy":
            return self.rng.randint(1, 2)
        if difficulty == "medium":
            return self.rng.randint(2, 4)
        return self.rng.randint(3, 6)

    def ops_for_difficulty(self, difficulty: str) -> Sequence[Operation]:
        if difficulty == "easy":
            return EASY_OPS
        if difficulty == "medium":
            return MEDIUM_OPS
        return HARD_OPS

    def corrupt(self, base_record: AddressRecord, force_difficulty: Optional[str] = None) -> Tuple[str, str, str]:
        canonical = canonical_address(base_record)
        difficulty = force_difficulty or self.choose_difficulty()

        for _ in range(20):
            record = deepcopy(base_record)
            style = RenderStyle()
            tags: List[str] = []
            pool = list(self.ops_for_difficulty(difficulty))
            self.rng.shuffle(pool)
            num_ops = min(self.difficulty_operation_count(difficulty), len(pool))

            profile_probability = 0.72 if difficulty == "hard" else 0.38 if difficulty == "medium" else 0.0
            if profile_probability and self.rng.random() < profile_probability:
                profile_ops = [
                    op_heavy_city_typo_no_state,
                    op_heavy_city_typo_with_state,
                    op_reordered_locality_typo,
                    op_scrambled_component_order,
                    op_phonetic_street,
                    op_typo_street_type,
                    op_extra_or_missing_street_letter,
                    op_house_near_miss,
                ]
                self.rng.shuffle(profile_ops)
                target_count = 2 if difficulty == "hard" and self.rng.random() < 0.55 else 1
                for op in profile_ops:
                    tag = self.apply_operation(op, record, style)
                    if tag:
                        tags.append(tag)
                    if len(tags) >= target_count:
                        break

            compound_probability = 0.45 if difficulty == "hard" else 0.18 if difficulty == "medium" else 0.0
            if compound_probability and self.rng.random() < compound_probability:
                tag = self.apply_operation(op_compound_local_typo, record, style)
                if tag:
                    tags.append(tag)

            for op in pool[:num_ops]:
                tag = self.apply_operation(op, record, style)
                if tag:
                    tags.append(tag)

            # Ensure medium/hard examples regularly include locality corruption.
            tag_parts = {part for tag in tags for part in tag.split("|")}
            if difficulty in {"medium", "hard"} and not any(
                tag in {
                    "city_typo",
                    "city_phonetic",
                    "city_ocr",
                    "wrong_city",
                    "wrong_city_same_state",
                    "state_typo",
                    "state_phonetic",
                    "state_ocr",
                    "drop_city",
                    "drop_state",
                }
                for tag in tag_parts
            ):
                locality_pool = list(LOCALITY_OPS)
                self.rng.shuffle(locality_pool)
                for op in locality_pool:
                    tag = self.apply_operation(op, record, style)
                    if tag:
                        tags.append(tag)
                        break

            # Small chance of title-case even when no explicit casing op chosen.
            if not style.uppercase and not style.lowercase and self.rng.random() < 0.25:
                style.lowercase = False

            query = render_address(record, style)
            if query != canonical:
                return query, difficulty, "|".join(tags) if tags else "style_variation"

        # Guaranteed fallback difference.
        fallback = render_address(base_record, RenderStyle(lowercase=True, include_commas=False))
        return fallback, difficulty, "fallback_lowercase"


# ---------------------------------------------------------------------------
# Dataset builder
# ---------------------------------------------------------------------------
