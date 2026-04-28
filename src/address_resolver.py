#!/usr/bin/env python3
"""Resolve messy addresses against a canonical real-address reference set.

Compatibility entrypoint for the address resolver CLI and public imports.
The large helper sections live in resolver_models, resolver_parsing,
resolver_reference, and resolver_stage2.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, Hashable, List, Optional, Sequence, Tuple

from address_dataset_generator import DIRECTION_TO_FULL, STREET_TYPE_CONFUSIONS, STREET_TYPE_TO_FULL
from resolver_models import *  # noqa: F401,F403 - preserve public imports from address_resolver
from resolver_parsing import *  # noqa: F401,F403 - preserve public imports from address_resolver
from resolver_reference import *  # noqa: F401,F403 - preserve public imports from address_resolver
from resolver_stage2 import *  # noqa: F401,F403 - preserve public imports from address_resolver


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
        self.by_city_street_token = defaultdict(set)
        self.by_city_street_phonetic = defaultdict(set)
        self.by_zip_prefix_street_token = defaultdict(set)
        self.by_zip_prefix_street_phonetic = defaultdict(set)
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
                self.by_city_street_token[(row.city, row.state, token)].add(row.address_id)
                self.by_zip_prefix_street_token[(row.zip_code[:3], token)].add(row.address_id)
                phonetic = soundex_token(token)
                if phonetic:
                    self.by_city_street_phonetic[(row.city, row.state, phonetic)].add(row.address_id)
                    self.by_zip_prefix_street_phonetic[(row.zip_code[:3], phonetic)].add(row.address_id)
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
            cached = self.apply_city_only_street_token(cached)
            cached = self.apply_rare_city_typo_correction(cached)
            cached = self.apply_split_city_suffix_correction(cached)
            cached = self.apply_fuzzy_city_anywhere(cached)
            cached = self.apply_fuzzy_city_suffix(cached)
            cached = self.apply_contextual_city_reassignment(cached)
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

    def prefix_city_choice(self, candidate: str, city_choices: Sequence[str], state: str = "") -> Tuple[str, float, float]:
        if len(candidate) < 5 or " " in candidate:
            return "", 0.0, 0.0
        matches = [city for city in city_choices if len(city) > len(candidate) and city.startswith(candidate)]
        if not matches:
            return "", 0.0, 0.0

        def city_count(city: str) -> int:
            if state:
                return self.city_counts_by_state[state][city]
            return sum(state_counts[city] for state_counts in self.city_counts_by_state.values())

        scored = sorted(((city_count(city), city) for city in matches), reverse=True)
        best_count, best_city = scored[0]
        second_count = scored[1][0] if len(scored) > 1 else 0
        if len(scored) > 1 and best_count < max(20, second_count * 2):
            return "", 0.0, 0.0
        score = min(0.95, 0.86 + 0.02 * min(4, len(candidate) - 4))
        second_score = min(score - 0.10, sequence_similarity(candidate, scored[1][1])) if len(scored) > 1 else 0.0
        return best_city, score, max(0.0, second_score)

    def city_count(self, city: str, state: str = "") -> int:
        if state:
            return self.city_counts_by_state[state][city]
        return sum(state_counts[city] for state_counts in self.city_counts_by_state.values())

    def closest_city_choice(
        self,
        candidate: str,
        city_choices: Sequence[str],
        state: str = "",
        minimum_score: float = 0.0,
        exclude: str = "",
    ) -> Tuple[str, float, float]:
        if not candidate or not city_choices:
            return "", 0.0, 0.0
        scored = sorted(
            (
                (city, sequence_similarity(candidate, city), self.city_count(city, state))
                for city in city_choices
                if city != exclude
            ),
            key=lambda item: (item[1], item[2]),
            reverse=True,
        )
        if not scored:
            return "", 0.0, 0.0
        top_score = scored[0][1]
        best_city, best_score, best_count = max(
            (item for item in scored if top_score - item[1] <= 0.02),
            key=lambda item: (item[2], item[1], item[0]),
        )
        second_score = max((score for city, score, _count in scored if city != best_city), default=0.0)
        second_count = max((count for city, score, count in scored if city != best_city and score == second_score), default=0)
        if second_score and best_count >= max(20, second_count * 10):
            second_score = min(second_score, best_score - 0.08)
        if best_score < minimum_score:
            return "", best_score, second_score
        return best_city, best_score, second_score

    def street_tokens_for_reparse(self, parsed: ParsedAddress) -> List[str]:
        tokens: List[str] = []
        if parsed.predir:
            tokens.append(parsed.predir)
        if parsed.street_name:
            tokens.extend(parsed.street_name.split())
        if parsed.street_type:
            tokens.append(self.street_type_name_token(parsed.street_type))
        if parsed.suffixdir:
            tokens.append(parsed.suffixdir)
        return tokens

    def street_type_name_token(self, street_type: str) -> str:
        if street_type == "VW":
            return "VIEW"
        return STREET_TYPE_TO_FULL.get(street_type, street_type)

    def apply_city_only_street_token(self, parsed: ParsedAddress) -> ParsedAddress:
        if (
            parsed.city
            or not parsed.house_number
            or not parsed.street_name
            or parsed.street_type
            or parsed.predir
            or parsed.suffixdir
        ):
            return parsed

        candidate = parsed.street_name
        if len(candidate.split()) > 3:
            return parsed

        city_choices, allow_global_city_match = self.fuzzy_city_choices(parsed)
        if not city_choices:
            return parsed

        minimum_score = 0.80 if allow_global_city_match else 0.78
        city_match, best_score, second_score = self.prefix_city_choice(candidate, city_choices, parsed.state)
        if not city_match:
            city_match, best_score, second_score = self.closest_city_choice(
                candidate,
                city_choices,
                parsed.state,
                minimum_score=minimum_score,
            )
        if not city_match or (best_score - second_score < 0.06 and best_score < 0.84):
            return parsed

        state = parsed.state
        if allow_global_city_match and not state:
            states = {candidate_state for candidate_state in self.states_by_city.get(city_match, ()) if candidate_state}
            if len(states) == 1:
                state = next(iter(states))

        return rebuild_parsed(parsed, street_name="", city=city_match, state=state)

    def apply_rare_city_typo_correction(self, parsed: ParsedAddress) -> ParsedAddress:
        if not parsed.city or not parsed.state or parsed.state not in self.cities_by_state:
            return parsed

        source_count = self.city_counts_by_state[parsed.state][parsed.city]
        if source_count > 5:
            return parsed

        city_choices = tuple(city for city in self.cities_by_state[parsed.state] if city != parsed.city)
        city_match, best_score, second_score = self.closest_city_choice(
            parsed.city,
            city_choices,
            parsed.state,
            minimum_score=0.84,
        )
        if not city_match:
            return parsed

        alt_count = self.city_counts_by_state[parsed.state][city_match]
        count_ratio_ok = alt_count >= max(20, source_count * 10)
        margin_ok = best_score - second_score >= 0.03 or best_score >= 0.92
        if not count_ratio_ok or not margin_ok:
            return parsed

        return rebuild_parsed(parsed, city=city_match)

    def apply_split_city_suffix_correction(self, parsed: ParsedAddress) -> ParsedAddress:
        if not parsed.city or not parsed.state or not parsed.street_name or parsed.state not in self.cities_by_state:
            return parsed

        street_tokens = parsed.street_name.split()
        if len(street_tokens) < 2:
            return parsed

        trailing_token = street_tokens[-1]
        if looks_like_street_descriptor(trailing_token):
            return parsed

        candidate = f"{parsed.city} {trailing_token}"
        city_choices = tuple(city for city in self.cities_by_state[parsed.state] if city != parsed.city)
        city_match, best_score, second_score = self.closest_city_choice(
            candidate,
            city_choices,
            parsed.state,
            minimum_score=0.84,
        )
        if not city_match:
            return parsed

        current_count = self.city_counts_by_state[parsed.state][parsed.city]
        alt_count = self.city_counts_by_state[parsed.state][city_match]
        count_ratio_ok = alt_count >= max(20, current_count * 2)
        margin_ok = best_score - second_score >= 0.03 or best_score >= 0.92
        if not count_ratio_ok or not margin_ok:
            return parsed

        return rebuild_parsed(parsed, street_name=" ".join(street_tokens[:-1]), city=city_match)

    def city_street_context_score(
        self,
        parsed: ParsedAddress,
        city: str,
        state: str,
        predir: str,
        street_name: str,
        street_type: str,
        suffixdir: str,
    ) -> float:
        if not parsed.house_number or not city or not street_name:
            return 0.0

        candidate_ids: List[str] = []
        if state:
            candidate_ids.extend(self.by_house_city_state.get((parsed.house_number, city, state), ()))
        if parsed.zip_code:
            candidate_ids.extend(self.by_house_zip.get((parsed.house_number, parsed.zip_code), ()))
        if not candidate_ids:
            candidate_ids.extend(self.by_house.get(parsed.house_number, ()))

        seen = set()
        best_score = 0.0
        query_core = " ".join(bit for bit in [predir, street_name, suffixdir] if bit)
        checked = 0
        scanned = 0
        for candidate_id in candidate_ids:
            if candidate_id in seen:
                continue
            seen.add(candidate_id)
            scanned += 1
            candidate = self.reference_by_id[candidate_id]
            if candidate.city != city:
                if scanned >= 5000:
                    break
                continue
            if state and candidate.state != state:
                if scanned >= 5000:
                    break
                continue
            if parsed.zip_code and candidate.zip_code and candidate.zip_code[:3] != parsed.zip_code[:3]:
                if scanned >= 5000:
                    break
                continue

            candidate_core = " ".join(bit for bit in [candidate.predir, candidate.street_name, candidate.suffixdir] if bit)
            street_similarity = max(
                cheap_similarity(street_name, candidate.street_name),
                sequence_similarity(street_name, candidate.street_name),
                token_overlap(query_core, candidate_core),
            )
            type_score = 1.0 if not street_type or street_type == candidate.street_type else 0.0
            best_score = max(best_score, 0.88 * street_similarity + 0.12 * type_score)
            checked += 1
            if checked >= 1500 or scanned >= 5000 or (checked >= 250 and best_score >= 0.90):
                break
        return best_score

    def apply_fuzzy_city_anywhere(self, parsed: ParsedAddress) -> ParsedAddress:
        if parsed.city or not parsed.street_name:
            return parsed

        street_tokens = self.street_tokens_for_reparse(parsed)
        if len(street_tokens) < 2:
            return parsed

        city_choices, allow_global_city_match = self.fuzzy_city_choices(parsed)
        if not city_choices:
            return parsed

        best: Tuple[int, float, float, int, int, str] = (0, 0.0, 0.0, 0, 0, "")
        best_start = -1
        best_width = 0
        max_width = min(3, len(street_tokens) - 1)
        for width in range(max_width, 0, -1):
            for start in range(0, len(street_tokens) - width + 1):
                candidate_tokens = street_tokens[start:start + width]
                if all(looks_like_street_descriptor(token) for token in candidate_tokens):
                    continue
                if width > 1:
                    trailing_tokens = candidate_tokens[1:]
                    if any(
                        token in STREET_TYPE_TYPO_ALIASES
                        or token in CONTEXTUAL_STREET_TYPE_TYPO_ALIASES
                        or (canonical_street_type_token(token, allow_typos=False) and token != "ST")
                        for token in trailing_tokens
                    ):
                        continue
                candidate = " ".join(candidate_tokens)
                if allow_global_city_match:
                    minimum_score = 0.78 if width == 1 else 0.76
                elif parsed.house_number and parsed.state:
                    minimum_score = 0.62 if width == 1 else 0.60
                else:
                    minimum_score = 0.80 if width == 1 else 0.76
                city_match, best_score, second_score = self.prefix_city_choice(candidate, city_choices, parsed.state)
                if not city_match:
                    city_match, best_score, second_score = self.closest_city_choice(
                        candidate,
                        city_choices,
                        parsed.state,
                        minimum_score=minimum_score,
                    )
                required_margin = 0.08 if allow_global_city_match else 0.05
                if not city_match or (best_score - second_score < required_margin and best_score < 0.90):
                    continue

                remaining_tokens = street_tokens[:start] + street_tokens[start + width:]
                predir, street_name, street_type, suffixdir = parse_street_tokens(remaining_tokens, allow_contextual_type=True)
                if not street_name:
                    continue

                state = parsed.state
                if allow_global_city_match and not state:
                    states = {candidate_state for candidate_state in self.states_by_city.get(city_match, ()) if candidate_state}
                    if len(states) == 1:
                        state = next(iter(states))

                context_score = self.city_street_context_score(
                    parsed,
                    city_match,
                    state,
                    predir,
                    street_name,
                    street_type,
                    suffixdir,
                )
                context_bucket = 1 if context_score >= 0.72 else 0
                position_score = 1 if start in {0, len(street_tokens) - width} else 0
                candidate_score = (
                    context_bucket,
                    best_score + 0.35 * context_score,
                    best_score,
                    position_score,
                    width,
                    city_match,
                )
                if candidate_score > best:
                    best = candidate_score
                    best_start = start
                    best_width = width

        if not best[5]:
            return parsed
        if allow_global_city_match and best[0] == 0 and best[2] < 0.86:
            return parsed

        remaining_tokens = street_tokens[:best_start] + street_tokens[best_start + best_width:]
        predir, street_name, street_type, suffixdir = parse_street_tokens(remaining_tokens, allow_contextual_type=True)
        if not street_name:
            return parsed

        state = parsed.state
        if allow_global_city_match and not state:
            states = {candidate_state for candidate_state in self.states_by_city.get(best[5], ()) if candidate_state}
            if len(states) == 1:
                state = next(iter(states))

        return rebuild_parsed(
            parsed,
            predir=predir,
            street_name=street_name,
            street_type=street_type,
            suffixdir=suffixdir,
            city=best[5],
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
                strong_street_context = bool(parsed.house_number and (parsed.street_type or len(street_tokens) >= 3))
                minimum_score = 0.80 if strong_street_context and width == 1 else 0.86 if width == 1 else 0.82
            elif street_context:
                minimum_score = 0.62 if width == 1 else 0.60
            else:
                minimum_score = 0.80 if width == 1 else 0.76
            city_match, best_score, second_score = self.prefix_city_choice(candidate, city_choices, parsed.state)
            if not city_match:
                city_match, best_score, second_score = self.closest_city_choice(
                    candidate,
                    city_choices,
                    parsed.state,
                    minimum_score=minimum_score,
                )
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

    def apply_contextual_city_reassignment(self, parsed: ParsedAddress) -> ParsedAddress:
        if not parsed.city or not parsed.house_number or not parsed.street_name:
            return parsed

        street_tokens = self.street_tokens_for_reparse(parsed)
        if len(street_tokens) < 2:
            return parsed

        city_choices, allow_global_city_match = self.fuzzy_city_choices(parsed)
        if not city_choices:
            return parsed

        current_state = parsed.state
        if not current_state:
            states = {state for state in self.states_by_city.get(parsed.city, ()) if state}
            if len(states) == 1:
                current_state = next(iter(states))
        current_context = self.city_street_context_score(
            parsed,
            parsed.city,
            current_state,
            parsed.predir,
            parsed.street_name,
            parsed.street_type,
            parsed.suffixdir,
        )

        best: Tuple[float, float, str, str, str, str, str, str] = (0.0, 0.0, "", "", "", "", "", "")
        max_width = min(3, len(street_tokens) - 1)
        for width in range(max_width, 0, -1):
            for start in range(0, len(street_tokens) - width + 1):
                candidate_tokens = street_tokens[start:start + width]
                if all(looks_like_street_descriptor(token) for token in candidate_tokens):
                    continue
                if width > 1:
                    trailing_tokens = candidate_tokens[1:]
                    if any(
                        token in STREET_TYPE_TYPO_ALIASES
                        or token in CONTEXTUAL_STREET_TYPE_TYPO_ALIASES
                        or (canonical_street_type_token(token, allow_typos=False) and token != "ST")
                        for token in trailing_tokens
                    ):
                        continue
                candidate = " ".join(candidate_tokens)
                minimum_score = 0.78 if allow_global_city_match else 0.62
                city_match, best_score, second_score = self.prefix_city_choice(candidate, city_choices, parsed.state)
                if not city_match:
                    city_match, best_score, second_score = self.closest_city_choice(
                        candidate,
                        city_choices,
                        parsed.state,
                        minimum_score=minimum_score,
                    )
                if not city_match or city_match == parsed.city:
                    continue
                if best_score - second_score < 0.04 and best_score < 0.90:
                    continue

                state = parsed.state
                if not state:
                    states = {candidate_state for candidate_state in self.states_by_city.get(city_match, ()) if candidate_state}
                    if len(states) == 1:
                        state = next(iter(states))
                if not state and not parsed.zip_code:
                    continue

                remaining_tokens = parsed.city.split() + street_tokens[:start] + street_tokens[start + width:]
                predir, street_name, street_type, suffixdir = parse_street_tokens(remaining_tokens, allow_contextual_type=True)
                if not street_name:
                    continue

                context_score = self.city_street_context_score(
                    parsed,
                    city_match,
                    state,
                    predir,
                    street_name,
                    street_type,
                    suffixdir,
                )
                if context_score < max(0.66, current_context + 0.18):
                    continue
                candidate_score = (context_score, best_score, city_match, state, predir, street_name, street_type, suffixdir)
                if candidate_score > best:
                    best = candidate_score

        if not best[2]:
            return parsed

        return rebuild_parsed(
            parsed,
            predir=best[4],
            street_name=best[5],
            street_type=best[6],
            suffixdir=best[7],
            city=best[2],
            state=best[3],
        )

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

        city_fuzzy_bases = [parsed, *(variant for reason, variant in variants if reason == "city_state")]
        for base in city_fuzzy_bases:
            if not base.city or base.state not in self.cities_by_state:
                continue
            city_choices = tuple(self.cities_by_state[base.state])
            city_match, best_score, second_score = self.closest_city_choice(
                base.city,
                city_choices,
                base.state,
                minimum_score=0.62,
            )
            if city_match and city_match != base.city:
                corrected = rebuild_parsed(base, city=city_match)
                context_score = self.city_street_context_score(
                    corrected,
                    city_match,
                    corrected.state,
                    corrected.predir,
                    corrected.street_name,
                    corrected.street_type,
                    corrected.suffixdir,
                )
                city_similarity_ok = best_score >= 0.82 and best_score - second_score >= 0.05
                context_ok = best_score >= 0.62 and context_score >= 0.72
                if city_similarity_ok or context_ok:
                    variants.append(("state_city_fuzzy", corrected))
            alternate_choices = tuple(city for city in city_choices if city != base.city)
            if alternate_choices:
                source_count = self.city_counts_by_state[base.state][base.city]
                alt_minimum_score = 0.80 if source_count <= 5 else 0.86
                alt_match, alt_score, alt_second_score = self.closest_city_choice(
                    base.city,
                    alternate_choices,
                    base.state,
                    minimum_score=alt_minimum_score,
                )
                alt_count = self.city_counts_by_state[base.state][alt_match] if alt_match else 0
                count_ratio_ok = source_count <= 3 and alt_count >= max(20, source_count * 10)
                margin_ok = alt_score - alt_second_score >= 0.03
                if alt_match and alt_count > source_count and (margin_ok or count_ratio_ok):
                    corrected = rebuild_parsed(base, city=alt_match)
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
            street_sim = max(
                cheap_similarity(parsed.street_name, candidate.street_name),
                sequence_similarity(parsed.street_name, candidate.street_name),
            )
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
            street_sim = max(
                cheap_similarity(parsed.street_name, best.street_name),
                sequence_similarity(parsed.street_name, best.street_name),
            )
            type_ok = not parsed.street_type or parsed.street_type == best.street_type
            if street_sim >= 0.62 and type_ok and best_score - second_score >= 0.16:
                return best_id
        if best_score >= 0.88 and best_score - second_score >= 0.08:
            return best_id
        return ""

    def stage2_variants(self, parsed: ParsedAddress) -> List[Tuple[str, ParsedAddress]]:
        variants: List[Tuple[str, ParsedAddress]] = [("original", parsed), *self.locality_variants(parsed)]
        variants.extend(self.street_type_as_name_variants(variant) for _reason, variant in list(variants) if self.street_type_as_name_variants(variant))
        variants.extend(self.embedded_unit_variants(variant) for _reason, variant in list(variants) if self.embedded_unit_variants(variant))
        return self.unique_parsed_variants(variants)

    def stage1_variants(self, parsed: ParsedAddress) -> List[Tuple[str, ParsedAddress]]:
        variants: List[Tuple[str, ParsedAddress]] = [("original", parsed), *self.locality_variants(parsed)]
        variants.extend(self.street_type_as_name_variants(variant) for _reason, variant in list(variants) if self.street_type_as_name_variants(variant))
        variants.extend(self.embedded_unit_variants(variant) for _reason, variant in list(variants) if self.embedded_unit_variants(variant))

        if parsed.unit_type or parsed.unit_value:
            variants.append(("drop_unit", rebuild_parsed(parsed, unit_type="", unit_value="")))

        if parsed.street_type:
            variants.append(("drop_type", rebuild_parsed(parsed, street_type="")))
            for candidate_type in STREET_TYPE_CONFUSIONS.get(parsed.street_type, ()):
                variants.append(("type_confusion", rebuild_parsed(parsed, street_type=candidate_type)))

        if parsed.zip_code and len(parsed.zip_code) >= 4:
            variants.append(("zip_prefix", rebuild_parsed(parsed, zip_code=parsed.zip_code[:3])))

        return self.unique_parsed_variants(variants)

    def street_type_as_name_variants(self, parsed: ParsedAddress) -> Tuple[str, ParsedAddress]:
        if parsed.street_type != "VW" or not parsed.street_name:
            return ()
        return (
            "type_as_name",
            rebuild_parsed(
                parsed,
                street_name=f"{parsed.street_name} {self.street_type_name_token(parsed.street_type)}",
                street_type="",
            ),
        )

    def embedded_unit_variants(self, parsed: ParsedAddress) -> Tuple[str, ParsedAddress]:
        if not parsed.unit_type or not parsed.unit_value:
            return ()
        embedded_bits = [parsed.street_name]
        if parsed.street_type:
            embedded_bits.append(parsed.street_type)
        embedded_bits.extend([parsed.unit_type, parsed.unit_value])
        return (
            "unit_as_street",
            rebuild_parsed(
                parsed,
                street_name=" ".join(bit for bit in embedded_bits if bit),
                street_type="",
                unit_type="",
                unit_value="",
            ),
        )

    def unique_parsed_variants(self, variants: Sequence[Tuple[str, ParsedAddress]]) -> List[Tuple[str, ParsedAddress]]:
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

    def candidate_generation_score(self, parsed: ParsedAddress, candidate: ReferenceAddress) -> float:
        score = self.rough_score(parsed, candidate)
        if not parsed.street_name:
            return score

        street_text_similarity = max(
            cheap_similarity(parsed.street_name, candidate.street_name),
            sequence_similarity(parsed.street_name, candidate.street_name),
            sequence_similarity(parsed.street_signature, candidate.street_signature),
            token_overlap(parsed.street_signature, candidate.street_signature),
        )
        street_evidence = max(
            street_text_similarity,
            0.90 * phonetic_similarity(parsed.street_name, candidate.street_name),
            0.90 * phonetic_similarity(parsed.street_signature, candidate.street_signature),
        )
        city_similarity = cheap_similarity(parsed.city, candidate.city)
        state_exact = 1.0 if parsed.state and parsed.state == candidate.state else 0.0
        zip_exact = 1.0 if parsed.zip_code and parsed.zip_code == candidate.zip_code else 0.0
        zip_prefix = 1.0 if parsed.zip_code[:3] and parsed.zip_code[:3] == candidate.zip_code[:3] else 0.0
        locality_evidence = max(city_similarity, state_exact, zip_exact, zip_prefix)

        if locality_evidence >= 0.86 and street_evidence >= 0.66 and street_text_similarity >= 0.58:
            score = max(
                score,
                0.34
                + 0.30 * street_evidence
                + 0.10 * city_similarity
                + 0.05 * state_exact
                + 0.05 * zip_exact
                + 0.04 * zip_prefix
                + 0.08 * numeric_similarity(parsed.house_number, candidate.house_number),
            )
        elif locality_evidence >= 0.86 and parsed.house_number:
            score = min(
                score,
                0.22
                + 0.35 * street_text_similarity
                + 0.08 * locality_evidence
                + 0.10 * numeric_similarity(parsed.house_number, candidate.house_number),
            )
        return score

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
        has_street_query = any(variant.street_name for _, variant in variants)
        if not has_street_query:
            return []

        for _, variant in variants:
            street_tokens = [token for token in token_set(variant.street_name) if len(token) >= 3]

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
                for token in street_tokens:
                    self.add_blocking_candidates(
                        blocking_scores,
                        self.by_city_street_token.get((variant.city, variant.state, token), ()),
                        10.0,
                    )
                    phonetic = soundex_token(token)
                    if phonetic:
                        self.add_blocking_candidates(
                            blocking_scores,
                            self.by_city_street_phonetic.get((variant.city, variant.state, phonetic), ()),
                            9.0,
                        )
            if variant.state:
                self.add_blocking_candidates(blocking_scores, self.by_state.get(variant.state, []), 1.0, limit=90)
            if variant.house_number:
                self.add_blocking_candidates(blocking_scores, self.by_house.get(variant.house_number, []), 6.0, limit=60)

            if variant.zip_code[:3]:
                for token in street_tokens:
                    self.add_blocking_candidates(
                        blocking_scores,
                        self.by_zip_prefix_street_token.get((variant.zip_code[:3], token), ()),
                        6.0,
                        limit=120,
                    )
                    phonetic = soundex_token(token)
                    if phonetic:
                        self.add_blocking_candidates(
                            blocking_scores,
                            self.by_zip_prefix_street_phonetic.get((variant.zip_code[:3], phonetic), ()),
                            5.0,
                            limit=140,
                        )
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
            if has_street_query:
                prefilter_limit = max(prefilter_limit, candidate_limit * 8, 240)
            blocking_pool = [candidate_id for candidate_id, _ in prioritized[: prefilter_limit]]

        scored = []
        for candidate_id in blocking_pool:
            candidate = self.reference_by_id[candidate_id]
            best = max(self.candidate_generation_score(variant, candidate) for _, variant in variants)
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
            street_name_similarity = max(
                sequence_similarity(variant.street_name, candidate.street_name),
                0.95 * sequence_similarity(variant.street_signature, candidate.street_signature),
            )
            street_signature_overlap = token_overlap(variant.street_signature, candidate.street_signature)
            city_similarity = sequence_similarity(variant.city, candidate.city)
            house_similarity = numeric_similarity(variant.house_number, candidate.house_number)
            street_phonetic_similarity = max(
                phonetic_similarity(variant.street_name, candidate.street_name),
                phonetic_similarity(variant.street_signature, candidate.street_signature),
            )
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
