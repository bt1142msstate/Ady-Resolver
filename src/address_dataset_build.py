#!/usr/bin/env python3
"""Dataset assembly, validation, export, and high-level generation."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections import Counter, defaultdict
from copy import deepcopy
from dataclasses import asdict, dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from address_noise import AddressFactory, Corruptor, canonical_address, choose_weighted
from address_source_loading import AddressRecord, query_text_key


@dataclass
class QueryExample:
    query_id: str
    split: str
    label: int
    true_match_id: str
    base_address_id: str
    source: str
    difficulty: str
    noise_tags: str
    query_address: str
    canonical_address: str


def stable_split_for_key(key: str) -> str:
    """Deterministic split using a cryptographic hash for better distribution."""
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "big") / float(2 ** (8 * len(digest)))
    if value < 0.70:
        return "train"
    if value < 0.85:
        return "validation"
    return "test"

class DatasetBuilder:
    def __init__(self, seed: int, real_address_pool: Optional[Sequence[AddressRecord]] = None) -> None:
        self.rng = random.Random(seed)
        self.factory = AddressFactory(self.rng)
        self.real_address_pool = list(real_address_pool or [])
        self.corruptor = Corruptor(self.rng, state_to_cities=self.real_state_to_cities())
        self._real_pool_order = list(self.real_address_pool)
        self.rng.shuffle(self._real_pool_order)
        self.adversarial_reasons: Dict[str, str] = {}

    def real_state_to_cities(self) -> Dict[str, Sequence[str]]:
        cities_by_state: Dict[str, set[str]] = defaultdict(set)
        for record in self.real_address_pool:
            if record.state and record.city:
                cities_by_state[record.state].add(record.city)
        return {state: tuple(sorted(cities)) for state, cities in cities_by_state.items()}

    def build_reference(self, n: int) -> List[AddressRecord]:
        if self.real_address_pool:
            return self.take_real_records(n, "REF")
        raise RuntimeError("No real address pool was supplied for reference generation.")

    def build_negative_bases(self, n: int) -> List[AddressRecord]:
        if self.real_address_pool:
            return self.take_real_records(n, "NEG")
        raise RuntimeError("No real address pool was supplied for negative generation.")

    def adversarial_neighbor_index(self, reference: Sequence[AddressRecord]) -> Dict[str, Dict[Tuple[str, ...], List[AddressRecord]]]:
        indexes: Dict[str, Dict[Tuple[str, ...], List[AddressRecord]]] = {
            "same_house_city": defaultdict(list),
            "same_street_city": defaultdict(list),
            "same_house_zip": defaultdict(list),
            "same_street_zip": defaultdict(list),
        }
        for record in reference:
            street_key = (record.predir, record.street_name, record.street_type, record.suffixdir)
            if record.house_number and record.city and record.state:
                indexes["same_house_city"][(record.house_number, record.city, record.state)].append(record)
            if record.city and record.state and record.street_name:
                indexes["same_street_city"][(*street_key, record.city, record.state)].append(record)
            if record.house_number and record.zip_code:
                indexes["same_house_zip"][(record.house_number, record.zip_code)].append(record)
            if record.zip_code and record.street_name:
                indexes["same_street_zip"][(*street_key, record.zip_code)].append(record)
        return indexes

    def candidate_reference_neighbors(
        self,
        candidate: AddressRecord,
        indexes: Dict[str, Dict[Tuple[str, ...], List[AddressRecord]]],
    ) -> List[Tuple[str, AddressRecord]]:
        neighbors: List[Tuple[str, AddressRecord]] = []
        street_key = (candidate.predir, candidate.street_name, candidate.street_type, candidate.suffixdir)
        lookups = (
            ("same_house_city", (candidate.house_number, candidate.city, candidate.state)),
            ("same_street_city", (*street_key, candidate.city, candidate.state)),
            ("same_house_zip", (candidate.house_number, candidate.zip_code)),
            ("same_street_zip", (*street_key, candidate.zip_code)),
        )
        seen = set()
        for reason, key in lookups:
            if not all(key):
                continue
            for reference in indexes[reason].get(key, ()):
                if reference.address_id in seen:
                    continue
                seen.add(reference.address_id)
                neighbors.append((reason, reference))
        return neighbors

    def adversarial_neighbor_score(self, reference: AddressRecord, candidate: AddressRecord, reason: str) -> float:
        street_similarity = SequenceMatcher(None, reference.street_name.upper(), candidate.street_name.upper()).ratio()
        same_house = reference.house_number == candidate.house_number
        same_city = bool(reference.city and reference.city == candidate.city and reference.state == candidate.state)
        same_zip = bool(reference.zip_code and reference.zip_code == candidate.zip_code)
        same_street = (
            reference.predir == candidate.predir
            and reference.street_name == candidate.street_name
            and reference.street_type == candidate.street_type
            and reference.suffixdir == candidate.suffixdir
        )
        house_gap = 999
        if reference.house_number.isdigit() and candidate.house_number.isdigit():
            house_gap = abs(int(reference.house_number) - int(candidate.house_number))

        score = 0.0
        if same_house and (same_city or same_zip):
            score = max(score, 0.68 + 0.25 * street_similarity)
        if same_street and (same_city or same_zip) and 0 < house_gap <= 20:
            score = max(score, 0.76 + 0.20 * (1.0 - min(house_gap, 20) / 20.0))
        if reason in {"same_house_city", "same_house_zip"} and street_similarity >= 0.45:
            score = max(score, 0.55 + 0.35 * street_similarity)
        if reason in {"same_street_city", "same_street_zip"} and 0 < house_gap <= 30:
            score = max(score, 0.58 + 0.30 * (1.0 - min(house_gap, 30) / 30.0))
        return score

    def take_real_records(self, n: int, prefix: str) -> List[AddressRecord]:
        results: List[AddressRecord] = []
        for source in self._real_pool_order:
            if len(results) >= n:
                break
            candidate = deepcopy(source)
            candidate.address_id = f"{prefix}_{len(results) + 1:06d}"
            key = canonical_address(candidate)
            if key in self.factory._seen_canonical:
                continue
            self.factory._seen_canonical.add(key)
            results.append(candidate)

        if len(results) == n:
            return results

        raise RuntimeError(
            f"Real address pool only had {len(results)} unused unique records for {prefix}; "
            f"{n} are required. Real-only generation will not invent replacement addresses; "
            "provide more source data or reduce --reference-size/--no-match-ratio."
        )

    def build_adversarial_negative_bases(
        self,
        reference: Sequence[AddressRecord],
        n: int,
        extra_excluded: Sequence[AddressRecord] = (),
    ) -> List[AddressRecord]:
        if self.real_address_pool:
            indexes = self.adversarial_neighbor_index(reference)
            excluded = {canonical_address(record) for record in reference}
            excluded.update(canonical_address(record) for record in extra_excluded)
            scored_candidates: List[Tuple[float, float, str, AddressRecord]] = []
            seen_candidates = set()

            for source in self._real_pool_order:
                source_key = canonical_address(source)
                if source_key in self.factory._seen_canonical or source_key in excluded or source_key in seen_candidates:
                    continue
                best_score = 0.0
                best_reason = ""
                for reason, neighbor in self.candidate_reference_neighbors(source, indexes):
                    score = self.adversarial_neighbor_score(neighbor, source, reason)
                    if score > best_score:
                        best_score = score
                        best_reason = reason
                if best_score <= 0.0:
                    continue
                seen_candidates.add(source_key)
                scored_candidates.append((best_score, self.rng.random(), best_reason, source))

            scored_candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
            results: List[AddressRecord] = []
            for _score, _tiebreaker, reason, source in scored_candidates:
                if len(results) >= n:
                    break
                candidate = deepcopy(source)
                candidate.address_id = f"NEG_ADV_{len(results) + 1:06d}"
                key = canonical_address(candidate)
                if key in self.factory._seen_canonical:
                    continue
                self.factory._seen_canonical.add(key)
                self.adversarial_reasons[candidate.address_id] = f"near_neighbor_{reason}"
                results.append(candidate)

            if len(results) < n:
                fallback = self.take_real_records(n - len(results), "NEG_ADV_FALLBACK")
                for record in fallback:
                    record.address_id = f"NEG_ADV_{len(results) + 1:06d}"
                    self.adversarial_reasons[record.address_id] = "fallback_holdout"
                    results.append(record)
            return results
        raise RuntimeError("No real address pool was supplied for adversarial negative generation.")

    def planned_difficulties(self, count: int, negative: bool = False, adversarial: bool = False) -> List[str]:
        if count <= 0:
            return []
        if adversarial:
            return ["hard"] * count

        planned: List[str] = []
        hard_floor = max(1, round(count * (0.35 if negative else 0.30))) if count >= 3 else 0
        medium_floor = max(1, round(count * 0.35)) if count >= 2 else 0
        hard_floor = min(hard_floor, count)
        medium_floor = min(medium_floor, max(0, count - hard_floor))

        planned.extend(["hard"] * hard_floor)
        planned.extend(["medium"] * medium_floor)
        remaining = count - len(planned)
        for _ in range(remaining):
            planned.append(choose_weighted(self.rng, [("easy", 0.25), ("medium", 0.40), ("hard", 0.35 if negative else 0.25)]))
        self.rng.shuffle(planned)
        return planned

    def build_reference_query_index(self, reference: Sequence[AddressRecord]) -> Dict[str, List[str]]:
        index: Dict[str, List[str]] = defaultdict(list)
        for record in reference:
            index[query_text_key(canonical_address(record))].append(record.address_id)
        return index

    def query_label_is_valid(
        self,
        query_address: str,
        label: int,
        true_match_id: str,
        reference_query_index: Dict[str, List[str]],
    ) -> bool:
        key = query_text_key(query_address)
        exact_matches = reference_query_index.get(key, [])
        if label == 1:
            if exact_matches and true_match_id not in exact_matches:
                return False
            if len(exact_matches) > 1:
                return False
            return True
        return not exact_matches

    def generate_labeled_query(
        self,
        record: AddressRecord,
        label: int,
        true_match_id: str,
        reference_query_index: Dict[str, List[str]],
        force_difficulty: Optional[str] = None,
    ) -> Tuple[str, str, str]:
        for _ in range(40):
            query_address, difficulty, tags = self.corruptor.corrupt(record, force_difficulty=force_difficulty)
            if self.query_label_is_valid(query_address, label, true_match_id, reference_query_index):
                return query_address, difficulty, tags
        raise RuntimeError(f"Could not generate a label-safe query for {record.address_id}")

    def build_queries(
        self,
        reference: Sequence[AddressRecord],
        negatives: Sequence[AddressRecord],
        adversarial_negatives: Sequence[AddressRecord],
        noisy_per_reference: int,
    ) -> List[QueryExample]:
        queries: List[QueryExample] = []
        q_counter = 1
        reference_query_index = self.build_reference_query_index(reference)

        for record in reference:
            split = stable_split_for_key(record.address_id)
            for difficulty_hint in self.planned_difficulties(noisy_per_reference):
                query_address, difficulty, tags = self.generate_labeled_query(
                    record,
                    label=1,
                    true_match_id=record.address_id,
                    reference_query_index=reference_query_index,
                    force_difficulty=difficulty_hint,
                )
                queries.append(
                    QueryExample(
                        query_id=f"Q_{q_counter:07d}",
                        split=split,
                        label=1,
                        true_match_id=record.address_id,
                        base_address_id=record.address_id,
                        source="reference_variant",
                        difficulty=difficulty,
                        noise_tags=tags,
                        query_address=query_address,
                        canonical_address=canonical_address(record),
                    )
                )
                q_counter += 1

        for record, difficulty_hint in zip(negatives, self.planned_difficulties(len(negatives), negative=True)):
            split = stable_split_for_key(record.address_id)
            query_address, difficulty, tags = self.generate_labeled_query(
                record,
                label=0,
                true_match_id="",
                reference_query_index=reference_query_index,
                force_difficulty=difficulty_hint,
            )
            queries.append(
                QueryExample(
                    query_id=f"Q_{q_counter:07d}",
                    split=split,
                    label=0,
                    true_match_id="",
                    base_address_id=record.address_id,
                    source="holdout_no_match",
                    difficulty=difficulty,
                    noise_tags=tags,
                    query_address=query_address,
                    canonical_address="NO_MATCH",
                )
            )
            q_counter += 1

        for record in adversarial_negatives:
            split = stable_split_for_key(record.address_id)
            query_address, difficulty, tags = self.generate_labeled_query(
                record,
                label=0,
                true_match_id="",
                reference_query_index=reference_query_index,
                force_difficulty="hard",
            )
            adversarial_reason = self.adversarial_reasons.get(record.address_id, "adversarial_holdout")
            combined_tags = "|".join(part for part in ["adversarial_base", adversarial_reason, tags] if part)
            queries.append(
                QueryExample(
                    query_id=f"Q_{q_counter:07d}",
                    split=split,
                    label=0,
                    true_match_id="",
                    base_address_id=record.address_id,
                    source="adversarial_no_match",
                    difficulty="hard",
                    noise_tags=combined_tags,
                    query_address=query_address,
                    canonical_address="NO_MATCH",
                )
            )
            q_counter += 1

        return queries


# ---------------------------------------------------------------------------
# Validation and export
# ---------------------------------------------------------------------------

def validate(
    reference: Sequence[AddressRecord],
    negatives: Sequence[AddressRecord],
    adversarial_negatives: Sequence[AddressRecord],
    queries: Sequence[QueryExample],
) -> None:
    ref_ids = {record.address_id for record in reference}
    neg_ids = {record.address_id for record in negatives}
    adv_neg_ids = {record.address_id for record in adversarial_negatives}
    if len(ref_ids) != len(reference):
        raise ValueError("Duplicate reference address IDs detected.")
    if ref_ids & neg_ids or ref_ids & adv_neg_ids or neg_ids & adv_neg_ids:
        raise ValueError("Reference and negative base IDs overlap.")

    ref_canonical = {canonical_address(record) for record in reference}
    neg_canonical = {canonical_address(record) for record in negatives}
    adv_neg_canonical = {canonical_address(record) for record in adversarial_negatives}
    if ref_canonical & neg_canonical or ref_canonical & adv_neg_canonical or neg_canonical & adv_neg_canonical:
        raise ValueError("A negative base address collides with a reference address.")

    reference_query_index: Dict[str, List[str]] = defaultdict(list)
    for record in reference:
        reference_query_index[query_text_key(canonical_address(record))].append(record.address_id)

    for query in queries:
        if query.label not in (0, 1):
            raise ValueError(f"Invalid label for query {query.query_id}: {query.label}")
        if query.label == 1 and query.true_match_id not in ref_ids:
            raise ValueError(f"Positive query {query.query_id} points to missing reference ID {query.true_match_id}")
        if query.label == 0 and query.true_match_id:
            raise ValueError(f"Negative query {query.query_id} should not have a true_match_id")
        if query.split not in {"train", "validation", "test"}:
            raise ValueError(f"Unexpected split on {query.query_id}: {query.split}")
        if not query.query_address:
            raise ValueError(f"Empty query address for {query.query_id}")
        exact_matches = reference_query_index.get(query_text_key(query.query_address), [])
        if query.label == 1:
            if exact_matches and query.true_match_id not in exact_matches:
                raise ValueError(f"Positive query {query.query_id} collides with a different reference canonical address.")
            if len(exact_matches) > 1:
                raise ValueError(f"Positive query {query.query_id} is exact-ambiguous against multiple reference addresses.")
        elif exact_matches:
            raise ValueError(f"Negative query {query.query_id} exactly matches a reference canonical address.")

    # All variants of the same base record should stay in the same split.
    split_by_base: Dict[str, str] = {}
    for query in queries:
        prior = split_by_base.get(query.base_address_id)
        if prior is not None and prior != query.split:
            raise ValueError(f"Base address {query.base_address_id} appears in multiple splits")
        split_by_base[query.base_address_id] = query.split

    split_counts = Counter(query.split for query in queries)
    if not split_counts:
        raise ValueError("No queries were generated.")


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def summarize(
    reference: Sequence[AddressRecord],
    negatives: Sequence[AddressRecord],
    adversarial_negatives: Sequence[AddressRecord],
    queries: Sequence[QueryExample],
) -> Dict[str, object]:
    by_split = defaultdict(lambda: {"positive": 0, "negative": 0})
    difficulty_counts = Counter(query.difficulty for query in queries)
    noise_counts = Counter(tag for query in queries for tag in query.noise_tags.split("|") if tag)
    source_counts = Counter(query.source for query in queries)
    source_difficulty_counts = defaultdict(Counter)

    for query in queries:
        key = "positive" if query.label == 1 else "negative"
        by_split[query.split][key] += 1
        source_difficulty_counts[query.source][query.difficulty] += 1

    return {
        "reference_records": len(reference),
        "negative_base_records": len(negatives) + len(adversarial_negatives),
        "holdout_negative_base_records": len(negatives),
        "adversarial_negative_base_records": len(adversarial_negatives),
        "query_records": len(queries),
        "splits": by_split,
        "difficulty_distribution": difficulty_counts,
        "query_sources": source_counts,
        "source_difficulty_distribution": source_difficulty_counts,
        "top_noise_tags": noise_counts.most_common(15),
    }


def export_dataset(
    output_dir: Path,
    reference: Sequence[AddressRecord],
    negatives: Sequence[AddressRecord],
    adversarial_negatives: Sequence[AddressRecord],
    queries: Sequence[QueryExample],
    config: Dict[str, object],
) -> Dict[str, object]:
    output_dir.mkdir(parents=True, exist_ok=True)

    reference_rows = []
    for record in reference:
        row = asdict(record)
        row["canonical_address"] = canonical_address(record)
        reference_rows.append(row)

    negative_rows = []
    for record in negatives:
        row = asdict(record)
        row["holdout_canonical_address"] = canonical_address(record)
        negative_rows.append(row)

    adversarial_negative_rows = []
    for record in adversarial_negatives:
        row = asdict(record)
        row["adversarial_canonical_address"] = canonical_address(record)
        adversarial_negative_rows.append(row)

    query_rows = [asdict(query) for query in queries]

    write_csv(
        output_dir / "reference_addresses.csv",
        list(reference_rows[0].keys()) if reference_rows else [],
        reference_rows,
    )
    write_csv(
        output_dir / "holdout_no_match_bases.csv",
        list(negative_rows[0].keys()) if negative_rows else [],
        negative_rows,
    )
    write_csv(
        output_dir / "adversarial_no_match_bases.csv",
        list(adversarial_negative_rows[0].keys()) if adversarial_negative_rows else [],
        adversarial_negative_rows,
    )
    write_csv(
        output_dir / "queries.csv",
        list(query_rows[0].keys()) if query_rows else [],
        query_rows,
    )
    write_csv(
        output_dir / "resolver_input.csv",
        ["query_id", "split", "query_address"],
        (
            {
                "query_id": query.query_id,
                "split": query.split,
                "query_address": query.query_address,
            }
            for query in queries
        ),
    )

    summary = summarize(reference, negatives, adversarial_negatives, queries)
    metadata = {
        "config": config,
        "summary": summary,
    }
    (output_dir / "metadata.json").write_text(json.dumps(metadata, indent=2, default=lambda x: dict(x)), encoding="utf-8")

    return summary


def preview_queries(queries: Sequence[QueryExample], limit: int = 8) -> str:
    lines = []
    for query in queries[:limit]:
        match = query.true_match_id if query.true_match_id else "NO_MATCH"
        lines.append(f"[{query.split}/{query.difficulty}/{query.label}] {query.query_address}  =>  {match}")
    return "\n".join(lines)


def generate_dataset(
    output_dir: Path,
    seed: int,
    args: argparse.Namespace,
    reference: Optional[Sequence[AddressRecord]] = None,
    real_address_pool: Optional[Sequence[AddressRecord]] = None,
    real_address_metadata: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    if not real_address_pool:
        raise RuntimeError(
            "No real address pool was supplied. The generator is real-address-only by default; "
            "provide --real-address-input or a downloader such as --download-maris-parcels."
        )

    builder = DatasetBuilder(
        seed=seed,
        real_address_pool=real_address_pool,
    )
    reference_records = list(reference) if reference is not None else builder.build_reference(args.reference_size)
    if reference is not None:
        builder.factory._seen_canonical.update(canonical_address(record) for record in reference_records)
    positive_queries = len(reference_records) * args.noisy_per_reference
    negative_base_count = max(1, round(positive_queries * args.no_match_ratio)) if args.no_match_ratio > 0 else 0
    adversarial_negative_count = round(negative_base_count * args.adversarial_no_match_share)
    holdout_negative_count = negative_base_count - adversarial_negative_count
    negatives = builder.build_negative_bases(holdout_negative_count) if holdout_negative_count else []
    adversarial_negatives = (
        builder.build_adversarial_negative_bases(reference_records, adversarial_negative_count, extra_excluded=negatives)
        if adversarial_negative_count
        else []
    )
    queries = builder.build_queries(reference_records, negatives, adversarial_negatives, args.noisy_per_reference)

    validate(reference_records, negatives, adversarial_negatives, queries)

    config = {
        "reference_size": args.reference_size,
        "noisy_per_reference": args.noisy_per_reference,
        "no_match_ratio": args.no_match_ratio,
        "adversarial_no_match_share": args.adversarial_no_match_share,
        "seed": seed,
        "address_source": "real",
        "adversarial_negative_base_strategy": "near_neighbor_real_holdout",
    }
    if real_address_metadata:
        config["real_address_source"] = real_address_metadata
    summary = export_dataset(output_dir, reference_records, negatives, adversarial_negatives, queries, config)
    return {"summary": summary, "queries": queries}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
