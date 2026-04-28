#!/usr/bin/env python3
"""Generate a labeled address-matching dataset from real address sources.

Compatibility entrypoint for the dataset generator CLI and public imports.
Source loading, noise generation, and dataset assembly live in focused modules.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

import address_source_loading as _source_loading
from address_dataset_build import *  # noqa: F401,F403 - preserve public imports from address_dataset_generator
from address_noise import *  # noqa: F401,F403 - preserve public imports from address_dataset_generator
from address_source_loading import *  # noqa: F401,F403 - preserve public imports from address_dataset_generator


def _sync_source_loading_hooks() -> None:
    """Keep legacy monkeypatches on this facade visible to moved source helpers."""
    _source_loading.read_json_url = read_json_url
    _source_loading.open_url = open_url
    _source_loading.download_file = download_file


def read_arcgis_features_for_object_ids(*args, **kwargs):
    _sync_source_loading_hooks()
    return _source_loading.read_arcgis_features_for_object_ids(*args, **kwargs)


def download_openaddresses_ms_direct(*args, **kwargs):
    _sync_source_loading_hooks()
    return _source_loading.download_openaddresses_ms_direct(*args, **kwargs)


def maris_parcel_layers(*args, **kwargs):
    _sync_source_loading_hooks()
    return _source_loading.maris_parcel_layers(*args, **kwargs)


def download_maris_parcels(*args, **kwargs):
    _sync_source_loading_hooks()
    return _source_loading.download_maris_parcels(*args, **kwargs)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate address-resolution data from real address sources.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "address_dataset",
        help="Directory for CSV outputs.",
    )
    parser.add_argument(
        "--paired-output-dir",
        type=Path,
        help="If set, generate separate train_dataset/ and eval_dataset/ directories under this root using different seeds.",
    )
    parser.add_argument(
        "--paired-shared-reference",
        action="store_true",
        help="In paired mode, reuse the same reference database for train/eval and generate only separate query/negative sets.",
    )
    parser.add_argument("--reference-size", type=int, default=5000, help="Number of reference addresses.")
    parser.add_argument("--noisy-per-reference", type=int, default=12, help="Positive noisy variants per reference address.")
    parser.add_argument("--no-match-ratio", type=float, default=0.5, help="Negative query count as a fraction of positive query count.")
    parser.add_argument(
        "--adversarial-no-match-share",
        type=float,
        default=0.5,
        help="Fraction of negative queries generated as adversarial near-match no-matches instead of independent holdout addresses.",
    )
    parser.add_argument(
        "--real-address-input",
        type=Path,
        action="append",
        default=[],
        help="File or directory of real addresses. Supports MARIS/MS811 shapefile ZIP/DBF, MARIS parcel CSV, OpenAddresses CSV, USDOT NAD text, generic address CSVs, and this generator's reference CSV schema.",
    )
    parser.add_argument(
        "--real-address-format",
        choices=["auto", "maris", "maris_parcels", "nad", "openaddresses", "address_record", "generic"],
        default="auto",
        help="Input schema for --real-address-input. Use generic for a bring-your-own CSV with full_address/address plus city/state/zip columns.",
    )
    parser.add_argument(
        "--source-manifest",
        type=Path,
        action="append",
        default=[],
        help="JSON manifest listing real address source paths/cache dirs and source formats. May be repeated.",
    )
    parser.add_argument(
        "--audit-sources",
        action="store_true",
        help="Audit configured real address sources and exit without generating a dataset.",
    )
    parser.add_argument(
        "--source-audit-output",
        type=Path,
        help="Optional JSON output path for --audit-sources.",
    )
    parser.add_argument(
        "--real-address-state",
        default="MS",
        help="Two-letter state filter for real address inputs. Defaults to MS.",
    )
    parser.add_argument(
        "--real-address-limit",
        type=int,
        help="Optional cap on loaded real addresses after filtering and de-duplication.",
    )
    parser.add_argument(
        "--download-openaddresses-ms",
        action="store_true",
        help="Download archived Mississippi OpenAddresses source ZIPs into --real-address-cache-dir and use them as the real address input.",
    )
    parser.add_argument(
        "--real-address-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "openaddresses_ms",
        help="Cache directory for --download-openaddresses-ms.",
    )
    parser.add_argument(
        "--download-openaddresses-ms-direct",
        action="store_true",
        help="Download current ESRI Mississippi address layers from the OpenAddresses source catalog into --openaddresses-ms-direct-cache-dir and use them as real address input.",
    )
    parser.add_argument(
        "--openaddresses-ms-direct-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "openaddresses_ms_direct",
        help="Cache directory for --download-openaddresses-ms-direct normalized CSV output.",
    )
    parser.add_argument(
        "--openaddresses-ms-source-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "openaddresses_ms_sources",
        help="Cache directory for OpenAddresses Mississippi source JSON definitions.",
    )
    parser.add_argument(
        "--refresh-openaddresses-ms-direct-cache",
        action="store_true",
        help="Force --download-openaddresses-ms-direct to re-query ESRI layers and replace cached normalized CSVs.",
    )
    parser.add_argument(
        "--refresh-openaddresses-ms-source-cache",
        action="store_true",
        help="Force --download-openaddresses-ms-direct to refresh the cached OpenAddresses source JSON definitions.",
    )
    parser.add_argument(
        "--include-openaddresses-ms-statewide-direct",
        action="store_true",
        help="Include older statewide OpenAddresses parcel layers that are skipped by default because the app uses newer MARIS parcels.",
    )
    parser.add_argument(
        "--download-maris-point-addresses",
        action="store_true",
        help="Download public MARIS Mississippi Point Addressing county shapefile ZIPs and use them as the real address input.",
    )
    parser.add_argument(
        "--maris-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "maris_point_addresses",
        help="Cache directory for --download-maris-point-addresses.",
    )
    parser.add_argument(
        "--download-maris-parcels",
        action="store_true",
        help="Download public MARIS statewide parcel situs-address CSVs and use them as a real address input fallback.",
    )
    parser.add_argument(
        "--maris-parcel-cache-dir",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "maris_parcels",
        help="Cache directory for --download-maris-parcels.",
    )
    parser.add_argument(
        "--maris-parcel-layer-limit",
        type=int,
        help="Optional development/testing cap on the number of MARIS parcel layers to download.",
    )
    parser.add_argument(
        "--refresh-maris-parcel-cache",
        action="store_true",
        help="Force --download-maris-parcels to re-query MARIS and replace cached parcel CSVs.",
    )
    parser.add_argument(
        "--download-nad",
        action="store_true",
        help="Download the official USDOT NAD national text ZIP into --nad-cache-path and use it as a real address input. The file is large.",
    )
    parser.add_argument(
        "--nad-cache-path",
        type=Path,
        default=Path.cwd() / "datasets" / "source_cache" / "nad" / "TXT.zip",
        help="Cache file for --download-nad.",
    )
    parser.add_argument(
        "--require-ms-county-coverage",
        action="store_true",
        help="Require real Mississippi county inputs to include filenames for all 82 Mississippi counties before generating.",
    )
    parser.add_argument("--seed", type=int, default=4633, help="Random seed for reproducibility.")
    parser.add_argument("--eval-seed", type=int, help="Optional seed for the separately generated evaluation dataset. Defaults to seed + 1000.")
    parser.add_argument("--preview", type=int, default=8, help="Number of sample queries to print.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir = args.output_dir.expanduser().resolve()
    if args.paired_output_dir:
        args.paired_output_dir = args.paired_output_dir.expanduser().resolve()
    args.real_address_cache_dir = args.real_address_cache_dir.expanduser().resolve()
    args.openaddresses_ms_direct_cache_dir = args.openaddresses_ms_direct_cache_dir.expanduser().resolve()
    args.openaddresses_ms_source_cache_dir = args.openaddresses_ms_source_cache_dir.expanduser().resolve()
    args.maris_cache_dir = args.maris_cache_dir.expanduser().resolve()
    args.maris_parcel_cache_dir = args.maris_parcel_cache_dir.expanduser().resolve()
    args.nad_cache_path = args.nad_cache_path.expanduser().resolve()
    args.real_address_input = [path.expanduser().resolve() for path in args.real_address_input]
    args.source_manifest = [path.expanduser().resolve() for path in args.source_manifest]
    if args.source_audit_output:
        args.source_audit_output = args.source_audit_output.expanduser().resolve()
    if args.reference_size <= 0:
        raise SystemExit("--reference-size must be positive")
    if args.noisy_per_reference <= 0:
        raise SystemExit("--noisy-per-reference must be positive")
    if args.no_match_ratio < 0:
        raise SystemExit("--no-match-ratio must be non-negative")
    if not 0 <= args.adversarial_no_match_share <= 1:
        raise SystemExit("--adversarial-no-match-share must be between 0 and 1")
    if args.real_address_limit is not None and args.real_address_limit <= 0:
        raise SystemExit("--real-address-limit must be positive when set")
    if args.maris_parcel_layer_limit is not None and args.maris_parcel_layer_limit <= 0:
        raise SystemExit("--maris-parcel-layer-limit must be positive when set")
    args.real_address_state = canonical_state(args.real_address_state)
    if not args.real_address_state:
        raise SystemExit("--real-address-state must be a recognized two-letter state code or state name")

    real_address_pool: Optional[List[AddressRecord]] = None
    real_address_metadata: Optional[Dict[str, object]] = None
    source_specs: List[SourceSpec] = []
    for manifest_path in args.source_manifest:
        source_specs.extend(source_specs_from_manifest(manifest_path))
    if args.download_openaddresses_ms:
        download_openaddresses_ms(args.real_address_cache_dir)
        source_specs.append(SourceSpec("openaddresses_processed", args.real_address_cache_dir, "openaddresses"))
    if args.download_openaddresses_ms_direct:
        download_openaddresses_ms_direct(
            args.openaddresses_ms_direct_cache_dir,
            args.openaddresses_ms_source_cache_dir,
            refresh=args.refresh_openaddresses_ms_direct_cache,
            refresh_configs=args.refresh_openaddresses_ms_source_cache,
            include_statewide=args.include_openaddresses_ms_statewide_direct,
        )
        source_specs.append(SourceSpec("openaddresses_direct", args.openaddresses_ms_direct_cache_dir, "openaddresses"))
    if args.download_maris_point_addresses:
        download_maris_point_addresses(args.maris_cache_dir)
        source_specs.append(SourceSpec("maris_point_addresses", args.maris_cache_dir, "maris"))
    if args.download_maris_parcels:
        download_maris_parcels(
            args.maris_parcel_cache_dir,
            layer_limit=args.maris_parcel_layer_limit,
            refresh=args.refresh_maris_parcel_cache,
        )
        source_specs.append(SourceSpec("maris_parcels", args.maris_parcel_cache_dir, "maris_parcels"))
    if args.download_nad:
        nad_path = download_file(NAD_TEXT_ZIP_URL, args.nad_cache_path, timeout=300)
        source_specs.append(SourceSpec("nad", nad_path, "nad"))
    for index, input_path in enumerate(args.real_address_input, 1):
        source_specs.append(SourceSpec(f"real_address_input_{index}", input_path, args.real_address_format))
    if args.audit_sources and not source_specs:
        source_specs = default_public_source_specs()

    real_input_files: List[Path] = []
    if source_specs:
        real_input_files = discover_input_files([spec.path for spec in source_specs if spec.path.exists()])

    county_coverage: Optional[Dict[str, object]] = None
    if real_input_files:
        covered_counties = mississippi_counties_in_paths(real_input_files)
        missing_counties = sorted(set(MISSISSIPPI_COUNTIES) - set(covered_counties))
        county_coverage = {
            "covered_counties": covered_counties,
            "covered_county_count": len(covered_counties),
            "expected_county_count": len(MISSISSIPPI_COUNTIES),
            "missing_counties": missing_counties,
        }
        if args.require_ms_county_coverage and missing_counties:
            raise SystemExit(
                "Mississippi county coverage check failed. Missing counties: "
                + ", ".join(missing_counties)
            )

    if args.audit_sources:
        audit = audit_source_specs(source_specs, state_filter=args.real_address_state)
        if args.source_audit_output:
            write_source_audit(audit, args.source_audit_output)
            print(f"Wrote source audit to: {args.source_audit_output}")
        print(json.dumps(audit["summary"], indent=2, sort_keys=True))
        return

    if source_specs:
        load_result = load_real_addresses_from_source_specs(
            source_specs,
            state_filter=args.real_address_state,
            limit=args.real_address_limit,
        )
        if not load_result.records:
            raise SystemExit("No usable real addresses were loaded from the supplied source data.")
        real_address_pool = load_result.records
        real_address_metadata = {
            "input_paths": load_result.input_paths,
            "source_format": load_result.source_format,
            "state": load_result.state,
            "rows_seen": load_result.rows_seen,
            "rows_loaded": load_result.rows_loaded,
            "rows_loaded_after_cross_source_deduplication": load_result.rows_loaded_after_cross_source_deduplication,
            "rows_skipped": load_result.rows_skipped,
            "skip_reasons": load_result.skip_reasons,
            "duplicate_across_sources": load_result.duplicate_across_sources,
            "mississippi_county_coverage": county_coverage,
            "sources": [
                {
                    "name": spec.name,
                    "path": str(spec.path),
                    "source_format": result.source_format,
                    "configured_source_format": spec.source_format,
                    "rows_seen": result.rows_seen,
                    "rows_loaded": result.rows_loaded,
                    "rows_skipped": result.rows_skipped,
                    "skip_reasons": result.skip_reasons,
                    "duplicate_rows": result.duplicate_rows,
                    "input_paths": result.input_paths,
                    "source_status": source_status_summary_for_path(spec.path),
                    "notes": spec.notes,
                }
                for spec, result in zip(load_result.source_specs, load_result.source_results)
            ],
            "source_note": (
                "For Mississippi-wide production use, provide the full MS811/MARIS county shapefile ZIP set and enable "
                "--require-ms-county-coverage. Public MARIS parcels are a broad parcel situs-address fallback, not true "
                "address points. Public MARIS point addresses, OpenAddresses, and NAD inputs are supported but should "
                "be verified before treating them as exhaustive."
            ),
        }
        print(
            f"Loaded {len(load_result.records):,} unique real {load_result.state} addresses "
            f"from {len(load_result.input_paths)} file(s); skipped {load_result.rows_skipped:,} rows."
        )

    if real_address_pool is None:
        raise SystemExit(
            "No usable real address source was supplied. The generator is real-address-only by default; "
            "provide --real-address-input or a downloader such as --download-maris-parcels."
        )

    if args.paired_output_dir:
        eval_seed = args.eval_seed if args.eval_seed is not None else args.seed + 1000
        train_output_dir = args.paired_output_dir / "train_dataset"
        eval_output_dir = args.paired_output_dir / "eval_dataset"
        shared_reference = None
        if args.paired_shared_reference:
            shared_reference = DatasetBuilder(
                args.seed,
                real_address_pool=real_address_pool,
            ).build_reference(args.reference_size)
        train_result = generate_dataset(
            train_output_dir,
            args.seed,
            args,
            reference=shared_reference,
            real_address_pool=real_address_pool,
            real_address_metadata=real_address_metadata,
        )
        eval_result = generate_dataset(
            eval_output_dir,
            eval_seed,
            args,
            reference=shared_reference,
            real_address_pool=real_address_pool,
            real_address_metadata=real_address_metadata,
        )

        print("Generated paired address-resolution datasets successfully.")
        print(json.dumps({
            "train_dataset_dir": str(train_output_dir),
            "eval_dataset_dir": str(eval_output_dir),
            "train_seed": args.seed,
            "eval_seed": eval_seed,
            "shared_reference": bool(args.paired_shared_reference),
            "train_summary": train_result["summary"],
            "eval_summary": eval_result["summary"],
        }, indent=2, default=lambda x: dict(x)))
        print("\nSample train queries:")
        print(preview_queries(train_result["queries"], limit=args.preview))
        print("\nSample eval queries:")
        print(preview_queries(eval_result["queries"], limit=args.preview))
        print(f"\nOutput written to: {args.paired_output_dir}")
        return

    result = generate_dataset(
        args.output_dir,
        args.seed,
        args,
        real_address_pool=real_address_pool,
        real_address_metadata=real_address_metadata,
    )

    print("Generated address-resolution dataset successfully.")
    print(json.dumps(result["summary"], indent=2, default=lambda x: dict(x)))
    print("\nSample queries:")
    print(preview_queries(result["queries"], limit=args.preview))
    print(f"\nOutput written to: {args.output_dir}")


if __name__ == "__main__":
    main()
