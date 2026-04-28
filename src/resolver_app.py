#!/usr/bin/env python3
"""Local web UI for resolving a typed address against the trained reference set.

Compatibility entrypoint for the app CLI and public helper imports.
"""
from __future__ import annotations

import argparse
from http.server import ThreadingHTTPServer
from pathlib import Path

from address_dataset_generator import (  # noqa: F401
    AddressRecord,
    MISSISSIPPI_COUNTIES,
    canonical_address,
    discover_input_files,
    load_real_addresses,
    mississippi_counties_in_paths,
    query_text_key,
    zip_code_matches_state,
)
from address_resolver import (  # noqa: F401
    ReferenceAddress,
    Resolver,
    Stage2Model,
    build_city_lookup,
    choose_combined_resolution,
    load_model,
    load_reference,
    normalize_text,
    standardize_parts,
)
from resolver_app_config import *  # noqa: F401,F403
from resolver_app_storage import *  # noqa: F401,F403
from resolver_app_ui import HTML  # noqa: F401
from resolver_batch_io import *  # noqa: F401,F403
from resolver_http import ResolverRequestHandler
from resolver_reference_cache import *  # noqa: F401,F403
from resolver_service import ResolverService

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local web UI for resolving typed addresses.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind.")
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR, help="Dataset directory with reference_addresses.csv.")
    parser.add_argument("--real-address-input", type=Path, action="append", help="Real address cache used to build the app reference cache. May be repeated. Defaults to cached MARIS parcels plus cached MARIS point addresses when available.")
    parser.add_argument("--real-address-format", default="auto", choices=["auto", "maris", "maris_parcels", "nad", "openaddresses", "address_record", "generic"], help="Input schema for custom --real-address-input values.")
    parser.add_argument("--rebuild-reference-cache", action="store_true", help="Rebuild the app reference cache from --real-address-input before starting.")
    parser.add_argument("--model-path", type=Path, default=DEFAULT_MODEL_PATH, help="Saved Stage 2 model JSON.")
    parser.add_argument("--train-dataset-dir", type=Path, default=DEFAULT_TRAIN_DATASET_DIR, help="Dataset directory used by automatic app retraining.")
    parser.add_argument("--eval-dataset-dir", type=Path, default=DEFAULT_EVAL_DATASET_DIR, help="Evaluation dataset directory used by automatic app retraining.")
    parser.add_argument("--training-output-dir", type=Path, default=DEFAULT_TRAINING_OUTPUT_DIR, help="Run output directory used by automatic app retraining.")
    parser.add_argument("--training-jobs", type=int, default=4, help="Worker count used by automatic app retraining.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_dir = args.dataset_dir.expanduser().resolve()
    if args.real_address_input:
        source_specs = [(path.expanduser().resolve(), args.real_address_format) for path in args.real_address_input]
    else:
        source_specs = [(path.expanduser().resolve(), source_format) for path, source_format in default_source_specs()]
    model_path = args.model_path.expanduser().resolve()
    train_dataset_dir = args.train_dataset_dir.expanduser().resolve()
    eval_dataset_dir = args.eval_dataset_dir.expanduser().resolve()
    training_output_dir = args.training_output_dir.expanduser().resolve()
    if (
        dataset_dir == DEFAULT_DATASET_DIR.resolve()
        and not reference_cache_ready(dataset_dir)
        and not source_specs
        and reference_cache_ready(DEMO_DATASET_DIR)
    ):
        print(f"Full reference cache not found; using demo dataset at {DEMO_DATASET_DIR}.")
        dataset_dir = DEMO_DATASET_DIR.resolve()
    if args.rebuild_reference_cache or not reference_cache_ready(dataset_dir):
        build_reference_cache(dataset_dir, source_specs)
    if not reference_cache_ready(dataset_dir):
        raise SystemExit(f"Reference CSV not found: {reference_csv_path(dataset_dir)}")
    if not model_path.exists():
        raise SystemExit(f"Model JSON not found: {model_path}")

    print(f"Loading resolver reference cache from {reference_csv_path(dataset_dir)}...")
    service = ResolverService(
        dataset_dir,
        model_path,
        train_dataset_dir=train_dataset_dir,
        eval_dataset_dir=eval_dataset_dir,
        training_output_dir=training_output_dir,
        training_jobs=args.training_jobs,
    )
    ResolverRequestHandler.service = service
    server = ThreadingHTTPServer((args.host, args.port), ResolverRequestHandler)
    print(f"Ady Resolver app running at http://{args.host}:{args.port}")
    print(f"Dataset: {service.dataset_name} ({service.reference_count:,} references)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
