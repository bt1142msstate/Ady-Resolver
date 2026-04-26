#!/usr/bin/env python3
"""Train Ady Resolver from a user-supplied address CSV."""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate typo-heavy training data from real addresses and train a Stage 2 resolver model.",
    )
    parser.add_argument(
        "--address-input",
        type=Path,
        action="append",
        required=True,
        help="Address CSV/file/directory. Repeat for multiple sources. Generic CSVs can use full_address/address plus optional city/state/zip columns.",
    )
    parser.add_argument(
        "--address-format",
        default="generic",
        choices=["auto", "generic", "address_record", "maris", "maris_parcels", "nad", "openaddresses"],
        help="Input schema. Use generic for most bring-your-own CSVs.",
    )
    parser.add_argument("--state", default="MS", help="Two-letter state filter or state name. Defaults to MS.")
    parser.add_argument("--work-dir", type=Path, default=PROJECT_ROOT / "datasets" / "custom_training", help="Generated train/eval dataset directory.")
    parser.add_argument("--model-path", type=Path, default=PROJECT_ROOT / "models" / "stage2_model.json", help="Output model JSON path.")
    parser.add_argument("--run-dir", type=Path, default=PROJECT_ROOT / "runs" / "custom_training", help="Training evaluation output directory.")
    parser.add_argument("--reference-size", type=int, default=5000, help="Reference records sampled from the supplied address pool.")
    parser.add_argument("--noisy-per-reference", type=int, default=12, help="Positive noisy examples generated per reference address.")
    parser.add_argument("--no-match-ratio", type=float, default=0.25, help="Negative no-match query ratio relative to positive query count.")
    parser.add_argument("--adversarial-no-match-share", type=float, default=0.5, help="Share of no-match queries generated as near-match adversarial examples.")
    parser.add_argument("--seed", type=int, default=4633, help="Dataset generation seed.")
    parser.add_argument("--jobs", type=int, default=4, help="Resolver evaluation worker count.")
    return parser.parse_args()


def run(command: list[str]) -> None:
    print("+ " + " ".join(command))
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def main() -> None:
    args = parse_args()
    work_dir = args.work_dir.expanduser().resolve()
    model_path = args.model_path.expanduser().resolve()
    run_dir = args.run_dir.expanduser().resolve()

    generate_command = [
        sys.executable,
        str(PROJECT_ROOT / "src" / "address_dataset_generator.py"),
        "--real-address-format",
        args.address_format,
        "--real-address-state",
        args.state,
        "--paired-output-dir",
        str(work_dir),
        "--paired-shared-reference",
        "--reference-size",
        str(args.reference_size),
        "--noisy-per-reference",
        str(args.noisy_per_reference),
        "--no-match-ratio",
        str(args.no_match_ratio),
        "--adversarial-no-match-share",
        str(args.adversarial_no_match_share),
        "--seed",
        str(args.seed),
    ]
    for address_input in args.address_input:
        generate_command.extend(["--real-address-input", str(address_input.expanduser().resolve())])

    train_command = [
        sys.executable,
        str(PROJECT_ROOT / "src" / "address_resolver.py"),
        "--mode",
        "fit-predict",
        "--train-dataset-dir",
        str(work_dir / "train_dataset"),
        "--eval-dataset-dir",
        str(work_dir / "eval_dataset"),
        "--model-path",
        str(model_path),
        "--output-dir",
        str(run_dir),
        "--compare-variants",
        "--jobs",
        str(args.jobs),
    ]

    run(generate_command)
    run(train_command)
    print(f"Model written to: {model_path}")
    print(f"Evaluation written to: {run_dir / 'evaluation.json'}")


if __name__ == "__main__":
    main()
