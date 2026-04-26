# Ady Resolver

[![Tests](https://github.com/bt1142msstate/Ady-Resolver/actions/workflows/tests.yml/badge.svg)](https://github.com/bt1142msstate/Ady-Resolver/actions/workflows/tests.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Ady Resolver is a Python address-resolution toolkit focused on messy
Mississippi addresses. It ships with a pretrained Stage 2 model and a small
checked-in demo reference set, builds larger canonical reference sets from
public or private address sources, generates typo-heavy training/evaluation
data from real source records, and serves a local browser app for inspecting how
an input address is standardized, scored, and matched.

## Features

- Real-address-first dataset generation. The generator samples reference,
  positive, no-match, and adversarial examples from loaded real address source
  records instead of inventing synthetic addresses.
- Hard adversarial no-matches. The generator deliberately chooses near-neighbor
  real non-matches such as same house/city with a similar street or same
  street/city with a nearby house number, then corrupts those examples.
- Bring-your-own address CSV training. Generic CSVs can use either structured
  columns or a single `full_address`/`address` column with optional
  `city`/`state`/`zip` columns.
- Mississippi source ingestion from MARIS parcel situs data, public MARIS point
  addressing ZIPs, OpenAddresses processed extracts, OpenAddresses source
  services, NAD text exports, and manual verified supplements.
- Locality-aware resolver pipeline with deterministic Stage 1 rules and a
  lightweight Stage 2 model trained with mined hard negatives, phonetic
  street/city features, rank/margin features, ZIP/city consistency,
  source-quality weighting, and stronger house-number mismatch penalties.
- Typo handling for street names, street suffixes, city names, directionals,
  and compounded input errors such as `101 candoowse sr newtooon MS`.
- Data-quality guards for obvious parcel/location artifacts such as zero house
  numbers, non-numeric house numbers, side-of-road markers, `N OF ...`
  descriptors, `DOD` note rows, and duplicated terminal street types.
- Browser app for typing an address and seeing the standardized query,
  selected match, confidence, stage, and top candidates. The app can collect
  correct/wrong/correction feedback, apply exact-input feedback overrides, and
  start active-learning retraining.

## Quick Start

Ady Resolver uses the Python standard library only.

```bash
git clone https://github.com/bt1142msstate/Ady-Resolver.git
cd Ady-Resolver
python3 -m unittest discover -s tests -v
```

Run the local app after building or restoring a reference cache:

```bash
python3 src/resolver_app.py
```

Then open `http://127.0.0.1:8765`.

## Repository Contents

- `src/address_dataset_generator.py` - source downloading, cache handling,
  parsing, cleanup, and training/evaluation dataset generation.
- `src/address_resolver.py` - Stage 1 resolver, Stage 2 model, metrics, and
  CLI entry points.
- `src/resolver_app.py` - local web app and reference-cache builder.
- `src/train_from_addresses.py` - one-command dataset generation and model
  training for custom address CSVs.
- `models/` - small checked-in Stage 2 model JSON artifacts.
- `examples/` - a small demo reference set and custom-address CSV example for
  fresh clones.
- `tests/` - regression tests for source parsing, resolver behavior, metrics,
  generator noise, ZIP/city enrichment, and OpenAddresses direct caching.

Generated `datasets/` and `runs/` directories are intentionally ignored by git.
They can be several GB because they contain downloaded public source archives,
normalized caches, full reference CSVs, and prediction outputs. The repository
includes the pretrained model at `models/stage2_model.json` and a tiny demo
reference set at `examples/demo_reference/reference_addresses.csv`; rebuild
larger local data with the commands below instead of committing generated
artifacts.

If `datasets/ms_full_reference/reference_addresses.csv` is missing, the local
app automatically falls back to `examples/demo_reference` so a fresh clone can
still run. That demo is intentionally small and is not the Mississippi-wide
reference cache.

## Real Mississippi Address Data

The generator now supports real address sources for Mississippi. For exhaustive
Mississippi coverage, use the MS811/MARIS county shapefile ZIP set and keep the
county-coverage guard enabled.

- MS811/MARIS full county shapefile ZIPs: production source for all 82
  Mississippi counties when obtained through the MARIS distribution agreement.
- Public MARIS Mississippi Point Addressing ZIPs: easiest public Mississippi
  source and the best public default tested here, but MARIS notes city point
  addresses may not be included and the public download page is only a subset
  of counties.
- Public MARIS statewide parcel situs addresses: broad public fallback with
  parcel `SITEADD`/`SCITY`/`SSTATE`/`SZIP` fields across all county parcel
  layers. This is not true point-address data, but it is the closest public
  statewide fallback found.
- OpenAddresses Mississippi extracts: supported as supplemental/development
  data. The current easy processed extracts are not exhaustive and many rows
  lack city/ZIP locality fields.
- Manual verified Mississippi supplement: optional local CSV for individually
  verified public addresses that are missing from the bulk public feeds.
- USDOT National Address Database (NAD): supported parser/download path, but
  Release 22 was not useful for Mississippi in testing. The national file had
  only three `State=MS` rows, all with non-Mississippi ZIPs, so it is cached for
  audit but not merged into the app reference cache.

Practical source strategy:

- Best public baseline: merge MARIS parcels, public MARIS Point Addressing,
  OpenAddresses, and the manual verified supplement.
- Best authoritative Mississippi route: obtain the local/state NG9-1-1 address
  point repository or full MS811/MARIS county point-address distribution.
- Best deliverable-mail route: use a licensed USPS/CASS/DPV-capable source or
  API. That is validation-grade for postal delivery, but it is not the same as
  a free downloadable public address list.
- Operational fallback: add verified misses to
  `datasets/source_cache/manual_verified_ms/verified_addresses.csv`. The local
  app has an Add Verified Address form that writes this supplement, updates the
  live resolver index, and persists the address into the current reference CSV.
  The resolver result panel also has Correct, Wrong, and Save Correction
  feedback controls. Feedback is written to
  `datasets/source_cache/active_learning/resolver_feedback.csv`; corrections
  also add the verified address to the manual supplement.

Generate from locally supplied MS811/MARIS county shapefile ZIPs with all-82
county enforcement:

```bash
python3 src/address_dataset_generator.py \
  --real-address-input datasets/source_cache/ms811 \
  --real-address-format maris \
  --real-address-state MS \
  --require-ms-county-coverage \
  --paired-output-dir datasets/ms811_real \
  --paired-shared-reference
```

Generate from the public MARIS Point Addressing page:

```bash
python3 src/address_dataset_generator.py \
  --download-maris-point-addresses \
  --real-address-format maris \
  --paired-output-dir datasets/ms_public_maris \
  --paired-shared-reference \
  --reference-size 5000 \
  --noisy-per-reference 8
```

Generate from the public MARIS statewide parcel fallback:

```bash
python3 src/address_dataset_generator.py \
  --download-maris-parcels \
  --real-address-format maris_parcels \
  --require-ms-county-coverage \
  --paired-output-dir datasets/ms_public_parcels \
  --paired-shared-reference
```

`--download-maris-parcels` uses `datasets/source_cache/maris_parcels` by
default. Once the 81 parcel CSVs are cached, later runs reuse those files and do
not download them again. Use `--refresh-maris-parcel-cache` only when you want
to replace the cached parcel files from MARIS.

Generate from OpenAddresses Mississippi extracts for supplemental/dev testing:

```bash
python3 src/address_dataset_generator.py \
  --download-openaddresses-ms \
  --real-address-format openaddresses \
  --paired-output-dir datasets/ms_openaddresses \
  --paired-shared-reference
```

Generate from the current OpenAddresses Mississippi source catalog by querying
the source ESRI services directly and caching normalized CSVs:

```bash
python3 src/address_dataset_generator.py \
  --download-openaddresses-ms-direct \
  --real-address-format openaddresses \
  --paired-output-dir datasets/ms_openaddresses_direct \
  --paired-shared-reference
```

`--download-openaddresses-ms-direct` uses
`datasets/source_cache/openaddresses_ms_sources` for cached source JSON and
`datasets/source_cache/openaddresses_ms_direct` for normalized CSV output.
Later runs reuse both caches. Use `--refresh-openaddresses-ms-source-cache` or
`--refresh-openaddresses-ms-direct-cache` only when you explicitly want to
re-query upstream services.

The generator is real-address-only by default. Reference records, standard
no-match bases, and adversarial no-match bases are all sampled from the loaded
real address pool. Query strings may still contain typos, missing fields, or
other resolver noise, but those variants are derived from real source records.
If the real source pool is too small, generation fails instead of inventing
replacement addresses.

## Train On Your Own Addresses

Ady Resolver is not locked to Mississippi. The checked-in model is a Mississippi
pretrained model, but the training pipeline can build a new model from any
address source you provide. The easiest path is a CSV with one of these shapes:

```csv
full_address
"101 Candace St, Newton, MS 39345"
```

or structured columns:

```csv
house_number,street_name,street_type,city,state,zip_code
101,Candace,ST,Newton,MS,39345
```

One-command custom training:

```bash
python3 src/train_from_addresses.py \
  --address-input examples/custom_addresses.csv \
  --address-format generic \
  --state MS \
  --reference-size 4 \
  --noisy-per-reference 2 \
  --model-path models/custom_stage2_model.json \
  --work-dir datasets/custom_training \
  --run-dir runs/custom_training
```

For larger address lists, raise `--reference-size` and keep
`--noisy-per-reference` high enough to create the typo patterns you care about.
The default generator setting is now `12` noisy positives per reference; use
`16` or more when you want a slower, harder training set for confidence
calibration. The resulting model can be passed to the app with `--model-path`,
and the app can use any reference directory containing a
`reference_addresses.csv`.

Important coverage note: no open public web download tested here proves every
current Mississippi address is present. The generator's
`--require-ms-county-coverage` check intentionally fails unless the input file
names cover all 82 Mississippi counties, so use it with the full MS811/MARIS
county ZIP directory.

Source comparison from the April 25, 2026 smoke tests:

- Public MARIS Point Addressing downloads: 25 ZIPs, 25 inferred counties,
  522,958 strict usable Mississippi address records after parsing the DBFs as
  MARIS/NG9-1-1 point-address data. The parser skips placeholder localities
  such as `COUNTY`/`RURAL`, falls through to real `Post_Comm` values when
  present, and recovers common street suffixes embedded in name fields.
- OpenAddresses processed Mississippi downloads: 23 ZIPs, 166,615 strict usable
  Mississippi address records after rejecting rows with ZIPs outside the
  Mississippi postal prefix range, and 14 inferred counties.
- OpenAddresses current Mississippi source catalog direct ESRI cache: 25 CSVs,
  478,023 rows seen, 413,532 strict usable Mississippi address records, and
  73,853 new canonical source addresses after de-duplicating against the older
  MARIS/OpenAddresses/manual source stack. Parcel-only source definitions whose
  city/ZIP columns are owner mailing fields are skipped unless they expose a
  real situs locality through OpenAddresses conform.
- Public MARIS parcel service: 81 parcel layers covering all 82 county names,
  with 1,970,713 non-empty `SITEADD` records before parser filtering and
  de-duplication. Use as a public fallback, not as a replacement for point
  addresses.
- The current local app cache merges MARIS parcels, public MARIS point-address
  ZIPs, archived OpenAddresses, current OpenAddresses direct ESRI CSVs, and the
  manual verified supplement. It now filters obvious parcel/location artifacts
  such as zero house numbers, non-numeric house numbers, `S/S` side-of-road
  markers, `N OF ...` descriptors, `DOD` note rows, and duplicated terminal
  street types. After filtering, it de-duplicates to 1,542,377 source reference
  addresses. It then adds 209,801 conservative ZIP-to-city consensus variants
  for source records whose city was blank, for a live resolver cache of
  1,752,178 reference addresses. These variants require at least 25 real records
  in a ZIP and a 98% dominant postal-community share.
- Full MS811/MARIS county ZIP input is the only configured path that is allowed
  to pass the all-82-county guard as true point-address input.

Train and evaluate:

```bash
python3 src/address_resolver.py \
  --mode fit-predict \
  --train-dataset-dir datasets/ms811_real/train_dataset \
  --eval-dataset-dir datasets/ms811_real/eval_dataset \
  --model-path models/stage2_model.json \
  --output-dir runs/ms811_real \
  --jobs 4
```

Stage 2 training runs a first-pass model, mines high-scoring wrong candidates
and no-match false-positive candidates, then retrains with those hard examples.
The saved model metadata records the base/mined row counts so each run can be
audited.

To include app feedback in the next training run, pass the feedback CSV:

```bash
python3 src/address_resolver.py \
  --mode fit-predict \
  --train-dataset-dir datasets/ms811_real/train_dataset \
  --eval-dataset-dir datasets/ms811_real/eval_dataset \
  --active-learning-feedback-csv datasets/source_cache/active_learning/resolver_feedback.csv \
  --model-path models/stage2_model.json \
  --output-dir runs/ms811_real_active \
  --jobs 4
```

Correction rows become positive training examples when the corrected canonical
address exists in the training reference set. Wrong rows become hard no-match
training examples.

The browser app also has an Update Training button. It runs the same
`fit-predict` flow locally using `datasets/fresh_60k_active_v2/train_dataset`,
`datasets/fresh_60k_active_v2/eval_dataset`, and
`datasets/source_cache/active_learning/resolver_feedback.csv`; successful runs
replace the current model JSON and reload it in the running app. Start the app
with `--train-dataset-dir` and `--eval-dataset-dir` if you want the button to use
another generated dataset.

To explicitly check whether Stage 2 is helping, run prediction with variant
comparison enabled:

```bash
python3 src/address_resolver.py \
  --mode predict \
  --eval-dataset-dir datasets/fresh_60k_active_v2/eval_dataset \
  --model-path models/stage2_model.json \
  --output-dir runs/stage_comparison_current \
  --compare-variants \
  --jobs 8
```

The resulting `evaluation.json` contains `variants.stage1_only`,
`variants.stage2_only`, `variants.combined`, and `comparisons.*_delta` blocks.

To evaluate a labeled sample against the full local Mississippi reference cache
as production-scale distractors, append the full reference CSV instead of
replacing the labeled eval reference IDs:

```bash
python3 src/address_resolver.py \
  --mode predict \
  --eval-dataset-dir datasets/fresh_60k_active_v2/eval_dataset \
  --model-path models/stage2_model.json \
  --output-dir runs/live_reference_smoke \
  --augment-eval-reference-csv datasets/ms_full_reference/reference_addresses.csv \
  --query-limit 1000 \
  --compare-variants \
  --jobs 1
```

Current checked-in model smoke results on April 25, 2026:

- 60k active-v2 eval, 5k-reference candidate universe: combined accuracy
  `0.8796`, recall `0.8900`, precision `0.9221`. On the same harder eval set,
  the previous default model scored combined accuracy `0.8088`, recall
  `0.8347`, precision `0.8694`.
- Stage 2 is materially better than Stage 1 on that active-v2 eval: Stage 1
  accuracy `0.5762`, Stage 2 accuracy `0.8859`, combined accuracy `0.8796`.
- 1k active-v2 live-reference smoke, 1.75M-reference candidate universe:
  combined accuracy `0.610`, recall `0.610`, precision `0.8828`, accepted
  accuracy `0.9057`. On the same live-reference slice, the previous default
  model scored combined accuracy `0.604`, recall `0.604`, precision `0.9042`,
  accepted accuracy `0.7474`.

Run the local resolver app:

```bash
python3 src/resolver_app.py
```

The app uses `datasets/ms_full_reference/reference_addresses.csv`, building it
from cached MARIS parcel CSVs plus cached public MARIS point-address ZIPs and
cached archived and direct OpenAddresses extracts when those directories exist.
It also merges `datasets/source_cache/manual_verified_ms` when present. Later
runs reuse that full reference cache. Then open `http://127.0.0.1:8765` and
type an address to see the standardized query, accepted match, confidence,
stage, and top candidates. Use the Add Verified Address form for confirmed
missing addresses; duplicates are detected and will not be added twice. Use the
feedback controls under each resolver result to capture real user misses for the
next active-learning training run. A Correct or Save Correction click also adds
an exact-input override, so resolving the same typo again can return a trusted
`feedback_override` match immediately instead of waiting for model retraining.

## Tests

```bash
python3 -m py_compile src/address_dataset_generator.py src/address_resolver.py src/resolver_app.py
python3 -m unittest discover -s tests -v
```

## License

Ady Resolver is open source under the [MIT License](LICENSE).
