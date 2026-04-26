#!/usr/bin/env python3
"""Local web UI for resolving a typed address against the trained reference set."""
from __future__ import annotations

import argparse
import json
import sys
import csv
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

from address_dataset_generator import (
    AddressRecord,
    MISSISSIPPI_COUNTIES,
    canonical_address,
    discover_input_files,
    load_real_addresses,
    mississippi_counties_in_paths,
    query_text_key,
    zip_code_matches_state,
)
from address_resolver import (
    ReferenceAddress,
    Resolver,
    Stage2Model,
    build_city_lookup,
    load_model,
    load_reference,
    normalize_text,
    standardize_parts,
)


def find_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "datasets").exists() and (parent / "models").exists():
            return parent
    return current.parents[1]


PROJECT_ROOT = find_project_root()
DEFAULT_DATASET_DIR = PROJECT_ROOT / "datasets" / "ms_full_reference"
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "maris_parcels"
DEFAULT_POINT_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "maris_point_addresses"
DEFAULT_OPENADDRESSES_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "openaddresses_ms"
DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "openaddresses_ms_direct"
DEFAULT_VERIFIED_SOURCE_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "manual_verified_ms"
DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "stage2_model.json"
ZIP_CITY_ENRICHMENT_MIN_RECORDS = 25
ZIP_CITY_ENRICHMENT_MIN_SHARE = 0.98
REFERENCE_FIELDNAMES = [
    "address_id",
    "house_number",
    "predir",
    "street_name",
    "street_type",
    "suffixdir",
    "unit_type",
    "unit_value",
    "city",
    "state",
    "zip_code",
    "canonical_address",
]
MANUAL_FIELDNAMES = [
    "address_id",
    "house_number",
    "predir",
    "street_name",
    "street_type",
    "suffixdir",
    "unit_type",
    "unit_value",
    "city",
    "state",
    "zip_code",
    "source_note",
]


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ady Resolver</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --ink: #18212f;
      --muted: #667085;
      --line: #d9dee7;
      --accent: #1565c0;
      --accent-dark: #0d47a1;
      --good: #147d4f;
      --warn: #a15c07;
      --bad: #b42318;
      --shadow: 0 12px 28px rgba(16, 24, 40, 0.08);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      min-height: 100vh;
    }

    .shell {
      width: min(1120px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 28px 0 40px;
    }

    header {
      display: flex;
      justify-content: space-between;
      gap: 24px;
      align-items: flex-end;
      padding-bottom: 18px;
      border-bottom: 1px solid var(--line);
    }

    h1 {
      font-size: 28px;
      line-height: 1.15;
      margin: 0 0 6px;
      letter-spacing: 0;
    }

    .subhead {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.45;
    }

    .status {
      display: grid;
      grid-template-columns: auto auto;
      gap: 6px 14px;
      align-items: center;
      font-size: 13px;
      color: var(--muted);
      text-align: right;
    }

    .status strong { color: var(--ink); font-weight: 650; }

    main {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 18px;
      margin-top: 20px;
      align-items: start;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
    }

    .workspace { padding: 18px; }

    label {
      display: block;
      font-size: 13px;
      font-weight: 700;
      margin-bottom: 8px;
    }

    textarea,
    input {
      width: 100%;
      border: 1px solid #b7c0cf;
      border-radius: 8px;
      padding: 12px 13px;
      color: var(--ink);
      font: inherit;
      font-size: 16px;
      line-height: 1.45;
      background: #fff;
      outline: none;
    }

    textarea {
      min-height: 96px;
      resize: vertical;
    }

    input {
      min-height: 44px;
    }

    textarea:focus,
    input:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(21, 101, 192, 0.16);
    }

    .controls {
      display: flex;
      gap: 10px;
      align-items: center;
      margin-top: 12px;
    }

    button {
      border: 0;
      border-radius: 8px;
      padding: 10px 14px;
      font: inherit;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      background: #e8edf5;
      color: var(--ink);
    }

    button.primary {
      background: var(--accent);
      color: #fff;
    }

    button.primary:hover { background: var(--accent-dark); }
    button:hover { filter: brightness(0.98); }
    button:disabled { opacity: 0.55; cursor: default; }

    .hint {
      color: var(--muted);
      font-size: 13px;
      margin-left: auto;
    }

    .result {
      margin-top: 18px;
      border-top: 1px solid var(--line);
      padding-top: 18px;
      min-height: 280px;
    }

    .empty {
      color: var(--muted);
      padding: 44px 0;
      text-align: center;
      font-size: 14px;
    }

    .verdict {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 14px;
      margin-bottom: 14px;
    }

    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      border-radius: 999px;
      padding: 5px 9px;
      font-size: 12px;
      font-weight: 800;
      letter-spacing: 0;
      white-space: nowrap;
    }

    .badge.match { color: var(--good); background: #e8f5ef; }
    .badge.review { color: var(--warn); background: #fff4df; }
    .badge.none { color: var(--bad); background: #ffebe9; }

    .confidence {
      font-size: 13px;
      color: var(--muted);
      white-space: nowrap;
    }

    .answer {
      border: 1px solid #cdd6e3;
      border-radius: 8px;
      padding: 14px;
      background: #fbfcfe;
      margin-bottom: 14px;
    }

    .answer .address {
      font-size: 20px;
      line-height: 1.35;
      font-weight: 750;
      overflow-wrap: anywhere;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 12px;
    }

    .kv {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      min-height: 62px;
    }

    .kv span {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 4px;
      font-weight: 650;
    }

    .kv strong {
      display: block;
      font-size: 14px;
      line-height: 1.3;
      overflow-wrap: anywhere;
    }

    .side { overflow: hidden; }

    .side h2 {
      font-size: 15px;
      line-height: 1.2;
      padding: 14px 14px 10px;
      margin: 0;
      border-bottom: 1px solid var(--line);
    }

    .examples {
      display: grid;
      gap: 8px;
      padding: 12px;
    }

    .add-form {
      border-top: 1px solid var(--line);
      padding: 12px;
      display: grid;
      gap: 10px;
    }

    .add-form textarea {
      min-height: 74px;
      font-size: 14px;
    }

    .add-status {
      min-height: 18px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }

    .add-status.ok { color: var(--good); font-weight: 700; }
    .add-status.bad { color: var(--bad); font-weight: 700; }

    .example {
      text-align: left;
      background: #f8fafc;
      border: 1px solid var(--line);
      color: var(--ink);
      font-weight: 600;
      line-height: 1.35;
      padding: 10px;
    }

    .candidates {
      display: grid;
      gap: 8px;
      margin-top: 14px;
    }

    .candidate {
      display: grid;
      grid-template-columns: 56px 1fr;
      gap: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #fff;
    }

    .score {
      font-variant-numeric: tabular-nums;
      color: var(--accent-dark);
      font-weight: 800;
      font-size: 13px;
    }

    .candidate-address {
      font-size: 13px;
      font-weight: 650;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }

    .candidate-id {
      color: var(--muted);
      font-size: 12px;
      margin-top: 3px;
    }

    .error {
      border: 1px solid #f1a29b;
      background: #fff5f4;
      color: var(--bad);
      border-radius: 8px;
      padding: 12px;
      font-size: 14px;
      font-weight: 650;
    }

    @media (max-width: 820px) {
      .shell { width: min(100vw - 20px, 760px); padding-top: 16px; }
      header { display: block; }
      .status { text-align: left; margin-top: 12px; }
      main { grid-template-columns: 1fr; }
      .grid { grid-template-columns: 1fr; }
      .controls { flex-wrap: wrap; }
      .hint { width: 100%; margin-left: 0; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <header>
      <div>
        <h1>Ady Resolver</h1>
        <p class="subhead">Current Mississippi reference dataset.</p>
      </div>
      <div class="status" id="status">
        <span>Dataset</span><strong>loading</strong>
        <span>References</span><strong>loading</strong>
      </div>
    </header>

    <main>
      <section class="panel workspace">
        <label for="address">Address</label>
        <textarea id="address" spellcheck="false" autocomplete="off" placeholder="563 Davis Crossing Rd MS 39046"></textarea>
        <div class="controls">
          <button class="primary" id="resolve">Resolve</button>
          <button id="clear">Clear</button>
        </div>
        <div class="result" id="result">
          <div class="empty">No address resolved.</div>
        </div>
      </section>

      <aside class="panel side">
        <h2>Try A Current Reference</h2>
        <div class="examples" id="examples"></div>
        <h2>Add Verified Address</h2>
        <div class="add-form">
          <div>
            <label for="add-address">Address</label>
            <textarea id="add-address" spellcheck="false" autocomplete="off" placeholder="102 Candace St Newton MS 39345"></textarea>
          </div>
          <div>
            <label for="source-note">Source Note</label>
            <input id="source-note" autocomplete="off" placeholder="public listing, county site, USPS lookup" />
          </div>
          <button class="primary" id="add-verified">Add</button>
          <div class="add-status" id="add-status"></div>
        </div>
      </aside>
    </main>
  </div>

  <script>
    const address = document.getElementById("address");
    const result = document.getElementById("result");
    const resolveButton = document.getElementById("resolve");
    const clearButton = document.getElementById("clear");
    const statusBox = document.getElementById("status");
    const examples = document.getElementById("examples");
    const addAddress = document.getElementById("add-address");
    const sourceNote = document.getElementById("source-note");
    const addVerifiedButton = document.getElementById("add-verified");
    const addStatus = document.getElementById("add-status");

    function escapeHtml(value) {
      return String(value ?? "").replace(/[&<>"']/g, ch => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      }[ch]));
    }

    function percent(value) {
      return `${Math.round((Number(value) || 0) * 100)}%`;
    }

    function renderEmpty(text) {
      result.innerHTML = `<div class="empty">${escapeHtml(text)}</div>`;
    }

    function renderError(text) {
      result.innerHTML = `<div class="error">${escapeHtml(text)}</div>`;
    }

    function renderResolution(data) {
      const hasMatch = Boolean(data.predicted_match_id);
      const needsReview = Boolean(data.needs_review);
      const badgeClass = hasMatch ? (needsReview ? "review" : "match") : "none";
      const badgeText = hasMatch ? (needsReview ? "Needs Review" : "Matched") : "No Match";
      const matchText = hasMatch ? data.predicted_canonical_address : "No reference address accepted";
      const candidates = (data.top_candidates || []).map(candidate => `
        <div class="candidate">
          <div class="score">${percent(candidate.score)}</div>
          <div>
            <div class="candidate-address">${escapeHtml(candidate.canonical_address)}</div>
            <div class="candidate-id">${escapeHtml(candidate.reference_id)}</div>
          </div>
        </div>
      `).join("");

      result.innerHTML = `
        <div class="verdict">
          <span class="badge ${badgeClass}">${badgeText}</span>
          <span class="confidence">confidence ${percent(data.confidence)}</span>
        </div>
        <div class="answer">
          <div class="address">${escapeHtml(matchText)}</div>
          <div class="grid">
            <div class="kv"><span>Input</span><strong>${escapeHtml(data.input_address)}</strong></div>
            <div class="kv"><span>Standardized</span><strong>${escapeHtml(data.standardized_address)}</strong></div>
            <div class="kv"><span>Match ID</span><strong>${escapeHtml(data.predicted_match_id || "NO_MATCH")}</strong></div>
            <div class="kv"><span>Stage</span><strong>${escapeHtml(data.stage)}</strong></div>
          </div>
        </div>
        <div class="candidates">${candidates || '<div class="empty">No candidates returned.</div>'}</div>
      `;
    }

    async function resolveAddress() {
      const value = address.value.trim();
      if (!value) {
        renderEmpty("Address is required.");
        address.focus();
        return;
      }

      resolveButton.disabled = true;
      resolveButton.textContent = "Resolving";
      try {
        const response = await fetch("/api/resolve", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ address: value })
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Resolution failed.");
        }
        renderResolution(payload);
      } catch (error) {
        renderError(error.message || "Resolution failed.");
      } finally {
        resolveButton.disabled = false;
        resolveButton.textContent = "Resolve";
      }
    }

    async function loadHealth() {
      const response = await fetch("/api/health");
      const data = await response.json();
      statusBox.innerHTML = `
        <span>Dataset</span><strong>${escapeHtml(data.dataset_name)}</strong>
        <span>References</span><strong>${Number(data.reference_count).toLocaleString()}</strong>
      `;
      examples.innerHTML = (data.examples || []).map(example => `
        <button class="example" type="button">${escapeHtml(example)}</button>
      `).join("");
      examples.querySelectorAll(".example").forEach(button => {
        button.addEventListener("click", () => {
          address.value = button.textContent;
          resolveAddress();
        });
      });
    }

    function renderAddStatus(text, kind = "") {
      addStatus.className = `add-status ${kind}`.trim();
      addStatus.textContent = text;
    }

    async function addVerifiedAddress() {
      const value = addAddress.value.trim();
      if (!value) {
        renderAddStatus("Address is required.", "bad");
        addAddress.focus();
        return;
      }

      addVerifiedButton.disabled = true;
      addVerifiedButton.textContent = "Adding";
      renderAddStatus("");
      try {
        const response = await fetch("/api/add-address", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ address: value, source_note: sourceNote.value.trim() })
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Add failed.");
        }
        renderAddStatus(payload.already_exists ? "Already in reference cache." : "Added to verified cache.", "ok");
        address.value = payload.canonical_address;
        await loadHealth();
        await resolveAddress();
      } catch (error) {
        renderAddStatus(error.message || "Add failed.", "bad");
      } finally {
        addVerifiedButton.disabled = false;
        addVerifiedButton.textContent = "Add";
      }
    }

    resolveButton.addEventListener("click", resolveAddress);
    addVerifiedButton.addEventListener("click", addVerifiedAddress);
    clearButton.addEventListener("click", () => {
      address.value = "";
        renderEmpty("No address resolved.");
      address.focus();
    });
    address.addEventListener("keydown", event => {
      if (event.key === "Enter" && !event.shiftKey) {
        event.preventDefault();
        resolveAddress();
      }
    });

    loadHealth().catch(() => {
      statusBox.innerHTML = "<span>Dataset</span><strong>unavailable</strong><span>References</span><strong>0</strong>";
    });
  </script>
</body>
</html>
"""


def manual_verified_csv_path() -> Path:
    return DEFAULT_VERIFIED_SOURCE_DIR / "verified_addresses.csv"


def address_record_csv_row(address_id: str, record: AddressRecord, source_note: str = "") -> Dict[str, str]:
    return {
        "address_id": address_id,
        "house_number": record.house_number,
        "predir": record.predir,
        "street_name": record.street_name.title(),
        "street_type": record.street_type,
        "suffixdir": record.suffixdir,
        "unit_type": record.unit_type,
        "unit_value": record.unit_value,
        "city": record.city.title(),
        "state": record.state,
        "zip_code": record.zip_code,
        "source_note": " ".join(source_note.split())[:240],
    }


def append_manual_verified_record(address_id: str, record: AddressRecord, source_note: str) -> None:
    path = manual_verified_csv_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANUAL_FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(address_record_csv_row(address_id, record, source_note))


def reference_csv_row(reference: ReferenceAddress) -> Dict[str, str]:
    return {
        "address_id": reference.address_id,
        "house_number": reference.house_number,
        "predir": reference.predir,
        "street_name": reference.street_name.title(),
        "street_type": reference.street_type,
        "suffixdir": reference.suffixdir,
        "unit_type": reference.unit_type,
        "unit_value": reference.unit_value,
        "city": reference.city.title(),
        "state": reference.state,
        "zip_code": reference.zip_code,
        "canonical_address": reference.canonical_address,
    }


def append_reference_record(dataset_dir: Path, reference: ReferenceAddress) -> None:
    with reference_csv_path(dataset_dir).open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REFERENCE_FIELDNAMES)
        writer.writerow(reference_csv_row(reference))


def update_reference_metadata(dataset_dir: Path) -> None:
    path = dataset_dir / "metadata.json"
    if not path.exists():
        return
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return
    for key in ("rows_seen", "rows_loaded", "reference_records"):
        metadata[key] = int(metadata.get(key, 0)) + 1
    sources = metadata.setdefault("sources", [])
    manual_source = None
    for source in sources:
        if source.get("source_format") == "address_record":
            manual_source = source
            break
    if manual_source is None:
        manual_source = {
            "source_format": "address_record",
            "state": "MS",
            "rows_seen": 0,
            "rows_loaded": 0,
            "rows_skipped": 0,
            "input_paths": [str(manual_verified_csv_path())],
        }
        sources.append(manual_source)
    manual_source["rows_seen"] = int(manual_source.get("rows_seen", 0)) + 1
    manual_source["rows_loaded"] = int(manual_source.get("rows_loaded", 0)) + 1
    if "source_format" in metadata and "address_record" not in str(metadata["source_format"]).split("+"):
        metadata["source_format"] = f"{metadata['source_format']}+address_record"
    input_paths = metadata.setdefault("input_paths", [])
    manual_path = str(manual_verified_csv_path())
    if manual_path not in input_paths:
        input_paths.append(manual_path)
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


class ResolverService:
    def __init__(self, dataset_dir: Path, model_path: Path) -> None:
        self.dataset_dir = dataset_dir
        self.model_path = model_path
        reference_rows, _ = load_reference(dataset_dir / "reference_addresses.csv")
        city_lookup = build_city_lookup(reference_rows)
        self.resolver = Resolver(reference_rows, city_lookup)
        self.model, self.accept_threshold, self.review_threshold, self.model_metadata = load_model(model_path, self.resolver)
        self.reference_count = len(reference_rows)
        self.examples = [row.canonical_address for row in reference_rows[:5]]
        self.next_reference_index = self.next_reference_number(reference_rows)

    @property
    def dataset_name(self) -> str:
        try:
            return str(self.dataset_dir.relative_to(PROJECT_ROOT))
        except ValueError:
            return str(self.dataset_dir)

    def resolve(self, raw_address: str) -> Dict[str, object]:
        parsed = self.resolver.parse(raw_address)
        stage1 = self.resolver.resolve_stage1(parsed, review_threshold=self.review_threshold)
        stage2 = self.model.resolve(parsed, accept_threshold=self.accept_threshold, review_threshold=self.review_threshold)
        combined = stage1 if stage1.predicted_match_id else stage2
        top_candidates = self.top_candidate_payload(combined)
        return {
            "input_address": raw_address,
            "standardized_address": combined.standardized_query_address,
            "predicted_match_id": combined.predicted_match_id,
            "predicted_canonical_address": combined.predicted_canonical_address,
            "confidence": combined.confidence,
            "needs_review": combined.needs_review,
            "stage": combined.stage,
            "top_candidates": top_candidates,
            "stage1": self.resolution_summary(stage1),
            "stage2": self.resolution_summary(stage2),
        }

    def top_candidate_payload(self, resolution) -> List[Dict[str, object]]:
        candidates = []
        for candidate in resolution.top_candidates[:5]:
            reference = self.resolver.reference_by_id.get(candidate.reference_id)
            candidates.append(
                {
                    "reference_id": candidate.reference_id,
                    "score": candidate.score,
                    "canonical_address": reference.canonical_address if reference else "",
                }
            )
        return candidates

    def resolution_summary(self, resolution) -> Dict[str, object]:
        return {
            "predicted_match_id": resolution.predicted_match_id,
            "predicted_canonical_address": resolution.predicted_canonical_address,
            "confidence": resolution.confidence,
            "needs_review": resolution.needs_review,
            "stage": resolution.stage,
            "standardized_address": resolution.standardized_query_address,
        }

    def health(self) -> Dict[str, object]:
        return {
            "dataset_name": self.dataset_name,
            "dataset_dir": str(self.dataset_dir),
            "model_path": str(self.model_path),
            "reference_count": self.reference_count,
            "accept_threshold": self.accept_threshold,
            "review_threshold": self.review_threshold,
            "examples": self.examples,
        }

    def next_reference_number(self, reference_rows: List[ReferenceAddress]) -> int:
        highest = 0
        for row in reference_rows:
            if not row.address_id.startswith("REF_"):
                continue
            try:
                highest = max(highest, int(row.address_id.removeprefix("REF_")))
            except ValueError:
                continue
        return highest + 1

    def infer_zip(self, parsed) -> str:
        if parsed.zip_code:
            return parsed.zip_code
        if not parsed.city or not parsed.state:
            return ""
        candidate_ids = self.resolver.by_house_city_street.get(
            (parsed.house_number, parsed.city, parsed.state, parsed.street_signature),
            [],
        )
        if not candidate_ids:
            candidate_ids = self.resolver.by_house_city_street_name.get(
                (parsed.house_number, parsed.city, parsed.state, parsed.street_name),
                [],
            )
        if not candidate_ids:
            candidate_ids = self.resolver.by_city_state.get((parsed.city, parsed.state), [])
            candidate_ids = [
                candidate_id
                for candidate_id in candidate_ids
                if self.resolver.reference_by_id[candidate_id].street_name == parsed.street_name
                and self.resolver.reference_by_id[candidate_id].street_type == parsed.street_type
            ]
        zip_codes = {
            self.resolver.reference_by_id[candidate_id].zip_code
            for candidate_id in candidate_ids
            if self.resolver.reference_by_id[candidate_id].zip_code
        }
        return next(iter(zip_codes)) if len(zip_codes) == 1 else ""

    def record_from_manual_input(self, raw_address: str) -> AddressRecord:
        parsed = self.resolver.parse(raw_address)
        state = parsed.state or "MS"
        zip_code = self.infer_zip(parsed)
        if state != "MS":
            raise ValueError("Only Mississippi addresses can be added to this resolver.")
        if not parsed.house_number or not parsed.street_name:
            raise ValueError("Address must include a house number and street.")
        if not parsed.city:
            raise ValueError("Address must include a city.")
        if not zip_code:
            raise ValueError("Address must include a ZIP, or the ZIP must be inferable from existing nearby references.")
        if not zip_code_matches_state(zip_code, state):
            raise ValueError("ZIP code does not look like a Mississippi ZIP.")
        return AddressRecord(
            address_id="",
            house_number=parsed.house_number,
            predir=parsed.predir,
            street_name=parsed.street_name,
            street_type=parsed.street_type,
            suffixdir=parsed.suffixdir,
            unit_type=parsed.unit_type,
            unit_value=parsed.unit_value,
            city=parsed.city,
            state=state,
            zip_code=zip_code,
        )

    def reference_from_record(self, record: AddressRecord, address_id: str) -> ReferenceAddress:
        standardized = standardize_parts(
            record.house_number.upper(),
            record.predir.upper(),
            normalize_text(record.street_name),
            record.street_type.upper(),
            record.suffixdir.upper(),
            record.unit_type.upper(),
            record.unit_value.upper(),
            normalize_text(record.city),
            record.state.upper(),
            record.zip_code,
        )
        street_signature = " ".join(
            bit
            for bit in [
                record.predir.upper(),
                normalize_text(record.street_name),
                record.street_type.upper(),
                record.suffixdir.upper(),
            ]
            if bit
        )
        return ReferenceAddress(
            address_id=address_id,
            canonical_address=canonical_address(record),
            house_number=record.house_number.upper(),
            predir=record.predir.upper(),
            street_name=normalize_text(record.street_name),
            street_type=record.street_type.upper(),
            suffixdir=record.suffixdir.upper(),
            unit_type=record.unit_type.upper(),
            unit_value=record.unit_value.upper(),
            city=normalize_text(record.city),
            state=record.state.upper(),
            zip_code=record.zip_code,
            standardized_address=standardized,
            street_signature=street_signature,
        )

    def next_manual_id(self) -> str:
        path = manual_verified_csv_path()
        highest = 0
        if path.exists():
            with path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    value = row.get("address_id", "")
                    if not value.startswith("MANUAL_MS_"):
                        continue
                    try:
                        highest = max(highest, int(value.removeprefix("MANUAL_MS_")))
                    except ValueError:
                        continue
        return f"MANUAL_MS_{highest + 1:06d}"

    def add_verified_address(self, raw_address: str, source_note: str) -> Dict[str, object]:
        record = self.record_from_manual_input(raw_address)
        reference_id = f"REF_{self.next_reference_index:07d}"
        reference = self.reference_from_record(record, reference_id)
        existing_ids = self.resolver.by_exact.get(reference.standardized_address, [])
        if existing_ids:
            existing = self.resolver.reference_by_id[existing_ids[0]]
            return {
                "already_exists": True,
                "reference_id": existing.address_id,
                "canonical_address": existing.canonical_address,
                "reference_count": self.reference_count,
            }

        manual_id = self.next_manual_id()
        append_manual_verified_record(manual_id, record, source_note)
        append_reference_record(self.dataset_dir, reference)
        update_reference_metadata(self.dataset_dir)
        self.resolver.add_reference(reference)
        self.reference_count += 1
        self.next_reference_index += 1
        return {
            "already_exists": False,
            "reference_id": reference.address_id,
            "canonical_address": reference.canonical_address,
            "reference_count": self.reference_count,
        }


def reference_csv_path(dataset_dir: Path) -> Path:
    return dataset_dir / "reference_addresses.csv"


def reference_cache_ready(dataset_dir: Path) -> bool:
    path = reference_csv_path(dataset_dir)
    return path.exists() and path.stat().st_size > 0


def default_source_specs() -> List[Tuple[Path, str]]:
    specs = [(DEFAULT_SOURCE_DIR, "maris_parcels")]
    if DEFAULT_POINT_SOURCE_DIR.exists():
        specs.append((DEFAULT_POINT_SOURCE_DIR, "maris"))
    if DEFAULT_OPENADDRESSES_SOURCE_DIR.exists():
        specs.append((DEFAULT_OPENADDRESSES_SOURCE_DIR, "auto"))
    if DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR.exists():
        specs.append((DEFAULT_OPENADDRESSES_DIRECT_SOURCE_DIR, "openaddresses"))
    if DEFAULT_VERIFIED_SOURCE_DIR.exists():
        specs.append((DEFAULT_VERIFIED_SOURCE_DIR, "address_record"))
    return specs


def zip_city_consensus(
    records: List[AddressRecord],
    min_records: int = ZIP_CITY_ENRICHMENT_MIN_RECORDS,
    min_share: float = ZIP_CITY_ENRICHMENT_MIN_SHARE,
) -> Dict[str, str]:
    zip_city_counts: Dict[str, Dict[str, int]] = {}
    city_display: Dict[str, str] = {}
    for record in records:
        if not record.zip_code or not record.city:
            continue
        city_key = normalize_text(record.city)
        if not city_key:
            continue
        zip_city_counts.setdefault(record.zip_code, {})
        zip_city_counts[record.zip_code][city_key] = zip_city_counts[record.zip_code].get(city_key, 0) + 1
        city_display.setdefault(city_key, record.city)

    consensus: Dict[str, str] = {}
    for zip_code, counts in zip_city_counts.items():
        total = sum(counts.values())
        if total < min_records:
            continue
        city_key, count = max(counts.items(), key=lambda item: item[1])
        if count / total >= min_share:
            consensus[zip_code] = city_display[city_key]
    return consensus


def add_zip_city_enrichment(
    records: List[AddressRecord],
    seen_keys: set[str],
    min_records: int = ZIP_CITY_ENRICHMENT_MIN_RECORDS,
    min_share: float = ZIP_CITY_ENRICHMENT_MIN_SHARE,
) -> Dict[str, object]:
    consensus = zip_city_consensus(records, min_records=min_records, min_share=min_share)
    added_records: List[AddressRecord] = []
    duplicate_count = 0
    eligible_blank_city_records = 0

    for record in records:
        if record.city or not record.zip_code:
            continue
        inferred_city = consensus.get(record.zip_code)
        if not inferred_city:
            continue
        eligible_blank_city_records += 1
        enriched = AddressRecord(
            address_id=record.address_id,
            house_number=record.house_number,
            predir=record.predir,
            street_name=record.street_name,
            street_type=record.street_type,
            suffixdir=record.suffixdir,
            unit_type=record.unit_type,
            unit_value=record.unit_value,
            city=inferred_city,
            state=record.state,
            zip_code=record.zip_code,
        )
        key = query_text_key(canonical_address(enriched))
        if key in seen_keys:
            duplicate_count += 1
            continue
        seen_keys.add(key)
        added_records.append(enriched)

    records.extend(added_records)
    return {
        "enabled": True,
        "min_records": min_records,
        "min_share": min_share,
        "consensus_zip_count": len(consensus),
        "eligible_blank_city_records": eligible_blank_city_records,
        "records_added": len(added_records),
        "duplicates_skipped": duplicate_count,
    }


def build_reference_cache(dataset_dir: Path, source_specs: List[Tuple[Path, str]]) -> None:
    if not source_specs:
        raise SystemExit("At least one real address source is required to build the app reference cache.")

    source_files: List[Path] = []
    for source_dir, _source_format in source_specs:
        if not source_dir.exists():
            raise SystemExit(f"Real address source cache not found: {source_dir}")
        source_files.extend(discover_input_files([source_dir]))

    covered_counties = mississippi_counties_in_paths(source_files)
    missing_counties = sorted(set(MISSISSIPPI_COUNTIES) - set(covered_counties))
    if missing_counties:
        raise SystemExit(
            "Cannot build full app reference cache; source is missing counties: "
            + ", ".join(missing_counties)
        )

    print("Building full Mississippi app reference cache from:")
    for source_dir, source_format in source_specs:
        print(f"  - {source_dir} ({source_format})")

    records = []
    seen_keys = set()
    source_results = []
    for source_dir, source_format in source_specs:
        load_result = load_real_addresses([source_dir], source_format=source_format, state_filter="MS")
        source_results.append(load_result)
        for record in load_result.records:
            key = query_text_key(canonical_address(record))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            records.append(record)

    if not records:
        raise SystemExit("No usable real addresses were loaded for the app reference cache.")

    zip_city_enrichment = add_zip_city_enrichment(records, seen_keys)

    dataset_dir.mkdir(parents=True, exist_ok=True)
    temporary_reference = reference_csv_path(dataset_dir).with_suffix(".csv.part")
    with temporary_reference.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=REFERENCE_FIELDNAMES)
        writer.writeheader()
        for index, record in enumerate(records, 1):
            record.address_id = f"REF_{index:07d}"
            writer.writerow(
                {
                    "address_id": record.address_id,
                    "house_number": record.house_number,
                    "predir": record.predir,
                    "street_name": record.street_name,
                    "street_type": record.street_type,
                    "suffixdir": record.suffixdir,
                    "unit_type": record.unit_type,
                    "unit_value": record.unit_value,
                    "city": record.city,
                    "state": record.state,
                    "zip_code": record.zip_code,
                    "canonical_address": canonical_address(record),
                }
            )
    temporary_reference.replace(reference_csv_path(dataset_dir))

    source_unique_count = len(records) - int(zip_city_enrichment["records_added"])
    metadata = {
        "address_source": "real",
        "source_format": "+".join(result.source_format for result in source_results),
        "state": "MS",
        "rows_seen": sum(result.rows_seen for result in source_results),
        "rows_loaded": sum(result.rows_loaded for result in source_results),
        "rows_skipped": sum(result.rows_skipped for result in source_results),
        "reference_records": len(records),
        "deduplicated_records": sum(result.rows_loaded for result in source_results) - source_unique_count,
        "source_records_after_deduplication": source_unique_count,
        "derived_records_added": int(zip_city_enrichment["records_added"]),
        "zip_city_enrichment": zip_city_enrichment,
        "sources": [
            {
                "source_format": result.source_format,
                "state": result.state,
                "rows_seen": result.rows_seen,
                "rows_loaded": result.rows_loaded,
                "rows_skipped": result.rows_skipped,
                "input_paths": result.input_paths,
            }
            for result in source_results
        ],
        "input_paths": [path for result in source_results for path in result.input_paths],
        "mississippi_county_coverage": {
            "covered_counties": covered_counties,
            "covered_county_count": len(covered_counties),
            "expected_county_count": len(MISSISSIPPI_COUNTIES),
            "missing_counties": missing_counties,
        },
    }
    (dataset_dir / "metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    print(f"Wrote {len(records):,} app reference records to {reference_csv_path(dataset_dir)}.")


class ResolverRequestHandler(BaseHTTPRequestHandler):
    service: ResolverService

    def log_message(self, format: str, *args) -> None:
        sys.stderr.write("%s - %s\n" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        route = urlparse(self.path).path
        if route == "/":
            self.send_html(HTML)
            return
        if route == "/api/health":
            self.send_json(self.service.health())
            return
        self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        route = urlparse(self.path).path
        if route == "/api/add-address":
            self.add_verified_address()
            return
        if route != "/api/resolve":
            self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)
            return

        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            if not raw_address:
                self.send_json({"error": "Address is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(self.service.resolve(raw_address))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def add_verified_address(self) -> None:
        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            source_note = str(payload.get("source_note", "")).strip()
            if not raw_address:
                self.send_json({"error": "Address is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(self.service.add_verified_address(raw_address, source_note))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def read_json_body(self) -> Dict[str, object]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def send_html(self, body: str) -> None:
        encoded = body.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def send_json(self, payload: Dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a local web UI for resolving typed addresses.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind.")
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR, help="Dataset directory with reference_addresses.csv.")
    parser.add_argument("--real-address-input", type=Path, action="append", help="Real address cache used to build the app reference cache. May be repeated. Defaults to cached MARIS parcels plus cached MARIS point addresses when available.")
    parser.add_argument("--real-address-format", default="auto", choices=["auto", "maris", "maris_parcels", "nad", "openaddresses", "address_record"], help="Input schema for custom --real-address-input values.")
    parser.add_argument("--rebuild-reference-cache", action="store_true", help="Rebuild the app reference cache from --real-address-input before starting.")
    parser.add_argument("--model-path", type=Path, default=DEFAULT_MODEL_PATH, help="Saved Stage 2 model JSON.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    dataset_dir = args.dataset_dir.expanduser().resolve()
    if args.real_address_input:
        source_specs = [(path.expanduser().resolve(), args.real_address_format) for path in args.real_address_input]
    else:
        source_specs = [(path.expanduser().resolve(), source_format) for path, source_format in default_source_specs()]
    model_path = args.model_path.expanduser().resolve()
    if args.rebuild_reference_cache or not reference_cache_ready(dataset_dir):
        build_reference_cache(dataset_dir, source_specs)
    if not reference_cache_ready(dataset_dir):
        raise SystemExit(f"Reference CSV not found: {reference_csv_path(dataset_dir)}")
    if not model_path.exists():
        raise SystemExit(f"Model JSON not found: {model_path}")

    print(f"Loading resolver reference cache from {reference_csv_path(dataset_dir)}...")
    service = ResolverService(dataset_dir, model_path)
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
