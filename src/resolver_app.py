#!/usr/bin/env python3
"""Local web UI for resolving a typed address against the trained reference set."""
from __future__ import annotations

import argparse
import json
import sys
import csv
import io
import re
import subprocess
import threading
import zipfile
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse
from xml.etree import ElementTree
from xml.sax.saxutils import escape as xml_escape

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
    choose_combined_resolution,
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
DEFAULT_ACTIVE_LEARNING_DIR = PROJECT_ROOT / "datasets" / "source_cache" / "active_learning"
DEMO_DATASET_DIR = PROJECT_ROOT / "examples" / "demo_reference"
DEFAULT_MODEL_PATH = PROJECT_ROOT / "models" / "stage2_model.json"
DEFAULT_TRAIN_DATASET_DIR = PROJECT_ROOT / "datasets" / "fresh_60k_active_v2" / "train_dataset"
DEFAULT_EVAL_DATASET_DIR = PROJECT_ROOT / "datasets" / "fresh_60k_active_v2" / "eval_dataset"
DEFAULT_TRAINING_OUTPUT_DIR = PROJECT_ROOT / "runs" / "app_training"
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
    "source_quality",
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
FEEDBACK_FIELDNAMES = [
    "created_at",
    "feedback_type",
    "input_address",
    "standardized_address",
    "predicted_match_id",
    "predicted_canonical_address",
    "confidence",
    "stage",
    "correct_address",
    "correct_reference_id",
    "correct_canonical_address",
    "top_candidates",
]


HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ady Resolver</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b0d10;
      --panel: #151a20;
      --panel-soft: #101419;
      --panel-raised: #1b222a;
      --ink: #f3f6f8;
      --muted: #9aa5b1;
      --line: #2a333d;
      --line-strong: #394653;
      --accent: #2dd4bf;
      --accent-dark: #14b8a6;
      --accent-ink: #061816;
      --good: #34d399;
      --warn: #fbbf24;
      --bad: #fb7185;
      --shadow: 0 20px 60px rgba(0, 0, 0, 0.38);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      background:
        linear-gradient(180deg, rgba(45, 212, 191, 0.08), transparent 260px),
        var(--bg);
      color: var(--ink);
      min-height: 100vh;
    }

    .shell {
      width: min(1180px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 30px 0 44px;
    }

    header {
      display: flex;
      justify-content: space-between;
      gap: 24px;
      align-items: center;
      padding: 2px 0 20px;
      border-bottom: 1px solid var(--line);
    }

    h1 {
      font-size: 30px;
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
      background: rgba(21, 26, 32, 0.78);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px 12px;
    }

    .status strong { color: var(--ink); font-weight: 650; }

    main {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 360px;
      gap: 20px;
      margin-top: 22px;
      align-items: start;
    }

    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }

    .workspace { padding: 18px; }

    label {
      display: block;
      font-size: 13px;
      font-weight: 700;
      margin-bottom: 8px;
      color: #d6dde5;
    }

    textarea,
    input,
    select {
      width: 100%;
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 12px 13px;
      color: var(--ink);
      font: inherit;
      font-size: 16px;
      line-height: 1.45;
      background: var(--panel-soft);
      outline: none;
    }

    textarea::placeholder,
    input::placeholder { color: #697581; }

    textarea {
      min-height: 96px;
      resize: vertical;
    }

    input,
    select {
      min-height: 44px;
    }

    textarea:focus,
    input:focus,
    select:focus {
      border-color: var(--accent);
      box-shadow: 0 0 0 3px rgba(45, 212, 191, 0.16);
    }

    .controls {
      display: flex;
      gap: 10px;
      align-items: center;
      margin-top: 12px;
    }

    button {
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 10px 14px;
      font: inherit;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      background: var(--panel-raised);
      color: var(--ink);
    }

    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: var(--accent-ink);
    }

    button.primary:hover { background: var(--accent-dark); }
    button:hover { border-color: #4b5a68; }
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

    .badge.match { color: #04130e; background: var(--good); }
    .badge.review { color: #1d1300; background: var(--warn); }
    .badge.none { color: #2a050c; background: var(--bad); }

    .confidence {
      font-size: 13px;
      color: var(--muted);
      white-space: nowrap;
    }

    .answer {
      border: 1px solid var(--line-strong);
      border-radius: 8px;
      padding: 14px;
      background: var(--panel-soft);
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
      background: rgba(255, 255, 255, 0.02);
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
      padding: 15px 14px 11px;
      margin: 0;
      border-bottom: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.02);
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

    .add-import {
      border-top: 1px solid var(--line);
      padding-top: 10px;
      display: grid;
      gap: 10px;
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

    .train-form {
      border-top: 1px solid var(--line);
      padding: 12px;
      display: grid;
      gap: 10px;
    }

    .train-status {
      min-height: 18px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }

    .train-status.ok { color: var(--good); font-weight: 700; }
    .train-status.bad { color: var(--bad); font-weight: 700; }

    .training-progress {
      height: 9px;
      border: 1px solid var(--line-strong);
      border-radius: 999px;
      background: var(--panel-soft);
      overflow: hidden;
    }

    .training-progress-bar {
      width: 0;
      height: 100%;
      border-radius: inherit;
      background: linear-gradient(90deg, var(--accent), #60a5fa);
      transition: width 240ms ease;
    }

    .batch-form {
      border-top: 1px solid var(--line);
      padding: 12px;
      display: grid;
      gap: 10px;
    }

    input[type="file"] {
      min-height: auto;
      font-size: 13px;
      padding: 10px;
    }

    .batch-status {
      min-height: 18px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }

    .batch-status.ok { color: var(--good); font-weight: 700; }
    .batch-status.bad { color: var(--bad); font-weight: 700; }

    .download-link {
      color: var(--accent);
      font-size: 13px;
      font-weight: 750;
      text-decoration: none;
    }

    .feedback {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: var(--panel-soft);
      margin-top: 14px;
      display: grid;
      gap: 10px;
    }

    .feedback-actions,
    .feedback-correction {
      display: flex;
      gap: 8px;
      align-items: center;
    }

    .feedback-actions button,
    .feedback-correction button {
      min-height: 40px;
      white-space: nowrap;
    }

    .feedback-correction input {
      min-height: 40px;
      font-size: 14px;
    }

    .feedback-status {
      min-height: 18px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      overflow-wrap: anywhere;
    }

    .feedback-status.ok { color: var(--good); font-weight: 700; }
    .feedback-status.bad { color: var(--bad); font-weight: 700; }

    .example {
      text-align: left;
      background: var(--panel-soft);
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
      background: var(--panel-soft);
    }

    .score {
      font-variant-numeric: tabular-nums;
      color: var(--accent);
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
      border: 1px solid rgba(251, 113, 133, 0.55);
      background: rgba(251, 113, 133, 0.10);
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
          <div class="add-import">
            <div>
              <label for="add-verified-file">CSV or Excel File</label>
              <input id="add-verified-file" name="file" type="file" accept=".csv,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" />
            </div>
            <div>
              <label for="add-verified-column">Address Column</label>
              <select id="add-verified-column" name="address_column" disabled>
                <option value="">Choose a file first</option>
              </select>
            </div>
            <button class="primary" id="import-verified">Import Verified</button>
          </div>
          <div class="add-status" id="add-status"></div>
        </div>
        <h2>Training</h2>
        <div class="train-form">
          <button class="primary" id="update-training">Train Now</button>
          <div class="training-progress" aria-label="Training progress">
            <div class="training-progress-bar" id="training-progress-bar"></div>
          </div>
          <div class="train-status" id="train-status"></div>
        </div>
        <h2>Batch Resolve</h2>
        <form class="batch-form" id="batch-form">
          <div>
            <label for="batch-file">CSV or Excel File</label>
            <input id="batch-file" name="file" type="file" accept=".csv,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" />
          </div>
          <div>
            <label for="batch-address-column">Address Column</label>
            <select id="batch-address-column" name="address_column" disabled>
              <option value="">Choose a file first</option>
            </select>
          </div>
          <div>
            <label for="batch-id-column">ID Column</label>
            <select id="batch-id-column" name="id_column" disabled>
              <option value="">No ID column</option>
            </select>
          </div>
          <button class="primary" id="batch-submit" type="submit">Resolve File</button>
          <div class="batch-status" id="batch-status"></div>
        </form>
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
    const addVerifiedFile = document.getElementById("add-verified-file");
    const addVerifiedColumn = document.getElementById("add-verified-column");
    const importVerifiedButton = document.getElementById("import-verified");
    const addStatus = document.getElementById("add-status");
    const updateTrainingButton = document.getElementById("update-training");
    const trainStatus = document.getElementById("train-status");
    const trainingProgressBar = document.getElementById("training-progress-bar");
    const batchForm = document.getElementById("batch-form");
    const batchFile = document.getElementById("batch-file");
    const batchAddressColumn = document.getElementById("batch-address-column");
    const batchIdColumn = document.getElementById("batch-id-column");
    const batchSubmit = document.getElementById("batch-submit");
    const batchStatus = document.getElementById("batch-status");
    let lastResolution = null;
    let trainingPoll = null;
    let batchHasHeader = null;
    let addVerifiedHasHeader = null;

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
      lastResolution = data;
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
        <div class="feedback">
          <div class="feedback-actions">
            <button type="button" data-feedback="correct">Correct</button>
            <button type="button" data-feedback="wrong">Wrong</button>
          </div>
          <div class="feedback-correction">
            <input id="correct-address" autocomplete="off" placeholder="Should be this address" />
            <button type="button" data-feedback="correction">Save Correction</button>
          </div>
          <div class="feedback-status" id="feedback-status"></div>
        </div>
      `;
      attachFeedbackHandlers();
    }

    function renderFeedbackStatus(text, kind = "") {
      const feedbackStatus = document.getElementById("feedback-status");
      if (!feedbackStatus) return;
      feedbackStatus.className = `feedback-status ${kind}`.trim();
      feedbackStatus.textContent = text;
    }

    function attachFeedbackHandlers() {
      result.querySelectorAll("[data-feedback]").forEach(button => {
        button.addEventListener("click", () => submitFeedback(button.dataset.feedback));
      });
    }

    async function submitFeedback(feedbackType) {
      if (!lastResolution) {
        renderFeedbackStatus("Resolve an address first.", "bad");
        return;
      }
      const correctAddress = document.getElementById("correct-address");
      const correctionValue = correctAddress ? correctAddress.value.trim() : "";
      if (feedbackType === "correction" && !correctionValue) {
        renderFeedbackStatus("Correction address is required.", "bad");
        correctAddress?.focus();
        return;
      }

      const buttons = result.querySelectorAll("[data-feedback]");
      buttons.forEach(button => { button.disabled = true; });
      renderFeedbackStatus("Saving");
      try {
        const response = await fetch("/api/feedback", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            address: lastResolution.input_address,
            feedback_type: feedbackType,
            correct_address: correctionValue
          })
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Feedback save failed.");
        }
        const training = payload.training || {};
        const trainingNote = training.auto_training_error
          ? `; training not queued: ${training.auto_training_error}`
          : training.state === "running"
            ? training.queued
              ? "; retraining queued"
              : "; retraining started"
            : "";
        renderFeedbackStatus((payload.correct_reference_id ? `Saved ${payload.correct_reference_id}` : "Saved") + trainingNote, "ok");
        await loadTrainingStatus();
        if (payload.correct_canonical_address) {
          address.value = payload.correct_canonical_address;
          await loadHealth();
          await resolveAddress();
        }
      } catch (error) {
        renderFeedbackStatus(error.message || "Feedback save failed.", "bad");
      } finally {
        buttons.forEach(button => { button.disabled = false; });
      }
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

    function resetAddVerifiedColumns(message = "Choose a file first") {
      addVerifiedHasHeader = null;
      addVerifiedColumn.replaceChildren(new Option(message, ""));
      addVerifiedColumn.disabled = true;
    }

    function populateAddVerifiedColumns(data) {
      const columns = data.columns || [];
      addVerifiedHasHeader = Boolean(data.has_header);
      addVerifiedColumn.replaceChildren();
      if (!columns.length) {
        resetAddVerifiedColumns("No columns found");
        return;
      }
      columns.forEach(column => {
        addVerifiedColumn.add(new Option(columnLabel(column), column.value));
      });
      if (data.guessed_address_column) {
        addVerifiedColumn.value = data.guessed_address_column;
      }
      addVerifiedColumn.disabled = false;
    }

    async function loadAddVerifiedColumns() {
      const file = addVerifiedFile.files && addVerifiedFile.files[0];
      resetAddVerifiedColumns("Inspecting file");
      if (!file) {
        renderAddStatus("");
        resetAddVerifiedColumns();
        return;
      }
      renderAddStatus("Reading columns");
      try {
        const formData = new FormData();
        formData.append("file", file);
        const response = await fetch("/api/batch-columns", {
          method: "POST",
          body: formData
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Column read failed.");
        }
        populateAddVerifiedColumns(payload);
        renderAddStatus(`${payload.columns.length} columns found`, "ok");
      } catch (error) {
        resetAddVerifiedColumns("Column read failed");
        renderAddStatus(error.message || "Column read failed.", "bad");
      }
    }

    function renderTrainStatus(text, kind = "") {
      trainStatus.className = `train-status ${kind}`.trim();
      trainStatus.textContent = text;
    }

    function setTrainingProgress(value) {
      const percentValue = Math.max(0, Math.min(100, Math.round(Number(value) || 0)));
      trainingProgressBar.style.width = `${percentValue}%`;
    }

    function renderBatchStatus(content, kind = "") {
      batchStatus.className = `batch-status ${kind}`.trim();
      if (content instanceof HTMLElement) {
        batchStatus.replaceChildren(content);
      } else {
        batchStatus.textContent = content;
      }
    }

    function resetBatchColumns(message = "Choose a file first") {
      batchHasHeader = null;
      batchAddressColumn.replaceChildren(new Option(message, ""));
      batchAddressColumn.disabled = true;
      batchIdColumn.replaceChildren(new Option("No ID column", ""));
      batchIdColumn.disabled = true;
    }

    function columnLabel(column) {
      const name = column.name ? `${column.name} ` : "";
      const preview = column.preview ? ` - ${column.preview}` : "";
      return `${name}(${column.letter})${preview}`;
    }

    function populateBatchColumns(data) {
      const columns = data.columns || [];
      batchHasHeader = Boolean(data.has_header);
      batchAddressColumn.replaceChildren();
      batchIdColumn.replaceChildren(new Option("No ID column", ""));
      if (!columns.length) {
        resetBatchColumns("No columns found");
        return;
      }
      columns.forEach(column => {
        batchAddressColumn.add(new Option(columnLabel(column), column.value));
        batchIdColumn.add(new Option(columnLabel(column), column.value));
      });
      if (data.guessed_address_column) {
        batchAddressColumn.value = data.guessed_address_column;
      }
      if (data.guessed_id_column) {
        batchIdColumn.value = data.guessed_id_column;
      }
      batchAddressColumn.disabled = false;
      batchIdColumn.disabled = false;
    }

    async function loadBatchColumns() {
      const file = batchFile.files && batchFile.files[0];
      resetBatchColumns("Inspecting file");
      if (!file) {
        renderBatchStatus("");
        resetBatchColumns();
        return;
      }
      renderBatchStatus("Reading columns");
      try {
        const formData = new FormData();
        formData.append("file", file);
        const response = await fetch("/api/batch-columns", {
          method: "POST",
          body: formData
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Column read failed.");
        }
        populateBatchColumns(payload);
        renderBatchStatus(`${payload.columns.length} columns found`, "ok");
      } catch (error) {
        resetBatchColumns("Column read failed");
        renderBatchStatus(error.message || "Column read failed.", "bad");
      }
    }

    function trainingStatusText(data) {
      if (!data || data.state === "idle") {
        return "Idle";
      }
      if (data.state === "running") {
        return `Training${data.started_at ? ` since ${data.started_at}` : ""}${data.queued ? "; another run queued" : ""}`;
      }
      if (data.state === "succeeded") {
        const accuracy = data.evaluation?.variants?.combined?.accuracy;
        return accuracy === undefined ? "Training complete" : `Training complete, combined accuracy ${percent(accuracy)}`;
      }
      return data.message || "Training failed";
    }

    async function loadTrainingStatus() {
      const response = await fetch("/api/training");
      const data = await response.json();
      const running = data.state === "running";
      updateTrainingButton.disabled = running;
      updateTrainingButton.textContent = running ? "Training" : "Train Now";
      setTrainingProgress(data.progress_pct || 0);
      renderTrainStatus(trainingStatusText(data), data.state === "failed" ? "bad" : data.state === "succeeded" ? "ok" : "");
      if (running && !trainingPoll) {
        trainingPoll = window.setInterval(loadTrainingStatus, 5000);
      }
      if (!running && trainingPoll) {
        window.clearInterval(trainingPoll);
        trainingPoll = null;
      }
      if (data.state === "succeeded") {
        await loadHealth();
      }
    }

    async function startTraining() {
      updateTrainingButton.disabled = true;
      updateTrainingButton.textContent = "Training";
      setTrainingProgress(4);
      renderTrainStatus("Starting");
      try {
        const response = await fetch("/api/training/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}"
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Training start failed.");
        }
        renderTrainStatus(trainingStatusText(payload));
        await loadTrainingStatus();
      } catch (error) {
        updateTrainingButton.disabled = false;
        updateTrainingButton.textContent = "Train Now";
        renderTrainStatus(error.message || "Training start failed.", "bad");
      }
    }

    async function resolveBatch(event) {
      event.preventDefault();
      const file = batchFile.files && batchFile.files[0];
      if (!file) {
        renderBatchStatus("File is required.", "bad");
        batchFile.focus();
        return;
      }
      if (!batchAddressColumn.value) {
        renderBatchStatus("Choose the address column.", "bad");
        batchAddressColumn.focus();
        return;
      }

      batchSubmit.disabled = true;
      batchSubmit.textContent = "Resolving";
      renderBatchStatus("Resolving file");
      try {
        const formData = new FormData();
        formData.append("file", file);
        formData.append("address_column", batchAddressColumn.value);
        formData.append("id_column", batchIdColumn.value);
        if (batchHasHeader !== null) {
          formData.append("has_header", batchHasHeader ? "1" : "0");
        }
        const response = await fetch("/api/batch-resolve", {
          method: "POST",
          body: formData
        });
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          throw new Error(payload.error || "Batch resolve failed.");
        }
        const blob = await response.blob();
        const disposition = response.headers.get("Content-Disposition") || "";
        const match = disposition.match(/filename="([^"]+)"/);
        const filename = match ? match[1] : "ady_resolved_addresses.xlsx";
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = filename;
        link.className = "download-link";
        link.textContent = `Download ${filename}`;
        renderBatchStatus(link, "ok");
        link.click();
      } catch (error) {
        renderBatchStatus(error.message || "Batch resolve failed.", "bad");
      } finally {
        batchSubmit.disabled = false;
        batchSubmit.textContent = "Resolve File";
      }
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

    async function importVerifiedAddresses() {
      const file = addVerifiedFile.files && addVerifiedFile.files[0];
      if (!file) {
        renderAddStatus("File is required.", "bad");
        addVerifiedFile.focus();
        return;
      }
      if (!addVerifiedColumn.value) {
        renderAddStatus("Choose the address column.", "bad");
        addVerifiedColumn.focus();
        return;
      }

      importVerifiedButton.disabled = true;
      importVerifiedButton.textContent = "Importing";
      renderAddStatus("Importing verified addresses");
      try {
        const formData = new FormData();
        formData.append("file", file);
        formData.append("address_column", addVerifiedColumn.value);
        formData.append("source_note", sourceNote.value.trim());
        if (addVerifiedHasHeader !== null) {
          formData.append("has_header", addVerifiedHasHeader ? "1" : "0");
        }
        const response = await fetch("/api/add-addresses", {
          method: "POST",
          body: formData
        });
        const payload = await response.json();
        if (!response.ok) {
          throw new Error(payload.error || "Import failed.");
        }
        const failedText = payload.failed_count ? `, ${payload.failed_count} failed` : "";
        renderAddStatus(`Imported ${payload.added_count} new, ${payload.existing_count} already existed${failedText}.`, payload.failed_count ? "bad" : "ok");
        await loadHealth();
      } catch (error) {
        renderAddStatus(error.message || "Import failed.", "bad");
      } finally {
        importVerifiedButton.disabled = false;
        importVerifiedButton.textContent = "Import Verified";
      }
    }

    resolveButton.addEventListener("click", resolveAddress);
    addVerifiedButton.addEventListener("click", addVerifiedAddress);
    importVerifiedButton.addEventListener("click", importVerifiedAddresses);
    addVerifiedFile.addEventListener("change", loadAddVerifiedColumns);
    updateTrainingButton.addEventListener("click", startTraining);
    batchForm.addEventListener("submit", resolveBatch);
    batchFile.addEventListener("change", loadBatchColumns);
    clearButton.addEventListener("click", () => {
      address.value = "";
      lastResolution = null;
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
    loadTrainingStatus().catch(() => {
      renderTrainStatus("Training status unavailable", "bad");
    });
  </script>
</body>
</html>
"""


def manual_verified_csv_path() -> Path:
    return DEFAULT_VERIFIED_SOURCE_DIR / "verified_addresses.csv"


def active_learning_feedback_csv_path() -> Path:
    return DEFAULT_ACTIVE_LEARNING_DIR / "resolver_feedback.csv"


def append_active_learning_feedback(row: Dict[str, object], path: Optional[Path] = None) -> None:
    feedback_path = path or active_learning_feedback_csv_path()
    feedback_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not feedback_path.exists() or feedback_path.stat().st_size == 0
    normalized = {
        name: json.dumps(value, separators=(",", ":"), sort_keys=True) if isinstance(value, (list, dict)) else value
        for name, value in row.items()
    }
    with feedback_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FEEDBACK_FIELDNAMES, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(normalized)


def feedback_override_keys(input_address: str, standardized_address: str = "") -> List[str]:
    keys = []
    for value in (input_address, standardized_address):
        key = query_text_key(value)
        if key and key not in keys:
            keys.append(key)
    return keys


def load_feedback_overrides(resolver: Resolver, path: Optional[Path] = None) -> Dict[str, str]:
    feedback_path = path or active_learning_feedback_csv_path()
    if not feedback_path.exists():
        return {}
    overrides: Dict[str, str] = {}
    missing_id_corrections: List[Dict[str, str]] = []
    with feedback_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            feedback_type = row.get("feedback_type", "").strip()
            if feedback_type == "correct":
                reference_id = row.get("predicted_match_id", "").strip()
            elif feedback_type == "correction":
                reference_id = row.get("correct_reference_id", "").strip()
                if not reference_id:
                    missing_id_corrections.append(row)
                    continue
            else:
                continue
            if reference_id not in resolver.reference_by_id:
                continue
            for key in feedback_override_keys(row.get("input_address", ""), row.get("standardized_address", "")):
                overrides[key] = reference_id
    if missing_id_corrections:
        needed_canonicals = {
            query_text_key(row.get("correct_canonical_address", ""))
            for row in missing_id_corrections
            if row.get("correct_canonical_address", "")
        }
        canonical_to_id = {
            query_text_key(reference.canonical_address): reference_id
            for reference_id, reference in resolver.reference_by_id.items()
            if query_text_key(reference.canonical_address) in needed_canonicals
        }
        for row in missing_id_corrections:
            reference_id = canonical_to_id.get(query_text_key(row.get("correct_canonical_address", "")), "")
            if reference_id:
                for key in feedback_override_keys(row.get("input_address", ""), row.get("standardized_address", "")):
                    overrides[key] = reference_id
    return overrides


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
        "source_quality": f"{reference.source_quality:.3f}",
    }


def reference_fieldnames_for_path(path: Path) -> List[str]:
    if path.exists() and path.stat().st_size > 0:
        with path.open(newline="", encoding="utf-8") as handle:
            header = next(csv.reader(handle), None)
        if header:
            return header
    return REFERENCE_FIELDNAMES


def append_reference_record(dataset_dir: Path, reference: ReferenceAddress) -> None:
    path = reference_csv_path(dataset_dir)
    fieldnames = reference_fieldnames_for_path(path)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writerow(reference_csv_row(reference))


def update_reference_metadata(dataset_dir: Path, increment: int = 1) -> None:
    if increment <= 0:
        return
    path = dataset_dir / "metadata.json"
    if not path.exists():
        return
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return
    for key in ("rows_seen", "rows_loaded", "reference_records"):
        metadata[key] = int(metadata.get(key, 0)) + increment
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
    manual_source["rows_seen"] = int(manual_source.get("rows_seen", 0)) + increment
    manual_source["rows_loaded"] = int(manual_source.get("rows_loaded", 0)) + increment
    if "source_format" in metadata and "address_record" not in str(metadata["source_format"]).split("+"):
        metadata["source_format"] = f"{metadata['source_format']}+address_record"
    input_paths = metadata.setdefault("input_paths", [])
    manual_path = str(manual_verified_csv_path())
    if manual_path not in input_paths:
        input_paths.append(manual_path)
    path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")


ADDRESS_COLUMN_NAMES = {
    "ADDRESS",
    "ADDR",
    "ADDR1",
    "ADDRESS1",
    "INPUT ADDRESS",
    "INPUT",
    "RAW ADDRESS",
    "ORIGINAL ADDRESS",
    "FULL ADDRESS",
    "STREET ADDRESS",
    "MAILING ADDRESS",
    "PHYSICAL ADDRESS",
    "PROPERTY ADDRESS",
    "SITUS ADDRESS",
    "SITE ADDRESS",
    "LOCATION",
}
ID_COLUMN_NAMES = {
    "ID",
    "RECORD ID",
    "SOURCE ID",
    "ROW ID",
    "CUSTOMER ID",
    "ACCOUNT ID",
    "PARCEL ID",
    "PARCEL",
    "PID",
    "APN",
    "OBJECT ID",
    "OBJECTID",
    "FID",
}
MAX_BATCH_ADDRESSES = 20000


def normalize_header(value: str) -> str:
    return " ".join(normalize_text(re.sub(r"[_/-]+", " ", value)).split())


def spreadsheet_column_index(value: str) -> Optional[int]:
    text = value.strip()
    if not text:
        return None
    if text.isdigit():
        index = int(text) - 1
        return index if index >= 0 else None
    if not re.fullmatch(r"[A-Za-z]{1,3}", text):
        return None
    index = 0
    for char in text.upper():
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def column_matches_name(value: str, names: set[str], suffix: str = "") -> bool:
    normalized = normalize_header(value)
    if not normalized:
        return False
    if normalized in names:
        return True
    return bool(suffix and normalized.endswith(suffix) and len(normalized) <= 40)


def looks_like_header_row(row: List[str]) -> bool:
    return any(
        column_matches_name(value, ADDRESS_COLUMN_NAMES, "ADDRESS") or column_matches_name(value, ID_COLUMN_NAMES)
        for value in row
    )


def detect_data_column(
    header: List[str],
    requested_column: str = "",
    names: Optional[set[str]] = None,
    suffix: str = "",
    fallback_index: int = 0,
    label: str = "Column",
) -> Tuple[int, bool]:
    names = names or set()
    if requested_column:
        requested = normalize_header(requested_column)
        if len(requested) > 1:
            for index, value in enumerate(header):
                if normalize_header(value) == requested:
                    return index, True
        positional = spreadsheet_column_index(requested_column)
        if positional is not None:
            header_value = header[positional] if positional < len(header) else ""
            return positional, column_matches_name(header_value, names, suffix)
        for index, value in enumerate(header):
            if normalize_header(value) == requested:
                return index, True
        raise ValueError(f"{label} not found: {requested_column}")

    normalized = [normalize_header(value) for value in header]
    for index, value in enumerate(normalized):
        if value in names or (suffix and value.endswith(suffix) and len(value) <= 40):
            return index, True
    return fallback_index, False


def detect_address_column(header: List[str], requested_column: str = "") -> Tuple[int, bool]:
    return detect_data_column(header, requested_column, ADDRESS_COLUMN_NAMES, "ADDRESS", 0, "Address column")


def detect_id_column(header: List[str], requested_column: str = "") -> Tuple[Optional[int], bool]:
    if not requested_column:
        for index, value in enumerate(header):
            if column_matches_name(value, ID_COLUMN_NAMES):
                return index, True
        return None, False
    index, has_header = detect_data_column(header, requested_column, ID_COLUMN_NAMES, "", 0, "ID column")
    return index, has_header


def extract_batch_addresses(
    rows: List[List[str]],
    requested_column: str = "",
    requested_id_column: str = "",
    has_header: Optional[bool] = None,
) -> List[Tuple[int, str, str]]:
    rows = [[str(cell or "").strip() for cell in row] for row in rows if any(str(cell or "").strip() for cell in row)]
    if not rows:
        raise ValueError("The uploaded file did not contain any rows.")
    column_index, address_has_header = detect_address_column(rows[0], requested_column)
    id_index, id_has_header = detect_id_column(rows[0], requested_id_column)
    header_present = looks_like_header_row(rows[0]) if has_header is None else has_header
    start = 1 if (header_present or address_has_header or id_has_header) else 0
    extracted: List[Tuple[int, str, str]] = []
    for row_offset, row in enumerate(rows[start:], start + 1):
        value = row[column_index].strip() if column_index < len(row) else ""
        if value:
            source_id = row[id_index].strip() if id_index is not None and id_index < len(row) else ""
            extracted.append((row_offset, source_id, value))
        if len(extracted) > MAX_BATCH_ADDRESSES:
            raise ValueError(f"Batch files are limited to {MAX_BATCH_ADDRESSES:,} addresses.")
    if not extracted:
        raise ValueError("No addresses were found in the selected column.")
    return extracted


def read_csv_upload(content: bytes) -> List[List[str]]:
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = content.decode("latin-1")
    return [row for row in csv.reader(io.StringIO(text))]


def xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def column_letters_from_cell_ref(cell_ref: str) -> str:
    return "".join(char for char in cell_ref if char.isalpha())


def column_letters_to_index(letters: str) -> int:
    index = 0
    for char in letters.upper():
        index = index * 26 + (ord(char) - ord("A") + 1)
    return index - 1


def worksheet_path_from_workbook(archive: zipfile.ZipFile) -> str:
    try:
        workbook = ElementTree.fromstring(archive.read("xl/workbook.xml"))
        first_sheet = next(element for element in workbook.iter() if xml_local_name(element.tag) == "sheet")
        relation_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id", "")
        rels = ElementTree.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        for rel in rels:
            if rel.attrib.get("Id") == relation_id:
                target = rel.attrib.get("Target", "worksheets/sheet1.xml")
                return target if target.startswith("xl/") else f"xl/{target.lstrip('/')}"
    except Exception:
        pass
    return "xl/worksheets/sheet1.xml"


def read_shared_strings(archive: zipfile.ZipFile) -> List[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
    values = []
    for item in root:
        if xml_local_name(item.tag) != "si":
            continue
        values.append("".join(text.text or "" for text in item.iter() if xml_local_name(text.tag) == "t"))
    return values


def read_xlsx_upload(content: bytes) -> List[List[str]]:
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        shared_strings = read_shared_strings(archive)
        worksheet_path = worksheet_path_from_workbook(archive)
        root = ElementTree.fromstring(archive.read(worksheet_path))
        rows: List[List[str]] = []
        for row_element in root.iter():
            if xml_local_name(row_element.tag) != "row":
                continue
            values: Dict[int, str] = {}
            for cell in row_element:
                if xml_local_name(cell.tag) != "c":
                    continue
                cell_ref = cell.attrib.get("r", "")
                column_index = column_letters_to_index(column_letters_from_cell_ref(cell_ref)) if cell_ref else len(values)
                cell_type = cell.attrib.get("t", "")
                value = ""
                if cell_type == "inlineStr":
                    value = "".join(text.text or "" for text in cell.iter() if xml_local_name(text.tag) == "t")
                else:
                    value_node = next((child for child in cell if xml_local_name(child.tag) == "v"), None)
                    if value_node is not None and value_node.text is not None:
                        value = value_node.text
                        if cell_type == "s":
                            shared_index = int(value)
                            value = shared_strings[shared_index] if shared_index < len(shared_strings) else ""
                values[column_index] = value
            if values:
                rows.append([values.get(index, "") for index in range(max(values) + 1)])
        return rows


def read_upload_rows(filename: str, content: bytes) -> List[List[str]]:
    suffix = Path(filename.lower()).suffix
    if suffix == ".xlsx":
        return read_xlsx_upload(content)
    elif suffix == ".csv" or not suffix:
        return read_csv_upload(content)
    raise ValueError("Upload a .csv or .xlsx file.")


def inspect_batch_columns(filename: str, content: bytes) -> Dict[str, object]:
    rows = [[str(cell or "").strip() for cell in row] for row in read_upload_rows(filename, content)]
    rows = [row for row in rows if any(row)]
    if not rows:
        raise ValueError("The uploaded file did not contain any rows.")
    has_header = looks_like_header_row(rows[0])
    header = rows[0]
    data_start = 1 if has_header else 0
    max_columns = max(len(row) for row in rows[:25])
    address_index, _ = detect_address_column(header)
    id_index, _ = detect_id_column(header)
    columns: List[Dict[str, object]] = []
    for index in range(max_columns):
        letter = excel_column_name(index)
        name = header[index] if has_header and index < len(header) else ""
        preview = ""
        for row in rows[data_start : data_start + 10]:
            if index < len(row) and row[index]:
                preview = row[index]
                break
        if len(preview) > 52:
            preview = preview[:49].rstrip() + "..."
        columns.append(
            {
                "value": letter,
                "letter": letter,
                "name": name,
                "preview": preview,
                "index": index,
            }
        )
    return {
        "columns": columns,
        "has_header": has_header,
        "guessed_address_column": excel_column_name(address_index) if address_index is not None and address_index < max_columns else "",
        "guessed_id_column": excel_column_name(id_index) if id_index is not None and id_index < max_columns else "",
    }


def read_batch_upload(
    filename: str,
    content: bytes,
    requested_column: str = "",
    requested_id_column: str = "",
    has_header: Optional[bool] = None,
) -> List[Tuple[int, str, str]]:
    return extract_batch_addresses(read_upload_rows(filename, content), requested_column, requested_id_column, has_header)


def excel_column_name(index: int) -> str:
    index += 1
    letters = ""
    while index:
        index, remainder = divmod(index - 1, 26)
        letters = chr(ord("A") + remainder) + letters
    return letters


def xlsx_cell(ref: str, value: object, style: int = 0) -> str:
    text = "" if value is None else str(value)
    style_attr = f' s="{style}"' if style else ""
    return f'<c r="{ref}" t="inlineStr"{style_attr}><is><t>{xml_escape(text)}</t></is></c>'


def write_xlsx_report(headers: List[str], rows: List[List[object]]) -> bytes:
    worksheet_rows = []
    all_rows = [headers, *rows]
    for row_index, row in enumerate(all_rows, 1):
        cells = []
        for column_index, value in enumerate(row):
            ref = f"{excel_column_name(column_index)}{row_index}"
            cells.append(xlsx_cell(ref, value, style=1 if row_index == 1 else 0))
        worksheet_rows.append(f'<row r="{row_index}">{"".join(cells)}</row>')
    sheet_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<cols>'
        '<col min="1" max="1" width="12" customWidth="1"/>'
        '<col min="2" max="9" width="28" customWidth="1"/>'
        '<col min="10" max="15" width="36" customWidth="1"/>'
        '</cols>'
        f'<sheetData>{"".join(worksheet_rows)}</sheetData>'
        '</worksheet>'
    )
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        '<sheets><sheet name="Resolved Addresses" sheetId="1" r:id="rId1"/></sheets>'
        '</workbook>'
    )
    styles_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
        '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '</styleSheet>'
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
        '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        '</Types>'
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>'
        '</Relationships>'
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
        '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>'
        '</Relationships>'
    )
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_rels)
        archive.writestr("xl/workbook.xml", workbook_xml)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        archive.writestr("xl/styles.xml", styles_xml)
        archive.writestr("xl/worksheets/sheet1.xml", sheet_xml)
    return output.getvalue()


def batch_report_filename(filename: str) -> str:
    stem = Path(filename).stem or "addresses"
    safe_stem = re.sub(r"[^A-Za-z0-9_.-]+", "_", stem).strip("._") or "addresses"
    return f"{safe_stem}_resolved.xlsx"


class ResolverService:
    def __init__(
        self,
        dataset_dir: Path,
        model_path: Path,
        train_dataset_dir: Path = DEFAULT_TRAIN_DATASET_DIR,
        eval_dataset_dir: Path = DEFAULT_EVAL_DATASET_DIR,
        training_output_dir: Path = DEFAULT_TRAINING_OUTPUT_DIR,
        training_jobs: int = 4,
    ) -> None:
        self.dataset_dir = dataset_dir
        self.model_path = model_path
        self.train_dataset_dir = train_dataset_dir
        self.eval_dataset_dir = eval_dataset_dir
        self.training_output_dir = training_output_dir
        self.training_jobs = max(1, training_jobs)
        self.model_lock = threading.RLock()
        self.training_lock = threading.RLock()
        self.training_job: Dict[str, object] = {
            "state": "idle",
            "message": "",
            "queued": False,
            "queued_at": "",
            "queue_reason": "",
            "log_tail": [],
            "progress_pct": 0,
            "phase": "Idle",
        }
        reference_rows, _ = load_reference(dataset_dir / "reference_addresses.csv")
        city_lookup = build_city_lookup(reference_rows)
        self.resolver = Resolver(reference_rows, city_lookup)
        self.model, self.accept_threshold, self.review_threshold, self.model_metadata = load_model(model_path, self.resolver)
        self.feedback_overrides = load_feedback_overrides(self.resolver)
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
        override = self.feedback_override(raw_address, parsed.standardized_address)
        if override:
            return override
        with self.model_lock:
            model = self.model
            accept_threshold = self.accept_threshold
            review_threshold = self.review_threshold
        stage1 = self.resolver.resolve_stage1(parsed, review_threshold=review_threshold)
        stage2 = model.resolve(parsed, accept_threshold=accept_threshold, review_threshold=review_threshold)
        combined = choose_combined_resolution(stage1, stage2)
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

    def resolve_batch(
        self,
        filename: str,
        content: bytes,
        address_column: str = "",
        id_column: str = "",
        has_header: Optional[bool] = None,
    ) -> Tuple[str, bytes, int]:
        addresses = read_batch_upload(filename, content, address_column, id_column, has_header)
        headers = [
            "source_row",
            "source_id",
            "original_address",
            "standardized_address",
            "resolved_address",
            "confidence",
            "needs_review",
            "match_id",
            "stage",
            "top_candidate_1",
            "top_candidate_1_score",
            "top_candidate_2",
            "top_candidate_2_score",
            "top_candidate_3",
            "top_candidate_3_score",
        ]
        report_rows: List[List[object]] = []
        for source_row, source_id, raw_address in addresses:
            resolution = self.resolve(raw_address)
            candidates = list(resolution.get("top_candidates") or [])
            row: List[object] = [
                source_row,
                source_id,
                raw_address,
                resolution.get("standardized_address", ""),
                resolution.get("predicted_canonical_address", ""),
                f"{float(resolution.get('confidence') or 0.0):.4f}",
                "yes" if resolution.get("needs_review") else "no",
                resolution.get("predicted_match_id", ""),
                resolution.get("stage", ""),
            ]
            for index in range(3):
                if index < len(candidates):
                    candidate = candidates[index]
                    row.extend(
                        [
                            candidate.get("canonical_address", ""),
                            f"{float(candidate.get('score') or 0.0):.4f}",
                        ]
                    )
                else:
                    row.extend(["", ""])
            report_rows.append(row)
        return batch_report_filename(filename), write_xlsx_report(headers, report_rows), len(report_rows)

    def feedback_override(self, raw_address: str, standardized_address: str) -> Optional[Dict[str, object]]:
        reference_id = ""
        for key in feedback_override_keys(raw_address, standardized_address):
            reference_id = self.feedback_overrides.get(key, "")
            if reference_id:
                break
        reference = self.resolver.reference_by_id.get(reference_id)
        if reference is None:
            return None
        top_candidates = [
            {
                "reference_id": reference.address_id,
                "score": 1.0,
                "canonical_address": reference.canonical_address,
            }
        ]
        summary = {
            "predicted_match_id": reference.address_id,
            "predicted_canonical_address": reference.canonical_address,
            "confidence": 1.0,
            "needs_review": False,
            "stage": "feedback_override",
            "standardized_address": standardized_address,
        }
        return {
            "input_address": raw_address,
            "standardized_address": standardized_address,
            "predicted_match_id": reference.address_id,
            "predicted_canonical_address": reference.canonical_address,
            "confidence": 1.0,
            "needs_review": False,
            "stage": "feedback_override",
            "top_candidates": top_candidates,
            "stage1": summary,
            "stage2": summary,
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
        training_status = self.training_status()
        return {
            "dataset_name": self.dataset_name,
            "dataset_dir": str(self.dataset_dir),
            "model_path": str(self.model_path),
            "reference_count": self.reference_count,
            "accept_threshold": self.accept_threshold,
            "review_threshold": self.review_threshold,
            "feedback_override_count": len(self.feedback_overrides),
            "training_state": training_status["state"],
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
            source_quality=1.0,
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

    def import_verified_addresses(
        self,
        filename: str,
        content: bytes,
        address_column: str = "",
        source_note: str = "",
        has_header: Optional[bool] = None,
    ) -> Dict[str, object]:
        addresses = read_batch_upload(filename, content, address_column, "", has_header)
        added: List[Dict[str, object]] = []
        existing: List[Dict[str, object]] = []
        failures: List[Dict[str, object]] = []
        manual_number = int(self.next_manual_id().removeprefix("MANUAL_MS_"))
        file_note = Path(filename).name or "uploaded file"

        for source_row, _source_id, raw_address in addresses:
            row_note = " ".join(part for part in [source_note, f"import:{file_note}", f"row:{source_row}"] if part)
            try:
                record = self.record_from_manual_input(raw_address)
                reference_id = f"REF_{self.next_reference_index:07d}"
                reference = self.reference_from_record(record, reference_id)
                existing_ids = self.resolver.by_exact.get(reference.standardized_address, [])
                if existing_ids:
                    existing_reference = self.resolver.reference_by_id[existing_ids[0]]
                    existing.append(
                        {
                            "source_row": source_row,
                            "input_address": raw_address,
                            "reference_id": existing_reference.address_id,
                            "canonical_address": existing_reference.canonical_address,
                        }
                    )
                    continue

                manual_id = f"MANUAL_MS_{manual_number:06d}"
                manual_number += 1
                append_manual_verified_record(manual_id, record, row_note)
                append_reference_record(self.dataset_dir, reference)
                self.resolver.add_reference(reference)
                self.reference_count += 1
                self.next_reference_index += 1
                added.append(
                    {
                        "source_row": source_row,
                        "input_address": raw_address,
                        "reference_id": reference.address_id,
                        "canonical_address": reference.canonical_address,
                    }
                )
            except ValueError as exc:
                failures.append(
                    {
                        "source_row": source_row,
                        "input_address": raw_address,
                        "error": str(exc),
                    }
                )

        if added:
            update_reference_metadata(self.dataset_dir, len(added))
        return {
            "imported": True,
            "row_count": len(addresses),
            "added_count": len(added),
            "existing_count": len(existing),
            "failed_count": len(failures),
            "added": added[:10],
            "existing": existing[:10],
            "failures": failures[:10],
            "reference_count": self.reference_count,
        }

    def record_feedback(self, raw_address: str, feedback_type: str, correct_address: str = "") -> Dict[str, object]:
        if feedback_type not in {"correct", "wrong", "correction"}:
            raise ValueError("Feedback type must be correct, wrong, or correction.")
        if not raw_address:
            raise ValueError("Address is required.")
        if feedback_type == "correction" and not correct_address:
            raise ValueError("Correction address is required.")

        resolution = self.resolve(raw_address)
        correct_reference_id = ""
        correct_canonical_address = ""
        if feedback_type == "correction":
            correction = self.add_verified_address(correct_address, f"active learning correction for: {raw_address}")
            correct_reference_id = str(correction["reference_id"])
            correct_canonical_address = str(correction["canonical_address"])

        row = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "feedback_type": feedback_type,
            "input_address": raw_address,
            "standardized_address": resolution["standardized_address"],
            "predicted_match_id": resolution["predicted_match_id"],
            "predicted_canonical_address": resolution["predicted_canonical_address"],
            "confidence": resolution["confidence"],
            "stage": resolution["stage"],
            "correct_address": correct_address,
            "correct_reference_id": correct_reference_id,
            "correct_canonical_address": correct_canonical_address,
            "top_candidates": resolution["top_candidates"],
        }
        append_active_learning_feedback(row)
        override_reference_id = ""
        if feedback_type == "correct":
            override_reference_id = str(resolution["predicted_match_id"] or "")
        elif feedback_type == "correction":
            override_reference_id = correct_reference_id
        if override_reference_id in self.resolver.reference_by_id:
            for key in feedback_override_keys(raw_address, str(resolution["standardized_address"])):
                self.feedback_overrides[key] = override_reference_id
        training_status = self.queue_training(f"feedback:{feedback_type}")
        return {
            "saved": True,
            "feedback_path": str(active_learning_feedback_csv_path()),
            "correct_reference_id": correct_reference_id,
            "correct_canonical_address": correct_canonical_address,
            "override_applied": bool(override_reference_id),
            "training": training_status,
            "reference_count": self.reference_count,
        }

    def training_dataset_ready(self) -> bool:
        return (
            (self.train_dataset_dir / "reference_addresses.csv").exists()
            and (self.train_dataset_dir / "queries.csv").exists()
            and (self.eval_dataset_dir / "reference_addresses.csv").exists()
            and (self.eval_dataset_dir / "queries.csv").exists()
        )

    def feedback_row_count(self) -> int:
        path = active_learning_feedback_csv_path()
        if not path.exists():
            return 0
        with path.open(newline="", encoding="utf-8") as handle:
            return sum(1 for _row in csv.DictReader(handle))

    def training_status(self) -> Dict[str, object]:
        with self.training_lock:
            status = dict(self.training_job)
            status["log_tail"] = list(status.get("log_tail", []))
            if status.get("state") == "running":
                started_at = str(status.get("started_at") or "")
                elapsed_seconds = 0.0
                if started_at:
                    try:
                        elapsed_seconds = max(
                            0.0,
                            (
                                datetime.now(timezone.utc)
                                - datetime.fromisoformat(started_at)
                            ).total_seconds(),
                        )
                    except ValueError:
                        elapsed_seconds = 0.0
                estimated = min(92, 6 + int(elapsed_seconds / 3))
                status["progress_pct"] = max(int(status.get("progress_pct") or 0), estimated)
        status["train_dataset_dir"] = str(self.train_dataset_dir)
        status["eval_dataset_dir"] = str(self.eval_dataset_dir)
        status["feedback_path"] = str(active_learning_feedback_csv_path())
        status["feedback_rows"] = self.feedback_row_count()
        status["training_dataset_ready"] = self.training_dataset_ready()
        return status

    def start_training(self, trigger: str = "manual", reason: str = "manual") -> Dict[str, object]:
        if not self.training_dataset_ready():
            raise ValueError(
                "Training datasets are missing. Generate datasets/fresh_60k_active_v2 first, "
                "or start the app with --train-dataset-dir and --eval-dataset-dir."
            )
        feedback_rows = self.feedback_row_count()
        if feedback_rows <= 0:
            raise ValueError("No feedback rows found yet. Mark results Correct/Wrong or Save Correction before updating training.")
        with self.training_lock:
            if self.training_job.get("state") == "running":
                return self.training_status()
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            run_dir = self.training_output_dir / timestamp
            temp_model_path = self.model_path.with_name(f"{self.model_path.stem}.training-{timestamp}{self.model_path.suffix}")
            log_path = run_dir / "training.log"
            command = [
                sys.executable,
                str(PROJECT_ROOT / "src" / "address_resolver.py"),
                "--mode",
                "fit-predict",
                "--train-dataset-dir",
                str(self.train_dataset_dir),
                "--eval-dataset-dir",
                str(self.eval_dataset_dir),
                "--active-learning-feedback-csv",
                str(active_learning_feedback_csv_path()),
                "--model-path",
                str(temp_model_path),
                "--output-dir",
                str(run_dir),
                "--compare-variants",
                "--jobs",
                str(self.training_jobs),
            ]
            self.training_job = {
                "state": "running",
                "message": "Training started",
                "trigger": trigger,
                "reason": reason,
                "queued": False,
                "queued_at": "",
                "queue_reason": "",
                "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "finished_at": "",
                "command": command,
                "run_dir": str(run_dir),
                "log_path": str(log_path),
                "temp_model_path": str(temp_model_path),
                "returncode": None,
                "log_tail": [],
                "evaluation": {},
                "progress_pct": 4,
                "phase": "Starting",
            }
            thread = threading.Thread(
                target=self.run_training_job,
                args=(command, run_dir, temp_model_path, log_path),
                daemon=True,
            )
            thread.start()
            return self.training_status()

    def queue_training(self, reason: str) -> Dict[str, object]:
        try:
            if not self.training_dataset_ready():
                status = self.training_status()
                status["auto_training_error"] = (
                    "Training datasets are missing. Generate datasets/fresh_60k_active_v2 first, "
                    "or start the app with --train-dataset-dir and --eval-dataset-dir."
                )
                return status
            if self.feedback_row_count() <= 0:
                status = self.training_status()
                status["auto_training_error"] = "No feedback rows found yet."
                return status
            with self.training_lock:
                if self.training_job.get("state") == "running":
                    self.training_job["queued"] = True
                    self.training_job["queued_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    self.training_job["queue_reason"] = reason
                    self.training_job["message"] = "Training running; another run is queued."
                    return self.training_status()
            return self.start_training(trigger="feedback", reason=reason)
        except ValueError as exc:
            status = self.training_status()
            status["auto_training_error"] = str(exc)
            return status

    def append_training_log_line(self, line: str) -> None:
        with self.training_lock:
            log_tail = list(self.training_job.get("log_tail", []))
            log_tail.append(line.rstrip())
            self.training_job["log_tail"] = log_tail[-40:]
            lowered = line.lower()
            progress = int(self.training_job.get("progress_pct") or 0)
            if "loaded" in lowered and "feedback" in lowered:
                progress = max(progress, 18)
                self.training_job["phase"] = "Loading feedback"
            elif "saved stage 2 model" in lowered:
                progress = max(progress, 70)
                self.training_job["phase"] = "Evaluating"
            elif "address resolver finished" in lowered:
                progress = max(progress, 94)
                self.training_job["phase"] = "Finalizing"
            self.training_job["progress_pct"] = progress

    def reload_model(self) -> None:
        model, accept_threshold, review_threshold, metadata = load_model(self.model_path, self.resolver)
        with self.model_lock:
            self.model = model
            self.accept_threshold = accept_threshold
            self.review_threshold = review_threshold
            self.model_metadata = metadata

    def run_training_job(self, command: List[str], run_dir: Path, temp_model_path: Path, log_path: Path) -> None:
        run_dir.mkdir(parents=True, exist_ok=True)
        returncode = 1
        try:
            with log_path.open("w", encoding="utf-8") as log_handle:
                process = subprocess.Popen(
                    command,
                    cwd=PROJECT_ROOT,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                with self.training_lock:
                    self.training_job["pid"] = process.pid
                assert process.stdout is not None
                for line in process.stdout:
                    log_handle.write(line)
                    log_handle.flush()
                    self.append_training_log_line(line)
                returncode = process.wait()
            evaluation = {}
            evaluation_path = run_dir / "evaluation.json"
            if evaluation_path.exists():
                try:
                    evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
                except json.JSONDecodeError:
                    evaluation = {}
            if returncode == 0 and temp_model_path.exists():
                temp_model_path.replace(self.model_path)
                self.reload_model()
                state = "succeeded"
                message = "Training complete; model reloaded."
            else:
                state = "failed"
                message = f"Training failed with return code {returncode}."
        except Exception as exc:  # pragma: no cover - surfaced through app status
            state = "failed"
            message = str(exc)
            evaluation = {}
        finally:
            if temp_model_path.exists() and state != "succeeded":
                temp_model_path.unlink()
            queued_reason = ""
            with self.training_lock:
                if self.training_job.get("queued"):
                    queued_reason = str(self.training_job.get("queue_reason") or "queued_feedback")
                self.training_job["state"] = state
                self.training_job["message"] = message
                self.training_job["finished_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                self.training_job["returncode"] = returncode
                self.training_job["evaluation"] = evaluation
                self.training_job["queued"] = False
                self.training_job["queued_at"] = ""
                self.training_job["queue_reason"] = ""
                self.training_job["progress_pct"] = 100 if state == "succeeded" else int(self.training_job.get("progress_pct") or 0)
                self.training_job["phase"] = "Complete" if state == "succeeded" else "Failed"
            if queued_reason:
                self.queue_training(queued_reason)


def reference_csv_path(dataset_dir: Path) -> Path:
    return dataset_dir / "reference_addresses.csv"


def reference_cache_ready(dataset_dir: Path) -> bool:
    path = reference_csv_path(dataset_dir)
    return path.exists() and path.stat().st_size > 0


def default_source_specs() -> List[Tuple[Path, str]]:
    specs = []
    if DEFAULT_SOURCE_DIR.exists():
        specs.append((DEFAULT_SOURCE_DIR, "maris_parcels"))
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
            source_quality=record.source_quality,
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
                    "source_quality": f"{record.source_quality:.3f}",
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
        if route == "/api/training":
            self.send_json(self.service.training_status())
            return
        self.send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:
        route = urlparse(self.path).path
        if route == "/api/add-address":
            self.add_verified_address()
            return
        if route == "/api/add-addresses":
            self.import_verified_addresses()
            return
        if route == "/api/feedback":
            self.record_feedback()
            return
        if route == "/api/training/start":
            self.start_training()
            return
        if route == "/api/batch-columns":
            self.batch_columns()
            return
        if route == "/api/batch-resolve":
            self.batch_resolve()
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

    def parse_multipart_form(self) -> Dict[str, object]:
        content_type = self.headers.get("Content-Type", "")
        marker = "boundary="
        if marker not in content_type:
            raise ValueError("Expected multipart form data.")
        boundary = content_type.split(marker, 1)[1].split(";", 1)[0].strip().strip('"')
        if not boundary:
            raise ValueError("Multipart boundary is missing.")
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            raise ValueError("Request body is empty.")
        if length > 32 * 1024 * 1024:
            raise ValueError("Upload is too large. Use a file under 32 MB.")

        body = self.rfile.read(length)
        delimiter = b"--" + boundary.encode("utf-8")
        form: Dict[str, object] = {}
        for raw_part in body.split(delimiter):
            part = raw_part
            if part.startswith(b"\r\n"):
                part = part[2:]
            if part.endswith(b"--"):
                part = part[:-2]
            if part.endswith(b"\r\n"):
                part = part[:-2]
            if not part or part == b"--":
                continue
            if b"\r\n\r\n" not in part:
                continue
            raw_headers, content = part.split(b"\r\n\r\n", 1)
            headers = raw_headers.decode("utf-8", errors="replace").split("\r\n")
            disposition = ""
            for header in headers:
                name, _, value = header.partition(":")
                if name.lower() == "content-disposition":
                    disposition = value.strip()
                    break
            if not disposition:
                continue
            fields: Dict[str, str] = {}
            for segment in disposition.split(";"):
                key, separator, value = segment.strip().partition("=")
                if separator:
                    fields[key] = value.strip().strip('"')
            name = fields.get("name", "")
            filename = fields.get("filename")
            if not name:
                continue
            if filename is not None:
                form[name] = {"filename": filename, "content": content}
            else:
                form[name] = content.decode("utf-8", errors="replace").strip()
        return form

    def start_training(self) -> None:
        try:
            self.read_json_body()
            self.send_json(self.service.start_training())
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def batch_columns(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            self.send_json(inspect_batch_columns(filename, content))
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def batch_resolve(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            address_column = str(form.get("address_column") or "")
            id_column = str(form.get("id_column") or "")
            has_header_value = str(form.get("has_header") or "").strip().lower()
            has_header = None
            if has_header_value in {"1", "true", "yes"}:
                has_header = True
            elif has_header_value in {"0", "false", "no"}:
                has_header = False
            output_filename, workbook, row_count = self.service.resolve_batch(
                filename,
                content,
                address_column,
                id_column,
                has_header,
            )
            self.send_bytes(
                workbook,
                content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                filename=output_filename,
                extra_headers={"X-Ady-Resolved-Rows": str(row_count)},
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def import_verified_addresses(self) -> None:
        try:
            form = self.parse_multipart_form()
            uploaded = form.get("file")
            if not isinstance(uploaded, dict):
                self.send_json({"error": "File is required."}, status=HTTPStatus.BAD_REQUEST)
                return
            filename = str(uploaded.get("filename") or "verified_addresses.csv")
            content = uploaded.get("content")
            if not isinstance(content, bytes) or not content:
                self.send_json({"error": "Uploaded file is empty."}, status=HTTPStatus.BAD_REQUEST)
                return
            address_column = str(form.get("address_column") or "")
            source_note = str(form.get("source_note") or "")
            has_header_value = str(form.get("has_header") or "").strip().lower()
            has_header = None
            if has_header_value in {"1", "true", "yes"}:
                has_header = True
            elif has_header_value in {"0", "false", "no"}:
                has_header = False
            self.send_json(
                self.service.import_verified_addresses(
                    filename,
                    content,
                    address_column,
                    source_note,
                    has_header,
                )
            )
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:  # pragma: no cover - returned to local UI
            self.send_json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def record_feedback(self) -> None:
        try:
            payload = self.read_json_body()
            raw_address = str(payload.get("address", "")).strip()
            feedback_type = str(payload.get("feedback_type", "")).strip()
            correct_address = str(payload.get("correct_address", "")).strip()
            self.send_json(self.service.record_feedback(raw_address, feedback_type, correct_address))
        except json.JSONDecodeError:
            self.send_json({"error": "Request body must be JSON."}, status=HTTPStatus.BAD_REQUEST)
        except ValueError as exc:
            self.send_json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
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

    def send_bytes(
        self,
        payload: bytes,
        content_type: str,
        filename: str,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(payload)


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
