#!/usr/bin/env python3
"""Static browser UI for the local resolver app."""
from __future__ import annotations

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
