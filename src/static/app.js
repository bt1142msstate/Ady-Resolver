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
