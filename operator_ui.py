OPERATOR_UI_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Rare Books Import Operator</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f4;
      --surface: #ffffff;
      --surface-2: #f0f4f3;
      --ink: #17211f;
      --muted: #5d6b66;
      --line: #d8dfdc;
      --accent: #0f766e;
      --accent-dark: #115e59;
      --warn: #b45309;
      --danger: #b42318;
      --ok: #15803d;
      --shadow: 0 8px 24px rgba(23, 33, 31, 0.08);
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--ink);
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      letter-spacing: 0;
    }

    button,
    input,
    textarea,
    select {
      font: inherit;
      letter-spacing: 0;
    }

    button {
      min-height: 38px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: var(--surface);
      color: var(--ink);
      padding: 8px 12px;
      cursor: pointer;
    }

    button:hover {
      border-color: var(--accent);
    }

    button.primary {
      background: var(--accent);
      border-color: var(--accent);
      color: #ffffff;
    }

    button.primary:hover {
      background: var(--accent-dark);
      border-color: var(--accent-dark);
    }

    button:disabled {
      cursor: not-allowed;
      opacity: 0.55;
    }

    input,
    textarea,
    select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #ffffff;
      color: var(--ink);
      padding: 9px 10px;
    }

    textarea {
      min-height: 74px;
      resize: vertical;
    }

    label {
      display: grid;
      gap: 6px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 600;
    }

    .app {
      width: min(1280px, calc(100% - 32px));
      margin: 0 auto;
      padding: 24px 0 32px;
    }

    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 18px;
    }

    h1,
    h2 {
      margin: 0;
      line-height: 1.15;
      font-weight: 720;
    }

    h1 {
      font-size: 24px;
    }

    h2 {
      font-size: 16px;
    }

    .version {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }

    .layout {
      display: grid;
      grid-template-columns: 360px minmax(0, 1fr);
      gap: 16px;
      align-items: start;
    }

    .panel {
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      padding: 16px;
    }

    .panel + .panel {
      margin-top: 16px;
    }

    .panel-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }

    .stack {
      display: grid;
      gap: 12px;
    }

    .row {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }

    .actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 4px;
    }

    .lookup-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
    }

    .status-line {
      min-height: 22px;
      color: var(--muted);
      font-size: 13px;
    }

    .status-line.ok {
      color: var(--ok);
    }

    .status-line.warn {
      color: var(--warn);
    }

    .status-line.error {
      color: var(--danger);
    }

    .metrics {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }

    .metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: var(--surface-2);
      min-width: 0;
    }

    .metric-value {
      display: block;
      font-size: 24px;
      font-weight: 760;
      line-height: 1;
    }

    .metric-label {
      display: block;
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
    }

    .batch-meta {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-bottom: 14px;
    }

    .meta-item {
      min-width: 0;
    }

    .meta-label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      font-weight: 650;
      margin-bottom: 4px;
    }

    .mono {
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace;
      font-size: 12px;
      overflow-wrap: anywhere;
    }

    .dropzone {
      border: 1px dashed #9fb1ac;
      border-radius: 8px;
      background: #fbfcfb;
      min-height: 156px;
      display: grid;
      place-items: center;
      text-align: center;
      padding: 20px;
      transition: border-color 0.15s ease, background 0.15s ease;
    }

    .dropzone.dragover {
      border-color: var(--accent);
      background: #eef8f6;
    }

    .drop-title {
      font-weight: 720;
      margin-bottom: 6px;
    }

    .drop-subtitle {
      color: var(--muted);
      font-size: 13px;
    }

    .file-list {
      display: grid;
      gap: 8px;
      max-height: 420px;
      overflow: auto;
      padding-right: 2px;
    }

    .batch-list {
      display: grid;
      gap: 8px;
      max-height: 360px;
      overflow: auto;
      padding-right: 2px;
    }

    .batch-row {
      display: grid;
      gap: 4px;
      width: 100%;
      text-align: left;
      border-radius: 8px;
      background: #ffffff;
    }

    .batch-row.active {
      border-color: var(--accent);
      background: #eef8f6;
    }

    .batch-row-title {
      font-weight: 720;
      overflow-wrap: anywhere;
    }

    .batch-row-meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }

    .empty-state {
      color: var(--muted);
      font-size: 13px;
      padding: 4px 0;
    }

    .file-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) 96px;
      gap: 10px;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #ffffff;
    }

    .failure-list {
      display: grid;
      gap: 8px;
      max-height: 360px;
      overflow: auto;
      padding-right: 2px;
    }

    .failure-row {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #ffffff;
    }

    .failure-title {
      font-weight: 720;
      overflow-wrap: anywhere;
    }

    .verification-list {
      display: grid;
      gap: 8px;
    }

    .verification-row {
      display: grid;
      grid-template-columns: 88px minmax(0, 1fr);
      gap: 10px;
      align-items: start;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: #ffffff;
    }

    .verification-badge {
      border-radius: 999px;
      padding: 3px 8px;
      text-align: center;
      font-size: 12px;
      font-weight: 720;
      background: var(--surface-2);
      color: var(--muted);
      text-transform: uppercase;
    }

    .verification-row.ok .verification-badge {
      background: #dcfce7;
      color: var(--ok);
    }

    .verification-row.warn .verification-badge {
      background: #fef3c7;
      color: var(--warn);
    }

    .verification-row.error .verification-badge {
      background: #fee2e2;
      color: var(--danger);
    }

    .verification-title {
      font-weight: 720;
    }

    .verification-detail {
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
    }

    .failure-detail {
      color: var(--muted);
      font-size: 12px;
      margin-top: 4px;
      overflow-wrap: anywhere;
    }

    .file-name {
      font-weight: 650;
      overflow-wrap: anywhere;
    }

    .file-detail {
      color: var(--muted);
      font-size: 12px;
      margin-top: 3px;
    }

    .progress {
      width: 100%;
      height: 8px;
      background: #e5e9e7;
      border-radius: 999px;
      overflow: hidden;
      margin-top: 8px;
    }

    .progress-bar {
      height: 100%;
      width: 0%;
      background: var(--accent);
      transition: width 0.12s ease;
    }

    .badge {
      justify-self: end;
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 8px;
      font-size: 12px;
      color: var(--muted);
      background: #ffffff;
      white-space: nowrap;
    }

    .badge.ok {
      color: var(--ok);
      border-color: rgba(21, 128, 61, 0.35);
      background: #f0fdf4;
    }

    .badge.error {
      color: var(--danger);
      border-color: rgba(180, 35, 24, 0.35);
      background: #fff7f5;
    }

    .command-box {
      display: grid;
      gap: 8px;
    }

    pre {
      margin: 0;
      min-height: 58px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #111816;
      color: #e8f3ef;
      padding: 12px;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
    }

    .run-log {
      max-height: 220px;
      min-height: 0;
      margin-top: 8px;
      overflow: auto;
      font-size: 11px;
    }

    .step-list {
      display: grid;
      gap: 8px;
      margin: 10px 0 12px;
    }

    .step-row {
      display: grid;
      grid-template-columns: 18px minmax(0, 1fr);
      gap: 8px;
      align-items: start;
      color: var(--muted);
      font-size: 13px;
    }

    .step-dot {
      width: 10px;
      height: 10px;
      margin-top: 4px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: #ffffff;
    }

    .step-row.done .step-dot {
      border-color: var(--ok);
      background: var(--ok);
    }

    .step-row.active {
      color: var(--ink);
      font-weight: 650;
    }

    .step-row.active .step-dot {
      border-color: var(--accent);
      background: var(--accent);
    }

    .step-row.error {
      color: var(--danger);
      font-weight: 650;
    }

    .step-row.error .step-dot {
      border-color: var(--danger);
      background: var(--danger);
    }

    .step-meta {
      margin-top: 2px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 500;
      overflow-wrap: anywhere;
    }

    .hidden {
      display: none;
    }

    @media (max-width: 900px) {
      .layout {
        grid-template-columns: 1fr;
      }

      .metrics,
      .batch-meta {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }

    @media (max-width: 560px) {
      .app {
        width: min(100% - 20px, 1280px);
        padding-top: 16px;
      }

      .topbar {
        align-items: flex-start;
        flex-direction: column;
      }

      .metrics,
      .batch-meta {
        grid-template-columns: 1fr;
      }

      .file-row,
      .command-box {
        grid-template-columns: 1fr;
      }

      .badge {
        justify-self: start;
      }
    }
  </style>
</head>
<body>
  <main class="app">
    <header class="topbar">
      <div>
        <h1>Rare Books Import Operator</h1>
      </div>
      <div class="version" id="version">API not checked</div>
    </header>

    <section class="layout">
      <aside>
        <div class="panel">
          <div class="panel-head">
            <h2>Access</h2>
            <button id="checkApiBtn" type="button">Check</button>
          </div>
          <div class="stack">
            <label>
              API token
              <input id="apiToken" type="password" autocomplete="off" placeholder="Bearer token">
            </label>
            <label>
              API base
              <input id="apiBase" type="url" autocomplete="off">
            </label>
            <div class="actions">
              <button id="saveAccessBtn" type="button" class="primary">Save</button>
              <button id="clearAccessBtn" type="button">Clear</button>
            </div>
            <div id="accessStatus" class="status-line"></div>
          </div>
        </div>

        <div class="panel">
          <div class="panel-head">
            <h2>Batch</h2>
          </div>
          <div class="stack">
            <label>
              Source
              <input id="sourceInput" type="text" value="web-upload">
            </label>
            <label>
              Target collection
              <select id="collectionSelect"></select>
            </label>
              <div class="lookup-row">
                <input id="collectionNewInput" type="text" placeholder="New collection">
                <button id="addCollectionBtn" type="button">Add</button>
              </div>
            <label>
              Location
              <select id="locationSelect"></select>
            </label>
              <div class="lookup-row">
                <input id="locationNewInput" type="text" placeholder="New location">
                <button id="addLocationBtn" type="button">Add</button>
              </div>
            <label>
              Notes
              <textarea id="notesInput" placeholder="Optional"></textarea>
            </label>
            <div class="actions">
              <button id="createBatchBtn" type="button" class="primary">Create Batch</button>
              <button id="refreshBatchBtn" type="button" disabled>Refresh</button>
            </div>
            <label>
              Existing batch ID
              <input id="existingBatchInput" type="text" autocomplete="off" placeholder="batch-20260505T055830Z">
            </label>
            <div class="actions">
              <button id="loadBatchBtn" type="button">Load Batch</button>
            </div>
            <div id="batchStatus" class="status-line"></div>
          </div>
        </div>

        <div class="panel">
          <div class="panel-head">
            <h2>Recent Batches</h2>
            <button id="listBatchesBtn" type="button">Refresh</button>
          </div>
          <div class="stack">
            <div id="batchListStatus" class="status-line"></div>
            <div id="batchList" class="batch-list"></div>
          </div>
        </div>
      </aside>

      <section>
        <div class="panel">
          <div class="panel-head">
            <h2>Current Batch</h2>
          </div>
          <div class="batch-meta">
            <div class="meta-item">
              <span class="meta-label">Batch ID</span>
              <div id="batchId" class="mono">None</div>
            </div>
            <div class="meta-item">
              <span class="meta-label">Bucket</span>
              <div id="bucketName" class="mono">None</div>
            </div>
            <div class="meta-item">
              <span class="meta-label">Input Prefix</span>
              <div id="inputPrefix" class="mono">None</div>
            </div>
            <div class="meta-item">
              <span class="meta-label">Results Path</span>
              <div id="resultsPath" class="mono">None</div>
            </div>
          </div>
          <div class="metrics">
            <div class="metric">
              <span class="metric-value" id="uploadedCount">0</span>
              <span class="metric-label">Uploaded</span>
            </div>
            <div class="metric">
              <span class="metric-value" id="resultTotal">0</span>
              <span class="metric-label">Rows</span>
            </div>
            <div class="metric">
              <span class="metric-value" id="resultSuccess">0</span>
              <span class="metric-label">Success</span>
            </div>
            <div class="metric">
              <span class="metric-value" id="resultFailed">0</span>
              <span class="metric-label">Failed</span>
            </div>
          </div>
        </div>

        <div class="panel">
          <div class="panel-head">
            <h2>Upload Files</h2>
            <label class="row">
              <input id="overwriteInput" type="checkbox" style="width:auto;">
              Overwrite
            </label>
          </div>
          <input id="fileInput" class="hidden" type="file" accept="image/*" multiple>
          <div id="dropzone" class="dropzone">
            <div>
              <div class="drop-title">Drop images here</div>
              <div class="drop-subtitle">or choose files from this computer</div>
              <div class="actions" style="justify-content:center; margin-top:12px;">
                <button id="chooseFilesBtn" type="button">Choose Files</button>
              </div>
            </div>
          </div>
          <div class="actions" style="margin-top:14px;">
            <button id="uploadBtn" type="button" class="primary" disabled>Upload Selected</button>
            <button id="clearFilesBtn" type="button" disabled>Clear Files</button>
          </div>
          <div id="uploadStatus" class="status-line"></div>
          <div id="fileList" class="file-list"></div>
        </div>

        <div class="panel">
          <div class="panel-head">
            <h2>Processing</h2>
            <div class="row">
              <button id="runBatchBtn" type="button" class="primary" disabled>Run Batch</button>
              <button id="copyCommandBtn" type="button" disabled>Copy Command</button>
              <button id="copyErrorReportBtn" type="button" disabled>Copy Error Report</button>
            </div>
          </div>
          <div id="runStatus" class="status-line"></div>
          <div id="runSteps" class="step-list"></div>
          <div class="command-box">
            <pre id="runCommand">Create a batch to generate the command.</pre>
          </div>
          <pre id="runLog" class="run-log hidden"></pre>
        </div>

        <div class="panel">
          <div class="panel-head">
            <h2>Batch Verification</h2>
            <button id="refreshVerificationBtn" type="button" disabled>Refresh</button>
          </div>
          <div class="metrics">
            <div class="metric">
              <span class="metric-value" id="verifyKnownFiles">0</span>
              <span class="metric-label">Known files</span>
            </div>
            <div class="metric">
              <span class="metric-value" id="verifyAirtableItems">0</span>
              <span class="metric-label">Airtable items</span>
            </div>
            <div class="metric">
              <span class="metric-value" id="verifyRemaining">0</span>
              <span class="metric-label">Waiting</span>
            </div>
            <div class="metric">
              <span class="metric-value" id="verifyFailures">0</span>
              <span class="metric-label">Unresolved</span>
            </div>
          </div>
          <div id="verificationStatus" class="status-line" style="margin-top:12px;"></div>
          <div id="verificationList" class="verification-list"></div>
        </div>

        <div class="panel">
          <div class="panel-head">
            <h2>Failures</h2>
            <div class="row">
              <button id="refreshFailuresBtn" type="button" disabled>Refresh</button>
              <button id="retryFailuresBtn" type="button" disabled>Retry Failed Files</button>
            </div>
          </div>
          <div id="failureStatus" class="status-line"></div>
          <div id="failureList" class="failure-list"></div>
        </div>
      </section>
    </section>
  </main>

  <script>
    const state = {
      token: sessionStorage.getItem("rb_api_token") || "",
      apiBase: sessionStorage.getItem("rb_api_base") || window.location.origin,
      batchId: sessionStorage.getItem("rb_batch_id") || "",
      batch: null,
      batches: [],
      lookupOptions: { collections: [], locations: [] },
      verification: null,
      failures: [],
      runPollTimer: null,
      uploadInProgress: false,
      files: [],
      uploads: new Map()
    };

    const el = (id) => document.getElementById(id);

    const nodes = {
      version: el("version"),
      apiToken: el("apiToken"),
      apiBase: el("apiBase"),
      checkApiBtn: el("checkApiBtn"),
      saveAccessBtn: el("saveAccessBtn"),
      clearAccessBtn: el("clearAccessBtn"),
      accessStatus: el("accessStatus"),
      sourceInput: el("sourceInput"),
      collectionSelect: el("collectionSelect"),
      collectionNewInput: el("collectionNewInput"),
      addCollectionBtn: el("addCollectionBtn"),
      locationSelect: el("locationSelect"),
      locationNewInput: el("locationNewInput"),
      addLocationBtn: el("addLocationBtn"),
      notesInput: el("notesInput"),
      createBatchBtn: el("createBatchBtn"),
      refreshBatchBtn: el("refreshBatchBtn"),
      existingBatchInput: el("existingBatchInput"),
      loadBatchBtn: el("loadBatchBtn"),
      listBatchesBtn: el("listBatchesBtn"),
      batchListStatus: el("batchListStatus"),
      batchList: el("batchList"),
      batchStatus: el("batchStatus"),
      batchId: el("batchId"),
      bucketName: el("bucketName"),
      inputPrefix: el("inputPrefix"),
      resultsPath: el("resultsPath"),
      uploadedCount: el("uploadedCount"),
      resultTotal: el("resultTotal"),
      resultSuccess: el("resultSuccess"),
      resultFailed: el("resultFailed"),
      overwriteInput: el("overwriteInput"),
      fileInput: el("fileInput"),
      dropzone: el("dropzone"),
      chooseFilesBtn: el("chooseFilesBtn"),
      uploadBtn: el("uploadBtn"),
      clearFilesBtn: el("clearFilesBtn"),
      uploadStatus: el("uploadStatus"),
      fileList: el("fileList"),
      runBatchBtn: el("runBatchBtn"),
      runStatus: el("runStatus"),
      runSteps: el("runSteps"),
      runCommand: el("runCommand"),
      runLog: el("runLog"),
      copyCommandBtn: el("copyCommandBtn"),
      copyErrorReportBtn: el("copyErrorReportBtn"),
      refreshVerificationBtn: el("refreshVerificationBtn"),
      verificationStatus: el("verificationStatus"),
      verificationList: el("verificationList"),
      verifyKnownFiles: el("verifyKnownFiles"),
      verifyAirtableItems: el("verifyAirtableItems"),
      verifyRemaining: el("verifyRemaining"),
      verifyFailures: el("verifyFailures"),
      refreshFailuresBtn: el("refreshFailuresBtn"),
      retryFailuresBtn: el("retryFailuresBtn"),
      failureStatus: el("failureStatus"),
      failureList: el("failureList")
    };

    const UPLOAD_CONCURRENCY = 4;
    let fileRenderScheduled = false;

    function scheduleRenderFiles() {
      if (fileRenderScheduled) return;
      fileRenderScheduled = true;
      window.requestAnimationFrame(() => {
        fileRenderScheduled = false;
        renderFiles();
      });
    }

    function setStatus(node, message, type = "") {
      node.textContent = message || "";
      node.className = `status-line ${type}`.trim();
    }

    function headers() {
      return {
        "Authorization": `Bearer ${state.token}`,
        "Content-Type": "application/json"
      };
    }

    async function apiFetch(path, options = {}) {
      const response = await fetch(`${state.apiBase}${path}`, {
        ...options,
        headers: {
          ...headers(),
          ...(options.headers || {})
        }
      });

      const text = await response.text();
      let data = {};

      if (text) {
        try {
          data = JSON.parse(text);
        } catch {
          data = { detail: text };
        }
      }

      if (!response.ok) {
        const detail = data.detail || data.error || response.statusText;
        throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
      }

      return data;
    }

    function saveAccess() {
      state.token = nodes.apiToken.value.trim();
      state.apiBase = nodes.apiBase.value.trim().replace(/\\/$/, "");
      sessionStorage.setItem("rb_api_token", state.token);
      sessionStorage.setItem("rb_api_base", state.apiBase);
      setStatus(nodes.accessStatus, "Saved for this browser session.", "ok");
    }

    function clearAccess() {
      state.token = "";
      state.apiBase = window.location.origin;
      sessionStorage.removeItem("rb_api_token");
      sessionStorage.removeItem("rb_api_base");
      nodes.apiToken.value = "";
      nodes.apiBase.value = state.apiBase;
      setStatus(nodes.accessStatus, "Cleared.", "warn");
      state.batches = [];
      state.lookupOptions = { collections: [], locations: [] };
      renderLookupOptions();
      renderBatchList();
    }

    function lookupNodes(kind) {
      if (kind === "collections") {
        return {
          select: nodes.collectionSelect,
          input: nodes.collectionNewInput,
          emptyLabel: "No collection"
        };
      }
      return {
        select: nodes.locationSelect,
        input: nodes.locationNewInput,
        emptyLabel: "No location"
      };
    }

    async function checkApi() {
      saveAccess();
      try {
        const data = await apiFetch("/");
        nodes.version.textContent = data.version ? `API ${data.version}` : "API ok";
        setStatus(nodes.accessStatus, "API reachable.", "ok");
        await loadLookupOptions(true);
      } catch (error) {
        nodes.version.textContent = "API check failed";
        setStatus(nodes.accessStatus, error.message, "error");
      }
    }

    function appendLookupOption(select, value, label) {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = label;
      select.appendChild(option);
    }

    function selectHasValue(select, value) {
      return Array.from(select.options).some((option) => option.value === value);
    }

    function renderLookupSelect(kind) {
      const { select, emptyLabel } = lookupNodes(kind);
      const currentValue = select.value;
      const options = state.lookupOptions[kind] || [];

      select.innerHTML = "";
      appendLookupOption(select, "", emptyLabel);

      for (const option of options) {
        appendLookupOption(select, option.name, option.name);
      }

      if (currentValue && !options.some((option) => option.name === currentValue)) {
        appendLookupOption(select, currentValue, currentValue);
      }

      select.value = currentValue || "";
    }

    function renderLookupOptions() {
      renderLookupSelect("collections");
      renderLookupSelect("locations");
    }

    function lookupDiagnosticMessage(data) {
      const diagnostics = data.diagnostics || {};
      const collectionCount = data.collections?.length || 0;
      const locationCount = data.locations?.length || 0;
      const warnings = [
        ...(diagnostics.collections?.warnings || []),
        ...(diagnostics.locations?.warnings || [])
      ];
      const counts = `Loaded ${collectionCount} collection${collectionCount === 1 ? "" : "s"} and ${locationCount} location${locationCount === 1 ? "" : "s"}.`;
      const legacyAdded = diagnostics.collections?.legacy_options_added || 0;
      const legacyText = legacyAdded
        ? ` Included ${legacyAdded} legacy Collection select option${legacyAdded === 1 ? "" : "s"}.`
        : "";
      return {
        message: warnings.length ? `${counts}${legacyText} ${warnings.join(" ")}` : `${counts}${legacyText}`,
        type: warnings.length ? "warn" : "ok"
      };
    }

    function setLookupValue(kind, value) {
      const { select, input } = lookupNodes(kind);
      const options = state.lookupOptions[kind] || [];
      value = (value || "").trim();

      input.value = "";
      if (value && !options.some((option) => option.name === value) && !selectHasValue(select, value)) {
        appendLookupOption(select, value, value);
      }
      select.value = value;
    }

    function selectedLookupValue(kind) {
      const { select, input } = lookupNodes(kind);
      return (input.value.trim() || select.value.trim());
    }

    async function loadLookupOptions(quiet = false) {
      if (!state.token) return;

      try {
        const data = await apiFetch("/airtable-options");
        state.lookupOptions = {
          collections: data.collections || [],
          locations: data.locations || []
        };
        renderLookupOptions();
        const diagnostic = lookupDiagnosticMessage(data);
        if (!quiet || diagnostic.type === "warn" || !state.lookupOptions.collections.length || !state.lookupOptions.locations.length) {
          setStatus(nodes.batchStatus, diagnostic.message, diagnostic.type);
        }
      } catch (error) {
        setStatus(nodes.batchStatus, `Could not load collection/location dropdowns: ${error.message}`, "error");
      }
    }

    async function addLookupOption(kind) {
      saveAccess();
      const { input } = lookupNodes(kind);
      const label = kind === "collections" ? "collection" : "location";
      const value = input.value.trim();

      if (!value) {
        setStatus(nodes.batchStatus, `Enter a ${label} first.`, "warn");
        return;
      }

      const button = kind === "collections" ? nodes.addCollectionBtn : nodes.addLocationBtn;
      button.disabled = true;
      setStatus(nodes.batchStatus, `Adding ${label}...`);

      try {
        const data = await apiFetch(`/airtable-options/${kind}`, {
          method: "POST",
          body: JSON.stringify({ name: value })
        });
        await loadLookupOptions(true);
        setLookupValue(kind, data.name || value);
        setStatus(
          nodes.batchStatus,
          `${data.created ? "Added" : "Found existing"} ${label}: ${data.name || value}.`,
          "ok"
        );
      } catch (error) {
        setStatus(nodes.batchStatus, error.message, "error");
      } finally {
        button.disabled = false;
      }
    }

    function updateBatchView(data) {
      if (!data) {
        state.batch = null;
        nodes.batchId.textContent = "None";
        nodes.bucketName.textContent = "None";
        nodes.inputPrefix.textContent = "None";
        nodes.resultsPath.textContent = "None";
        nodes.uploadedCount.textContent = "0";
        nodes.resultTotal.textContent = "0";
        nodes.resultSuccess.textContent = "0";
        nodes.resultFailed.textContent = "0";
        nodes.runCommand.textContent = "Create a batch to generate the command.";
        nodes.refreshBatchBtn.disabled = true;
        nodes.runBatchBtn.disabled = true;
        nodes.runBatchBtn.textContent = "Run Batch";
        nodes.runBatchBtn.title = "";
        nodes.copyCommandBtn.disabled = true;
        nodes.copyErrorReportBtn.disabled = true;
        nodes.refreshVerificationBtn.disabled = true;
        nodes.refreshFailuresBtn.disabled = true;
        state.verification = null;
        state.failures = [];
        renderRunStatus(null);
        renderVerification();
        renderFailures();
        return;
      }

      state.batch = data;
      state.batchId = data.batch_id || "";
      if (state.batchId) {
        sessionStorage.setItem("rb_batch_id", state.batchId);
        nodes.existingBatchInput.value = state.batchId;
      }
      setLookupValue("collections", data.target_collection || "");
      setLookupValue("locations", data.location || "");
      nodes.batchId.textContent = data.batch_id || "None";
      nodes.bucketName.textContent = data.bucket || "None";
      nodes.inputPrefix.textContent = data.input_prefix || "None";
      nodes.resultsPath.textContent = data.results_path || "None";
      nodes.uploadedCount.textContent = String(data.uploaded_count ?? 0);
      nodes.resultTotal.textContent = String(data.results?.total ?? 0);
      nodes.resultSuccess.textContent = String(data.results?.success ?? 0);
      nodes.resultFailed.textContent = String(data.results?.failed ?? 0);
      nodes.runCommand.textContent = data.run_command || "No command available.";
      nodes.refreshBatchBtn.disabled = false;
      nodes.runBatchBtn.disabled = !data.can_run;
      nodes.runBatchBtn.textContent = runButtonLabel(data);
      nodes.runBatchBtn.title = runButtonTitle(data);
      nodes.copyCommandBtn.disabled = !data.run_command;
      nodes.copyErrorReportBtn.disabled = !data.batch_id;
      nodes.refreshVerificationBtn.disabled = false;
      nodes.refreshFailuresBtn.disabled = false;
      renderRunStatus(data.run);
      updateUploadButtons();
      renderBatchList();
      renderVerification();
      renderFailures();
    }

    function runButtonLabel(data) {
      const status = data?.run?.status || "not_started";
      const waiting = data?.uploaded_count ?? 0;

      if (status === "running") return "Running...";
      if (status === "succeeded" && waiting > 0) return "Run New Files";
      if (status === "succeeded") return "Run Complete";
      if (status === "failed" || status === "stale") return "Retry Run";
      if (waiting < 1) return "No Files Waiting";
      return "Run Batch";
    }

    function runButtonTitle(data) {
      const status = data?.run?.status || "not_started";
      const waiting = data?.uploaded_count ?? 0;

      if (status === "running") return "This batch already has an active run lock.";
      if (status === "succeeded" && waiting < 1) return "This batch has already succeeded and has no new uploads waiting.";
      if (waiting < 1 && !(status === "failed" || status === "stale")) return "Upload files before running the batch.";
      if (status === "failed" || status === "stale") return "Retry the batch. This can retry Airtable import or any remaining waiting files.";
      if (status === "succeeded" && waiting > 0) return "Run only the newly uploaded files waiting in this batch.";
      return "Start the end-to-end import for this batch.";
    }

    function setRunPolling(active) {
      if (active && !state.runPollTimer) {
        state.runPollTimer = window.setInterval(async () => {
          if (!state.batch?.batch_id) return;
          await loadBatch(state.batch.batch_id, true);
          await listBatches(true);
          await loadFailures(true);
        }, 5000);
      } else if (!active && state.runPollTimer) {
        window.clearInterval(state.runPollTimer);
        state.runPollTimer = null;
      }
    }

    function runStageLabel(stage) {
      const labels = {
        not_started: "Not started",
        queued: "Queued",
        starting: "Starting",
        batch_processor: "Batch processor",
        extracting: "Extracting files",
        results_written: "Results CSV written",
        airtable_importer: "Airtable import",
        airtable_summary: "Updating batch summary",
        complete: "Complete",
        failed: "Failed",
        stale: "Stale",
        unknown: "Unknown"
      };
      return labels[stage] || labels.unknown;
    }

    function runStageIndex(stage) {
      const indexes = {
        not_started: 0,
        queued: 0,
        starting: 0,
        batch_processor: 1,
        extracting: 2,
        results_written: 2,
        airtable_importer: 3,
        airtable_summary: 3,
        complete: 4,
        failed: 4,
        stale: 4
      };
      return indexes[stage] ?? 0;
    }

    function renderRunSteps(run) {
      const status = run?.status || "not_started";
      const stage = run?.stage || status;
      const activeIndex = runStageIndex(stage);
      const filesFound = run?.files_found;
      const processed = run?.files_processed ?? 0;
      const succeeded = run?.files_succeeded ?? 0;
      const failed = run?.files_failed ?? 0;

      const fileProgress = filesFound !== "" && filesFound !== undefined
        ? `${processed}/${filesFound} files checked, ${succeeded} success, ${failed} failed`
        : "";

      const steps = [
        {
          label: "Queued",
          meta: run?.started_at ? `Started ${formatBatchDate(run.started_at)}` : ""
        },
        {
          label: "Batch processor",
          meta: filesFound !== "" && filesFound !== undefined ? `${filesFound} files found` : ""
        },
        {
          label: "Extracting files",
          meta: run?.current_file || fileProgress
        },
        {
          label: "Airtable import",
          meta: run?.imported_records !== "" && run?.imported_records !== undefined
            ? `${run.imported_records} records imported`
            : ""
        },
        {
          label: "Complete",
          meta: run?.finished_at ? `Finished ${formatBatchDate(run.finished_at)}` : ""
        }
      ];

      nodes.runSteps.innerHTML = "";
      for (const [index, step] of steps.entries()) {
        const row = document.createElement("div");
        let className = "step-row";
        if ((status === "failed" || status === "stale") && index === activeIndex) {
          className += " error";
        } else if (status === "succeeded" || index < activeIndex) {
          className += " done";
        } else if (status === "running" && index === activeIndex) {
          className += " active";
        }
        row.className = className;

        const dot = document.createElement("div");
        dot.className = "step-dot";

        const content = document.createElement("div");
        const label = document.createElement("div");
        label.textContent = step.label;
        content.appendChild(label);

        if (step.meta) {
          const meta = document.createElement("div");
          meta.className = "step-meta";
          meta.textContent = step.meta;
          content.appendChild(meta);
        }

        row.append(dot, content);
        nodes.runSteps.appendChild(row);
      }
    }

    function renderRunStatus(run) {
      const status = run?.status || "not_started";

      if (status === "running") {
        setStatus(nodes.runStatus, `Batch is running: ${runStageLabel(run?.stage)}.`, "warn");
        setRunPolling(true);
      } else if (status === "succeeded") {
        setStatus(nodes.runStatus, "Batch run finished successfully.", "ok");
        setRunPolling(false);
      } else if (status === "failed") {
        setStatus(nodes.runStatus, run?.error || "Batch run failed.", "error");
        setRunPolling(false);
      } else if (status === "stale") {
        setStatus(nodes.runStatus, run?.error || "Previous run looks stale. Retry is available.", "warn");
        setRunPolling(false);
      } else {
        setStatus(nodes.runStatus, "Ready to run after files are uploaded.");
        setRunPolling(false);
      }

      renderRunSteps(run);
      const logTail = (run?.log_tail || "").trim();
      if (logTail) {
        nodes.runLog.textContent = logTail;
        nodes.runLog.classList.remove("hidden");
      } else {
        nodes.runLog.textContent = "";
        nodes.runLog.classList.add("hidden");
      }
    }

    function formatBatchDate(value) {
      if (!value) return "";

      try {
        return new Intl.DateTimeFormat(undefined, {
          dateStyle: "short",
          timeStyle: "short"
        }).format(new Date(value));
      } catch {
        return value;
      }
    }

    function renderBatchList() {
      nodes.batchList.innerHTML = "";

      if (!state.batches.length) {
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.textContent = "No recent batches loaded.";
        nodes.batchList.appendChild(empty);
        return;
      }

      for (const batch of state.batches) {
        const row = document.createElement("button");
        row.type = "button";
        row.className = `batch-row ${batch.batch_id === state.batch?.batch_id ? "active" : ""}`.trim();

        const title = document.createElement("div");
        title.className = "batch-row-title";
        title.textContent = batch.batch_id;

        const meta = document.createElement("div");
        meta.className = "batch-row-meta";
        const parts = [
          formatBatchDate(batch.created_at),
          batch.target_collection ? `Collection: ${batch.target_collection}` : "",
          batch.location ? `Location: ${batch.location}` : "",
          batch.run?.status && batch.run.status !== "not_started" ? `Run: ${batch.run.status}` : "",
          `Uploads: ${batch.uploaded_count ?? 0}`,
          `Rows: ${batch.results?.total ?? 0}`,
          `Failed: ${batch.results?.failed ?? 0}`
        ].filter(Boolean);
        meta.textContent = parts.join(" | ");

        row.append(title, meta);
        row.addEventListener("click", () => loadBatch(batch.batch_id));
        nodes.batchList.appendChild(row);
      }
    }

    function renderVerification() {
      const verification = state.verification;
      nodes.verificationList.innerHTML = "";
      nodes.refreshVerificationBtn.disabled = !state.batch?.batch_id;

      if (!verification) {
        nodes.verifyKnownFiles.textContent = "0";
        nodes.verifyAirtableItems.textContent = "0";
        nodes.verifyRemaining.textContent = "0";
        nodes.verifyFailures.textContent = "0";
        setStatus(nodes.verificationStatus, state.batch?.batch_id ? "Run verification after processing." : "Load a batch to verify it.");
        return;
      }

      const counts = verification.counts || {};
      nodes.verifyKnownFiles.textContent = String(counts.known_file_rows ?? 0);
      nodes.verifyAirtableItems.textContent = counts.airtable_item_records === null || counts.airtable_item_records === undefined
        ? "?"
        : String(counts.airtable_item_records);
      nodes.verifyRemaining.textContent = String(counts.remaining_input_files ?? 0);
      nodes.verifyFailures.textContent = String(counts.unresolved_failure_rows ?? 0);

      const status = verification.overall_status || "warn";
      const statusText = {
        ok: "Batch verification looks healthy.",
        warn: "Batch verification has warnings.",
        error: "Batch verification found problems."
      }[status] || "Batch verification needs attention.";
      setStatus(nodes.verificationStatus, statusText, status === "error" ? "error" : status === "warn" ? "warn" : "ok");

      for (const check of verification.checks || []) {
        const row = document.createElement("div");
        row.className = `verification-row ${check.status || ""}`.trim();

        const badge = document.createElement("div");
        badge.className = "verification-badge";
        badge.textContent = check.status || "info";

        const content = document.createElement("div");
        const title = document.createElement("div");
        title.className = "verification-title";
        title.textContent = check.label || "Check";
        const detail = document.createElement("div");
        detail.className = "verification-detail";
        detail.textContent = check.detail || "";
        content.append(title, detail);

        row.append(badge, content);
        nodes.verificationList.appendChild(row);
      }
    }

    async function loadVerification(quiet = false) {
      if (!state.batch?.batch_id) {
        state.verification = null;
        renderVerification();
        return;
      }

      if (!quiet) {
        setStatus(nodes.verificationStatus, "Checking batch...");
      }
      nodes.refreshVerificationBtn.disabled = true;

      try {
        state.verification = await apiFetch(`/batches/${encodeURIComponent(state.batch.batch_id)}/verification`);
        renderVerification();
      } catch (error) {
        if (!quiet) {
          setStatus(nodes.verificationStatus, error.message, "error");
        }
      } finally {
        nodes.refreshVerificationBtn.disabled = !state.batch?.batch_id;
      }
    }

    function renderFailures() {
      nodes.failureList.innerHTML = "";
      const isRunning = state.batch?.run?.status === "running";
      const queuedForRetry = state.failures.filter((failure) => failure.retry_queued).length;
      nodes.retryFailuresBtn.disabled = !state.batch?.batch_id || isRunning || state.failures.length < 1 || queuedForRetry === state.failures.length;
      nodes.refreshFailuresBtn.disabled = !state.batch?.batch_id;

      if (!state.batch?.batch_id) {
        setStatus(nodes.failureStatus, "Load a batch to see failures.");
        return;
      }

      if (!state.failures.length) {
        setStatus(nodes.failureStatus, "No unresolved failed files for this batch.", "ok");
        const empty = document.createElement("div");
        empty.className = "empty-state";
        empty.textContent = "Failures from successful retry rows are hidden here.";
        nodes.failureList.appendChild(empty);
        return;
      }

      setStatus(
        nodes.failureStatus,
        `${state.failures.length} unresolved failed file${state.failures.length === 1 ? "" : "s"}${queuedForRetry ? `, ${queuedForRetry} queued for retry` : ""}.`,
        "warn"
      );

      for (const failure of state.failures) {
        const row = document.createElement("div");
        row.className = "failure-row";

        const title = document.createElement("div");
        title.className = "failure-title";
        title.textContent = failure.filename || failure.key || "Unknown file";

        const path = document.createElement("div");
        path.className = "failure-detail mono";
        path.textContent = failure.retry_queued
          ? `Queued: ${failure.retry_path}`
          : failure.final_gcs_path || failure.source_gcs_path || "";

        const detail = document.createElement("div");
        detail.className = "failure-detail";
        detail.textContent = failure.error || "No error message recorded.";

        row.append(title, path, detail);
        nodes.failureList.appendChild(row);
      }
    }

    async function loadFailures(quiet = false) {
      if (!state.batch?.batch_id) {
        state.failures = [];
        renderFailures();
        return;
      }

      if (!quiet) {
        setStatus(nodes.failureStatus, "Loading failures...");
      }
      nodes.refreshFailuresBtn.disabled = true;

      try {
        const data = await apiFetch(`/batches/${encodeURIComponent(state.batch.batch_id)}/failures`);
        state.failures = data.failures || [];
        renderFailures();
      } catch (error) {
        if (!quiet) {
          setStatus(nodes.failureStatus, error.message, "error");
        }
      } finally {
        nodes.refreshFailuresBtn.disabled = !state.batch?.batch_id;
      }
    }

    async function retryFailures() {
      if (!state.batch?.batch_id || !state.failures.length) return;
      saveAccess();
      nodes.retryFailuresBtn.disabled = true;
      setStatus(nodes.failureStatus, "Queueing failed files for retry...", "warn");

      try {
        const data = await apiFetch(`/batches/${encodeURIComponent(state.batch.batch_id)}/retry-failures`, {
          method: "POST",
          body: JSON.stringify({ max_files: 100 })
        });
        state.batch = data.batch || state.batch;
        state.failures = data.failures || [];
        updateBatchView(state.batch);
        renderFailures();
        await loadVerification(true);
        await listBatches(true);
        const queued = data.summary?.queued || 0;
        const alreadyQueued = data.summary?.already_queued || 0;
        setStatus(
          nodes.failureStatus,
          `Queued ${queued} file${queued === 1 ? "" : "s"} for retry${alreadyQueued ? `, ${alreadyQueued} already queued` : ""}. Click Run Batch when ready.`,
          queued || alreadyQueued ? "ok" : "warn"
        );
      } catch (error) {
        setStatus(nodes.failureStatus, error.message, "error");
      }
    }

    async function listBatches(quiet = false) {
      saveAccess();
      if (!quiet) {
        setStatus(nodes.batchListStatus, "Loading batches...");
      }
      nodes.listBatchesBtn.disabled = true;

      try {
        const data = await apiFetch("/batches?limit=20");
        state.batches = data.batches || [];
        renderBatchList();
        setStatus(nodes.batchListStatus, `Loaded ${state.batches.length} recent batches.`, "ok");
      } catch (error) {
        if (!quiet) {
          setStatus(nodes.batchListStatus, error.message, "error");
        }
      } finally {
        nodes.listBatchesBtn.disabled = false;
      }
    }

    async function loadBatch(batchId, quiet = false) {
      const trimmedBatchId = (batchId || "").trim();
      if (!trimmedBatchId) {
        setStatus(nodes.batchStatus, "Enter a batch ID to load.", "warn");
        return;
      }

      saveAccess();
      if (!quiet) {
        setStatus(nodes.batchStatus, "Loading batch...");
      }
      nodes.loadBatchBtn.disabled = true;

      try {
        const data = await apiFetch(`/batches/${encodeURIComponent(trimmedBatchId)}`);
        updateBatchView(data);
        await loadFailures(true);
        if (data.run?.status !== "running") {
          await loadVerification(true);
        }
        if (!quiet) {
          setStatus(nodes.batchStatus, `Loaded ${trimmedBatchId}.`, "ok");
        }
      } catch (error) {
        if (!quiet) {
          setStatus(nodes.batchStatus, error.message, "error");
        }
      } finally {
        nodes.loadBatchBtn.disabled = false;
      }
    }

    async function createBatch() {
      saveAccess();
      setStatus(nodes.batchStatus, "Creating batch...");
      nodes.createBatchBtn.disabled = true;

      try {
        const data = await apiFetch("/batches", {
          method: "POST",
          body: JSON.stringify({
            source: nodes.sourceInput.value.trim(),
            target_collection: selectedLookupValue("collections"),
            location: selectedLookupValue("locations"),
            notes: nodes.notesInput.value.trim()
          })
        });
        updateBatchView({
          ...data,
          uploaded_count: 0,
          results: { exists: false, total: 0, success: 0, failed: 0 }
        });
        state.verification = null;
        state.failures = [];
        renderVerification();
        renderFailures();
        await listBatches(true);
        setStatus(nodes.batchStatus, `Created ${data.batch_id}.`, "ok");
      } catch (error) {
        setStatus(nodes.batchStatus, error.message, "error");
      } finally {
        nodes.createBatchBtn.disabled = false;
      }
    }

    async function refreshBatch() {
      if (!state.batch?.batch_id) return;
      await loadBatch(state.batch.batch_id);
    }

    async function runBatch() {
      if (!state.batch?.batch_id) return;
      saveAccess();
      setStatus(nodes.runStatus, "Starting batch run...", "warn");
      nodes.runBatchBtn.disabled = true;

      try {
        const data = await apiFetch(`/batches/${encodeURIComponent(state.batch.batch_id)}/run`, {
          method: "POST",
          body: JSON.stringify({})
        });
        updateBatchView(data);
        await loadFailures(true);
        if (data.run?.status !== "running") {
          await loadVerification(true);
        }
        await listBatches(true);
      } catch (error) {
        setStatus(nodes.runStatus, error.message, "error");
        nodes.runBatchBtn.disabled = !state.batch?.can_run;
        nodes.runBatchBtn.textContent = runButtonLabel(state.batch);
      }
    }

    function addFiles(fileList) {
      const incoming = Array.from(fileList || []);
      const existingKeys = new Set(state.files.map((file) => `${file.name}:${file.size}:${file.lastModified}`));

      for (const file of incoming) {
        const key = `${file.name}:${file.size}:${file.lastModified}`;
        if (!existingKeys.has(key)) {
          state.files.push(file);
          state.uploads.set(key, { status: "ready", progress: 0, message: "" });
          existingKeys.add(key);
        }
      }

      renderFiles();
      updateUploadButtons();
    }

    function fileKey(file) {
      return `${file.name}:${file.size}:${file.lastModified}`;
    }

    function formatBytes(bytes) {
      if (bytes < 1024) return `${bytes} B`;
      const units = ["KB", "MB", "GB"];
      let value = bytes / 1024;
      for (const unit of units) {
        if (value < 1024) return `${value.toFixed(value < 10 ? 1 : 0)} ${unit}`;
        value /= 1024;
      }
      return `${value.toFixed(1)} TB`;
    }

    function renderFiles() {
      nodes.fileList.innerHTML = "";

      if (!state.files.length) {
        setStatus(nodes.uploadStatus, "No files selected.");
        return;
      }

      for (const file of state.files) {
        const key = fileKey(file);
        const upload = state.uploads.get(key) || { status: "ready", progress: 0, message: "" };
        const row = document.createElement("div");
        row.className = "file-row";

        const left = document.createElement("div");
        const name = document.createElement("div");
        name.className = "file-name";
        name.textContent = file.name;
        const detail = document.createElement("div");
        detail.className = "file-detail";
        detail.textContent = `${formatBytes(file.size)}${upload.message ? ` - ${upload.message}` : ""}`;
        const progress = document.createElement("div");
        progress.className = "progress";
        const bar = document.createElement("div");
        bar.className = "progress-bar";
        bar.style.width = `${upload.progress || 0}%`;
        progress.appendChild(bar);
        left.append(name, detail, progress);

        const badge = document.createElement("div");
        badge.className = `badge ${upload.status === "done" ? "ok" : upload.status === "error" ? "error" : ""}`.trim();
        badge.textContent = upload.status || "ready";

        row.append(left, badge);
        nodes.fileList.appendChild(row);
      }
    }

    function updateUploadButtons() {
      const hasBatch = Boolean(state.batch?.batch_id);
      const hasFiles = state.files.length > 0;
      nodes.uploadBtn.disabled = !hasBatch || !hasFiles || state.uploadInProgress;
      nodes.clearFilesBtn.disabled = !hasFiles || state.uploadInProgress;
      nodes.uploadBtn.textContent = state.uploadInProgress ? "Uploading..." : "Upload Selected";
    }

    function uploadWithProgress(url, file, contentType, onProgress) {
      return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open("PUT", url);
        xhr.setRequestHeader("Content-Type", contentType);
        xhr.upload.onprogress = (event) => {
          if (event.lengthComputable) {
            onProgress(Math.round((event.loaded / event.total) * 100));
          }
        };
        xhr.onload = () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            onProgress(100);
            resolve();
          } else {
            reject(new Error(`GCS upload failed: HTTP ${xhr.status}`));
          }
        };
        xhr.onerror = () => reject(new Error("GCS upload failed. Check bucket CORS for browser uploads."));
        xhr.send(file);
      });
    }

    async function uploadOne(file) {
      const key = fileKey(file);
      const contentType = file.type || "application/octet-stream";
      state.uploads.set(key, { status: "signing", progress: 0, message: "" });
      scheduleRenderFiles();

      const signed = await apiFetch(`/batches/${encodeURIComponent(state.batch.batch_id)}/upload-url`, {
        method: "POST",
        body: JSON.stringify({
          filename: file.name,
          content_type: contentType,
          overwrite: nodes.overwriteInput.checked
        })
      });

      state.uploads.set(key, { status: "uploading", progress: 1, message: signed.object_path });
      scheduleRenderFiles();

      await uploadWithProgress(signed.upload_url, file, contentType, (progress) => {
        state.uploads.set(key, { status: "uploading", progress, message: signed.object_path });
        scheduleRenderFiles();
      });

      state.uploads.set(key, { status: "done", progress: 100, message: signed.object_path });
      scheduleRenderFiles();
    }

    async function uploadSelected() {
      if (!state.batch?.batch_id || !state.files.length) return;
      saveAccess();
      state.uploadInProgress = true;
      updateUploadButtons();

      const pending = state.files.filter((file) => {
        const current = state.uploads.get(fileKey(file));
        return current?.status !== "done";
      });
      const alreadyUploaded = state.files.length - pending.length;
      const total = pending.length;
      let index = 0;
      let completed = 0;
      let success = alreadyUploaded;
      let failed = 0;

      if (!pending.length) {
        state.uploadInProgress = false;
        setStatus(nodes.uploadStatus, "All selected files are already uploaded.", "ok");
        updateUploadButtons();
        return;
      }

      const parallelUploads = Math.min(UPLOAD_CONCURRENCY, pending.length);
      const setProgressStatus = () => {
        setStatus(
          nodes.uploadStatus,
          `Uploading ${completed}/${total} file${total === 1 ? "" : "s"} (${parallelUploads} at a time)...`
        );
      };
      setProgressStatus();

      async function uploadWorker() {
        while (index < pending.length) {
          const file = pending[index];
          index += 1;
          const key = fileKey(file);

          try {
            await uploadOne(file);
            success += 1;
          } catch (error) {
            failed += 1;
            state.uploads.set(key, { status: "error", progress: 0, message: error.message });
            scheduleRenderFiles();
          } finally {
            completed += 1;
            setProgressStatus();
          }
        }
      }

      try {
        const workers = Array.from(
          { length: parallelUploads },
          uploadWorker,
        );
        await Promise.all(workers);
      } finally {
        state.uploadInProgress = false;
      }

      renderFiles();
      setStatus(
        nodes.uploadStatus,
        `Uploaded ${success} file${success === 1 ? "" : "s"}${failed ? `, ${failed} failed` : ""}.`,
        failed ? "warn" : "ok"
      );
      try {
        await refreshBatch();
        await listBatches(true);
      } finally {
        updateUploadButtons();
      }
    }

    async function copyCommand() {
      const command = nodes.runCommand.textContent.trim();
      if (!command) return;

      try {
        await navigator.clipboard.writeText(command);
        setStatus(nodes.batchStatus, "Command copied.", "ok");
      } catch {
        setStatus(nodes.batchStatus, "Could not copy command.", "warn");
      }
    }

    function errorReportFromData(logData) {
      const batch = state.batch || {};
      const verification = state.verification;
      const checks = verification?.checks || [];
      const checkLines = checks.length
        ? checks.map((check) => `- [${(check.status || "info").toUpperCase()}] ${check.label}: ${check.detail || ""}`).join("\\n")
        : "- No verification data loaded.";

      return [
        "RB Extractor error report",
        `Batch ID: ${batch.batch_id || ""}`,
        `Run status: ${batch.run?.status || ""}`,
        `Run stage: ${batch.run?.stage || ""}`,
        `Results path: gs://${batch.bucket || ""}/${batch.results_path || ""}`,
        `Input prefix: ${batch.input_prefix || ""}`,
        `Collection: ${batch.target_collection || ""}`,
        `Location: ${batch.location || ""}`,
        `Verification status: ${verification?.overall_status || ""}`,
        "",
        "Verification checks:",
        checkLines,
        "",
        `Log path: ${logData.log_path || batch.run?.log_path || ""}`,
        "",
        "Run log:",
        logData.log_text || batch.run?.log_tail || nodes.runLog.textContent || ""
      ].join("\\n");
    }

    async function copyErrorReport() {
      if (!state.batch?.batch_id) return;
      saveAccess();
      nodes.copyErrorReportBtn.disabled = true;

      try {
        if (!state.verification) {
          await loadVerification(true);
        }
        const logData = await apiFetch(`/batches/${encodeURIComponent(state.batch.batch_id)}/log`);
        await navigator.clipboard.writeText(errorReportFromData(logData));
        setStatus(nodes.runStatus, "Error report copied.", "ok");
      } catch (error) {
        setStatus(nodes.runStatus, `Could not copy error report: ${error.message}`, "error");
      } finally {
        nodes.copyErrorReportBtn.disabled = !state.batch?.batch_id;
      }
    }

    function init() {
      nodes.apiToken.value = state.token;
      nodes.apiBase.value = state.apiBase;
      nodes.existingBatchInput.value = state.batchId;
      updateBatchView(null);
      renderBatchList();
      renderVerification();
      renderFailures();
      renderFiles();

      nodes.saveAccessBtn.addEventListener("click", saveAccess);
      nodes.clearAccessBtn.addEventListener("click", clearAccess);
      nodes.checkApiBtn.addEventListener("click", checkApi);
      nodes.createBatchBtn.addEventListener("click", createBatch);
      nodes.addCollectionBtn.addEventListener("click", () => addLookupOption("collections"));
      nodes.addLocationBtn.addEventListener("click", () => addLookupOption("locations"));
      nodes.refreshBatchBtn.addEventListener("click", refreshBatch);
      nodes.loadBatchBtn.addEventListener("click", () => loadBatch(nodes.existingBatchInput.value));
      nodes.listBatchesBtn.addEventListener("click", () => listBatches());
      nodes.runBatchBtn.addEventListener("click", runBatch);
      nodes.refreshVerificationBtn.addEventListener("click", () => loadVerification());
      nodes.refreshFailuresBtn.addEventListener("click", () => loadFailures());
      nodes.retryFailuresBtn.addEventListener("click", retryFailures);
      nodes.chooseFilesBtn.addEventListener("click", () => nodes.fileInput.click());
      nodes.fileInput.addEventListener("change", (event) => addFiles(event.target.files));
      nodes.uploadBtn.addEventListener("click", uploadSelected);
      nodes.clearFilesBtn.addEventListener("click", () => {
        state.files = [];
        state.uploads.clear();
        renderFiles();
        updateUploadButtons();
      });
      nodes.copyCommandBtn.addEventListener("click", copyCommand);
      nodes.copyErrorReportBtn.addEventListener("click", copyErrorReport);

      window.addEventListener("beforeunload", (event) => {
        if (!state.uploadInProgress) return;
        event.preventDefault();
        event.returnValue = "";
      });

      ["dragenter", "dragover"].forEach((eventName) => {
        nodes.dropzone.addEventListener(eventName, (event) => {
          event.preventDefault();
          nodes.dropzone.classList.add("dragover");
        });
      });

      ["dragleave", "drop"].forEach((eventName) => {
        nodes.dropzone.addEventListener(eventName, (event) => {
          event.preventDefault();
          nodes.dropzone.classList.remove("dragover");
        });
      });

      nodes.dropzone.addEventListener("drop", (event) => {
        addFiles(event.dataTransfer.files);
      });

      if (state.token && state.batchId) {
        loadBatch(state.batchId, true);
      }
      if (state.token) {
        loadLookupOptions(true);
        listBatches(true);
      }
    }

    init();
  </script>
</body>
</html>"""
