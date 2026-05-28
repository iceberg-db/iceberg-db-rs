/**
 * Snowsight-style SQL workspace over idb-wasm (Horizon IRC or demo).
 *
 * This module wires the DOM to the WASM bindings exposed by Trunk
 * (`window.wasmBindings`). All pure formatting helpers live in
 * `./lib/format.mjs` so they can be unit-tested under Node.
 */

import {
  escapeHtml,
  formatBytes,
  formatFetchProgress,
  formatCell,
  formatElapsedMs,
  jsNumber,
  pickSqlToRun,
} from "./lib/format.mjs";

// ---------------------------------------------------------------------------
// Configuration constants
// ---------------------------------------------------------------------------

/** Soft UI threshold for long-running queries; we keep awaiting WASM after this. */
const QUERY_UI_WAIT_MS = 10 * 60 * 1000;

/** Internal marker so we can distinguish UI timeout from real query errors. */
const UI_TIMEOUT_MARKER = "IDB_UI_TIMEOUT";

/** Maximum saved queries in localStorage history. */
const HISTORY_LIMIT = 12;

/** Local proxy that signs and forwards Snowflake REST + S3 calls in dev. */
const DEV_PROXY_ORIGIN = "http://127.0.0.1:8787";

/** Hosts treated as "local dev" — these trigger dev-proxy routing. */
const LOCAL_HOSTS = new Set(["127.0.0.1", "localhost", "::1"]);

/** Storage keys. */
const STORAGE_KEYS = {
  history: "idb-query-history",
  horizonSettings: "idb-horizon-settings",
};

// ---------------------------------------------------------------------------
// Static sample data (schema tree + sample query list)
// ---------------------------------------------------------------------------

const DEMO_SCHEMA = {
  catalog: "local",
  schemas: [
    {
      name: "demo",
      tables: [{ name: "customers", rows: 3, columns: ["id", "name", "region"] }],
    },
  ],
};

const HORIZON_SCHEMA = {
  catalog: "snowflake_horizon",
  schemas: [
    {
      name: "iceberg_test",
      tables: [{ name: "employee", rows: "?", columns: ["(Iceberg table)"] }],
    },
  ],
};

const DEMO_QUERIES = [
  { label: "Count customers", sql: "SELECT COUNT(*) AS n FROM demo.customers" },
  { label: "All rows", sql: "SELECT * FROM demo.customers ORDER BY id" },
  {
    label: "By region",
    sql: "SELECT region, COUNT(*) AS n FROM demo.customers GROUP BY region ORDER BY n DESC",
  },
];

const HORIZON_QUERIES = [
  { label: "Sample rows", sql: "SELECT * FROM iceberg_test.employee LIMIT 10" },
  { label: "Show tables", sql: "SHOW TABLES IN iceberg_test" },
  { label: "Count", sql: "SELECT COUNT(*) AS n FROM iceberg_test.employee" },
];

// ---------------------------------------------------------------------------
// Mutable session state (deliberately scoped to this module)
// ---------------------------------------------------------------------------

let currentMode = "horizon";
let activeSchema = HORIZON_SCHEMA;
let activeSampleQueries = HORIZON_QUERIES;
let queryRunning = false;
let queryTimerInterval = null;
let queryBytesInterval = null;

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

function $(id) {
  return document.getElementById(id);
}

function isLocalHost() {
  return LOCAL_HOSTS.has(window.location.hostname);
}

async function loadWasm() {
  if (window.wasmBindings) return window.wasmBindings;
  return new Promise((resolve) => {
    window.addEventListener(
      "TrunkApplicationStarted",
      () => resolve(window.wasmBindings),
      { once: true }
    );
  });
}

// ---------------------------------------------------------------------------
// SQL editor (line gutter + selection helpers)
// ---------------------------------------------------------------------------

function syncLineNumbers() {
  const sql = $("sql");
  const gutter = $("line-gutter");
  const lines = sql.value.split("\n").length;
  gutter.textContent = Array.from({ length: Math.max(lines, 1) }, (_, i) => i + 1).join("\n");
  gutter.scrollTop = sql.scrollTop;
}

function setSql(text) {
  $("sql").value = text;
  syncLineNumbers();
}

function getSqlToRun() {
  const sqlEl = $("sql");
  return pickSqlToRun({
    value: sqlEl.value,
    selectionStart: sqlEl.selectionStart,
    selectionEnd: sqlEl.selectionEnd,
  });
}

// ---------------------------------------------------------------------------
// Horizon connection: YAML builder + settings persistence
// ---------------------------------------------------------------------------

/** YAML double-quoted scalar (PAT/scope contain `:` and break unquoted YAML). */
function yamlScalar(value) {
  return JSON.stringify(String(value));
}

function horizonUriFor(account) {
  return isLocalHost()
    ? `${DEV_PROXY_ORIGIN}/${account}/polaris/api/catalog`
    : `https://${account}.snowflakecomputing.com/polaris/api/catalog`;
}

function readHorizonForm() {
  return {
    account: $("sf-account").value.trim(),
    warehouse: $("sf-warehouse").value.trim(),
    schema: $("sf-schema").value.trim(),
    username: $("sf-username").value.trim(),
    scope: $("sf-scope").value.trim(),
    pat: $("sf-pat").value.trim(),
  };
}

function buildHorizonYaml() {
  const form = readHorizonForm();
  if (!form.account) throw new Error("Account host is required (e.g. qtfneqx-er54214)");
  if (!form.warehouse) throw new Error("Database (warehouse) is required");
  if (!form.pat || form.pat.length < 32) {
    throw new Error("Paste a valid Snowflake PAT (32+ characters)");
  }

  const uri = horizonUriFor(form.account);
  const usernameLine = form.username
    ? `    username: ${yamlScalar(form.username)}\n`
    : "";

  return `default-catalog: snowflake_horizon

catalogs:
  snowflake_horizon:
    type: rest
    profile: snowflake-horizon
    uri: ${yamlScalar(uri)}
    warehouse: ${yamlScalar(form.warehouse)}
    default-schema: ${yamlScalar(form.schema)}
    token: ${yamlScalar(form.pat)}
    scope: ${yamlScalar(form.scope)}
${usernameLine}`;
}

function saveHorizonSettings() {
  const { pat: _ignored, ...persistable } = readHorizonForm();
  localStorage.setItem(STORAGE_KEYS.horizonSettings, JSON.stringify(persistable));
}

function loadHorizonSettings() {
  try {
    const raw = localStorage.getItem(STORAGE_KEYS.horizonSettings);
    if (!raw) return;
    const s = JSON.parse(raw);
    if (s.account) $("sf-account").value = s.account;
    if (s.warehouse) $("sf-warehouse").value = s.warehouse;
    if (s.schema) $("sf-schema").value = s.schema;
    if (s.username) $("sf-username").value = s.username;
    if (s.scope) $("sf-scope").value = s.scope;
  } catch {
    // Ignore corrupted localStorage entries.
  }
}

// ---------------------------------------------------------------------------
// Mode switching (demo vs Horizon)
// ---------------------------------------------------------------------------

function setMode(mode) {
  currentMode = mode;
  if (mode === "demo") {
    activeSchema = DEMO_SCHEMA;
    activeSampleQueries = DEMO_QUERIES;
    $("mode-pill").textContent = "demo";
    $("sql-hint").textContent = "SELECT * FROM demo.customers";
    setSql("SELECT COUNT(*) AS n FROM demo.customers");
  } else {
    activeSchema = HORIZON_SCHEMA;
    activeSampleQueries = HORIZON_QUERIES;
    $("mode-pill").textContent = "horizon";
    const schema = $("sf-schema").value.trim() || "iceberg_test";
    $("sql-hint").textContent = `SELECT * FROM ${schema}.employee LIMIT 10`;
    setSql(`SELECT * FROM ${schema}.employee LIMIT 10`);
  }
  renderSchemaTree();
  renderSamples();
}

// ---------------------------------------------------------------------------
// Sidebar rendering (schema tree, samples, history)
// ---------------------------------------------------------------------------

function renderSchemaTree() {
  const root = $("schema-tree");
  root.innerHTML = "";

  const catLi = document.createElement("li");
  catLi.className = "tree-item";
  catLi.innerHTML = `<div class="tree-row"><span class="tree-icon">▾</span><span>${activeSchema.catalog}</span></div>`;
  const catUl = document.createElement("ul");
  catUl.className = "tree-children";

  for (const schema of activeSchema.schemas) {
    const schLi = document.createElement("li");
    schLi.className = "tree-item";
    schLi.innerHTML = `<div class="tree-row"><span class="tree-icon">▾</span><span>${schema.name}</span></div>`;
    const tblUl = document.createElement("ul");
    tblUl.className = "tree-children";

    for (const table of schema.tables) {
      const tblLi = document.createElement("li");
      tblLi.className = "tree-item";
      const fq = `${schema.name}.${table.name}`;
      tblLi.innerHTML = `<div class="tree-row" data-fq="${fq}" title="${table.columns.join(
        ", "
      )}"><span class="tree-icon">◇</span><span>${table.name}</span><span style="margin-left:auto;opacity:.6;font-size:11px">${table.rows}</span></div>`;
      tblLi.querySelector(".tree-row").addEventListener("click", () => {
        setSql(`SELECT *\nFROM ${fq}\nLIMIT 100`);
        document
          .querySelectorAll(".tree-row.active")
          .forEach((el) => el.classList.remove("active"));
        tblLi.querySelector(".tree-row").classList.add("active");
      });
      tblUl.appendChild(tblLi);
    }
    schLi.appendChild(tblUl);
    catUl.appendChild(schLi);
  }
  catLi.appendChild(catUl);
  root.appendChild(catLi);
}

function renderSamples() {
  const list = $("sample-list");
  list.innerHTML = "";
  for (const sample of activeSampleQueries) {
    const li = document.createElement("li");
    li.textContent = sample.label;
    li.title = sample.sql;
    li.addEventListener("click", () => setSql(sample.sql));
    list.appendChild(li);
  }
}

function readHistory() {
  try {
    return JSON.parse(localStorage.getItem(STORAGE_KEYS.history) || "[]");
  } catch {
    return [];
  }
}

function pushHistory(sql) {
  const trimmed = sql.trim();
  if (!trimmed) return;
  const history = [trimmed, ...readHistory().filter((q) => q !== trimmed)].slice(0, HISTORY_LIMIT);
  localStorage.setItem(STORAGE_KEYS.history, JSON.stringify(history));
  renderHistory();
}

function renderHistory() {
  const list = $("history-list");
  const history = readHistory();
  list.innerHTML = "";
  if (!history.length) {
    list.innerHTML = '<li style="cursor:default;opacity:.6">No runs yet</li>';
    return;
  }
  for (const sql of history) {
    const li = document.createElement("li");
    li.textContent = sql.replace(/\s+/g, " ").slice(0, 80);
    li.title = sql;
    li.addEventListener("click", () => setSql(sql));
    list.appendChild(li);
  }
}

// ---------------------------------------------------------------------------
// Results panel (tabs, grid, status bar)
// ---------------------------------------------------------------------------

function switchResultsTab(name) {
  document.querySelectorAll(".results-tab").forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.tab === name);
  });
  document.querySelectorAll(".panel").forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.panel === name);
  });
}

function renderGrid(result) {
  const wrap = $("results-grid-wrap");
  if (!result.columns?.length) {
    wrap.innerHTML = '<div class="empty-state">Query returned no columns.</div>';
    return;
  }

  const table = document.createElement("table");
  table.className = "results-grid";

  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  for (const col of result.columns) {
    const th = document.createElement("th");
    th.innerHTML = `${escapeHtml(col.name)}<span class="type-badge">${escapeHtml(col.data_type)}</span>`;
    headRow.appendChild(th);
  }
  thead.appendChild(headRow);
  table.appendChild(thead);

  const tbody = document.createElement("tbody");
  const rows = result.rows || [];
  if (!rows.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = result.columns.length;
    td.textContent = "(no rows)";
    tr.appendChild(td);
    tbody.appendChild(tr);
  } else {
    for (const row of rows) {
      const tr = document.createElement("tr");
      for (const cell of row) {
        const td = document.createElement("td");
        const text = formatCell(cell);
        td.textContent = text;
        td.title = text;
        tr.appendChild(td);
      }
      tbody.appendChild(tr);
    }
  }
  table.appendChild(tbody);
  wrap.innerHTML = "";
  wrap.appendChild(table);
}

function setStatus({ message, kind = "muted", rows, files, bytes, ms }) {
  const el = $("status-message");
  el.className = `status-message${
    kind === "ok" ? " status-ok" : kind === "err" ? " status-err" : ""
  }`;
  el.textContent = message;
  el.title = message;
  $("status-rows").textContent = rows != null ? `${jsNumber(rows)} row(s)` : "—";
  if (files != null || bytes != null) {
    $("status-bytes").textContent = formatFetchProgress(files, bytes);
  } else if (!queryRunning) {
    $("status-bytes").textContent = "—";
  }
  if (ms != null) {
    $("status-time").textContent = formatElapsedMs(ms);
  } else if (!queryRunning) {
    $("status-time").textContent = "—";
  }
}

function startQueryElapsedTimer(t0) {
  stopQueryElapsedTimer();
  const timeEl = $("status-time");
  const bar = timeEl?.closest(".statusbar");
  bar?.classList.add("statusbar-running");
  const tick = () => {
    timeEl.textContent = formatElapsedMs(performance.now() - t0);
  };
  tick();
  queryTimerInterval = setInterval(tick, 1000);
}

function stopQueryElapsedTimer() {
  if (queryTimerInterval != null) {
    clearInterval(queryTimerInterval);
    queryTimerInterval = null;
  }
  $("status-time")?.closest(".statusbar")?.classList.remove("statusbar-running");
}

function readLiveFetchStats() {
  const bytesFn = window.__idb?.idb_bytes_fetched;
  const filesFn = window.__idb?.idb_files_fetched;
  if (typeof bytesFn !== "function" || typeof filesFn !== "function") {
    return null;
  }
  return { files: filesFn(), bytes: bytesFn() };
}

function startQueryBytesPoller() {
  stopQueryBytesPoller();
  const el = $("status-bytes");
  el.textContent = formatFetchProgress(0, 0);
  const tick = () => {
    const stats = readLiveFetchStats();
    if (stats != null) {
      el.textContent = formatFetchProgress(stats.files, stats.bytes);
    }
  };
  tick();
  queryBytesInterval = setInterval(tick, 400);
}

function stopQueryBytesPoller() {
  if (queryBytesInterval != null) {
    clearInterval(queryBytesInterval);
    queryBytesInterval = null;
  }
}

function setQueryStep(step) {
  if (!queryRunning) return;
  const el = $("status-message");
  el.className = "status-message";
  el.textContent = `Running… · ${step}`;
  el.title = step;
}

function installQueryLogHook() {
  const origLog = console.log.bind(console);
  console.log = (...args) => {
    origLog(...args);
    const msg = args.map((a) => (typeof a === "string" ? a : String(a))).join(" ");
    if (msg.startsWith("idb_query: ")) {
      setQueryStep(msg.slice("idb_query: ".length));
    }
  };
}

function setLoadingOverlay(visible, message) {
  const card = $("loading").querySelector(".loading-card");
  const label = card?.querySelector(".loading-label");
  if (label && message) label.textContent = message;
  $("loading").classList.toggle("hidden", !visible);
}

function setRunning(running) {
  $("run-btn").disabled = running || !window.__idbReady;
  // Queries use the status bar only — do not reuse the full-screen init overlay.
}

// ---------------------------------------------------------------------------
// Run query: race against a soft UI timeout, then await WASM regardless
// ---------------------------------------------------------------------------

async function runQuery() {
  if (!window.__idbReady || !window.__idb?.idb_query) {
    console.error("[iceberg-db] Run ignored — connect or demo first");
    setStatus({ message: "Connect or Demo first", kind: "err" });
    return;
  }
  const { idb_query } = window.__idb;
  const sql = getSqlToRun();
  if (!sql) {
    setStatus({ message: "Nothing to run", kind: "err" });
    return;
  }
  const ver = window.__idb?.idb_wasm_version?.() ?? "?";
  console.info("[iceberg-db] runQuery", { ver, sql: sql.slice(0, 120) });

  queryRunning = true;
  setRunning(true);
  setStatus({ message: "Running…", kind: "muted" });
  const t0 = performance.now();
  startQueryElapsedTimer(t0);
  startQueryBytesPoller();

  const queryPromise = idb_query(sql);

  try {
    let result;
    try {
      result = await Promise.race([
        queryPromise,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error(UI_TIMEOUT_MARKER)), QUERY_UI_WAIT_MS)
        ),
      ]);
    } catch (raceErr) {
      if (!(raceErr instanceof Error && raceErr.message === UI_TIMEOUT_MARKER)) {
        throw raceErr;
      }
      const waitedSec = Math.round((performance.now() - t0) / 1000);
      setStatus({
        message: `Still running (${waitedSec}s) — large Iceberg scan; see console`,
        kind: "muted",
      });
      $("results-grid-wrap").innerHTML =
        '<div class="empty-state">Query still running (S3 parquet scan). Results will appear when finished.</div>';
      result = await queryPromise;
    }

    console.info("[iceberg-db] runQuery ok", result?.row_count, "rows");
    pushHistory(sql);
    renderGrid(result);
    $("out-text").textContent = result.text || "";
    setStatus({
      message: "Completed",
      kind: "ok",
      rows: result.row_count,
      files: result.files_fetched,
      bytes: result.bytes_fetched,
      ms: result.elapsed_ms,
    });
    switchResultsTab("grid");
  } catch (e) {
    const msg = String(e);
    $("results-grid-wrap").innerHTML = `<div class="empty-state" style="color:var(--error)">${escapeHtml(
      msg
    )}</div>`;
    $("out-text").textContent = msg;
    setStatus({
      message: "Failed",
      kind: "err",
      ms: Math.round(performance.now() - t0),
    });
    switchResultsTab("text");
  } finally {
    stopQueryElapsedTimer();
    stopQueryBytesPoller();
    queryRunning = false;
    setRunning(false);
  }
}

// ---------------------------------------------------------------------------
// Engine init (demo / Horizon)
// ---------------------------------------------------------------------------

async function initEngine(initFn, label) {
  setStatus({ message: `${label}…`, kind: "muted" });
  setLoadingOverlay(true, label.endsWith("…") ? label : `${label}…`);
  window.__idbReady = false;
  $("run-btn").disabled = true;
  try {
    await initFn();
    window.__idbReady = true;
    $("run-btn").disabled = false;
    setStatus({ message: "Ready", kind: "ok" });
    $("results-grid-wrap").innerHTML =
      '<div class="empty-state">Run a query or pick a sample from the left.</div>';
  } catch (e) {
    setStatus({ message: "Init failed", kind: "err" });
    $("results-grid-wrap").innerHTML = `<div class="empty-state" style="color:var(--error)">${escapeHtml(
      e
    )}</div>`;
    console.error(e);
  } finally {
    setLoadingOverlay(false);
  }
}

async function connectHorizon() {
  const { idb_init_horizon } = window.__idb;
  if (!idb_init_horizon) {
    throw new Error("WASM build missing idb_init_horizon — rebuild with horizon feature");
  }
  saveHorizonSettings();
  const yaml = buildHorizonYaml();
  const account = $("sf-account").value.trim();
  const scope = $("sf-scope").value.trim();
  const patLen = $("sf-pat").value.trim().length;
  const oauthUri = `${horizonUriFor(account)}/v1/oauth/tokens`;
  console.info("Horizon connect", {
    oauthUri,
    scope,
    patLen,
    username: $("sf-username").value.trim() || "(none)",
  });
  setMode("horizon");
  await initEngine(() => idb_init_horizon(yaml), "Connecting to Horizon");
}

async function connectDemo() {
  const { idb_init_demo } = window.__idb;
  if (!idb_init_demo) {
    throw new Error("WASM build missing idb_init_demo");
  }
  setMode("demo");
  await initEngine(() => idb_init_demo(), "Starting demo");
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

function wireEditor() {
  const sqlEl = $("sql");
  sqlEl.addEventListener("input", syncLineNumbers);
  sqlEl.addEventListener("scroll", () => {
    $("line-gutter").scrollTop = sqlEl.scrollTop;
  });
  sqlEl.addEventListener("keydown", (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      runQuery();
      return;
    }
    if (e.key === "Tab") {
      e.preventDefault();
      const start = sqlEl.selectionStart;
      const end = sqlEl.selectionEnd;
      sqlEl.value = `${sqlEl.value.slice(0, start)}  ${sqlEl.value.slice(end)}`;
      sqlEl.selectionStart = sqlEl.selectionEnd = start + 2;
      syncLineNumbers();
    }
  });
}

function wireToolbar() {
  document.querySelectorAll(".results-tab").forEach((tab) => {
    tab.addEventListener("click", () => switchResultsTab(tab.dataset.tab));
  });
  $("run-btn").addEventListener("click", runQuery);
  $("clear-btn").addEventListener("click", () => {
    $("results-grid-wrap").innerHTML = '<div class="empty-state">Run a query to see results.</div>';
    $("out-text").textContent = "";
    setStatus({ message: "Ready", kind: "ok" });
  });
  $("connect-horizon-btn").addEventListener("click", () =>
    connectHorizon().catch(console.error)
  );
  $("demo-btn").addEventListener("click", () => connectDemo().catch(console.error));
}

async function boot() {
  installQueryLogHook();
  loadHorizonSettings();
  setMode("horizon");
  renderSchemaTree();
  renderSamples();
  renderHistory();
  wireEditor();
  wireToolbar();
  syncLineNumbers();

  const bindings = await loadWasm();
  const {
    idb_init_horizon,
    idb_init_demo,
    idb_query,
    idb_bytes_fetched,
    idb_files_fetched,
    idb_wasm_version,
  } = bindings;
  window.__idb = {
    idb_init_horizon,
    idb_init_demo,
    idb_query,
    idb_bytes_fetched,
    idb_files_fetched,
    idb_wasm_version,
  };

  const wasmVer = idb_wasm_version();
  console.info("[iceberg-db] wasm build", wasmVer);
  $("engine-version").textContent = `iceberg-db wasm ${wasmVer}`;
  setStatus({ message: "Enter PAT and click Connect", kind: "muted" });
  $("results-grid-wrap").innerHTML =
    '<div class="empty-state">Connect to Snowflake Horizon or use Demo data.</div>';
  setLoadingOverlay(false);
}

boot().catch((e) => {
  console.error(e);
  setLoadingOverlay(false);
  const status = document.getElementById("status-message");
  if (status) status.textContent = "Failed to start";
});
