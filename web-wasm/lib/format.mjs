/**
 * Pure UI formatting helpers — no DOM, no globals. Imported by app.js in
 * the browser and exercised by `web-wasm/tests/format.test.mjs` in Node.
 */

/**
 * Coerce `bigint | number | string | null | undefined` to a plain `number`.
 * `serde_wasm_bindgen` returns `u64`/`u128` Rust integers as JS `BigInt`,
 * which can't be mixed with regular numbers in arithmetic.
 */
export function jsNumber(value) {
  if (typeof value === "bigint") return Number(value);
  if (typeof value === "number") return value;
  if (value == null) return NaN;
  return Number(value);
}

/** Render a single grid cell, tolerating null/BigInt/everything-else. */
export function formatCell(value) {
  if (value == null) return "";
  if (typeof value === "bigint") return value.toString();
  return String(value);
}

/**
 * Format a duration in milliseconds as a short human string.
 *
 *   `0s`, `12s`, `1m`, `1m 15s`, `1h`, `1h 5s`, `2h 30m`, `1h 2m 3s`.
 *
 * Returns the em-dash `—` for null/undefined/negative/NaN inputs so the
 * status bar never shows a misleading `NaNs`.
 */
export function formatElapsedMs(ms) {
  const n = jsNumber(ms);
  if (!Number.isFinite(n) || n < 0) return "—";
  const totalSec = Math.floor(n / 1000);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;

  if (h > 0) {
    const parts = [`${h}h`];
    if (m > 0) parts.push(`${m}m`);
    if (s > 0) parts.push(`${s}s`);
    return parts.join(" ");
  }
  if (m > 0) {
    return s > 0 ? `${m}m ${s}s` : `${m}m`;
  }
  return `${s}s`;
}

/** HTML-escape an arbitrary value for safe interpolation in `innerHTML`. */
export function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/**
 * Choose which SQL to run from a textarea-like `{ value, selectionStart, selectionEnd }`.
 * Returns the selected text when a non-empty selection is present, else the full trimmed value.
 */
export function pickSqlToRun({ value, selectionStart, selectionEnd }) {
  if (selectionStart !== selectionEnd) {
    const selected = value.slice(selectionStart, selectionEnd).trim();
    if (selected) return selected;
  }
  return value.trim();
}
