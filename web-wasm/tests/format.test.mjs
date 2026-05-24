/**
 * Tests for `web-wasm/lib/format.mjs` — run with:
 *
 *   node --test web-wasm/tests/format.test.mjs
 *
 * No package.json or external deps required: uses Node's built-in
 * `node:test` runner (Node 18+).
 */

import test from "node:test";
import assert from "node:assert/strict";

import {
  escapeHtml,
  formatCell,
  formatElapsedMs,
  jsNumber,
  pickSqlToRun,
} from "../lib/format.mjs";

test("jsNumber coerces bigints", () => {
  assert.equal(jsNumber(0n), 0);
  assert.equal(jsNumber(42n), 42);
  assert.equal(jsNumber(-1n), -1);
});

test("jsNumber passes through numbers", () => {
  assert.equal(jsNumber(0), 0);
  assert.equal(jsNumber(3.14), 3.14);
});

test("jsNumber handles null/undefined as NaN", () => {
  assert.ok(Number.isNaN(jsNumber(null)));
  assert.ok(Number.isNaN(jsNumber(undefined)));
});

test("jsNumber parses numeric strings", () => {
  assert.equal(jsNumber("42"), 42);
  assert.equal(jsNumber("3.14"), 3.14);
});

test("formatCell handles null/undefined", () => {
  assert.equal(formatCell(null), "");
  assert.equal(formatCell(undefined), "");
});

test("formatCell stringifies bigints exactly", () => {
  assert.equal(formatCell(42n), "42");
  assert.equal(
    formatCell(9007199254740993n), // > Number.MAX_SAFE_INTEGER
    "9007199254740993"
  );
});

test("formatCell falls back to String(value)", () => {
  assert.equal(formatCell(0), "0");
  assert.equal(formatCell(false), "false");
  assert.equal(formatCell("hello"), "hello");
});

test("formatElapsedMs formats seconds under a minute", () => {
  assert.equal(formatElapsedMs(0), "0s");
  assert.equal(formatElapsedMs(999), "0s");
  assert.equal(formatElapsedMs(1000), "1s");
  assert.equal(formatElapsedMs(12_345), "12s");
  assert.equal(formatElapsedMs(59_999), "59s");
});

test("formatElapsedMs formats minutes and seconds", () => {
  assert.equal(formatElapsedMs(60_000), "1m");
  assert.equal(formatElapsedMs(60_500), "1m");
  assert.equal(formatElapsedMs(75_000), "1m 15s");
  assert.equal(formatElapsedMs(256_819), "4m 16s");
});

test("formatElapsedMs formats hours, dropping zero components", () => {
  assert.equal(formatElapsedMs(60 * 60 * 1000), "1h");
  assert.equal(formatElapsedMs(60 * 60 * 1000 + 5_000), "1h 5s");
  assert.equal(formatElapsedMs(2 * 60 * 60 * 1000 + 30 * 60 * 1000), "2h 30m");
  assert.equal(
    formatElapsedMs(1 * 60 * 60 * 1000 + 2 * 60 * 1000 + 3 * 1000),
    "1h 2m 3s"
  );
});

test("formatElapsedMs accepts bigint without throwing", () => {
  assert.equal(formatElapsedMs(12_000n), "12s");
});

test("formatElapsedMs returns em-dash for invalid input", () => {
  assert.equal(formatElapsedMs(null), "—");
  assert.equal(formatElapsedMs(undefined), "—");
  assert.equal(formatElapsedMs(-1), "—");
  assert.equal(formatElapsedMs(Number.NaN), "—");
});

test("escapeHtml escapes the standard 4 chars", () => {
  assert.equal(
    escapeHtml(`<a href="x">&copy;</a>`),
    "&lt;a href=&quot;x&quot;&gt;&amp;copy;&lt;/a&gt;"
  );
  assert.equal(escapeHtml(""), "");
});

test("escapeHtml coerces non-strings", () => {
  assert.equal(escapeHtml(42), "42");
  assert.equal(escapeHtml(null), "null");
  assert.equal(escapeHtml(undefined), "undefined");
});

test("pickSqlToRun returns selection when non-empty", () => {
  const value = "SELECT 1;\nSELECT 2;";
  const selected = pickSqlToRun({
    value,
    selectionStart: 0,
    selectionEnd: "SELECT 1;".length,
  });
  assert.equal(selected, "SELECT 1;");
});

test("pickSqlToRun falls back to full value when selection collapsed", () => {
  const value = "  SELECT 1;\n  ";
  const picked = pickSqlToRun({ value, selectionStart: 4, selectionEnd: 4 });
  assert.equal(picked, "SELECT 1;");
});

test("pickSqlToRun trims whitespace-only selection", () => {
  const value = "SELECT 1;";
  // Selecting just whitespace ("") between two equal positions is not really
  // a selection; verify that a whitespace-only selection falls back too.
  const picked = pickSqlToRun({
    value: "SELECT 1;   ",
    selectionStart: 9,
    selectionEnd: 12, // selects "   "
  });
  assert.equal(picked, "SELECT 1;");
  // And a real collapsed cursor returns full trimmed value.
  assert.equal(
    pickSqlToRun({ value, selectionStart: 3, selectionEnd: 3 }),
    "SELECT 1;"
  );
});
