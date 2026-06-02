# iceberg 0.9.1 — iceberg-db-rs fork

Upstream: [apache/iceberg-rust](https://github.com/apache/iceberg-rust) `iceberg` 0.9.1.

## Changes (summary)

| Area | Files | Purpose |
|------|-------|---------|
| WASM / browser scans | `src/io/object_cache.rs` (via overlay), `src/utils.rs` | Bounded S3 concurrency, higher default scan parallelism on WASM |
| Equality deletes | `src/arrow/caching_delete_file_loader.rs`, `delete_filter.rs` | Fix inverted predicate / missing delete predicate errors |
| Native scans | `src/utils.rs` | Tunable `available_parallelism()` for file/manifest concurrency |
| Efficient `IN` row filter | `src/arrow/reader.rs` | Single-pass hash-set membership for integer `IN` predicates (see below) |

## Efficient integer `IN` row filter

`PredicateConverter::r#in` previously built its Arrow `RowFilter` as
`OR(eq(col, lit) for lit in literals)` — an `O(rows × literals)` loop that runs a
full-column comparison kernel per literal. That made large `IN` lists (e.g. a
semi-join reduction pushing ~27K matching `cd_demo_sk` keys onto `store_sales`)
pathologically slow, so such predicates could not be used as scan-time filters.

The patched `r#in` adds a fast path: when every literal is an integer (`Int`/`Long`,
the common case for foreign-key columns), it builds a `HashSet<i128>` once and tests
membership in a single pass over the column (`integer_literal_set` /
`integer_array_is_in`). `i128` widens both 32- and 64-bit keys so column/literal
width mismatches still compare correctly; null rows do not match. Non-integer sets
fall back to the original `eq`/`or` accumulation.

Combined with Parquet predicate pushdown (late materialization), a large pushed `IN`
list now decodes the non-filter projected columns only for surviving rows — letting
the star-schema FK-bound pushdown reduce a fact scan's output to the matching rows
(DuckDB's runtime "`IN BF`" equivalent) instead of materializing the full fact side
before the join.

Refresh baseline from crates.io with [`scripts/vendor-iceberg-patch.ps1`](../../scripts/vendor-iceberg-patch.ps1), then re-apply overlay + edits.

See also [`patches/README.md`](../README.md) and the root [`README.md`](../../README.md) vendor patches section.
