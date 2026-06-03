# iceberg-datafusion 0.9.1 — iceberg-db-rs fork

Upstream: [apache/iceberg-rust](https://github.com/apache/iceberg-rust) `iceberg-datafusion` 0.9.1.

This fork adds **general** scan optimizations for DataFusion 52 (no query-specific rewrites).

## Changes

### 1. Dynamic & static filter pushdown (`IcebergTableScan`)

**Problem:** Upstream `IcebergTableScan` only converted logical `Expr` filters at plan construction. DataFusion 52 pushes additional filters in a second phase (`FilterPushdownPhase::Post`), including [`DynamicFilterPhysicalExpr`](https://docs.rs/datafusion/latest/datafusion/physical_expr/expressions/struct.DynamicFilterPhysicalExpr.html) from hash joins. Those never reached Iceberg, so fact-table scans showed `predicate:[]` even when dimension joins were selective.

**Fix:**

- Implement [`ExecutionPlan::handle_child_pushdown_result`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.handle_child_pushdown_result) on `IcebergTableScan`.
- Store accepted physical filters and merge them at execute time with static predicates.
- New module `physical_plan/physical_expr_to_predicate.rs` converts physical expressions (including resolved dynamic filters and literal `InList`) to Iceberg [`Predicate`](https://docs.rs/iceberg/latest/iceberg/expr/enum.Predicate.html) for manifest + row-group pruning.

**Files:**

- `src/physical_plan/scan.rs` — pushdown hook, execute-time merge
- `src/physical_plan/physical_expr_to_predicate.rs` — physical → Iceberg conversion
- `src/physical_plan/expr_to_predicate.rs` — export `scalar_value_to_datum` for reuse
- `src/physical_plan/mod.rs` — module exports

**Note:** Dynamic filters apply to the **probe** side of hash joins. Star-schema queries still need the planner to put filtered dimensions on the build side and the fact table on the probe side for fact-table pruning (standard DataFusion join order, not TPC-DS-specific SQL rewriting).

### 2. Parallel scan partitions

**Problem:** Upstream hard-coded `Partitioning::UnknownPartitioning(1)` and ignored the partition index in `execute()`, so Iceberg scans stayed single-threaded at the plan level.

**Fix:**

- Pass `session.config().target_partitions()` from `TableProvider::scan` into `IcebergTableScan`.
- Implement [`ExecutionPlan::repartitioned`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html#method.repartitioned) when `repartition_file_scans` is enabled.
- At execute time: `plan_files()` → split [`FileScanTask`](https://docs.rs/iceberg/latest/iceberg/scan/struct.FileScanTask.html)s across partitions (`file_groups.rs`) → read one subset per partition via `ArrowReaderBuilder`.

**Files:**

- `src/physical_plan/file_groups.rs` — size-balanced file grouping
- `src/physical_plan/scan.rs` — partitioned `execute`
- `src/table/mod.rs` — `scan_partitions` from session config

### 3. Iceberg snapshot statistics (join reordering)

**Problem:** Upstream `IcebergTableScan` returned no row estimates, so DataFusion kept SQL join order: the fact table (`store_sales`) stayed on the **build** side (~28M rows) and dynamic filters never reached the scan.

**Fix:**

- New module `physical_plan/scan_statistics.rs` reads `total-records` / `total-files-size` from the Iceberg snapshot summary.
- New physical rule `physical_plan/join_reorder.rs` (`IcebergJoinReorder`) swaps hash-join inputs when one side is an unfiltered Iceberg scan with a larger snapshot row count than a filtered sibling scan.
- Registered via `insert_iceberg_join_reorder()` when building the DataFusion session (`idb-sql`).

**Note:** We intentionally do **not** implement `ExecutionPlan::partition_statistics` on `IcebergTableScan`. Returning snapshot totals through DataFusion's `Statistics` API currently triggers planner bugs in DataFusion 52 (broken `FilterExec` projections / analysis assertions on TPC-DS q03).

**Validate:** `idb-bench --explain q07` — `store_sales` `IcebergTableScan` should be the **probe** child of selective joins and show non-empty `predicate:[...]` when dynamic pushdown applies.

### 4. Fact-table dynamic filter accumulation (`IcebergFactFilterPushdown`)

**Problem:** Nested star joins only push the innermost join dynamic filter onto the fact scan. Outer dimension joins (date, item, promotion) produce additional bounds that never reach `store_sales` manifest pruning.

**Fix:**

- New rule `physical_plan/fact_filter_pushdown.rs` walks the physical plan post–filter-pushdown and attaches each ancestor hash join's `DynamicFilterPhysicalExpr` to **direct** large fact `IcebergTableScan` nodes on the probe path (matches scan nodes only — does not peel `RepartitionExec`, which would drop distribution wrappers).
- Targets scans via `is_large_fact_table()` (snapshot row count), **not** `is_fact_like()` (unfiltered only), so dynamic filters still stack after `IcebergFkBoundPushdown` has AND-merged static FK bounds onto the same fact scan.
- Execute-time merge in `scan.rs` resolves dynamic filters via non-blocking `current()` and AND-decomposes convertible bounds in `physical_expr_to_predicate.rs`.

**Files:** `fact_filter_pushdown.rs`, `physical_expr_to_predicate.rs`, `scan.rs`

### 5. Bounds / InList physical predicate conversion

**Problem:** Join dynamic filters often arrive as compound physical expressions (`HashTableLookupExpr` + sibling bounds). Upstream conversion was all-or-nothing.

**Fix:** `convert_physical_expr_to_predicates()` AND-decomposes filters, skips non-prunable `HashTableLookupExpr` / `lit(true)`, and converts remaining bounds and small `InList` literals to Iceberg predicates for manifest min/max and bloom pruning.

**Files:** `physical_plan/physical_expr_to_predicate.rs`

### 6. CollectLeft for small Iceberg dimensions (`IcebergCollectLeft`)

**Problem:** Partitioned hash joins repartition tiny filtered dimensions (e.g. `date_dim` with `d_year = 2000`) across all cores unnecessarily.

**Fix:**

- New rule `physical_plan/join_collect_left.rs` converts eligible dim↔fact joins to `PartitionMode::CollectLeft`, inserting `CoalescePartitionsExec` on the build side when `EnforceDistribution` has already added hash repartition.
- Runs immediately before `SanityCheckPlan`; skips joins whose **direct parent** is an aggregate (preserves hash-partitioned partial-agg inputs in q01).
- Helpers in `physical_plan/join_utils.rs` (`is_fact_like`, `is_small_iceberg_build`, snapshot weights).

**Files:** `join_collect_left.rs`, `join_utils.rs`, `lib.rs` (`insert_iceberg_physical_optimizer_rules`)

### 7. Static FK bound inference (`IcebergFkBoundPushdown`)

**Problem:** Star-schema joins keep unfiltered fact scans (`predicate:[]`) because dimension filters live on build-side scans only. DuckDB derives static bounds (e.g. `ss_sold_date_sk` IN / range from `d_year = 2000`) before the fact scan.

**Fix:**

- New rule `physical_plan/fk_bound_pushdown.rs` walks hash joins post–filter-pushdown. When one side is a **filtered** dimension Iceberg scan (static Iceberg predicates) and the other side contains a fact scan, plan-time key inference attaches FK constraints to matching fact scans in that join subtree.
- New module `physical_plan/dim_key_bounds.rs` reads filtered dimension keys via `plan_files()` + Arrow (capped at 512 Ki rows). **Prefers tight min/max `Range` predicates** when `max−min` is selective (e.g. `d_year = 2000` → 366-day span) so Iceberg manifest file pruning can drop non-overlapping data files; falls back to `IN` lists for ≤131072 distinct non-selective sets (e.g. ~27 Ki `cd_demo_sk`). Skips unfiltered dimensions and useless wide ranges.
- `IcebergTableScan` records `planned_files=N` after the first `plan_files()` during execute (visible in `idb-bench --explain q07` summary and EXPLAIN ANALYZE plan text).
- `IcebergTableScan::with_additional_static_predicate()` AND-merges constraints. Applies only when the fact column exists on the target scan (avoids cross-table predicate bugs in multi-fact queries like q01).

**Plan-time cache (a):** `dim_key_bounds` memoizes the inferred constraint per `(table uuid, snapshot, dimension predicate, key column)` in a process-global cache, so the synchronous dimension reads run once instead of on every warmup / iteration / EXPLAIN plan.

**Stacking constraints across joins (b):** fact-target matching (`contains_fact_iceberg_scan`, `apply_predicate_to_fact_scans`) uses `IcebergTableScan::is_large_fact_table()` — a snapshot-row-count check that stays true after predicates are pushed — so `cdemo`, `date` and `promo` constraints all AND onto the same `store_sales` scan. Dimension *detection* (`direct_dimension_scan`) still uses the predicate-based `is_fact_like()` so that a large-but-filtered dimension (e.g. `customer_demographics` ~1.9M rows) is still recognized as a dimension, not a fact.

Together with the patched integer `IN` row filter in `iceberg` (single-pass hash-set membership + Parquet late materialization), the pushed `IN` lists now act as DuckDB's runtime "`IN BF`" equivalent.

**Files:** `fk_bound_pushdown.rs`, `dim_key_bounds.rs`, `scan.rs`, `lib.rs`

**Validate:** `idb-bench --explain q07` — summary block lists `store_sales: planned_files=2` (same file count as DuckDB on Tier-1 date-clustered warehouse) with `ss_sold_date_sk` pushed as **range** (`>=` / `<=`). Fact scan still uses `IN` for wide semi-join keys (e.g. `cd_demo_sk`). Innermost join fact input ~**75 Ki** rows, matching DuckDB's ~94 Ki. Row-group decode pruning still benefits from blooms + multi–row-group files when present.

**Tier 1 data + scan (see `benchmarks/tpcds/README.md`):** Regenerate with `setup-local-tpcds.ps1` (star Parquet + sorted/bloom Iceberg write). `read_partition` enables iceberg `row_selection` when a scan predicate is present. Use `bench-tier1.yaml` (`repartition_joins: false`, `target_partitions: 4`).

## Optimizer registration order

| Rule | Insert | Purpose |
|------|--------|---------|
| `iceberg_join_reorder` | before `join_selection` | Fact probe / dim build swap via snapshot weights |
| `iceberg_fk_bound_pushdown` | after `FilterPushdown(Post)` | Static FK bounds from filtered dimension scans |
| `iceberg_fact_filter_pushdown` | after `iceberg_fk_bound_pushdown` | Accumulate probe-path join filters on fact scans |
| `iceberg_collect_left` | before `SanityCheckPlan` | Broadcast small Iceberg dims; coalesce build side |

Registered via `insert_iceberg_physical_optimizer_rules()` in `idb-sql`.

| Option | Effect |
|--------|--------|
| `execution.target_partitions` | Initial scan partition count (also set via `idb-cli --target-partitions`) |
| `optimizer.repartition_file_scans` | Allows `repartitioned()` to increase scan partitions |
| `optimizer.repartition_file_min_size` | Minimum table bytes before splitting files across partitions (default 10 MiB) |
| `optimizer.enable_join_dynamic_filter_pushdown` | Hash join → scan dynamic filters (default **on**) |
| `optimizer.repartition_joins` | Parallel hash joins (default **on**, overridable via `SessionOptions` / `bench.yaml`) |

Note: Join input swap for Iceberg scans is handled by `IcebergJoinReorder` using snapshot row estimates, not DataFusion `partition_statistics`.

## Tests

```powershell
cargo test -p iceberg-datafusion --lib physical_expr
cargo test -p iceberg-datafusion --lib file_groups
```

Integration: `idb-cli --explain-analyze` on a selective join; probe-side `IcebergTableScan` should show `partitions>1` on large tables and merged predicates when pushdown succeeds.
