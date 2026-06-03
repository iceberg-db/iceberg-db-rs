// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Plan-time join-key constraints from filtered dimension Iceberg scans.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use datafusion::arrow::array::Array;
use datafusion::common::ScalarValue;
use datafusion::error::Result as DFResult;
use futures::TryStreamExt;
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::scan::FileScanTask;
use iceberg::spec::Datum;
use tokio::runtime::Handle;
use uuid::Uuid;

use super::expr_to_predicate::scalar_value_to_datum;
use super::scan::IcebergTableScan;
use crate::to_datafusion_error;

/// Skip plan-time dimension reads larger than this (rows).
const MAX_PLAN_TIME_DIM_ROWS: u64 = 512 * 1024;

/// Maximum distinct join keys to emit as an Iceberg `IN` predicate. Sized so that
/// selective semi-join reductions (e.g. ~27K matching `cd_demo_sk`) are pushed onto
/// the fact scan as a membership `RowFilter`, while pathological sets are skipped.
const MAX_INLIST_VALUES: usize = 131_072;

/// Static FK constraint inferred from a filtered dimension scan.
#[derive(Debug, Clone)]
pub enum JoinKeyConstraint {
    Range(Datum, Datum),
    InList(Vec<Datum>),
}

/// Process-global cache of plan-time FK constraints. Plan-time dimension reads are
/// expensive and the same `(table, snapshot, filter, key)` recurs across warmups,
/// iterations and EXPLAIN runs in a single process, so we memoize the result.
type ConstraintCacheKey = (Uuid, Option<i64>, String, String);

fn constraint_cache() -> &'static Mutex<HashMap<ConstraintCacheKey, Option<JoinKeyConstraint>>> {
    static CACHE: OnceLock<Mutex<HashMap<ConstraintCacheKey, Option<JoinKeyConstraint>>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_key(scan: &IcebergTableScan, key_column: &str) -> ConstraintCacheKey {
    let predicate_repr = scan
        .predicates()
        .map(|p| p.to_string())
        .unwrap_or_default();
    (
        scan.table().metadata().uuid(),
        scan.snapshot_id(),
        predicate_repr,
        key_column.to_string(),
    )
}

/// Infer a static FK constraint for `key_column` on a filtered dimension scan.
pub fn compute_join_key_constraint(
    scan: &IcebergTableScan,
    key_column: &str,
) -> DFResult<Option<JoinKeyConstraint>> {
    let key = cache_key(scan, key_column);
    if let Some(cached) = constraint_cache()
        .lock()
        .expect("constraint cache poisoned")
        .get(&key)
        .cloned()
    {
        return Ok(cached);
    }

    let constraint = block_on(plan_time_join_key_constraint(scan, key_column))?;

    constraint_cache()
        .lock()
        .expect("constraint cache poisoned")
        .insert(key, constraint.clone());
    Ok(constraint)
}

fn block_on<T>(
    fut: impl std::future::Future<Output = DFResult<T>>,
) -> DFResult<T> {
    match Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
            rt.block_on(fut)
        }
    }
}

async fn plan_time_join_key_constraint(
    scan: &IcebergTableScan,
    key_column: &str,
) -> DFResult<Option<JoinKeyConstraint>> {
    let table = scan.table().clone();
    let mut builder = match scan.snapshot_id() {
        Some(id) => table.scan().snapshot_id(id),
        None => table.scan(),
    };
    builder = builder.select([key_column]);
    if let Some(pred) = scan.predicates().cloned() {
        builder = builder.with_filter(pred);
    }
    let table_scan = builder.build().map_err(to_datafusion_error)?;
    let tasks: Vec<FileScanTask> = table_scan
        .plan_files()
        .await
        .map_err(to_datafusion_error)?
        .try_collect()
        .await
        .map_err(to_datafusion_error)?;

    if tasks.is_empty() {
        return Ok(None);
    }

    // Unfiltered dimensions: use manifest row counts to avoid full-table reads at plan time.
    if scan.predicates().is_none() {
        let total_rows: u64 = tasks.iter().filter_map(|t| t.record_count).sum();
        if total_rows == 0 || total_rows > MAX_PLAN_TIME_DIM_ROWS {
            return Ok(None);
        }
    }

    let task_stream = futures::stream::iter(tasks.into_iter().map(Ok));
    let mut batch_stream = ArrowReaderBuilder::new(table.file_io().clone())
        .build()
        .read(Box::pin(task_stream))
        .map_err(to_datafusion_error)?;

    let mut distinct: HashSet<Datum> = HashSet::new();
    let mut rows_read: u64 = 0;

    while let Some(batch) = batch_stream.try_next().await.map_err(to_datafusion_error)? {
        if batch.num_rows() == 0 {
            continue;
        }
        rows_read += batch.num_rows() as u64;
        if rows_read > MAX_PLAN_TIME_DIM_ROWS {
            return Ok(None);
        }
        let col_idx = batch
            .schema()
            .index_of(key_column)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        collect_distinct_from_array(batch.column(col_idx).as_ref(), &mut distinct)?;
    }

    if distinct.is_empty() {
        return Ok(None);
    }

    let min = min_datum(&distinct).expect("non-empty distinct set");
    let max = max_datum(&distinct).expect("non-empty distinct set");
    // Prefer tight min/max for manifest file pruning (e.g. d_year=2000 → ~366-day range).
    if range_is_selective(&min, &max, distinct.len()) {
        return Ok(Some(JoinKeyConstraint::Range(min, max)));
    }

    if distinct.len() <= MAX_INLIST_VALUES {
        return Ok(Some(JoinKeyConstraint::InList(distinct.into_iter().collect())));
    }

    if scan.predicates().is_none() {
        return Ok(None);
    }

    Ok(None)
}

fn collect_distinct_from_array(array: &dyn Array, distinct: &mut HashSet<Datum>) -> DFResult<()> {
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let value = ScalarValue::try_from_array(array, row)
            .map_err(|e| datafusion::common::DataFusionError::External(Box::new(e)))?;
        if let Some(datum) = scalar_value_to_datum(&value) {
            distinct.insert(datum);
        }
    }
    Ok(())
}

fn min_datum(values: &HashSet<Datum>) -> Option<Datum> {
    values
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
        .cloned()
}

fn max_datum(values: &HashSet<Datum>) -> Option<Datum> {
    values
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal))
        .cloned()
}

/// Min/max is useful only when the value range is roughly as tight as the distinct count.
fn range_is_selective(min: &Datum, max: &Datum, distinct: usize) -> bool {
    let Some(lo) = datum_as_i64(min) else {
        return false;
    };
    let Some(hi) = datum_as_i64(max) else {
        return false;
    };
    if hi < lo {
        return false;
    }
    let span = hi.saturating_sub(lo).saturating_add(1) as usize;
    span <= distinct.saturating_mul(4)
}

fn datum_as_i64(datum: &Datum) -> Option<i64> {
    use iceberg::spec::PrimitiveLiteral;
    match datum.literal() {
        PrimitiveLiteral::Int(v) => Some(*v as i64),
        PrimitiveLiteral::Long(v) => Some(*v),
        _ => None,
    }
}
