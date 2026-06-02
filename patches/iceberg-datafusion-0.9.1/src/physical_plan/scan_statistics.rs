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

//! Iceberg snapshot statistics for DataFusion cost-based decisions.
//!
//! DataFusion's physical join optimizer swaps hash-join build/probe inputs using
//! `Statistics::num_rows` or `Statistics::total_byte_size`. Upstream iceberg-datafusion did
//! not expose any stats, so the planner kept SQL join order — large fact tables stayed on the
//! build side and dynamic join filters never reached `IcebergTableScan`.

use std::collections::HashMap;

use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use iceberg::table::Table;

/// Iceberg snapshot summary key: cumulative row count in the snapshot.
pub const TOTAL_RECORDS: &str = "total-records";
/// Iceberg snapshot summary key: cumulative data file size in bytes.
pub const TOTAL_FILE_SIZE: &str = "total-files-size";

/// Snapshot row count for join reordering (does not use DataFusion `Statistics` API).
pub fn row_estimate(table: &Table, snapshot_id: Option<i64>) -> Option<usize> {
    statistics_from_table(table, snapshot_id)
        .num_rows
        .get_value()
        .copied()
}

/// Read table-level [`Statistics`] from the Iceberg snapshot summary.
pub fn statistics_from_table(table: &Table, snapshot_id: Option<i64>) -> Statistics {
    let metadata = table.metadata();
    let snapshot = snapshot_id
        .and_then(|id| metadata.snapshot_by_id(id))
        .or_else(|| metadata.current_snapshot());

    let Some(snapshot) = snapshot else {
        return Statistics::default();
    };

    let props = &snapshot.summary().additional_properties;
    // Inexact: snapshot totals ignore scan-local filters pushed into IcebergTableScan.
    let num_rows = parse_u64_prop(props, TOTAL_RECORDS).map(|n| Precision::Inexact(n as usize));
    let total_byte_size =
        parse_u64_prop(props, TOTAL_FILE_SIZE).map(|n| Precision::Inexact(n as usize));

    Statistics {
        num_rows: num_rows.unwrap_or(Precision::Absent),
        total_byte_size: total_byte_size.unwrap_or(Precision::Absent),
        column_statistics: vec![],
    }
}

/// Per-partition stats for parallel scans (reserved for future use).
#[allow(dead_code)]
pub fn partition_statistics_from_table(
    table: &Table,
    snapshot_id: Option<i64>,
    partition: Option<usize>,
    scan_partitions: usize,
) -> Statistics {
    let stats = statistics_from_table(table, snapshot_id);
    match partition {
        None => stats,
        Some(_) if scan_partitions <= 1 => stats,
        Some(_) => scale_statistics(stats, 1.0 / scan_partitions as f64),
    }
}

fn parse_u64_prop(props: &HashMap<String, String>, key: &str) -> Option<u64> {
    props.get(key)?.parse().ok()
}

fn scale_statistics(stats: Statistics, factor: f64) -> Statistics {
    Statistics {
        num_rows: scale_usize_precision(stats.num_rows, factor),
        total_byte_size: scale_usize_precision(stats.total_byte_size, factor),
        column_statistics: stats.column_statistics,
    }
}

fn scale_usize_precision(p: Precision<usize>, factor: f64) -> Precision<usize> {
    match p {
        Precision::Exact(v) => Precision::Exact(scale_usize(v, factor)),
        Precision::Inexact(v) => Precision::Inexact(scale_usize(v, factor)),
        other => other,
    }
}

fn scale_usize(v: usize, factor: f64) -> usize {
    ((v as f64) * factor).ceil().max(1.0) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_stats_are_scaled() {
        let stats = Statistics {
            num_rows: Precision::Inexact(1000),
            total_byte_size: Precision::Inexact(10000),
            column_statistics: vec![],
        };
        let scaled = scale_statistics(stats, 0.25);
        assert_eq!(scaled.num_rows, Precision::Inexact(250));
        assert_eq!(scaled.total_byte_size, Precision::Inexact(2500));
    }
}
