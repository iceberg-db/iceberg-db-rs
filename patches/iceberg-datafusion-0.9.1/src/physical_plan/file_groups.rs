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

//! Split Iceberg [`FileScanTask`] lists across DataFusion scan partitions.
//!
//! DataFusion's physical optimizer expects leaf scans to expose multiple output
//! partitions when `target_partitions > 1` and `repartition_file_scans` is enabled.
//! Upstream iceberg-datafusion used `UnknownPartitioning(1)` and ignored the partition
//! index in `execute()`. We assign files to partitions with a greedy size balancer
//! (same idea as DataFusion's [`FileGroupPartitioner`](https://docs.rs/datafusion/latest/datafusion/datasource/file_groups/struct.FileGroupPartitioner.html)).

use iceberg::scan::FileScanTask;

/// Default minimum file size (bytes) before we bother splitting a scan across partitions.
/// Matches DataFusion `OptimizerOptions::repartition_file_min_size` (10 MiB).
pub const DEFAULT_REPARTITION_FILE_MIN_SIZE: usize = 10 * 1024 * 1024;

/// Assign each [`FileScanTask`] to one of `target_partitions` groups.
///
/// Returns exactly `target_partitions` vectors (some may be empty). When the total
/// planned byte volume is below `min_file_size`, all tasks stay in partition 0 so
/// small tables are not over-sharded.
pub fn partition_file_scan_tasks(
    mut tasks: Vec<FileScanTask>,
    target_partitions: usize,
    min_file_size: usize,
) -> Vec<Vec<FileScanTask>> {
    let target_partitions = target_partitions.max(1);
    if tasks.is_empty() {
        return vec![Vec::new(); target_partitions];
    }
    if target_partitions == 1 {
        return vec![tasks];
    }

    let total_bytes: u64 = tasks.iter().map(task_bytes).sum();
    if total_bytes < min_file_size as u64 {
        let mut groups = vec![Vec::new(); target_partitions];
        groups[0] = tasks;
        return groups;
    }

    // Largest files first → assign each to the currently lightest partition.
    tasks.sort_by_key(|t| std::cmp::Reverse(task_bytes(t)));
    let mut groups: Vec<Vec<FileScanTask>> = vec![Vec::new(); target_partitions];
    let mut group_bytes = vec![0u64; target_partitions];

    for task in tasks {
        let idx = group_bytes
            .iter()
            .enumerate()
            .min_by_key(|(_, bytes)| **bytes)
            .map(|(i, _)| i)
            .unwrap_or(0);
        group_bytes[idx] += task_bytes(&task);
        groups[idx].push(task);
    }

    groups
}

fn task_bytes(task: &FileScanTask) -> u64 {
    if task.length > 0 {
        task.length
    } else {
        task.file_size_in_bytes
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use iceberg::spec::{DataFileFormat, Schema};

    fn dummy_task(bytes: u64) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: bytes,
            start: 0,
            length: bytes,
            record_count: None,
            data_file_path: format!("file-{bytes}.parquet"),
            data_file_format: DataFileFormat::Parquet,
            schema: Arc::new(Schema::builder().build().unwrap()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: true,
        }
    }

    #[test]
    fn single_partition_returns_all_tasks() {
        let tasks = vec![dummy_task(100), dummy_task(200)];
        let groups = partition_file_scan_tasks(tasks, 1, 0);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
    }

    #[test]
    fn small_table_stays_on_partition_zero() {
        let tasks = vec![dummy_task(1024), dummy_task(2048)];
        let groups = partition_file_scan_tasks(tasks, 8, DEFAULT_REPARTITION_FILE_MIN_SIZE);
        assert!(groups[0].len() == 2);
        assert!(groups[1..].iter().all(|g| g.is_empty()));
    }

    #[test]
    fn balances_large_files_across_partitions() {
        let tasks = vec![
            dummy_task(50_000_000),
            dummy_task(50_000_000),
            dummy_task(50_000_000),
            dummy_task(50_000_000),
        ];
        let groups = partition_file_scan_tasks(tasks, 2, 0);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].len(), 2);
        assert_eq!(groups[1].len(), 2);
    }
}
