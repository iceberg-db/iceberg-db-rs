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

pub(crate) mod commit;
pub(crate) mod expr_to_predicate;
pub(crate) mod file_groups;
pub(crate) mod metadata_scan;
pub(crate) mod physical_expr_to_predicate;
pub(crate) mod project;
pub(crate) mod repartition;
pub(crate) mod dim_key_bounds;
pub(crate) mod fact_filter_pushdown;
pub(crate) mod fk_bound_pushdown;
pub(crate) mod join_collect_left;
pub(crate) mod join_reorder;
pub(crate) mod join_utils;
pub(crate) mod scan;
pub(crate) mod scan_statistics;
pub(crate) mod sort;
pub(crate) mod write;

pub(crate) const DATA_FILES_COL_NAME: &str = "data_files";

pub use project::project_with_partition;
pub use scan::IcebergTableScan;
