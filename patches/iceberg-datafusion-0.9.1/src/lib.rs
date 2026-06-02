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

mod catalog;
pub use catalog::*;

mod error;
pub use error::*;

pub mod physical_plan;
mod schema;
pub mod table;
pub use table::table_provider_factory::IcebergTableProviderFactory;
pub use table::*;

use std::sync::Arc;

use datafusion::physical_optimizer::PhysicalOptimizerRule;

use physical_plan::fact_filter_pushdown::IcebergFactFilterPushdown;
use physical_plan::fk_bound_pushdown::IcebergFkBoundPushdown;
use physical_plan::join_collect_left::IcebergCollectLeft;
use physical_plan::join_reorder::IcebergJoinReorder;

/// Iceberg hash-join reorder rule (insert before DataFusion `join_selection`).
pub fn iceberg_join_reorder_rule() -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    Arc::new(IcebergJoinReorder::new())
}

/// Broadcast small Iceberg build sides via `CollectLeft` (insert before `SanityCheckPlan`).
pub fn iceberg_collect_left_rule() -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    Arc::new(IcebergCollectLeft::new())
}

/// Accumulate probe-path join dynamic filters on fact Iceberg scans (insert after post filter pushdown).
pub fn iceberg_fact_filter_pushdown_rule() -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    Arc::new(IcebergFactFilterPushdown::new())
}

/// Static FK bounds from filtered dimension scans onto fact scans (insert after post filter pushdown).
pub fn iceberg_fk_bound_pushdown_rule() -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    Arc::new(IcebergFkBoundPushdown::new())
}

fn insert_before_rule(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    before: &str,
    rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
) {
    let insert_at = rules
        .iter()
        .position(|r| r.name() == before)
        .unwrap_or(rules.len());
    rules.insert(insert_at, rule);
}

fn insert_after_rule(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    after: &str,
    rule: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
) {
    let insert_at = rules
        .iter()
        .position(|r| r.name() == after)
        .map(|i| i + 1)
        .unwrap_or(rules.len());
    rules.insert(insert_at, rule);
}

/// Register Iceberg-specific physical optimizer rules on a DataFusion session rule list.
pub fn insert_iceberg_physical_optimizer_rules(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) {
    insert_before_rule(rules, "join_selection", iceberg_join_reorder_rule());
    insert_before_rule(rules, "SanityCheckPlan", iceberg_collect_left_rule());
    insert_after_rule(
        rules,
        "FilterPushdown(Post)",
        iceberg_fk_bound_pushdown_rule(),
    );
    insert_after_rule(
        rules,
        "iceberg_fk_bound_pushdown",
        iceberg_fact_filter_pushdown_rule(),
    );
}

/// Backward-compatible alias.
pub fn insert_iceberg_join_reorder(
    rules: &mut Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
) {
    insert_iceberg_physical_optimizer_rules(rules);
}

pub(crate) mod task_writer;
