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

//! Iceberg-aware hash-join input reordering without `ExecutionPlan::partition_statistics`.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::Result as DFResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use super::join_utils::{
    as_hash_join, as_iceberg_scan, iceberg_join_weight, is_large_unfiltered_fact,
    is_small_iceberg_build,
};

#[derive(Default, Debug)]
pub struct IcebergJoinReorder {}

impl IcebergJoinReorder {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for IcebergJoinReorder {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(|plan| {
            let Some(hash_join) = as_hash_join(&plan) else {
                return Ok(Transformed::no(plan));
            };
            if !hash_join.join_type().supports_swap() {
                return Ok(Transformed::no(plan));
            }

            if should_swap_for_star_schema(hash_join)? {
                let swapped = hash_join.swap_inputs(*hash_join.partition_mode())?;
                return Ok(Transformed::yes(swapped));
            }

            Ok(Transformed::no(plan))
        })
        .data()
    }

    fn name(&self) -> &str {
        "iceberg_join_reorder"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn should_swap_for_star_schema(
    hash_join: &datafusion::physical_plan::joins::HashJoinExec,
) -> DFResult<bool> {
    let left = hash_join.left();
    let right = hash_join.right();

    // Direct fact-on-build / filtered-dimension-on-probe pattern (both Iceberg scans).
    if as_iceberg_scan(left).is_some() && as_iceberg_scan(right).is_some() {
        if is_large_unfiltered_fact(left) && is_small_iceberg_build(right) {
            return Ok(true);
        }
        if let (Some(lw), Some(rw)) = (iceberg_join_weight(left), iceberg_join_weight(right)) {
            return Ok(lw > rw);
        }
    }

    Ok(false)
}
