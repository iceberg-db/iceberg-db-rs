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

//! Force `CollectLeft` hash joins when an Iceberg dimension is small enough to broadcast.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use super::join_utils::{
    as_hash_join, as_iceberg_scan, is_large_unfiltered_fact, is_small_iceberg_build,
};

#[derive(Default, Debug)]
pub struct IcebergCollectLeft {}

impl IcebergCollectLeft {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for IcebergCollectLeft {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        visit(plan, false)
    }

    fn name(&self) -> &str {
        "iceberg_collect_left"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn visit(plan: Arc<dyn ExecutionPlan>, parent_is_aggregate: bool) -> DFResult<Arc<dyn ExecutionPlan>> {
    let parent_is_aggregate_for_children = plan.as_any().is::<AggregateExec>();

    let children: Vec<_> = plan
        .children()
        .iter()
        .map(|c| visit(Arc::clone(c), parent_is_aggregate_for_children))
        .collect::<DFResult<_>>()?;

    let mut plan = if children.is_empty() {
        plan
    } else {
        plan.with_new_children(children)?
    };

    let Some(hash_join) = as_hash_join(&plan) else {
        return Ok(plan);
    };
    if !hash_join.join_type().supports_swap() {
        return Ok(plan);
    }

    // Converting or rewriting CollectLeft after EnforceDistribution breaks partitioned
    // parents (e.g. q01 partial aggregates that expect hash-partitioned join output).
    if parent_is_aggregate {
        return Ok(plan);
    }

    if matches!(hash_join.partition_mode(), PartitionMode::CollectLeft) {
        return ensure_collect_left_build_side(plan);
    }

    let left = hash_join.left();
    let right = hash_join.right();
    let left_dim = is_small_iceberg_build(left) && as_iceberg_scan(left).is_some();
    let right_dim = is_small_iceberg_build(right) && as_iceberg_scan(right).is_some();
    let left_fact = is_large_unfiltered_fact(left);
    let right_fact = is_large_unfiltered_fact(right);

    let eligible = (left_dim && right_fact) || (right_dim && left_fact);
    if !eligible {
        return Ok(plan);
    }

    let updated = if right_dim && left_fact {
        hash_join.swap_inputs(PartitionMode::CollectLeft)?
    } else {
        Arc::new(HashJoinExec::try_new(
            Arc::clone(left),
            Arc::clone(right),
            hash_join.on().to_vec(),
            hash_join.filter().cloned(),
            hash_join.join_type(),
            hash_join.projection.clone(),
            PartitionMode::CollectLeft,
            hash_join.null_equality(),
        )?) as Arc<dyn ExecutionPlan>
    };

    ensure_collect_left_build_side(updated)
}

/// CollectLeft requires a single-partition build side; coalesce after EnforceDistribution.
fn ensure_collect_left_build_side(plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
    let Some(hash_join) = as_hash_join(&plan) else {
        return Ok(plan);
    };
    if !matches!(hash_join.partition_mode(), PartitionMode::CollectLeft) {
        return Ok(plan);
    }
    let coalesced = coalesce_if_needed(Arc::clone(hash_join.left()));
    if Arc::ptr_eq(hash_join.left(), &coalesced) {
        return Ok(plan);
    }
    Ok(Arc::new(HashJoinExec::try_new(
        coalesced,
        Arc::clone(hash_join.right()),
        hash_join.on().to_vec(),
        hash_join.filter().cloned(),
        hash_join.join_type(),
        hash_join.projection.clone(),
        PartitionMode::CollectLeft,
        hash_join.null_equality(),
    )?) as Arc<dyn ExecutionPlan>)
}

fn coalesce_if_needed(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if plan.output_partitioning().partition_count() <= 1 {
        return plan;
    }
    Arc::new(CoalescePartitionsExec::new(plan)) as Arc<dyn ExecutionPlan>
}
