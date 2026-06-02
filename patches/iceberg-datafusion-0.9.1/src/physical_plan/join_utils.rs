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
// software distributed under this License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared helpers for Iceberg-aware join physical optimizers.

use std::sync::Arc;

use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::ExecutionPlan;

use super::scan::IcebergTableScan;

/// Default row-count threshold for broadcast / CollectLeft on Iceberg build sides.
pub const SMALL_ICEBERG_BUILD_ROWS: usize = 512 * 1024;

/// Traverse single-input wrappers to the underlying plan node.
pub fn peel_single_input(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    let mut current = plan;
    loop {
        let children = current.children();
        if children.len() != 1 {
            break;
        }
        if current.as_any().is::<RepartitionExec>()
            || current.as_any().is::<CoalescePartitionsExec>()
            || current.as_any().is::<ProjectionExec>()
            || current.name() == "CooperativeExec"
        {
            current = children[0];
        } else {
            break;
        }
    }
    current
}

pub fn as_iceberg_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&IcebergTableScan> {
    peel_single_input(plan)
        .as_any()
        .downcast_ref::<IcebergTableScan>()
}

pub fn iceberg_join_weight(plan: &Arc<dyn ExecutionPlan>) -> Option<usize> {
    as_iceberg_scan(plan).map(|scan| scan.join_order_weight())
}

pub fn is_small_iceberg_build(plan: &Arc<dyn ExecutionPlan>) -> bool {
    as_iceberg_scan(plan)
        .map(|scan| scan.join_order_weight() < SMALL_ICEBERG_BUILD_ROWS)
        .unwrap_or(false)
}

pub fn is_large_unfiltered_fact(plan: &Arc<dyn ExecutionPlan>) -> bool {
    as_iceberg_scan(plan)
        .map(|scan| scan.is_fact_like())
        .unwrap_or(false)
}

pub fn as_hash_join(plan: &Arc<dyn ExecutionPlan>) -> Option<&HashJoinExec> {
    plan.as_any().downcast_ref::<HashJoinExec>()
}

