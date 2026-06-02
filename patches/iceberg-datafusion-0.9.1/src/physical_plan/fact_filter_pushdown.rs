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

//! Attach hash-join dynamic filters from ancestor joins onto fact-table Iceberg scans.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult};
use datafusion::error::Result as DFResult;
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

use super::join_utils::as_hash_join;

#[derive(Default, Debug)]
pub struct IcebergFactFilterPushdown {}

impl IcebergFactFilterPushdown {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for IcebergFactFilterPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        push_filters_to_fact_scans(plan, &[]).data()
    }

    fn name(&self) -> &str {
        "iceberg_fact_filter_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn push_filters_to_fact_scans(
    plan: Arc<dyn ExecutionPlan>,
    inherited: &[Arc<dyn PhysicalExpr>],
) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
    // Match scan nodes only — do not peel RepartitionExec/Projection wrappers or the
    // RepartitionExec above a probe-side scan is dropped from the plan.
    if let Some(scan) = plan.as_any().downcast_ref::<super::scan::IcebergTableScan>()
        && scan.is_fact_like()
    {
        let updated = scan.with_extra_physical_filters(inherited);
        if updated.physical_filters().len() == scan.physical_filters().len() {
            return Ok(Transformed::no(plan));
        }
        return Ok(Transformed::yes(
            Arc::new(updated) as Arc<dyn ExecutionPlan>
        ));
    }

    if let Some(join) = as_hash_join(&plan) {
        let mut probe_filters = inherited.to_vec();
        if let Some(dynamic) = join.dynamic_filter_for_test() {
            probe_filters.push(dynamic);
        }
        let left = push_filters_to_fact_scans(Arc::clone(join.left()), inherited)?;
        let right = push_filters_to_fact_scans(Arc::clone(join.right()), &probe_filters)?;
        if left.transformed || right.transformed {
            let updated = plan.with_new_children(vec![left.data, right.data])?;
            return Ok(Transformed::yes(updated));
        }
        return Ok(Transformed::no(plan));
    }

    if plan.children().len() == 1 {
        let child = plan.children()[0];
        let updated_child = push_filters_to_fact_scans(Arc::clone(child), inherited)?;
        if updated_child.transformed {
            let updated = plan.with_new_children(vec![updated_child.data])?;
            return Ok(Transformed::yes(updated));
        }
    }

    Ok(Transformed::no(plan))
}