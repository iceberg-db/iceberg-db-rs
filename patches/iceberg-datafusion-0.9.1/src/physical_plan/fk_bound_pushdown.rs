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

//! Infer static FK bounds on fact-table scans from filtered dimension Iceberg scans in join trees.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference};

use super::dim_key_bounds::{compute_join_key_constraint, JoinKeyConstraint};
use super::join_utils::{as_hash_join, as_iceberg_scan};
use super::scan::IcebergTableScan;

#[derive(Default, Debug)]
pub struct IcebergFkBoundPushdown {}

impl IcebergFkBoundPushdown {
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for IcebergFkBoundPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        push_fk_bounds(plan)
    }

    fn name(&self) -> &str {
        "iceberg_fk_bound_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn push_fk_bounds(plan: Arc<dyn ExecutionPlan>) -> DFResult<Arc<dyn ExecutionPlan>> {
    let children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .iter()
        .map(|c| push_fk_bounds(Arc::clone(c)))
        .collect::<DFResult<_>>()?;

    let plan = if children.is_empty() {
        plan
    } else {
        plan.with_new_children(children)?
    };

    let Some(join) = as_hash_join(&plan) else {
        return Ok(plan);
    };

    let left = join.left();
    let right = join.right();
    let on = join.on();

    if let Some((dim, dim_key, fact_key)) = resolve_dim_fact_keys(left, right, on) {
        if let Some(constraint) = compute_join_key_constraint(&dim, &dim_key)? {
            let pred = constraint_to_predicate(&fact_key, constraint);
            return apply_predicate_to_fact_scans(plan, pred, &fact_key);
        }
    }

    Ok(plan)
}

/// Dimension scan on `dim_side`, fact scan nested in `fact_side`.
fn resolve_dim_fact_keys(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
    on: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)],
) -> Option<(IcebergTableScan, String, String)> {
    let (left_key, right_key) = join_column_names(on)?;

    if let Some(dim) = direct_dimension_scan(left) {
        if contains_fact_iceberg_scan(right) {
            return Some((dim.clone(), left_key.to_string(), right_key.to_string()));
        }
    }
    if let Some(dim) = direct_dimension_scan(right) {
        if contains_fact_iceberg_scan(left) {
            return Some((dim.clone(), right_key.to_string(), left_key.to_string()));
        }
    }
    None
}

fn direct_dimension_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&IcebergTableScan> {
    let scan = as_iceberg_scan(plan)?;
    // A filterable dimension has static predicates and is not an unfiltered fact scan.
    // Note: a dimension may itself be large (e.g. `customer_demographics` ~1.9M rows),
    // so we must NOT exclude it by row count here — only the fact *target* matching uses
    // the predicate-independent `is_large_fact_table` check.
    if scan.predicates().is_none() || scan.is_fact_like() {
        return None;
    }
    Some(scan)
}

fn contains_fact_iceberg_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if let Some(scan) = plan.as_any().downcast_ref::<IcebergTableScan>() {
        return scan.is_large_fact_table();
    }
    plan.children().iter().any(|child| contains_fact_iceberg_scan(child))
}

fn join_column_names(on: &[(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)]) -> Option<(&str, &str)> {
    let (l, r) = on.first()?;
    let left = l.as_any().downcast_ref::<Column>()?.name();
    let right = r.as_any().downcast_ref::<Column>()?.name();
    Some((left, right))
}

fn constraint_to_predicate(column: &str, constraint: JoinKeyConstraint) -> Predicate {
    let col = Reference::new(column);
    match constraint {
        JoinKeyConstraint::InList(values) => {
            if values.len() == 1 {
                Predicate::Binary(BinaryExpression::new(
                    PredicateOperator::Eq,
                    col,
                    values[0].clone(),
                ))
            } else {
                col.is_in(values)
            }
        }
        JoinKeyConstraint::Range(min, max) => {
            if min == max {
                Predicate::Binary(BinaryExpression::new(
                    PredicateOperator::Eq,
                    col,
                    min,
                ))
            } else {
                Predicate::and(
                    Predicate::Binary(BinaryExpression::new(
                        PredicateOperator::GreaterThanOrEq,
                        col.clone(),
                        min,
                    )),
                    Predicate::Binary(BinaryExpression::new(
                        PredicateOperator::LessThanOrEq,
                        col,
                        max,
                    )),
                )
            }
        }
    }
}

fn apply_predicate_to_fact_scans(
    plan: Arc<dyn ExecutionPlan>,
    pred: Predicate,
    fact_column: &str,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    // Match scan nodes only — do not peel RepartitionExec wrappers.
    if let Some(scan) = plan.as_any().downcast_ref::<IcebergTableScan>() {
        if scan.is_large_fact_table() && scan_has_column(scan, fact_column) {
            return Ok(Arc::new(scan.with_additional_static_predicate(pred))
                as Arc<dyn ExecutionPlan>);
        }
    }

    if plan.children().is_empty() {
        return Ok(plan);
    }

    let children = plan
        .children()
        .iter()
        .map(|c| apply_predicate_to_fact_scans(Arc::clone(c), pred.clone(), fact_column))
        .collect::<DFResult<_>>()?;
    Ok(plan.with_new_children(children)?)
}

fn scan_has_column(scan: &IcebergTableScan, column: &str) -> bool {
    scan.table_schema()
        .fields()
        .iter()
        .any(|field| field.name() == column)
}
