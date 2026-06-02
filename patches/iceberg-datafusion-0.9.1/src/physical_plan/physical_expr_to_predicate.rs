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

//! Convert DataFusion **physical** filter expressions into Iceberg [`Predicate`]s.
//!
//! DataFusion 52 pushes filters in two phases ([`FilterPushdownPhase`]):
//!
//! - **Pre:** static predicates from `FilterExec` (logical → physical at plan time).
//! - **Post:** dynamic predicates from `HashJoinExec` / `SortExec` top-K, represented as
//!   [`DynamicFilterPhysicalExpr`] and updated when the build side finishes.
//!
//! Upstream iceberg-datafusion only converted logical [`Expr`] filters at scan construction.
//! This module closes the gap so `IcebergTableScan::handle_child_pushdown_result` can feed
//! join/runtime filters into Iceberg manifest + row-group pruning.
//!
//! [`FilterPushdownPhase`]: datafusion::physical_plan::filter_pushdown::FilterPushdownPhase
//! [`DynamicFilterPhysicalExpr`]: datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, Column, DynamicFilterPhysicalExpr, InListExpr, IsNotNullExpr, IsNullExpr,
    Literal, NotExpr,
};
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::HashTableLookupExpr;
use iceberg::expr::{BinaryExpression, Predicate, PredicateOperator, Reference, UnaryExpression};
use iceberg::spec::Datum;

use super::expr_to_predicate::scalar_value_to_datum;

enum PhysicalTransformed {
    Predicate(Predicate),
    Column(Reference),
    Literal(Datum),
    NotTransformed,
}

enum PhysicalOp {
    Compare(PredicateOperator),
    And,
    Or,
    NotTransformed,
}

/// Resolve dynamic filters to their current expression (non-blocking).
fn resolve_physical_expr(
    expr: &Arc<dyn PhysicalExpr>,
) -> datafusion::error::Result<Arc<dyn PhysicalExpr>> {
    if let Some(dynamic) = expr.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() {
        return dynamic.current();
    }
    Ok(Arc::clone(expr))
}

/// Resolve dynamic filters, then convert a physical expression to zero or more Iceberg [`Predicate`]s.
pub fn convert_physical_expr_to_predicates(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
) -> datafusion::error::Result<Vec<Predicate>> {
    let resolved = resolve_physical_expr(expr)?;
    Ok(extract_pushdown_predicates(&resolved, schema))
}

/// Returns true when every column referenced by `expr` exists in the scan schema.
pub fn can_pushdown_physical_expr(expr: &Arc<dyn PhysicalExpr>, schema: &SchemaRef) -> bool {
    use datafusion::physical_expr::utils::collect_columns;

    collect_columns(expr)
        .iter()
        .all(|col| schema.fields().iter().any(|f| f.name() == col.name()))
}

/// Merge static and physical Iceberg predicates with logical AND.
pub fn merge_iceberg_predicates(
    static_pred: Option<Predicate>,
    physical_filters: &[Arc<dyn PhysicalExpr>],
    schema: &SchemaRef,
) -> datafusion::error::Result<Option<Predicate>> {
    let mut merged = static_pred;
    for filter in physical_filters {
        let parts = convert_physical_expr_to_predicates(filter, schema)?;
        for pred in parts {
            merged = Some(match merged {
                Some(existing) => existing.and(pred),
                None => pred,
            });
        }
    }
    Ok(merged)
}

/// Extract all manifest-prunable predicates from a (possibly AND-composed) physical filter.
fn extract_pushdown_predicates(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
) -> Vec<Predicate> {
    if expr.as_any().downcast_ref::<HashTableLookupExpr>().is_some() {
        return vec![];
    }
    if is_literal_true(expr) {
        return vec![];
    }
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if binary.op() == &Operator::And {
            let mut out = extract_pushdown_predicates(binary.left(), schema);
            out.extend(extract_pushdown_predicates(binary.right(), schema));
            return out;
        }
    }
    match to_iceberg_from_physical(expr) {
        PhysicalTransformed::Predicate(p) => vec![p],
        PhysicalTransformed::Column(r) => vec![Predicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            r,
            Datum::bool(true),
        ))],
        _ => vec![],
    }
}

fn is_literal_true(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.as_any()
        .downcast_ref::<Literal>()
        .is_some_and(|lit| matches!(lit.value(), ScalarValue::Boolean(Some(true))))
}

fn to_iceberg_from_physical(expr: &Arc<dyn PhysicalExpr>) -> PhysicalTransformed {
    if expr.as_any().downcast_ref::<HashTableLookupExpr>().is_some() {
        return PhysicalTransformed::NotTransformed;
    }
    if is_literal_true(expr) {
        return PhysicalTransformed::NotTransformed;
    }
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let left = to_iceberg_from_physical(binary.left());
        let right = to_iceberg_from_physical(binary.right());
        return match map_operator(binary.op()) {
            PhysicalOp::Compare(op) => to_binary(left, right, op),
            PhysicalOp::And => to_and(left, right),
            PhysicalOp::Or => to_or(left, right),
            PhysicalOp::NotTransformed => PhysicalTransformed::NotTransformed,
        };
    }
    if let Some(not) = expr.as_any().downcast_ref::<NotExpr>() {
        return match to_iceberg_from_physical(not.arg()) {
            PhysicalTransformed::Predicate(p) => PhysicalTransformed::Predicate(!p),
            PhysicalTransformed::Column(r) => PhysicalTransformed::Predicate(Predicate::Binary(
                BinaryExpression::new(PredicateOperator::Eq, r, Datum::bool(false)),
            )),
            _ => PhysicalTransformed::NotTransformed,
        };
    }
    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
        return PhysicalTransformed::Column(Reference::new(col.name()));
    }
    if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
        return match scalar_value_to_datum(lit.value()) {
            Some(d) => PhysicalTransformed::Literal(d),
            None => PhysicalTransformed::NotTransformed,
        };
    }
    if let Some(is_null) = expr.as_any().downcast_ref::<IsNullExpr>() {
        if let PhysicalTransformed::Column(r) = to_iceberg_from_physical(is_null.arg()) {
            return PhysicalTransformed::Predicate(Predicate::Unary(UnaryExpression::new(
                PredicateOperator::IsNull,
                r,
            )));
        }
    }
    if let Some(is_not_null) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        if let PhysicalTransformed::Column(r) = to_iceberg_from_physical(is_not_null.arg()) {
            return PhysicalTransformed::Predicate(Predicate::Unary(UnaryExpression::new(
                PredicateOperator::NotNull,
                r,
            )));
        }
    }
    if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
        if let Some(pred) = in_list_to_predicate(in_list) {
            return PhysicalTransformed::Predicate(pred);
        }
    }

    PhysicalTransformed::NotTransformed
}

fn in_list_to_predicate(in_list: &InListExpr) -> Option<Predicate> {
    let mut datums = Vec::new();
    for item in in_list.list() {
        if let Some(lit) = item.as_any().downcast_ref::<Literal>() {
            if let Some(d) = scalar_value_to_datum(lit.value()) {
                datums.push(d);
                continue;
            }
        }
        return None;
    }
    if datums.is_empty() {
        return None;
    }
    if let PhysicalTransformed::Column(r) = to_iceberg_from_physical(in_list.expr()) {
        let pred = if in_list.negated() {
            r.is_not_in(datums)
        } else {
            r.is_in(datums)
        };
        return Some(pred);
    }
    None
}

fn map_operator(op: &Operator) -> PhysicalOp {
    match op {
        Operator::Eq => PhysicalOp::Compare(PredicateOperator::Eq),
        Operator::NotEq => PhysicalOp::Compare(PredicateOperator::NotEq),
        Operator::Lt => PhysicalOp::Compare(PredicateOperator::LessThan),
        Operator::LtEq => PhysicalOp::Compare(PredicateOperator::LessThanOrEq),
        Operator::Gt => PhysicalOp::Compare(PredicateOperator::GreaterThan),
        Operator::GtEq => PhysicalOp::Compare(PredicateOperator::GreaterThanOrEq),
        Operator::And => PhysicalOp::And,
        Operator::Or => PhysicalOp::Or,
        _ => PhysicalOp::NotTransformed,
    }
}

fn to_binary(left: PhysicalTransformed, right: PhysicalTransformed, op: PredicateOperator) -> PhysicalTransformed {
    let (r, d, op) = match (left, right) {
        (PhysicalTransformed::NotTransformed, _) | (_, PhysicalTransformed::NotTransformed) => {
            return PhysicalTransformed::NotTransformed;
        }
        (PhysicalTransformed::Column(r), PhysicalTransformed::Literal(d)) => (r, d, op),
        (PhysicalTransformed::Literal(d), PhysicalTransformed::Column(r)) => (r, d, reverse_op(op)),
        _ => return PhysicalTransformed::NotTransformed,
    };
    PhysicalTransformed::Predicate(Predicate::Binary(BinaryExpression::new(op, r, d)))
}

fn to_and(left: PhysicalTransformed, right: PhysicalTransformed) -> PhysicalTransformed {
    match (left, right) {
        (PhysicalTransformed::Predicate(l), PhysicalTransformed::Predicate(r)) => {
            PhysicalTransformed::Predicate(l.and(r))
        }
        (PhysicalTransformed::Predicate(l), _) => PhysicalTransformed::Predicate(l),
        (_, PhysicalTransformed::Predicate(r)) => PhysicalTransformed::Predicate(r),
        _ => PhysicalTransformed::NotTransformed,
    }
}

fn to_or(left: PhysicalTransformed, right: PhysicalTransformed) -> PhysicalTransformed {
    match (left, right) {
        (PhysicalTransformed::Predicate(l), PhysicalTransformed::Predicate(r)) => {
            PhysicalTransformed::Predicate(l.or(r))
        }
        _ => PhysicalTransformed::NotTransformed,
    }
}

fn reverse_op(op: PredicateOperator) -> PredicateOperator {
    match op {
        PredicateOperator::Eq => PredicateOperator::Eq,
        PredicateOperator::NotEq => PredicateOperator::NotEq,
        PredicateOperator::GreaterThan => PredicateOperator::LessThan,
        PredicateOperator::GreaterThanOrEq => PredicateOperator::LessThanOrEq,
        PredicateOperator::LessThan => PredicateOperator::GreaterThan,
        PredicateOperator::LessThanOrEq => PredicateOperator::GreaterThanOrEq,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{col, lit};

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("ss_cdemo_sk", DataType::Int64, true),
            Field::new("ss_sold_date_sk", DataType::Int64, true),
            Field::new("d_year", DataType::Int32, true),
        ]))
    }

    #[test]
    fn converts_physical_equality() {
        let schema = test_schema();
        let expr = Arc::new(BinaryExpr::new(
            col("ss_cdemo_sk", schema.as_ref()).expect("column"),
            Operator::Eq,
            lit(42i64),
        )) as _;
        let preds = convert_physical_expr_to_predicates(&expr, &schema).unwrap();
        assert_eq!(preds.len(), 1);
        assert!(preds[0].to_string().contains("ss_cdemo_sk"));
    }

    #[test]
    fn extracts_bounds_from_and_filter() {
        let schema = test_schema();
        let col = col("ss_sold_date_sk", schema.as_ref()).expect("column");
        let range = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::clone(&col),
                Operator::GtEq,
                lit(100_i64),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(col, Operator::LtEq, lit(200_i64))),
        )) as Arc<dyn PhysicalExpr>;
        let preds = extract_pushdown_predicates(&range, &schema);
        assert_eq!(preds.len(), 2);
    }

    #[test]
    fn converts_physical_in_list() {
        let schema = test_schema();
        let expr = datafusion::physical_expr::expressions::in_list(
            col("ss_cdemo_sk", schema.as_ref()).expect("column"),
            vec![lit(1i64), lit(2i64)],
            &false,
            schema.as_ref(),
        )
        .expect("in_list expr");
        let preds = convert_physical_expr_to_predicates(&expr, &schema).unwrap();
        assert_eq!(preds.len(), 1);
        assert!(preds[0].to_string().contains("ss_cdemo_sk"));
    }

    #[test]
    fn skips_literal_true() {
        let schema = test_schema();
        let expr = lit(true) as Arc<dyn PhysicalExpr>;
        assert!(convert_physical_expr_to_predicates(&expr, &schema)
            .unwrap()
            .is_empty());
    }
}
