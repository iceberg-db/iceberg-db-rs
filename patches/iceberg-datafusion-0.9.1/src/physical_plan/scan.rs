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

//! Iceberg table scan execution plan for DataFusion.
//!
//! # Upstream gaps addressed in this fork (see `patches/iceberg-datafusion-0.9.1/PATCH.md`)
//!
//! 1. **Filter pushdown:** implements [`ExecutionPlan::handle_child_pushdown_result`] so
//!    static and dynamic join filters reach Iceberg manifest / Parquet pruning.
//! 2. **Parallel scans:** exposes `scan_partitions` output partitions and reads a disjoint
//!    file subset per partition index in [`ExecutionPlan::execute`].
//! 3. **Join reordering:** snapshot row estimates on the scan node feed the
//!    `IcebergJoinReorder` physical rule (see `join_reorder.rs`) so filtered dimensions
//!    build and unfiltered fact tables probe for dynamic filter pushdown.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::config::ConfigOptions;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr_common::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::filter_pushdown::{
    ChildPushdownResult, FilterPushdownPhase, FilterPushdownPropagation, PushedDown,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PlanProperties};
use datafusion::prelude::Expr;
use futures::{Stream, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::scan::{FileScanTask, FileScanTaskStream};
use iceberg::table::Table;

use super::expr_to_predicate::convert_filters_to_predicate;
use super::file_groups::{DEFAULT_REPARTITION_FILE_MIN_SIZE, partition_file_scan_tasks};
use super::physical_expr_to_predicate::{
    can_pushdown_physical_expr, merge_iceberg_predicates,
};
use super::scan_statistics::row_estimate;
use crate::to_datafusion_error;

/// Weight for scans with static filters pushed into Iceberg (prefer hash-join build side).
const FILTERED_SCAN_JOIN_WEIGHT: usize = 1;

/// Manages the scanning process of an Iceberg [`Table`], encapsulating the
/// necessary details and computed properties required for execution planning.
#[derive(Debug, Clone)]
pub struct IcebergTableScan {
    /// A table in the catalog.
    table: Table,
    /// Snapshot of the table to scan.
    snapshot_id: Option<i64>,
    /// Full table Arrow schema (used for filter column validation).
    table_schema: ArrowSchemaRef,
    /// Output schema after projection.
    output_schema: ArrowSchemaRef,
    /// Stores certain, often expensive to compute,
    /// plan properties used in query optimization.
    plan_properties: PlanProperties,
    /// Projection column names, None means all columns
    projection: Option<Vec<String>>,
    /// Static filters converted from logical [`Expr`] at scan construction.
    predicates: Option<Predicate>,
    /// Physical filters from DataFusion filter pushdown (includes [`DynamicFilterPhysicalExpr`]).
    physical_filters: Vec<Arc<dyn PhysicalExpr>>,
    /// Optional limit on the number of rows to return
    limit: Option<usize>,
    /// Number of parallel scan partitions exposed to DataFusion.
    scan_partitions: usize,
    /// Filled on first `plan_files()` during execute (partition 0); used in EXPLAIN output.
    planned_file_count: Arc<AtomicUsize>,
}

impl IcebergTableScan {
    /// Creates a new [`IcebergTableScan`] object.
    pub(crate) fn new(
        table: Table,
        snapshot_id: Option<i64>,
        schema: ArrowSchemaRef,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
        scan_partitions: usize,
    ) -> Self {
        let output_schema = match projection {
            None => schema.clone(),
            Some(projection) => Arc::new(schema.project(projection).unwrap()),
        };
        let scan_partitions = scan_partitions.max(1);
        let plan_properties =
            Self::compute_properties(output_schema.clone(), scan_partitions);
        let projection = get_column_names(schema.clone(), projection);
        let predicates = convert_filters_to_predicate(filters);

        Self {
            table,
            snapshot_id,
            table_schema: schema,
            output_schema,
            plan_properties,
            projection,
            predicates,
            physical_filters: Vec::new(),
            limit,
            scan_partitions,
            planned_file_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn table(&self) -> &Table {
        &self.table
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    pub fn projection(&self) -> Option<&[String]> {
        self.projection.as_deref()
    }

    pub fn predicates(&self) -> Option<&Predicate> {
        self.predicates.as_ref()
    }

    pub(crate) fn table_schema(&self) -> &ArrowSchemaRef {
        &self.table_schema
    }

    pub fn physical_filters(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.physical_filters
    }

    pub fn scan_partitions(&self) -> usize {
        self.scan_partitions
    }

    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Heuristic weight for [`super::join_reorder::IcebergJoinReorder`]: filtered scans are
    /// treated as small; unfiltered scans use Iceberg snapshot `total-records`.
    pub fn join_order_weight(&self) -> usize {
        if self.predicates.is_some() {
            return FILTERED_SCAN_JOIN_WEIGHT;
        }
        row_estimate(&self.table, self.snapshot_id).unwrap_or(usize::MAX / 4)
    }

    /// Large fact-table scan candidate (no static scan filters, high snapshot row count).
    pub fn is_fact_like(&self) -> bool {
        self.predicates.is_none()
            && self
                .join_order_weight()
                .saturating_mul(4)
                .max(FILTERED_SCAN_JOIN_WEIGHT + 1)
                > super::join_utils::SMALL_ICEBERG_BUILD_ROWS
    }

    /// True when the underlying snapshot row count is large, **regardless** of any
    /// static predicates already pushed onto this scan. Used by FK-bound pushdown so
    /// that multiple dimension constraints stack onto the same fact scan (each pushed
    /// predicate would otherwise make [`Self::is_fact_like`] return false).
    pub fn is_large_fact_table(&self) -> bool {
        row_estimate(&self.table, self.snapshot_id)
            .map(|rows| rows > super::join_utils::SMALL_ICEBERG_BUILD_ROWS)
            .unwrap_or(false)
    }

    /// AND-merge an additional static Iceberg predicate (plan-time FK bound inference).
    pub(crate) fn with_additional_static_predicate(&self, pred: Predicate) -> Self {
        let mut updated = self.clone();
        updated.predicates = Some(match updated.predicates.take() {
            Some(existing) => existing.and(pred),
            None => pred,
        });
        updated
    }

    /// Attach additional physical filters (deduped by pointer identity).
    pub(crate) fn with_extra_physical_filters(
        &self,
        extra: &[Arc<dyn PhysicalExpr>],
    ) -> Self {
        if extra.is_empty() {
            return self.clone();
        }
        let mut updated = self.clone();
        for filter in extra {
            if updated
                .physical_filters
                .iter()
                .any(|existing| Arc::ptr_eq(existing, filter))
            {
                continue;
            }
            updated.physical_filters.push(Arc::clone(filter));
        }
        updated
    }

    fn with_scan_partitions(&self, scan_partitions: usize) -> Self {
        let scan_partitions = scan_partitions.max(1);
        Self {
            plan_properties: Self::compute_properties(
                self.output_schema.clone(),
                scan_partitions,
            ),
            scan_partitions,
            ..self.clone()
        }
    }

    /// Computes [`PlanProperties`] used in query optimization.
    fn compute_properties(schema: ArrowSchemaRef, scan_partitions: usize) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(scan_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    fn format_predicate_summary(&self) -> String {
        let static_part = self
            .predicates
            .clone()
            .map(|p| format!("{p}"))
            .unwrap_or_default();
        if self.physical_filters.is_empty() {
            return static_part;
        }
        if static_part.is_empty() {
            return format!("+{} physical filter(s)", self.physical_filters.len());
        }
        format!("{static_part} +{} physical filter(s)", self.physical_filters.len())
    }

}

impl ExecutionPlan for IcebergTableScan {
    fn name(&self) -> &str {
        "IcebergTableScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if target_partitions <= 1 || self.scan_partitions >= target_partitions {
            return Ok(None);
        }
        let min_size = config.optimizer.repartition_file_min_size;
        // Respect DataFusion's repartition_file_scans toggle: when disabled, keep single partition.
        if !config.optimizer.repartition_file_scans {
            return Ok(None);
        }
        let _ = min_size; // used at execute time via session config
        Ok(Some(Arc::new(self.with_scan_partitions(target_partitions))))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> DFResult<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        if child_pushdown_result.parent_filters.is_empty() {
            return Ok(FilterPushdownPropagation::if_all(child_pushdown_result));
        }

        let parent_filters: Vec<Arc<dyn PhysicalExpr>> = child_pushdown_result
            .parent_filters
            .into_iter()
            .map(|f| f.filter)
            .collect();

        let schema = self.table_schema.clone();
        let mut pushdown_results = Vec::with_capacity(parent_filters.len());
        let mut accepted = Vec::new();

        for filter in parent_filters {
            if can_pushdown_physical_expr(&filter, &schema) {
                pushdown_results.push(PushedDown::Yes);
                accepted.push(filter);
            } else {
                pushdown_results.push(PushedDown::No);
            }
        }

        if accepted.is_empty() {
            return Ok(FilterPushdownPropagation::with_parent_pushdown_result(
                pushdown_results,
            ));
        }

        let mut updated = self.clone();
        updated.physical_filters.extend(accepted);

        Ok(
            FilterPushdownPropagation::with_parent_pushdown_result(pushdown_results)
                .with_updated_node(Arc::new(updated) as Arc<dyn ExecutionPlan>),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.scan_partitions {
            return Err(datafusion::common::DataFusionError::Internal(format!(
                "IcebergTableScan partition {partition} out of range (scan_partitions={})",
                self.scan_partitions
            )));
        }

        let table = self.table.clone();
        let snapshot_id = self.snapshot_id;
        let projection = self.projection.clone();
        let static_pred = self.predicates.clone();
        let physical_filters = self.physical_filters.clone();
        let table_schema = self.table_schema.clone();
        let limit = self.limit;
        let scan_partitions = self.scan_partitions;
        let min_file_size = context
            .session_config()
            .options()
            .optimizer
            .repartition_file_min_size;
        let planned_file_count = Arc::clone(&self.planned_file_count);

        let fut = async move {
            let merged = merge_iceberg_predicates(
                static_pred,
                &physical_filters,
                &table_schema,
            )?;
            read_partition(
                table,
                snapshot_id,
                projection,
                merged,
                partition,
                scan_partitions,
                min_file_size,
                planned_file_count,
            )
            .await
        };

        let stream = futures::stream::once(fut).try_flatten();

        let limited_stream: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            if let Some(limit) = limit {
                let mut remaining = limit;
                Box::pin(stream.try_filter_map(move |batch| {
                    futures::future::ready(if remaining == 0 {
                        Ok(None)
                    } else if batch.num_rows() <= remaining {
                        remaining -= batch.num_rows();
                        Ok(Some(batch))
                    } else {
                        let limited_batch = batch.slice(0, remaining);
                        remaining = 0;
                        Ok(Some(limited_batch))
                    })
                }))
            } else {
                Box::pin(stream)
            };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            limited_stream,
        )))
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let planned = self.planned_file_count.load(Ordering::Relaxed);
        let planned_suffix = if planned > 0 {
            format!(" planned_files={planned}")
        } else {
            String::new()
        };
        write!(
            f,
            "IcebergTableScan partitions={}{planned_suffix} projection:[{}] predicate:[{}]",
            self.scan_partitions,
            self.projection
                .clone()
                .map_or(String::new(), |v| v.join(",")),
            self.format_predicate_summary()
        )
    }
}

async fn read_partition(
    table: Table,
    snapshot_id: Option<i64>,
    column_names: Option<Vec<String>>,
    predicates: Option<Predicate>,
    partition: usize,
    scan_partitions: usize,
    min_file_size: usize,
    planned_file_count: Arc<AtomicUsize>,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let scan_builder = match snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    };

    let has_predicate = predicates.is_some();
    let mut scan_builder = match column_names {
        Some(column_names) => scan_builder.select(column_names),
        None => scan_builder.select_all(),
    };
    if let Some(pred) = predicates {
        scan_builder = scan_builder.with_filter(pred);
    }
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;

    let tasks: Vec<FileScanTask> = table_scan
        .plan_files()
        .await
        .map_err(to_datafusion_error)?
        .try_collect()
        .await
        .map_err(to_datafusion_error)?;

    if partition == 0 {
        let _ = planned_file_count.compare_exchange(
            0,
            tasks.len(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    let min_file_size = if min_file_size == 0 {
        DEFAULT_REPARTITION_FILE_MIN_SIZE
    } else {
        min_file_size
    };
    let groups = partition_file_scan_tasks(tasks, scan_partitions, min_file_size);
    let partition_tasks = groups.into_iter().nth(partition).unwrap_or_default();

    if partition_tasks.is_empty() {
        return Ok(Box::pin(futures::stream::empty()));
    }

    let task_stream: FileScanTaskStream =
        Box::pin(futures::stream::iter(partition_tasks.into_iter().map(Ok)));

    let mut arrow_reader = ArrowReaderBuilder::new(table.file_io().clone());
    if has_predicate {
        // Page-index row selection when metadata supports it (see iceberg reader fallback).
        arrow_reader = arrow_reader.with_row_selection_enabled(true);
    }
    let stream = arrow_reader
        .build()
        .read(task_stream)
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);

    Ok(Box::pin(stream))
}

fn get_column_names(
    schema: ArrowSchemaRef,
    projection: Option<&Vec<usize>>,
) -> Option<Vec<String>> {
    projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    })
}
