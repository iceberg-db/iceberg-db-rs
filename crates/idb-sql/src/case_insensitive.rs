//! Case-insensitive schema/table/column lookup for Iceberg catalogs (Snowflake returns uppercase).

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

pub fn wrap_catalog(provider: Arc<dyn CatalogProvider>) -> Arc<dyn CatalogProvider> {
    Arc::new(CaseInsensitiveCatalog { inner: provider })
}

struct CaseInsensitiveCatalog {
    inner: Arc<dyn CatalogProvider>,
}

impl std::fmt::Debug for CaseInsensitiveCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaseInsensitiveCatalog")
            .field("schema_names", &self.inner.schema_names())
            .finish_non_exhaustive()
    }
}

impl CaseInsensitiveCatalog {
    fn resolve_schema_name(&self, name: &str) -> Option<String> {
        if self.inner.schema(name).is_some() {
            return Some(name.to_string());
        }
        self.inner
            .schema_names()
            .into_iter()
            .find(|n| n.eq_ignore_ascii_case(name))
    }
}

impl CatalogProvider for CaseInsensitiveCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let resolved = self.resolve_schema_name(name)?;
        let inner = self.inner.schema(&resolved)?;
        Some(Arc::new(CaseInsensitiveSchema::new(inner)))
    }
}

struct CaseInsensitiveSchema {
    inner: Arc<dyn SchemaProvider>,
    table_names: HashMap<String, String>,
}

impl std::fmt::Debug for CaseInsensitiveSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaseInsensitiveSchema")
            .field("table_names", &self.table_names)
            .finish_non_exhaustive()
    }
}

impl CaseInsensitiveSchema {
    fn new(inner: Arc<dyn SchemaProvider>) -> Self {
        let table_names = inner
            .table_names()
            .into_iter()
            .map(|n| (n.to_ascii_lowercase(), n))
            .collect();
        Self { inner, table_names }
    }

    fn resolve_table_name(&self, name: &str) -> Option<String> {
        if self.inner.table_exist(name) {
            return Some(name.to_string());
        }
        self.table_names.get(&name.to_ascii_lowercase()).cloned()
    }
}

#[async_trait]
impl SchemaProvider for CaseInsensitiveSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.resolve_table_name(name).is_some()
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some(resolved) = self.resolve_table_name(name) else {
            return Ok(None);
        };
        self.inner
            .table(&resolved)
            .await
            .map(|opt| opt.map(wrap_table))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        let Some(resolved) = self.resolve_table_name(name) else {
            return Ok(None);
        };
        self.inner.deregister_table(&resolved)
    }
}

fn wrap_table(table: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    Arc::new(CaseInsensitiveTable::new(table))
}

fn lowercase_schema(schema: &Schema) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Field::new(
                field.name().to_ascii_lowercase(),
                field.data_type().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone())
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new(fields))
}

struct CaseInsensitiveTable {
    inner: Arc<dyn TableProvider>,
    schema: SchemaRef,
}

impl std::fmt::Debug for CaseInsensitiveTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CaseInsensitiveTable")
            .field("columns", &self.schema.fields().len())
            .finish_non_exhaustive()
    }
}

impl CaseInsensitiveTable {
    fn new(inner: Arc<dyn TableProvider>) -> Self {
        let schema = lowercase_schema(inner.schema().as_ref());
        Self { inner, schema }
    }
}

#[async_trait]
impl TableProvider for CaseInsensitiveTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan = self
            .inner
            .scan(state, projection, filters, limit)
            .await?;
        rename_output_columns(plan, &self.schema)
    }
}

fn rename_output_columns(
    plan: Arc<dyn ExecutionPlan>,
    target_schema: &SchemaRef,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let plan_schema = plan.schema();
    if plan_schema.fields().len() != target_schema.fields().len() {
        return Ok(plan);
    }

    let needs_rename = plan_schema
        .fields()
        .iter()
        .zip(target_schema.fields().iter())
        .any(|(src, dst)| src.name() != dst.name());
    if !needs_rename {
        return Ok(plan);
    }

    let exprs = plan_schema
        .fields()
        .iter()
        .zip(target_schema.fields().iter())
        .enumerate()
        .map(|(index, (src, dst))| {
            (
                Arc::new(Column::new(src.name(), index)) as Arc<dyn PhysicalExpr>,
                dst.name().clone(),
            )
        })
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}
