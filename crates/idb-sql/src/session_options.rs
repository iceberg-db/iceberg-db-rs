//! DataFusion session tuning for native queries.

/// Options applied when opening a [`SqlSession`](crate::SqlSession).
#[derive(Debug, Clone, Default)]
pub struct SessionOptions {
    /// Override DataFusion `target_partitions` (defaults to logical CPU count when unset).
    pub target_partitions: Option<usize>,
    /// Override `optimizer.repartition_file_scans` (DataFusion default: true).
    pub repartition_file_scans: Option<bool>,
    /// Override `optimizer.enable_join_dynamic_filter_pushdown` (default: true).
    pub enable_join_dynamic_filter_pushdown: Option<bool>,
    /// Override `optimizer.repartition_joins` (default: true).
    pub repartition_joins: Option<bool>,
}

impl SessionOptions {
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        self.target_partitions = Some(n);
        self
    }
}

pub(crate) fn apply_to_config(
    mut config: datafusion::prelude::SessionConfig,
    options: &SessionOptions,
) -> datafusion::prelude::SessionConfig {
    if let Some(n) = options.target_partitions {
        config.options_mut().execution.target_partitions = n;
    }
    let optimizer = &mut config.options_mut().optimizer;
    if let Some(v) = options.repartition_file_scans {
        optimizer.repartition_file_scans = v;
    }
    if let Some(v) = options.enable_join_dynamic_filter_pushdown {
        optimizer.enable_join_dynamic_filter_pushdown = v;
    }
    if let Some(v) = options.repartition_joins {
        optimizer.repartition_joins = v;
    }
    config
}
