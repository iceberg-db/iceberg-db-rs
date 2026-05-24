//! In-memory demo catalog for WASM and tests (no local filesystem warehouse).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use iceberg::io::MemoryStorageFactory;
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::table::Table;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

const WAREHOUSE: &str = "idb://wasm-warehouse";

/// Opens a memory-backed catalog with `demo.customers` (3 rows), matching the Java seeder shape.
pub async fn open_memory_demo_catalog() -> Result<Arc<dyn Catalog>> {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "local",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), WAREHOUSE.to_string())]),
        )
        .await
        .context("open memory catalog")?;
    let catalog: Arc<dyn Catalog> = Arc::new(catalog);

    let namespace = NamespaceIdent::new("demo".into());
    if !catalog.namespace_exists(&namespace).await? {
        catalog.create_namespace(&namespace, HashMap::new()).await?;
    }

    let customers_id = TableIdent::new(namespace.clone(), "customers".into());
    if catalog.table_exists(&customers_id).await? {
        catalog.drop_table(&customers_id).await?;
    }

    let table = catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("customers".into())
                .location(format!("{WAREHOUSE}/demo/customers"))
                .schema(customers_schema())
                .build(),
        )
        .await?;

    let data_files = write_customers_parquet(&table).await?;
    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx).context("schedule fast append")?;
    tx.commit(catalog.as_ref()).await.context("commit demo data")?;

    Ok(catalog)
}

fn customers_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(3, "region", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("customers schema")
}

async fn write_customers_parquet(table: &Table) -> Result<Vec<iceberg::spec::DataFile>> {
    let schema = table.metadata().current_schema();
    let data_location = format!("{}/data", table.metadata().location());
    let location_gen = DefaultLocationGenerator::with_data_location(data_location);
    let file_name_gen = DefaultFileNameGenerator::new(
        "customers".to_string(),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );

    let parquet_props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let parquet_writer = ParquetWriterBuilder::new(parquet_props, schema.clone());
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_writer,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );

    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .context("data file writer")?;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "1".to_string(),
        )])),
        Field::new("name", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "2".to_string(),
        )])),
        Field::new("region", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            "3".to_string(),
        )])),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Carlos"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["US", "US", "EU"])) as ArrayRef,
        ],
    )
    .context("record batch")?;

    writer.write(batch).await.context("write parquet")?;
    writer.close().await.context("close parquet writer")
}
