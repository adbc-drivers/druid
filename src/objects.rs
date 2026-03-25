//! Builder for constructing the `get_objects` response `RecordBatch`.
//!
//! The ADBC `get_objects` schema is hierarchical:
//! - Catalog → Schemas → Tables → Columns/Constraints

use adbc_core::error::{Error, Result, Status};
use adbc_core::options::ObjectDepth;
use adbc_core::schemas::{
    COLUMN_SCHEMA, CONSTRAINT_SCHEMA, GET_OBJECTS_SCHEMA, OBJECTS_DB_SCHEMA_SCHEMA, TABLE_SCHEMA,
};
use arrow_array::builder::{
    ArrayBuilder, BooleanBuilder, Int16Builder, Int32Builder, StringBuilder,
};
use arrow_array::{ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};
use std::collections::HashMap;
use std::sync::Arc;

/// Column metadata for XDBC fields.
#[derive(Debug, Clone, Default)]
pub struct ColumnInfo {
    pub name: String,
    pub ordinal_position: Option<i32>,
    pub remarks: Option<String>,
    pub xdbc_data_type: Option<i16>,
    pub xdbc_type_name: Option<String>,
    pub xdbc_column_size: Option<i32>,
    pub xdbc_decimal_digits: Option<i16>,
    pub xdbc_num_prec_radix: Option<i16>,
    pub xdbc_nullable: Option<i16>,
    pub xdbc_column_def: Option<String>,
    pub xdbc_sql_data_type: Option<i16>,
    pub xdbc_datetime_sub: Option<i16>,
    pub xdbc_char_octet_length: Option<i32>,
    pub xdbc_is_nullable: Option<String>,
    pub xdbc_scope_catalog: Option<String>,
    pub xdbc_scope_schema: Option<String>,
    pub xdbc_scope_table: Option<String>,
    pub xdbc_is_autoincrement: Option<bool>,
    pub xdbc_is_generatedcolumn: Option<bool>,
}

/// Table metadata.
#[derive(Debug, Clone)]
struct TableInfo {
    name: String,
    table_type: String,
    columns: Vec<ColumnInfo>,
}

/// Schema metadata.
#[derive(Debug, Clone)]
struct SchemaInfo {
    name: Option<String>,
    tables: Vec<TableInfo>,
}

/// Catalog metadata.
#[derive(Debug, Clone)]
struct CatalogInfo {
    name: Option<String>,
    schemas: Vec<SchemaInfo>,
}

/// Builder for constructing the `get_objects` response `RecordBatch`.
pub struct GetObjectsBuilder {
    catalogs: Vec<CatalogInfo>,
    /// Maps catalog name to index in catalogs vec
    catalog_index: HashMap<String, usize>,
}

impl GetObjectsBuilder {
    /// Creates a new `GetObjectsBuilder`.
    pub fn new() -> Self {
        Self {
            catalogs: Vec::new(),
            catalog_index: HashMap::new(),
        }
    }

    /// Adds a catalog entry.
    pub fn add_catalog(&mut self, name: impl Into<String>) {
        let name = name.into();
        if !self.catalog_index.contains_key(&name) {
            self.catalog_index.insert(name.clone(), self.catalogs.len());
            self.catalogs.push(CatalogInfo {
                name: Some(name),
                schemas: Vec::new(),
            });
        }
    }

    /// Adds a schema to a catalog.
    pub fn add_schema(&mut self, catalog: impl Into<String>, schema: impl Into<String>) {
        let catalog = catalog.into();
        let schema = schema.into();

        self.add_catalog(catalog.clone());
        let catalog_idx = self.catalog_index[&catalog];
        let catalog_info = &mut self.catalogs[catalog_idx];

        // Check if schema already exists
        if !catalog_info
            .schemas
            .iter()
            .any(|s| s.name.as_deref() == Some(&schema))
        {
            catalog_info.schemas.push(SchemaInfo {
                name: Some(schema),
                tables: Vec::new(),
            });
        }
    }

    /// Adds a table to a schema.
    pub fn add_table(
        &mut self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table_name: impl Into<String>,
        table_type: impl Into<String>,
    ) {
        let catalog = catalog.into();
        let schema = schema.into();
        let table_name = table_name.into();
        let table_type = table_type.into();

        self.add_schema(catalog.clone(), schema.clone());
        let catalog_idx = self.catalog_index[&catalog];
        let schema_info = self.catalogs[catalog_idx]
            .schemas
            .iter_mut()
            .find(|s| s.name.as_deref() == Some(&schema))
            .unwrap();

        // Check if table already exists
        if !schema_info.tables.iter().any(|t| t.name == table_name) {
            schema_info.tables.push(TableInfo {
                name: table_name,
                table_type,
                columns: Vec::new(),
            });
        }
    }

    /// Adds a column to a table.
    pub fn add_column(
        &mut self,
        catalog: impl Into<String>,
        schema: impl Into<String>,
        table_name: impl Into<String>,
        column: ColumnInfo,
    ) {
        let catalog = catalog.into();
        let schema = schema.into();
        let table_name = table_name.into();

        // Ensure table exists (with empty type - caller should have added it)
        let catalog_idx = self.catalog_index.get(&catalog);
        if catalog_idx.is_none() {
            return;
        }
        let catalog_idx = *catalog_idx.unwrap();

        let schema_info = self.catalogs[catalog_idx]
            .schemas
            .iter_mut()
            .find(|s| s.name.as_deref() == Some(&schema));
        if schema_info.is_none() {
            return;
        }

        let table_info = schema_info
            .unwrap()
            .tables
            .iter_mut()
            .find(|t| t.name == table_name);
        if let Some(table) = table_info {
            table.columns.push(column);
        }
    }

    /// Builds the final `RecordBatch` with the ADBC `get_objects` schema.
    #[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
    pub fn finish(self, depth: ObjectDepth) -> Result<RecordBatch> {
        let include_schemas = !matches!(depth, ObjectDepth::Catalogs);
        let include_tables = matches!(
            depth,
            ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All
        );
        let include_columns = matches!(depth, ObjectDepth::Columns | ObjectDepth::All);

        // Build catalog_name array
        let mut catalog_names = StringBuilder::new();
        let mut catalog_nulls = Vec::new();
        let mut schema_offsets: Vec<i32> = vec![0];

        // Flat arrays for schemas
        let mut schema_names = StringBuilder::new();
        let mut schema_nulls = Vec::new();
        let mut table_offsets: Vec<i32> = vec![0];

        // Flat arrays for tables
        let mut table_names = StringBuilder::new();
        let mut table_types = StringBuilder::new();
        let mut column_offsets: Vec<i32> = vec![0];
        let mut constraint_offsets: Vec<i32> = vec![0];

        // Flat arrays for columns (19 fields)
        let mut col_names = StringBuilder::new();
        let mut col_ordinals = Int32Builder::new();
        let mut col_remarks = StringBuilder::new();
        let mut col_xdbc_data_type = Int16Builder::new();
        let mut col_xdbc_type_name = StringBuilder::new();
        let mut col_xdbc_column_size = Int32Builder::new();
        let mut col_xdbc_decimal_digits = Int16Builder::new();
        let mut col_xdbc_num_prec_radix = Int16Builder::new();
        let mut col_xdbc_nullable = Int16Builder::new();
        let mut col_xdbc_column_def = StringBuilder::new();
        let mut col_xdbc_sql_data_type = Int16Builder::new();
        let mut col_xdbc_datetime_sub = Int16Builder::new();
        let mut col_xdbc_char_octet_length = Int32Builder::new();
        let mut col_xdbc_is_nullable = StringBuilder::new();
        let mut col_xdbc_scope_catalog = StringBuilder::new();
        let mut col_xdbc_scope_schema = StringBuilder::new();
        let mut col_xdbc_scope_table = StringBuilder::new();
        let mut col_xdbc_is_autoincrement = BooleanBuilder::new();
        let mut col_xdbc_is_generatedcolumn = BooleanBuilder::new();

        for catalog in &self.catalogs {
            match &catalog.name {
                Some(name) => catalog_names.append_value(name),
                None => catalog_names.append_null(),
            }

            if include_schemas {
                catalog_nulls.push(true);
                for schema in &catalog.schemas {
                    match &schema.name {
                        Some(name) => schema_names.append_value(name),
                        None => schema_names.append_null(),
                    }

                    if include_tables {
                        schema_nulls.push(true);
                        for table in &schema.tables {
                            table_names.append_value(&table.name);
                            table_types.append_value(&table.table_type);

                            if include_columns {
                                for col in &table.columns {
                                    col_names.append_value(&col.name);
                                    col_ordinals.append_option(col.ordinal_position);
                                    append_option_str(&mut col_remarks, col.remarks.as_deref());
                                    col_xdbc_data_type.append_option(col.xdbc_data_type);
                                    append_option_str(
                                        &mut col_xdbc_type_name,
                                        col.xdbc_type_name.as_deref(),
                                    );
                                    col_xdbc_column_size.append_option(col.xdbc_column_size);
                                    col_xdbc_decimal_digits.append_option(col.xdbc_decimal_digits);
                                    col_xdbc_num_prec_radix.append_option(col.xdbc_num_prec_radix);
                                    col_xdbc_nullable.append_option(col.xdbc_nullable);
                                    append_option_str(
                                        &mut col_xdbc_column_def,
                                        col.xdbc_column_def.as_deref(),
                                    );
                                    col_xdbc_sql_data_type.append_option(col.xdbc_sql_data_type);
                                    col_xdbc_datetime_sub.append_option(col.xdbc_datetime_sub);
                                    col_xdbc_char_octet_length
                                        .append_option(col.xdbc_char_octet_length);
                                    append_option_str(
                                        &mut col_xdbc_is_nullable,
                                        col.xdbc_is_nullable.as_deref(),
                                    );
                                    append_option_str(
                                        &mut col_xdbc_scope_catalog,
                                        col.xdbc_scope_catalog.as_deref(),
                                    );
                                    append_option_str(
                                        &mut col_xdbc_scope_schema,
                                        col.xdbc_scope_schema.as_deref(),
                                    );
                                    append_option_str(
                                        &mut col_xdbc_scope_table,
                                        col.xdbc_scope_table.as_deref(),
                                    );
                                    col_xdbc_is_autoincrement
                                        .append_option(col.xdbc_is_autoincrement);
                                    col_xdbc_is_generatedcolumn
                                        .append_option(col.xdbc_is_generatedcolumn);
                                }
                                #[allow(
                                    clippy::cast_possible_truncation,
                                    clippy::cast_possible_wrap
                                )]
                                column_offsets.push(col_names.len() as i32);
                                // Empty constraints list (Druid has no constraints)
                                constraint_offsets.push(*constraint_offsets.last().unwrap());
                            }
                        }
                        #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                        table_offsets.push(table_names.len() as i32);
                    } else {
                        schema_nulls.push(false);
                    }
                }
                #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
                schema_offsets.push(schema_names.len() as i32);
            } else {
                catalog_nulls.push(false);
            }
        }

        // Build arrays from bottom up
        let columns_array = if include_columns {
            let column_fields = get_column_fields();
            let columns_struct = StructArray::try_new(
                column_fields.clone(),
                vec![
                    Arc::new(col_names.finish()),
                    Arc::new(col_ordinals.finish()),
                    Arc::new(col_remarks.finish()),
                    Arc::new(col_xdbc_data_type.finish()),
                    Arc::new(col_xdbc_type_name.finish()),
                    Arc::new(col_xdbc_column_size.finish()),
                    Arc::new(col_xdbc_decimal_digits.finish()),
                    Arc::new(col_xdbc_num_prec_radix.finish()),
                    Arc::new(col_xdbc_nullable.finish()),
                    Arc::new(col_xdbc_column_def.finish()),
                    Arc::new(col_xdbc_sql_data_type.finish()),
                    Arc::new(col_xdbc_datetime_sub.finish()),
                    Arc::new(col_xdbc_char_octet_length.finish()),
                    Arc::new(col_xdbc_is_nullable.finish()),
                    Arc::new(col_xdbc_scope_catalog.finish()),
                    Arc::new(col_xdbc_scope_schema.finish()),
                    Arc::new(col_xdbc_scope_table.finish()),
                    Arc::new(col_xdbc_is_autoincrement.finish()),
                    Arc::new(col_xdbc_is_generatedcolumn.finish()),
                ],
                None,
            )
            .map_err(to_internal_err)?;

            let column_list_field = Arc::new(Field::new("item", COLUMN_SCHEMA.clone(), true));
            Some(ListArray::new(
                column_list_field,
                OffsetBuffer::new(column_offsets.into()),
                Arc::new(columns_struct),
                None,
            ))
        } else {
            None
        };

        // Build empty constraints array
        let constraints_array = if include_columns {
            let constraint_fields = get_constraint_fields();
            // We need to build with correct schema
            let constraints_struct = StructArray::try_new(
                constraint_fields.clone(),
                vec![
                    Arc::new(StringBuilder::new().finish()), // constraint_name
                    Arc::new(StringBuilder::new().finish()), // constraint_type
                    Arc::new(ListArray::new_null(
                        // constraint_column_names
                        Arc::new(Field::new("item", DataType::Utf8, true)),
                        0,
                    )),
                    Arc::new(ListArray::new_null(
                        // constraint_column_usage
                        Arc::new(Field::new("item", get_usage_schema(), true)),
                        0,
                    )),
                ],
                None,
            )
            .map_err(to_internal_err)?;

            let constraint_list_field =
                Arc::new(Field::new("item", CONSTRAINT_SCHEMA.clone(), true));
            Some(ListArray::new(
                constraint_list_field,
                OffsetBuffer::new(constraint_offsets.into()),
                Arc::new(constraints_struct),
                None,
            ))
        } else {
            None
        };

        // Build tables array
        let tables_array = if include_tables {
            let table_fields = get_table_fields();
            let num_tables = table_names.len();

            let table_columns_array: ArrayRef = match columns_array {
                Some(arr) => Arc::new(arr),
                None => Arc::new(ListArray::new_null(
                    Arc::new(Field::new("item", COLUMN_SCHEMA.clone(), true)),
                    num_tables,
                )),
            };

            let table_constraints_array: ArrayRef = match constraints_array {
                Some(arr) => Arc::new(arr),
                None => Arc::new(ListArray::new_null(
                    Arc::new(Field::new("item", CONSTRAINT_SCHEMA.clone(), true)),
                    num_tables,
                )),
            };

            let tables_struct = StructArray::try_new(
                table_fields.clone(),
                vec![
                    Arc::new(table_names.finish()),
                    Arc::new(table_types.finish()),
                    table_columns_array,
                    table_constraints_array,
                ],
                None,
            )
            .map_err(to_internal_err)?;

            let table_list_field = Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true));
            Some(ListArray::new(
                table_list_field,
                OffsetBuffer::new(table_offsets.into()),
                Arc::new(tables_struct),
                None,
            ))
        } else {
            None
        };

        // Build schemas array
        let schemas_array = if include_schemas {
            let schema_fields = get_db_schema_fields();
            let num_schemas = schema_names.len();

            let schema_tables_array: ArrayRef = match tables_array {
                Some(arr) => Arc::new(arr),
                None => Arc::new(ListArray::new_null(
                    Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true)),
                    num_schemas,
                )),
            };

            // Build null buffer for tables based on schema_nulls
            let tables_null_buffer = if include_tables {
                None
            } else {
                Some(NullBuffer::from(schema_nulls.clone()))
            };

            let schema_tables_array: ArrayRef = if include_tables {
                schema_tables_array
            } else {
                // Need to create a ListArray with nulls
                Arc::new(ListArray::new(
                    Arc::new(Field::new("item", TABLE_SCHEMA.clone(), true)),
                    OffsetBuffer::new(vec![0i32; num_schemas + 1].into()),
                    Arc::new(empty_struct_array(&get_table_fields())),
                    tables_null_buffer,
                ))
            };

            let schemas_struct = StructArray::try_new(
                schema_fields.clone(),
                vec![Arc::new(schema_names.finish()), schema_tables_array],
                None,
            )
            .map_err(to_internal_err)?;

            let schema_list_field =
                Arc::new(Field::new("item", OBJECTS_DB_SCHEMA_SCHEMA.clone(), true));
            Some(ListArray::new(
                schema_list_field,
                OffsetBuffer::new(schema_offsets.into()),
                Arc::new(schemas_struct),
                None,
            ))
        } else {
            None
        };

        // Build catalog_db_schemas array
        let num_catalogs = catalog_names.len();
        let catalog_db_schemas_array: ArrayRef = match schemas_array {
            Some(arr) => Arc::new(arr),
            None => {
                // At Catalogs depth, return null lists
                Arc::new(ListArray::new(
                    Arc::new(Field::new("item", OBJECTS_DB_SCHEMA_SCHEMA.clone(), true)),
                    OffsetBuffer::new(vec![0i32; num_catalogs + 1].into()),
                    Arc::new(empty_struct_array(&get_db_schema_fields())),
                    Some(NullBuffer::from(catalog_nulls)),
                ))
            }
        };

        let catalog_name_array = Arc::new(catalog_names.finish()) as ArrayRef;

        RecordBatch::try_new(
            GET_OBJECTS_SCHEMA.clone(),
            vec![catalog_name_array, catalog_db_schemas_array],
        )
        .map_err(to_internal_err)
    }
}

fn append_option_str(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

fn get_struct_fields(schema: &DataType) -> Fields {
    match schema {
        DataType::Struct(fields) => fields.clone(),
        _ => unreachable!("Expected struct schema"),
    }
}

fn get_column_fields() -> Fields {
    get_struct_fields(&COLUMN_SCHEMA)
}

fn get_constraint_fields() -> Fields {
    get_struct_fields(&CONSTRAINT_SCHEMA)
}

fn get_table_fields() -> Fields {
    get_struct_fields(&TABLE_SCHEMA)
}

fn get_db_schema_fields() -> Fields {
    get_struct_fields(&OBJECTS_DB_SCHEMA_SCHEMA)
}

fn get_usage_schema() -> DataType {
    use adbc_core::schemas::USAGE_SCHEMA;
    USAGE_SCHEMA.clone()
}

/// Creates an empty struct array with the given schema
fn empty_struct_array(fields: &Fields) -> StructArray {
    let arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|f| arrow_array::new_empty_array(f.data_type()))
        .collect();
    StructArray::try_new(fields.clone(), arrays, None).unwrap()
}

fn to_internal_err(e: impl std::fmt::Display) -> Error {
    Error::with_message_and_status(format!("{e}"), Status::Internal)
}

impl Default for GetObjectsBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn test_builder_empty_has_correct_schema() {
        let builder = GetObjectsBuilder::new();
        let batch = builder.finish(ObjectDepth::Catalogs).unwrap();

        assert_eq!(batch.schema(), GET_OBJECTS_SCHEMA.clone());
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_builder_catalogs_depth_returns_catalog_with_null_schemas() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_catalog("druid");

        let batch = builder.finish(ObjectDepth::Catalogs).unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Verify catalog_name column
        let catalog_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(catalog_col.value(0), "druid");

        // Verify catalog_db_schemas is null at Catalogs depth
        let schemas_col = batch.column(1);
        assert!(schemas_col.is_null(0));
    }

    #[test]
    fn test_builder_schemas_depth_returns_schemas_with_null_tables() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_schema("druid", "druid");
        builder.add_schema("druid", "sys");

        let batch = builder.finish(ObjectDepth::Schemas).unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Verify catalog_db_schemas is NOT null at Schemas depth
        let schemas_col = batch.column(1);
        assert!(!schemas_col.is_null(0));

        // Get the list array and verify it has 2 schemas
        let list_col = schemas_col
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let schemas_struct = list_col.value(0);
        assert_eq!(schemas_struct.len(), 2);

        // Verify schema names
        let struct_arr = schemas_struct
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        let schema_names = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(schema_names.value(0), "druid");
        assert_eq!(schema_names.value(1), "sys");

        // Verify db_schema_tables is null at Schemas depth
        let tables_col = struct_arr.column(1);
        assert!(tables_col.is_null(0));
        assert!(tables_col.is_null(1));
    }

    #[test]
    fn test_builder_tables_depth_returns_tables_with_null_columns() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_table("druid", "druid", "wikipedia", "TABLE");
        builder.add_table("druid", "sys", "segments", "SYSTEM TABLE");

        let batch = builder.finish(ObjectDepth::Tables).unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Navigate to tables
        let schemas_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let first_catalog_schemas = schemas_col.value(0);
        let schemas_struct = first_catalog_schemas
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();

        // Get first schema's tables
        let tables_list = schemas_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let first_schema_tables = tables_list.value(0);
        let tables_struct = first_schema_tables
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();

        // Verify table name and type
        let table_names = tables_struct
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(table_names.value(0), "wikipedia");

        let table_types = tables_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(table_types.value(0), "TABLE");

        // Verify table_columns is null at Tables depth
        let columns_col = tables_struct.column(2);
        assert!(columns_col.is_null(0));
    }

    #[test]
    fn test_builder_columns_depth_returns_columns() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_table("druid", "druid", "wikipedia", "TABLE");
        builder.add_column(
            "druid",
            "druid",
            "wikipedia",
            ColumnInfo {
                name: "__time".to_string(),
                ordinal_position: Some(1),
                xdbc_type_name: Some("TIMESTAMP".to_string()),
                xdbc_nullable: Some(0),
                xdbc_is_nullable: Some("NO".to_string()),
                ..Default::default()
            },
        );
        builder.add_column(
            "druid",
            "druid",
            "wikipedia",
            ColumnInfo {
                name: "channel".to_string(),
                ordinal_position: Some(2),
                xdbc_type_name: Some("VARCHAR".to_string()),
                xdbc_nullable: Some(1),
                xdbc_is_nullable: Some("YES".to_string()),
                ..Default::default()
            },
        );

        let batch = builder.finish(ObjectDepth::Columns).unwrap();

        // Navigate to columns
        let schemas_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let first_catalog_schemas = schemas_col.value(0);
        let schemas_struct = first_catalog_schemas
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        let tables_list = schemas_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let first_schema_tables = tables_list.value(0);
        let tables_struct = first_schema_tables
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        let columns_list = tables_struct
            .column(2)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();

        // Verify columns are NOT null at Columns depth
        assert!(!columns_list.is_null(0));

        let first_table_columns = columns_list.value(0);
        let columns_struct = first_table_columns
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();

        assert_eq!(columns_struct.len(), 2);

        // Verify column names
        let column_names = columns_struct
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(column_names.value(0), "__time");
        assert_eq!(column_names.value(1), "channel");

        // Verify ordinal positions
        let ordinals = columns_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        assert_eq!(ordinals.value(0), 1);
        assert_eq!(ordinals.value(1), 2);

        // Verify xdbc_type_name
        let type_names = columns_struct
            .column(4)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(type_names.value(0), "TIMESTAMP");
        assert_eq!(type_names.value(1), "VARCHAR");
    }

    #[test]
    fn test_builder_all_depth_same_as_columns() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_table("druid", "druid", "wikipedia", "TABLE");

        let batch = builder.finish(ObjectDepth::All).unwrap();

        // Verify columns list is present (not null) at All depth
        let schemas_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let first_catalog_schemas = schemas_col.value(0);
        let schemas_struct = first_catalog_schemas
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        let tables_list = schemas_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let first_schema_tables = tables_list.value(0);
        let tables_struct = first_schema_tables
            .as_any()
            .downcast_ref::<arrow_array::StructArray>()
            .unwrap();
        let columns_col = tables_struct.column(2);

        // At All depth, columns should be an empty list, not null
        assert!(!columns_col.is_null(0));
    }

    #[test]
    fn test_add_catalog_is_idempotent() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_catalog("druid");
        builder.add_catalog("druid"); // Add again

        let batch = builder.finish(ObjectDepth::Catalogs).unwrap();
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn test_add_schema_is_idempotent() {
        let mut builder = GetObjectsBuilder::new();
        builder.add_schema("druid", "druid");
        builder.add_schema("druid", "druid"); // Add again

        let batch = builder.finish(ObjectDepth::Schemas).unwrap();

        let schemas_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        let schemas_struct = schemas_col.value(0);
        assert_eq!(schemas_struct.len(), 1);
    }
}
