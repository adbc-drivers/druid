use crate::batch_reader::SingleBatchReader;
use crate::client::{DruidClient, DruidType, SqlParameter};
use crate::info::GetInfoBuilder;
use crate::objects::{ColumnInfo, GetObjectsBuilder};
use crate::statement::DruidStatement;
use adbc_core::constants::ADBC_VERSION_1_1_0;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{InfoCode, ObjectDepth, OptionConnection, OptionValue};
use adbc_core::schemas::GET_TABLE_TYPES_SCHEMA;
use adbc_core::{Connection, Optionable};
use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{Field, Schema};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// The set of info codes supported by this driver.
const SUPPORTED_INFO_CODES: &[InfoCode] = &[
    InfoCode::VendorName,
    InfoCode::VendorVersion,
    InfoCode::VendorSql,
    InfoCode::VendorSubstrait,
    InfoCode::DriverName,
    InfoCode::DriverVersion,
    InfoCode::DriverArrowVersion,
    InfoCode::DriverAdbcVersion,
];

/// Arrow library version used by this driver.
const ARROW_VERSION: &str = "57";

/// Table types supported by Druid.
const SUPPORTED_TABLE_TYPES: &[&str] = &["TABLE", "SYSTEM TABLE"];

#[derive(Debug)]
pub struct DruidConnection {
    client: Arc<DruidClient>,
}

impl DruidConnection {
    /// Creates a new `DruidConnection` with the given URI.
    ///
    /// # Errors
    /// Returns an error if the HTTP client fails to build.
    pub fn new(uri: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: Arc::new(DruidClient::new(uri)?),
        })
    }

    /// Query schemas from `INFORMATION_SCHEMA`, applying optional filter pattern.
    fn query_schemas(&self, db_schema: Option<&str>) -> Result<Vec<String>> {
        let (where_clause, params) = build_like_clause("SCHEMA_NAME", db_schema);

        let query = format!(
            "SELECT DISTINCT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA \
             WHERE CATALOG_NAME = 'druid'{where_clause} ORDER BY SCHEMA_NAME"
        );

        let batch = self.client.execute_query(&query, params, HashMap::new())?;

        let col = batch.column(0).as_string::<i32>();
        Ok((0..batch.num_rows())
            .map(|i| col.value(i).to_string())
            .collect())
    }

    /// Query tables from `INFORMATION_SCHEMA`, applying optional filter patterns.
    fn query_tables(
        &self,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
    ) -> Result<Vec<(String, String, String)>> {
        let (schema_clause, mut params) = build_like_clause("TABLE_SCHEMA", db_schema);
        let (table_clause, table_params) = build_like_clause("TABLE_NAME", table_name);
        params.extend(table_params);

        // Build table type filter
        let type_clause = match table_type {
            Some(types) if !types.is_empty() => {
                // Normalize types: "SYSTEM TABLE" -> "SYSTEM_TABLE" for Druid's format
                let placeholders = types
                    .iter()
                    .map(|t| {
                        params.push(SqlParameter::varchar(normalize_table_type_to_druid(t)));
                        "?"
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!(" AND TABLE_TYPE IN ({placeholders})")
            }
            _ => String::new(),
        };

        let query = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_CATALOG = 'druid'{schema_clause}{table_clause}{type_clause} \
             ORDER BY TABLE_SCHEMA, TABLE_NAME"
        );

        let batch = self.client.execute_query(&query, params, HashMap::new())?;

        let schema_col = batch.column(0).as_string::<i32>();
        let name_col = batch.column(1).as_string::<i32>();
        let type_col = batch.column(2).as_string::<i32>();

        Ok((0..batch.num_rows())
            .map(|i| {
                (
                    schema_col.value(i).to_string(),
                    name_col.value(i).to_string(),
                    normalize_table_type_to_adbc(type_col.value(i)),
                )
            })
            .collect())
    }

    /// Query columns from `INFORMATION_SCHEMA`, applying optional filter patterns.
    fn query_columns(
        &self,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        column_name: Option<&str>,
    ) -> Result<Vec<ColumnQueryResult>> {
        let (schema_clause, mut params) = build_like_clause("TABLE_SCHEMA", db_schema);
        let (table_clause, table_params) = build_like_clause("TABLE_NAME", table_name);
        params.extend(table_params);
        let (column_clause, column_params) = build_like_clause("COLUMN_NAME", column_name);
        params.extend(column_params);

        let query = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, \
             IS_NULLABLE, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, \
             NUMERIC_PRECISION_RADIX, JDBC_TYPE \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_CATALOG = 'druid'{schema_clause}{table_clause}{column_clause} \
             ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
        );

        let batch = self.client.execute_query(&query, params, HashMap::new())?;

        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let schema_col = batch.column(0).as_string::<i32>();
        let table_col = batch.column(1).as_string::<i32>();
        let col_name = batch.column(2).as_string::<i32>();
        let ordinal_col = batch.column(3);
        let nullable_col = batch.column(4).as_string::<i32>();
        let data_type_col = batch.column(5).as_string::<i32>();
        let _precision_col = batch.column(6);
        let scale_col = batch.column(7);
        let radix_col = batch.column(8);
        let jdbc_type_col = batch.column(9);

        Ok((0..batch.num_rows())
            .map(|i| {
                let is_nullable_str = nullable_col.value(i);
                let xdbc_nullable = match is_nullable_str.to_uppercase().as_str() {
                    "YES" => Some(1i16),
                    "NO" => Some(0i16),
                    _ => Some(2i16), // Unknown
                };

                ColumnQueryResult {
                    schema_name: schema_col.value(i).to_string(),
                    table_name: table_col.value(i).to_string(),
                    info: ColumnInfo {
                        name: col_name.value(i).to_string(),
                        ordinal_position: get_i64_value(ordinal_col, i),
                        remarks: None,
                        xdbc_data_type: get_i64_value(jdbc_type_col, i),
                        xdbc_type_name: Some(data_type_col.value(i).to_string()),
                        xdbc_column_size: None,
                        xdbc_decimal_digits: get_i64_value(scale_col, i),
                        xdbc_num_prec_radix: get_i64_value(radix_col, i),
                        xdbc_nullable,
                        xdbc_column_def: None,
                        xdbc_sql_data_type: get_i64_value(jdbc_type_col, i),
                        xdbc_datetime_sub: None,
                        xdbc_char_octet_length: None,
                        xdbc_is_nullable: Some(is_nullable_str.to_string()),
                        xdbc_scope_catalog: None,
                        xdbc_scope_schema: None,
                        xdbc_scope_table: None,
                        xdbc_is_autoincrement: None,
                        xdbc_is_generatedcolumn: None,
                    },
                }
            })
            .collect())
    }
}

/// Result of querying a column from `INFORMATION_SCHEMA`.
struct ColumnQueryResult {
    schema_name: String,
    table_name: String,
    info: ColumnInfo,
}

/// Builds a SQL LIKE clause and parameters for a filter pattern.
fn build_like_clause(column: &str, pattern: Option<&str>) -> (String, Vec<SqlParameter>) {
    match pattern {
        None => (String::new(), Vec::new()),
        Some("") => {
            // Empty string means exact match on empty
            (
                format!(" AND {column} = ?"),
                vec![SqlParameter::varchar("")],
            )
        }
        Some(p) => (
            format!(" AND {column} LIKE ?"),
            vec![SqlParameter::varchar(p)],
        ),
    }
}

/// Extracts an i64 value from an Arrow array and converts it to the target type.
fn get_i64_value<T: TryFrom<i64>>(array: &dyn arrow_array::Array, idx: usize) -> Option<T> {
    use arrow_array::types::Int64Type;
    if array.is_null(idx) {
        return None;
    }
    array
        .as_any()
        .downcast_ref::<arrow_array::PrimitiveArray<Int64Type>>()
        .and_then(|a| T::try_from(a.value(idx)).ok())
}

/// Converts ADBC table type to Druid's format (e.g., `SYSTEM TABLE` → `SYSTEM_TABLE`).
fn normalize_table_type_to_druid(table_type: &str) -> String {
    if table_type.eq_ignore_ascii_case("SYSTEM TABLE") {
        "SYSTEM_TABLE".to_string()
    } else {
        table_type.to_string()
    }
}

/// Converts Druid table type to ADBC format (e.g., `SYSTEM_TABLE` → `SYSTEM TABLE`).
fn normalize_table_type_to_adbc(table_type: &str) -> String {
    if table_type.eq_ignore_ascii_case("SYSTEM_TABLE") {
        "SYSTEM TABLE".to_string()
    } else {
        table_type.to_string()
    }
}

impl Connection for DruidConnection {
    type StatementType = DruidStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(DruidStatement::new(Arc::clone(&self.client)))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "cancel not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader + Send> {
        let mut builder = GetInfoBuilder::new();

        // Determine which codes to return
        let codes_to_return: Vec<InfoCode> = match codes {
            Some(requested) => {
                // Filter to only supported codes, preserving request order isn't required
                let supported: HashSet<InfoCode> = SUPPORTED_INFO_CODES.iter().copied().collect();
                requested.intersection(&supported).copied().collect()
            }
            None => SUPPORTED_INFO_CODES.to_vec(),
        };

        for code in codes_to_return {
            match code {
                InfoCode::VendorName => builder.add_string(code, "Apache Druid"),
                InfoCode::VendorVersion => {
                    let version = self.client.get_server_version();
                    builder.add_string(code, &version);
                }
                InfoCode::VendorSql => builder.add_bool(code, true),
                InfoCode::VendorSubstrait => builder.add_bool(code, false),
                InfoCode::DriverName => builder.add_string(code, "ADBC Druid Driver"),
                InfoCode::DriverVersion => builder.add_string(code, env!("CARGO_PKG_VERSION")),
                InfoCode::DriverArrowVersion => builder.add_string(code, ARROW_VERSION),
                InfoCode::DriverAdbcVersion => {
                    builder.add_int64(code, i64::from(ADBC_VERSION_1_1_0));
                }
                _ => {} // Silently ignore unsupported codes (shouldn't happen due to filter above)
            }
        }

        let batch = builder.finish()?;
        Ok(SingleBatchReader::new(batch))
    }

    fn get_objects(
        &self,
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        let mut builder = GetObjectsBuilder::new();

        // Druid only has "druid" catalog
        // If catalog filter is specified and doesn't match "druid", return empty
        if let Some(cat) = catalog {
            if cat.is_empty() {
                // Empty string = only objects without catalog, Druid always has "druid"
                return Ok(SingleBatchReader::new(builder.finish(depth)?));
            }
            if !cat.eq_ignore_ascii_case("druid") {
                return Ok(SingleBatchReader::new(builder.finish(depth)?));
            }
        }

        builder.add_catalog("druid");

        // At Catalogs depth, we're done
        if matches!(depth, ObjectDepth::Catalogs) {
            return Ok(SingleBatchReader::new(builder.finish(depth)?));
        }

        // Query schemas
        let schemas = self.query_schemas(db_schema)?;
        for schema_name in &schemas {
            builder.add_schema("druid", schema_name);
        }

        // At Schemas depth, we're done
        if matches!(depth, ObjectDepth::Schemas) {
            return Ok(SingleBatchReader::new(builder.finish(depth)?));
        }

        // Query tables
        let tables = self.query_tables(db_schema, table_name, table_type.as_deref())?;
        for (schema_name, tbl_name, tbl_type) in &tables {
            builder.add_table("druid", schema_name, tbl_name, tbl_type);
        }

        // At Tables depth, we're done
        if matches!(depth, ObjectDepth::Tables) {
            return Ok(SingleBatchReader::new(builder.finish(depth)?));
        }

        // Query columns (for Columns or All depth)
        let columns = self.query_columns(db_schema, table_name, column_name)?;
        for col in columns {
            builder.add_column("druid", &col.schema_name, &col.table_name, col.info);
        }

        Ok(SingleBatchReader::new(builder.finish(depth)?))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        // Druid only supports "druid" catalog
        if catalog.is_some_and(|c| !c.eq_ignore_ascii_case("druid")) {
            return Err(Error::with_message_and_status(
                format!(
                    "Invalid catalog '{}'. Druid only supports 'druid' catalog.",
                    catalog.unwrap()
                ),
                Status::InvalidArguments,
            ));
        }

        if table_name.is_empty() {
            return Err(Error::with_message_and_status(
                "Table name cannot be empty".to_string(),
                Status::InvalidArguments,
            ));
        }

        let schema_name = db_schema.unwrap_or("druid");

        let batch = self.client.execute_query(
            "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
            vec![
                SqlParameter::varchar(schema_name),
                SqlParameter::varchar(table_name),
            ],
            HashMap::new(),
        )?;

        if batch.num_rows() == 0 {
            return Err(Error::with_message_and_status(
                format!("Table '{schema_name}.{table_name}' not found"),
                Status::NotFound,
            ));
        }

        let names = batch.column(0).as_string::<i32>();
        let types = batch.column(1).as_string::<i32>();

        let fields: Vec<_> = (0..batch.num_rows())
            .map(|i| {
                let arrow_type = DruidType::from_sql_type(types.value(i)).to_arrow_type();
                Field::new(names.value(i), arrow_type, true)
            })
            .collect();

        Ok(Schema::new(fields))
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        let mut builder = StringBuilder::new();
        for table_type in SUPPORTED_TABLE_TYPES {
            builder.append_value(table_type);
        }

        let batch = RecordBatch::try_new(
            GET_TABLE_TYPES_SCHEMA.clone(),
            vec![Arc::new(builder.finish())],
        )
        .map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create RecordBatch: {e}"),
                Status::Internal,
            )
        })?;

        Ok(SingleBatchReader::new(batch))
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "get_statistic_names not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "get_statistics not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn commit(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "commit not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn rollback(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "rollback not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> Result<impl RecordBatchReader + Send> {
        Err::<SingleBatchReader, Error>(Error::with_message_and_status(
            "read_partition not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}

impl Optionable for DruidConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, _key: Self::Option, _value: OptionValue) -> Result<()> {
        Err(Error::with_message_and_status(
            "set_option not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_string(&self, _key: Self::Option) -> Result<String> {
        Err(Error::with_message_and_status(
            "get_option_string not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "get_option_bytes not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, _key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            "get_option_int not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_double(&self, _key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            "get_option_double not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

    #[test]
    fn test_get_table_types_returns_correct_schema() {
        let conn = DruidConnection::new("http://localhost:8888").unwrap();
        let reader = conn.get_table_types().unwrap();

        assert_eq!(reader.schema(), GET_TABLE_TYPES_SCHEMA.clone());
    }

    #[test]
    fn test_get_table_types_returns_table_and_system_table() {
        let conn = DruidConnection::new("http://localhost:8888").unwrap();
        let mut reader = conn.get_table_types().unwrap();
        let batch = reader.next().unwrap().unwrap();

        assert_eq!(batch.num_rows(), 2);

        let col = batch.column(0).as_string::<i32>();
        let types: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();

        assert!(types.contains(&"TABLE"));
        assert!(types.contains(&"SYSTEM TABLE"));
    }

    #[test]
    fn test_get_table_schema_invalid_catalog_returns_error() {
        let conn = DruidConnection::new("http://localhost:8888").unwrap();
        let result = conn.get_table_schema(Some("invalid_catalog"), Some("druid"), "wikipedia");
        assert!(result.is_err());
    }

    #[test]
    fn test_get_table_schema_empty_table_name_returns_error() {
        let conn = DruidConnection::new("http://localhost:8888").unwrap();
        let result = conn.get_table_schema(None, Some("druid"), "");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_like_clause_none() {
        let (clause, params) = build_like_clause("COL", None);
        assert_eq!(clause, "");
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_like_clause_empty() {
        let (clause, params) = build_like_clause("COL", Some(""));
        assert_eq!(clause, " AND COL = ?");
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_build_like_clause_pattern() {
        let (clause, params) = build_like_clause("COL", Some("test%"));
        assert_eq!(clause, " AND COL LIKE ?");
        assert_eq!(params.len(), 1);
    }
}
