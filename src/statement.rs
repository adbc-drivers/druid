// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::batch_reader::SingleBatchReader;
use crate::client::{DruidClient, SqlParameter};
use crate::parameters::build_parameters;
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use arrow_select::concat::concat_batches;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug)]
pub struct DruidStatement {
    client: Arc<DruidClient>,
    sql_query: Option<String>,
    bind_data: Option<RecordBatch>,
    context: HashMap<String, OptionValue>,
}

impl DruidStatement {
    #[must_use]
    pub fn new(client: Arc<DruidClient>) -> Self {
        Self {
            client,
            sql_query: None,
            bind_data: None,
            context: HashMap::new(),
        }
    }

    fn query(&self) -> Result<&str> {
        self.sql_query.as_deref().ok_or_else(|| {
            Error::with_message_and_status(
                "No SQL query set. Call set_sql_query first.".to_string(),
                Status::InvalidState,
            )
        })
    }

    fn take_parameters(&mut self) -> Result<Vec<SqlParameter>> {
        self.bind_data
            .take()
            .map_or_else(|| Ok(vec![]), |batch| build_parameters(&batch))
    }

    fn build_schema_query(&self) -> Result<String> {
        let query = self.query()?;
        // Wrap in subquery with LIMIT 0 to get schema without data
        // Druid requires subqueries to have an alias
        Ok(format!("SELECT * FROM ({query}) AS __schema_query LIMIT 0"))
    }

    fn build_context(&self) -> HashMap<String, serde_json::Value> {
        self.context
            .iter()
            .filter_map(|(k, v)| Self::option_value_to_json(v).map(|v| (k.clone(), v)))
            .collect()
    }

    fn option_value_to_json(value: &OptionValue) -> Option<serde_json::Value> {
        match value {
            OptionValue::String(s) => Some(serde_json::Value::String(s.clone())),
            OptionValue::Int(i) => Some(serde_json::json!(*i)),
            OptionValue::Double(d) => Some(serde_json::json!(*d)),
            _ => None, // Bytes not supported; wildcard for #[non_exhaustive]
        }
    }

    /// Returns the stored option value for a given key, or an error if not found
    /// or if the key is a standard ADBC option (which Druid doesn't support).
    fn get_context_value(&self, key: &OptionStatement) -> Result<&OptionValue> {
        match key {
            OptionStatement::Other(name) => self.context.get(name).ok_or_else(|| {
                Error::with_message_and_status(
                    format!("Option '{name}' not found"),
                    Status::NotFound,
                )
            }),
            _ => Err(Error::with_message_and_status(
                format!("Option {key:?} is not supported by Druid driver"),
                Status::NotImplemented,
            )),
        }
    }
}

impl Statement for DruidStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() != 1 {
            return Err(Error::with_message_and_status(
                format!("bind expects exactly 1 row, got {}", batch.num_rows()),
                Status::InvalidArguments,
            ));
        }
        self.bind_data = Some(batch);
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let schema = reader.schema();

        // Collect all batches from the stream
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to read from stream: {e}"),
                    Status::IO,
                )
            })?;

        // Handle empty stream (no batches at all)
        if batches.is_empty() {
            return Err(Error::with_message_and_status(
                "bind_stream received empty stream".to_string(),
                Status::InvalidArguments,
            ));
        }

        // Concatenate all batches into a single RecordBatch
        let concatenated = concat_batches(&schema, &batches).map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to concatenate batches: {e}"),
                Status::Internal,
            )
        })?;

        // Validate exactly 1 row (same constraint as bind)
        if concatenated.num_rows() != 1 {
            return Err(Error::with_message_and_status(
                format!(
                    "bind_stream expects exactly 1 row, got {}",
                    concatenated.num_rows()
                ),
                Status::InvalidArguments,
            ));
        }

        self.bind_data = Some(concatenated);
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        let params = self.take_parameters()?;
        let context = self.build_context();
        let batch = self.client.execute_query(self.query()?, params, context)?;
        Ok(SingleBatchReader::new(batch))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let params = self.take_parameters()?;
        let context = self.build_context();
        // Execute the query and discard the result batch. Druid's SQL API
        // doesn't return affected row counts for DML/DDL statements.
        let _result = self.client.execute_query(self.query()?, params, context)?;
        Ok(None)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let schema_query = self.build_schema_query()?;
        let context = self.build_context();
        let batch = self.client.execute_query(&schema_query, vec![], context)?;
        Ok(batch.schema().as_ref().clone())
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Err(Error::with_message_and_status(
            "execute_partitions not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Err(Error::with_message_and_status(
            "get_parameter_schema not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "prepare not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Err(Error::with_message_and_status(
            "set_substrait_plan not implemented".to_string(),
            Status::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> Result<()> {
        Err(Error::with_message_and_status(
            "cancel not implemented".to_string(),
            Status::NotImplemented,
        ))
    }
}

impl Optionable for DruidStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        match key {
            OptionStatement::Other(name) => {
                if matches!(value, OptionValue::Bytes(_)) {
                    return Err(Error::with_message_and_status(
                        "Druid context does not support bytes values".to_string(),
                        Status::NotImplemented,
                    ));
                }
                self.context.insert(name, value);
                Ok(())
            }
            _ => Err(Error::with_message_and_status(
                format!("Option {key:?} is not supported by Druid driver"),
                Status::NotImplemented,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match self.get_context_value(&key)? {
            OptionValue::String(s) => Ok(s.clone()),
            _ => Err(Error::with_message_and_status(
                format!("Option {key:?} is not a string"),
                Status::InvalidArguments,
            )),
        }
    }

    fn get_option_bytes(&self, _key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            "Druid context does not support bytes values".to_string(),
            Status::NotImplemented,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        match self.get_context_value(&key)? {
            OptionValue::Int(i) => Ok(*i),
            _ => Err(Error::with_message_and_status(
                format!("Option {key:?} is not an integer"),
                Status::InvalidArguments,
            )),
        }
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        match self.get_context_value(&key)? {
            OptionValue::Double(d) => Ok(*d),
            _ => Err(Error::with_message_and_status(
                format!("Option {key:?} is not a double"),
                Status::InvalidArguments,
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch_reader::SingleBatchReader;
    use arrow_array::ArrayRef;
    use arrow_array::builder::Int64Builder;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int64Type;
    use arrow_schema::{ArrowError, DataType, Field, SchemaRef};

    // ========== Optionable tests ==========

    #[test]
    fn test_set_option_stores_string_value() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.set_option(
            OptionStatement::Other("sqlTimeZone".to_string()),
            OptionValue::String("America/New_York".to_string()),
        );
        assert!(result.is_ok());

        let retrieved = stmt.get_option_string(OptionStatement::Other("sqlTimeZone".to_string()));
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap(), "America/New_York");
    }

    #[test]
    fn test_set_option_stores_int_value() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.set_option(
            OptionStatement::Other("timeout".to_string()),
            OptionValue::Int(30000),
        );
        assert!(result.is_ok());

        let retrieved = stmt.get_option_int(OptionStatement::Other("timeout".to_string()));
        assert!(retrieved.is_ok());
        assert_eq!(retrieved.unwrap(), 30000);
    }

    #[test]
    fn test_set_option_stores_double_value() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.set_option(
            OptionStatement::Other("someDouble".to_string()),
            OptionValue::Double(3.14),
        );
        assert!(result.is_ok());

        let retrieved = stmt.get_option_double(OptionStatement::Other("someDouble".to_string()));
        assert!(retrieved.is_ok());
        assert!((retrieved.unwrap() - 3.14).abs() < f64::EPSILON);
    }

    #[test]
    fn test_set_option_bytes_returns_not_implemented() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.set_option(
            OptionStatement::Other("someBytes".to_string()),
            OptionValue::Bytes(vec![1, 2, 3]),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);
    }

    #[test]
    fn test_set_option_standard_options_return_not_implemented() {
        let mut stmt = DruidStatement::new(create_test_client());

        // IngestMode is not applicable to Druid
        let result = stmt.set_option(
            OptionStatement::IngestMode,
            OptionValue::String("create".to_string()),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);

        // TargetTable is not applicable to Druid
        let result = stmt.set_option(
            OptionStatement::TargetTable,
            OptionValue::String("my_table".to_string()),
        );
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);
    }

    #[test]
    fn test_get_option_string_wrong_type_returns_error() {
        let mut stmt = DruidStatement::new(create_test_client());
        stmt.set_option(
            OptionStatement::Other("timeout".to_string()),
            OptionValue::Int(30000),
        )
        .unwrap();

        let result = stmt.get_option_string(OptionStatement::Other("timeout".to_string()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_get_option_not_found_returns_error() {
        let stmt = DruidStatement::new(create_test_client());
        let result = stmt.get_option_string(OptionStatement::Other("nonexistent".to_string()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotFound);
    }

    #[test]
    fn test_set_option_replaces_existing() {
        let mut stmt = DruidStatement::new(create_test_client());
        stmt.set_option(
            OptionStatement::Other("timeout".to_string()),
            OptionValue::Int(1000),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("timeout".to_string()),
            OptionValue::Int(2000),
        )
        .unwrap();

        let retrieved = stmt.get_option_int(OptionStatement::Other("timeout".to_string()));
        assert_eq!(retrieved.unwrap(), 2000);
    }

    #[test]
    fn test_get_option_bytes_returns_not_implemented() {
        let stmt = DruidStatement::new(create_test_client());
        let result = stmt.get_option_bytes(OptionStatement::Other("anything".to_string()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);
    }

    #[test]
    fn test_get_option_standard_options_return_not_implemented() {
        let stmt = DruidStatement::new(create_test_client());

        let result = stmt.get_option_string(OptionStatement::IngestMode);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::NotImplemented);
    }

    #[test]
    fn test_build_context_converts_to_json() {
        let mut stmt = DruidStatement::new(create_test_client());
        stmt.set_option(
            OptionStatement::Other("sqlTimeZone".to_string()),
            OptionValue::String("America/New_York".to_string()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("timeout".to_string()),
            OptionValue::Int(30000),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("someDouble".to_string()),
            OptionValue::Double(1.5),
        )
        .unwrap();

        let context = stmt.build_context();

        assert_eq!(context.len(), 3);
        assert_eq!(
            context.get("sqlTimeZone"),
            Some(&serde_json::json!("America/New_York"))
        );
        assert_eq!(context.get("timeout"), Some(&serde_json::json!(30000)));
        assert_eq!(context.get("someDouble"), Some(&serde_json::json!(1.5)));
    }

    // ========== End Optionable tests ==========

    fn create_test_client() -> Arc<DruidClient> {
        Arc::new(DruidClient::new("http://localhost:8888").unwrap())
    }

    fn make_batch(values: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = values
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<ArrayRef> = values.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    #[test]
    fn test_set_sql_query() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.set_sql_query("SELECT 1");
        assert!(result.is_ok());
        assert_eq!(stmt.sql_query, Some("SELECT 1".to_string()));
    }

    #[test]
    fn test_set_sql_query_overwrites_previous() {
        let mut stmt = DruidStatement::new(create_test_client());
        stmt.set_sql_query("SELECT 1").unwrap();
        stmt.set_sql_query("SELECT 2").unwrap();
        assert_eq!(stmt.sql_query, Some("SELECT 2".to_string()));
    }

    #[test]
    fn test_execute_update_without_query_returns_error() {
        let mut stmt = DruidStatement::new(create_test_client());
        let result = stmt.execute_update();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidState);
    }

    #[test]
    fn test_bind_stores_data() {
        let mut stmt = DruidStatement::new(create_test_client());
        let mut builder = Int64Builder::new();
        builder.append_value(42);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("param", array)]);

        let result = stmt.bind(batch);
        assert!(result.is_ok());
        assert!(stmt.bind_data.is_some());
    }

    #[test]
    fn test_bind_rejects_multiple_rows() {
        let mut stmt = DruidStatement::new(create_test_client());
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        builder.append_value(2);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("param", array)]);

        let result = stmt.bind(batch);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_bind_rejects_zero_rows() {
        let mut stmt = DruidStatement::new(create_test_client());
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::new_empty(schema);

        let result = stmt.bind(batch);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_bind_replaces_previous() {
        let mut stmt = DruidStatement::new(create_test_client());

        let mut builder = Int64Builder::new();
        builder.append_value(1);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch1 = make_batch(vec![("param", array)]);
        stmt.bind(batch1).unwrap();

        let mut builder = Int64Builder::new();
        builder.append_value(2);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch2 = make_batch(vec![("param", array)]);
        stmt.bind(batch2).unwrap();

        assert!(stmt.bind_data.is_some());
        assert_eq!(stmt.bind_data.as_ref().unwrap().num_columns(), 1);
    }

    // Helper for testing bind_stream with multiple batches
    struct MultiBatchReader {
        batches: std::vec::IntoIter<RecordBatch>,
        schema: SchemaRef,
    }

    impl MultiBatchReader {
        fn new(batches: Vec<RecordBatch>) -> Self {
            let schema = batches
                .first()
                .map_or_else(|| Arc::new(Schema::empty()), |b| b.schema());
            Self {
                batches: batches.into_iter(),
                schema,
            }
        }
    }

    impl Iterator for MultiBatchReader {
        type Item = std::result::Result<RecordBatch, ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            self.batches.next().map(Ok)
        }
    }

    impl RecordBatchReader for MultiBatchReader {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    #[test]
    fn test_bind_stream_single_batch_single_row() {
        let mut stmt = DruidStatement::new(create_test_client());
        let mut builder = Int64Builder::new();
        builder.append_value(42);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("param", array)]);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(SingleBatchReader::new(batch));

        let result = stmt.bind_stream(reader);
        assert!(result.is_ok());
        assert!(stmt.bind_data.is_some());
        assert_eq!(stmt.bind_data.as_ref().unwrap().num_rows(), 1);
    }

    #[test]
    fn test_bind_stream_rejects_multiple_rows() {
        let mut stmt = DruidStatement::new(create_test_client());
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        builder.append_value(2);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("param", array)]);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(SingleBatchReader::new(batch));

        let result = stmt.bind_stream(reader);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_bind_stream_rejects_zero_rows() {
        let mut stmt = DruidStatement::new(create_test_client());
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::new_empty(schema);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(SingleBatchReader::new(batch));

        let result = stmt.bind_stream(reader);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().status, Status::InvalidArguments);
    }

    #[test]
    fn test_bind_stream_replaces_previous_bind_data() {
        let mut stmt = DruidStatement::new(create_test_client());

        // First bind with bind()
        let mut builder = Int64Builder::new();
        builder.append_value(1);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("param", array)]);
        stmt.bind(batch).unwrap();

        // Then bind_stream
        let mut builder = Int64Builder::new();
        builder.append_value(99);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("param", array)]);
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(SingleBatchReader::new(batch));
        stmt.bind_stream(reader).unwrap();

        // Verify bind_data is from bind_stream
        let value = stmt
            .bind_data
            .as_ref()
            .unwrap()
            .column(0)
            .as_primitive::<Int64Type>()
            .value(0);
        assert_eq!(value, 99);
    }

    #[test]
    fn test_bind_stream_concatenates_multiple_batches() {
        let mut stmt = DruidStatement::new(create_test_client());

        // Create two empty batches and one batch with 1 row
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let empty1 = RecordBatch::new_empty(schema.clone());
        let empty2 = RecordBatch::new_empty(schema);

        let mut builder = Int64Builder::new();
        builder.append_value(42);
        let array: ArrayRef = Arc::new(builder.finish());
        let with_row = make_batch(vec![("a", array)]);

        let reader: Box<dyn RecordBatchReader + Send> =
            Box::new(MultiBatchReader::new(vec![empty1, empty2, with_row]));

        let result = stmt.bind_stream(reader);
        assert!(result.is_ok());
        assert_eq!(stmt.bind_data.as_ref().unwrap().num_rows(), 1);
    }
}
