use crate::batch_reader::SingleBatchReader;
use crate::client::{DruidClient, SqlParameter};
use adbc_core::error::{Error, Result, Status};
use adbc_core::options::{OptionStatement, OptionValue};
use adbc_core::{Optionable, PartitionedResult, Statement};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{ArrayRef, RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Schema, TimeUnit};
use arrow_select::concat::concat_batches;
use std::sync::Arc;

#[derive(Debug)]
pub struct DruidStatement {
    client: Arc<DruidClient>,
    sql_query: Option<String>,
    bind_data: Option<RecordBatch>,
}

impl DruidStatement {
    #[must_use]
    pub fn new(client: Arc<DruidClient>) -> Self {
        Self {
            client,
            sql_query: None,
            bind_data: None,
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

    fn arrow_to_druid_type(data_type: &DataType) -> Result<&'static str> {
        match data_type {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => Ok("BIGINT"),
            DataType::Float32 => Ok("FLOAT"),
            DataType::Float64 => Ok("DOUBLE"),
            DataType::Boolean => Ok("BOOLEAN"),
            DataType::Utf8 | DataType::LargeUtf8 => Ok("VARCHAR"),
            DataType::Timestamp(_, _) => Ok("TIMESTAMP"),
            DataType::Date32 | DataType::Date64 => Ok("DATE"),
            _ => Err(Error::with_message_and_status(
                format!("Unsupported Arrow type for Druid parameter: {data_type}"),
                Status::InvalidArguments,
            )),
        }
    }

    fn extract_value(array: &ArrayRef, row: usize) -> Result<serde_json::Value> {
        if array.is_null(row) {
            return Ok(serde_json::Value::Null);
        }

        match array.data_type() {
            DataType::Int8 => Ok(serde_json::json!(
                array.as_primitive::<Int8Type>().value(row)
            )),
            DataType::Int16 => Ok(serde_json::json!(
                array.as_primitive::<Int16Type>().value(row)
            )),
            DataType::Int32 => Ok(serde_json::json!(
                array.as_primitive::<Int32Type>().value(row)
            )),
            DataType::Int64 => Ok(serde_json::json!(
                array.as_primitive::<Int64Type>().value(row)
            )),
            DataType::UInt8 => Ok(serde_json::json!(
                array.as_primitive::<UInt8Type>().value(row)
            )),
            DataType::UInt16 => Ok(serde_json::json!(
                array.as_primitive::<UInt16Type>().value(row)
            )),
            DataType::UInt32 => Ok(serde_json::json!(
                array.as_primitive::<UInt32Type>().value(row)
            )),
            DataType::UInt64 => Ok(serde_json::json!(
                array.as_primitive::<UInt64Type>().value(row)
            )),
            DataType::Float32 => Ok(serde_json::json!(
                array.as_primitive::<Float32Type>().value(row)
            )),
            DataType::Float64 => Ok(serde_json::json!(
                array.as_primitive::<Float64Type>().value(row)
            )),
            DataType::Boolean => Ok(serde_json::json!(array.as_boolean().value(row))),
            DataType::Utf8 => Ok(serde_json::json!(array.as_string::<i32>().value(row))),
            DataType::LargeUtf8 => Ok(serde_json::json!(array.as_string::<i64>().value(row))),
            DataType::Timestamp(TimeUnit::Second, _) => Ok(serde_json::json!(
                array.as_primitive::<TimestampSecondType>().value(row)
            )),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Ok(serde_json::json!(
                array.as_primitive::<TimestampMillisecondType>().value(row)
            )),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Ok(serde_json::json!(
                array.as_primitive::<TimestampMicrosecondType>().value(row)
            )),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(serde_json::json!(
                array.as_primitive::<TimestampNanosecondType>().value(row)
            )),
            DataType::Date32 => Ok(serde_json::json!(
                array.as_primitive::<Date32Type>().value(row)
            )),
            DataType::Date64 => Ok(serde_json::json!(
                array.as_primitive::<Date64Type>().value(row)
            )),
            dt => Err(Error::with_message_and_status(
                format!("Unsupported Arrow type for Druid parameter value: {dt}"),
                Status::InvalidArguments,
            )),
        }
    }

    fn build_parameters(batch: &RecordBatch) -> Result<Vec<SqlParameter>> {
        batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Ok(SqlParameter {
                    sql_type: Self::arrow_to_druid_type(field.data_type())?.to_string(),
                    value: Self::extract_value(batch.column(i), 0)?,
                })
            })
            .collect()
    }

    fn take_parameters(&mut self) -> Result<Vec<SqlParameter>> {
        self.bind_data
            .take()
            .map_or_else(|| Ok(vec![]), |batch| Self::build_parameters(&batch))
    }

    fn build_schema_query(&self) -> Result<String> {
        let query = self.query()?;
        // Wrap in subquery with LIMIT 0 to get schema without data
        // Druid requires subqueries to have an alias
        Ok(format!("SELECT * FROM ({query}) AS __schema_query LIMIT 0"))
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
        let batch = self.client.execute_query(self.query()?, params)?;
        Ok(SingleBatchReader::new(batch))
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let params = self.take_parameters()?;
        // Execute the query and discard the result batch. Druid's SQL API
        // doesn't return affected row counts for DML/DDL statements.
        let _result = self.client.execute_query(self.query()?, params)?;
        Ok(None)
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        let schema_query = self.build_schema_query()?;
        let batch = self.client.execute_query(&schema_query, vec![])?;
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
    use crate::batch_reader::SingleBatchReader;
    use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder};
    use arrow_schema::{ArrowError, DataType, Field, SchemaRef};

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

    #[test]
    fn test_build_parameters_int64() {
        let mut builder = Int64Builder::new();
        builder.append_value(42);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = DruidStatement::build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "BIGINT");
        assert_eq!(params[0].value, serde_json::json!(42));
    }

    #[test]
    fn test_build_parameters_float64() {
        let mut builder = Float64Builder::new();
        builder.append_value(3.14);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = DruidStatement::build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "DOUBLE");
        assert_eq!(params[0].value, serde_json::json!(3.14));
    }

    #[test]
    fn test_build_parameters_string() {
        let mut builder = StringBuilder::new();
        builder.append_value("hello");
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = DruidStatement::build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "VARCHAR");
        assert_eq!(params[0].value, serde_json::json!("hello"));
    }

    #[test]
    fn test_build_parameters_null_value() {
        let mut builder = Int64Builder::new();
        builder.append_null();
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = DruidStatement::build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "BIGINT");
        assert!(params[0].value.is_null());
    }

    #[test]
    fn test_build_parameters_multiple_columns() {
        let mut int_builder = Int64Builder::new();
        int_builder.append_value(42);
        let int_array: ArrayRef = Arc::new(int_builder.finish());

        let mut str_builder = StringBuilder::new();
        str_builder.append_value("test");
        let str_array: ArrayRef = Arc::new(str_builder.finish());

        let batch = make_batch(vec![("a", int_array), ("b", str_array)]);

        let params = DruidStatement::build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].sql_type, "BIGINT");
        assert_eq!(params[0].value, serde_json::json!(42));
        assert_eq!(params[1].sql_type, "VARCHAR");
        assert_eq!(params[1].value, serde_json::json!("test"));
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
