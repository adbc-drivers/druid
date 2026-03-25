//! Arrow to Druid SQL parameter conversion utilities.

use crate::client::SqlParameter;
use adbc_core::error::{Error, Result, Status};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};

/// Maps an Arrow `DataType` to the corresponding Druid SQL type name.
pub(crate) fn arrow_to_druid_type(data_type: &DataType) -> Result<&'static str> {
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

/// Extracts a value from an Arrow array at a given row index as JSON.
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

/// Converts a `RecordBatch` (expected to have exactly 1 row) into Druid SQL parameters.
pub(crate) fn build_parameters(batch: &RecordBatch) -> Result<Vec<SqlParameter>> {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            Ok(SqlParameter {
                sql_type: arrow_to_druid_type(field.data_type())?.to_string(),
                value: extract_value(batch.column(i), 0)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

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
    fn test_build_parameters_int64() {
        let mut builder = Int64Builder::new();
        builder.append_value(42);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = build_parameters(&batch).unwrap();
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

        let params = build_parameters(&batch).unwrap();
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

        let params = build_parameters(&batch).unwrap();
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

        let params = build_parameters(&batch).unwrap();
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

        let params = build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].sql_type, "BIGINT");
        assert_eq!(params[0].value, serde_json::json!(42));
        assert_eq!(params[1].sql_type, "VARCHAR");
        assert_eq!(params[1].value, serde_json::json!("test"));
    }
}
