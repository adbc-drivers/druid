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

use crate::client::SqlParameter;
use adbc_core::error::{Error, Result, Status};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, DecimalType, Float16Type, Float32Type, Float64Type,
    Int8Type, Int16Type, Int32Type, Int64Type, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use chrono::{NaiveDate, TimeDelta};

const MILLIS_PER_DAY: i64 = 86_400_000;

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
        DataType::Float16 | DataType::Float32 => Ok("FLOAT"),
        DataType::Float64 | DataType::Decimal128(_, _) => Ok("DOUBLE"),
        DataType::Boolean => Ok("BOOLEAN"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("VARCHAR"),
        DataType::Timestamp(_, _) => Ok("TIMESTAMP"),
        DataType::Date32 | DataType::Date64 => Ok("DATE"),
        DataType::Dictionary(_, value_type) => arrow_to_druid_type(value_type),
        _ => Err(Error::with_message_and_status(
            format!("Unsupported Arrow type for Druid parameter: {data_type}"),
            Status::InvalidArguments,
        )),
    }
}

/// Converts an Arrow date value to the ISO-8601 representation expected by Druid.
fn date_to_json(days_since_epoch: i64) -> Result<serde_json::Value> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");
    let date = TimeDelta::try_days(days_since_epoch)
        .and_then(|offset| epoch.checked_add_signed(offset))
        .ok_or_else(|| {
            Error::with_message_and_status(
                format!("Arrow date is outside Druid's supported range: {days_since_epoch}"),
                Status::InvalidArguments,
            )
        })?;
    Ok(serde_json::json!(date.format("%Y-%m-%d").to_string()))
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
        DataType::Float16 => Ok(serde_json::json!(
            array.as_primitive::<Float16Type>().value(row).to_f32()
        )),
        DataType::Float32 => Ok(serde_json::json!(
            array.as_primitive::<Float32Type>().value(row)
        )),
        DataType::Float64 => Ok(serde_json::json!(
            array.as_primitive::<Float64Type>().value(row)
        )),
        DataType::Decimal128(precision, scale) => {
            let value = Decimal128Type::format_decimal(
                array.as_primitive::<Decimal128Type>().value(row),
                *precision,
                *scale,
            );
            let value = value.parse::<f64>().map_err(|error| {
                Error::with_message_and_status(
                    format!("Failed to convert decimal parameter to DOUBLE: {error}"),
                    Status::InvalidArguments,
                )
            })?;
            Ok(serde_json::json!(value))
        }
        DataType::Boolean => Ok(serde_json::json!(array.as_boolean().value(row))),
        DataType::Utf8 => Ok(serde_json::json!(array.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => Ok(serde_json::json!(array.as_string::<i64>().value(row))),
        DataType::Utf8View => Ok(serde_json::json!(array.as_string_view().value(row))),
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
        DataType::Date32 => date_to_json(i64::from(array.as_primitive::<Date32Type>().value(row))),
        DataType::Date64 => date_to_json(
            array
                .as_primitive::<Date64Type>()
                .value(row)
                .div_euclid(MILLIS_PER_DAY),
        ),
        DataType::Dictionary(_, _) => {
            let dictionary = array.as_any_dictionary();
            extract_value(dictionary.values(), dictionary.normalized_keys()[row])
        }
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
    use arrow_array::builder::{
        Date32Builder, Date64Builder, Float16Builder, Float64Builder, Int64Builder, StringBuilder,
        StringDictionaryBuilder, StringViewBuilder,
    };
    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, Decimal128Array};
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
    fn test_build_parameters_decimal128_as_double() {
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(12_345)])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        let batch = make_batch(vec![("p", array)]);

        let params = build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "DOUBLE");
        assert_eq!(params[0].value, serde_json::json!(123.45));
    }

    #[test]
    fn test_build_parameters_date32_as_iso_string() {
        let mut builder = Date32Builder::new();
        builder.append_value(19_492);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "DATE");
        assert_eq!(params[0].value, serde_json::json!("2023-05-15"));
    }

    #[test]
    fn test_build_parameters_date64_as_iso_string() {
        let mut builder = Date64Builder::new();
        builder.append_value(1_684_108_800_000);
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "DATE");
        assert_eq!(params[0].value, serde_json::json!("2023-05-15"));
    }

    #[test]
    fn test_build_parameters_float16() {
        let mut builder = Float16Builder::new();
        builder.append_value(Default::default());
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "FLOAT");
        assert_eq!(params[0].value, serde_json::json!(0.0));
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
    fn test_build_parameters_string_view() {
        let mut builder = StringViewBuilder::new();
        builder.append_value("hello");
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = make_batch(vec![("p", array)]);

        let params = build_parameters(&batch).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].sql_type, "VARCHAR");
        assert_eq!(params[0].value, serde_json::json!("hello"));
    }

    #[test]
    fn test_build_parameters_dictionary_string() {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append("hello").unwrap();
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
