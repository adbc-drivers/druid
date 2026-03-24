use adbc_core::error::{Error, Result, Status};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMillisecondBuilder,
    },
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

/// Maps Druid SQL types to Arrow types and handles array construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DruidType {
    Int64,
    Float32,
    Float64,
    Boolean,
    Timestamp,
    Date,
    String,
}

impl DruidType {
    /// Parse a Druid/SQL type string into a `DruidType`.
    fn from_sql_type(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            // Native Druid integer type and SQL integer types
            "LONG" | "BIGINT" | "INTEGER" | "INT" | "SMALLINT" | "TINYINT" => Self::Int64,
            // Single-precision float
            "FLOAT" => Self::Float32,
            // Double-precision float types
            "DOUBLE" | "DECIMAL" | "REAL" => Self::Float64,
            // Boolean
            "BOOLEAN" => Self::Boolean,
            // Timestamp (Druid returns as ISO 8601 string)
            "TIMESTAMP" => Self::Timestamp,
            // Date (Druid returns as YYYY-MM-DD string)
            "DATE" => Self::Date,
            // String types and unknown/complex types
            _ => Self::String,
        }
    }

    /// Convert to the corresponding Arrow `DataType`.
    fn to_arrow_type(self) -> DataType {
        match self {
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
            Self::Date => DataType::Date32,
            Self::String => DataType::Utf8,
        }
    }

    /// Build an Arrow array from an iterator of optional JSON values.
    fn build_array<'a>(
        self,
        values: impl Iterator<Item = Option<&'a serde_json::Value>>,
    ) -> ArrayRef {
        // Collect to get length for capacity hints
        let values: Vec<_> = values.collect();
        match self {
            Self::Int64 => {
                let mut builder = Int64Builder::with_capacity(values.len());
                for v in values {
                    builder.append_option(v.and_then(serde_json::Value::as_i64));
                }
                Arc::new(builder.finish())
            }
            Self::Float32 => {
                let mut builder = Float32Builder::with_capacity(values.len());
                for v in values {
                    #[allow(clippy::cast_possible_truncation)]
                    builder.append_option(v.and_then(serde_json::Value::as_f64).map(|f| f as f32));
                }
                Arc::new(builder.finish())
            }
            Self::Float64 => {
                let mut builder = Float64Builder::with_capacity(values.len());
                for v in values {
                    builder.append_option(v.and_then(serde_json::Value::as_f64));
                }
                Arc::new(builder.finish())
            }
            Self::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(values.len());
                for v in values {
                    builder.append_option(v.and_then(serde_json::Value::as_bool));
                }
                Arc::new(builder.finish())
            }
            Self::Timestamp => {
                let mut builder = TimestampMillisecondBuilder::with_capacity(values.len());
                for v in values {
                    let ts = v.and_then(serde_json::Value::as_str).and_then(|s| {
                        chrono::DateTime::parse_from_rfc3339(s)
                            .ok()
                            .map(|dt| dt.timestamp_millis())
                    });
                    builder.append_option(ts);
                }
                Arc::new(builder.finish())
            }
            Self::Date => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                    .expect("1970-01-01 is a valid date");
                let mut builder = Date32Builder::with_capacity(values.len());
                for v in values {
                    #[allow(clippy::cast_possible_truncation)]
                    let days = v.and_then(serde_json::Value::as_str).and_then(|s| {
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .ok()
                            .map(|d| (d - epoch).num_days() as i32)
                    });
                    builder.append_option(days);
                }
                Arc::new(builder.finish())
            }
            Self::String => {
                let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
                for v in values {
                    match v {
                        None | Some(serde_json::Value::Null) => builder.append_null(),
                        Some(serde_json::Value::String(s)) => builder.append_value(s),
                        Some(v) => builder.append_value(v.to_string()),
                    }
                }
                Arc::new(builder.finish())
            }
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlRequest {
    query: String,
    result_format: String,
    header: bool,
    sql_types_header: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DruidError {
    error: String,
    error_message: String,
}

/// Default connection timeout
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// Default request timeout
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Debug)]
pub struct DruidClient {
    client: Client,
    base_url: String,
}

impl DruidClient {
    /// Creates a new `DruidClient` with default timeout settings.
    ///
    /// Default timeouts:
    /// - Connection timeout: 30 seconds
    /// - Request timeout: 300 seconds (5 minutes)
    ///
    /// # Errors
    /// Returns an error if the HTTP client fails to build.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Self::with_timeouts(base_url, DEFAULT_CONNECT_TIMEOUT, DEFAULT_REQUEST_TIMEOUT)
    }

    /// Creates a new `DruidClient` with custom timeout settings.
    ///
    /// # Arguments
    /// * `base_url` - The base URL of the Druid server (e.g., `http://localhost:8888`)
    /// * `connect_timeout` - Maximum time to wait for a connection to be established
    /// * `request_timeout` - Maximum time to wait for a complete response
    ///
    /// # Errors
    /// Returns an error if the HTTP client fails to build.
    pub fn with_timeouts(
        base_url: impl Into<String>,
        connect_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()
            .map_err(|e| {
                Error::with_message_and_status(
                    format!("Failed to build HTTP client: {e}"),
                    Status::Internal,
                )
            })?;

        Ok(Self {
            client,
            base_url: base_url.into(),
        })
    }

    pub fn execute_query(&self, query: &str) -> Result<RecordBatch> {
        let url = format!("{}/druid/v2/sql", self.base_url);

        let request = SqlRequest {
            query: query.to_string(),
            result_format: "array".to_string(),
            header: true,
            sql_types_header: true,
        };

        let response = self.client.post(&url).json(&request).send().map_err(|e| {
            Error::with_message_and_status(format!("Failed to execute query: {e}"), Status::IO)
        })?;

        let status = response.status();
        let body = response.text().map_err(|e| {
            Error::with_message_and_status(format!("Failed to read response body: {e}"), Status::IO)
        })?;

        if !status.is_success() {
            if let Ok(druid_error) = serde_json::from_str::<DruidError>(&body) {
                return Err(Error::with_message_and_status(
                    format!("{}: {}", druid_error.error, druid_error.error_message),
                    Status::InvalidArguments,
                ));
            }
            return Err(Error::with_message_and_status(
                format!("Query failed with status {status}: {body}"),
                Status::Internal,
            ));
        }

        let rows: Vec<Vec<serde_json::Value>> = serde_json::from_str(&body).map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to parse response: {e}"),
                Status::Internal,
            )
        })?;

        Self::rows_to_record_batch(&rows)
    }

    fn rows_to_record_batch(rows: &[Vec<serde_json::Value>]) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        if rows.len() < 2 {
            return Err(Error::with_message_and_status(
                "Response must contain at least column names and types rows".to_string(),
                Status::Internal,
            ));
        }

        // Parse column names, defaulting to empty string for non-string values
        let column_names: Vec<&str> = rows[0].iter().map(|v| v.as_str().unwrap_or("")).collect();

        // Parse column types, defaulting to STRING for non-string or unknown types
        let column_types: Vec<DruidType> = rows[1]
            .iter()
            .map(|v| DruidType::from_sql_type(v.as_str().unwrap_or("STRING")))
            .collect();

        let data_rows = &rows[2..];

        let (fields, arrays): (Vec<_>, Vec<_>) = column_names
            .iter()
            .zip(&column_types)
            .enumerate()
            .map(|(col_idx, (name, dtype))| {
                // Use .get() for bounds-safe access, returning None for missing values
                let values = data_rows.iter().map(|row| row.get(col_idx));
                let field = Field::new(*name, dtype.to_arrow_type(), true);
                let array = dtype.build_array(values);
                (field, array)
            })
            .unzip();

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, arrays).map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to create RecordBatch: {e}"),
                Status::Internal,
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_druid_type_from_sql_type() {
        // Native Druid types
        assert_eq!(DruidType::from_sql_type("LONG"), DruidType::Int64);
        assert_eq!(DruidType::from_sql_type("FLOAT"), DruidType::Float32);
        assert_eq!(DruidType::from_sql_type("DOUBLE"), DruidType::Float64);
        assert_eq!(DruidType::from_sql_type("STRING"), DruidType::String);

        // SQL integer types
        assert_eq!(DruidType::from_sql_type("BIGINT"), DruidType::Int64);
        assert_eq!(DruidType::from_sql_type("INTEGER"), DruidType::Int64);
        assert_eq!(DruidType::from_sql_type("SMALLINT"), DruidType::Int64);
        assert_eq!(DruidType::from_sql_type("TINYINT"), DruidType::Int64);

        // SQL float types
        assert_eq!(DruidType::from_sql_type("DECIMAL"), DruidType::Float64);
        assert_eq!(DruidType::from_sql_type("REAL"), DruidType::Float64);

        // String types
        assert_eq!(DruidType::from_sql_type("VARCHAR"), DruidType::String);
        assert_eq!(DruidType::from_sql_type("CHAR"), DruidType::String);

        // Other types
        assert_eq!(DruidType::from_sql_type("BOOLEAN"), DruidType::Boolean);
        assert_eq!(DruidType::from_sql_type("TIMESTAMP"), DruidType::Timestamp);
        assert_eq!(DruidType::from_sql_type("DATE"), DruidType::Date);

        // Unknown defaults to String
        assert_eq!(DruidType::from_sql_type("COMPLEX<json>"), DruidType::String);
    }

    #[test]
    fn test_druid_type_to_arrow_type() {
        assert_eq!(DruidType::Int64.to_arrow_type(), DataType::Int64);
        assert_eq!(DruidType::Float32.to_arrow_type(), DataType::Float32);
        assert_eq!(DruidType::Float64.to_arrow_type(), DataType::Float64);
        assert_eq!(DruidType::Boolean.to_arrow_type(), DataType::Boolean);
        assert_eq!(DruidType::String.to_arrow_type(), DataType::Utf8);
        assert_eq!(
            DruidType::Timestamp.to_arrow_type(),
            DataType::Timestamp(TimeUnit::Millisecond, None)
        );
        assert_eq!(DruidType::Date.to_arrow_type(), DataType::Date32);
    }

    #[test]
    fn test_rows_to_record_batch_empty() {
        let result = DruidClient::rows_to_record_batch(&[]);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 0);
    }

    #[test]
    fn test_rows_to_record_batch_with_data() {
        let rows = vec![
            vec![serde_json::json!("name"), serde_json::json!("value")],
            vec![serde_json::json!("VARCHAR"), serde_json::json!("BIGINT")],
            vec![serde_json::json!("test"), serde_json::json!(42)],
        ];
        let result = DruidClient::rows_to_record_batch(&rows);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_rows_to_record_batch_missing_types() {
        let rows = vec![vec![serde_json::json!("name")]];
        let result = DruidClient::rows_to_record_batch(&rows);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_timestamp_array() {
        let values = [
            serde_json::json!("2015-09-12T00:46:58.771Z"),
            serde_json::json!(null),
        ];
        let array = DruidType::Timestamp.build_array(values.iter().map(Some));
        assert_eq!(array.len(), 2);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
    }

    #[test]
    fn test_build_date_array() {
        let values = [serde_json::json!("2015-09-12"), serde_json::json!(null)];
        let array = DruidType::Date.build_array(values.iter().map(Some));
        assert_eq!(array.len(), 2);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
    }
}
