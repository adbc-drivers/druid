use adbc_core::error::{Error, Result, Status};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int64Builder, ListBuilder,
        StringBuilder, TimestampMillisecondBuilder,
    },
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

/// Maps Druid SQL types to Arrow types and handles array construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DruidType {
    Int64,
    Float32,
    Float64,
    Boolean,
    Timestamp,
    Date,
    String,
    List(Box<DruidType>),
}

impl DruidType {
    /// Parse a Druid/SQL type string into a `DruidType`.
    pub(crate) fn from_sql_type(s: &str) -> Self {
        let upper = s.to_uppercase();
        // Handle parameterized ARRAY types like ARRAY<LONG>, ARRAY<STRING>
        if let Some(inner) = upper
            .strip_prefix("ARRAY<")
            .and_then(|rest| rest.strip_suffix('>'))
        {
            return Self::List(Box::new(Self::from_sql_type(inner)));
        }
        match upper.as_str() {
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
            // Bare ARRAY without element type defaults to List(String)
            "ARRAY" => Self::List(Box::new(Self::String)),
            // String types and unknown/complex types
            _ => Self::String,
        }
    }

    /// Convert to the corresponding Arrow `DataType`.
    pub(crate) fn to_arrow_type(&self) -> DataType {
        match self {
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Boolean => DataType::Boolean,
            Self::Timestamp => DataType::Timestamp(TimeUnit::Millisecond, None),
            Self::Date => DataType::Date32,
            Self::String => DataType::Utf8,
            Self::List(inner) => {
                DataType::List(Arc::new(Field::new("item", inner.to_arrow_type(), true)))
            }
        }
    }

    /// Build an Arrow array from an iterator of optional JSON values.
    fn build_array<'a>(
        &self,
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
                        chrono::DateTime::parse_from_rfc3339(s)
                            .ok()
                            .map(|dt| (dt.date_naive() - epoch).num_days() as i32)
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
            Self::List(inner_type) => Self::build_list_array(inner_type, &values),
        }
    }

    /// Build a `ListArray` from JSON array values using the given inner element type.
    fn build_list_array(inner_type: &DruidType, values: &[Option<&serde_json::Value>]) -> ArrayRef {
        /// Iterates over row values, appending each JSON array's elements to a
        /// `ListBuilder` using the provided element-extraction closure.
        /// Non-array and null values produce null list entries.
        macro_rules! build_typed_list {
            ($builder_type:ty, $extract:expr) => {{
                let mut builder = ListBuilder::new(<$builder_type>::new());
                for v in values {
                    if let Some(serde_json::Value::Array(arr)) = v {
                        for elem in arr {
                            builder.values().append_option($extract(elem));
                        }
                        builder.append(true);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }};
        }

        match inner_type {
            #[allow(clippy::cast_possible_truncation)]
            DruidType::Int64 => build_typed_list!(Int64Builder, |elem: &serde_json::Value| {
                elem.as_i64().or_else(|| elem.as_f64().map(|f| f as i64))
            }),
            #[allow(clippy::cast_possible_truncation)]
            DruidType::Float32 => {
                build_typed_list!(Float32Builder, |elem: &serde_json::Value| {
                    elem.as_f64().map(|f| f as f32)
                })
            }
            DruidType::Float64 => {
                build_typed_list!(Float64Builder, serde_json::Value::as_f64)
            }
            DruidType::Boolean => {
                build_typed_list!(BooleanBuilder, serde_json::Value::as_bool)
            }
            // For String and any other inner types, store elements as strings
            _ => {
                let mut builder = ListBuilder::new(StringBuilder::new());
                for v in values {
                    if let Some(serde_json::Value::Array(arr)) = v {
                        for elem in arr {
                            if elem.is_null() {
                                builder.values().append_null();
                            } else if let Some(s) = elem.as_str() {
                                builder.values().append_value(s);
                            } else {
                                builder.values().append_value(elem.to_string());
                            }
                        }
                        builder.append(true);
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SqlParameter {
    #[serde(rename = "type")]
    pub sql_type: String,
    pub value: serde_json::Value,
}

impl SqlParameter {
    /// Creates a VARCHAR parameter.
    pub fn varchar(value: impl Into<String>) -> Self {
        Self {
            sql_type: "VARCHAR".to_string(),
            value: serde_json::Value::String(value.into()),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SqlRequest {
    query: String,
    result_format: String,
    header: bool,
    types_header: bool,
    sql_types_header: bool,
    context: serde_json::Value,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    parameters: Vec<SqlParameter>,
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

type Credentials = (String, String);

#[derive(Debug)]
pub struct DruidClient {
    client: Client,
    base_url: String,
    credentials: Option<Credentials>,
}

impl DruidClient {
    /// Creates a new `DruidClient` with default timeout settings and no authentication.
    ///
    /// Default timeouts:
    /// - Connection timeout: 30 seconds
    /// - Request timeout: 300 seconds (5 minutes)
    ///
    /// # Errors
    /// Returns an error if the HTTP client fails to build.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        Self::with_auth(base_url, None, None)
    }

    /// Creates a new `DruidClient` with optional authentication credentials.
    ///
    /// Uses default timeout settings:
    /// - Connection timeout: 30 seconds
    /// - Request timeout: 300 seconds (5 minutes)
    ///
    /// # Arguments
    /// * `base_url` - The base URL of the Druid server (e.g., `http://localhost:8888`)
    /// * `username` - Optional username for HTTP Basic Authentication
    /// * `password` - Optional password for HTTP Basic Authentication
    ///
    /// # Errors
    /// Returns an error if the HTTP client fails to build.
    pub fn with_auth(
        base_url: impl Into<String>,
        username: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        Self::with_auth_and_timeouts(
            base_url,
            username,
            password,
            DEFAULT_CONNECT_TIMEOUT,
            DEFAULT_REQUEST_TIMEOUT,
        )
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
        Self::with_auth_and_timeouts(base_url, None, None, connect_timeout, request_timeout)
    }

    /// Creates a new `DruidClient` with optional authentication and custom timeouts.
    ///
    /// # Arguments
    /// * `base_url` - The base URL of the Druid server (e.g., `http://localhost:8888`)
    /// * `username` - Optional username for HTTP Basic Authentication
    /// * `password` - Optional password for HTTP Basic Authentication
    /// * `connect_timeout` - Maximum time to wait for a connection to be established
    /// * `request_timeout` - Maximum time to wait for a complete response
    ///
    /// # Errors
    /// Returns an error if the HTTP client fails to build.
    pub fn with_auth_and_timeouts(
        base_url: impl Into<String>,
        username: Option<String>,
        password: Option<String>,
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

        let credentials = match (&username, &password) {
            (Some(_), None) => {
                return Err(Error::with_message_and_status(
                    "Username provided without password".to_string(),
                    Status::InvalidArguments,
                ));
            }
            (None, Some(_)) => {
                return Err(Error::with_message_and_status(
                    "Password provided without username".to_string(),
                    Status::InvalidArguments,
                ));
            }
            _ => username.zip(password),
        };

        Ok(Self {
            client,
            base_url: base_url.into(),
            credentials,
        })
    }

    /// Fetches the Druid server version from the `/status` endpoint.
    ///
    /// Returns "unknown" if the version cannot be determined.
    pub fn get_server_version(&self) -> String {
        self.fetch_server_version()
            .unwrap_or_else(|_| "unknown".to_string())
    }

    /// Internal method to fetch server version, returning Result for error handling.
    fn fetch_server_version(&self) -> Result<String> {
        let url = format!("{}/status", self.base_url);

        let request = self.client.get(&url);
        let request = self.apply_auth(request);
        let response = request.send().map_err(|e| {
            Error::with_message_and_status(format!("Failed to fetch status: {e}"), Status::IO)
        })?;

        if !response.status().is_success() {
            return Err(Error::with_message_and_status(
                format!("Status request failed with status {}", response.status()),
                Status::IO,
            ));
        }

        let body: serde_json::Value = response.json().map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to parse status response: {e}"),
                Status::Internal,
            )
        })?;

        body.get("version")
            .and_then(|v| v.as_str())
            .map(String::from)
            .ok_or_else(|| {
                Error::with_message_and_status(
                    "Version field not found in status response".to_string(),
                    Status::Internal,
                )
            })
    }

    /// Applies HTTP Basic Authentication to a request if credentials are configured.
    fn apply_auth(
        &self,
        request: reqwest::blocking::RequestBuilder,
    ) -> reqwest::blocking::RequestBuilder {
        if let Some((username, password)) = &self.credentials {
            request.basic_auth(username, Some(password))
        } else {
            request
        }
    }

    pub(crate) fn execute_query(
        &self,
        query: &str,
        parameters: Vec<SqlParameter>,
        user_context: HashMap<String, serde_json::Value>,
    ) -> Result<RecordBatch> {
        let url = format!("{}/druid/v2/sql", self.base_url);

        // Build context: start with user context, then ensure sqlStringifyArrays is false
        // (the driver requires this for proper array handling)
        let mut context = user_context;
        context.insert(
            "sqlStringifyArrays".to_string(),
            serde_json::Value::Bool(false),
        );

        let request_body = SqlRequest {
            query: query.to_string(),
            result_format: "array".to_string(),
            header: true,
            types_header: true,
            sql_types_header: true,
            context: serde_json::json!(context),
            parameters,
        };

        let request = self.client.post(&url).json(&request_body);
        let request = self.apply_auth(request);
        let response = request.send().map_err(|e| {
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

        // With both typesHeader and sqlTypesHeader enabled, the response contains:
        // Row 0 = column names
        // Row 1 = native Druid types (e.g., STRING, LONG, ARRAY<LONG>)
        // Row 2 = SQL types (e.g., VARCHAR, BIGINT, ARRAY)
        // Rows 3+ = data
        if rows.len() < 3 {
            return Err(Error::with_message_and_status(
                "Response must contain column names, native types, and SQL types rows".to_string(),
                Status::Internal,
            ));
        }

        // Parse column names, defaulting to empty string for non-string values
        let column_names: Vec<&str> = rows[0].iter().map(|v| v.as_str().unwrap_or("")).collect();

        // Resolve column types from both header rows. The native type row (row 1)
        // is the richer source — it carries parameterized types like ARRAY<LONG>.
        // However, native type "LONG" is ambiguous (BIGINT, TIMESTAMP, DATE, BOOLEAN
        // all map to LONG at runtime), so we fall back to the SQL type row (row 2)
        // to disambiguate.
        let column_types: Vec<DruidType> = rows[1]
            .iter()
            .zip(rows[2].iter())
            .map(|(native_type, sql_type)| {
                let native = native_type.as_str().unwrap_or("STRING");
                if native.eq_ignore_ascii_case("LONG") {
                    DruidType::from_sql_type(sql_type.as_str().unwrap_or("BIGINT"))
                } else {
                    DruidType::from_sql_type(native)
                }
            })
            .collect();

        let data_rows = &rows[3..];

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
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

    #[test]
    fn test_new_client_without_auth() {
        let client = DruidClient::new("http://localhost:8888").unwrap();
        assert!(client.credentials.is_none());
    }

    #[test]
    fn test_new_client_with_auth() {
        let client = DruidClient::with_auth(
            "http://localhost:8888",
            Some("admin".to_string()),
            Some("secret".to_string()),
        )
        .unwrap();
        assert!(client.credentials.is_some());
        let (username, password) = client.credentials.unwrap();
        assert_eq!(username, "admin");
        assert_eq!(password, "secret");
    }

    #[test]
    fn test_new_client_with_partial_auth_username_only_fails() {
        let result =
            DruidClient::with_auth("http://localhost:8888", Some("admin".to_string()), None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("password"));
    }

    #[test]
    fn test_new_client_with_partial_auth_password_only_fails() {
        let result =
            DruidClient::with_auth("http://localhost:8888", None, Some("secret".to_string()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(err.message.contains("username"));
    }

    fn make_sql_request(query: &str, parameters: Vec<SqlParameter>) -> SqlRequest {
        SqlRequest {
            query: query.to_string(),
            result_format: "array".to_string(),
            header: true,
            types_header: true,
            sql_types_header: true,
            context: serde_json::json!({ "sqlStringifyArrays": false }),
            parameters,
        }
    }

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
            vec![serde_json::json!("STRING"), serde_json::json!("LONG")],
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
        // With both typesHeader and sqlTypesHeader, we need at least 3 rows
        let rows = vec![
            vec![serde_json::json!("name")],
            vec![serde_json::json!("STRING")],
        ];
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
        // Druid returns DATE values as ISO 8601 timestamp strings
        let values = [
            serde_json::json!("2015-09-12T00:00:00.000Z"),
            serde_json::json!(null),
        ];
        let array = DruidType::Date.build_array(values.iter().map(Some));
        assert_eq!(array.len(), 2);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
    }

    #[test]
    fn test_sql_request_serializes_with_parameters() {
        let request = make_sql_request(
            "SELECT ? + 1",
            vec![SqlParameter {
                sql_type: "BIGINT".to_string(),
                value: serde_json::json!(41),
            }],
        );
        let json = serde_json::to_value(&request).unwrap();
        let params = json.get("parameters").unwrap().as_array().unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0]["type"], "BIGINT");
        assert_eq!(params[0]["value"], 41);
    }

    #[test]
    fn test_sql_request_omits_empty_parameters() {
        let json = serde_json::to_value(&make_sql_request("SELECT 1", vec![])).unwrap();
        assert!(json.get("parameters").is_none());
    }

    #[test]
    fn test_druid_type_from_sql_type_array() {
        // ARRAY<LONG> -> List(Int64)
        assert_eq!(
            DruidType::from_sql_type("ARRAY<LONG>"),
            DruidType::List(Box::new(DruidType::Int64))
        );
        // ARRAY<STRING> -> List(String)
        assert_eq!(
            DruidType::from_sql_type("ARRAY<STRING>"),
            DruidType::List(Box::new(DruidType::String))
        );
        // ARRAY<DOUBLE> -> List(Float64)
        assert_eq!(
            DruidType::from_sql_type("ARRAY<DOUBLE>"),
            DruidType::List(Box::new(DruidType::Float64))
        );
        // ARRAY<FLOAT> -> List(Float32)
        assert_eq!(
            DruidType::from_sql_type("ARRAY<FLOAT>"),
            DruidType::List(Box::new(DruidType::Float32))
        );
    }

    #[test]
    fn test_druid_type_to_arrow_type_list() {
        let list_int = DruidType::List(Box::new(DruidType::Int64));
        assert_eq!(
            list_int.to_arrow_type(),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
        );

        let list_str = DruidType::List(Box::new(DruidType::String));
        assert_eq!(
            list_str.to_arrow_type(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn test_build_list_int64_array() {
        let values = [
            serde_json::json!([1, 2, 3]),
            serde_json::json!(null),
            serde_json::json!([4, 5]),
        ];
        let list_type = DruidType::List(Box::new(DruidType::Int64));
        let array = list_type.build_array(values.iter().map(Some));

        assert_eq!(array.len(), 3);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));

        let list_array = array
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .expect("should be a ListArray");

        // First element: [1, 2, 3]
        let first = list_array.value(0);
        let first_ints = first
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(first_ints.len(), 3);
        assert_eq!(first_ints.value(0), 1);
        assert_eq!(first_ints.value(1), 2);
        assert_eq!(first_ints.value(2), 3);

        // Third element: [4, 5]
        let third = list_array.value(2);
        let third_ints = third
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(third_ints.len(), 2);
        assert_eq!(third_ints.value(0), 4);
        assert_eq!(third_ints.value(1), 5);
    }

    #[test]
    fn test_build_list_string_array() {
        let values = [serde_json::json!(["a", "b", "c"]), serde_json::json!(["d"])];
        let list_type = DruidType::List(Box::new(DruidType::String));
        let array = list_type.build_array(values.iter().map(Some));

        assert_eq!(array.len(), 2);

        let list_array = array
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .expect("should be a ListArray");

        let first = list_array.value(0);
        let first_strs = first.as_string::<i32>();
        assert_eq!(first_strs.len(), 3);
        assert_eq!(first_strs.value(0), "a");
        assert_eq!(first_strs.value(1), "b");
        assert_eq!(first_strs.value(2), "c");
    }

    #[test]
    fn test_rows_to_record_batch_with_array_columns() {
        // Simulates a response with both typesHeader and sqlTypesHeader enabled:
        // row 0 = column names
        // row 1 = native Druid types
        // row 2 = SQL types
        // rows 3+ = data
        let rows = vec![
            vec![serde_json::json!("name"), serde_json::json!("scores")],
            vec![
                serde_json::json!("STRING"),
                serde_json::json!("ARRAY<LONG>"),
            ],
            vec![serde_json::json!("VARCHAR"), serde_json::json!("ARRAY")],
            vec![serde_json::json!("Alice"), serde_json::json!([90, 85, 92])],
        ];
        let result = DruidClient::rows_to_record_batch(&rows);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 2);

        // First column should be Utf8
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);

        // Second column should be List<Int64>
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
        );
    }

    #[test]
    fn test_sql_request_includes_types_header() {
        let json = serde_json::to_value(&make_sql_request("SELECT 1", vec![])).unwrap();
        assert_eq!(json.get("typesHeader").unwrap(), true);
        assert_eq!(json.get("sqlTypesHeader").unwrap(), true);
        assert_eq!(json["context"]["sqlStringifyArrays"], false);
    }
}
