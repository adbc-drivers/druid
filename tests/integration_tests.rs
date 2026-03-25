//! Integration tests for the Druid ADBC driver
//!
//! Most tests require a running Druid instance at <http://localhost:8888>
//! with the wikipedia datasource loaded. These are marked with `#[ignore]`
//! and can be run with `cargo test -- --ignored`.

use adbc_core::options::{InfoCode, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_array::cast::AsArray;
use druid_driver::{DruidConnection, DruidDriver};
use std::collections::HashSet;

fn get_connection() -> DruidConnection {
    let mut driver = DruidDriver::default();
    let mut db = driver.new_database().unwrap();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("http://localhost:8888".to_string()),
    )
    .unwrap();
    db.new_connection().unwrap()
}

#[test]
#[ignore]
fn test_execute_simple_query() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 1").unwrap();
    let reader = stmt.execute().unwrap();

    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "EXPR$0");

    let batches: Vec<_> = reader.collect();
    assert_eq!(batches.len(), 1);

    let batch = batches[0].as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);

    let col = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(col.value(0), 1);
}

#[test]
#[ignore]
fn test_execute_query_with_multiple_columns() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT channel, page, added FROM wikipedia LIMIT 3")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "channel");
    assert_eq!(schema.field(1).name(), "page");
    assert_eq!(schema.field(2).name(), "added");

    let batches: Vec<_> = reader.collect();
    assert_eq!(batches.len(), 1);

    let batch = batches[0].as_ref().unwrap();
    assert_eq!(batch.num_rows(), 3);
}

#[test]
#[ignore]
fn test_execute_query_with_string_columns() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT channel FROM wikipedia LIMIT 1")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let batches: Vec<_> = reader.collect();
    let batch = batches[0].as_ref().unwrap();

    let channel_col = batch.column(0).as_string::<i32>();
    assert!(!channel_col.value(0).is_empty());
}

#[test]
#[ignore]
fn test_execute_query_returns_correct_types() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT added, deleted, channel FROM wikipedia LIMIT 1")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let schema = reader.schema();

    // added and deleted are LONG in Druid -> Int64 in Arrow
    assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Int64);
    // channel is STRING in Druid -> Utf8 in Arrow
    assert_eq!(schema.field(2).data_type(), &arrow_schema::DataType::Utf8);
}

#[test]
fn test_execute_without_query_fails() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    let result = stmt.execute();
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_execute_invalid_sql_returns_error() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT * FROM nonexistent_table")
        .unwrap();
    let result = stmt.execute();
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_query_information_schema() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query(
        "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'druid'",
    )
    .unwrap();
    let reader = stmt.execute().unwrap();

    let batches: Vec<_> = reader.collect();
    assert!(!batches.is_empty());

    let batch = batches[0].as_ref().unwrap();
    assert!(batch.num_rows() >= 1); // At least wikipedia table should exist
}

#[test]
#[ignore]
fn test_query_with_null_values() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    // cityName and countryName can be null
    stmt.set_sql_query("SELECT cityName, countryName FROM wikipedia LIMIT 5")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let batches: Vec<_> = reader.collect();
    let batch = batches[0].as_ref().unwrap();
    assert_eq!(batch.num_rows(), 5);
}

#[test]
#[ignore]
fn test_multiple_statements_on_connection() {
    let mut conn = get_connection();

    // First query
    let mut stmt1 = conn.new_statement().unwrap();
    stmt1.set_sql_query("SELECT 1").unwrap();
    let reader1 = stmt1.execute().unwrap();
    let batches1: Vec<_> = reader1.collect();
    assert_eq!(batches1.len(), 1);

    // Second query
    let mut stmt2 = conn.new_statement().unwrap();
    stmt2.set_sql_query("SELECT 2").unwrap();
    let reader2 = stmt2.execute().unwrap();
    let batches2: Vec<_> = reader2.collect();
    assert_eq!(batches2.len(), 1);
}

#[test]
fn test_connection_without_uri_fails() {
    let mut driver = DruidDriver::default();
    let db = driver.new_database().unwrap();
    let result = db.new_connection();
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_connection_with_invalid_uri() {
    let mut driver = DruidDriver::default();
    let mut db = driver.new_database().unwrap();
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("http://nonexistent-host:9999".to_string()),
    )
    .unwrap();
    let mut conn = db.new_connection().unwrap();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT 1").unwrap();
    let result = stmt.execute();
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_aggregation_query() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT COUNT(*) AS cnt FROM wikipedia")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let batches: Vec<_> = reader.collect();
    assert_eq!(batches.len(), 1);

    let batch = batches[0].as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);

    let cnt = batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert!(cnt.value(0) > 0);
}

#[test]
#[ignore]
fn test_group_by_query() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT channel, COUNT(*) AS cnt FROM wikipedia GROUP BY channel LIMIT 5")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 2);

    let batches: Vec<_> = reader.collect();
    let batch = batches[0].as_ref().unwrap();
    assert!(batch.num_rows() > 0);
    assert!(batch.num_rows() <= 5);
}

#[test]
#[ignore]
fn test_timestamp_column() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT __time FROM wikipedia LIMIT 1")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    // __time should be reported as TIMESTAMP -> Arrow Timestamp
    assert_eq!(
        schema.field(0).data_type(),
        &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
    );

    let batches: Vec<_> = reader.collect();
    let batch = batches[0].as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1);

    // Verify we can read the timestamp value
    let time_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::TimestampMillisecondArray>()
        .unwrap();
    // Wikipedia data is from 2015, so timestamp should be > 0
    assert!(time_col.value(0) > 0);
}

#[test]
#[ignore]
fn test_bind_parameterized_query() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT ? + 1 AS the_answer").unwrap();

    let mut builder = arrow_array::builder::Int64Builder::new();
    builder.append_value(41);
    let array: arrow_array::ArrayRef = std::sync::Arc::new(builder.finish());
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "p",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

    stmt.bind(batch).unwrap();
    let reader = stmt.execute().unwrap();

    let batches: Vec<_> = reader.collect();
    assert_eq!(batches.len(), 1);

    let result_batch = batches[0].as_ref().unwrap();
    assert_eq!(result_batch.num_rows(), 1);

    let col = result_batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(col.value(0), 42);
}

#[test]
#[ignore]
fn test_array_columns() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT ARRAY[1, 2, 3] AS int_arr, ARRAY['a', 'b'] AS str_arr")
        .unwrap();
    let reader = stmt.execute().unwrap();

    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 2);

    // Verify List types with correct element types
    assert!(
        matches!(schema.field(0).data_type(), arrow_schema::DataType::List(f) if *f.data_type() == arrow_schema::DataType::Int64),
        "expected List<Int64>, got {:?}",
        schema.field(0).data_type()
    );
    assert!(
        matches!(schema.field(1).data_type(), arrow_schema::DataType::List(f) if *f.data_type() == arrow_schema::DataType::Utf8),
        "expected List<Utf8>, got {:?}",
        schema.field(1).data_type()
    );

    let batches: Vec<_> = reader.collect();
    let batch = batches[0].as_ref().unwrap();
    assert_eq!(batch.num_rows(), 1);

    // Verify int array values [1, 2, 3]
    let int_list = batch.column(0).as_list::<i32>();
    let int_arr = int_list.value(0);
    let int_values = int_arr.as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(int_values.values(), &[1, 2, 3]);

    // Verify string array values ["a", "b"]
    let str_list = batch.column(1).as_list::<i32>();
    let str_values = str_list.value(0);
    let str_array = str_values
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(str_array.value(0), "a");
    assert_eq!(str_array.value(1), "b");
}

#[test]
#[ignore]
fn test_execute_schema_simple() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT channel, page, added FROM wikipedia LIMIT 10")
        .unwrap();

    let schema = stmt.execute_schema().unwrap();

    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "channel");
    assert_eq!(schema.field(1).name(), "page");
    assert_eq!(schema.field(2).name(), "added");
    assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Utf8);
    assert_eq!(schema.field(2).data_type(), &arrow_schema::DataType::Int64);
}

#[test]
fn test_execute_schema_without_query_fails() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    let result = stmt.execute_schema();
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_execute_schema_does_not_consume_bind_data() {
    let mut conn = get_connection();
    let mut stmt = conn.new_statement().unwrap();
    stmt.set_sql_query("SELECT channel, page FROM wikipedia LIMIT 1")
        .unwrap();

    // Bind parameters (even though this query doesn't use them)
    let mut builder = arrow_array::builder::Int64Builder::new();
    builder.append_value(41);
    let array: arrow_array::ArrayRef = std::sync::Arc::new(builder.finish());
    let schema = std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "p",
        arrow_schema::DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
    stmt.bind(batch).unwrap();

    // Get schema - should NOT consume bind data
    let result_schema = stmt.execute_schema().unwrap();
    assert_eq!(result_schema.fields().len(), 2);
    assert_eq!(result_schema.field(0).name(), "channel");

    // Verify bind_data is still present by setting a new query with parameters
    // and verifying execution works
    stmt.set_sql_query("SELECT ? + 1 AS the_answer").unwrap();

    // Execute should still work with the previously bound parameters
    let reader = stmt.execute().unwrap();
    let batches: Vec<_> = reader.collect();
    assert_eq!(batches.len(), 1);

    let result_batch = batches[0].as_ref().unwrap();
    let col = result_batch
        .column(0)
        .as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(col.value(0), 42);
}

#[test]
#[ignore]
fn test_get_info_returns_all_codes() {
    let conn = get_connection();

    // Call get_info with None to get all supported info codes
    let mut reader = conn.get_info(None).unwrap();
    let batch = reader.next().unwrap().unwrap();

    // Should have at least 8 rows (all supported codes)
    assert!(batch.num_rows() >= 8, "Expected at least 8 info codes");

    // Verify schema
    assert_eq!(batch.schema().field(0).name(), "info_name");
    assert_eq!(batch.schema().field(1).name(), "info_value");
}

#[test]
#[ignore]
fn test_get_info_filters_by_codes() {
    let conn = get_connection();

    // Request only specific codes
    let codes = HashSet::from([InfoCode::VendorName, InfoCode::DriverName]);
    let mut reader = conn.get_info(Some(codes)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 2);
}

#[test]
#[ignore]
fn test_get_info_vendor_name() {
    let conn = get_connection();

    let codes = HashSet::from([InfoCode::VendorName]);
    let mut reader = conn.get_info(Some(codes)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify the value is "Apache Druid"
    let value_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::UnionArray>()
        .unwrap();

    let string_value = value_col.value(0);
    let string_array = string_value.as_string::<i32>();
    assert_eq!(string_array.value(0), "Apache Druid");
}

#[test]
#[ignore]
fn test_get_info_vendor_version() {
    let conn = get_connection();

    let codes = HashSet::from([InfoCode::VendorVersion]);
    let mut reader = conn.get_info(Some(codes)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify a version string is returned (should be like "26.0.0" or similar)
    let value_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::UnionArray>()
        .unwrap();

    let string_value = value_col.value(0);
    let string_array = string_value.as_string::<i32>();
    let version = string_array.value(0);

    // Version should not be empty or "unknown" when connected to a running Druid
    assert!(!version.is_empty(), "Version should not be empty");
    // Version format check - should contain at least one digit
    assert!(
        version.chars().any(|c| c.is_ascii_digit()),
        "Version should contain numbers: {version}"
    );
}

#[test]
#[ignore]
fn test_get_info_vendor_sql_is_true() {
    let conn = get_connection();

    let codes = HashSet::from([InfoCode::VendorSql]);
    let mut reader = conn.get_info(Some(codes)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify VendorSql is true
    let value_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::UnionArray>()
        .unwrap();

    let bool_value = value_col.value(0);
    let bool_array = bool_value.as_boolean();
    assert!(bool_array.value(0), "VendorSql should be true");
}

#[test]
#[ignore]
fn test_get_info_driver_adbc_version() {
    let conn = get_connection();

    let codes = HashSet::from([InfoCode::DriverAdbcVersion]);
    let mut reader = conn.get_info(Some(codes)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    assert_eq!(batch.num_rows(), 1);

    // Verify DriverAdbcVersion is 1_001_000 (ADBC 1.1.0)
    let value_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::UnionArray>()
        .unwrap();

    let int64_value = value_col.value(0);
    let int64_array = int64_value.as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(int64_array.value(0), 1_001_000);
}

#[test]
fn test_get_info_ignores_unsupported_codes() {
    let mut driver = DruidDriver::default();
    let mut db = driver.new_database().unwrap();
    // Use a fake URI - we won't actually connect
    db.set_option(
        OptionDatabase::Uri,
        OptionValue::String("http://localhost:9999".to_string()),
    )
    .unwrap();
    let conn = db.new_connection().unwrap();

    // Request only unsupported codes
    let codes = HashSet::from([InfoCode::VendorSubstraitMinVersion]);
    let mut reader = conn.get_info(Some(codes)).unwrap();
    let batch = reader.next().unwrap().unwrap();

    // Should return empty batch (no rows)
    assert_eq!(batch.num_rows(), 0);
}

#[test]
#[ignore]
fn test_get_table_types() {
    let conn = get_connection();
    let mut reader = conn.get_table_types().unwrap();

    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 2);

    let col = batch.column(0).as_string::<i32>();
    let types: Vec<&str> = (0..col.len()).map(|i| col.value(i)).collect();

    assert!(types.contains(&"TABLE"));
    assert!(types.contains(&"SYSTEM TABLE"));
}

#[test]
#[ignore]
fn test_get_table_schema_wikipedia() {
    let conn = get_connection();
    let schema = conn
        .get_table_schema(None, Some("druid"), "wikipedia")
        .unwrap();

    // Wikipedia table should have at least these columns
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();

    assert!(field_names.contains(&"__time"), "Should have __time column");
    assert!(
        field_names.contains(&"channel"),
        "Should have channel column"
    );
    assert!(field_names.contains(&"page"), "Should have page column");

    // Verify types
    let time_field = schema.field_with_name("__time").unwrap();
    assert_eq!(
        time_field.data_type(),
        &arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
        "__time should be Timestamp"
    );

    let channel_field = schema.field_with_name("channel").unwrap();
    assert_eq!(
        channel_field.data_type(),
        &arrow_schema::DataType::Utf8,
        "channel should be Utf8"
    );
}

#[test]
#[ignore]
fn test_get_table_schema_nonexistent_table() {
    let conn = get_connection();
    let result = conn.get_table_schema(None, Some("druid"), "nonexistent_table_12345");
    assert!(result.is_err(), "Should return error for nonexistent table");
}

#[test]
#[ignore]
fn test_get_table_schema_with_druid_catalog() {
    let conn = get_connection();
    // Druid uses "druid" as the only catalog
    let schema = conn
        .get_table_schema(Some("druid"), Some("druid"), "wikipedia")
        .unwrap();
    assert!(!schema.fields().is_empty(), "Schema should have fields");
}

#[test]
#[ignore]
fn test_get_table_schema_invalid_catalog() {
    let conn = get_connection();
    let result = conn.get_table_schema(Some("invalid_catalog"), Some("druid"), "wikipedia");
    assert!(result.is_err(), "Should return error for invalid catalog");
}
